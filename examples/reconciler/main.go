// Command example is the USER side of the PoC: a typed resource managed as
// an Entity Workflow via k8slib, exercising every property from the
// article — update-with-start, validators, serialized command queue,
// drift-healing reconcile ticks, request-id dedup across Continue-as-New,
// a user-defined saga command with a child workflow, a user-defined typed
// query, and deletion as a lifecycle transition. Commands and queries are
// self-contained TYPES (entity.Command / entity.Query interfaces) — no
// descriptor vars, no strings, and no explicit type parameters at any
// call site.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/temporal-entity/examples/reconciler/k8slib"
	"github.com/graphene-ci/temporal-entity/examples/reconciler/k8slib/fake"
	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	"github.com/graphene-ci/temporal-entity/pkg/entity"
)

// Deployment is the USER'S typed structure — not generated, not provided
// by any library. Spec fields are desired; ReadyReplicas is filled by the
// (fake) cluster as it converges.
type Deployment struct {
	Image         string `json:"image"`
	Replicas      int    `json:"replicas"`
	ReadyReplicas int    `json:"readyReplicas,omitempty"`
}

// BlueGreen is a user-defined COMMAND TYPE: its fields are the parameters,
// its methods carry the name, the response type, and the validation — one
// self-contained declaration, no descriptor vars, no strings at call sites.
type BlueGreen struct {
	NewImage string `json:"newImage"`
}
type BlueGreenRes struct {
	Switched string `json:"switched"`
}

func (BlueGreen) Name() entity.CommandName { return "blue-green" }
func (BlueGreen) Result() BlueGreenRes     { return BlueGreenRes{} }

// Request-only validation: no Snapshot needed, so no type parameters —
// the chassis detects the plain Validate() error form.
func (b BlueGreen) Validate() error {
	if b.NewImage == "" {
		return fmt.Errorf("newImage is required")
	}
	return nil
}

// Health is a user-defined QUERY TYPE, same shape.
type Health struct{}
type HealthRes struct {
	Ready   bool `json:"ready"`
	Heals   int  `json:"heals"`
	Drifted bool `json:"drifted"`
}

func (Health) Name() entity.QueryName { return "health" }
func (Health) Result() HealthRes      { return HealthRes{} }

// WarmupWorkflow is a user-defined CHILD workflow: independent work with
// its own event history, launched from a command handler.
func WarmupWorkflow(ctx workflow.Context, target string) (string, error) {
	if err := workflow.Sleep(ctx, 200*time.Millisecond); err != nil {
		return "", err
	}
	return "warmed:" + target, nil
}

const gvk = k8slib.GVK("apps/v1/Deployment")

var (
	webRef   = k8slib.ObjectRef{Namespace: "prod", Name: "web"}
	greenRef = k8slib.ObjectRef{Namespace: "prod", Name: "web-green"}
)

func newDeploymentKind() *k8slib.Kind[Deployment] {
	k := k8slib.NewKind[Deployment](gvk,
		// Kind-specific knowledge belongs to the user:
		k8slib.WithReady[Deployment](func(live Deployment) bool {
			return live.ReadyReplicas >= live.Replicas
		}),
		k8slib.WithDrifted[Deployment](func(desired, live Deployment) bool {
			return desired.Image != live.Image || desired.Replicas != live.Replicas
		}),
		k8slib.WithValidate[Deployment](func(d Deployment) error {
			if d.Replicas < 1 {
				return fmt.Errorf("replicas must be >= 1, got %d", d.Replicas)
			}
			if d.Image == "" {
				return fmt.Errorf("image is required")
			}
			return nil
		}),
		k8slib.WithReconcileEvery[Deployment](2*time.Second),
		k8slib.WithPolling[Deployment](200*time.Millisecond, 100),
		// Test-only: force Continue-as-New after every 2 commands so the
		// CAN path is observable in a short demo.
		k8slib.WithForceCANEveryNCommands[Deployment](2),
	)

	// User-defined command: blue-green image switch as a saga with a child
	// workflow — plain Temporal inside the handler. The command's name,
	// types, and validation all come from the BlueGreen type itself.
	entdefine.Handle(k.Def(),
		func(
			ctx workflow.Context,
			ec *k8slib.Ctx[Deployment],
			cmd BlueGreen,
		) (BlueGreenRes, error) {
			green := ec.Spec()
			green.Image = cmd.NewImage

			var warmed string
			saga := entdefine.NewSaga(ctx).
				Step("provision-green",
					func(c workflow.Context) error { return k8slib.ApplyObject(c, gvk, greenRef, green) },
					func(c workflow.Context) error { return k8slib.DeleteObject(c, gvk, greenRef) },
				).
				Step("warmup",
					func(c workflow.Context) error {
						cctx := workflow.WithChildOptions(c, workflow.ChildWorkflowOptions{
							WorkflowID: workflow.GetInfo(c).WorkflowExecution.ID + "/warmup",
						})
						return workflow.ExecuteChildWorkflow(cctx, WarmupWorkflow, string(greenRef.ID())).Get(c, &warmed)
					},
					nil,
				).
				Step("switch-traffic",
					func(c workflow.Context) error {
						ec.SetSpec(green)
						return k8slib.ApplyObject(c, gvk, webRef, green)
					},
					nil,
				).
				Step("retire-green",
					func(c workflow.Context) error { return k8slib.DeleteObject(c, gvk, greenRef) },
					nil,
				)
			if err := saga.Run(); err != nil {
				return BlueGreenRes{}, err
			}
			return BlueGreenRes{Switched: warmed}, nil
		},
	)

	// User-defined typed query: a projection over the read-only Snapshot.
	entdefine.HandleQuery(k.Def(),
		func(s k8slib.Snap[Deployment], _ Health) (HealthRes, error) {
			return HealthRes{
				Ready:   s.State.Ready,
				Heals:   s.State.Heals,
				Drifted: s.State.Drifted,
			}, nil
		})
	return k
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	c, err := client.Dial(client.Options{})
	if err != nil {
		return fmt.Errorf("temporal dial: %w", err)
	}
	defer c.Close()

	// Fake cluster converges one replica per observation.
	cluster := fake.New()
	cluster.Converge = func(live []byte) []byte {
		var d Deployment
		if json.Unmarshal(live, &d) != nil {
			return live
		}
		if d.ReadyReplicas < d.Replicas {
			d.ReadyReplicas++
		} else if d.ReadyReplicas > d.Replicas {
			d.ReadyReplicas = d.Replicas
		}
		out, _ := json.Marshal(d)
		return out
	}

	deployments := newDeploymentKind()

	// --- composition root: worker ---
	w := worker.New(c, "k8s", worker.Options{})
	if err := deployments.Register(w); err != nil {
		return fmt.Errorf("register kind: %w", err)
	}
	w.RegisterWorkflow(WarmupWorkflow)
	k8slib.RegisterActivities(w, cluster)
	if err := w.Start(); err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	defer w.Stop()

	for _, ci := range deployments.Def().Commands() {
		fmt.Printf("[reg] command %-12s req=%-25s res=%s\n", ci.Name, ci.ReqType, ci.ResType)
	}
	for _, qi := range deployments.Def().Queries() {
		fmt.Printf("[reg] query   %-12s req=%-25s res=%s\n", qi.Name, qi.ReqType, qi.ResType)
	}

	// --- scenario ---
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	web := entclient.Bind(deployments.Def(), c, "k8s")
	webID := webRef.ID()
	failed := false
	check := func(name string, cond bool, detail string) {
		status := "OK "
		if !cond {
			status = "FAIL"
			failed = true
		}
		fmt.Printf("[%s] %-42s %s\n", status, name, detail)
	}

	// 1. Update-with-start: create the entity and apply in one round trip.
	// Note the call shape: descriptor carries the types, nothing explicit.
	res, err := entclient.ExecWithStart(ctx, web, webID,
		Deployment{Image: "nginx:1.25", Replicas: 3},
		k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.25", Replicas: 3}})
	check("update-with-start creates entity", err == nil && res.Ready, fmt.Sprintf("res=%+v err=%v", res, err))

	d1, err := web.Describe(ctx, webID)
	check("describe query", err == nil && d1.Phase == entity.PhaseReady,
		fmt.Sprintf("phase=%s ready=%v run=%s", d1.Phase, d1.State.Ready, short(d1.RunID)))

	// 2. Validator rejects before Event History.
	_, err = entclient.Exec(ctx, web, webID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.25", Replicas: 0}})
	check("validator rejects bad spec", err != nil, fmt.Sprintf("err=%v", err))

	// 3. Drift injection -> reconcile tick heals; user-defined typed query
	// observes it.
	cluster.Corrupt(string(gvk), string(webID), mustJSON(Deployment{Image: "evil:latest", Replicas: 1, ReadyReplicas: 1}))
	healed := waitFor(ctx, func() bool {
		h, err := entclient.Read(ctx, web, webID, Health{})
		return err == nil && h.Heals >= 1 && h.Ready
	})
	h1, _ := entclient.Read(ctx, web, webID, Health{})
	check("drift healed by reconcile tick", healed, fmt.Sprintf("health=%+v", h1))

	// 4. Out-of-band deletion -> recreated.
	cluster.Vanish(string(gvk), string(webID))
	recreated := waitFor(ctx, func() bool {
		h, err := entclient.Read(ctx, web, webID, Health{})
		return err == nil && h.Heals >= 2 && h.Ready
	})
	check("vanished object recreated", recreated, "")

	// 5. Request-id dedup: same request id twice -> second is served from
	// the completed-ops cache (same result, no re-execution).
	reqID := entity.RequestID("dedup-demo-1")
	r1, err1 := entclient.ExecWithRequestID(ctx, web, webID, reqID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.26", Replicas: 3}})
	r2, err2 := entclient.ExecWithRequestID(ctx, web, webID, reqID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.26", Replicas: 3}})
	check("request-id dedup", err1 == nil && err2 == nil && r1.Digest == r2.Digest,
		fmt.Sprintf("digest1=%s digest2=%s", r1.Digest, r2.Digest))

	// 6. Continue-as-New (forced every 2 commands): run id changes, entity
	// survives, and the dedup cache crossed the CAN boundary.
	for i := 0; i < 3; i++ {
		_, err := entclient.Exec(ctx, web, webID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.26", Replicas: 3 + i}})
		if err != nil {
			return fmt.Errorf("apply %d: %w", i, err)
		}
	}
	canOK := waitFor(ctx, func() bool {
		d, err := web.Describe(ctx, webID)
		return err == nil && d.RunID != d1.RunID && d.Phase == entity.PhaseReady
	})
	d3, _ := web.Describe(ctx, webID)
	r3, err3 := entclient.ExecWithRequestID(ctx, web, webID, reqID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.26", Replicas: 3}})
	check("continue-as-new happened", canOK, fmt.Sprintf("run %s -> %s", short(d1.RunID), short(d3.RunID)))
	check("dedup survives continue-as-new", err3 == nil && r3.Digest == r1.Digest,
		fmt.Sprintf("cached digest=%s", r3.Digest))

	// 7. User-defined saga command with a child workflow.
	bg, err := entclient.Exec(ctx, web, webID, BlueGreen{NewImage: "nginx:1.27"})
	d4, _ := web.Describe(ctx, webID)
	check("blue-green saga + child workflow", err == nil && d4.Spec.Image == "nginx:1.27",
		fmt.Sprintf("res=%+v spec.image=%s", bg, d4.Spec.Image))

	// 8. Deletion as lifecycle transition: signal -> finalize -> completed;
	// the completed workflow is still queryable and the object is gone.
	if err := web.Delete(ctx, webID); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	deleted := waitFor(ctx, func() bool {
		d, err := web.Describe(ctx, webID)
		return err == nil && d.Phase == entity.PhaseDeleted
	})
	_, foundAfter, _ := cluster.Get(context.Background(), string(gvk), string(webID))
	check("deletion lifecycle", deleted && !foundAfter, fmt.Sprintf("objectInCluster=%v", foundAfter))

	// 9. Commands to a deleted entity are rejected.
	_, err = entclient.Exec(ctx, web, webID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.27", Replicas: 1}})
	check("commands after delete rejected", err != nil, fmt.Sprintf("err=%v", err))

	if failed {
		return fmt.Errorf("some checks failed")
	}
	fmt.Println("all checks passed")
	return nil
}

func waitFor(ctx context.Context, cond func() bool) bool {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		if cond() {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func short(runID string) string {
	if len(runID) > 8 {
		return runID[:8]
	}
	return runID
}
