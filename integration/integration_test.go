// Package integration runs the full Entity Lifecycle Pattern property
// suite against a real Temporal dev server (started via the SDK's
// testsuite; set TEMPORAL_CLI to an existing binary to skip the download).
// The fixture is a "deployment" resource managed by the reconciler example
// library — every check maps 1:1 to a property from Temporal's platform
// control-plane article.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/temporal-entity/examples/reconciler/k8slib"
	"github.com/graphene-ci/temporal-entity/examples/reconciler/k8slib/fake"
	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	"github.com/graphene-ci/temporal-entity/pkg/entity"
)

type Deployment struct {
	Image         string `json:"image"`
	Replicas      int    `json:"replicas"`
	ReadyReplicas int    `json:"readyReplicas,omitempty"`
}

type BlueGreen struct {
	NewImage string `json:"newImage"`
}
type BlueGreenRes struct {
	Switched string `json:"switched"`
}

func (BlueGreen) Name() entity.CommandName { return "blue-green" }
func (BlueGreen) Result() BlueGreenRes     { return BlueGreenRes{} }
func (b BlueGreen) Validate() error {
	if b.NewImage == "" {
		return fmt.Errorf("newImage is required")
	}
	return nil
}

type Health struct{}
type HealthRes struct {
	Ready   bool `json:"ready"`
	Heals   int  `json:"heals"`
	Drifted bool `json:"drifted"`
}

// ValidateWith covers the state-aware validator layer: reject the switch
// while the entity is being deleted.
func (b BlueGreen) ValidateWith(s k8slib.Snap[Deployment]) error {
	if s.MarkedForDeletion {
		return fmt.Errorf("entity is being deleted")
	}
	return nil
}

func (Health) Name() entity.QueryName { return "health" }
func (Health) Result() HealthRes      { return HealthRes{} }

func WarmupWorkflow(ctx workflow.Context, target string) (string, error) {
	if err := workflow.Sleep(ctx, 100*time.Millisecond); err != nil {
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
			return nil
		}),
		k8slib.WithReconcileEvery[Deployment](2*time.Second),
		k8slib.WithPolling[Deployment](100*time.Millisecond, 200),
		k8slib.WithForceCANEveryNCommands[Deployment](2),
		k8slib.WithSearchAttributes[Deployment](true),
	)

	entdefine.Handle(k.Def(),
		func(ctx workflow.Context, ec *k8slib.Ctx[Deployment], cmd BlueGreen) (BlueGreenRes, error) {
			green := ec.Spec()
			green.Image = cmd.NewImage

			var warmed string
			saga := entdefine.NewSaga(ctx).
				Step("provision-green",
					func(c workflow.Context) error { return k8slib.ApplyObject(c, gvk, greenRef, green) },
					func(c workflow.Context) error { return k8slib.DeleteObject(c, gvk, greenRef) },
				).
				Step("verify-green",
					func(c workflow.Context) error {
						_, found, err := k8slib.GetObject[Deployment](c, gvk, greenRef)
						if err != nil {
							return err
						}
						if !found {
							return fmt.Errorf("green object missing")
						}
						return nil
					},
					nil,
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

	entdefine.HandleQuery(k.Def(),
		func(s k8slib.Snap[Deployment], _ Health) (HealthRes, error) {
			return HealthRes{Ready: s.State.Ready, Heals: s.State.Heals, Drifted: s.State.Drifted}, nil
		})
	return k
}

func TestEntityLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test needs a dev server")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	srv, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath:  os.Getenv("TEMPORAL_CLI"),
		ClientOptions: &client.Options{},
		LogLevel:      "error",
		SearchAttributes: temporal.NewSearchAttributes(
			entdefine.SearchAttrKind.ValueSet("seed"),
			entdefine.SearchAttrPhase.ValueSet("seed"),
		),
	})
	if err != nil {
		t.Fatalf("start dev server: %v", err)
	}
	defer func() { _ = srv.Stop() }()
	c := srv.Client()

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
	w := worker.New(c, "k8s", worker.Options{})
	if err := deployments.Register(w); err != nil {
		t.Fatalf("register kind: %v", err)
	}
	w.RegisterWorkflow(WarmupWorkflow)
	k8slib.RegisterActivities(w, cluster)
	if err := w.Start(); err != nil {
		t.Fatalf("worker: %v", err)
	}
	defer w.Stop()

	web := entclient.Bind(deployments.Def(), c, "k8s")
	webID := webRef.ID()

	waitFor := func(cond func() bool) bool {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && ctx.Err() == nil {
			if cond() {
				return true
			}
			time.Sleep(200 * time.Millisecond)
		}
		return false
	}

	// 1. Update-with-start creates the entity and applies in one round trip.
	res, err := entclient.ExecWithStart(ctx, web, webID,
		Deployment{Image: "nginx:1.25", Replicas: 3},
		k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.25", Replicas: 3}})
	if err != nil || !res.Ready {
		t.Fatalf("update-with-start: res=%+v err=%v", res, err)
	}

	d1, err := web.Describe(ctx, webID)
	if err != nil || d1.Phase != entity.PhaseReady {
		t.Fatalf("describe: %+v err=%v", d1, err)
	}

	// 2. Kind-level validator rejects before Event History.
	if _, err := entclient.Exec(ctx, web, webID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.25", Replicas: 0}}); err == nil {
		t.Fatal("bad spec accepted")
	}
	// 2b. Command's own Validate() rejects too.
	if _, err := entclient.Exec(ctx, web, webID, BlueGreen{}); err == nil {
		t.Fatal("empty blue-green accepted")
	}

	// 3. Drift injection -> reconcile tick heals.
	corrupt, _ := json.Marshal(Deployment{Image: "evil:latest", Replicas: 1, ReadyReplicas: 1})
	cluster.Corrupt(string(gvk), string(webID), corrupt)
	if !waitFor(func() bool {
		h, err := entclient.Read(ctx, web, webID, Health{})
		return err == nil && h.Heals >= 1 && h.Ready
	}) {
		t.Fatal("drift not healed")
	}

	// 4. Out-of-band deletion -> recreated.
	cluster.Vanish(string(gvk), string(webID))
	if !waitFor(func() bool {
		h, err := entclient.Read(ctx, web, webID, Health{})
		return err == nil && h.Heals >= 2 && h.Ready
	}) {
		t.Fatal("vanished object not recreated")
	}

	// 5. Request-id dedup: same id twice -> cached result.
	reqID := entity.RequestID("dedup-1")
	r1, err1 := entclient.ExecWithRequestID(ctx, web, webID, reqID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.26", Replicas: 3}})
	r2, err2 := entclient.ExecWithRequestID(ctx, web, webID, reqID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.26", Replicas: 3}})
	if err1 != nil || err2 != nil || r1.Digest != r2.Digest {
		t.Fatalf("dedup: %v %v %s %s", err1, err2, r1.Digest, r2.Digest)
	}

	// 6. Continue-as-New (forced): run id changes, dedup survives the boundary.
	for i := 0; i < 3; i++ {
		if _, err := entclient.Exec(ctx, web, webID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.26", Replicas: 3 + i}}); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if !waitFor(func() bool {
		d, err := web.Describe(ctx, webID)
		return err == nil && d.RunID != d1.RunID && d.Phase == entity.PhaseReady
	}) {
		t.Fatal("continue-as-new did not happen")
	}
	r3, err := entclient.ExecWithRequestID(ctx, web, webID, reqID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.26", Replicas: 3}})
	if err != nil || r3.Digest != r1.Digest {
		t.Fatalf("dedup across CAN: %v %s vs %s", err, r3.Digest, r1.Digest)
	}

	// 7. User-defined saga command with a child workflow.
	bg, err := entclient.Exec(ctx, web, webID, BlueGreen{NewImage: "nginx:1.27"})
	if err != nil {
		t.Fatalf("blue-green: %v", err)
	}
	if d, _ := web.Describe(ctx, webID); d.Spec.Image != "nginx:1.27" {
		t.Fatalf("blue-green did not switch: %+v (res=%+v)", d.Spec, bg)
	}

	// 8. Deletion lifecycle: drain -> finalize -> completed but queryable.
	if err := web.Delete(ctx, webID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !waitFor(func() bool {
		d, err := web.Describe(ctx, webID)
		return err == nil && d.Phase == entity.PhaseDeleted
	}) {
		t.Fatal("entity not deleted")
	}
	if _, found, _ := cluster.Get(context.Background(), string(gvk), string(webID)); found {
		t.Fatal("object still in cluster after delete")
	}

	// 9. Commands to a deleted entity are rejected.
	if _, err := entclient.Exec(ctx, web, webID, k8slib.Apply[Deployment]{Spec: Deployment{Image: "nginx:1.27", Replicas: 1}}); err == nil {
		t.Fatal("command to deleted entity accepted")
	}

	// 10. CreateOrAttach: first call creates, second attaches to the same run.
	apiRef := k8slib.ObjectRef{Namespace: "prod", Name: "api"}
	run1, err := web.CreateOrAttach(ctx, apiRef.ID(), Deployment{Image: "api:1", Replicas: 1})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !waitFor(func() bool {
		d, err := web.Describe(ctx, apiRef.ID())
		return err == nil && d.Phase == entity.PhaseReady
	}) {
		t.Fatal("created entity not ready")
	}
	run2, err := web.CreateOrAttach(ctx, apiRef.ID(), Deployment{Image: "api:2", Replicas: 9})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if run1.GetRunID() != run2.GetRunID() {
		t.Fatalf("attach spawned a new run: %s vs %s", run1.GetRunID(), run2.GetRunID())
	}
	if err := web.Delete(ctx, apiRef.ID()); err != nil {
		t.Fatalf("delete api: %v", err)
	}

	// 11. Failing creation -> create_failed phase, error surfaced to caller.
	brokenRef := k8slib.ObjectRef{Namespace: "prod", Name: "broken"}
	cluster.FailApplyWith(string(gvk), string(brokenRef.ID()),
		temporal.NewNonRetryableApplicationError("quota exceeded", "QuotaError", nil))
	if _, err := entclient.ExecWithStart(ctx, web, brokenRef.ID(),
		Deployment{Image: "broken:1", Replicas: 1},
		k8slib.Apply[Deployment]{Spec: Deployment{Image: "broken:1", Replicas: 1}}); err == nil {
		t.Fatal("creation against failing cluster succeeded")
	}
	if !waitFor(func() bool {
		d, err := web.Describe(ctx, brokenRef.ID())
		return err == nil && d.Phase == entity.PhaseCreateFailed
	}) {
		t.Fatal("phase is not create_failed")
	}

	// 12. Failing finalizer -> delete_failed phase.
	fragileRef := k8slib.ObjectRef{Namespace: "prod", Name: "fragile"}
	if _, err := entclient.ExecWithStart(ctx, web, fragileRef.ID(),
		Deployment{Image: "fragile:1", Replicas: 1},
		k8slib.Apply[Deployment]{Spec: Deployment{Image: "fragile:1", Replicas: 1}}); err != nil {
		t.Fatalf("create fragile: %v", err)
	}
	cluster.FailDeleteWith(string(gvk), string(fragileRef.ID()),
		temporal.NewNonRetryableApplicationError("stuck finalizer", "StuckError", nil))
	if err := web.Delete(ctx, fragileRef.ID()); err != nil {
		t.Fatalf("delete fragile: %v", err)
	}
	if !waitFor(func() bool {
		d, err := web.Describe(ctx, fragileRef.ID())
		return err == nil && d.Phase == entity.PhaseDeleteFailed
	}) {
		t.Fatal("phase is not delete_failed")
	}
}
