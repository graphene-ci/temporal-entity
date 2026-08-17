// Package k8slib manages Kubernetes-style resources as Temporal Entity
// Workflows: one durable workflow per resource, holding its desired spec,
// applying it to the cluster, watching for drift on a periodic tick, and
// healing it — a reconciler whose control loop is an entity workflow.
//
// The package knows nothing about concrete kinds. The USER brings the
// typed structure (their own struct, k8s.io/api types, CRD types — any
// JSON-serializable T) plus the two pieces of kind-specific knowledge the
// generic machinery can't have: what "ready" means and what counts as
// drift. No code generation anywhere.
package k8slib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	"github.com/graphene-ci/temporal-entity/pkg/entity"
)

// State is the compact per-resource state carried in the entity envelope.
// Live is the last observed object; the full desired manifest lives in the
// envelope's Spec. Anything bigger (rendered manifests, diffs, plans)
// belongs in external storage with references kept here.
type State[T any] struct {
	Live          T      `json:"live"`
	Ready         bool   `json:"ready"`
	DesiredDigest string `json:"desiredDigest"`
	Drifted       bool   `json:"drifted"`
	Heals         int    `json:"heals"`
}

// Ctx is a generic ALIAS (not a wrapper — the identical type) that bakes
// in the functional dependency State = State[T], so k8slib users write one
// type parameter instead of two:
//
//	func(ctx workflow.Context, ec *k8slib.Ctx[Deployment], cmd BlueGreen) ...
type Ctx[T any] = entdefine.Ctx[T, State[T]]

// Snap is the read-only sibling of Ctx for validators and query handlers:
//
//	func (b BlueGreen) ValidateWith(s k8slib.Snap[Deployment]) error
type Snap[T any] = entity.Snapshot[T, State[T]]

// ApplyRes is the result of the built-in Apply command.
type ApplyRes struct {
	Digest string `json:"digest"`
	Ready  bool   `json:"ready"`
}

// Apply is the kind's built-in command TYPE: replace the desired spec.
//
//	res, err := entclient.Exec(ctx, cl, id, k8slib.Apply[Deployment]{Spec: d})
type Apply[T any] struct {
	Spec T `json:"spec"`
}

// Name implements entity.Command.
func (Apply[T]) Name() entity.CommandName { return "apply" }

// Result implements entity.Command (phantom).
func (Apply[T]) Result() ApplyRes { return ApplyRes{} }

// GVK identifies a resource kind ("apps/v1/Deployment").
type GVK string

// ObjectRef names one object within a kind. Value type; converts to the
// entity's ResourceID.
type ObjectRef struct {
	Namespace string
	Name      string
}

// ID renders the ref as the entity resource id ("namespace/name").
func (r ObjectRef) ID() entity.ResourceID {
	return entity.ResourceID(r.Namespace + "/" + r.Name)
}

// ParseObjectRef validates a "namespace/name" string from external input.
func ParseObjectRef(s string) (ObjectRef, error) {
	i := strings.IndexByte(s, '/')
	if i <= 0 || i == len(s)-1 {
		return ObjectRef{}, fmt.Errorf("object ref %q: want namespace/name", s)
	}
	return ObjectRef{Namespace: s[:i], Name: s[i+1:]}, nil
}

// Kind wires one user-typed resource kind into the entity chassis.
type Kind[T any] struct {
	def *entdefine.Definition[T, State[T]]
	cfg *config[T]
}

type config[T any] struct {
	gvk            GVK
	ready          func(live T) bool
	drifted        func(desired, live T) bool
	validate       func(desired T) error
	reconcileEvery time.Duration
	pollInterval   time.Duration
	pollAttempts   int
	searchAttrs    bool
	forceCANEvery  int
}

// Option configures a Kind at declaration time.
type Option[T any] func(*config[T])

// WithReady supplies the user's readiness predicate over the live object.
// Default: object exists.
func WithReady[T any](fn func(live T) bool) Option[T] {
	return func(c *config[T]) { c.ready = fn }
}

// WithDrifted supplies the user's drift predicate: does the live object
// still match the desired one? Default: byte-equality of JSON encodings.
func WithDrifted[T any](fn func(desired, live T) bool) Option[T] {
	return func(c *config[T]) { c.drifted = fn }
}

// WithValidate supplies a deterministic structural validator for desired
// specs, run as the apply command's update validator — invalid specs are
// rejected before anything reaches Event History. Real validation is the
// cluster's job; its errors come back through the apply result.
func WithValidate[T any](fn func(desired T) error) Option[T] {
	return func(c *config[T]) { c.validate = fn }
}

// WithReconcileEvery sets the drift-check tick period (default 30s).
func WithReconcileEvery[T any](d time.Duration) Option[T] {
	return func(c *config[T]) { c.reconcileEvery = d }
}

// WithPolling tunes the wait-until-ready poll loop (default 1s x 60).
func WithPolling[T any](interval time.Duration, attempts int) Option[T] {
	return func(c *config[T]) { c.pollInterval = interval; c.pollAttempts = attempts }
}

// WithSearchAttributes enables EntityKind/EntityPhase search attributes.
func WithSearchAttributes[T any](on bool) Option[T] {
	return func(c *config[T]) { c.searchAttrs = on }
}

// WithForceCANEveryNCommands is test-only; see the entity package.
func WithForceCANEveryNCommands[T any](n int) Option[T] {
	return func(c *config[T]) { c.forceCANEvery = n }
}

// NewKind declares a resource kind for the user's type T. gvk becomes part
// of the entity kind name and thus of every workflow ID:
// "k8s/{gvk}/{resource-id}".
func NewKind[T any](gvk GVK, opts ...Option[T]) *Kind[T] {
	cfg := &config[T]{
		gvk:            gvk,
		reconcileEvery: 30 * time.Second,
		pollInterval:   time.Second,
		pollAttempts:   60,
	}
	for _, o := range opts {
		o(cfg)
	}

	def := entdefine.New[T, State[T]](entity.KindName("k8s/"+string(gvk)),
		entdefine.WithInit[T, State[T]](func(ctx workflow.Context, desired T) (State[T], error) {
			return applyAndWaitReady(ctx, cfg, desired)
		}),
		entdefine.WithFinalize[T, State[T]](func(ctx workflow.Context, _ *State[T]) error {
			return deleteAndWaitGone(ctx, cfg)
		}),
		entdefine.WithReconcileEvery[T, State[T]](cfg.reconcileEvery, func(ctx workflow.Context, ec *Ctx[T]) error {
			return reconcileDrift(ctx, cfg, ec)
		}),
		entdefine.WithSearchAttributes[T, State[T]](cfg.searchAttrs),
		entdefine.WithForceCANEveryNCommands[T, State[T]](cfg.forceCANEvery),
	)

	// The universal command: replace the desired spec. Runs on the entity's
	// main loop, serialized with every other mutation and with drift ticks.
	// The kind's user-supplied spec validation attaches as a kind-level
	// validator — Apply[T] itself cannot capture cfg.
	entdefine.Handle(def,
		func(ctx workflow.Context, ec *Ctx[T], cmd Apply[T]) (ApplyRes, error) {
			ec.SetSpec(cmd.Spec)
			st, err := applyAndWaitReady(ctx, cfg, cmd.Spec)
			if err != nil {
				return ApplyRes{}, err
			}
			*ec.State() = st
			return ApplyRes{Digest: st.DesiredDigest, Ready: st.Ready}, nil
		},
	).Validate(func(_ Snap[T], cmd Apply[T]) error {
		if cfg.validate != nil {
			return cfg.validate(cmd.Spec)
		}
		return nil
	})

	return &Kind[T]{def: def, cfg: cfg}
}

// Def exposes the underlying entity definition so users can register their
// own typed commands on the kind (entdefine.Handle(kind.Def(), ...)).
func (k *Kind[T]) Def() *entdefine.Definition[T, State[T]] { return k.def }

// Register attaches the kind's entity workflow to a worker.
func (k *Kind[T]) Register(w worker.WorkflowRegistry) error {
	return k.def.Register(w)
}

// --- universal handlers: ordinary Temporal code, generic over T ---

func activityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	})
}

func resourceID(ctx workflow.Context, gvk GVK) string {
	// Workflow ID is "k8s/{gvk}/{resource-id}"; strip the prefix.
	full := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := "k8s/" + string(gvk) + "/"
	if len(full) > len(prefix) {
		return full[len(prefix):]
	}
	return full
}

func digestOf(manifest []byte) string {
	sum := sha256.Sum256(manifest)
	return hex.EncodeToString(sum[:8])
}

func applyAndWaitReady[T any](ctx workflow.Context, cfg *config[T], desired T) (State[T], error) {
	var st State[T]
	manifest, err := json.Marshal(desired)
	if err != nil {
		return st, fmt.Errorf("encode desired: %w", err)
	}
	id := resourceID(ctx, cfg.gvk)
	actx := activityCtx(ctx)

	if err := workflow.ExecuteActivity(actx, applyActivityName, cfg.gvk, id, manifest).Get(ctx, nil); err != nil {
		return st, fmt.Errorf("apply: %w", err)
	}
	st.DesiredDigest = digestOf(manifest)

	// Poll until the user's readiness predicate holds — the cluster
	// converges asynchronously.
	for attempt := 0; attempt < cfg.pollAttempts; attempt++ {
		var got getResult
		if err := workflow.ExecuteActivity(actx, getActivityName, cfg.gvk, id).Get(ctx, &got); err != nil {
			return st, fmt.Errorf("get: %w", err)
		}
		if got.Found {
			var live T
			if err := json.Unmarshal(got.Manifest, &live); err != nil {
				return st, fmt.Errorf("decode live: %w", err)
			}
			st.Live = live
			if cfg.ready == nil || cfg.ready(live) {
				st.Ready = true
				return st, nil
			}
		}
		if err := workflow.Sleep(ctx, cfg.pollInterval); err != nil {
			return st, err
		}
	}
	return st, fmt.Errorf("resource %s/%s not ready after %d attempts", cfg.gvk, id, cfg.pollAttempts)
}

// reconcileDrift is the periodic health check: read the live object,
// compare with desired, re-apply on drift or disappearance.
func reconcileDrift[T any](ctx workflow.Context, cfg *config[T], ec *Ctx[T]) error {
	if ec.Phase() != entity.PhaseReady {
		return nil
	}
	id := resourceID(ctx, cfg.gvk)
	actx := activityCtx(ctx)
	st := ec.State()

	var got getResult
	if err := workflow.ExecuteActivity(actx, getActivityName, cfg.gvk, id).Get(ctx, &got); err != nil {
		return fmt.Errorf("get: %w", err)
	}

	drifted := !got.Found
	if got.Found {
		var live T
		if err := json.Unmarshal(got.Manifest, &live); err != nil {
			return fmt.Errorf("decode live: %w", err)
		}
		st.Live = live
		if cfg.drifted != nil {
			drifted = cfg.drifted(ec.Spec(), live)
		} else {
			desired, err := json.Marshal(ec.Spec())
			if err != nil {
				return err
			}
			drifted = digestOf(desired) != digestOf(got.Manifest)
		}
	}
	st.Drifted = drifted
	if !drifted {
		return nil
	}

	workflow.GetLogger(ctx).Info("drift detected, healing", "gvk", cfg.gvk, "id", id)
	healed, err := applyAndWaitReady(ctx, cfg, ec.Spec())
	if err != nil {
		return fmt.Errorf("heal: %w", err)
	}
	healed.Heals = st.Heals + 1
	*st = healed
	return nil
}

func deleteAndWaitGone[T any](ctx workflow.Context, cfg *config[T]) error {
	id := resourceID(ctx, cfg.gvk)
	actx := activityCtx(ctx)
	if err := workflow.ExecuteActivity(actx, deleteActivityName, cfg.gvk, id).Get(ctx, nil); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	for attempt := 0; attempt < cfg.pollAttempts; attempt++ {
		var got getResult
		if err := workflow.ExecuteActivity(actx, getActivityName, cfg.gvk, id).Get(ctx, &got); err != nil {
			return fmt.Errorf("get: %w", err)
		}
		if !got.Found {
			return nil
		}
		if err := workflow.Sleep(ctx, cfg.pollInterval); err != nil {
			return err
		}
	}
	return fmt.Errorf("resource %s/%s still present after delete", cfg.gvk, id)
}
