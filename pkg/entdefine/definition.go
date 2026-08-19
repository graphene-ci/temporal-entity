// Package entdefine is the defining side of the Temporal Entity Lifecycle Pattern chassis
// (aka Entity Workflow): one long-running workflow per resource, acting as
// the durable control-plane record for it. Users declare typed command and
// query descriptors and register handlers containing ordinary Temporal
// code (activities, child workflows, timers); the chassis owns everything
// the pattern requires — the serialized command queue and main loop,
// update validators, request-id deduplication, Continue-as-New, deletion
// as a lifecycle transition, periodic reconcile ticks, queries, and search
// attributes.
package entdefine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/temporal-entity/internal/wire"
	"github.com/graphene-ci/temporal-entity/pkg/entity"
)

// Search attribute keys (article: "use resource-oriented Workflow IDs and
// Search Attributes for queryability"). Registered on the cluster by run.sh.
var (
	SearchAttrKind  = temporal.NewSearchAttributeKeyKeyword("EntityKind")
	SearchAttrPhase = temporal.NewSearchAttributeKeyKeyword("EntityPhase")
	// SearchAttrLabels mirrors the entity's labels as "k=v" keywords, so
	// visibility queries can select by label: EntityLabels IN ("env=prod").
	SearchAttrLabels = temporal.NewSearchAttributeKeyKeywordList("EntityLabels")
)

// Ctx is what command/reconcile handlers get: access to the entity's
// desired Spec and current State. Handlers run on the main loop, so all
// mutations are serialized by construction.
type Ctx[Spec, State any] struct {
	env         *wire.Envelope[Spec, State]
	labelsDirty bool
}

// Spec returns the current desired spec.
func (c *Ctx[Spec, State]) Spec() Spec { return c.env.Spec }

// SetSpec replaces the desired spec.
func (c *Ctx[Spec, State]) SetSpec(s Spec) { c.env.Spec = s }

// State returns the mutable user state.
func (c *Ctx[Spec, State]) State() *State { return &c.env.State }

// Phase returns the entity lifecycle phase.
func (c *Ctx[Spec, State]) Phase() entity.Phase { return c.env.Phase }

// Labels returns a copy of the entity's labels.
func (c *Ctx[Spec, State]) Labels() map[string]string {
	out := make(map[string]string, len(c.env.Labels))
	for k, v := range c.env.Labels {
		out[k] = v
	}
	return out
}

// SetLabel sets (or, with an empty value, removes) one label from a
// command/reconcile handler. The search-attribute mirror follows on the
// main loop.
func (c *Ctx[Spec, State]) SetLabel(key, value string) {
	if c.env.MergeLabels(map[string]string{key: value}) {
		c.labelsDirty = true
	}
}

// commandImpl is the wire-level form of a registered command: closures
// decode the JSON payload into the descriptor's Req type and dispatch to
// the typed handler.
type commandImpl[Spec, State any] struct {
	info     entity.CommandInfo
	exec     func(ctx workflow.Context, ec *Ctx[Spec, State], payload json.RawMessage) (json.RawMessage, error)
	validate func(s entity.Snapshot[Spec, State], payload json.RawMessage) error
}

type queryImpl[Spec, State any] struct {
	info entity.QueryInfo
	exec func(s entity.Snapshot[Spec, State], payload json.RawMessage) (json.RawMessage, error)
}

// Definition describes one entity kind: its lifecycle handlers plus the
// registry of declared commands and queries. Mutable only during
// registration (composition root); frozen once Register is called.
// Registration problems accumulate as an error list and fail Register —
// they never panic (the protobuf-go registration lesson).
type Definition[Spec, State any] struct {
	kind           entity.KindName
	init           func(ctx workflow.Context, spec Spec) (State, error)
	finalize       func(ctx workflow.Context, state *State) error
	reconcile      func(ctx workflow.Context, ec *Ctx[Spec, State]) error
	reconcileEvery time.Duration
	commands       map[entity.CommandName]*commandImpl[Spec, State]
	queries        map[entity.QueryName]*queryImpl[Spec, State]
	useSearchAttrs bool

	// forceCANEvery is a test-only knob: force Continue-as-New after N
	// processed commands so the CAN path is exercisable without generating
	// thousands of history events. Production trigger is always
	// GetContinueAsNewSuggested().
	forceCANEvery int

	errs []error
}

// Option configures a Definition at declaration time.
type Option[Spec, State any] func(*Definition[Spec, State])

// WithInit sets the creation handler: bring the resource to life from its
// desired Spec (ordinary Temporal code inside).
func WithInit[Spec, State any](fn func(ctx workflow.Context, spec Spec) (State, error)) Option[Spec, State] {
	return func(d *Definition[Spec, State]) { d.init = fn }
}

// WithFinalize sets the deletion handler, run after the entity is marked
// for deletion and its queue is drained.
func WithFinalize[Spec, State any](fn func(ctx workflow.Context, state *State) error) Option[Spec, State] {
	return func(d *Definition[Spec, State]) { d.finalize = fn }
}

// WithReconcileEvery installs the periodic health/drift tick: when the main
// loop's wait times out (article: timer expiration in the main loop), fn
// runs — serialized with commands like everything else.
func WithReconcileEvery[Spec, State any](every time.Duration, fn func(ctx workflow.Context, ec *Ctx[Spec, State]) error) Option[Spec, State] {
	return func(d *Definition[Spec, State]) { d.reconcileEvery = every; d.reconcile = fn }
}

// WithSearchAttributes toggles upserting EntityKind/EntityPhase typed
// search attributes (they must exist on the cluster).
func WithSearchAttributes[Spec, State any](on bool) Option[Spec, State] {
	return func(d *Definition[Spec, State]) { d.useSearchAttrs = on }
}

// WithForceCANEveryNCommands is test-only; see forceCANEvery.
func WithForceCANEveryNCommands[Spec, State any](n int) Option[Spec, State] {
	return func(d *Definition[Spec, State]) { d.forceCANEvery = n }
}

// New declares an entity kind.
func New[Spec, State any](kind entity.KindName, opts ...Option[Spec, State]) *Definition[Spec, State] {
	d := &Definition[Spec, State]{
		kind:     kind,
		commands: map[entity.CommandName]*commandImpl[Spec, State]{},
		queries:  map[entity.QueryName]*queryImpl[Spec, State]{},
	}
	if err := kind.Validate(); err != nil {
		d.errs = append(d.errs, fmt.Errorf("entity kind: %w", err))
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Kind returns the entity kind name.
func (d *Definition[Spec, State]) Kind() entity.KindName { return d.kind }

// Commands enumerates the registered commands — introspection for a future
// manifest.
func (d *Definition[Spec, State]) Commands() []entity.CommandInfo {
	out := make([]entity.CommandInfo, 0, len(d.commands))
	for _, c := range d.commands {
		out = append(out, c.info)
	}
	return out
}

// Queries enumerates the registered queries.
func (d *Definition[Spec, State]) Queries() []entity.QueryInfo {
	out := make([]entity.QueryInfo, 0, len(d.queries))
	for _, q := range d.queries {
		out = append(out, q.info)
	}
	return out
}

// CommandRegistration is the fluent tail of Handle, for attaching a
// kind-level validator (one that needs registration-time dependencies a
// command type's own Validate method cannot capture — e.g. k8slib's
// per-kind validation config).
type CommandRegistration[Spec, State, Req any] struct {
	impl *commandImpl[Spec, State]
	dead bool
}

// Validate attaches a kind-level update validator. It runs in addition to
// the command type's own Validate method (if any); both see a read-only
// Snapshot and must be pure and deterministic.
func (r *CommandRegistration[Spec, State, Req]) Validate(fn func(s entity.Snapshot[Spec, State], req Req) error) *CommandRegistration[Spec, State, Req] {
	if r.dead {
		return r
	}
	prev := r.impl.validate
	r.impl.validate = func(s entity.Snapshot[Spec, State], payload json.RawMessage) error {
		if err := prev(s, payload); err != nil {
			return err
		}
		var req Req
		if err := json.Unmarshal(payload, &req); err != nil {
			return fmt.Errorf("decode %s request: %w", r.impl.info.Name, err)
		}
		return fn(s, req)
	}
	return r
}

// Handle registers a command handler. The command is the REQUEST TYPE
// itself (implementing entity.Command[Res]): its name, parameters, response type,
// and optional Validate method all live on that one type, and everything
// is inferred from the handler's signature — no descriptors, no strings.
// Handlers run on the entity's main loop — never concurrently with another
// command — and contain ordinary Temporal code.
func Handle[Spec, State, Res any, Req entity.Command[Res]](
	d *Definition[Spec, State],
	fn func(ctx workflow.Context, ec *Ctx[Spec, State], req Req) (Res, error),
) *CommandRegistration[Spec, State, Req] {
	var zero Req
	name := zero.Name()
	if name == "" || string(name) == wire.DeleteSignalName || string(name) == wire.DescribeQueryName || string(name) == wire.SetLabelsCommandName {
		d.errs = append(d.errs, fmt.Errorf("entity %q: reserved or empty command name %q (%s)", d.kind, name, typeName[Req]()))
		return &CommandRegistration[Spec, State, Req]{dead: true, impl: &commandImpl[Spec, State]{}}
	}
	if _, dup := d.commands[name]; dup {
		d.errs = append(d.errs, fmt.Errorf("entity %q: duplicate command %q (%s)", d.kind, name, typeName[Req]()))
		return &CommandRegistration[Spec, State, Req]{dead: true, impl: &commandImpl[Spec, State]{}}
	}
	impl := &commandImpl[Spec, State]{
		info: entity.CommandInfo{
			Name:    name,
			ReqType: typeName[Req](),
			ResType: typeName[Res](),
		},
		exec: func(ctx workflow.Context, ec *Ctx[Spec, State], payload json.RawMessage) (json.RawMessage, error) {
			var req Req
			if err := json.Unmarshal(payload, &req); err != nil {
				return nil, fmt.Errorf("decode %s request: %w", name, err)
			}
			res, err := fn(ctx, ec, req)
			if err != nil {
				return nil, err
			}
			out, mErr := json.Marshal(res)
			if mErr != nil {
				return nil, fmt.Errorf("encode %s result: %w", name, mErr)
			}
			return out, nil
		},
		// Base validator: the command type's own validation methods, when
		// implemented — Validate() for request-only checks (no generics),
		// then ValidateWith(Snapshot) for state-aware ones. Kind-level
		// validators chain on top via the fluent Validate below.
		validate: func(s entity.Snapshot[Spec, State], payload json.RawMessage) error {
			var req Req
			if err := json.Unmarshal(payload, &req); err != nil {
				return fmt.Errorf("decode %s request: %w", name, err)
			}
			if v, ok := any(req).(entity.SelfValidator); ok {
				if err := v.Validate(); err != nil {
					return err
				}
			}
			if v, ok := any(req).(entity.Validator[Spec, State]); ok {
				return v.ValidateWith(s)
			}
			return nil
		},
	}
	d.commands[name] = impl
	return &CommandRegistration[Spec, State, Req]{impl: impl}
}

// HandleQuery registers a query handler; the query is the request type
// itself (implementing entity.Query[Res]). Query handlers see a read-only
// Snapshot and MUST be pure: no side effects, no workflow APIs, no
// blocking — Temporal query semantics.
func HandleQuery[Spec, State, Res any, Req entity.Query[Res]](
	d *Definition[Spec, State],
	fn func(s entity.Snapshot[Spec, State], req Req) (Res, error),
) {
	var zero Req
	name := zero.Name()
	if name == "" || string(name) == wire.DescribeQueryName {
		d.errs = append(d.errs, fmt.Errorf("entity %q: reserved or empty query name %q (%s)", d.kind, name, typeName[Req]()))
		return
	}
	if _, dup := d.queries[name]; dup {
		d.errs = append(d.errs, fmt.Errorf("entity %q: duplicate query %q (%s)", d.kind, name, typeName[Req]()))
		return
	}
	d.queries[name] = &queryImpl[Spec, State]{
		info: entity.QueryInfo{
			Name:    name,
			ReqType: typeName[Req](),
			ResType: typeName[Res](),
		},
		exec: func(s entity.Snapshot[Spec, State], payload json.RawMessage) (json.RawMessage, error) {
			var req Req
			if err := json.Unmarshal(payload, &req); err != nil {
				return nil, fmt.Errorf("decode %s request: %w", name, err)
			}
			res, err := fn(s, req)
			if err != nil {
				return nil, err
			}
			out, mErr := json.Marshal(res)
			if mErr != nil {
				return nil, fmt.Errorf("encode %s result: %w", name, mErr)
			}
			return out, nil
		},
	}
}

// Register attaches the entity workflow to a worker. All accumulated
// registration errors surface here as one list.
func (d *Definition[Spec, State]) Register(w worker.WorkflowRegistry) error {
	if len(d.errs) > 0 {
		return errors.Join(d.errs...)
	}
	w.RegisterWorkflowWithOptions(d.workflowFn, workflow.RegisterOptions{Name: string(d.kind)})
	return nil
}

func typeName[T any]() string {
	var zero T
	return fmt.Sprintf("%T", zero)
}
