# temporal-entity

**Manage long-lived resources as durable Temporal workflows.**

`temporal-entity` is a library chassis for the **Entity Lifecycle Pattern**
(aka *Entity Workflow*) on top of the vanilla
[Temporal Go SDK](https://github.com/temporalio/sdk-go): one long-running
workflow per resource, acting as the durable control-plane record for that
resource. You write ordinary Temporal code — activities, child workflows,
timers. The chassis owns the pattern.

```go
h := entclient.Bind(databaseKind, temporalClient, "task-queue")

entclient.ExecWithStart(ctx, h, "db-42", spec, Scale{Replicas: 5}) // create-or-attach + command
st, _ := h.Describe(ctx, "db-42")                                  // durable status, any time
h.Delete(ctx, "db-42")                                             // graceful teardown
```

## Why

If a one-shot workflow is a **verb** ("provision this database"), an entity
workflow is a **noun** ("this database"). The entity workflow *is* the
resource's control-plane record: it holds the desired spec, tracks current
state, accepts commands, serializes side effects, reports status, and heals
drift — durably, across process restarts, for months or years.

Temporal's
[platform control-plane article](https://temporal.io/blog/how-to-build-a-resilient-platform-control-plane-with-temporal)
describes the pattern in detail. Implementing it correctly by hand means
getting all of this right, every time, for every resource type:

| Concern | What goes wrong without it |
|---|---|
| Deterministic workflow IDs | duplicate entities for one resource |
| Command queue + single main loop | racing side effects |
| Update validators | garbage requests polluting Event History |
| Request-id deduplication | a retried command executing twice |
| Continue-as-New discipline | unbounded history → slow replay → forced termination |
| Deletion as a lifecycle transition | orphaned resources, callers hanging forever |
| Saga compensation | half-applied multi-step operations |

This library implements each of those once, as a chassis, and leaves you
exactly the parts that are yours: the resource types, the command handlers,
and the domain knowledge (what "ready" means, what counts as drift).

**No code generation anywhere.** Commands, queries, and resource specs are
your own typed Go structs.

## Install

```bash
go get github.com/graphene-ci/temporal-entity
```

Requires Go ≥ 1.24 (generic type aliases) and any Temporal server the Go
SDK supports.

## Quickstart

### 1. Declare commands as types

A command is a struct: its fields are the parameters, its methods carry the
name, the response type, and (optionally) validation. No descriptor
variables, no name strings anywhere else.

```go
import entity "github.com/graphene-ci/temporal-entity/pkg/entity"

type ScaleRes struct{ OK bool }

type Scale struct{ Replicas int }

func (Scale) Name() entity.CommandName { return "scale" }
func (Scale) Result() ScaleRes         { return ScaleRes{} } // phantom: binds the response type, never called
func (s Scale) Validate() error {                            // optional; runs BEFORE Event History
    if s.Replicas < 1 {
        return errors.New("replicas must be >= 1")
    }
    return nil
}
```

`Result()` is the Go encoding of an associated type: it lets the compiler
infer the response type at every call site, so calls need zero explicit
type parameters.

### 2. Define the kind

A *kind* is one resource type: its desired-spec type, its state type, and
its lifecycle handlers. Handlers contain ordinary Temporal code.

```go
import "github.com/graphene-ci/temporal-entity/pkg/entdefine"

type DBSpec struct{ Tier string; Replicas int }
type DBState struct{ Endpoint string; Replicas int }

db := entdefine.New[DBSpec, DBState]("database",
    // creation: bring the resource to life from its desired spec
    entdefine.WithInit[DBSpec, DBState](func(ctx workflow.Context, spec DBSpec) (DBState, error) {
        var st DBState
        err := workflow.ExecuteActivity(actx(ctx), acts.Provision, spec).Get(ctx, &st)
        return st, err
    }),
    // deletion: tear the resource down after the entity is drained
    entdefine.WithFinalize[DBSpec, DBState](func(ctx workflow.Context, st *DBState) error {
        return workflow.ExecuteActivity(actx(ctx), acts.Teardown, st.Endpoint).Get(ctx, nil)
    }),
    // periodic health/drift tick, serialized with commands
    entdefine.WithReconcileEvery[DBSpec, DBState](30*time.Second,
        func(ctx workflow.Context, ec *entdefine.Ctx[DBSpec, DBState]) error {
            return workflow.ExecuteActivity(actx(ctx), acts.CheckHealth, ec.State().Endpoint).Get(ctx, nil)
        }),
)
```

### 3. Handle commands

Everything is inferred from the handler's signature — the command type
supplies the name and the response type:

```go
entdefine.Handle(db, func(ctx workflow.Context, ec *entdefine.Ctx[DBSpec, DBState], cmd Scale) (ScaleRes, error) {
    if err := workflow.ExecuteActivity(actx(ctx), acts.Resize, cmd.Replicas).Get(ctx, nil); err != nil {
        return ScaleRes{}, err // returned to the CALLER; the entity keeps living
    }
    ec.State().Replicas = cmd.Replicas
    return ScaleRes{OK: true}, nil
})
```

Handlers always run on the entity's main loop — one command at a time, in
order, never concurrently with another command or a reconcile tick. That is
the chassis's core guarantee: **side effects are serialized by
construction**.

### 4. Register with a worker

```go
w := worker.New(c, "task-queue", worker.Options{})
if err := db.Register(w); err != nil {
    log.Fatal(err) // ALL registration problems surface here as one error list — no panics
}
```

### 5. Drive it from anywhere with a Temporal client

```go
import "github.com/graphene-ci/temporal-entity/pkg/entclient"

h := entclient.Bind(db, c, "task-queue")

// create-or-attach + first command, atomically (update-with-start):
res, err := entclient.ExecWithStart(ctx, h, "db-42", DBSpec{Tier: "m", Replicas: 3}, Scale{Replicas: 3})

// tracked command against a running entity; waits for the result:
res, err = entclient.Exec(ctx, h, "db-42", Scale{Replicas: 5})

// built-in status query (works even after the entity completed):
st, err := h.Describe(ctx, "db-42")

// graceful teardown: drain -> finalize -> complete:
err = h.Delete(ctx, "db-42")
```

Workflow IDs are deterministic — `"database/db-42"` — so at most one open
entity exists per resource, and callers reconnect instead of spawning
duplicates.

## Guide

### Queries

Queries are types too, with the same contract shape. Handlers receive a
read-only, **by-value** `Snapshot` — mutating entity state from a query is
a replay-determinism bug, and the snapshot makes it impossible by
construction:

```go
type Health struct{}
type HealthRes struct{ Ready bool }

func (Health) Name() entity.QueryName { return "health" }
func (Health) Result() HealthRes      { return HealthRes{} }

entdefine.HandleQuery(db, func(s entity.Snapshot[DBSpec, DBState], _ Health) (HealthRes, error) {
    return HealthRes{Ready: s.State.Replicas == s.Spec.Replicas}, nil
})

h, err := entclient.Read(ctx, h, "db-42", Health{})
```

### Validation: three layers, all optional, all combined

1. **`Validate() error`** on the command type — request-only checks, no
   type parameters. The common case.
2. **`ValidateWith(s entity.Snapshot[Spec, State]) error`** on the command
   type — checks that need the entity's current state.
3. **Kind-level** via the fluent tail of `Handle` — for validation that
   needs registration-time dependencies a command type cannot capture:

```go
entdefine.Handle(db, applyHandler).Validate(func(s entity.Snapshot[DBSpec, DBState], cmd Apply) error {
    return kindConfig.validateSpec(cmd.Spec)
})
```

All validators run **before anything is committed to Event History**:
rejected requests leave no trace and cost nothing. They must be pure and
deterministic.

### Idempotent retries: request ids

Every command carries an application-level request id (auto-generated by
`Exec`). To make an operation idempotent end-to-end, supply your own and
retry with the same id — the entity serves the cached result instead of
re-executing, **even across Continue-as-New**:

```go
reqID := entity.RequestID(myIdempotencyKey)
res, err := entclient.ExecWithRequestID(ctx, h, "db-42", reqID, Scale{Replicas: 5})
```

### Multi-step operations: the saga helper

For operations that must undo partial progress on failure:

```go
saga := entdefine.NewSaga(ctx).
    Step("resize",
        func(c workflow.Context) error { return exec(c, acts.Resize, n) },
        func(c workflow.Context) error { return exec(c, acts.Resize, prev) }, // compensation
    ).
    Step("update-dns",
        func(c workflow.Context) error { return exec(c, acts.UpdateDNS, n) },
        nil, // no undo needed
    )
if err := saga.Run(); err != nil {
    return Res{}, err // completed steps were compensated in reverse order
}
```

`Run` returns the error to the caller instead of failing the workflow — the
entity keeps living. Forward and compensating actions run as activities and
must be idempotent (safe to retry).

### Child workflows and activities

Nothing is wrapped: inside any handler you use `workflow.ExecuteActivity`,
`workflow.ExecuteChildWorkflow`, timers, selectors — plain Temporal. Rule
of thumb from the pattern: activities for single side effects, entity
command handlers for multi-step serialized operations, child workflows for
independent work that needs its own event history or parallelism.

### The lifecycle

```
creating ──> ready ──(Delete signal)──> deleting ──> deleted
    │                                       │
    └─> create_failed                       └─> delete_failed
```

- **creating**: `WithInit` runs. Commands sent this early queue up.
- **ready**: the main loop serves commands and reconcile ticks.
- **deleting**: pending commands are failed with an explicit error, new
  commands are rejected by the validator layer, in-flight handlers drain,
  then `WithFinalize` runs.
- **deleted**: the workflow completes. `Describe` and queries still work
  (Temporal serves queries on closed workflows).

### Continue-as-New

Temporal workflows must not grow history forever. The chassis handles this
invisibly: when the server suggests it (`GetContinueAsNewSuggested`), the
main loop waits for the queue to empty and all handlers to finish, then
continues-as-new carrying only the compact envelope — phase, spec, state,
deletion flag, pending queue, dedup cache. Callers never notice; the
workflow ID stays the same.

Keep the carried state compact: store large artifacts (manifests, plans,
logs) in external storage and keep references in state.

`entdefine.WithForceCANEveryNCommands(n)` forces the transition every *n*
commands — a **test-only** knob for exercising the path quickly.

### Building a domain library on top

The two type parameters `[Spec, State]` are the chassis's irreducible
minimum, but a domain library can hide them with generic type aliases
(Go ≥ 1.24) when its state type is a function of the resource type:

```go
type Ctx[T any]  = entdefine.Ctx[T, State[T]]
type Snap[T any] = entity.Snapshot[T, State[T]]
```

`examples/reconciler` shows the full shape: a library that manages typed
resources the way a Kubernetes controller would — apply the desired spec,
poll for readiness, heal drift on the reconcile tick, tear down on delete —
with the entity workflow as the control loop. Its users write:

```go
deployments := k8slib.NewKind[Deployment]("apps/v1/Deployment",
    k8slib.WithReady[Deployment](func(live Deployment) bool {
        return live.ReadyReplicas >= live.Replicas
    }),
    k8slib.WithDrifted[Deployment](func(desired, live Deployment) bool {
        return desired.Image != live.Image
    }),
)
```

The user brings the resource type and the two pieces of domain knowledge
generic machinery cannot have: what "ready" means and what counts as drift.

## Package layout

| Package | Role |
|---|---|
| `pkg/entity` | shared vocabulary: id types (`ResourceID`, `RequestID`, ...), the `Command[Res]`/`Query[Res]` contracts, `Phase`, `Snapshot`, `DescribeOut` |
| `pkg/entdefine` | the kind author's side: `Definition`, options, `Handle`/`HandleQuery`, `Ctx`, the main loop, `Saga` |
| `pkg/entclient` | the caller's side: `Bind`, `Exec`, `ExecWithStart`, `ExecWithRequestID`, `Read`, `Describe`, `Delete` |
| `internal/wire` | envelope and update wire shapes, hidden from both sides |

Command/query types reference only `pkg/entity` — declaring commands pulls
in neither the defining nor the calling side.

## Development

```bash
make configure   # install pinned tools into ./bin (nothing global)
make lint        # golangci-lint, must be 0 issues
make test        # unit tests + integration suite on a real dev server
```

The integration suite (`integration/`) starts a Temporal dev server from
`./bin/temporal` (version pinned in the Makefile) and verifies every
property of the pattern end-to-end: update-with-start, validator rejection,
drift healing, out-of-band recreation, request-id dedup, Continue-as-New
with the dedup cache surviving the boundary, a saga command with a child
workflow, and the deletion lifecycle.

## Notes and caveats

- **Never put secrets or PII in resource ids** — they end up in workflow
  IDs and Event History. Pass sensitive payloads via Temporal Data
  Converters.
- Commands are Temporal **Updates** under the hood: the server caps
  in-flight updates (~10 per workflow); `Exec` waits for completion, so
  callers are naturally throttled.
- Query and validator functions must be pure — no side effects, no
  workflow APIs, no blocking.
- Value-type commands: implement the contract methods on value receivers;
  the chassis instantiates zero values to read names at registration.
