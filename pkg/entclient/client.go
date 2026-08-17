// Package entclient is the CALLER side of an entity kind: bind a
// definition to a Temporal client, then drive the entity — Exec commands,
// Read queries, Describe, Delete. Command/query values are the request
// types themselves (entity.Command / entity.Query); call sites carry no
// strings and no explicit type parameters.
package entclient

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/temporal-entity/internal/wire"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	"github.com/graphene-ci/temporal-entity/pkg/entity"
)

// Client is the caller-side handle for one entity kind. Workflow IDs are
// deterministic — "kind/{resource-id}" — so at most one open entity exists
// per resource and callers reconnect instead of creating duplicates.
type Client[Spec, State any] struct {
	d         *entdefine.Definition[Spec, State]
	c         client.Client
	taskQueue string
}

// Bind creates a client handle for a definition.
func Bind[Spec, State any](d *entdefine.Definition[Spec, State], c client.Client, taskQueue string) *Client[Spec, State] {
	return &Client[Spec, State]{d: d, c: c, taskQueue: taskQueue}
}

// WorkflowID derives the deterministic workflow ID for a resource. Never
// put secrets or PII in resource ids — they end up in workflow IDs and
// Event History.
func (cl *Client[Spec, State]) WorkflowID(id entity.ResourceID) string {
	return string(cl.d.Kind()) + "/" + string(id)
}

func (cl *Client[Spec, State]) startOptions(id entity.ResourceID) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:        cl.WorkflowID(id),
		TaskQueue: cl.taskQueue,
		// Reconnect-not-duplicate: if the entity is already running, attach
		// to it instead of failing or spawning a second one.
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}
}

// CreateOrAttach starts the entity for id with the given desired spec, or
// attaches to the already-running one.
func (cl *Client[Spec, State]) CreateOrAttach(ctx context.Context, id entity.ResourceID, spec Spec) (client.WorkflowRun, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	env := &wire.Envelope[Spec, State]{Spec: spec}
	return cl.c.ExecuteWorkflow(ctx, cl.startOptions(id), string(cl.d.Kind()), env)
}

// Describe queries the entity's current status. Non-blocking; works even
// after the entity workflow has completed.
func (cl *Client[Spec, State]) Describe(ctx context.Context, id entity.ResourceID) (entity.DescribeOut[Spec, State], error) {
	var out entity.DescribeOut[Spec, State]
	if err := id.Validate(); err != nil {
		return out, err
	}
	val, err := cl.c.QueryWorkflow(ctx, cl.WorkflowID(id), "", wire.DescribeQueryName)
	if err != nil {
		return out, err
	}
	if err := val.Get(&out); err != nil {
		return out, err
	}
	return out, nil
}

// Delete signals the entity to tear itself down: fire-and-forget lifecycle
// event. The entity drains pending work, runs its finalizer, and completes.
func (cl *Client[Spec, State]) Delete(ctx context.Context, id entity.ResourceID) error {
	if err := id.Validate(); err != nil {
		return err
	}
	return cl.c.SignalWorkflow(ctx, cl.WorkflowID(id), "", wire.DeleteSignalName, nil)
}

// Exec sends a command to a running entity as a Temporal Update and waits
// for its result. The command value IS the request: its type carries the
// name and the response type, so the call site has no strings and no
// explicit type parameters. A fresh application-level request id is
// generated; to retry an operation idempotently, use ExecWithRequestID
// with the same id — the entity dedups it even across Continue-as-New.
func Exec[Spec, State, Res any, Req entity.Command[Res]](ctx context.Context, cl *Client[Spec, State], id entity.ResourceID, req Req) (Res, error) {
	return ExecWithRequestID(ctx, cl, id, entity.RequestID(uuid.NewString()), req)
}

// ExecWithRequestID is Exec with an explicit request id for deduplication.
func ExecWithRequestID[Spec, State, Res any, Req entity.Command[Res]](ctx context.Context, cl *Client[Spec, State], id entity.ResourceID, requestID entity.RequestID, req Req) (Res, error) {
	var res Res
	if err := id.Validate(); err != nil {
		return res, err
	}
	if err := requestID.Validate(); err != nil {
		return res, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return res, fmt.Errorf("encode %s request: %w", req.Name(), err)
	}
	handle, err := cl.c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   cl.WorkflowID(id),
		UpdateName:   string(req.Name()),
		Args:         []any{wire.UpdateArgs{RequestID: requestID, Payload: payload}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return res, err
	}
	return decodeRaw[Res](func(out *json.RawMessage) error { return handle.Get(ctx, out) })
}

// ExecWithStart atomically starts the entity if absent (with the given
// desired spec) AND executes the command — the article's update-with-start:
// one round trip, no create/attach race.
func ExecWithStart[Spec, State, Res any, Req entity.Command[Res]](ctx context.Context, cl *Client[Spec, State], id entity.ResourceID, spec Spec, req Req) (Res, error) {
	var res Res
	if err := id.Validate(); err != nil {
		return res, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return res, fmt.Errorf("encode %s request: %w", req.Name(), err)
	}
	env := &wire.Envelope[Spec, State]{Spec: spec}
	startOp := cl.c.NewWithStartWorkflowOperation(cl.startOptions(id), string(cl.d.Kind()), env)
	handle, err := cl.c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateName:   string(req.Name()),
			Args:         []any{wire.UpdateArgs{RequestID: entity.RequestID(uuid.NewString()), Payload: payload}},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return res, err
	}
	return decodeRaw[Res](func(out *json.RawMessage) error { return handle.Get(ctx, out) })
}

// Read runs a read-only query against the entity; the query value is the
// request, typed like commands. Works even after the entity completed.
func Read[Spec, State, Res any, Req entity.Query[Res]](ctx context.Context, cl *Client[Spec, State], id entity.ResourceID, req Req) (Res, error) {
	var res Res
	if err := id.Validate(); err != nil {
		return res, err
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return res, fmt.Errorf("encode %s request: %w", req.Name(), err)
	}
	val, err := cl.c.QueryWorkflow(ctx, cl.WorkflowID(id), "", string(req.Name()), payload)
	if err != nil {
		return res, err
	}
	return decodeRaw[Res](func(out *json.RawMessage) error { return val.Get(out) })
}

func decodeRaw[Res any](get func(*json.RawMessage) error) (Res, error) {
	var res Res
	var raw json.RawMessage
	if err := get(&raw); err != nil {
		return res, err
	}
	if len(raw) == 0 {
		return res, nil
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return res, fmt.Errorf("decode result: %w", err)
	}
	return res, nil
}
