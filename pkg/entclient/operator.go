// Operator surface: address ANY entity by its wire identity — workflow
// id, command name, raw JSON — without the Go definition in hand. This
// is what a control plane needs to drive entities its users defined:
// the typed surface (Exec/Read) stays the right tool when the
// definition is compiled in.
package entclient

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/graphene-ci/temporal-entity/internal/wire"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
)

// ExecRaw sends a command to a running entity by wire identity and
// returns the raw JSON result. The update's validators still run; the
// request id deduplicates across retries and Continue-as-New.
func ExecRaw(ctx context.Context, c client.Client, workflowID, command string, payload json.RawMessage, requestID string) (json.RawMessage, error) {
	if workflowID == "" || command == "" {
		return nil, fmt.Errorf("workflow id and command are required")
	}
	if requestID == "" {
		requestID = uuid.NewString()
	}
	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		UpdateName:   command,
		Args:         []any{wire.UpdateArgs{RequestID: entity.RequestID(requestID), Payload: payload}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, err
	}
	var out json.RawMessage
	if err := handle.Get(ctx, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ReadRaw runs a query against an entity by wire identity. Works even
// after the entity completed.
func ReadRaw(ctx context.Context, c client.Client, workflowID, query string, payload json.RawMessage) (json.RawMessage, error) {
	if workflowID == "" || query == "" {
		return nil, fmt.Errorf("workflow id and query are required")
	}
	val, err := c.QueryWorkflow(ctx, workflowID, "", query, payload)
	if err != nil {
		return nil, err
	}
	var out json.RawMessage
	if err := val.Get(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// DescribeRaw reads the built-in describe of any entity: phase, spec,
// state — as raw JSON.
func DescribeRaw(ctx context.Context, c client.Client, workflowID string) (json.RawMessage, error) {
	val, err := c.QueryWorkflow(ctx, workflowID, "", entity.DescribeQueryName)
	if err != nil {
		return nil, err
	}
	var out json.RawMessage
	if err := val.Get(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// SetLabelsRaw patches any entity's labels by wire identity: empty
// values delete keys. The chassis serves this on every entity.
func SetLabelsRaw(ctx context.Context, c client.Client, workflowID string, patch map[string]string) error {
	payload, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("encode label patch: %w", err)
	}
	_, err = ExecRaw(ctx, c, workflowID, entity.SetLabelsCommandName, payload, "")
	return err
}
