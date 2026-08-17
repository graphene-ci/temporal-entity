package k8slib

import (
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/workflow"
)

// Workflow-side cluster operations for USER-DEFINED commands registered on
// a Kind (entity.Handle(kind.Def(), ...)): ordinary activity calls, usable
// inside sagas and alongside child workflows.

// ApplyObject upserts an object. Idempotent — safe inside saga steps and
// compensations.
func ApplyObject[T any](ctx workflow.Context, gvk GVK, ref ObjectRef, obj T) error {
	manifest, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("encode %s %v: %w", gvk, ref, err)
	}
	return workflow.ExecuteActivity(activityCtx(ctx), applyActivityName, gvk, ref.ID(), manifest).Get(ctx, nil)
}

// GetObject reads an object's live manifest.
func GetObject[T any](ctx workflow.Context, gvk GVK, ref ObjectRef) (T, bool, error) {
	var obj T
	var got getResult
	if err := workflow.ExecuteActivity(activityCtx(ctx), getActivityName, gvk, ref.ID()).Get(ctx, &got); err != nil {
		return obj, false, err
	}
	if !got.Found {
		return obj, false, nil
	}
	if err := json.Unmarshal(got.Manifest, &obj); err != nil {
		return obj, true, fmt.Errorf("decode %s %v: %w", gvk, ref, err)
	}
	return obj, true, nil
}

// DeleteObject removes an object; not-found is not an error.
func DeleteObject(ctx workflow.Context, gvk GVK, ref ObjectRef) error {
	return workflow.ExecuteActivity(activityCtx(ctx), deleteActivityName, gvk, ref.ID()).Get(ctx, nil)
}
