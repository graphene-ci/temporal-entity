package k8slib

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
)

// Cluster is the side-effect boundary: everything that touches the actual
// cluster goes through it. Implementations MUST be idempotent — every call
// can be retried by Temporal (Apply is an upsert, Delete tolerates
// not-found), matching the article's idempotency requirement for
// activities that talk to external APIs.
type Cluster interface {
	Apply(ctx context.Context, gvk, id string, manifest []byte) error
	Get(ctx context.Context, gvk, id string) (manifest []byte, found bool, err error)
	Delete(ctx context.Context, gvk, id string) error
}

// Activity names are stable strings so entity workflows reference them
// without importing the implementation.
const (
	applyActivityName  = "k8s.apply"
	getActivityName    = "k8s.get"
	deleteActivityName = "k8s.delete"
)

type activities struct {
	cluster Cluster
}

func (a *activities) Apply(ctx context.Context, gvk, id string, manifest []byte) error {
	return a.cluster.Apply(ctx, gvk, id, manifest)
}

type getResult struct {
	Manifest []byte `json:"manifest,omitempty"`
	Found    bool   `json:"found"`
}

func (a *activities) Get(ctx context.Context, gvk, id string) (getResult, error) {
	m, found, err := a.cluster.Get(ctx, gvk, id)
	return getResult{Manifest: m, Found: found}, err
}

func (a *activities) Delete(ctx context.Context, gvk, id string) error {
	return a.cluster.Delete(ctx, gvk, id)
}

// RegisterActivities wires a Cluster implementation into a worker. Called
// once from the composition root.
func RegisterActivities(w worker.ActivityRegistry, c Cluster) {
	a := &activities{cluster: c}
	w.RegisterActivityWithOptions(a.Apply, activity.RegisterOptions{Name: applyActivityName})
	w.RegisterActivityWithOptions(a.Get, activity.RegisterOptions{Name: getActivityName})
	w.RegisterActivityWithOptions(a.Delete, activity.RegisterOptions{Name: deleteActivityName})
}
