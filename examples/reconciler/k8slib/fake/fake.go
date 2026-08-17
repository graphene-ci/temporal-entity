// Package fake is an in-memory Cluster for verifying the entity chassis
// without a real Kubernetes. It converges gradually (a Converge hook runs
// on every Get, simulating an async cluster) and supports drift injection.
package fake

import (
	"context"
	"sync"
)

type object struct {
	live []byte
}

// Cluster is an in-memory, idempotent implementation of k8slib.Cluster.
// Apply is an upsert; Delete tolerates not-found — safe to retry, as the
// pattern requires.
type Cluster struct {
	mu      sync.Mutex
	objects map[string]*object

	// Converge, if set, is invoked on every Get with the current live
	// manifest and returns the next observed one — simulating a cluster
	// that reaches readiness asynchronously.
	Converge func(live []byte) []byte

	failApply  map[string]error
	failDelete map[string]error
}

// New creates an empty in-memory cluster.
func New() *Cluster {
	return &Cluster{objects: map[string]*object{}}
}

func key(gvk, id string) string { return gvk + "|" + id }

// FailApplyWith makes subsequent Apply calls for the object return err
// (nil clears). Error injection for tests.
func (c *Cluster) FailApplyWith(gvk, id string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failApply == nil {
		c.failApply = map[string]error{}
	}
	c.failApply[key(gvk, id)] = err
}

// FailDeleteWith makes subsequent Delete calls for the object return err
// (nil clears). Error injection for tests.
func (c *Cluster) FailDeleteWith(gvk, id string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failDelete == nil {
		c.failDelete = map[string]error{}
	}
	c.failDelete[key(gvk, id)] = err
}

// Apply upserts the object (idempotent).
func (c *Cluster) Apply(_ context.Context, gvk, id string, manifest []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.failApply[key(gvk, id)]; err != nil {
		return err
	}
	c.objects[key(gvk, id)] = &object{live: append([]byte(nil), manifest...)}
	return nil
}

// Get returns the live manifest, running the Converge hook if set.
func (c *Cluster) Get(_ context.Context, gvk, id string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o, ok := c.objects[key(gvk, id)]
	if !ok {
		return nil, false, nil
	}
	if c.Converge != nil {
		o.live = c.Converge(o.live)
	}
	return append([]byte(nil), o.live...), true, nil
}

// Delete removes the object; not-found is not an error.
func (c *Cluster) Delete(_ context.Context, gvk, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.failDelete[key(gvk, id)]; err != nil {
		return err
	}
	delete(c.objects, key(gvk, id))
	return nil
}

// Corrupt overwrites the live manifest of an object — drift injection for
// tests and demos.
func (c *Cluster) Corrupt(gvk, id string, manifest []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if o, ok := c.objects[key(gvk, id)]; ok {
		o.live = append([]byte(nil), manifest...)
	}
}

// Vanish removes an object out-of-band (someone kubectl-deleted it).
func (c *Cluster) Vanish(gvk, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.objects, key(gvk, id))
}
