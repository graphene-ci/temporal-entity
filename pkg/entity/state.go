package entity

// Phase is the lifecycle phase of an entity, per the Entity Lifecycle
// Pattern: creation -> ready -> (deleting -> deleted | delete_failed).
type Phase string

// Lifecycle phases.
const (
	PhaseCreating     Phase = "creating"
	PhaseReady        Phase = "ready"
	PhaseDeleting     Phase = "deleting"
	PhaseDeleted      Phase = "deleted"
	PhaseDeleteFailed Phase = "delete_failed"
	PhaseCreateFailed Phase = "create_failed"
)

// Snapshot is a read-only, by-value view of the entity handed to query
// handlers and command validators. Both MUST be pure: a query or validator
// that mutates state is a replay-determinism bug, so they never see the
// live state — only this copy.
type Snapshot[Spec, State any] struct {
	Phase             Phase
	Spec              Spec
	State             State
	PendingCommands   int
	MarkedForDeletion bool
}

// DescribeOut is the answer to the built-in "describe" query: current
// status plus pending work, queryable at any time without blocking.
type DescribeOut[Spec, State any] struct {
	Phase             Phase  `json:"phase"`
	Spec              Spec   `json:"spec"`
	State             State  `json:"state"`
	PendingCommands   int    `json:"pendingCommands"`
	MarkedForDeletion bool   `json:"markedForDeletion"`
	RunID             string `json:"runId"`
}

// Wire names of the entity protocol: the lifecycle signal and the
// describe query every entity serves. Exported so that OPERATORS — a
// control plane driving entities it did not define in Go — can address
// any entity generically; typed access still goes through entclient.
const (
	// DeleteSignalName marks an entity for deletion: drain, finalize,
	// complete.
	DeleteSignalName = "entity-delete"
	// DescribeQueryName answers DescribeOut at any time, even after the
	// workflow closed.
	DescribeQueryName = "describe"
)
