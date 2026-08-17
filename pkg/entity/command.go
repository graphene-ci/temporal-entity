// Package entity is the shared vocabulary of the Entity Lifecycle
// Pattern chassis: identifier types, the Command/Query contracts, and
// the read-only state views seen by both the defining and calling sides.
package entity

// A command is a TYPE, not a value-descriptor: the request struct itself
// carries the command's name, parameters (its fields), response type, and
// optionally its validation. Nothing else exists — no registries of vars,
// no name strings at call sites.
//
//	type Scale struct{ Replicas int }
//
//	func (Scale) Name() entity.CommandName { return "scale" }
//	func (Scale) Result() ScaleRes         { return ScaleRes{} }
//
//	// optional, colocated with the command:
//	func (s Scale) Validate(snap entity.Snapshot[Spec, State]) error { ... }
//
// Result is phantom — never called at runtime; it exists only so the
// compiler can bind Res to the request type (Go's encoding of associated
// types) and infer everything at call sites:
//
//	res, err := entity.Exec(ctx, cl, id, Scale{Replicas: 5})

// CommandName names a command within a kind.
type CommandName string

// QueryName names a query within a kind.
type QueryName string

// Command is the contract of a tracked mutation (a Temporal Update):
// implemented by the user's request type.
type Command[Res any] interface {
	Name() CommandName
	Result() Res
}

// Query is the contract of a read-only projection: implemented by the
// user's request type.
type Query[Res any] interface {
	Name() QueryName
	Result() Res
}

// SelfValidator is the optional method a command type may implement to
// reject requests before they reach Event History based on the request
// alone — the common case, and it needs no type parameters:
//
//	func (b BlueGreen) Validate() error { ... }
//
// Must be pure and deterministic.
type SelfValidator interface {
	Validate() error
}

// Validator is the state-aware variant: for validations that need the
// entity's current Snapshot. A command type may implement either or both;
// Validate runs first, then ValidateWith.
type Validator[Spec, State any] interface {
	ValidateWith(s Snapshot[Spec, State]) error
}

// CommandInfo describes a registered command for introspection — the seed
// of a future manifest (contracts enumerable from the same
// table that dispatches).
type CommandInfo struct {
	Name    CommandName
	ReqType string
	ResType string
}

// QueryInfo describes a registered query for introspection.
type QueryInfo struct {
	Name    QueryName
	ReqType string
	ResType string
}
