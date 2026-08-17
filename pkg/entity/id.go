package entity

import (
	"fmt"
	"strings"
)

// Identifier dictionary: distinct named
// types for every identifier the library owns; literal casts for constants
// in code, Parse* for values arriving from the outside world. Invalid
// literals surface as errors at the point of use (Register / client call),
// never as panics.
//
// Identifiers end up in Workflow IDs and Event History — never put secrets
// or PII in them.

// KindName names an entity kind; it becomes the workflow type name and the
// workflow ID prefix.
type KindName string

// Validate reports whether the kind name is well-formed.
func (k KindName) Validate() error {
	if k == "" {
		return fmt.Errorf("empty kind name")
	}
	return nil
}

// ResourceID identifies one resource within a kind. Workflow ID =
// "{kind}/{resource-id}".
type ResourceID string

// Validate reports whether the resource id is well-formed.
func (r ResourceID) Validate() error {
	if r == "" {
		return fmt.Errorf("empty resource id")
	}
	if strings.HasPrefix(string(r), "/") || strings.HasSuffix(string(r), "/") {
		return fmt.Errorf("resource id %q must not start or end with '/'", r)
	}
	return nil
}

// ParseResourceID validates a resource id from external input.
func ParseResourceID(s string) (ResourceID, error) {
	r := ResourceID(s)
	if err := r.Validate(); err != nil {
		return "", err
	}
	return r, nil
}

// RequestID is the application-level idempotency key of one command
// execution; the entity dedups it even across Continue-as-New.
type RequestID string

// Validate reports whether the request id is well-formed.
func (r RequestID) Validate() error {
	if r == "" {
		return fmt.Errorf("empty request id")
	}
	return nil
}

// ParseRequestID validates a request id from external input.
func ParseRequestID(s string) (RequestID, error) {
	r := RequestID(s)
	if err := r.Validate(); err != nil {
		return "", err
	}
	return r, nil
}
