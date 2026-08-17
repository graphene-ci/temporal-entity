package entdefine

import (
	"errors"
	"fmt"

	"go.temporal.io/sdk/workflow"
)

// Saga implements the article's compensation pattern for multi-step
// operations inside a command handler: execute steps forward tracking the
// completed ones; on failure, run their compensations in reverse order.
// Run returns the original error instead of failing the whole workflow, so
// the entity keeps living and the command's caller sees the failure.
// Both forward and compensating actions must be idempotent — they run as
// activities and can be retried.
type Saga struct {
	ctx   workflow.Context
	steps []sagaStep
}

type sagaStep struct {
	name       string
	forward    func(ctx workflow.Context) error
	compensate func(ctx workflow.Context) error
}

// NewSaga starts an empty saga bound to the workflow context.
func NewSaga(ctx workflow.Context) *Saga {
	return &Saga{ctx: ctx}
}

// Step adds a forward action with its compensation. compensate may be nil
// for steps that need no undo.
func (s *Saga) Step(name string, forward, compensate func(ctx workflow.Context) error) *Saga {
	s.steps = append(s.steps, sagaStep{name: name, forward: forward, compensate: compensate})
	return s
}

// Run executes all steps in order. On the first failure it compensates the
// already-completed steps in reverse order and returns the step's error
// (with any compensation errors joined in — compensation failures must be
// visible, not swallowed).
func (s *Saga) Run() error {
	logger := workflow.GetLogger(s.ctx)
	var completed []sagaStep
	for _, st := range s.steps {
		if err := st.forward(s.ctx); err != nil {
			errs := []error{fmt.Errorf("saga step %q: %w", st.name, err)}
			for i := len(completed) - 1; i >= 0; i-- {
				c := completed[i]
				if c.compensate == nil {
					continue
				}
				logger.Info("saga compensating", "step", c.name)
				if cErr := c.compensate(s.ctx); cErr != nil {
					errs = append(errs, fmt.Errorf("compensate %q: %w", c.name, cErr))
				}
			}
			return errors.Join(errs...)
		}
		completed = append(completed, st)
	}
	return nil
}
