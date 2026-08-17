package entdefine

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestSagaCompensatesInReverseOrder(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	var trace []string
	wf := func(ctx workflow.Context) ([]string, error) {
		s := NewSaga(ctx).
			Step("a",
				func(workflow.Context) error { trace = append(trace, "a"); return nil },
				func(workflow.Context) error { trace = append(trace, "undo-a"); return nil },
			).
			Step("b",
				func(workflow.Context) error { trace = append(trace, "b"); return nil },
				func(workflow.Context) error { trace = append(trace, "undo-b"); return nil },
			).
			Step("c",
				func(workflow.Context) error { return errors.New("boom") },
				func(workflow.Context) error { trace = append(trace, "undo-c"); return nil },
			)
		// Run returns the error to the CALLER (in real use — the command
		// handler, whose error the chassis records as the command result);
		// the workflow itself keeps living.
		if err := s.Run(); err == nil {
			return nil, errors.New("expected saga error")
		}
		return trace, nil
	}
	env.RegisterWorkflow(wf)
	env.ExecuteWorkflow(wf)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var got []string
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Equal(t, []string{"a", "b", "undo-b", "undo-a"}, got)
}

func TestSagaSuccessRunsNoCompensation(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	var trace []string
	wf := func(ctx workflow.Context) ([]string, error) {
		err := NewSaga(ctx).
			Step("a",
				func(workflow.Context) error { trace = append(trace, "a"); return nil },
				func(workflow.Context) error { trace = append(trace, "undo-a"); return nil },
			).
			Step("b",
				func(workflow.Context) error { trace = append(trace, "b"); return nil },
				nil,
			).Run()
		return trace, err
	}
	env.RegisterWorkflow(wf)
	env.ExecuteWorkflow(wf)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var got []string
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Equal(t, []string{"a", "b"}, got)
}
