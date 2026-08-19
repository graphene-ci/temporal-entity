package entdefine

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"go.temporal.io/sdk/workflow"

	"github.com/graphene-ci/temporal-entity/internal/wire"
	"github.com/graphene-ci/temporal-entity/pkg/entity"
)

// workflowFn is the entity workflow: the chassis main loop. It is the ONLY
// place side effects execute; message handlers just enqueue commands and
// wait. Continue-as-New happens only here (article: "implement from main
// Workflow method, not handlers").
func (d *Definition[Spec, State]) workflowFn(ctx workflow.Context, env *wire.Envelope[Spec, State]) error {
	if env == nil {
		return errors.New("entity: nil envelope input")
	}
	ec := &Ctx[Spec, State]{env: env}
	info := workflow.GetInfo(ctx)
	logger := workflow.GetLogger(ctx)

	// --- queries: non-blocking read of current status, callable anytime ---
	err := workflow.SetQueryHandler(ctx, wire.DescribeQueryName, func() (entity.DescribeOut[Spec, State], error) {
		return entity.DescribeOut[Spec, State]{
			Phase:             env.Phase,
			Spec:              env.Spec,
			State:             env.State,
			Labels:            env.Labels,
			PendingCommands:   len(env.Pending),
			MarkedForDeletion: env.MarkedForDeletion,
			RunID:             info.WorkflowExecution.RunID,
		}, nil
	})
	if err != nil {
		return fmt.Errorf("register describe query: %w", err)
	}

	// --- user-declared queries: pure read-only projections over a
	// by-value Snapshot, callable anytime (even after completion). ---
	for qname := range d.queries {
		q := d.queries[qname]
		err := workflow.SetQueryHandler(ctx, string(qname), func(payload json.RawMessage) (json.RawMessage, error) {
			return q.exec(env.Snapshot(), payload)
		})
		if err != nil {
			return fmt.Errorf("register query %q: %w", qname, err)
		}
	}

	// --- delete signal: fire-and-forget lifecycle event, no response ---
	deleteCh := workflow.GetSignalChannel(ctx, wire.DeleteSignalName)
	workflow.Go(ctx, func(gctx workflow.Context) {
		for {
			deleteCh.Receive(gctx, nil)
			env.MarkedForDeletion = true
		}
	})

	// --- built-in set-labels: a label patch is a tracked mutation like
	// any command — queued, serialized, deduplicated — but the chassis
	// owns it so EVERY entity has it without declaring anything.
	err = workflow.SetUpdateHandlerWithOptions(ctx, wire.SetLabelsCommandName,
		func(uctx workflow.Context, args wire.UpdateArgs) (json.RawMessage, error) {
			if r, ok := env.Completed[args.RequestID]; ok {
				return updateResult(r)
			}
			env.Pending = append(env.Pending, wire.CommandEnvelope{
				RequestID: args.RequestID,
				Name:      wire.SetLabelsCommandName,
				Payload:   args.Payload,
			})
			if err := workflow.Await(uctx, func() bool {
				_, done := env.Completed[args.RequestID]
				return done
			}); err != nil {
				return nil, err
			}
			return updateResult(env.Completed[args.RequestID])
		},
		workflow.UpdateHandlerOptions{
			Validator: func(args wire.UpdateArgs) error {
				if env.MarkedForDeletion {
					return fmt.Errorf("entity %s is being deleted", info.WorkflowExecution.ID)
				}
				var patch map[string]string
				if err := json.Unmarshal(args.Payload, &patch); err != nil {
					return fmt.Errorf("decode label patch: %w", err)
				}
				return nil
			},
		},
	)
	if err != nil {
		return fmt.Errorf("register set-labels update: %w", err)
	}

	// --- updates: tracked mutations. The handler validates, enqueues, then
	// waits for the main loop to execute its command and returns the result
	// (or error) to the caller. Dedup by request id happens first.
	for name := range d.commands {
		cmdName := name
		cmd := d.commands[name]
		err := workflow.SetUpdateHandlerWithOptions(ctx, string(cmdName),
			func(uctx workflow.Context, args wire.UpdateArgs) (json.RawMessage, error) {
				if r, ok := env.Completed[args.RequestID]; ok {
					return updateResult(r)
				}
				env.Pending = append(env.Pending, wire.CommandEnvelope{
					RequestID: args.RequestID,
					Name:      cmdName,
					Payload:   args.Payload,
				})
				if err := workflow.Await(uctx, func() bool {
					_, done := env.Completed[args.RequestID]
					return done
				}); err != nil {
					return nil, err
				}
				return updateResult(env.Completed[args.RequestID])
			},
			workflow.UpdateHandlerOptions{
				// Validator runs BEFORE anything is written to Event
				// History: rejected updates leave no trace and cost nothing.
				Validator: func(args wire.UpdateArgs) error {
					if env.MarkedForDeletion {
						return fmt.Errorf("entity %s is being deleted", info.WorkflowExecution.ID)
					}
					if _, dup := env.Completed[args.RequestID]; dup {
						// Duplicate of a completed request: accept it so the
						// handler can serve the cached result.
						return nil
					}
					if cmd.validate == nil {
						return nil
					}
					// Validators see a read-only Snapshot, never the live
					// envelope.
					return cmd.validate(env.Snapshot(), args.Payload)
				},
			},
		)
		if err != nil {
			return fmt.Errorf("register update %q: %w", cmdName, err)
		}
	}

	// --- creation ---
	if !env.Initialized {
		env.Phase = entity.PhaseCreating
		d.upsertSearchAttrs(ctx, env)
		if d.init != nil {
			st, err := d.init(ctx, env.Spec)
			if err != nil {
				env.Phase = entity.PhaseCreateFailed
				d.upsertSearchAttrs(ctx, env)
				return fmt.Errorf("entity init: %w", err)
			}
			env.State = st
		}
		env.Initialized = true
		env.Phase = entity.PhaseReady
		d.upsertSearchAttrs(ctx, env)
	}

	// --- main loop: serializes ALL side effects. One command per
	// iteration, strict ordering, no concurrent modifications. ---
	processedThisRun := 0
	for {
		// Deletion is a lifecycle transition with explicit handling of
		// pending work: queued commands are failed (their callers get an
		// error), in-flight handlers drain, then the finalizer runs.
		if env.MarkedForDeletion {
			env.Phase = entity.PhaseDeleting
			d.upsertSearchAttrs(ctx, env)
			for _, c := range env.Pending {
				env.RecordCompleted(c.RequestID, nil,
					fmt.Errorf("command %s rejected: entity deleted", c.Name))
			}
			env.Pending = nil
			if err := workflow.Await(ctx, func() bool {
				return workflow.AllHandlersFinished(ctx)
			}); err != nil {
				return err
			}
			if d.finalize != nil {
				if err := d.finalize(ctx, &env.State); err != nil {
					env.Phase = entity.PhaseDeleteFailed
					d.upsertSearchAttrs(ctx, env)
					return fmt.Errorf("entity finalize: %w", err)
				}
			}
			env.Phase = entity.PhaseDeleted
			d.upsertSearchAttrs(ctx, env)
			return nil
		}

		// Execute exactly one pending command, then loop.
		if len(env.Pending) > 0 {
			c := env.Pending[0]
			env.Pending = env.Pending[1:]
			if string(c.Name) == wire.SetLabelsCommandName {
				var patch map[string]string
				err := json.Unmarshal(c.Payload, &patch)
				if err == nil && env.MergeLabels(patch) {
					ec.labelsDirty = true
				}
				env.RecordCompleted(c.RequestID, nil, err)
				processedThisRun++
				d.mirrorLabels(ctx, ec, env)
				continue
			}
			cmd, ok := d.commands[c.Name]
			if !ok {
				env.RecordCompleted(c.RequestID, nil, fmt.Errorf("unknown command %q", c.Name))
				continue
			}
			res, err := cmd.exec(ctx, ec, c.Payload)
			if err != nil {
				logger.Error("entity command failed", "command", c.Name, "error", err)
			}
			env.RecordCompleted(c.RequestID, res, err)
			processedThisRun++
			d.mirrorLabels(ctx, ec, env)
			continue
		}

		// Continue-as-New: only from here, only with an empty queue, and
		// only after every in-flight handler has finished, carrying the
		// compact envelope into a fresh run under the same workflow ID.
		if d.canWanted(info, processedThisRun) {
			if err := workflow.Await(ctx, func() bool {
				return workflow.AllHandlersFinished(ctx)
			}); err != nil {
				return err
			}
			return workflow.NewContinueAsNewError(ctx, d.kind, env)
		}

		// Idle: wait for work, deletion, or CAN pressure. If a reconcile
		// interval is configured, the wait times out periodically and the
		// tick runs the health/drift check — the article's "periodic health
		// checks via timeout expiration", serialized in the main loop.
		cond := func() bool {
			return len(env.Pending) > 0 || env.MarkedForDeletion || d.canWanted(info, processedThisRun)
		}
		if d.reconcile != nil && d.reconcileEvery > 0 {
			fired, err := workflow.AwaitWithTimeout(ctx, d.reconcileEvery, cond)
			if err != nil {
				return err
			}
			if !fired {
				if rErr := d.reconcile(ctx, ec); rErr != nil {
					// A failed health check must not kill the entity.
					logger.Error("entity reconcile tick failed", "error", rErr)
				}
			}
		} else {
			if err := workflow.Await(ctx, cond); err != nil {
				return err
			}
		}
	}
}

func (d *Definition[Spec, State]) canWanted(info *workflow.Info, processed int) bool {
	if info.GetContinueAsNewSuggested() {
		return true
	}
	return d.forceCANEvery > 0 && processed >= d.forceCANEvery
}

func (d *Definition[Spec, State]) upsertSearchAttrs(ctx workflow.Context, env *wire.Envelope[Spec, State]) {
	if !d.useSearchAttrs {
		return
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx,
		SearchAttrKind.ValueSet(string(d.kind)),
		SearchAttrPhase.ValueSet(string(env.Phase)),
		SearchAttrLabels.ValueSet(labelValues(env.Labels)),
	); err != nil {
		workflow.GetLogger(ctx).Error("upsert search attributes failed", "error", err)
	}
}

// mirrorLabels re-upserts the search attributes when a handler or the
// built-in patch changed labels since the last mirror.
func (d *Definition[Spec, State]) mirrorLabels(ctx workflow.Context, ec *Ctx[Spec, State], env *wire.Envelope[Spec, State]) {
	if !ec.labelsDirty {
		return
	}
	ec.labelsDirty = false
	d.upsertSearchAttrs(ctx, env)
}

// labelValues renders labels as sorted "k=v" keywords — the visibility
// form: EntityLabels IN ("env=prod").
func labelValues(labels map[string]string) []string {
	out := make([]string, 0, len(labels))
	for k, v := range labels {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

func updateResult(r wire.OpResult) (json.RawMessage, error) {
	if r.Err != "" {
		return nil, errors.New(r.Err)
	}
	return r.Result, nil
}
