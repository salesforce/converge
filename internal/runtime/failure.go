package runtime

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// ─────────────────────────────────────────────────────────────────────────
// Failure path — write a False outbox row + emit a WorkFailed event so
// the API's grouped-failures view can roll it up.
// ─────────────────────────────────────────────────────────────────────────

// recordFailed records a TRANSIENT failure (retried after the reaper's
// window). Most framework-internal errors (panic, kind-not-registered,
// missing op) are transient by nature.
func (l *Loop) recordFailed(ctx context.Context, task store.WorkTask, msg string) {
	l.recordFailedTerminal(ctx, task, msg, false)
}

// recordFailedErr records a STAGE failure, deriving terminal-ness from the
// returned error: a provider that wraps with model.Terminal() marks
// the failure non-retryable (the row stays phase=Failed until the spec
// changes). A plain error is transient. `prefix` labels the stage.
func (l *Loop) recordFailedErr(ctx context.Context, task store.WorkTask, prefix string, err error) {
	l.recordFailedTerminal(ctx, task, fmt.Sprintf("%s: %v", prefix, err), model.IsTerminal(err))
}

func (l *Loop) recordFailedTerminal(ctx context.Context, task store.WorkTask, msg string, terminal bool) {
	// A cancelled ctx means this is a timed-out orphan that lost the
	// deadline race (runOne already recorded the failure via the live
	// parent ctx) OR a shutdown. Either way our writes here would fail on
	// the dead ctx; skip them quietly rather than emitting a duplicate
	// attempt-bump/event and a spray of "context canceled" error logs.
	if ctx.Err() != nil {
		return
	}
	// POISON-PILL is enforced DB-SIDE (drain_outbox_batch), gating on the DURABLE
	// resources.failure_attempts counter against kind_config.max_transient_attempts.
	// It cannot live here: a transient failure DELETEs this work_queue row and re-pends
	// a FRESH one, so task.Attempts (the work_queue-local counter) resets every cycle
	// and would never reach the cap. The drain increments the resource counter across
	// cycles and escalates to terminal at the cap. We still record the transient
	// failure normally below.
	if err := l.Repo.WorkQueueIncrementAttempts(ctx, task.ID, task.ShardID); err != nil {
		slog.Warn("loop increment attempts", "kind", task.Kind, "resource", task.ResourceID, "err", err)
	}
	reason := "ReconcileError"
	if terminal {
		reason = "Terminal"
	}
	// Carry the log-locator fields in detail so the failure event is
	// self-documenting: an operator reading it knows exactly which broker
	// (pod/process) ran it and the slog keys to grep by. The broker's
	// structured logs are tagged broker=/resource=/kind=/task_type=/
	// generation= (see newEnv), so these reproduce that filter. `broker`
	// (= Actor) is the host-pid-uuid8 id; `attempts` is the retry count.
	_ = l.Repo.EmitEvent(ctx, store.Event{
		ResourceID: task.ResourceID,
		Type:       store.EventWorkFailed,
		Actor:      l.BrokerID,
		Reason:     reason,
		Message:    msg,
		Detail: map[string]any{
			"broker":     l.BrokerID,
			"task_type":  string(task.TaskType),
			"generation": task.Generation,
			"attempts":   task.Attempts,
			"shard_id":   task.ShardID,
			"terminal":   terminal,
		},
	})
	a := store.OutboxAppend{
		WorkID:             task.ID,
		ResourceID:         task.ResourceID,
		ClaimEpoch:         task.ClaimEpoch,
		ShardID:            task.ShardID,
		TaskType:           task.TaskType,
		Succeeded:          false,
		ObservedGeneration: task.Generation,
		ErrorMessage:       msg,
	}
	// For reconcile failures, flip the scalar failure axis: the drainer
	// stamps failure_gen = task.Generation, so the resource's generated
	// `phase` becomes 'Failed' — directly queryable/countable, not inferred
	// from a condition. A terminal failure also sets failure_terminal so the
	// scheduler stops re-queuing it. We ALSO store a Synced=False condition
	// to carry the human MESSAGE for the grouped-failures view; that row is
	// pure decoration (membership is decided by phase) and is auto-cleared
	// by the drainer's recovery pass once the resource reconciles cleanly.
	if task.TaskType == store.TaskReconcile {
		a.Failed = true
		a.Terminal = terminal
		a.Conditions = marshalConditions([]model.Condition{{
			Type:    model.TypeSynced,
			Status:  model.ConditionFalse,
			Reason:  reason,
			Message: msg,
		}})
	}
	if task.TaskType == store.TaskOperate && task.OpID != nil {
		a.OpID = task.OpID
	}
	if task.TaskType == store.TaskDelete {
		// A failed delete (teardown rejected/broken) must not vanish: the drain
		// re-pends it so it retries (STRICT — a node that can't finish its teardown
		// keeps blocking its parent until it succeeds; there is no dead-letter
		// unblock). Flip Failed so the drain's failed-delete re-pend arm fires;
		// without it the outbox row carries Failed=false and the failed delete is
		// silently dropped and its parent wedges.
		a.Failed = true
		if l.Manifests != nil {
			if m, ok := l.Manifests.Get(ctx, task.Kind, task.KindVersion); ok {
				a.FinalizerName = m.FinalizerName
			}
		}
	}
	if err := l.Repo.AppendOutbox(ctx, a); err != nil {
		slog.Error("loop record failed", "kind", task.Kind, "resource", task.ResourceID, "err", err)
	}
}
