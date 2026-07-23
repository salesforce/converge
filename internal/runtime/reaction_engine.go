package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// maxStatusBytes caps the status a worker returns for one reconcile. A provider
// (especially a remote / external worker over the broker) is untrusted here: a
// buggy or hostile handler could return a huge status that would bloat the
// work_outbox row, the drain transaction, and resources.status — a resource-
// exhaustion vector against the control plane and DB. Enforced at the single
// point where the final status is written (before AppendOutbox), so it catches
// EVERY worker path — in-process AND remote-via-broker — not just one transport.
// An oversized status is a deterministic provider/attack condition, so the task
// fails TERMINALLY (won't retry-loop); the resource surfaces phase=Failed with a
// clear reason instead of silently OOMing the drain. (The Connect transport also
// caps whole messages at wirelimits.MaxMessageBytes; this is the tighter,
// field-precise semantic cap on the status payload specifically.)
const maxStatusBytes = 50 * 1024 * 1024

// ─────────────────────────────────────────────────────────────────────────
// runReaction — the manifest-driven dispatch engine.
//
// It selects the reaction(s) whose Trigger matches the claimed task, runs each
// through the unified DispatchStage seam, and APPLIES only the Outcome parts each
// reaction's declared Emits mask permits. The core stays KIND-BLIND — it reads
// the masks, never a kind name.
//
// CRITICAL: every correctness gate stays IN THE CORE and SQL-backed:
//   - composed_gen >= generation gate (compose skip);
//   - descendantsSettled gate (rollup) + advance=false-until-settled;
//   - the single fenced AppendOutbox at the end (carrying manifest_version);
//   - ApplyComposeResult's single-tx fence (work_id, worker_id, generation).
// WHICH reactions run is data, decided by the manifest's declared Emits masks.
//
// This is the ONLY reconcile/delete/operate path: dispatch always routes a
// claimed task through runReaction. A kind with no manifest fails loudly here
// rather than silently no-op-ing.
// ─────────────────────────────────────────────────────────────────────────

// runReaction dispatches a claimed task against its kind model. It routes by
// task_type to the reconcile-shaped (compose/work/rollup) or delete or operate
// reaction path; lifecycle reactions are NOT claimed here (they ride
// lifecycle_outbox via the reactor dispatcher, which keeps its separate
// at-least-once queue — see reactor.go).
func (l *Loop) runReaction(ctx context.Context, task store.WorkTask, m model.KindManifest) {
	switch task.TaskType {
	case store.TaskReconcile:
		l.reactReconcile(ctx, task, m)
	case store.TaskDelete:
		l.reactDelete(ctx, task, m)
	case store.TaskOperate:
		l.reactOperate(ctx, task, m)
	default:
		l.recordFailed(ctx, task, fmt.Sprintf("unknown task_type %q", task.TaskType))
	}
}

// reactReconcile is the manifest-driven reconcile: it runs the composer reaction
// (if declared and composed_gen < generation), the worker reaction (if declared),
// and the rollup reaction (if declared and descendants settled), accumulating one
// status + conditions and writing ONE fenced AppendOutbox. Reaction selection
// comes from the manifest's declared Emits masks.
func (l *Loop) reactReconcile(ctx context.Context, task store.WorkTask, m model.KindManifest) {
	composer, hasComposer := m.ComposerReaction()
	worker, hasWorker := m.WorkerReaction()
	rollup, hasRollup := m.RollupReaction()

	if !hasComposer && !hasWorker {
		// Empty-reactions guard (adversarial finding): a reconcile task for a kind
		// whose manifest declares no spec-change reaction is a misconfiguration —
		// fail loudly rather than silently no-op (which would strand the resource).
		l.recordFailed(ctx, task, fmt.Sprintf("kind %q manifest declares no specChange reaction (compose/work)", task.Kind))
		return
	}

	resource := taskToResource(task)
	env := l.newEnv(task)

	var status json.RawMessage
	var conditions []model.Condition

	// ── Composer reaction ──
	if hasComposer {
		composedGen, err := l.Repo.GetComposedGen(ctx, task.ResourceID)
		if err != nil {
			l.recordFailed(ctx, task, fmt.Sprintf("read composed_gen: %v", err))
			return
		}
		if composedGen < task.Generation { // gate: only when the spec changed
			out, _, err := l.dispatcher().DispatchStage(ctx, task.Kind, task.KindVersion, composer, model.ReactionRequest{
				Reaction: composer.Name, Trigger: model.TriggerSpecChange,
				Resource: resource, Generation: task.Generation, Env: env,
			})
			if err != nil {
				l.recordFailedErr(ctx, task, fmt.Sprintf("compose reaction %q", composer.Name), err)
				return
			}
			if len(out.Status) > 0 {
				status = out.Status
			}
			conditions = append(conditions, out.Conditions...)

			// The worker's compose handler has returned; this pod's own goroutine now
			// owns the (potentially minute-long, for a 1M-child BOM) apply below. Flip the
			// lease to local so its liveness is this live goroutine — not the now-finished
			// worker's attestation — and the long apply can't starve past requireWithin and
			// reap itself. (No-op on the in-process path, where MarkLocal is unset.)
			if l.MarkLocal != nil {
				l.MarkLocal(task.ID)
			}

			// Apply the compose result in ONE fenced tx.
			tx, err := l.Tx.Begin(ctx)
			if err != nil {
				l.recordFailed(ctx, task, fmt.Sprintf("begin compose tx: %v", err))
				return
			}
			txRepo := l.Repo.WithTx(tx)
			counts, err := txRepo.ApplyComposeResult(ctx, task.ResourceID, task.Generation, task.ID, task.ClaimEpoch, task.ManifestVersion, out.Children, out.Edges, out.Configs, l.Manifests)
			if err != nil {
				_ = tx.Rollback(ctx)
				if errors.Is(err, store.ErrComposeClaimLost) {
					return // benign reassign — the authoritative pod composes
				}
				l.recordFailed(ctx, task, fmt.Sprintf("apply compose result: %v", err))
				return
			}
			_ = txRepo.EmitEvent(ctx, store.Event{
				ResourceID: task.ResourceID, Type: store.EventComposeSucceeded, Actor: l.BrokerID,
				Detail: map[string]any{
					"broker": l.BrokerID, "generation": task.Generation,
					"children_created": counts.Created, "children_updated": counts.Updated, "children_deleted": counts.Deleted,
					// A no-longer-produced child whose kind has orphan_grace_secs > 0 is
					// SET ASIDE (Orphaned, awaiting the reaper's grace-expiry teardown),
					// NOT hard-deleted — so it's counted here, not in children_deleted.
					// Surfacing it makes deleted=0 alongside a non-zero orphaned reconcile
					// (a relocated/dropped child shows up as orphaned, not deleted).
					// children_readopted = previously-orphaned children this compose
					// re-emitted (grace marks cleared, teardown cancelled).
					"children_orphaned": counts.Orphaned, "children_readopted": counts.Readopted,
				},
			})
			// Deferred schedule: ARM the composed children into schedule_recheck IN
			// THIS TX. Scheduling ~10^5–10^6 candidates in one post-commit call would
			// be a multi-second writer overlapping the whole drain fleet, colliding
			// with DrainOutboxBatch into a 40P01 deadlock whose dropped wake would
			// strand the children on the reaper backstop. Arming in-tx commits
			// atomically with the children (the wake can never be lost), and the
			// drainer schedules them band-by-band, paced with its drains, in small
			// non-deadlocking batches (drain_schedule_rechecks). The reaper remains
			// the durable backstop.
			if len(counts.CandidateIDs) > 0 {
				if err := txRepo.ArmScheduleRecheck(ctx, counts.CandidateIDs); err != nil {
					_ = tx.Rollback(ctx)
					l.recordFailed(ctx, task, fmt.Sprintf("arm schedule recheck: %v", err))
					return
				}
			}
			if err := tx.Commit(ctx); err != nil {
				l.recordFailed(ctx, task, fmt.Sprintf("commit compose tx: %v", err))
				return
			}

			// A large compose just bulk-inserted thousands of children + edges. Refresh
			// planner stats on resources/resource_deps so the downstream schedule_eligible
			// / substitute / drain plans for those rows are costed against real
			// cardinalities, not the pre-insert (often near-empty) snapshot. Without this
			// the stale stats mis-cost every subsequent reconcile cycle fleet-wide, which
			// stretches the fixed-cadence drain/schedule loops into many more, smaller
			// passes (the 1M regression this restores). Best-effort: a failed ANALYZE only
			// means autovacuum catches up later, so log and continue.
			const analyzeThreshold = 1000
			if counts.Upserted+counts.Edges >= analyzeThreshold {
				if err := l.Repo.AnalyzeComposeTables(ctx); err != nil {
					slog.Warn("post-compose ANALYZE failed", "resource", task.ResourceID, "err", err)
				}
			}

			// (Scheduling is now done by the in-tx ArmScheduleRecheck above +
			// the drainer's drain_schedule_rechecks — see OPT-A note.)
		}
	}

	// ── Worker reaction ──
	if hasWorker {
		out, _, err := l.dispatcher().DispatchStage(ctx, task.Kind, task.KindVersion, worker, model.ReactionRequest{
			Reaction: worker.Name, Trigger: model.TriggerSpecChange,
			Resource: resource, Status: status, Generation: task.Generation, Env: env,
		})
		if err != nil {
			l.recordFailedErr(ctx, task, fmt.Sprintf("work reaction %q", worker.Name), err)
			return
		}
		if len(out.Status) > 0 {
			status = out.Status
		}
		conditions = append(conditions, out.Conditions...)
	}

	// ── Rollup reaction ──
	// advance=false until the subtree settles (preserves Advance-Synced-Gen Only
	// on Full Pipeline): a kind with a rollup reaction must wait for descendants.
	advance := true
	if hasRollup {
		settled, err := l.descendantsSettled(ctx, task.ResourceID)
		if err != nil {
			l.recordFailed(ctx, task, fmt.Sprintf("descendants-settled check: %v", err))
			return
		}
		if !settled {
			advance = false
		} else {
			rows, err := l.Repo.ListDescendants(ctx, task.ResourceID)
			if err != nil {
				l.recordFailed(ctx, task, fmt.Sprintf("list descendants: %v", err))
				return
			}
			descendants := make([]model.Resource, len(rows))
			for i, r := range rows {
				descendants[i] = dbqToResource(r)
			}
			out, _, err := l.dispatcher().DispatchStage(ctx, task.Kind, task.KindVersion, rollup, model.ReactionRequest{
				Reaction: rollup.Name, Trigger: model.TriggerChildrenSettled,
				Resource: resource, Descendants: descendants, Status: status, Generation: task.Generation, Env: env,
			})
			if err != nil {
				_ = l.Repo.EmitEvent(ctx, store.Event{
					ResourceID: task.ResourceID, Type: store.EventRollupFailed, Actor: l.BrokerID,
					Detail: map[string]any{"broker": l.BrokerID, "generation": task.Generation, "error": err.Error()},
				})
				l.recordFailedErr(ctx, task, fmt.Sprintf("rollup reaction %q", rollup.Name), err)
				return
			}
			if len(out.Status) > 0 {
				status = out.Status
			}
			conditions = append(conditions, out.Conditions...)
			_ = l.Repo.EmitEvent(ctx, store.Event{
				ResourceID: task.ResourceID, Type: store.EventRollupSucceeded, Actor: l.BrokerID,
				Detail: map[string]any{"broker": l.BrokerID, "generation": task.Generation, "descendants": len(descendants)},
			})
		}
	}

	// Reject an oversized worker status before it hits the outbox/DB (untrusted
	// provider — esp. a remote worker over the broker). Deterministic → terminal.
	if len(status) > maxStatusBytes {
		l.recordFailedTerminal(ctx, task,
			fmt.Sprintf("status exceeds the %d MiB limit (%d bytes)", maxStatusBytes>>20, len(status)), true)
		return
	}
	healthOK, condsJSON := deriveHealthAndConditions(conditions, advance)
	if ctx.Err() != nil {
		return // timed-out orphan: skip the success write
	}
	if err := l.Repo.AppendOutbox(ctx, store.OutboxAppend{
		WorkID: task.ID, ResourceID: task.ResourceID, ClaimEpoch: task.ClaimEpoch, ShardID: task.ShardID,
		TaskType: store.TaskReconcile, Succeeded: true, ObservedGeneration: task.Generation,
		AdvanceSyncedGen: advance, HealthOK: healthOK, Conditions: condsJSON, Payload: status,
		ManifestVersion: task.ManifestVersion,
	}); err != nil {
		l.recordFailed(ctx, task, fmt.Sprintf("append reconcile outbox: %v", err))
		return
	}
}

// reactDelete runs the deleteRequested reaction (finalizer teardown) and strips
// the finalizer via a fenced AppendOutbox.
func (l *Loop) reactDelete(ctx context.Context, task store.WorkTask, m model.KindManifest) {
	del, found := m.DeleteReaction()
	if !found {
		l.recordFailed(ctx, task, fmt.Sprintf("kind %q manifest declares no deleteRequested reaction", task.Kind))
		return
	}
	resource := taskToResource(task)
	env := l.newEnv(task)

	_, _, err := l.dispatcher().DispatchStage(ctx, task.Kind, task.KindVersion, del, model.ReactionRequest{
		Reaction: del.Name, Trigger: model.TriggerDeleteRequested,
		Resource: resource, Generation: task.Generation, Env: env,
	})
	if err != nil {
		l.recordFailedErr(ctx, task, fmt.Sprintf("delete reaction %q", del.Name), err)
		return
	}
	if ctx.Err() != nil {
		return
	}
	finalizer := del.Finalizer
	if finalizer == "" {
		finalizer = m.FinalizerName
	}
	if err := l.Repo.AppendOutbox(ctx, store.OutboxAppend{
		WorkID: task.ID, ResourceID: task.ResourceID, ClaimEpoch: task.ClaimEpoch, ShardID: task.ShardID,
		TaskType: store.TaskDelete, Succeeded: true, ObservedGeneration: task.Generation,
		FinalizerName: finalizer, ManifestVersion: task.ManifestVersion,
	}); err != nil {
		l.recordFailed(ctx, task, fmt.Sprintf("append delete outbox: %v", err))
	}
}

// reactOperate runs the operation reaction matching the claimed op's verb and
// writes its output via a fenced AppendOutbox. The verb handler is looked up
// from the manifest's operation reaction (the StageDispatcher) during the
// transition.
func (l *Loop) reactOperate(ctx context.Context, task store.WorkTask, m model.KindManifest) {
	if task.OpID == nil {
		l.recordFailed(ctx, task, "operate task missing op_id")
		return
	}
	op, err := l.Repo.GetOperation(ctx, *task.OpID)
	if err != nil {
		l.recordFailed(ctx, task, fmt.Sprintf("load operation: %v", err))
		return
	}
	// Find the operation reaction for this verb.
	opRx, found := m.OperationReaction(op.Verb)
	if !found {
		l.recordFailed(ctx, task, fmt.Sprintf("kind %q manifest declares no operation reaction for verb %q", task.Kind, op.Verb))
		return
	}

	// The verb handler is resolved by the reaction executor (by opRx.Name) — the
	// in-process executor looks it up in its ReactionRegistry; a broker ships the
	// reaction name to a dumb worker, which looks up its own handler. The core
	// holds no handler code.
	resource := taskToResource(task)

	// fwOp is the provider-side view: no row id (the broker correlates the result
	// by op.ID internally; the handler only needs ResourceID + verb + input).
	fwOp := model.Operation{
		ResourceID:  op.ResourceID,
		Verb:        op.Verb,
		Input:       op.Input,
		Attempts:    int(op.Attempts),
		RequestedBy: derefStringPort(op.RequestedBy),
	}
	out, _, err := l.dispatcher().DispatchStage(ctx, task.Kind, task.KindVersion, opRx, model.ReactionRequest{
		Reaction: opRx.Name, Trigger: model.TriggerOperation,
		Resource: resource, Operation: &fwOp, Generation: task.Generation, Env: l.newEnv(task),
	})
	if err != nil {
		l.recordFailedErr(ctx, task, fmt.Sprintf("operate reaction %q", opRx.Name), err)
		return
	}
	if ctx.Err() != nil {
		return
	}
	// Same untrusted-worker cap as the reconcile status: an operation's output
	// lands in resource_operations.output, so bound it too. Deterministic → terminal.
	if len(out.OperationOutput) > maxStatusBytes {
		l.recordFailedTerminal(ctx, task,
			fmt.Sprintf("operation output exceeds the %d MiB limit (%d bytes)", maxStatusBytes>>20, len(out.OperationOutput)), true)
		return
	}
	if err := l.Repo.AppendOutbox(ctx, store.OutboxAppend{
		WorkID: task.ID, ResourceID: task.ResourceID, ClaimEpoch: task.ClaimEpoch, ShardID: task.ShardID,
		TaskType: store.TaskOperate, Succeeded: true, ObservedGeneration: task.Generation,
		OpID: task.OpID, Payload: out.OperationOutput, ManifestVersion: task.ManifestVersion,
	}); err != nil {
		l.recordFailed(ctx, task, fmt.Sprintf("append operate outbox: %v", err))
	}
}
