package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/shardutil"
	"github.com/salesforce/converge/internal/store"
)

// NumShards aliases shardutil.NumShards for downstream wiring.
const NumShards = shardutil.NumShards

// AllShards is the default sharding domain.
func AllShards() []int16 {
	out := make([]int16, NumShards)
	for i := range out {
		out[i] = int16(i)
	}
	return out
}

// Loop is the execution handle for the dispatch path. With the unified
// dispatcher it is no longer a per-pair goroutine driver — it just bundles
// the shared deps the runOne… methods read. Kept as the method receiver so
// the (large, well-tested) execution path is untouched.
//
// The wakes the dispatcher reacts to (work_ready) and emits indirectly are
// fired ENTIRELY in the DB, so no Go-side notifier lives here (a Go-side
// per-completion pg_notify storm was an earlier regression). The two channels
// use DIFFERENT mechanisms on purpose:
//   - outbox_ready: per-row notify_gated in AppendOutbox — globally
//     rate-limited via the wake_state advisory lock (a result-drain wake can
//     tolerate coalescing).
//   - work_ready: a DIRECT, intentionally UNGATED pg_notify in
//     schedule_eligible / the cascade + resync paths. Gating it was actively
//     harmful: notify_gated's pg_try_advisory_XACT_lock is held to COMMIT, so
//     a wake fired inside the drainer's seconds-long drain tx was lost,
//     stranding rollups.
type Loop struct {
	// Tx opens the compose-fence transaction (ApplyComposeResult in one tx via
	// Repo.WithTx). A narrow txBeginner, not the concrete pool — the dispatch
	// core depends on the transaction capability, not the substrate.
	Tx       txBeginner
	Repo     DispatcherRepo
	Listener Listener // work_ready wake subscription
	BrokerID string

	// Dispatch runs a kind's reaction handler: a broker ships it to a dumb worker
	// via its remote fanout executor; the integration test harness injects an
	// in-process executor (test/internal/inproc) that runs handlers directly from a
	// ReactionRegistry. The DB reads/writes around every reaction (gate reads,
	// ApplyComposeResult, ListDescendants, AppendOutbox, …) stay on this
	// lease-holder. nil → the fail-closed noHandlerExecutor (a task fails
	// transiently and re-arms; production always installs the fanout via SetDispatcher).
	Dispatch model.StageDispatcher

	// Manifests is the kind_manifest cache: dispatch reads a kind's reactions
	// from it (selected by Trigger+Emits) to drive the reaction engine. Required —
	// a kind with no manifest can't be dispatched (the task fails loudly).
	Manifests *KindManifestCache

	// MarkLocal, when set, flips a task's in-flight record to local at the moment this
	// pod's OWN goroutine starts the post-Complete apply phase (ApplyComposeResult) — so
	// the long local apply is exempt from the worker-attestation staleness gate and can't
	// reap its own lease mid-apply. Until then the task is worker-attested (kept fresh by
	// the remote worker's WorkHeartbeat), so a never-delivered task correctly lapses and
	// the reaper re-dispatches it. Wired by the Dispatcher to inFlight.markLocal; nil on
	// the in-process test path (whose staleness gate is disabled anyway).
	MarkLocal func(id uuid.UUID)

	// RemoteFanoutCeiling bounds a task whose kind set NO TaskDeadline (the common
	// default) when the executor is REMOTE (a broker fanning out to a dumb
	// worker): such a task otherwise parks the dispatchStage goroutine + its
	// pair.inFlight slot indefinitely if the worker wedges-but-stays-connected (the
	// stream never drops, so the abandon-on-disconnect path can't fire and the DB
	// heartbeat keeps the lease un-reapable). A broker sets this to a generous
	// ceiling (well above the slowest legitimate handler) so a wedged remote task
	// is eventually cancelled via the proven WithTimeout/orphan path below and its
	// slot freed. 0 (the default, and the in-process executor) = unbounded, exactly
	// as before — the in-process dispatch is synchronous and frees its own slot, so
	// it never needs this.
	RemoteFanoutCeiling time.Duration
}

// dispatcher returns the configured reaction executor, defaulting to the
// fail-closed noHandlerExecutor so a zero-value Loop is safe: production installs
// the broker fanout via SetDispatcher and the test harness injects an in-process
// executor, so the default is only reached on a wiring bug — where it fails the
// task transiently (reaper re-arms) rather than panicking.
func (l *Loop) dispatcher() model.StageDispatcher {
	if l.Dispatch == nil {
		return noHandlerExecutor{}
	}
	return l.Dispatch
}

// drainTimer clears a stopped/fired timer's channel so a later Reset is
// race-free. Safe to call whether or not the timer has fired.
func drainTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

// runOne dispatches one task. A panic inside the pipeline is recovered
// into a False outbox row.
//
// When the kind declares a TaskDeadline, runOne enforces it at the
// DISPATCHER level rather than trusting the handler to honor a context
// deadline: it runs the dispatch in a goroutine under a cancelable
// taskCtx and races it against a timer. If the deadline fires first, it
// cancels taskCtx (signaling cooperative handlers) and records a
// TRANSIENT failure right away — freeing the worker slot on time even
// for a wedged, ctx-ignoring handler. The orphaned goroutine keeps
// running but is neutered: every result it would write (success or
// failure) goes through taskCtx, which is now cancelled, so its
// AppendOutbox fails and writes nothing — no double-processing, no
// resurrecting the failure the deadline just recorded. The deadline's
// own failure report uses the parent ctx (uncancelled) so it commits.
//
// SHUTDOWN vs TIMEOUT — two different exits from the race, but the orphan
// is ALWAYS tracked in the dispatcher's wg so it can't outlive Run():
//   - Genuine TIMEOUT (parent ctx still live): runOne returns WITHOUT
//     blocking on the orphan — that's the whole point, a wedged handler
//     can't pin the slot. But the orphan was launched under `wg`, so
//     Run's `defer wg.Wait()` still joins it on shutdown; its DB writes go
//     through the (cancelled-on-shutdown) taskCtx so it can't corrupt
//     state. Frees the slot on time AND keeps the no-orphan-outlives-Run
//     invariant.
//   - SHUTDOWN (parent ctx cancelled): we additionally block on the orphan
//     here so its writes settle before the slot is reported free, and
//     don't write a deadline failure on a dead ctx — the reaper reclaims.
//
// wg is the dispatcher's goroutine tracker; the orphan joins it so no
// launched dispatch outlives the loop, even on the TIMEOUT-first path.
func (l *Loop) runOne(ctx context.Context, task store.WorkTask, wg *sync.WaitGroup) {
	// Carry the work_queue lease identity (row id + shard + claim epoch + manifest
	// version) on the ctx so a remote reaction executor (the broker's fanout) can
	// scoped-release the DB lease the instant it abandons the task (deadline / dropped
	// worker stream) instead of waiting out the reaper's StaleAfter window, and so the
	// fenced result write carries the claim epoch. The in-process executor ignores it.
	ctx = withLease(ctx, Lease{
		WorkID:          task.ID,
		ShardID:         task.ShardID,
		BrokerID:        l.BrokerID,
		KindVersion:     task.KindVersion,
		ClaimEpoch:      task.ClaimEpoch,
		ManifestVersion: task.ManifestVersion,
	})

	// TaskDeadline comes from the claim (kind_config), so an operator's edit is
	// live on the next claim with no restart. A kind with no deadline falls back to
	// the REMOTE fanout ceiling (if a broker set one) so a wedged-but-connected
	// worker can't park this slot forever; the in-process path leaves the ceiling
	// at 0 and dispatches synchronously (frees its own slot), so it's unbounded as
	// before.
	deadline := task.TaskDeadline
	if deadline <= 0 {
		deadline = l.RemoteFanoutCeiling
	}
	if deadline <= 0 {
		l.dispatch(ctx, task)
		return
	}

	taskCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	done := make(chan struct{})
	// Track the orphan in the dispatcher's wg: on a genuine TIMEOUT we return
	// without waiting for it (so a wedged handler can't pin the slot), but
	// Run's defer wg.Wait() must still join it before pool.Close on shutdown.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		l.dispatch(taskCtx, task)
	}()
	select {
	case <-done:
		// Handler finished within the window (or shutdown cancelled the parent
		// ctx, which propagated to taskCtx) — it reported its own result.
	case <-taskCtx.Done():
		if ctx.Err() != nil {
			// SHUTDOWN: parent ctx dead. Wait for the orphan to drain (bounded —
			// taskCtx is cancelled and handlers honor it) so it can't race
			// pool.Close() once Run() returns. No failure write on a dead ctx.
			<-done
			return
		}
		// Genuine TIMEOUT: free the slot now without blocking on the orphan (the
		// point of the deadline) — it stays tracked in wg — and record a
		// retryable failure via the live parent ctx.
		l.recordFailed(ctx, task, fmt.Sprintf("deadline exceeded: task did not complete within %s", deadline))
	}
}

// dispatch routes one task to the reaction engine and reports the result. A
// panic inside a reaction is recovered into a False outbox row. The ctx here is
// the per-task context (deadline-bounded when the kind sets TaskDeadline); every
// result write goes through it, so cancelling it neuters a timed-out orphan's
// writes (see runOne). The kind's manifest selects which reaction(s) run; a kind
// with no manifest fails loudly (it can't be dispatched).
func (l *Loop) dispatch(ctx context.Context, task store.WorkTask) {
	defer func() {
		if r := recover(); r != nil {
			l.recordFailed(ctx, task, fmt.Sprintf("panic: %v", r))
		}
	}()

	if l.Manifests == nil {
		l.recordFailed(ctx, task, "dispatch: no manifest cache configured")
		return
	}
	m, ok := l.Manifests.Get(ctx, task.Kind, task.KindVersion)
	if !ok {
		l.recordFailed(ctx, task, fmt.Sprintf("kind %q/v%d has no manifest", task.Kind, task.KindVersion))
		return
	}
	l.runReaction(ctx, task, m)
}

// descendantsSettled reports whether no live descendant of root is still
// PROGRESSING — i.e. every descendant is either synced (synced_gen >=
// generation), failed (failure_gen = generation), or set aside (deleting /
// frozen). A failed or set-aside descendant is "settled" so the root's rollup
// runs and reports Degraded/ChildrenNotReady instead of stalling forever on a
// doomed or frozen child. The gate query + its "MUST mirror schedule_eligible"
// contract now live in store.DescendantsSettled (behind DispatcherRepo) so the
// dispatch core carries no inline SQL and no pool.
func (l *Loop) descendantsSettled(ctx context.Context, rootID uuid.UUID) (bool, error) {
	return l.Repo.DescendantsSettled(ctx, rootID)
}

func (l *Loop) newEnv(task store.WorkTask) *model.Env {
	return &model.Env{
		Logger: slog.With(
			"kind", task.Kind,
			"resource", task.ResourceID,
			"task_type", task.TaskType,
			"generation", task.Generation,
			"broker", l.BrokerID,
		),
		// Per-task CUSTOM config override, cloned into work_queue at schedule
		// time (nil when the resource carries none). Every stage of the task
		// sees it; a provider overlays it on its kind default (an object merge
		// via converge.EffectiveConfig). Spec is a separate axis, untouched.
		ProviderConfig: task.ProviderConfig,
		// Per-task CUSTOM bundle override (the opaque artifact clone). nil = use
		// the kind default bundle; non-nil REPLACES it (converge.EffectiveBundle).
		ProviderBundle: task.ProviderBundle,
	}
}
