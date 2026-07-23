package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Reaper default windows — the SINGLE source of truth. NewReaper seeds them and
// cmd/converge's Config.applyDefaults fills the SAME values so the reported
// cluster config matches what runs. The control plane overrides each from its
// SWEEPER_* env only when set (>0). Reference THESE constants, never the literals.
const (
	// DefaultSweeperStaleAfter is the lease-expiry crash backstop: a claimed task
	// whose heartbeat stops for this long is reaped + requeued.
	DefaultSweeperStaleAfter = 30 * time.Second
	// DefaultSweeperUnclaimedDeleteAfter is the abandoned-UNCLAIMED-work GC window:
	// a work_queue row no worker claimed for this long is dropped.
	DefaultSweeperUnclaimedDeleteAfter = 24 * time.Hour
)

// Reaper drives the two periodic Postgres calls the trigger-based
// scheduler can't do on its own. Same shape as today's reaper.WorkReaper.
type Reaper struct {
	Repo       ReaperRepo
	StaleAfter time.Duration
	RetryAfter time.Duration
	Interval   time.Duration
	BatchSize  int

	// LifecycleStaleAfter is the stale-claim window for lifecycle_outbox reactor
	// deliveries — its own knob so a failed or crashed delivery re-arms for
	// another at-least-once attempt. Safe to be short because the
	// ReactorDispatcher heartbeats its in-flight claims (HeartbeatEvery), so a
	// legitimately slow reactor keeps its lease; only a truly dead/failed
	// delivery goes stale. Must exceed the dispatcher's HeartbeatEvery.
	LifecycleStaleAfter time.Duration

	// UnclaimedDeleteAfter is the GC window for abandoned work: a work_queue row
	// that has sat UNCLAIMED (worker_id NULL) this long is hard-deleted, because
	// nothing ever picked it up — its kind has no worker (provider removed /
	// decommissioned). Distinct from StaleAfter, which RECOVERS a stale CLAIMED
	// row (worker died mid-task); this DROPS a never-claimed one. Deliberately
	// long (default 24h) so a transiently-down fleet has ample time to come back
	// and drain its backlog before anything is reclaimed. 0 disables the pass.
	UnclaimedDeleteAfter time.Duration

	IdleInterval time.Duration

	// OrphanSweepEvery throttles the orphan-grace sweep: it runs every Nth tick,
	// not every tick. frozen_until is an ABSOLUTE deadline baked at stamp time,
	// so a coarse cadence only delays a grace-expired teardown by at most one
	// sweep interval — fine for a window measured in tens of seconds to minutes,
	// and it keeps the sweep's per-tick lock-manager footprint off the hot drain
	// path at 1M scale (where LockManager is already the dominant wait). Default
	// 6 → with Interval=5s that's a sweep ~every 30s. 0/1 = every tick.
	OrphanSweepEvery int
	orphanSweepTick  uint64 // monotonic per-Reaper tick counter for the throttle

	// Shards is the control pod's CURRENT sweep range, read lock-free each tick
	// via Shards.Snapshot(). A *ShardSet so the Resharder can swap it at runtime
	// on membership change; the reaper picks up the new range on its next tick.
	Shards *ShardSet

	// OnSwept is an optional metrics observer: called with (sweep, n) after a sweep
	// that acted on n rows (n>0). nil → no-op (the generic reaper stays metrics-free;
	// the engine wires this to the OTel sweeper counter). Off the hot path.
	OnSwept func(sweep string, n int64)
}

// reportSwept fires the OnSwept observer if wired and n>0 (a tiny helper so each call site
// stays one line and nil-safe).
func (r *Reaper) reportSwept(sweep string, n int) {
	if r.OnSwept != nil && n > 0 {
		r.OnSwept(sweep, int64(n))
	}
}

func NewReaper(repo ReaperRepo) *Reaper {
	// Reaper is a backstop: the cascade trigger already calls
	// schedule_eligible on every is_ready flip, so the only rows the
	// trigger misses are crash-recovered ones (worker died) or rows
	// whose deps converged on a different pod's tx (or — pre-fix —
	// rollup-having roots whose enqueue raced; that path now also
	// goes through schedule_eligible).
	//
	// Cadence:
	//   Interval=5s        while we're finding work to do — fast
	//                      enough that requeue tail latency stays low,
	//                      slow enough that the requeue_failed_and_
	//                      pending O(unready rows) scan doesn't burn
	//                      CPU at 1M scale.
	//   IdleInterval=10s   when ticks have been empty. Keeps the
	//                      backstop responsive for the genuinely-stuck
	//                      cases (cascade trigger missed an enqueue,
	//                      worker died) without paying full 5s cost
	//                      every tick. Was 30s; that left a stuck
	//                      rollup-root invisible for up to 30s after
	//                      the last activity ended.
	// BatchSize bounds the number of rows reap_stale_work and
	// requeue_failed_and_pending lock + process per BATCH. Both run
	// inside a single transaction with FOR UPDATE SKIP LOCKED on
	// resources/work_queue rows; bigger batches mean longer lock-
	// holding times, which under multi-pod load piles up against
	// drainers also touching those rows. Was 5000 — at multi-batch
	// dev workloads (200+ roots, 700k+ resources) that produced 2+
	// minute on-CPU calls because the inner schedule_eligible step
	// scans resource_deps for every candidate. 500 keeps each BATCH
	// under a second so locks rotate quickly through the pool of
	// concurrent reapers + drainers. The tick BURSTS batches back-to-
	// back (see tick) until the backlog is drained, so a small batch
	// no longer means a slow drain — it bounds the per-batch lock-hold,
	// not the rows cleared per tick.
	return &Reaper{
		Repo: repo,
		// StaleAfter is the single source of the lease-expiry window: a claimed
		// task whose heartbeat stops for this long is reaped + requeued. 30s is
		// the CRASH backstop — a pod that dies without SIGTERM (OOM-kill, node
		// loss) can't run the shutdown claim-release (cmd/converge releases
		// its claims on a graceful stop, so a rollout hands work off in
		// milliseconds, not via this window); 30s bounds how long that crashed
		// pod's in-flight rows sit unclaimable. Safe to be this short because a
		// LIVE task is heartbeated every HeartbeatEvery (default 5s, loop.go), so
		// it refreshes ~6× inside the window and is never falsely reaped mid-run;
		// only a genuinely dead/silent claim crosses 30s. The engine overrides it
		// from SWEEPER_STALE_AFTER when that env is set (keep it comfortably above
		// HeartbeatEvery if you raise the heartbeat cadence).
		StaleAfter: DefaultSweeperStaleAfter,
		// Reactor deliveries re-arm FAST on failure (a failed upload/POST should
		// retry in seconds), and the dispatcher heartbeats its in-flight claims, so
		// this short window only catches truly dead/failed deliveries. Must exceed
		// ReactorDispatcher.HeartbeatEvery (10s) by a margin.
		LifecycleStaleAfter: 60 * time.Second,
		RetryAfter:          5 * time.Second,
		// Abandoned-task GC window: a row no worker claimed for a full day is
		// dropped. Long on purpose — see UnclaimedDeleteAfter. Overridable from
		// SWEEPER_UNCLAIMED_DELETE_AFTER; 0 disables the pass.
		UnclaimedDeleteAfter: DefaultSweeperUnclaimedDeleteAfter,
		// Orphan-grace sweep runs ~every 6th tick (≈30s at Interval=5s): grace
		// windows are tens of seconds+, delete_after is absolute, so a coarse
		// cadence costs at most one interval of teardown latency while keeping the
		// sweep's lock-manager probe off every hot-path tick at 1M scale.
		OrphanSweepEvery: 6,
		Shards:           NewShardSet(AllShards()),
		Interval:         5 * time.Second,
		IdleInterval:     10 * time.Second,
		BatchSize:        500,
	}
}

func (r *Reaper) Run(ctx context.Context) error {
	// No work_ready NOTIFY: the reaper is a failsafe backstop, so its recovered
	// re-pends are picked up by the worker's own failsafe poll (the recovery
	// path is "eventually", not latency-critical). The cascade/drain path that
	// IS latency-critical is woken by the drainer's work_ready.
	// repollOnWork=false: the tick already BURSTS its batches internally (see
	// tick) until the backlog is drained, so it needs no pollLoop-level re-poll.
	// Keeping it false also paces the once-per-tick recount_inflight self-heal at
	// Interval instead of spinning it per batch through a multi-batch drain.
	// nil wakeup: time-driven failsafe with no NOTIFY source — timer alone.
	// hotWindow=0: paced re-check cadence, no post-work hot polling.
	pollLoop(ctx, "reaper", r.Interval, r.IdleInterval, 0, false, nil, r.tick)
	return nil
}

// tick drains the reaper's two backstop ops in a BURST, then self-heals the
// per-kind cap tally once. The burst loops reap + requeue batches back-to-
// back until BOTH come back short of BatchSize: a saturated batch
// (== BatchSize) is the signal there's more of that work queued than one
// batch can hold, so we take the next immediately instead of waiting out the
// pollLoop Interval. This is what stops a backlog from starving — at
// thousands of stale/lagging rows against a deliberately small BatchSize,
// one-batch-per-5s-tick would take hours to clear; bursting clears every
// currently-eligible row in a single tick. A short batch ends the burst (we
// drained everything eligible), and pollLoop's paced Interval/IdleInterval
// then governs the re-check for newly-eligible rows.
//
// Both sub-ops are wrapped in retryOnDeadlock per batch. requeue's
// schedule_eligible writes work_queue with ON CONFLICT, which can deadlock
// against concurrent drainer cascade-trigger writes under load — residual
// cycles between schedule_eligible and the drainer's coalesced status UPDATE
// surface as SQLSTATE 40P01. Retry handles them transparently.
func (r *Reaper) tick(ctx context.Context) (bool, error) {
	// Snapshot the range ONCE per tick so every sweep works the same shards even
	// if a reshard lands mid-tick. Empty range (owns nothing yet) → the repo's
	// shardBounds yields (0,-1) → every sub-op no-ops cheaply.
	shards := r.Shards.Snapshot().Shards

	// The throughput core: recover stale claims + requeue eligible rows, bursting
	// while batches come back full. Its error is FATAL to the tick (the queue's
	// recovery path); the periodic sweeps below are recovery janitors whose errors
	// are non-fatal (they retry next tick).
	didWork, err := r.reapAndRequeueBurst(ctx, shards)
	if err != nil {
		return didWork, err
	}

	r.recountInflight(ctx, shards)
	r.reapStaleLifecycle(ctx, shards)
	r.gcUnclaimedWork(ctx, shards)
	r.sweepExpiredOrphans(ctx, shards)
	didWork = r.sweepDeletable(ctx, shards) || didWork
	return didWork, nil
}

// reapAndRequeueBurst is the reaper's throughput core: recover stale claims
// (ReapStaleWork) and requeue eligible failed/pending rows (RequeueFailedAndPending),
// re-bursting while EITHER batch comes back full so a backlog drains in one tick
// instead of waiting out pollLoop. Both writes can 40P01-deadlock against the
// drainer's cascade writes under load, so each retries on deadlock. Returns
// whether it moved any rows; its error is fatal (the queue recovery path).
func (r *Reaper) reapAndRequeueBurst(ctx context.Context, shards []int16) (bool, error) {
	didWork := false
	for {
		reaped, err := retryOnDeadlock(ctx, "reaper reap", func(ctx context.Context) (int, error) {
			return r.Repo.ReapStaleWork(ctx, r.StaleAfter, r.BatchSize, shards)
		})
		if err != nil {
			return didWork, fmt.Errorf("reap stale work: %w", err)
		}
		if reaped > 0 {
			slog.Debug("reaper recovered stale work", "count", reaped)
			r.reportSwept("reaper_stale", reaped)
		}

		rescheduled, err := retryOnDeadlock(ctx, "reaper requeue", func(ctx context.Context) (int, error) {
			return r.Repo.RequeueFailedAndPending(ctx, r.RetryAfter, r.BatchSize, shards)
		})
		if err != nil {
			return didWork || reaped > 0, fmt.Errorf("requeue failed and pending: %w", err)
		}
		if rescheduled > 0 {
			slog.Debug("reaper rescheduled rows", "count", rescheduled)
			r.reportSwept("reaper_rescheduled", rescheduled)
		}

		didWork = didWork || reaped > 0 || rescheduled > 0

		// Re-burst only while a batch came back FULL. A short batch on BOTH ops
		// means we took every currently-eligible row, so end the burst and let
		// pollLoop pace the next re-check.
		if reaped < r.BatchSize && rescheduled < r.BatchSize {
			return didWork, nil
		}
	}
}

// recountInflight self-heals the per-kind concurrency-cap tally over this
// reaper's shard range, ONCE after the burst (not per batch — it's a periodic
// self-heal): reclaim worker hint partials and write the authoritative per-range
// count. This keeps the claim's optimistic +N honest, so drain/reap/re-point
// carry no counter bookkeeping. No-op (cheap) when no kind has a cap. Non-fatal:
// a failed recount just heals next tick.
func (r *Reaper) recountInflight(ctx context.Context, shards []int16) {
	if err := r.Repo.RecountInflight(ctx, shards); err != nil {
		slog.Warn("reaper recount in-flight", "err", err)
	}
}

// reapStaleLifecycle frees lifecycle_outbox claims whose reactor dispatcher died
// or whose delivery FAILED, re-arming them for another at-least-once attempt.
// Uses its own LifecycleStaleAfter window so a failed upload retries promptly;
// safe because the dispatcher heartbeats live claims, so only dead/failed
// deliveries go stale. The dispatcher's own poll is the primary delivery path;
// this is recovery only — non-fatal.
func (r *Reaper) reapStaleLifecycle(ctx context.Context, shards []int16) {
	stale := r.LifecycleStaleAfter
	if stale <= 0 {
		stale = r.StaleAfter // fall back to the work window if unset
	}
	if err := r.Repo.ReapStaleLifecycle(ctx, stale, r.BatchSize, shards); err != nil {
		slog.Warn("reaper reap stale lifecycle", "err", err)
	}
}

// gcUnclaimedWork hard-deletes rows unclaimed past UnclaimedDeleteAfter (default
// 24h) — tasks no worker ever picked up because their kind has no worker.
// Distinct from the reap (which RECOVERS a dead claim); this DROPS a
// never-claimed row that would otherwise sit in the pending index forever,
// re-scanned by every claim for that kind. DeleteUnclaimedWork carries its own
// zero-cost idle gate, so this is ~free on a fleet whose pending rows all get
// claimed promptly. Drains up to BatchSize per tick (the cadence is days, so one
// batch/tick clears any real backlog fast); non-fatal. Disabled when the window is 0.
func (r *Reaper) gcUnclaimedWork(ctx context.Context, shards []int16) {
	if r.UnclaimedDeleteAfter <= 0 {
		return
	}
	deleted, err := r.Repo.DeleteUnclaimedWork(ctx, r.UnclaimedDeleteAfter, r.BatchSize, shards)
	if err != nil {
		slog.Warn("reaper delete unclaimed work", "err", err)
	} else if deleted > 0 {
		slog.Info("reaper deleted abandoned unclaimed work", "count", deleted)
		r.reportSwept("reaper_unclaimed_deleted", deleted)
	}
}

// sweepExpiredOrphans tears down composer-dropped children whose orphan-grace
// window elapsed without a re-emit: finalizer kinds escalate to soft-delete
// (their Deleter runs), leaf kinds are hard-deleted. The grace deadline (per-kind
// orphan_grace_secs) is baked into resources.frozen_until by the composer, so the
// sweep takes no time arg. THROTTLED to every Nth tick (OrphanSweepEvery): even
// its zero-cost idle gate takes a lock-manager slot, and at 1M scale LockManager
// is the dominant wait shared with the hot drain path — running an empty-index
// probe on every tick (× every control pod) is pure contention for a janitor whose
// deadline is absolute and tens-of-seconds coarse. A coarse cadence delays a
// teardown by at most one sweep interval. Non-fatal.
func (r *Reaper) sweepExpiredOrphans(ctx context.Context, shards []int16) {
	r.orphanSweepTick++
	every := uint64(r.OrphanSweepEvery)
	if every < 1 {
		every = 1
	}
	if r.orphanSweepTick%every != 0 {
		return
	}
	swept, err := r.Repo.SweepExpiredOrphans(ctx, r.BatchSize, shards)
	if err != nil {
		slog.Warn("reaper sweep expired orphans", "err", err)
	} else if swept > 0 {
		slog.Info("reaper swept expired orphans", "count", swept)
		r.reportSwept("orphans_swept", swept)
	}
}

// sweepDeletable is the level-triggered backstop for reverse-dependency cascade
// delete: remove any marked node whose finalizers + owned children + dependents
// are all gone. The marked teardown tree collapses inward one layer per tick
// (removing a leaf unblocks its parent next tick), converging bottom-up. Its own
// zero-cost idle gate makes this ~free when no teardown is in flight; non-fatal.
// Returns whether it removed any resource (folds into the tick's didWork).
func (r *Reaper) sweepDeletable(ctx context.Context, shards []int16) bool {
	removed, err := r.Repo.SweepDeletable(ctx, r.BatchSize, shards)
	if err != nil {
		slog.Warn("reaper sweep deletable", "err", err)
		return false
	}
	if removed > 0 {
		slog.Info("reaper removed torn-down resources", "count", removed)
		r.reportSwept("resources_removed", removed)
		return true
	}
	return false
}
