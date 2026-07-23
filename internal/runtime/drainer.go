package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Drainer pops work_outbox rows in batches and applies them to
// resources + work_queue via the plpgsql drain_outbox_batch function.
type Drainer struct {
	Repo      DrainerRepo
	Listener  Listener // outbox_ready + rollup_recheck wake subscriptions
	Interval  time.Duration
	BatchSize int

	IdleInterval time.Duration
	HotWindow    time.Duration

	// Shards is the control pod's CURRENT drain range, read lock-free each tick
	// via Shards.Snapshot(). A *ShardSet so the Resharder can swap it at runtime
	// on membership change; each band recomputes its sub-slice from the new
	// snapshot on its next tick (snapshot-pointer-cached so an unchanged range
	// costs one pointer compare).
	Shards      *ShardSet
	WorkerCount int
}

func NewDrainer(repo DrainerRepo, listener Listener) *Drainer {
	// BatchSize bounds how many work_outbox rows one tick of
	// drain_outbox_batch processes. The function flips is_ready on
	// many resources in a single coalesced UPDATE, which fires the
	// statement-level cascade trigger — that trigger's NOT EXISTS
	// scans + schedule_eligible recursion run once per batch.
	// Larger batches mean the cascade trigger spans more shards
	// per fire, which under multi-pod load (12 drainer goroutines
	// across 3 control pods) created lock convoys: each tick took
	// seconds because of cross-drainer row-lock waits, drainers
	// piled up faster than they drained. 100 keeps each batch's
	// cascade-trigger blast radius small enough that drainers
	// rotate through their shards without contending. The drainer
	// loops through multiple ticks back-to-back (see runSingle's
	// inner loop), so total throughput is unchanged — we just
	// hold locks for shorter periods per tick.
	//
	// MEASURED OPTIMUM — do not change in either direction. After the
	// deterministic-lock-order fix (drain_outbox_batch,
	// apply_value_flows_for_dependents, requeue_for_resync all lock resources
	// in ascending-id order) removed the DEADLOCKS, both larger and smaller
	// batches were A/B-tested at N=5 (18k resources, 3 control pods). 100 won
	// on essentially every DB metric; both neighbours regressed, for OPPOSITE
	// reasons:
	//
	//   BatchSize=500 (too big): each productive batch holds its cross-shard
	//   dependent locks longer — substitute-pass mean 50ms→130ms, drain total
	//   42s→49s, AppendOutbox mean 1.07ms→1.66ms, peak backlog 3.4k→4.6k.
	//
	//   BatchSize=50 (too small): the cascade trigger (hence schedule_eligible
	//   + its eligibility-gate scans) fires ~2× as often — schedule_eligible
	//   mean 9.7ms→15.9ms, AppendOutbox mean 1.07ms→2.21ms (2× worse, from the
	//   extra work_queue ON CONFLICT churn contending with worker claims),
	//   drain total 42s→49s.
	//
	// Wall-clock drain was ~5s in all three (it's bounded by total work + the
	// synchronous 68MB root inserts), so the DB-work and contention metrics are
	// the tie-breaker, and 100 minimizes them. The call count is dominated by
	// EMPTY hot-window polls (25ms cadence for HotWindow after the last drain),
	// not productive batches, so raising the batch can't buy a "fewer calls"
	// win — it only lengthens lock holds; lowering it only multiplies trigger
	// fires. 100 balances lock-hold time against cascade-fire frequency.
	//
	// The drainer shares the worker dispatcher's backpressure model (pollLoop):
	//
	//   - WORK-CONSERVING: a tick that drained rows re-polls IMMEDIATELY (zero
	//     delay — pollLoop's work path), so a backlog drains back-to-back at
	//     full speed and the re-poll right after a drain catches any rows that
	//     landed DURING it. That makes a mid-drain wake un-missable without
	//     depending on a NOTIFY surviving the busy window.
	//
	//   - NOTIFY breaks the idle sleep; it does NOT pace the busy loop. The DB
	//     fires a coalesced pg_notify('outbox_ready') from AppendOutbox via
	//     notify_gated() — GLOBALLY rate-limited to ~1 per 50ms window across
	//     ALL backends (not per pod), and lock-free for the ~99.99% of appends
	//     inside the window (a single unlocked SELECT that returns early) — so
	//     an idle drainer wakes in ~one window. The gate runs per appended row
	//     but costs ~nothing on the 1M-append hot path; only ~20 calls/sec
	//     fleet-wide reach the actual pg_notify, so the async-notification
	//     queue lock stays uncontended (a Go-side per-completion pg_notify
	//     storm serialized 1000+ backends on that lock — the prior regression).
	//     See notify_gated() in db/migrations/00001_schema.sql.
	//
	//   - Interval (25ms) is the HOT-WINDOW pace: for HotWindow (5s) after the
	//     LAST drain, empty ticks keep polling at 25ms so a straggler whose
	//     outbox_ready was coalesced/dropped is caught within ~25ms, not after
	//     the failsafe. IdleInterval=2s is the failsafe ONLY once the queue has
	//     been empty for the whole hot window. The outbox has no reaper backstop
	//     (only the drainer applies it), so 2s bounds worst-case rollup latency
	//     on a dropped notify in the cold state; the wake + hot window make the
	//     active/just-active state sub-window.
	//
	//   - HotWindow (5s) is sized to outlast a fan-out's settle: after a root's
	//     children start completing, more outbox rows (sibling completions, the
	//     root's own rollup re-pend) keep arriving for seconds. Staying hot at
	//     25ms across that span means the LAST child's rollup is drained reactively
	//     even if its notify was the one the gate happened to coalesce away.
	//
	// BatchSize bounds the cascade-trigger blast radius per tick (above);
	// tickUntilEmpty still bursts batches back-to-back within a tick.
	return &Drainer{
		Repo:         repo,
		Listener:     listener,
		Interval:     25 * time.Millisecond,
		IdleInterval: 2 * time.Second,
		HotWindow:    5 * time.Second,
		BatchSize:    100,
		Shards:       NewShardSet(AllShards()),
		// WorkerCount = drainer goroutines (bands) PER control pod; each owns
		// a disjoint sub-slice of the pod's shards. 4 (→ 12 bands fleet-wide
		// across 3 control pods) is tuned for drain LATENCY. MEASURED at N=5
		// (18k resources) against WorkerCount=2 (→ 6 bands): halving the bands
		// cut drainer-vs-drainer contention (lockwait peak 8→5) and total
		// drain-poll CPU (42s→27s, mostly fewer empty hot-window polls), BUT
		// drained the fan-out backlog SLOWER (peak-clear ~3s→~4s) and raised
		// per-op latency across the board (drain mean 6.9→9.6ms, AppendOutbox
		// 1.07→1.84ms, schedule_eligible 9.7→18.2ms) because each of the
		// fewer, wider bands processes a larger cross-shard span per batch.
		// Since the deterministic-lock-order fix already drove deadlocks to 0,
		// there's no contention problem left to solve by shedding parallelism —
		// so keep the higher band count for the faster wall-clock drain.
		WorkerCount: 4,
	}
}

func (d *Drainer) Run(ctx context.Context) error {
	// A FIXED number of band goroutines, independent of how many shards this pod
	// currently owns: with dynamic sharding the owned range changes at runtime,
	// so each band derives its sub-slice from the CURRENT snapshot every tick
	// (bandShards) rather than from a slice fixed at startup. A band whose share
	// is momentarily empty (pod owns fewer shards than bands, or owns none yet)
	// just no-ops its tick. Capped at NumShards (no point having more bands than
	// shards in the whole space).
	workers := d.WorkerCount
	if workers < 1 {
		workers = 1
	}
	if workers > NumShards {
		workers = NumShards
	}

	// The drainer's drain_outbox_batch fires the cascade trigger, which calls
	// schedule_eligible → notify_gated('work_ready') in the DB, so workers are
	// woken from there (gated, lock-free) — no Go-side work_ready emit here.

	// One wakeup chan per band (fixed). The single pod-level listener broadcasts
	// to all of them on each 'outbox_ready' NOTIFY (wake-all: the NOTIFY carries
	// no shard, and a spurious wake on a band whose share is empty costs exactly
	// one range-pruned DrainOutboxBatch probe that returns 0 — the same probe
	// the idle poll would make). buffered-1 + non-blocking send coalesces,
	// mirroring the dispatcher's Trigger().
	wakeups := make([]chan struct{}, workers)

	// Run blocks until ctx is cancelled: it launches the band goroutines + the
	// listener (all on ctx), then `defer wg.Wait()` holds Run here until every
	// one of them has returned. So on shutdown, ctx cancellation unblocks the
	// bands' pollLoop AND the listener's WaitForNotification together, each
	// releases its conn, and wg.Wait joins them all before Run returns — which
	// is what makes the pool safe for ControlPlane to Close afterwards. (No
	// separate hbCtx: the dispatcher needs one because its Run blocks inline on
	// its own loop, so a child ctx is the only way to stop its helper
	// goroutines first; here Run launches-and-waits, so a single ctx + wg.Wait
	// stops and joins everything uniformly.)
	var wg sync.WaitGroup
	defer wg.Wait()

	for i := 0; i < workers; i++ {
		wakeup := make(chan struct{}, 1)
		wakeups[i] = wakeup
		wg.Add(1)
		go func(band int, w <-chan struct{}) {
			defer wg.Done()
			d.runSingle(ctx, band, workers, w)
		}(i, wakeup)
	}

	// Pod-level listener: one held LISTEN conn fans 'outbox_ready' out to every
	// band's wakeup. Runs on ctx + tracked in wg, so it lives for the drainer's
	// whole lifetime and is JOINED on shutdown. Pure latency optimisation over
	// the poll — a dead listener just degrades to the failsafe IdleInterval
	// sweep, never a stuck rollup, so it reconnects lazily and never fails Run.
	wakeAll := func() {
		// Coalescing send to every band (buffered-1, so a band that hasn't consumed
		// its last wake just keeps the pending one).
		for _, w := range wakeups {
			notify(w)
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.Listener.Listen(ctx, "drainer outbox_ready", outboxReadyChannel, wakeAll)
	}()
	// Second listener: the cascade fires 'rollup_recheck' when it queues a
	// straddled rollup root. Waking the bands here means the deferred recheck
	// (drain_rollup_rechecks, called at the end of each band's tick) runs within
	// one tick of the straddle instead of waiting for the failsafe poll — the
	// difference between a sub-second and a multi-second rollup on a straddle.
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.Listener.Listen(ctx, "drainer rollup_recheck", rollupRecheckChannel, wakeAll)
	}()
	return nil
}

// outboxReadyChannel is the LISTEN/NOTIFY channel the DB's notify_gated() fires
// from AppendOutbox when a worker appended a task result. Empty payload: the
// listener just wakes the drainer bands to re-drain.
const outboxReadyChannel = "outbox_ready"

// rollupRecheckChannel is fired DIRECTLY (ungated, per-statement — rare, only on
// a straddle) by cascade_on_ready_change when it queues a straddled rollup root
// in rollup_recheck. Empty payload: it just wakes the bands so the next tick's
// drain_rollup_rechecks re-pends the root at a fresh snapshot.
const rollupRecheckChannel = "rollup_recheck"

// scheduleRecheckBatch is how many armed children one drain_schedule_rechecks
// call schedules (OPT-A). Large on purpose — it's a plain schedule_eligible with
// NO cascade blast radius (unlike drain_outbox_batch, whose small BatchSize bounds
// the trigger fan-out), so a big batch is cheap and necessary: the drainer bursts
// these until the band's armed set is empty, and at a small batch the ~333k armed
// leaves of a wide compose trickled out over thousands of ticks. Band-scoped, so
// still a bounded statement that doesn't recreate the ~1M deadlock convoy.
const scheduleRecheckBatch = 10000

// bandShards returns band `i` of an `n`-way contiguous split of shards — the
// i-th sub-slice when [0..len) is range-partitioned into n bands with the same
// integer math as shardutil.ShardsForPod. Returns empty when the band collapses
// (fewer shards than bands → high-index bands get nothing) or shards is empty.
// Deterministic given (shards, n, i), so a band always owns the same contiguous
// piece of the current range — store.shardBounds' BETWEEN pruning holds.
func bandShards(shards []int16, n, i int) []int16 {
	if n <= 0 || i < 0 || i >= n || len(shards) == 0 {
		return nil
	}
	start := i * len(shards) / n
	end := (i + 1) * len(shards) / n
	return shards[start:end]
}

// runSingle drives one drainer band. Each tick it reads the pod's CURRENT shard
// snapshot and carves out THIS band's contiguous sub-slice (bandShards). The
// snapshot pointer is cached so an unchanged range (the common case) recomputes
// the sub-slice only when the Resharder actually swaps — one atomic load + one
// pointer compare per tick. A reshard that shrinks/grows/empties the range is
// picked up transparently on the next tick.
func (d *Drainer) runSingle(ctx context.Context, band, workers int, wakeup <-chan struct{}) {
	var (
		cached   *ShardSnapshot
		myShards []int16
	)
	pollLoop(ctx, "drainer", d.Interval, d.IdleInterval, d.HotWindow, true, wakeup,
		func(ctx context.Context) (bool, error) {
			if snap := d.Shards.Snapshot(); snap != cached {
				cached = snap
				myShards = bandShards(snap.Shards, workers, band)
			}
			if len(myShards) == 0 {
				return false, nil // this band owns nothing right now
			}
			return d.tickUntilEmpty(ctx, myShards)
		})
}

// tickUntilEmpty drains batches back-to-back until the outbox tick
// returns 0. Reports work=true if any rows drained — pollLoop uses
// that to decide busy vs idle interval. Holding the inner loop here
// (vs going through pollLoop one batch at a time) gives the
// "burst-drain a wide queue then sleep" behaviour the drainer needs
// to keep up under load: pollLoop sleeps Interval between iterations,
// so the 25ms cadence outer loop bursts N batches per tick before
// pacing.
//
// Each batch is retried up to 3 times on SQLSTATE 40P01. The cascade
// trigger inside drain_outbox_batch issues work_queue ON CONFLICT
// writes that cross lock orders with concurrent drainers' resource
// UPDATEs; brief deadlocks are expected under multi-drainer load
// and the right response is "be the victim, retry" rather than
// failing the tick and waiting for the next interval.
func (d *Drainer) tickUntilEmpty(ctx context.Context, shards []int16) (bool, error) {
	drained := 0
	for {
		n, err := retryOnDeadlock(ctx, "drainer", func(ctx context.Context) (int, error) {
			return d.Repo.DrainOutboxBatch(ctx, d.BatchSize, shards)
		})
		if err != nil {
			return drained > 0, fmt.Errorf("drain outbox: %w", err)
		}
		drained += n
		if n == 0 {
			// Drained to empty. Workers were already woken by the cascade
			// trigger's schedule_eligible → pg_notify('work_ready') inside
			// drain_outbox_batch (direct, un-gated, commit-atomic with the
			// enqueue) — no Go-side wake needed here.
			break
		}
	}

	// DEFERRED ROLLUP RECHECK: now that this band's outbox is drained (every
	// settling batch from this tick has COMMITTED), re-pend any rollup root the
	// cascade queued in rollup_recheck because a concurrent batch's snapshot
	// straddled its last descendants. This call runs in a FRESH transaction, so
	// schedule_eligible's descendant gate now sees 0 lagging and the root pends —
	// closing the straddle in ms instead of the reaper's 5–10s poll. Empty (and a
	// single cheap range-pruned probe) on the common no-straddle path.
	rechecked, err := retryOnDeadlock(ctx, "drainer rollup recheck", func(ctx context.Context) (int, error) {
		return d.Repo.DrainRollupRechecks(ctx, d.BatchSize, shards)
	})
	if err != nil {
		return drained > 0, fmt.Errorf("drain rollup rechecks: %w", err)
	}

	// DEFERRED POST-COMPOSE SCHEDULE (OPT-A): schedule the children a wide compose
	// armed in schedule_recheck. Replaces the post-commit ScheduleEligible(~1M) that
	// deadlocked vs the drainers and dropped its wake. BURSTS in scheduleRecheckBatch
	// chunks until the band's armed set is drained: unlike drain_outbox_batch (whose
	// small BatchSize bounds the cascade-trigger blast radius), this is a plain
	// schedule_eligible with no cascade fan-out, so a large batch is cheap AND must
	// be large — at BatchSize=100 the 333k armed leaves trickled out over thousands
	// of ticks (7m tail). Each chunk is still a bounded, band-scoped statement that
	// doesn't form the deadlock convoy the single ~1M call did. A full batch means
	// more armed rows remain → take the next immediately; a short batch ends the
	// burst and the next tick's cadence governs re-check. retryOnDeadlock guards a
	// rare residual collision.
	scheduled := 0
	for {
		n, err := retryOnDeadlock(ctx, "drainer schedule recheck", func(ctx context.Context) (int, error) {
			return d.Repo.DrainScheduleRechecks(ctx, scheduleRecheckBatch, shards)
		})
		if err != nil {
			return drained > 0 || rechecked > 0 || scheduled > 0, fmt.Errorf("drain schedule rechecks: %w", err)
		}
		scheduled += n
		if n < scheduleRecheckBatch {
			break
		}
	}
	return drained > 0 || rechecked > 0 || scheduled > 0, nil
}
