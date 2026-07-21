package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/pkg/drainctx"
)

// Dispatcher default cadences — the SINGLE source of truth for these knobs.
// NewWithBrokerID seeds a fresh Dispatcher with them, cmd/converge's
// Config.applyDefaults fills the same values so the reported cluster config
// shows what actually runs, and the worker node overrides each from its env var
// only when set. Keep the two in lockstep by referencing THESE constants, never
// re-typing the literals.
const (
	// DefaultHeartbeatEvery is the claimed-task LEASE renewal cadence (keeps a
	// live task from looking stale to the reaper).
	DefaultHeartbeatEvery = 5 * time.Second
	// DefaultRequireWithin bounds how stale a worker's per-task liveness attestation may
	// be for the dispatcher to keep heartbeating a NON-LOCAL (forwarded mesh) task's lease.
	// Sized between 2×DefaultHeartbeatEvery (a couple of missed frames tolerated) and half
	// the reaper's StaleAfter (broker-clock skew can't cross the stale boundary): a silent
	// peer's forwarded lease goes stale within a bounded number of missed frames, never
	// indefinitely. It does NOT gate a LOCAL task (one whose post-Complete apply phase is
	// running on this pod's own goroutine — flipped local via markLocal): that lease is
	// heartbeated by the live goroutine itself (see inFlightRecord.local). A task still
	// PARKED awaiting a worker's Complete is non-local and gated here. A broker sets this;
	// the in-process path leaves it 0 (the executor IS the worker → every task refreshed).
	DefaultRequireWithin = 15 * time.Second
	// DefaultPollMin is the claim-poll floor (busy/hot cadence).
	DefaultPollMin = 50 * time.Millisecond
	// DefaultPollMax is the deep-idle claim-poll ceiling.
	DefaultPollMax = 30 * time.Second
)

// pair is one (kind, task_type) the dispatcher claims for. A kind opts
// into at most three:
//
//	reconcile -- the unified pipeline (compose? + work? + rollup?)
//	delete    -- run the kind's Deleter; on success removes the finalizer
//	operate   -- run a registered subresource verb
type pair struct {
	kind model.Kind
	// kindVersion is the web-API version this pair claims for. A worker advertises a
	// (kind, kindVersion, task_type) it serves; the dispatcher issues an exact-kindVersion
	// claim per pair, so a v2 worker's pairs only ever pull v2 rows. Strict-kindVersion:
	// every pair carries an explicit kindVersion (>= 1).
	kindVersion int
	taskType    store.TaskType

	// inFlight is this pair's current concurrent count. The dispatcher goroutine
	// INCREMENTS it when a claim lands; each finishing TASK goroutine DECREMENTS
	// its own pair directly (p.inFlight.Add(-1)) and then Triggers a re-sweep.
	// Atomic so that cross-goroutine decrement is race-free without a completion
	// channel — a finishing task can never block (no buffer to overflow), so the
	// number of live kinds is unbounded by construction. The free-slot budget is
	// `Dispatcher.maxParallel - inFlight.Load()`. The pod's per-pair ceiling
	// (maxParallel) is UNIFORM across every pair, so it lives once on the
	// Dispatcher rather than copied onto each pair (see Dispatcher.maxParallel).
	inFlight atomic.Int64
}

// Dispatcher is the pod's single work-claiming loop. It replaces the
// former one-goroutine-per-(kind,task_type) model: ONE goroutine
// round-robins the proven single-kind WorkQueueTakeBatch over every pair
// the pod runs, and ONE heartbeat goroutine refreshes the union of all
// in-flight task ids. This collapses ~3×kinds goroutines + timers +
// poll cadences into 1 + 1, while the claim SQL (per-kind equality probe
// + partition pruning + the per-kind global cap) stays byte-for-byte the
// hot path.
//
// Concurrency model — single-writer claims, atomic completions:
//   - The dispatcher goroutine is the ONLY INCREMENTER of every pair.inFlight
//     and the only broker, so per-kind free-slot accounting has no overfill:
//     limits are computed (atomic Load) and claims issued sequentially.
//   - Each finishing TASK goroutine DECREMENTS its own pair (atomic Add(-1))
//     and Triggers a re-sweep — no completion channel to size or overflow, so
//     the count of live kinds is unbounded. The heartbeat reads a locked
//     snapshot of the in-flight id set.
//
// The execution path (runOne → dispatch → runReaction → reactReconcile/
// reactDelete/reactOperate, or recordFailed) dispatches off the CLAIMED row's
// kind/task_type, never the pair, so the one set of methods on the embedded *Loop serves every
// pair.
type Dispatcher struct {
	// Loop bundles the shared execution deps (Pool/Repo/Registry/BrokerID)
	// the runOne… methods read; embedded so the dispatcher reuses Repo for
	// claims + heartbeats and BrokerID/Pool for the pod's single identity.
	*Loop

	HeartbeatEvery time.Duration
	// RequireWithin bounds how stale a worker's per-task liveness attestation may be
	// for the dispatcher to keep heartbeating that task's lease. A task whose worker
	// stopped attesting (silent connection or a wedged handler on a live one) past
	// this window drops out of the refresh set, so its heartbeat_at goes stale and
	// the reaper reclaims it — making heartbeat_at attest live-WORKER execution, not
	// mere broker liveness. Kept comfortably under the reaper's StaleAfter (≥ 2×) so
	// broker-clock skew can't cross it. 0 (the in-process default, where the executor
	// IS the worker) disables the gate: every in-flight task is refreshed.
	RequireWithin time.Duration
	// inFlight is the live heartbeat state for this pod's claimed tasks, created in
	// Run. The broker's WorkHeartbeat relay reaches it via Attest so a worker's
	// per-task liveness feeds the epoch-fenced heartbeat. Set once at Run start.
	inFlight *inFlightSet
	// heartbeatFailing is set by heartbeatLoop after enough consecutive
	// WorkQueueHeartbeat failures that the pod's in-flight leases are at risk of
	// crossing the reaper's StaleAfter and being FALSELY reclaimed (→ double
	// execution). While set, sweep() STOPS claiming NEW work — piling on more
	// leases this pod can't refresh only widens the blast radius. It clears on the
	// first successful heartbeat. blast-radius bound; the fast in-tick retry in
	// heartbeatLoop is the first line of defense (a single DB blip costs no beat).
	heartbeatFailing atomic.Bool
	PollMin          time.Duration
	// PollMax is the dispatcher's DEEP-IDLE backoff ceiling, shared by every
	// pair: the work_ready NOTIFY carries cascade-re-pend latency, so the poll is
	// only a failsafe sweeper. One pod-level knob (not per pair) — every pair
	// backs off to the same ceiling; a real per-pair need has never appeared.
	PollMax time.Duration
	// DrainGrace is the graceful in-flight drain window on shutdown: when Run's ctx
	// is cancelled, claiming stops but already-claimed tasks (and a broker's parked
	// dispatchStage awaiting a worker's Complete) finish for up to this long before
	// being cancelled. 0 → cancel in-flight at once with ctx.
	DrainGrace time.Duration
	// HotWindow keeps an idle dispatcher sweeping at PollMin for this long after
	// the last activity (claim/completion/wake) before the idle backoff is
	// allowed to grow toward PollMax. Bounds the cost of a coalesced/dropped
	// work_ready NOTIFY to one PollMin instead of the full failsafe backoff.
	HotWindow time.Duration

	// Shards is the pod's CURRENT claim range, read lock-free every sweep via
	// Shards.Snapshot(). A *ShardSet (not a plain []int16) so the Resharder can
	// swap the range at runtime when cluster membership changes; the dispatcher
	// picks up the new range on its next sweep with no restart.
	Shards *ShardSet
	// pairs holds *pair (not pair) so a finishing task goroutine can hold a stable
	// pointer to its pair's atomic counter even as the Run goroutine appends new
	// pairs (AddPairLive) and reallocates the backing array. Single-writer: only
	// the Run goroutine appends.
	pairs []*pair

	// ClaimGate, when non-nil, is consulted per (kind, kindVersion, task_type) each
	// sweep: a (kind, kindVersion) it rejects is NOT claimed this sweep. The BROKER tier
	// sets it to "is a worker for this (kind, kindVersion) connected?", so a broker
	// never pulls work it can't deliver (the rows stay worker_id IS NULL for
	// another broker / a later poll — no buffer fill, no stranding). A pinned kindVersion
	// whose worker fleet retired parks safely (visible via the zero-worker signal),
	// never mis-routes. nil → claim always (in-process default).
	ClaimGate func(model.Kind, int) bool

	// maxParallel is the pod's UNIFORM per-pair concurrency ceiling: the
	// dispatcher keeps every (kind, task_type) pair topped up to this many tasks,
	// and a pair that fills it is skipped (free==0) so it can't consume another
	// pair's headroom — the structural cross-kind starvation bound. One value for
	// the whole pod (every kind gets the same cap; a per-pair override has never
	// been needed), set once by SetMaxParallel before Run. 0 → treated as
	// unbounded per pair (the SQL global cap still applies); the broker always
	// sets a positive default.
	maxParallel int

	// newPairs carries (kind, task_type) pairs added AFTER Run started — a kind
	// whose manifest landed post-boot (AddPairLive). The Run goroutine drains it
	// into d.pairs each loop, keeping d.pairs single-writer (no lock on the
	// sweep's hot path). Buffered well above any realistic pair count (#kinds ×
	// #task-types) so AddPairLive never blocks.
	newPairs chan *pair

	// stopped is closed when Run returns, so AddPairLive can give up its send
	// instead of blocking forever on a full newPairs buffer after the drain
	// loop is gone (shutdown).
	stopped chan struct{}

	wakeup chan struct{}

	// inFlightPub publishes the pod's current total in-flight count for the
	// out-of-band ClusterMemberReporter (cluster view), WITHOUT touching the hot
	// claim path's single-writer invariant: only the Run goroutine ever
	// Stores it (once per sweep decision, lock-free), and InFlight() Loads it
	// from any goroutine. It is a DISPLAY mirror of totalInFlight(), not the
	// authoritative counter the sweep uses.
	inFlightPub atomic.Int64

	// pairCount is a lock-free mirror of len(d.pairs), bumped at every pair add
	// (AddPair at boot, drainNewPairs for a live add) so PairCount() is readable
	// from any goroutine without racing the Run goroutine's single-writer slice.
	// For tests/metrics ("is this kind being claimed yet?").
	pairCount atomic.Int64
}

// PairCount returns how many (kind, task_type) pairs the dispatcher is claiming.
// Lock-free; 0 means it claims nothing (e.g. a broker booted before any CRD was
// applied). For tests + the cluster view.
func (d *Dispatcher) PairCount() int { return int(d.pairCount.Load()) }

// Attest records that a live worker reported the given in-flight tasks' handlers
// still progressing — the broker's WorkHeartbeat relay calls it (having mapped each
// lease_token to its work_id). It refreshes each task's last-attested time so the
// heartbeat keeps its lease fresh; a task NOT attested within RequireWithin drops
// out of the heartbeat set and the reaper reclaims it. A no-op before Run starts
// (inFlight nil) or for ids this pod no longer holds.
func (d *Dispatcher) Attest(workIDs []uuid.UUID) {
	if d.inFlight != nil {
		d.inFlight.attest(workIDs)
	}
}

// InFlight returns the pod's last-published total in-flight task count. Safe
// to call from any goroutine (lock-free atomic Load); used by the
// ClusterMemberReporter to stamp the registry row. 0 until the first sweep.
func (d *Dispatcher) InFlight() int { return int(d.inFlightPub.Load()) }

// ReleaseClaims frees every work_queue row this pod still holds, so a surviving
// pod can re-claim the work IMMEDIATELY instead of waiting out the reaper's
// stale window. Call ONLY after Run has returned (the dispatcher is quiesced —
// no goroutine can re-claim or re-heartbeat after release): the shutdown path
// runs it between eng.Stop (joins Run) and Deregister. Returns the count freed.
// Best-effort: a DB error just falls back to the reaper backstop.
func (d *Dispatcher) ReleaseClaims(ctx context.Context) (int64, error) {
	return d.Repo.WorkQueueReleaseBroker(ctx, d.BrokerID)
}

// New constructs a pod-scoped Dispatcher with one stable BrokerID for the whole
// pod (the heartbeat/reaper guard is keyed on the pod, not per kind). The reaction
// executor is unset here — the caller installs it with SetDispatcher before Run (a
// broker its remote fanout; the in-process test harness its registry executor).
// Set the manifest cache with SetManifests and add pairs with AddPair before Run.
func New(pool *pgxpool.Pool) *Dispatcher {
	hn, _ := os.Hostname()
	if hn == "" {
		hn = "unknown"
	}
	return NewWithBrokerID(pool, fmt.Sprintf("%s-%d-%s", hn, os.Getpid(), uuid.New().String()[:8]))
}

// NewWithBrokerID is New with an explicit lease identity. The broker tier uses
// it so a broker's BrokerID (the value stamped at claim and checked by the
// AppendOutbox fence) is a stable, externally-chosen id rather than a fresh
// per-process one — letting the same logical broker be addressed across the
// cluster_members registry and the reaper.
func NewWithBrokerID(pool *pgxpool.Pool, brokerID string) *Dispatcher {
	// Construction helper: build the Loop's INTERFACE dependencies from the pool
	// once here. The Loop itself holds only DispatcherRepo / txBeginner / Listener
	// — no concrete store or pool leaks into the dispatch core. Dispatch is left
	// nil (the dispatcher() accessor defaults to the fail-closed noHandlerExecutor);
	// the caller MUST SetDispatcher before Run.
	return &Dispatcher{
		Loop: &Loop{
			Tx:       pool,
			Repo:     NewDispatcherRepo(store.New(pool)),
			Listener: NewPgxListener(pool),
			BrokerID: brokerID,
		},
		// The lease-renew tick that keeps a claimed task from looking stale to the
		// reaper. The worker node overrides it from HEARTBEAT_EVERY only when set.
		HeartbeatEvery: DefaultHeartbeatEvery,
		PollMin:        DefaultPollMin,
		// The deep-idle poll ceiling; the worker node overrides it from
		// WORKER_POLL_MAX only when that env is set.
		PollMax:   DefaultPollMax,
		HotWindow: 5 * time.Second,
		Shards:    NewShardSet(AllShards()),
		newPairs:  make(chan *pair, 256),
		stopped:   make(chan struct{}),
		wakeup:    make(chan struct{}, 1),
	}
}

// SetMaxParallel sets the pod's UNIFORM per-pair concurrency ceiling. Call once
// at boot (the broker does) before Run; the value applies to every pair the
// dispatcher serves, including those added live. 0 leaves it unbounded per pair
// (the SQL global cap still applies).
func (d *Dispatcher) SetMaxParallel(n int) { d.maxParallel = n }

// NOTE: the transient-failure poison-pill is enforced DB-SIDE (drain_outbox_batch
// gates on resources.failure_attempts vs kind_config.max_transient_attempts), not on
// the Dispatcher — a work_queue-local counter resets every retry cycle (the row is
// deleted + re-pended), so the cap could never be reached from here.

// SetManifests wires the kind_manifest cache the dispatch reads to select
// each claimed task's reactions (by Trigger+Emits). Required before Run — the
// reaction engine is the only execution path. Call before Run.
func (d *Dispatcher) SetManifests(cache *KindManifestCache) {
	d.Manifests = cache
}

// SetDispatcher swaps the reaction executor (a broker installs its remote fanout
// here so reactions ship to dumb workers instead of running in-process).
func (d *Dispatcher) SetDispatcher(exec model.StageDispatcher) {
	d.Dispatch = exec
}

// AddPair registers a (kind, kindVersion, task_type) for the dispatcher to claim (its
// concurrency ceiling is the pod-level d.maxParallel; the idle-backoff ceiling
// is the pod-level d.PollMax). Use BEFORE Run (boot wiring); for a kind that
// comes online after Run started, use AddPairLive.
func (d *Dispatcher) AddPair(kind model.Kind, kindVersion int, taskType store.TaskType) {
	// kindVersion is the kind's explicit web-API version (>= 1, from its manifest).
	// No normalization: a pair registered at (kind, 0) claims nothing — the omission
	// surfaces as no work, never a silent v1.
	d.pairs = append(d.pairs, &pair{kind: kind, kindVersion: kindVersion, taskType: taskType})
	d.pairCount.Add(1)
}

// AddPairLive registers a (kind, kindVersion, task_type) AFTER Run has started — used
// when a provider's Setup, retried in the background, finally succeeds so the
// kind starts being claimed with NO restart. Safe to call from any goroutine: it
// hands the pair to the Run goroutine (the sole writer of d.pairs) over a
// channel and wakes it, so the next sweep includes the new pair. Never blocks
// past shutdown: once Run has returned (d.stopped closed) there is no drainer,
// so the send is abandoned rather than parking the caller forever on a full
// buffer.
func (d *Dispatcher) AddPairLive(kind model.Kind, kindVersion int, taskType store.TaskType) {
	select {
	case d.newPairs <- &pair{kind: kind, kindVersion: kindVersion, taskType: taskType}:
		d.Trigger()
	case <-d.stopped:
		slog.Warn("dispatcher: dropping live pair; dispatcher stopped",
			"broker", d.BrokerID, "kind", kind, "kind_version", kindVersion, "task", taskType)
	}
}

// drainNewPairs folds any live-added pairs into the ring. Called ONLY by the
// Run goroutine, so reading and appending to d.pairs keeps its single-writer
// invariant — the sweep reads d.pairs with no lock. A pair whose (kind, kindVersion,
// task_type) is already in the ring is skipped: AddKindLive is fired once per
// succeeding provider, but a duplicate signal (a future double-ready, a provider
// re-yielding a kind) would otherwise double-claim and inflate the pod's
// concurrency for that (kind, kindVersion).
func (d *Dispatcher) drainNewPairs() {
	for {
		select {
		case p := <-d.newPairs:
			if d.hasPair(p.kind, p.kindVersion, p.taskType) {
				slog.Warn("dispatcher: ignoring duplicate live pair",
					"broker", d.BrokerID, "kind", p.kind, "kind_version", p.kindVersion, "task", p.taskType)
				continue
			}
			d.pairs = append(d.pairs, p)
			d.pairCount.Add(1)
			slog.Info("dispatcher: kind now serving (live)",
				"broker", d.BrokerID, "kind", p.kind, "kind_version", p.kindVersion, "task", p.taskType)
		default:
			return
		}
	}
}

// hasPair reports whether the ring already serves (kind, kindVersion, task_type). Run-
// goroutine-only (reads d.pairs without a lock), so call it only from the drain.
func (d *Dispatcher) hasPair(kind model.Kind, kindVersion int, taskType store.TaskType) bool {
	for i := range d.pairs {
		if d.pairs[i].kind == kind && d.pairs[i].kindVersion == kindVersion && d.pairs[i].taskType == taskType {
			return true
		}
	}
	return false
}

// Trigger wakes the dispatcher from its idle backoff (e.g. a same-pod
// cascade re-pend). Non-blocking and coalescing.
func (d *Dispatcher) Trigger() { notify(d.wakeup) }

// totalInFlight sums the per-pair counts. Dispatcher-goroutine-only, so
// no lock.
func (d *Dispatcher) totalInFlight() int {
	n := 0
	for i := range d.pairs {
		n += int(d.pairs[i].inFlight.Load())
	}
	return n
}

// totalCapacity is the pod's aggregate concurrency ceiling. Every pair shares
// the uniform d.maxParallel, so it's simply pairs × maxParallel.
func (d *Dispatcher) totalCapacity() int {
	return len(d.pairs) * d.maxParallel
}

// Run drives the continuous-fill dispatcher over all registered pairs.
//
// Invariant: each pair runs at most the pod-uniform d.maxParallel tasks
// concurrently, and the dispatcher keeps every pair topped up whenever it has
// eligible work. Each sweep:
//
//   - walks the pairs ring from a rotating cursor (fairness), and for each
//     pair with free slots issues the per-kind claim for up to its free
//     count, launching one goroutine per claimed task.
//   - whenever a slot frees (a finishing task decrements its pair and Triggers
//     a wake) it loops again right away to refill — it does NOT wait for the
//     rest of the cohort, so a slow task holds only its own slot.
//   - when a FULL ring sweep claims nothing AND nothing is in flight, it
//     cools off PollMin→PollMax; a freed slot or a `work_ready` NOTIFY (both
//     arrive as a `wakeup` Trigger) collapses the backoff back to PollMin.
//
// ONE heartbeat goroutine refreshes heartbeat_at for the union of all
// in-flight ids (across every pair), so every task's claim stays fresh for
// its whole lifetime regardless of which kind it is or how long it runs.
// Run drives the dispatcher until ctx is cancelled.
//
// GRACEFUL DRAIN: when ctx is cancelled (SIGTERM), the claim loop, heartbeat, and
// listener stop (no new work is claimed), but already-claimed tasks — and, for a
// BROKER, the parked dispatchStage awaiting a remote worker's Complete — keep
// running for up to DrainGrace so in-flight work lands its result instead of
// being abandoned + re-dispatched. A straggler past the grace is cancelled.
// DrainGrace 0 → in-flight cancelled at once with ctx. Run returns once the claim
// loop exits AND every in-flight task goroutine has drained (wg.Wait), bounded by
// the drain window, so no goroutine outlives it.
func (d *Dispatcher) Run(ctx context.Context) error {
	// In-flight tasks run under drainCtx (their ctx parent): it lingers DrainGrace
	// past ctx so running tasks — and a broker's parked dispatchStage awaiting a
	// worker's Complete — finish before being cancelled. The claim loop / heartbeat
	// / listener below still follow ctx (stop claiming at once).
	drainCtx, cancelDrain := drainctx.New(ctx, d.DrainGrace)
	defer cancelDrain()

	// An empty start is legal: every kind this pod runs had its Setup fail and
	// is being retried in the background (see ProviderRetrier). The loop parks
	// on wakeup and AddPairLive feeds it pairs as those Setups succeed — no
	// crash, no restart.
	if len(d.pairs) == 0 {
		slog.Info("dispatcher: starting with no pairs; awaiting live registration", "broker", d.BrokerID)
	}

	// in-flight id set drives the union heartbeat. Mutated by the dispatcher
	// (add on claim) and task goroutines (remove on finish) — locked. Published on
	// the Dispatcher so the broker's WorkHeartbeat relay can Attest into it (a
	// worker's per-task liveness gating the epoch-fenced heartbeat).
	inFlight := newInFlightSet(d.RequireWithin)
	d.inFlight = inFlight
	// Wire the reaction loop's apply-phase hook: when a compose's post-Complete apply
	// begins on this pod's goroutine, flip the task to local so the long apply isn't
	// reaped by the worker-attestation staleness gate.
	d.MarkLocal = inFlight.markLocal

	// A finishing task decrements its own pair (atomic) and Triggers a re-sweep —
	// there is no completion channel to size, so the number of live kinds is
	// unbounded by construction (a fixed-size completion buffer would overflow for
	// a claim-all broker whose kinds register live after boot).

	// wg tracks EVERY goroutine the dispatcher launches — sweep-launched task
	// goroutines (incl. deadline orphans, see runOne), the heartbeat, and the
	// LISTEN listener — so NONE outlives Run(). This is what makes
	// the broker Stop → wg.Wait → pool.Close safe: no dispatch, heartbeat, or
	// listener goroutine can still be touching the pool when it closes.
	//
	// Defer ORDER (LIFO): register the drain FIRST and the cancel SECOND, so on
	// return hbCancel() runs BEFORE wg.Wait() — the heartbeat/listener get the
	// stop signal, then we wait out every tracked goroutine (their DB ops run
	// under hbCtx/taskCtx, now cancelled, so they unblock promptly; the reaper
	// owns reclamation regardless).
	var wg sync.WaitGroup
	defer wg.Wait()

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()

	// Closed (before wg.Wait/hbCancel run, since defers are LIFO) the moment Run
	// returns, so a late AddPairLive abandons its send instead of blocking on a
	// buffer no one will drain.
	defer close(d.stopped)
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.heartbeatLoop(hbCtx, inFlight)
	}()

	// LISTEN for the DB's coalesced 'work_ready' wake so a freshly-enqueued
	// task collapses the idle backoff to PollMin immediately, instead of
	// waiting out the poll interval. Pure latency optimisation layered on the
	// poll: a missed/dropped notify just falls back to the next poll tick +
	// the reaper, never a stuck task — so the listener can reconnect lazily
	// and its failure is non-fatal. Tracked in wg + on hbCtx so it's signalled
	// and JOINED on shutdown, releasing its held conn before pool.Close.
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.Listener.Listen(hbCtx, "dispatcher work_ready", workReadyChannel, d.Trigger)
	}()

	idle := d.PollMin // current idle/no-work backoff
	t := time.NewTimer(d.PollMin)
	t.Stop()
	drainTimer(t)
	defer t.Stop()

	// hotUntil: while it's in the future, an idle dispatcher keeps re-sweeping
	// at PollMin (≈50ms) instead of letting the idle backoff grow toward pollMax
	// (30s). Refreshed by every claim, completion, and wake — i.e. for HotWindow
	// after the LAST sign of activity. This is the reactivity guarantee: a
	// freshly-pended row whose work_ready NOTIFY was coalesced/dropped is still
	// picked up within one PollMin during the window, not after the 30s
	// failsafe. Only a queue that has been genuinely quiescent for the whole
	// window cools to pollMax (where a delivered NOTIFY collapses it instantly).
	// time.Now() carries a monotonic reading, so the comparisons are skew-safe.
	hotUntil := time.Now()
	hot := func() { hotUntil = time.Now().Add(d.HotWindow) }

	cursor := 0
	for {
		// Fold in any kinds that came online since the last sweep (AddPairLive),
		// then recompute the derived ceilings: a live pair grows both the pod's
		// aggregate capacity and (potentially) the idle-poll ceiling. Cheap —
		// sums over a handful of pairs — and keeps the saturation/cool-off
		// decisions below correct as the ring grows.
		d.drainNewPairs()
		capacity := d.totalCapacity()
		pollMax := d.PollMax

		// Publish the freshly-reconciled in-flight count for the cluster-view
		// reporter. Single-writer (this goroutine), lock-free; a pure display
		// mirror — it never feeds back into the claim decision below.
		d.inFlightPub.Store(int64(d.totalInFlight()))

		// One fairness sweep: from the rotating cursor, claim for every pair
		// with free slots. Returns total claimed this sweep. The claim SQL uses the
		// claim ctx; launched tasks run under drainCtx (graceful-drain seam).
		claimed, err := d.sweep(ctx, drainCtx, &cursor, &wg, inFlight)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("dispatcher claim", "broker", d.BrokerID, "err", err)
			// fall through to a short backoff rather than hot-spinning on error
		}
		if claimed > 0 {
			idle = d.PollMin // work flowing — reset cool-off
			hot()            // refresh the hot window: stay reactive to siblings
			continue
		}

		// Empty sweep — nothing claimable right now. Three distinct waits:
		//
		//   FULLY FULL (inFlight == capacity): there is no free slot, so a
		//   re-sweep can't claim anything until one frees. Block PURELY on
		//   `wakeup` (plus ctx) — NO timer. This is the key not-busy-spin case:
		//   under saturation a `work_ready` NOTIFY that arrives while full would
		//   otherwise reset the backoff and spin the sweep every PollMin against
		//   zero free slots. The only thing that creates capacity — a finishing
		//   task — decrements its pair (atomic) and Triggers `wakeup`, so the
		//   refill claim happens then. (A spurious wakeup just re-loops; the next
		//   sweep is still empty while full and falls right back here, costing one
		//   in-memory ring walk, no DB claim and no timer spin.)
		if d.totalInFlight() >= capacity {
			select {
			case <-ctx.Done():
				return nil
			case <-d.wakeup:
				idle = d.PollMin
				hot() // a slot freed (or a NOTIFY) — stay hot to refill it
			}
			continue
		}

		// PARTIALLY BUSY (0 < inFlight < capacity): there are free slots but
		// nothing claimed them — wait a short PollMin so a freeing slot or a
		// newly-pended row (discovered only by the next sweep) is picked up
		// promptly, without busy-spinning.
		wait := idle
		if d.totalInFlight() > 0 {
			wait = d.PollMin
		} else if time.Now().Before(hotUntil) {
			// FULLY IDLE but inside the HOT WINDOW: there was activity within the
			// last HotWindow, so more work is likely imminent (a cascade re-pend,
			// the root's own rollup re-pend after its last child drained). Keep
			// probing at PollMin rather than cooling off, so a work_ready that was
			// coalesced/dropped costs ≤PollMin, not the full failsafe backoff.
			wait = d.PollMin
		}
		// FULLY IDLE past the hot window: cool off PollMin→pollMax; the poll is
		// then just the failsafe sweeper (≈30s) — a `work_ready` NOTIFY or a freed
		// slot (both Trigger `wakeup`) collapses it back to PollMin and re-sweeps.
		t.Reset(wait)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			// Only grow the backoff once idle AND past the hot window; inside the
			// window we re-swept at PollMin (wait above) and leave idle untouched.
			if d.totalInFlight() == 0 && !time.Now().Before(hotUntil) {
				idle *= 2
				if idle > pollMax {
					idle = pollMax
				}
			}
		case <-d.wakeup:
			// A finishing task (it decremented its own pair already) or a delivered
			// work_ready. Either way: re-sweep promptly and keep the window open.
			drainTimer(t)
			idle = d.PollMin
			hot()
		}
	}
}

// sweep walks the pairs ring once from *cursor, issuing the per-kind claim
// for each pair that has free slots, and launches a goroutine per claimed
// task. It advances *cursor by one so the next sweep starts at a different
// pair (round-robin fairness). Returns the total tasks claimed this sweep.
//
// Per-pair LIMIT = free slots (maxParallel − inFlight.Load()); the SQL budget
// CTE is the second, fleet-wide ceiling for capped kinds. Only this goroutine
// INCREMENTS pair.inFlight (the free computation and the increment are on the
// same goroutine, so overfill is impossible); finishing tasks decrement their
// own pair atomically.
func (d *Dispatcher) sweep(ctx, taskCtx context.Context, cursor *int, wg *sync.WaitGroup, inFlight *inFlightSet) (int, error) {
	// heartbeats are failing long enough that this pod's already-claimed leases
	// risk crossing the reaper's StaleAfter and being falsely reclaimed elsewhere
	// (→ double execution). DON'T claim MORE work we can't refresh — hold until the
	// heartbeat recovers (it clears the flag on the next success). Existing in-flight
	// tasks keep running; this only stops NEW claims, bounding the blast radius.
	if d.heartbeatFailing.Load() {
		return 0, nil
	}
	n := len(d.pairs)
	// No pairs yet — an all-pending start, before any AddPairLive lands. Nothing
	// to claim, and crucially the cursor advance below is `% n`, so returning
	// here avoids an integer divide-by-zero. The Run loop then parks on
	// wakeup until a live pair arrives.
	if n == 0 {
		return 0, nil
	}
	total := 0
	// Read the current shard range ONCE per sweep (one atomic load): every
	// pair's claim this sweep uses the same range, and a reshard mid-sweep just
	// takes effect on the next sweep. snap.Shards is contiguous, so the claim's
	// BETWEEN pruning holds.
	shards := d.Shards.Snapshot().Shards
	for off := 0; off < n; off++ {
		idx := (*cursor + off) % n
		p := d.pairs[idx]
		free := d.maxParallel - int(p.inFlight.Load())
		if free <= 0 {
			continue
		}
		// Optional claim gate: the broker sets this to "is there a connected
		// worker for this kind?" — so a broker never pulls work it can't deliver
		// (the rows stay unclaimed for another broker / a later poll). nil → claim
		// always (the in-process / single-pod default).
		if d.ClaimGate != nil && !d.ClaimGate(p.kind, p.kindVersion) {
			continue
		}
		tasks, err := d.Repo.WorkQueueTakeBatch(ctx, p.kind, p.kindVersion, p.taskType, d.BrokerID, free, shards)
		if err != nil {
			return total, fmt.Errorf("take batch %s/v%d/%s: %w", p.kind, p.kindVersion, p.taskType, err)
		}
		if len(tasks) == 0 {
			continue
		}
		// INCREMENT here on the single dispatcher goroutine (no overfill: free was
		// computed from the same goroutine just above). Each task DECREMENTS its own
		// pair in its completion defer.
		p.inFlight.Add(int64(len(tasks)))
		total += len(tasks)
		for _, task := range tasks {
			// local=false at claim: the task is about to be dispatched to a worker (local
			// or, via the mesh, a peer's) and PARKED awaiting its Complete. During that
			// phase the WORKER's WorkHeartbeat attestation keeps the lease fresh, so a task
			// that is never delivered/attested (a lost or undispatched-relay task) lapses
			// past requireWithin and the reaper reclaims + re-dispatches it — the self-heal
			// path. The record flips to local ONLY when this pod's own goroutine begins the
			// post-Complete apply (inFlight.markLocal, in the reaction loop), where the live
			// goroutine is the liveness proof and the long apply must not reap itself.
			inFlight.add(task.ID, task.ShardID, task.ClaimEpoch)
			wg.Add(1)
			go func(task store.WorkTask, p *pair) {
				defer wg.Done()
				defer func() {
					inFlight.remove(task.ID)
					// Decrement THIS task's pair (atomic — cross-goroutine safe) and
					// Trigger the dispatcher to re-sweep and refill the freed slot.
					// No Go-side drainer wake for the QUEUE: the task's AppendOutbox SQL
					// fires notify_gated('outbox_ready') itself (DB-side, gated,
					// lock-free) — a per-completion Go pg_notify storm was the
					// async-queue-lock regression. d.Trigger is a coalescing in-memory
					// wake of OUR OWN loop, not a DB notify, so it carries no such cost.
					p.inFlight.Add(-1)
					d.Trigger()
				}()
				// Execute under taskCtx (the DRAIN ctx), NOT the sweep/claim ctx: on a
				// graceful shutdown the claim loop's ctx is cancelled to stop pulling new
				// work, but already-claimed tasks (and, for a broker, the parked
				// dispatchStage awaiting a remote worker's Complete) keep running under
				// taskCtx until the drain budget elapses — so in-flight work finishes
				// instead of being abandoned mid-flight.
				d.runOne(taskCtx, task, wg)
			}(task, p)
		}
	}
	*cursor = (*cursor + 1) % n
	return total, nil
}

// heartbeatLoop refreshes heartbeat_at for the CURRENT union of all
// in-flight ids on each tick: it snapshots the live set every tick so tasks
// that start mid-interval are covered and finished ones are dropped. One
// heartbeat for the whole pod (all kinds), keyed on the pod's single
// BrokerID, so a long-running task's claim stays fresh and the reaper
// doesn't reclaim it as stale.
func (d *Dispatcher) heartbeatLoop(ctx context.Context, inFlight *inFlightSet) {
	t := time.NewTicker(d.HeartbeatEvery)
	defer t.Stop()
	// consecutive missed beats. The reaper's StaleAfter (default 30s) assumes
	// ~6 successful beats (HeartbeatEvery 5s) inside the window; a run of failed
	// beats erodes that margin and can let a peer FALSELY reclaim a live lease.
	// After heartbeatFailPauseAt consecutive failures we trip heartbeatFailing so
	// sweep() stops claiming NEW work (blast-radius bound); a success clears it.
	consecutiveFail := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Cover the tasks a live worker ATTESTED within RequireWithin, over their
			// ACTUAL shard span (not the dispatcher's current ownership range: a
			// reshard can move ownership while a task is still running, and the lease
			// must keep getting refreshed for every task this pod holds regardless of
			// where its shard now sits — see WorkQueueHeartbeat's contract). A task
			// whose worker went silent/hung is EXCLUDED here, so its heartbeat_at goes
			// stale and the reaper reclaims it. lo/hi = min/max attested-task shard.
			ids, epochs, lo, hi := inFlight.snapshot()
			if len(ids) == 0 {
				consecutiveFail = 0
				d.heartbeatFailing.Store(false)
				continue
			}
			// Retry WITHIN the tick so a single transient DB blip (a few-hundred-ms
			// pool-exhaustion spike) doesn't cost a whole beat — the reaper's margin
			// is measured in successful beats, so salvaging the beat is what keeps a
			// LIVE lease from crossing StaleAfter. Bounded so the retries never bleed
			// into the next tick.
			err := d.heartbeatOnce(ctx, ids, epochs, lo, hi)
			if err == nil {
				consecutiveFail = 0
				d.heartbeatFailing.Store(false)
				continue
			}
			if ctx.Err() != nil {
				return
			}
			consecutiveFail++
			slog.Warn("dispatcher heartbeat failed",
				"broker", d.BrokerID, "consecutive", consecutiveFail, "leases", len(ids), "err", err)
			if consecutiveFail >= heartbeatFailPauseAt && !d.heartbeatFailing.Swap(true) {
				slog.Error("dispatcher heartbeat degraded: pausing NEW claims to avoid false lease reclamation",
					"broker", d.BrokerID, "consecutive", consecutiveFail)
			}
		}
	}
}

// heartbeatFailPauseAt is the consecutive-failure count at which the dispatcher
// stops claiming new work. 2 missed beats ≈ 2×HeartbeatEvery of no refresh —
// well inside the reaper's StaleAfter margin, so we pause BEFORE a live lease is at
// risk, not after.
const heartbeatFailPauseAt = 2

// heartbeatBlipRetries / heartbeatBlipBackoff bound the in-tick retry of a failed
// heartbeat. Kept small so all retries complete well within one HeartbeatEvery.
const (
	heartbeatBlipRetries = 2
	heartbeatBlipBackoff = 250 * time.Millisecond
)

// heartbeatOnce refreshes the in-flight leases, retrying a transient failure a few
// times within the tick. Returns nil on the first success, or the last error
// after the bounded retries. ctx-cancel short-circuits (shutdown).
func (d *Dispatcher) heartbeatOnce(ctx context.Context, ids []uuid.UUID, epochs []int64, lo, hi int16) error {
	var err error
	for attempt := 0; attempt <= heartbeatBlipRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(heartbeatBlipBackoff):
			}
		}
		if err = d.Repo.WorkQueueHeartbeat(ctx, ids, epochs, lo, hi); err == nil {
			return nil
		}
	}
	return err
}

// workReadyChannel is the LISTEN/NOTIFY channel the DB's notify_gated() fires
// (from schedule_eligible / cascade / resync) when work_queue rows are enqueued.
// Empty payload: the shared listen() loop just calls Trigger() to wake an idle
// dispatcher immediately instead of waiting out the poll backoff. notify_gated()
// coalesces emissions to a bounded trickle regardless of enqueue volume.
const workReadyChannel = "work_ready"

// inFlightRecord is one claimed-but-unfinished task's heartbeat state: its shard
// (for BETWEEN pruning), its claim epoch (the fence the heartbeat carries so a
// reaped/re-issued lease can't be kept alive by its prior holder), the wall clock of
// the last worker liveness attestation for it, and whether it runs LOCALLY on this pod.
//
// local marks that this pod's OWN goroutine is ACTIVELY executing the task right now —
// specifically the broker's post-Complete DB apply (ApplyComposeResult / AppendOutbox; for
// a 1M-child compose the apply is the ~1-minute long pole, long after the worker's handler
// returned in µs). A live goroutine on a live pod IS the liveness proof, so a local record
// is NOT subject to the worker-attestation staleness gate — otherwise a broker doing a long
// apply would starve its own lease past requireWithin and reap itself mid-apply. It is set
// LATE, by inFlight.markLocal, only when the apply phase begins — NOT at claim.
//
// Until then (the claim → dispatch → parked-awaiting-Complete phase) local is FALSE: the
// task runs on a WORKER (local or, via the mesh, a peer's), whose WorkHeartbeat attestation
// keeps the lease fresh through Attest. The requireWithin gate applies to these non-local
// records, so a task whose worker went silent — OR that was never delivered at all (a lost
// dispatch / an undispatched mesh-relay task) — lapses past requireWithin, its heartbeat_at
// goes stale, and the reaper reclaims + re-dispatches it. That is the crucial self-heal:
// marking a not-yet-executing task local would keep heartbeating a lease no worker holds,
// hiding the strand from the reaper forever.
type inFlightRecord struct {
	shard    int16
	epoch    int64
	attested time.Time
	local    bool
}

// inFlightSet is a concurrency-safe map of claimed-but-unfinished task id → its
// heartbeat state, read by the heartbeat goroutine and mutated by task goroutines.
// Tracking (shard, epoch) lets the heartbeat scope its UPDATE to the tasks' ACTUAL
// shard span for partition pruning (independent of the dispatcher's possibly just-
// reshared ownership range) and fence the refresh on the claim epoch. Tracking the
// last-attested time makes heartbeat_at a TRANSITIVE worker-liveness signal: the
// heartbeat refreshes only tasks a live worker attested within requireWithin, so a
// silent or hung task (its worker stopped attesting) falls out and the reaper
// reclaims it. When no attestation source is wired (the in-process path, where the
// executor IS the worker), attested is left zero and every in-flight task is
// refreshed — process liveness is worker liveness there.
type inFlightSet struct {
	mu  sync.Mutex
	ids map[uuid.UUID]*inFlightRecord
	// requireWithin bounds how stale an attestation may be for its task to keep
	// getting heartbeated. 0 disables the gate (in-process / no attestation source):
	// every task is refreshed regardless of attested.
	requireWithin time.Duration
	// now is the clock (real in prod; injectable in tests). nil → time.Now.
	now func() time.Time
}

func newInFlightSet(requireWithin time.Duration) *inFlightSet {
	return &inFlightSet{ids: make(map[uuid.UUID]*inFlightRecord), requireWithin: requireWithin}
}

func (s *inFlightSet) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// add records a freshly-claimed task. It starts NON-local: the task is about to be
// dispatched to a worker and is worker-attested until its apply phase (markLocal flips it —
// see inFlightRecord). attested starts at claim time so the task is covered from the first
// beat even before its first worker attestation arrives; its ABSENCE past requireWithin then
// drops it from the refresh set (so a never-delivered task self-heals via the reaper).
func (s *inFlightSet) add(id uuid.UUID, shard int16, epoch int64) {
	s.mu.Lock()
	s.ids[id] = &inFlightRecord{shard: shard, epoch: epoch, attested: s.clock()}
	s.mu.Unlock()
}

func (s *inFlightSet) remove(id uuid.UUID) {
	s.mu.Lock()
	delete(s.ids, id)
	s.mu.Unlock()
}

// markLocal flips a task to local once this pod's OWN goroutine starts executing it
// end-to-end — the broker's post-Complete apply phase (ApplyComposeResult), which runs
// here with no worker attesting. From this point the record is exempt from the
// worker-attestation staleness gate (the live goroutine is the liveness proof), so a
// long apply can't starve its own lease. Until this is called a task is local=false:
// it is parked awaiting a REMOTE worker's Complete, kept fresh by that worker's
// WorkHeartbeat attestation — and if the worker never delivers/attests (a lost or
// undispatched task), its attestation lapses, the lease goes stale, and the reaper
// reclaims + re-dispatches it. No-op for an already-removed id.
func (s *inFlightSet) markLocal(id uuid.UUID) {
	s.mu.Lock()
	if r := s.ids[id]; r != nil {
		r.local = true
		r.attested = s.clock() // reset the clock so the apply phase starts with a full window
	}
	s.mu.Unlock()
}

// attest marks that a live worker reported this task's handler still progressing.
// Called from the broker's WorkHeartbeat relay (mapped token → work_id). Unknown
// ids (already completed / not ours) are ignored.
func (s *inFlightSet) attest(ids []uuid.UUID) {
	if len(ids) == 0 {
		return
	}
	now := s.clock()
	s.mu.Lock()
	for _, id := range ids {
		if r := s.ids[id]; r != nil {
			r.attested = now
		}
	}
	s.mu.Unlock()
}

// snapshot returns the (id, epoch) pairs to heartbeat, plus the inclusive [lo, hi] span
// of their shards (for the heartbeat's BETWEEN pruning). A LOCAL task (this pod's own
// reaction goroutine is executing it — worker-run + broker apply) is ALWAYS returned: the
// live goroutine on this live pod is its liveness proof, so it is never dropped by the
// staleness gate (otherwise a broker doing a long compose apply would starve and reap its
// own lease). A NON-LOCAL (forwarded mesh) task is subject to the worker-attestation gate:
// if its executing peer went silent past requireWithin it is EXCLUDED so its heartbeat_at
// goes stale and the reaper reclaims it. With requireWithin==0 the gate is off for all.
// Empty result → (nil, nil, 0, -1), an empty range that matches nothing — the caller skips
// the UPDATE.
func (s *inFlightSet) snapshot() (ids []uuid.UUID, epochs []int64, lo, hi int16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.ids) == 0 {
		return nil, nil, 0, -1
	}
	cutoff := s.clock().Add(-s.requireWithin)
	ids = make([]uuid.UUID, 0, len(s.ids))
	epochs = make([]int64, 0, len(s.ids))
	first := true
	for id, r := range s.ids {
		if s.requireWithin > 0 && !r.local && r.attested.Before(cutoff) {
			continue // parked awaiting a worker that went silent/never delivered — let the lease go stale so the reaper re-dispatches it
		}
		ids = append(ids, id)
		epochs = append(epochs, r.epoch)
		if first || r.shard < lo {
			lo = r.shard
		}
		if first || r.shard > hi {
			hi = r.shard
		}
		first = false
	}
	if len(ids) == 0 {
		return nil, nil, 0, -1
	}
	return ids, epochs, lo, hi
}
