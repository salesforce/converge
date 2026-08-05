package runtime

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// TopologyWatcher is the SINGLE reactor to a cluster-topology change: it owns ONE
// LISTEN on cluster_changed + ONE debounce + ONE failsafe poll, and on each wake
// runs a set of registered reactors. The resharder (recompute this pod's shard
// tile) and the broker mesh (dial/drop peer routes) are both reactors — so both
// react to the SAME membership snapshot on the SAME schedule, instead of each
// opening its own LISTEN connection and re-reading cluster_members independently.
//
// REACTIVITY (two layers, the shared wake model):
//   - cluster_changed NOTIFY — a JOIN (INSERT) or LEAVE (DELETE) wakes every node's
//     watcher, so a scale-up/down/Deregister is reflected within ~one NOTIFY window
//     (plus DebounceWindow).
//   - Interval failsafe — a CRASHED peer does NOT DELETE its row (no NOTIFY fires),
//     so a periodic re-run catches it once its heartbeat ages past LivenessWindow.
//     This is why BOTH reactors must key off the liveness window, not the NOTIFY
//     alone: a hard crash is a silent heartbeat lapse, only the poll sees it.
//
// LivenessWindow is the ONE definition of "member alive" the reactors share (the
// resharder passes it to assign_member_shards; the mesh reads ListLiveClusterMembers
// with it), so shard ownership and mesh routing never disagree on who is live. The
// ClusterMemberGC (row hard-delete at a much longer TTL) is a SEPARATE, orthogonal
// UI/history sweeper — NOT a routing signal, so it is not a reactor here.
type TopologyWatcher struct {
	Listener Listener // cluster_changed wake subscription (ONE for all reactors)

	// LivenessWindow bounds how recent a member's heartbeat must be to count as
	// live. Reactors that filter by liveness read it (via their own store calls);
	// the watcher itself just hands it to them at registration. Must be ≫ the
	// member-heartbeat cadence so a slow beat never drops a live peer, and ≤ the GC
	// TTL so an excluded member is also on its way to being reclaimed.
	LivenessWindow time.Duration

	// Interval is the failsafe re-run cadence (the NOTIFY handles prompt joins/
	// leaves; this catches a crashed peer whose beat aged out without a DELETE).
	Interval time.Duration

	// DebounceWindow coalesces a BURST of cluster_changed NOTIFYs (a scale event's
	// per-pod INSERT/DELETE) into a SINGLE run on the settled topology, so a 1→50
	// scale-out re-tiles + re-dials once, not N times mid-flux. The failsafe Interval
	// still backstops a crashed peer regardless. 0 ⇒ no debounce (every NOTIFY wakes
	// the loop directly; used by tests).
	DebounceWindow time.Duration

	reactors []namedReactor

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// TopologyReactor recomputes one subsystem's view from the current membership and
// reports whether it CHANGED (for logging / an optional onChange). It must be
// idempotent and respect ctx — the watcher runs it on every wake + failsafe tick.
type TopologyReactor func(ctx context.Context) (changed bool, err error)

type namedReactor struct {
	name     string
	react    TopologyReactor
	onChange func() // optional: invoked when this reactor's run reports changed
}

// NewTopologyWatcher builds a watcher with production-sane cadences; the caller
// sets LivenessWindow (and may override Interval/DebounceWindow) and registers
// reactors before Start.
func NewTopologyWatcher(listener Listener) *TopologyWatcher {
	return &TopologyWatcher{
		Listener: listener,
		// Failsafe re-run cadence: match the member-heartbeat beat so a crashed peer
		// (silent, no DELETE → no NOTIFY) is caught within ~one beat past its liveness
		// window. The NOTIFY handles prompt joins/leaves; this is only the backstop.
		Interval: DefaultMemberHeartbeatEvery,
		// 2s comfortably spans a scale-event's inter-pod membership burst (k8s admits
		// pods ms apart) while keeping an isolated join/leave reacting within ~2s.
		DebounceWindow: 2 * time.Second,
		LivenessWindow: DefaultMeshLivenessWindow,
	}
}

// Register adds a reactor run on every wake + failsafe tick. onChange (may be nil)
// fires from the watcher goroutine right after a run of THIS reactor reports a
// change — e.g. the resharder wires it to the ClusterMemberReporter's Trigger so a
// new shard span lands in the registry promptly. Call before Start.
func (w *TopologyWatcher) Register(name string, react TopologyReactor, onChange func()) {
	w.reactors = append(w.reactors, namedReactor{name: name, react: react, onChange: onChange})
}

// Start runs every reactor once SYNCHRONOUSLY (so the drivers see this member's
// shard range + the mesh has its routes before the first sweep/forward), then runs
// the reactive loop until Stop. The synchronous run is best-effort — a failure
// (e.g. the member's first heartbeat hasn't landed yet) just leaves each reactor's
// state at its prior value and the loop retries on the next NOTIFY/tick.
func (w *TopologyWatcher) Start(ctx context.Context) {
	w.runAll(ctx)
	loopCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	// buffered-1 coalescing wake consumed by the pollLoop. With debounce on it is
	// fed by the debouncer (one wake per settled burst); without, directly by the
	// listener — the same shape the dispatcher/drainer use.
	wakeup := make(chan struct{}, 1)

	if w.DebounceWindow > 0 {
		raw := make(chan struct{}, 1)
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.Listener.Listen(loopCtx, "topology cluster_changed", ClusterChangedChannel, func() { notify(raw) })
		}()
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			Debounce(loopCtx, w.DebounceWindow, raw, wakeup)
		}()
	} else {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.Listener.Listen(loopCtx, "topology cluster_changed", ClusterChangedChannel, func() { notify(wakeup) })
		}()
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		// idle==busy (Interval): topology reaction is membership-driven, not
		// backlog-driven — the wake handles prompt joins/leaves and Interval is the
		// failsafe. repollOnWork=false, hotWindow=0: a paced re-run, never a tight
		// loop. tick always returns (false, nil): the per-reactor change/error is
		// handled inside runAll (logged + onChange), and the watcher never needs the
		// pollLoop's own change/error handling.
		pollLoop(loopCtx, "topology", w.Interval, w.Interval, 0, false, wakeup,
			func(ctx context.Context) (bool, error) { w.runAll(ctx); return false, nil })
	}()
}

// runAll runs every registered reactor once. A reactor error is logged (with its
// name) and does NOT stop the others — one subsystem failing to reconcile (e.g. a
// transient DB blip) must not stall the rest; the next tick retries. A reactor
// reporting changed fires its onChange.
func (w *TopologyWatcher) runAll(ctx context.Context) {
	for _, r := range w.reactors {
		changed, err := r.react(ctx)
		if err != nil {
			// A ctx cancellation is Stop in progress, not a reactor failure — don't
			// log it as one (the loop is about to exit anyway).
			if ctx.Err() == nil {
				slog.Warn("topology reactor failed", "reactor", r.name, "err", err)
			}
			continue
		}
		if changed && r.onChange != nil {
			r.onChange()
		}
	}
}

// Stop cancels the loop + listener and joins them, releasing the held LISTEN
// connection before the pool is closed. It does NOT touch any reactor's published
// state (the ShardSet / mesh peer set) — those keep their last value until the
// process exits.
func (w *TopologyWatcher) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}
