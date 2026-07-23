package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// nopListener is a Listener that never fires a NOTIFY, so a TopologyWatcher under
// test runs purely on its failsafe Interval — deterministic, no live DB/LISTEN.
type nopListener struct{}

func (nopListener) Listen(ctx context.Context, _, _ string, _ func(), _ ...func()) {
	<-ctx.Done()
}

// TestTopologyWatcher_RunsReactorsOnStartAndInterval proves the watcher runs every
// registered reactor once synchronously at Start (so shard tile + mesh routes exist
// before the first sweep/forward) and again on each failsafe tick, and that a
// reactor reporting changed fires its onChange.
func TestTopologyWatcher_RunsReactorsOnStartAndInterval(t *testing.T) {
	var runs, changes atomic.Int32
	w := NewTopologyWatcher(nopListener{})
	w.DebounceWindow = 0               // no debounce → deterministic
	w.Interval = 30 * time.Millisecond // snappy failsafe for the test
	w.Register("r", func(context.Context) (bool, error) {
		runs.Add(1)
		return true, nil // always "changed" so onChange fires each run
	}, func() { changes.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	// One synchronous run at Start, then at least two more failsafe ticks.
	deadline := time.After(500 * time.Millisecond)
	for runs.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("watcher ran reactor %d times; want >=3 (Start + failsafe ticks)", runs.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	w.Stop()
	// onChange fired once per changed run.
	if c := changes.Load(); c < runs.Load() {
		t.Fatalf("onChange fired %d times for %d changed runs; want equal", c, runs.Load())
	}
}

// TestTopologyWatcher_ReactorErrorDoesNotStopOthers proves one reactor's error is
// isolated: a failing reactor is logged and skipped, the others still run, and the
// loop keeps ticking (a transient DB blip in one subsystem must not stall the rest).
func TestTopologyWatcher_ReactorErrorDoesNotStopOthers(t *testing.T) {
	var bad, good atomic.Int32
	w := NewTopologyWatcher(nopListener{})
	w.DebounceWindow = 0
	w.Interval = 20 * time.Millisecond
	w.Register("bad", func(context.Context) (bool, error) {
		bad.Add(1)
		return false, context.DeadlineExceeded // simulate a transient failure
	}, nil)
	w.Register("good", func(context.Context) (bool, error) {
		good.Add(1)
		return false, nil
	}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	deadline := time.After(500 * time.Millisecond)
	for good.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("good reactor ran %d times; the failing reactor stalled the loop", good.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	w.Stop()
	if bad.Load() == 0 {
		t.Fatal("failing reactor never ran")
	}
}

// TestTopologyWatcher_PropagatesLivenessChange proves the watcher actually PROPAGATES a
// membership-liveness change through a reactor — the semantic the resharder + mesh
// reactors rely on: when a member ages out of the live set (a hard crash, whose
// heartbeat lapses and fires no NOTIFY), the next failsafe tick's reactor run must
// observe the smaller set and, because its published view CHANGED, fire onChange (the
// resharder wires onChange to the reporter's re-beat; the mesh cancels the dropped
// peer's route). This models a reactor over a mutable "live members" set — the unit-
// level analogue of a stale broker dropping out of ListLiveClusterMembers.
func TestTopologyWatcher_PropagatesLivenessChange(t *testing.T) {
	// The reactor's "live view": starts with 3 members, then one ages out. The reactor
	// reports changed=true only when the observed set differs from what it last published
	// (exactly how the Resharder's assignOnce reports a changed shard span).
	var mu sync.Mutex
	liveCount := 3
	setLive := func(n int) { mu.Lock(); liveCount = n; mu.Unlock() }

	var lastPublished atomic.Int32
	lastPublished.Store(-1) // nothing published yet
	var observed atomic.Int32
	var changes atomic.Int32

	w := NewTopologyWatcher(nopListener{})
	w.DebounceWindow = 0
	w.Interval = 20 * time.Millisecond
	w.Register("live-reactor", func(context.Context) (bool, error) {
		mu.Lock()
		n := liveCount
		mu.Unlock()
		observed.Store(int32(n))
		changed := lastPublished.Load() != int32(n)
		lastPublished.Store(int32(n))
		return changed, nil
	}, func() { changes.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx) // synchronous first run: observes 3, publishes it (changed from -1 → onChange #1)

	// The first run must have observed the full set and fired onChange (initial publish).
	if got := observed.Load(); got != 3 {
		t.Fatalf("initial run observed %d live members; want 3", got)
	}
	afterInitial := changes.Load()
	if afterInitial < 1 {
		t.Fatalf("initial publish did not fire onChange (got %d)", afterInitial)
	}

	// A member crashes: the live set shrinks to 2. The next tick must OBSERVE 2 and, since
	// the published view changed, fire onChange again.
	setLive(2)
	deadline := time.After(500 * time.Millisecond)
	for observed.Load() != 2 {
		select {
		case <-deadline:
			t.Fatalf("watcher never propagated the liveness drop (still observing %d)", observed.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	// onChange must have fired for the drop (strictly more than after the initial publish).
	for changes.Load() <= afterInitial {
		select {
		case <-deadline:
			t.Fatalf("liveness drop was observed but onChange did not fire (changes=%d, want >%d)", changes.Load(), afterInitial)
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Steady state: once the set stops changing, onChange stops firing (a reactor that
	// reports changed=false must NOT keep waking the reporter — no churn on a quiet fleet).
	stable := changes.Load()
	time.Sleep(120 * time.Millisecond) // several ticks with liveCount unchanged
	if got := changes.Load(); got != stable {
		t.Fatalf("onChange fired %d extra times on an UNCHANGED set (want 0 — no churn when quiet)", got-stable)
	}
}
