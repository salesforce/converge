package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeListener is a hand-driven Listener: the test fires onNotify / onReady at
// will (no real Postgres), so NotifyRefresher's wake handling is exercised
// deterministically. Listen blocks until ctx is cancelled, matching the real
// contract (it returns only on ctx cancel).
type fakeListener struct {
	mu       sync.Mutex
	onNotify func()
	onReady  func()
	ready    chan struct{} // closed once Listen has captured the callbacks
}

func newFakeListener() *fakeListener { return &fakeListener{ready: make(chan struct{})} }

func (f *fakeListener) Listen(ctx context.Context, _, _ string, onNotify func(), onReady ...func()) {
	f.mu.Lock()
	f.onNotify = onNotify
	if len(onReady) > 0 {
		f.onReady = onReady[0]
	}
	f.mu.Unlock()
	close(f.ready)
	if f.onReady != nil {
		f.onReady() // mirror the real listener: onReady fires once per subscribe
	}
	<-ctx.Done()
}

// fireNotify simulates a NOTIFY arriving on the channel.
func (f *fakeListener) fireNotify() {
	f.mu.Lock()
	fn := f.onNotify
	f.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// TestNotifyRefresherReloadsOnEveryEdge proves the refresher reloads on start
// (immediate), on the listener's onReady catch-up, and on each NOTIFY — the
// three prompt edges layered over the failsafe tick.
func TestNotifyRefresherReloadsOnEveryEdge(t *testing.T) {
	var reloads atomic.Int64
	done := make(chan struct{}, 64)
	fl := newFakeListener()

	// A long interval so the failsafe timer never fires during the test — we're
	// asserting the PROMPT edges here, not the tick.
	r := NewNotifyRefresher(fl, "test", "test_channel", time.Hour, func(context.Context) {
		reloads.Add(1)
		select {
		case done <- struct{}{}:
		default:
		}
	})
	r.Start(context.Background())
	defer r.Stop()

	// Wait for the listener to capture callbacks; the loop's immediate reload +
	// the onReady catch-up drive at least one reload (they may coalesce onto the
	// single buffered wakeup, so assert ">=1", not "==2").
	<-fl.ready
	waitReload(t, done, "startup reload")
	if got := reloads.Load(); got < 1 {
		t.Fatalf("expected >=1 reload after start, got %d", got)
	}

	// Let the startup wakeups settle so the loop is parked in its select with an
	// empty wakeup channel — otherwise a NOTIFY could coalesce with a still-pending
	// startup wake and be indistinguishable from it.
	time.Sleep(50 * time.Millisecond)
	drain(done)
	before := reloads.Load()

	fl.fireNotify()
	waitReload(t, done, "NOTIFY reload")
	if reloads.Load() <= before {
		t.Fatalf("a NOTIFY must trigger a reload: before=%d after=%d", before, reloads.Load())
	}
}

// TestNotifyRefresherFailsafeTick proves a reload happens on the interval even
// with NO NOTIFY — the dropped-NOTIFY backstop.
func TestNotifyRefresherFailsafeTick(t *testing.T) {
	var reloads atomic.Int64
	done := make(chan struct{}, 64)
	fl := newFakeListener()

	// Short interval; do NOT fire any NOTIFY — only the failsafe should drive reloads.
	r := NewNotifyRefresher(fl, "test", "test_channel", 30*time.Millisecond, func(context.Context) {
		reloads.Add(1)
		select {
		case done <- struct{}{}:
		default:
		}
	})
	r.Start(context.Background())
	defer r.Stop()

	<-fl.ready
	// Drain the startup reload(s), then let the buffered signals settle so the
	// count below reflects ONLY failsafe ticks (no NOTIFY is ever fired here).
	waitReload(t, done, "startup reload")
	time.Sleep(45 * time.Millisecond)
	drain(done)
	base := reloads.Load()

	// Over the next window, the failsafe tick alone must produce further reloads.
	waitReload(t, done, "failsafe-tick reload")
	if reloads.Load() <= base {
		t.Fatalf("failsafe tick must reload without a NOTIFY: base=%d later=%d", base, reloads.Load())
	}
}

// drain empties any buffered reload signals without blocking.
func drain(done <-chan struct{}) {
	for {
		select {
		case <-done:
		default:
			return
		}
	}
}

// TestNotifyRefresherStopIsClean proves Stop cancels the loop + listener and
// joins them (no reload after Stop returns).
func TestNotifyRefresherStopIsClean(t *testing.T) {
	var reloads atomic.Int64
	fl := newFakeListener()
	r := NewNotifyRefresher(fl, "test", "test_channel", time.Hour, func(context.Context) {
		reloads.Add(1)
	})
	r.Start(context.Background())
	<-fl.ready
	r.Stop() // blocks until both goroutines exit

	settled := reloads.Load()
	// A NOTIFY fired after Stop must not reload (callbacks are inert post-cancel).
	fl.fireNotify()
	time.Sleep(20 * time.Millisecond)
	if reloads.Load() != settled {
		t.Fatalf("no reload may occur after Stop: settled=%d now=%d", settled, reloads.Load())
	}
}

// waitReload waits for one reload signal or fails the test on timeout.
func waitReload(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}
