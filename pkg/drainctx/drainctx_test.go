package drainctx

import (
	"context"
	"testing"
	"time"
)

// TestGraceKeepsDrainAliveThenCancels: after the parent is cancelled, drainCtx
// stays live for ~grace (so in-flight work can finish), then is cancelled.
func TestGraceKeepsDrainAliveThenCancels(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	drainCtx, cancel := New(parent, 100*time.Millisecond)
	defer cancel()

	cancelParent()

	// Immediately after parent cancel, drainCtx must still be live (the grace).
	select {
	case <-drainCtx.Done():
		t.Fatal("drainCtx cancelled immediately; grace window did not hold in-flight work")
	case <-time.After(20 * time.Millisecond):
	}

	// After the grace elapses, drainCtx must be cancelled.
	select {
	case <-drainCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("drainCtx not cancelled after the grace window elapsed")
	}
}

// TestZeroGraceCancelsImmediately: grace 0 = no drain, drainCtx dies with parent.
func TestZeroGraceCancelsImmediately(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	drainCtx, cancel := New(parent, 0)
	defer cancel()

	cancelParent()
	select {
	case <-drainCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("grace 0: drainCtx should be cancelled as soon as parent is")
	}
}

// TestCallerCancelStopsWatcher: the caller's cancel (Run returning, drained
// early) cancels drainCtx and releases the watcher even while parent is still
// live — so the goroutine never lingers past the loop.
func TestCallerCancelStopsWatcher(t *testing.T) {
	parent := context.Background() // never cancelled
	drainCtx, cancel := New(parent, time.Hour)
	cancel()
	select {
	case <-drainCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("caller cancel did not cancel drainCtx")
	}
}

// TestDrainDoesNotInheritParentCancel: drainCtx is WithoutCancel — a parent
// cancel does not propagate INTO drainCtx except via the grace timer. (Proven by
// the grace test above; here we assert drainCtx has no deadline of its own.)
func TestDrainHasNoInheritedDeadline(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelParent()
	drainCtx, cancel := New(parent, time.Hour)
	defer cancel()
	if _, ok := drainCtx.Deadline(); ok {
		t.Fatal("drainCtx should not inherit the parent's deadline (WithoutCancel)")
	}
}
