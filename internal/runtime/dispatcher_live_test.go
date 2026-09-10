package runtime

import (
	"context"
	"sync"
	"testing"

	"github.com/salesforce/converge/internal/store"
)

// TestSweepEmptyPairsNoPanic guards the all-pending start: the dispatcher comes
// up with zero pairs (every kind's Setup failed and is being retried), so the
// first sweep must be a clean no-op, NOT an integer divide-by-zero — the cursor
// advance is `% len(pairs)`.
func TestSweepEmptyPairsNoPanic(t *testing.T) {
	d := &Dispatcher{}
	cursor := 0
	var wg sync.WaitGroup
	claimed, err := d.sweep(context.Background(), context.Background(), &cursor, &wg, newInFlightSet(0))
	if err != nil {
		t.Fatalf("empty sweep returned error: %v", err)
	}
	if claimed != 0 {
		t.Fatalf("empty sweep claimed %d, want 0", claimed)
	}
}

// TestDrainNewPairsDedupe verifies a live-added (kind, task_type) already in the
// ring is skipped, so a duplicate AddKindLive can't double the pod's per-kind
// concurrency. A distinct task_type for the same kind is still added.
func TestDrainNewPairsDedupe(t *testing.T) {
	d := &Dispatcher{Loop: &Loop{BrokerID: "test"}, newPairs: make(chan *pair, 8), maxParallel: 1}
	d.newPairs <- &pair{kind: "k", taskType: store.TaskReconcile}
	d.newPairs <- &pair{kind: "k", taskType: store.TaskReconcile} // duplicate
	d.newPairs <- &pair{kind: "k", taskType: store.TaskDelete}    // distinct task
	d.drainNewPairs()
	if len(d.pairs) != 2 {
		t.Fatalf("after dedupe want 2 pairs, got %d", len(d.pairs))
	}
}
