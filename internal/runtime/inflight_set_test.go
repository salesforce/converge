package runtime

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestInFlightSetMarkLocalNeverStales proves the broker-apply reap-loop fix: once a task's
// apply phase begins on this pod's own goroutine (markLocal), it is ALWAYS in the heartbeat
// snapshot — the staleness gate never drops it, however old its last worker attestation,
// because the live goroutine IS the liveness proof. Without this, a broker doing a
// >requireWithin apply would starve and reap its own lease mid-apply.
func TestInFlightSetMarkLocalNeverStales(t *testing.T) {
	now := time.Unix(1000, 0)
	s := newInFlightSet(10 * time.Second)
	s.now = func() time.Time { return now }

	id := uuid.New()
	s.add(id, 3, 100) // claimed non-local (parked awaiting the worker)
	s.markLocal(id)   // worker Complete arrived → apply phase on this goroutine

	// Jump FAR past requireWithin with NO further attestation — a local task stays.
	now = now.Add(time.Hour)
	ids, epochs, lo, hi := s.snapshot()
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("snapshot ids = %v, want the local task %s (a local task is never staled)", ids, id)
	}
	if len(epochs) != 1 || epochs[0] != 100 || lo != 3 || hi != 3 {
		t.Fatalf("snapshot epoch/span = %v/[%d,%d], want [100]/[3,3]", epochs, lo, hi)
	}
}

// TestInFlightSetUnattestedParkedTaskStales is the strand-fix regression: a task added at
// claim (local=false, worker-attested) that is NEVER attested — because it was never
// delivered to a worker (a lost dispatch / an undispatched mesh-relay task) — MUST drop out
// of the heartbeat snapshot past requireWithin. That lets its heartbeat_at go stale so the
// reaper reclaims + re-dispatches it. The pre-fix bug marked such a task local at CLAIM, so
// it was heartbeated forever and the strand was hidden from the reaper.
func TestInFlightSetUnattestedParkedTaskStales(t *testing.T) {
	now := time.Unix(2000, 0)
	s := newInFlightSet(10 * time.Second)
	s.now = func() time.Time { return now }

	stranded := uuid.New()
	s.add(stranded, 7, 300) // claimed, dispatched, but the worker never attests it

	now = now.Add(11 * time.Second) // past requireWithin with no attestation, no markLocal
	ids, _, _, _ := s.snapshot()
	if len(ids) != 0 {
		t.Fatalf("snapshot ids = %v, want EMPTY — an un-attested parked task must go stale so the reaper re-dispatches it", ids)
	}
}

// TestInFlightSetNonLocalStaleGate proves the worker-attestation gate still applies to a
// NON-LOCAL (forwarded mesh) task: its executing peer lives elsewhere, so if it stops
// attesting past requireWithin the task drops out of the snapshot (its heartbeat_at goes
// stale and the reaper reclaims it), while a still-attested non-local task stays.
func TestInFlightSetNonLocalStaleGate(t *testing.T) {
	now := time.Unix(2000, 0)
	s := newInFlightSet(10 * time.Second)
	s.now = func() time.Time { return now }

	live := uuid.New()
	silent := uuid.New()
	s.add(live, 3, 100)   // forwarded, will keep being attested
	s.add(silent, 5, 200) // forwarded, will go silent

	now = now.Add(6 * time.Second)
	s.attest([]uuid.UUID{live}) // re-attest only `live`

	now = now.Add(6 * time.Second) // live attested 6s ago (<10s → stays); silent 12s ago (>10s → drops)
	ids, _, lo, hi := s.snapshot()
	if len(ids) != 1 || ids[0] != live {
		t.Fatalf("snapshot ids = %v, want just the attested forwarded task %s", ids, live)
	}
	if lo != 3 || hi != 3 {
		t.Fatalf("snapshot shard span = [%d,%d], want [3,3] (only the live task's shard)", lo, hi)
	}
}

// TestInFlightSetGateDisabled proves requireWithin==0 (the in-process path) disables the
// gate for ALL records — every in-flight task is refreshed regardless of attestation age.
func TestInFlightSetGateDisabled(t *testing.T) {
	now := time.Unix(3000, 0)
	s := newInFlightSet(0) // gate off
	s.now = func() time.Time { return now }

	a, b := uuid.New(), uuid.New()
	s.add(a, 1, 10)
	s.add(b, 2, 20)

	now = now.Add(time.Hour)
	ids, epochs, lo, hi := s.snapshot()
	if len(ids) != 2 || len(epochs) != 2 {
		t.Fatalf("gate-off snapshot returned %d ids / %d epochs, want both tasks", len(ids), len(epochs))
	}
	if lo != 1 || hi != 2 {
		t.Fatalf("gate-off shard span = [%d,%d], want [1,2] (both tasks)", lo, hi)
	}
}
