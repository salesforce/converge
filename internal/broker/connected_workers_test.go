package broker

// connected_workers_test.go — DETERMINISTIC, pure-Go unit tests for the
// connected-worker cluster-view snapshot (broker.go connectedWorkers) and the
// cluster_members.config round-trip (relay.go AdvertiseWorkers). These are the
// data path behind "conctl cluster --workers" / the UI's per-broker worker list:
// one row per open WorkStream, each with its friendly id + the kinds it
// serves. No network, no Postgres.

import (
	"encoding/json"
	"testing"

	"github.com/salesforce/converge/internal/model"
)

// TestConnectedWorkersSnapshot proves connectedWorkers() projects each live
// WorkStream to one ConnectedWorker with its id, its kinds (SORTED for a
// stable view), and its live/ceiling slots — and that the rows are ordered by
// worker id. It's the exact shape the reporter publishes to the cluster view.
func TestConnectedWorkersSnapshot(t *testing.T) {
	f := newFanoutExecutor()
	// A multi-kind worker (kinds given out of order → must come back sorted) and
	// a single-kind worker. Distinct ids and ceilings.
	subMulti := f.addSubscriber([]model.KindVersion{{Kind: "policy", Version: 1}, {Kind: "classicbom", Version: 1}}, 10, peerIdentity{id: "fleet-b"})
	f.addSubscriber([]model.KindVersion{{Kind: "policy", Version: 1}}, 5, peerIdentity{id: "fleet-a"})

	// Give the multi-kind worker some in-flight load so the slot count is real.
	f.mu.Lock()
	subMulti.inflight = 3
	f.mu.Unlock()

	got := f.connectedWorkers()
	if len(got) != 2 {
		t.Fatalf("connectedWorkers len = %d, want 2", len(got))
	}
	// Ordered by worker id: fleet-a before fleet-b.
	if got[0].WorkerID != "fleet-a" || got[1].WorkerID != "fleet-b" {
		t.Fatalf("worker order = %q,%q; want fleet-a,fleet-b (sorted by id)", got[0].WorkerID, got[1].WorkerID)
	}
	// fleet-a: one kind, no load, ceiling 5.
	a := got[0]
	if len(a.Kinds) != 1 || a.Kinds[0] != "policy" || a.InFlight != 0 || a.MaxInflight != 5 {
		t.Errorf("fleet-a = %+v; want kinds=[policy] inflight=0 max=5", a)
	}
	// fleet-b: kinds sorted (classicbom < policy), load 3, ceiling 10.
	b := got[1]
	if len(b.Kinds) != 2 || b.Kinds[0] != "classicbom" || b.Kinds[1] != "policy" {
		t.Errorf("fleet-b kinds = %v; want sorted [classicbom policy]", b.Kinds)
	}
	if b.InFlight != 3 || b.MaxInflight != 10 {
		t.Errorf("fleet-b slots = %d/%d; want 3/10", b.InFlight, b.MaxInflight)
	}
}

// TestConnectedWorkersEmptyIDSynthesizesFallback proves a legacy worker that
// sends no worker_id still lists distinctly (a synthesized "worker-N" id) rather
// than collapsing two anonymous streams onto one blank row.
func TestConnectedWorkersEmptyIDSynthesizesFallback(t *testing.T) {
	f := newFanoutExecutor()
	f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 1, peerIdentity{id: ""}) // legacy: no id
	f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 1, peerIdentity{id: ""}) // another legacy worker

	got := f.connectedWorkers()
	if len(got) != 2 {
		t.Fatalf("connectedWorkers len = %d, want 2 (two anonymous streams)", len(got))
	}
	if got[0].WorkerID == "" || got[1].WorkerID == "" {
		t.Fatalf("empty worker_id was not synthesized: %q,%q", got[0].WorkerID, got[1].WorkerID)
	}
	if got[0].WorkerID == got[1].WorkerID {
		t.Fatalf("two anonymous workers got the SAME synthesized id %q (they'd collapse in the view)", got[0].WorkerID)
	}

	// STABILITY: the synthesized ids come from each stream's stable seq (not a
	// per-snapshot counter over a randomly-ordered map), so repeated snapshots
	// yield the SAME set of labels — the cluster view doesn't flap beat-to-beat.
	for i := 0; i < 20; i++ {
		again := f.connectedWorkers()
		if len(again) != 2 || again[0].WorkerID != got[0].WorkerID || again[1].WorkerID != got[1].WorkerID {
			t.Fatalf("synthesized ids not stable across snapshots: first=%v later=%v", []string{got[0].WorkerID, got[1].WorkerID}, []string{again[0].WorkerID, again[1].WorkerID})
		}
	}
}

// TestConnectedWorkersDropReflected proves the snapshot tracks removeSubscriber:
// a worker that disconnects drops out of the next snapshot (the cluster view is
// live, not a stale set from when the worker first connected).
func TestConnectedWorkersDropReflected(t *testing.T) {
	f := newFanoutExecutor()
	sub := f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 1, peerIdentity{id: "gone"})
	f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 1, peerIdentity{id: "stays"})

	f.removeSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, sub)

	got := f.connectedWorkers()
	if len(got) != 1 || got[0].WorkerID != "stays" {
		t.Fatalf("after a worker left, snapshot = %+v; want only [stays]", got)
	}
}

// TestWorkersJSON proves WorkersJSON marshals the snapshot to the JSON array a
// broker writes to its cluster_members.workers column — matching the ConfigWorker
// contract the API parses back — and that an empty snapshot is "[]" (never a
// stale set, never null). This is the broker→cluster_members→API boundary the
// cluster view rides; workers are their OWN column, never merged into config.
func TestWorkersJSON(t *testing.T) {
	workers := []ConnectedWorker{
		{WorkerID: "fleet-a", Kinds: []model.Kind{"classicbom", "policy"}, InFlight: 3, MaxInflight: 10},
	}
	arr := WorkersJSON(workers)

	var got []ConfigWorker
	if err := json.Unmarshal(arr, &got); err != nil {
		t.Fatalf("WorkersJSON not a valid JSON array: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("workers len = %d, want 1", len(got))
	}
	w := got[0]
	if w.WorkerID != "fleet-a" || len(w.Kinds) != 2 || w.Kinds[0] != "classicbom" || w.InFlight != 3 || w.MaxInflight != 10 {
		t.Fatalf("published worker = %+v; want fleet-a [classicbom policy] 3/10", w)
	}

	// An empty snapshot marshals to "[]" (a broker whose last worker left shows
	// zero workers, not a stale set, never null).
	if empty := WorkersJSON(nil); string(empty) != "[]" {
		t.Fatalf("empty snapshot → %q; want []", string(empty))
	}
}
