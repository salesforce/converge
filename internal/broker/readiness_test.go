package broker

// readiness_test.go — DETERMINISTIC, pure-Go unit tests for PER-KIND WORKER
// READINESS (the WorkStream Interest RS+/RS- path): a connected worker can flip a
// single (kind, kindVersion) unready (its provider's downstream degraded — e.g.
// Kafka down) without dropping the stream, and the broker then stops CLAIMING and
// PUSHING that pair while the worker's OTHER kinds keep flowing. Recovery (RS+)
// re-opens the gate. Also asserts the mesh Interest retracts in lockstep so peers
// stop forwarding the degraded pair too. No network, no Postgres.

import (
	"testing"

	"github.com/salesforce/converge/internal/model"
)

// TestReadinessGatesClaimForDegradedKind: a worker serving two kinds flips kindA
// unready (RS-); HasSubscriber(kindA) goes false (the claim gate closes) while
// HasSubscriber(kindB) stays true — one degraded provider must not gate the healthy
// siblings on the same stream. RS+ re-opens kindA.
func TestReadinessGatesClaimForDegradedKind(t *testing.T) {
	f := newFanoutExecutor()
	kindA := model.KindVersion{Kind: "kafka", Version: 1}
	kindB := model.KindVersion{Kind: "noop", Version: 1}
	sub := f.addSubscriber([]model.KindVersion{kindA, kindB}, 4, peerIdentity{id: "w1"})

	// Baseline: a fresh stream is ready for every kind it serves.
	if !f.HasSubscriber("kafka", 1) || !f.HasSubscriber("noop", 1) {
		t.Fatal("fresh subscriber is not ready for both kinds")
	}

	// RS- on kindA only.
	f.setStreamReadiness(sub, kindA, false)
	if f.HasSubscriber("kafka", 1) {
		t.Fatal("HasSubscriber(kafka) still true after the only worker went RS- for it")
	}
	if !f.HasSubscriber("noop", 1) {
		t.Fatal("HasSubscriber(noop) went false — a per-kind RS- leaked to a sibling kind on the same stream")
	}

	// RS+ on kindA: recovered.
	f.setStreamReadiness(sub, kindA, true)
	if !f.HasSubscriber("kafka", 1) {
		t.Fatal("HasSubscriber(kafka) still false after RS+ recovery")
	}
}

// TestReadinessRefCountAcrossWorkers: with TWO workers for a kind, one going RS-
// must NOT close the gate (the other is still healthy); only when the LAST ready
// worker goes RS- does the gate close. This is the readySubscribers ref-count edge,
// mirroring the subscribers 0↔1 edge but for readiness.
func TestReadinessRefCountAcrossWorkers(t *testing.T) {
	f := newFanoutExecutor()
	km := model.KindVersion{Kind: "kafka", Version: 1}
	subA := f.addSubscriber([]model.KindVersion{km}, 2, peerIdentity{id: "w-a"})
	subB := f.addSubscriber([]model.KindVersion{km}, 2, peerIdentity{id: "w-b"})

	f.setStreamReadiness(subA, km, false) // A degraded; B still healthy
	if !f.HasSubscriber("kafka", 1) {
		t.Fatal("gate closed while a second healthy worker (B) is still ready")
	}
	f.setStreamReadiness(subB, km, false) // now BOTH degraded
	if f.HasSubscriber("kafka", 1) {
		t.Fatal("gate still open after the LAST ready worker went RS-")
	}
	f.setStreamReadiness(subA, km, true) // A recovers
	if !f.HasSubscriber("kafka", 1) {
		t.Fatal("gate did not re-open after a worker recovered (RS+)")
	}
}

// TestReadinessIdempotentAndStrict: a duplicate/reordered absolute frame (RS- twice,
// RS+ twice) must not double-count the readySubscribers ref count, an Interest for a
// pair the stream never subscribed is a no-op, and readiness is STRICT per-kindVersion
// (an RS- on v1 does not gate v2).
func TestReadinessIdempotentAndStrict(t *testing.T) {
	f := newFanoutExecutor()
	v1 := model.KindVersion{Kind: "kafka", Version: 1}
	v2 := model.KindVersion{Kind: "kafka", Version: 2}
	sub := f.addSubscriber([]model.KindVersion{v1, v2}, 3, peerIdentity{id: "w1"})

	// RS- twice for v1: idempotent (a second RS- must not underflow readySubscribers).
	f.setStreamReadiness(sub, v1, false)
	f.setStreamReadiness(sub, v1, false)
	if f.HasSubscriber("kafka", 1) {
		t.Fatal("v1 gate open after RS-")
	}
	if !f.HasSubscriber("kafka", 2) {
		t.Fatal("RS- on kafka/v1 gated kafka/v2 — readiness is not STRICT per kindVersion")
	}
	// RS+ twice: back to ready, no double count.
	f.setStreamReadiness(sub, v1, true)
	f.setStreamReadiness(sub, v1, true)
	if !f.HasSubscriber("kafka", 1) {
		t.Fatal("v1 gate did not re-open after RS+")
	}
	f.mu.Lock()
	got := f.readySubscribers["kafka"][1]
	f.mu.Unlock()
	if got != 1 {
		t.Fatalf("readySubscribers[kafka][1] = %d; want 1 (no double-count from duplicate RS+/RS-)", got)
	}

	// An Interest for a pair this stream never subscribed is a no-op (no phantom entry).
	f.setStreamReadiness(sub, model.KindVersion{Kind: "other", Version: 1}, false)
	f.mu.Lock()
	_, phantom := f.readySubscribers["other"]
	f.mu.Unlock()
	if phantom {
		t.Fatal("an Interest for an unsubscribed pair created a phantom readySubscribers entry")
	}
}

// TestReadinessExcludedFromCredit: a stream that has RS-'d a kind contributes ZERO
// credit for it (creditForLocked skips a degraded stream) even though it is still
// subscribed and holds free slots — advertising its slots would let a peer push work
// it would only fail.
func TestReadinessExcludedFromCredit(t *testing.T) {
	f := newFanoutExecutor()
	km := model.KindVersion{Kind: "kafka", Version: 1}
	sub := f.addSubscriber([]model.KindVersion{km}, 5, peerIdentity{id: "w1"}) // 5 free slots

	f.mu.Lock()
	ready := f.creditForLocked(km)
	f.mu.Unlock()
	if ready != 5 {
		t.Fatalf("creditForLocked = %d; want 5 while ready", ready)
	}

	f.setStreamReadiness(sub, km, false) // degraded
	f.mu.Lock()
	degraded := f.creditForLocked(km)
	f.mu.Unlock()
	if degraded != 0 {
		t.Fatalf("creditForLocked = %d; want 0 while the only stream is RS-'d (its slots must not be advertised)", degraded)
	}
}

// TestReadinessRetractsMeshInterest: with the mesh attached, a worker RS- retracts
// this broker's advertised mesh Interest for the pair (selfCredit drops the entry),
// so peers' anyPeerServes goes false and they stop forwarding the degraded pair here.
// RS+ restores it. This proves per-kind readiness composes with the broker mesh via
// the existing advertiseCredit plumbing.
func TestReadinessRetractsMeshInterest(t *testing.T) {
	f := newFanoutExecutor()
	// Attach a mesh with no peers (nil lister/http — we never Run it; advertiseCredit
	// only touches selfCredit + loops the empty peer set).
	f.mesh = newPeerMesh("self", nil, nil, nil, 0, 0, f)
	km := model.KindVersion{Kind: "kafka", Version: 1}
	sub := f.addSubscriber([]model.KindVersion{km}, 2, peerIdentity{id: "w1"})

	hasSelfCredit := func() bool {
		f.mesh.mu.Lock()
		defer f.mesh.mu.Unlock()
		_, ok := f.mesh.selfCredit[km]
		return ok
	}
	if !hasSelfCredit() {
		t.Fatal("mesh selfCredit missing for a ready subscriber (addSubscriber should have advertised it)")
	}
	f.setStreamReadiness(sub, km, false) // RS-
	if hasSelfCredit() {
		t.Fatal("mesh selfCredit still advertised after the only worker went RS- (peers would keep forwarding)")
	}
	f.setStreamReadiness(sub, km, true) // RS+
	if !hasSelfCredit() {
		t.Fatal("mesh selfCredit not restored after RS+ recovery")
	}
}

// TestRemoveDegradedSubscriberNoUnderflow: removing a stream that had already RS-'d a
// kind must not double-decrement readySubscribers (the RS- already decremented it) —
// a second healthy worker for the pair must stay ready after the degraded one leaves.
func TestRemoveDegradedSubscriberNoUnderflow(t *testing.T) {
	f := newFanoutExecutor()
	km := model.KindVersion{Kind: "kafka", Version: 1}
	subA := f.addSubscriber([]model.KindVersion{km}, 2, peerIdentity{id: "w-a"})
	subB := f.addSubscriber([]model.KindVersion{km}, 2, peerIdentity{id: "w-b"})

	f.setStreamReadiness(subA, km, false)             // A degraded → readySubscribers[kafka][1] = 1 (B)
	f.removeSubscriber([]model.KindVersion{km}, subA) // A leaves; must NOT decrement again
	if !f.HasSubscriber("kafka", 1) {
		t.Fatal("gate closed after a DEGRADED worker left — readySubscribers underflowed (double decrement)")
	}
	f.mu.Lock()
	got := f.readySubscribers["kafka"][1]
	f.mu.Unlock()
	if got != 1 {
		t.Fatalf("readySubscribers[kafka][1] = %d; want 1 (only B remains ready)", got)
	}
	// Sanity: B leaving closes it cleanly.
	f.removeSubscriber([]model.KindVersion{km}, subB)
	if f.HasSubscriber("kafka", 1) {
		t.Fatal("gate open after the last worker left")
	}
}
