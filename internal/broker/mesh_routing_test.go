package broker

// mesh_routing_test.go — DETERMINISTIC, pure-Go unit tests for the broker↔broker MESH
// routing + interest-propagation + peer-lifecycle logic (relay.go): pushTask placement
// matrix, RS+/RS- interest, non-blocking credit fan-out, refreshOnce churn (cancel
// departed + reuse-live-pointer), and the onRouteTask enqueue edges. No network, no
// Postgres, no waited timers — they drive peerMesh/peerClient value literals and a fake
// sink/lister.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/meshpb"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// mkPeer builds an in-memory peerClient with the given per-(kind, kindVersion) credit/presence.
func mkPeer(id string, outCap int, credit map[model.KindVersion]int32, serves map[model.KindVersion]bool) *peerClient {
	if credit == nil {
		credit = map[model.KindVersion]int32{}
	}
	if serves == nil {
		serves = map[model.KindVersion]bool{}
	}
	return &peerClient{
		memberID: id,
		out:      make(chan *meshpb.RouteFrame, outCap),
		credit:   credit,
		serves:   serves,
	}
}

func meshWith(selfID string, peers map[string]*peerClient) *peerMesh {
	m := &peerMesh{selfID: selfID, selfCredit: map[model.KindVersion]int32{}, sink: &spySink{}}
	m.peers.Store(&peers)
	return m
}

// ── pushTask placement matrix (anti-strand routing) ───────────────────────────────

// TestPushTaskFalseWhenNoPeerServes: no peer has credit AND none advertises presence for
// the kind → pushTask returns false so the caller PARKS+retries (never drops the task).
func TestPushTaskFalseWhenNoPeerServes(t *testing.T) {
	// One peer that serves a DIFFERENT kind; nothing serves "noop".
	p := mkPeer("p", 4, map[model.KindVersion]int32{{Kind: "other", Version: 1}: 3}, map[model.KindVersion]bool{{Kind: "other", Version: 1}: true})
	m := meshWith("self", map[string]*peerClient{"p": p})
	if _, ok := m.pushTask(model.KindVersion{Kind: "noop", Version: 1}, &workerpb.StageTask{Kind: "noop", LeaseToken: uuid.NewString()}); ok {
		t.Fatal("pushTask returned true for a kind no peer serves; want false (park)")
	}
	select {
	case <-p.out:
		t.Fatal("a frame was pushed to a peer that doesn't serve the kind")
	default:
	}
}

// TestPushTaskBufferPushesToServingZeroCreditPeer: no free credit anywhere but a peer
// HAS a worker (presence) → pushTask buffer-pushes to it (returns true) rather than
// dropping. This is the anti-strand path when every worker is momentarily full.
func TestPushTaskBufferPushesToServingZeroCreditPeer(t *testing.T) {
	p := mkPeer("p", 4, map[model.KindVersion]int32{}, map[model.KindVersion]bool{{Kind: "noop", Version: 1}: true}) // presence, zero credit
	m := meshWith("self", map[string]*peerClient{"p": p})
	task := &workerpb.StageTask{Kind: "noop", LeaseToken: uuid.NewString()}
	if _, ok := m.pushTask(model.KindVersion{Kind: "noop", Version: 1}, task); !ok {
		t.Fatal("pushTask returned false; want true (buffer-push to the serving-but-zero-credit peer)")
	}
	select {
	case fr := <-p.out:
		if fr.GetTask().GetLeaseToken() != task.GetLeaseToken() {
			t.Fatalf("buffered frame token mismatch")
		}
	default:
		t.Fatal("no frame buffer-pushed to the serving peer")
	}
}

// TestPushTaskFalseWhenChosenQueueFull: the only credited peer's out is FULL → the final
// non-blocking send hits default, pushTask returns false (caller retries) and credit is
// NOT optimistically decremented.
func TestPushTaskFalseWhenChosenQueueFull(t *testing.T) {
	p := mkPeer("p", 1, map[model.KindVersion]int32{{Kind: "noop", Version: 1}: 5}, map[model.KindVersion]bool{{Kind: "noop", Version: 1}: true})
	p.out <- &meshpb.RouteFrame{} // fill the cap-1 queue
	m := meshWith("self", map[string]*peerClient{"p": p})
	if _, ok := m.pushTask(model.KindVersion{Kind: "noop", Version: 1}, &workerpb.StageTask{Kind: "noop", LeaseToken: uuid.NewString()}); ok {
		t.Fatal("pushTask returned true despite a full out queue; want false")
	}
	if got := p.getCredit(model.KindVersion{Kind: "noop", Version: 1}); got != 5 {
		t.Fatalf("credit = %d after a failed push; want 5 (no decrement on drop)", got)
	}
}

// ── interest / credit semantics (RS+/RS-) ─────────────────────────────────────────

// TestAnyPeerServesFalseOnlyWhenAllPresenceDropped: the cluster-aware claim gate ORs
// presence — true while ANY peer serves the kind, false only after the LAST drops.
// Credit is irrelevant (a serves-true/credit-0 peer keeps the gate open).
func TestAnyPeerServesFalseOnlyWhenAllPresenceDropped(t *testing.T) {
	pA := mkPeer("a", 4, map[model.KindVersion]int32{{Kind: "noop", Version: 1}: 2}, map[model.KindVersion]bool{{Kind: "noop", Version: 1}: true})
	pB := mkPeer("b", 4, map[model.KindVersion]int32{}, map[model.KindVersion]bool{{Kind: "noop", Version: 1}: true}) // presence, no credit
	m := meshWith("self", map[string]*peerClient{"a": pA, "b": pB})

	if !m.anyPeerServes("noop", 1) {
		t.Fatal("anyPeerServes false with two serving peers")
	}
	pA.setInterest(model.KindVersion{Kind: "noop", Version: 1}, 0, false) // A drops presence
	if !m.anyPeerServes("noop", 1) {
		t.Fatal("anyPeerServes flipped false while B still serves (credit-0 presence must keep it open)")
	}
	pB.setInterest(model.KindVersion{Kind: "noop", Version: 1}, 0, false) // last presence drops
	if m.anyPeerServes("noop", 1) {
		t.Fatal("anyPeerServes still true after ALL presence dropped (RS-)")
	}
}

// TestSetInterestPresenceAndCreditSemantics drives the 4 RS+/RS- combinations on one
// peer: credit>0+worker → stores credit + serves; credit<=0+worker → deletes credit but
// KEEPS presence; !worker → deletes presence regardless of credit.
func TestSetInterestPresenceAndCreditSemantics(t *testing.T) {
	p := mkPeer("p", 4, nil, nil)
	noop := model.KindVersion{Kind: "noop", Version: 1}

	p.setInterest(noop, 5, true)
	if p.getCredit(noop) != 5 || !p.servesKindVersion(noop) {
		t.Fatalf("RS+ credit>0: credit=%d serves=%v; want 5/true", p.getCredit(noop), p.servesKindVersion(noop))
	}
	p.setInterest(noop, 0, true) // busy worker: no free credit, still present
	if p.getCredit(noop) != 0 || !p.servesKindVersion(noop) {
		t.Fatalf("credit<=0+worker: credit=%d serves=%v; want 0/true (presence kept)", p.getCredit(noop), p.servesKindVersion(noop))
	}
	p.setInterest(noop, 3, false) // RS-: worker gone, presence must drop even with credit
	if p.servesKindVersion(noop) {
		t.Fatal("RS- (hasWorker=false) did not drop presence")
	}
}

// TestDecCreditFloorsAtZero: optimistic per-push decrement drops one free slot, floors
// at 0 (never negative — deletes the entry), and leaves presence intact; a re-advertise
// restores the true credit.
func TestDecCreditFloorsAtZero(t *testing.T) {
	noop := model.KindVersion{Kind: "noop", Version: 1}
	p := mkPeer("p", 4, map[model.KindVersion]int32{noop: 2}, map[model.KindVersion]bool{noop: true})
	p.decCredit(noop) // 2→1
	p.decCredit(noop) // 1→0 (entry deleted)
	p.decCredit(noop) // stays 0, no negative
	if got := p.getCredit(noop); got != 0 {
		t.Fatalf("credit = %d after over-decrement; want 0 (floored, never negative)", got)
	}
	if !p.servesKindVersion(noop) {
		t.Fatal("decCredit dropped presence; it must only touch credit")
	}
	p.setInterest(noop, 5, true) // peer re-advertises absolute credit
	if got := p.getCredit(noop); got != 5 {
		t.Fatalf("credit = %d after re-advertise; want 5 (absolute state corrects the optimistic drop)", got)
	}
}

// ── non-blocking fan-out (never wedge the hot path) ───────────────────────────────

// TestAdvertiseCreditNonBlockingOnFullPeerQueue: advertiseCredit updates selfCredit and
// fans an Interest frame to EVERY peer, non-blocking — a peer with a full out queue is
// skipped (drop) without blocking the others or the caller.
func TestAdvertiseCreditNonBlockingOnFullPeerQueue(t *testing.T) {
	pFull := mkPeer("full", 1, nil, nil)
	pFull.out <- &meshpb.RouteFrame{} // fill it
	pFree := mkPeer("free", 4, nil, nil)
	m := meshWith("self", map[string]*peerClient{"full": pFull, "free": pFree})

	m.advertiseCredit("noop", 1, 3, true) // must return promptly despite pFull being full

	if got := m.snapshotSelfCredit()[model.KindVersion{Kind: "noop", Version: 1}]; got != 3 {
		t.Fatalf("selfCredit[noop] = %d; want 3", got)
	}
	// pFree got an Interest frame; pFull stayed full with only its filler.
	if len(pFree.out) != 1 {
		t.Fatalf("pFree.out len = %d; want 1 (Interest delivered)", len(pFree.out))
	}
	if len(pFull.out) != 1 {
		t.Fatalf("pFull.out len = %d; want 1 (Interest dropped, only the filler)", len(pFull.out))
	}
}

// TestAdvertiseCreditRSMinusDeletesSelfCredit: hasWorker=false REMOVES the kind from
// selfCredit (true RS-); hasWorker=true with credit 0 KEEPS it (busy worker still present).
func TestAdvertiseCreditRSMinusDeletesSelfCredit(t *testing.T) {
	m := meshWith("self", map[string]*peerClient{})
	noop := model.KindVersion{Kind: "noop", Version: 1}
	m.advertiseCredit("noop", 1, 0, true) // present but busy
	if _, ok := m.snapshotSelfCredit()[noop]; !ok {
		t.Fatal("credit=0+worker deleted selfCredit; presence must be kept at 0")
	}
	m.advertiseCredit("noop", 1, 0, false) // RS-: worker gone
	if _, ok := m.snapshotSelfCredit()[noop]; ok {
		t.Fatal("RS- (hasWorker=false) did not delete selfCredit")
	}
}

// TestSnapshotSelfCreditIsIndependentCopy: snapshotSelfCredit returns a defensive copy
// (incl. presence-at-0) — mutating it must not affect the mesh's own state.
func TestSnapshotSelfCreditIsIndependentCopy(t *testing.T) {
	m := meshWith("self", map[string]*peerClient{})
	kmA := model.KindVersion{Kind: "a", Version: 1}
	kmB := model.KindVersion{Kind: "b", Version: 1}
	m.advertiseCredit("a", 1, 3, true)
	m.advertiseCredit("b", 1, 0, true)
	snap := m.snapshotSelfCredit()
	if snap[kmA] != 3 || func() bool { _, ok := snap[kmB]; return !ok }() {
		t.Fatalf("snapshot = %v; want a=3,b=0 present", snap)
	}
	snap[kmA] = 999
	delete(snap, kmB)
	if again := m.snapshotSelfCredit(); again[kmA] != 3 || func() bool { _, ok := again[kmB]; return !ok }() {
		t.Fatalf("mutating the snapshot leaked into the mesh: %v", again)
	}
}

// ── refreshOnce churn: peer lifecycle (cancel departed, reuse live) ────────────────

// spySink is a no-op routeSink for tests that don't exercise inbound task delivery;
// it only counts onPeerGone so a test can assert a departed/stale peer's parked
// pushes were woken.
type spySink struct{ gone atomic.Int32 }

func (s *spySink) onRouteTask(*peerClient, *workerpb.StageTask) {}
func (s *spySink) onRouteComplete(*workerpb.StageComplete)      {}
func (s *spySink) onPeerGone(*peerClient)                       { s.gone.Add(1) }
func (s *spySink) goneCount() int                               { return int(s.gone.Load()) }

// newRefreshMesh builds a mesh whose selfID sorts AFTER every peer id (so refreshOnce
// never spawns a dial goroutine — relay.go only dials when selfID < peer id), with a
// spy sink and a mutable static lister. Pure-Go: no real HTTP dialing occurs.
func newRefreshMesh(sink *spySink, lister *mutableLister) *peerMesh {
	m := &peerMesh{selfID: "zzz-self", sink: sink, lister: lister, selfCredit: map[model.KindVersion]int32{}}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	empty := map[string]*peerClient{}
	m.peers.Store(&empty)
	return m
}

type mutableLister struct{ members []store.ClusterMember }

func (l *mutableLister) ListLiveClusterMembers(context.Context, time.Duration) ([]store.ClusterMember, error) {
	return l.members, nil
}

func brokerMember(id, url string) store.ClusterMember {
	return store.ClusterMember{MemberID: id, Role: "broker", Config: AdvertiseAddr(nil, url)}
}

// TestRefreshOnceCancelsDepartedPeer: when a peer disappears from cluster_members,
// refreshOnce removes it from the published set and cancels its ctx (stopping its route
// reconnect loop). A task this broker forwarded to the departed peer is recovered by the
// reaper — its heartbeat (attested on the departed peer) lapses and the row re-claims
// under a bumped claim_epoch — so there is no per-departure wake to fire.
func TestRefreshOnceCancelsDepartedPeer(t *testing.T) {
	sink := &spySink{}
	lister := &mutableLister{members: []store.ClusterMember{brokerMember("aaa", "http://a")}}
	m := newRefreshMesh(sink, lister)
	defer m.cancel()

	m.refreshOnce(m.ctx) // peer "aaa" joins
	pA := m.loadPeers()["aaa"]
	if pA == nil {
		t.Fatal("peer aaa not created on first refresh")
	}

	lister.members = nil // aaa departs
	m.refreshOnce(m.ctx)

	if _, ok := m.loadPeers()["aaa"]; ok {
		t.Fatal("departed peer still in the published set")
	}
	if pA.ctx.Err() == nil {
		t.Fatal("departed peer's ctx was not cancelled")
	}
}

// livenessLister is a MemberLister that stamps each member with a heartbeat age and
// applies the SAME liveness filter the real store query does — so a member whose
// heartbeat is older than the window is excluded from the result, exactly as a
// hard-crashed peer (row not yet GC'd) is. Lets the test drive the mesh's crash-
// eviction path without a live DB.
type livenessLister struct {
	// members maps a member to how long ago it last beat; the read returns only
	// those within the window passed to ListLiveClusterMembers.
	members map[string]livenessEntry
}

type livenessEntry struct {
	m   store.ClusterMember
	age time.Duration // how long ago this member last heartbeat
}

func (l *livenessLister) ListLiveClusterMembers(_ context.Context, window time.Duration) ([]store.ClusterMember, error) {
	var out []store.ClusterMember
	for _, e := range l.members {
		if e.age <= window { // the liveness predicate the store SQL applies
			out = append(out, e.m)
		}
	}
	return out, nil
}

// TestRefreshOnceEvictsStalePeer proves the crash-recovery fix: a peer whose
// heartbeat has lapsed past the mesh's liveness window falls out of the LIVE member
// read, so refreshOnce cancels its route + wakes parked pushes (onPeerGone) — the
// mesh stops routing to a hard-crashed peer within one window instead of waiting out
// the ClusterMemberGC TTL. Distinct from TestRefreshOnceCancelsDepartedPeer (a clean
// DELETE): here the row STILL EXISTS, only its heartbeat is stale.
func TestRefreshOnceEvictsStalePeer(t *testing.T) {
	sink := &spySink{}
	const window = 30 * time.Second
	lister := &livenessLister{members: map[string]livenessEntry{
		"aaa": {m: brokerMember("aaa", "http://a"), age: 0}, // fresh
	}}
	m := &peerMesh{selfID: "zzz-self", sink: sink, lister: lister, livenessWindow: window, selfCredit: map[model.KindVersion]int32{}}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	empty := map[string]*peerClient{}
	m.peers.Store(&empty)
	defer m.cancel()

	m.refreshOnce(m.ctx) // "aaa" is live → routed
	pA := m.loadPeers()["aaa"]
	if pA == nil {
		t.Fatal("live peer aaa not created")
	}

	// "aaa" hard-crashes: its row lingers but its heartbeat ages past the window.
	lister.members["aaa"] = livenessEntry{m: brokerMember("aaa", "http://a"), age: 2 * window}
	m.refreshOnce(m.ctx)

	if _, ok := m.loadPeers()["aaa"]; ok {
		t.Fatal("stale peer still in the published set (should be evicted on liveness)")
	}
	if pA.ctx.Err() == nil {
		t.Fatal("stale peer's route ctx was not cancelled")
	}
	if sink.goneCount() == 0 {
		t.Fatal("onPeerGone was not fired for the evicted stale peer (parked pushes would strand)")
	}
}

// TestRefreshOnceReusesLivePeerPointer: an unchanged (id+addr) member keeps the SAME
// *peerClient across refreshes (so its route + credit survive); an ADDRESS change
// replaces it with a fresh pointer and cancels the old one.
func TestRefreshOnceReusesLivePeerPointer(t *testing.T) {
	sink := &spySink{}
	lister := &mutableLister{members: []store.ClusterMember{brokerMember("aaa", "http://a1")}}
	m := newRefreshMesh(sink, lister)
	defer m.cancel()

	m.refreshOnce(m.ctx)
	p1 := m.loadPeers()["aaa"]
	p1.setInterest(model.KindVersion{Kind: "noop", Version: 1}, 7, true) // credit we expect to survive an unchanged refresh

	m.refreshOnce(m.ctx) // same id+addr → reuse
	if got := m.loadPeers()["aaa"]; got != p1 {
		t.Fatal("unchanged peer got a NEW pointer (route + credit would be lost)")
	}
	if p1.getCredit(model.KindVersion{Kind: "noop", Version: 1}) != 7 {
		t.Fatal("reused peer lost its credit across an unchanged refresh")
	}

	lister.members = []store.ClusterMember{brokerMember("aaa", "http://a2")} // addr changed
	m.refreshOnce(m.ctx)
	if got := m.loadPeers()["aaa"]; got == p1 {
		t.Fatal("address change reused the stale pointer; want a fresh peerClient")
	}
	if p1.ctx.Err() == nil {
		t.Fatal("old peer (addr changed) was not cancelled")
	}
}

// ── onRouteTask (accept a peer-pushed reference) ──────────────────────────────────

// TestOnRouteTask covers accepting a peer-pushed task REFERENCE: the reference is
// enqueued on the local kind buffer for a worker to run AND registered in-flight with a
// resultCh + a foreignOwner mapping, so when the local worker Completes it the result is
// RETURNED to the owning peer over its route (a `complete` frame) — the owner holds the
// lease + does the fenced write. When the buffer is full the reference is dropped (no
// state left behind) — never blocking the route receive loop — and the owner's reaper
// re-claims it.
func TestOnRouteTask(t *testing.T) {
	t.Run("buffer-has-room-forwards-result-home", func(t *testing.T) {
		f := newFanoutExecutor()
		owner := &peerClient{memberID: "owner", out: make(chan *meshpb.RouteFrame, 1)}
		q := f.queueFor(model.KindVersion{Kind: "noop", Version: 1}) // cap 1024, has room
		task := f.baseTask("noop", 1, 0, "work", &model.Env{}, uuid.New(), 1)
		task.TaskDeadlineMs = 5000
		token := task.GetLeaseToken()

		f.onRouteTask(owner, task)

		// The reference is registered in-flight (so our worker's Complete resolves it) with
		// a resultCh but no local lease, and mapped to its owner for the result return.
		f.mu.Lock()
		ps, live := f.inflight[token]
		gotOwner := f.foreignOwner[token]
		f.mu.Unlock()
		if !live || ps == nil || ps.resultCh == nil {
			t.Fatal("onRouteTask did not register the foreign reference with a resultCh")
		}
		if ps.lease.WorkID != uuid.Nil {
			t.Fatal("foreign reference holds a local lease; the owner keeps the work_queue row")
		}
		if gotOwner != owner {
			t.Fatal("foreignOwner not recorded for the token")
		}
		select {
		case got := <-q:
			if got.task.GetLeaseToken() != token {
				t.Fatal("wrong task enqueued")
			}
		default:
			t.Fatal("task was not enqueued on the kind buffer")
		}

		// A worker Complete for the token is delivered locally and FORWARDED HOME to the
		// owner over its route as a `complete` frame (the owner does the fenced write).
		sc := &workerpb.StageComplete{LeaseToken: token, ClaimEpoch: 7}
		f.deliver(token, sc)
		select {
		case fr := <-owner.out:
			if fr.GetComplete().GetLeaseToken() != token || fr.GetComplete().GetClaimEpoch() != 7 {
				t.Fatal("owner did not receive the forwarded StageComplete with its epoch")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("result was not forwarded home to the owner")
		}
	})

	t.Run("buffer-full-drops", func(t *testing.T) {
		f := newFanoutExecutor()
		owner := &peerClient{memberID: "owner", out: make(chan *meshpb.RouteFrame, 1)}
		// Pre-seed a cap-0 (unbuffered, no reader) queue so the non-blocking enqueue fails.
		f.mu.Lock()
		f.byKind[model.KindVersion{Kind: "noop", Version: 1}] = make(chan *pendingStage) // cap 0, no reader → send hits default
		f.mu.Unlock()
		task := f.baseTask("noop", 1, 0, "work", &model.Env{}, uuid.New(), 1)
		token := task.GetLeaseToken()

		f.onRouteTask(owner, task) // must not block or panic

		// Dropped: no local inflight / owner state left behind.
		f.mu.Lock()
		_, live := f.inflight[token]
		_, mapped := f.foreignOwner[token]
		f.mu.Unlock()
		if live || mapped {
			t.Fatal("buffer-full path left local state; the reference should be dropped clean")
		}
	})
}
