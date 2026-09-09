package broker

// mesh_credit_test.go — DETERMINISTIC, pure-Go unit tests for the worker↔broker
// SUBSCRIBER + CREDIT + CONFIG-BROADCAST + REQUEUE lifecycle (broker.go / connect.go):
// subscriber ref-counting (the 0↔1 presence edge), maxInflight clamp, creditForLocked
// summation, non-blocking config broadcast, and the requeue / forward stop-branch that
// return work instead of leaking on a worker drop. No network, no Postgres.

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// ── subscriber ref-count + maxInflight clamp ──────────────────────────────────────

// TestSubscriberRefCountGatesLastLeaver: HasSubscriber(kind) is a REF COUNT, not a
// boolean — true after N addSubscriber, stays true as they leave, false only when the
// LAST one removes. This is the 0↔1 presence edge the mesh RS+/RS- rides.
func TestSubscriberRefCountGatesLastLeaver(t *testing.T) {
	f := newFanoutExecutor()
	if f.HasSubscriber("noop", 1) {
		t.Fatal("HasSubscriber true with no workers")
	}
	subA := f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 2, peerIdentity{id: "worker-a"})
	subB := f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 2, peerIdentity{id: "worker-b"})
	if !f.HasSubscriber("noop", 1) {
		t.Fatal("HasSubscriber false with two workers")
	}
	f.removeSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, subA)
	if !f.HasSubscriber("noop", 1) {
		t.Fatal("HasSubscriber flipped false while one worker (B) remains — the 0↔1 edge is wrong")
	}
	f.removeSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, subB)
	if f.HasSubscriber("noop", 1) {
		t.Fatal("HasSubscriber still true after the LAST worker left")
	}
	// An extra remove must not underflow the counter below zero.
	f.removeSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, subB)
	f.mu.Lock()
	n := f.subscribers["noop"][1]
	f.mu.Unlock()
	if n < 0 {
		t.Fatalf("subscriber count went negative (%d) on an extra remove", n)
	}
}

// TestAddSubscriberClampsMaxInflightToOne: a worker advertising maxInflight <= 0 still
// gets one slot (clamped to 1) — a zero/negative ceiling must not mean "no credit ever".
func TestAddSubscriberClampsMaxInflightToOne(t *testing.T) {
	f := newFanoutExecutor()
	for _, in := range []int{0, -5} {
		sub := f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, in, peerIdentity{id: "w"})
		if sub.maxInflight != 1 {
			t.Fatalf("addSubscriber(%d).maxInflight = %d; want 1 (clamped)", in, sub.maxInflight)
		}
		f.removeSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, sub)
	}
	sub := f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 7, peerIdentity{id: "w"})
	if sub.maxInflight != 7 {
		t.Fatalf("addSubscriber(7).maxInflight = %d; want 7 (positive passes through)", sub.maxInflight)
	}
}

// TestCreditForLockedSumsRemainingAcrossStreams: creditForLocked(kind) sums
// max(0, maxInflight-inflight) over ONLY the streams advertising the kind; a saturated
// or over-subscribed stream contributes 0 (never negative), and streams for other kinds
// don't count.
func TestCreditForLockedSumsRemainingAcrossStreams(t *testing.T) {
	f := newFanoutExecutor()
	mk := func(km model.KindVersion, max, inflight int) *configSub {
		return &configSub{kinds: map[model.KindVersion]struct{}{km: {}}, maxInflight: max, inflight: inflight}
	}
	noop := model.KindVersion{Kind: "noop", Version: 1}
	f.mu.Lock()
	f.configSubs[mk(noop, 5, 2)] = struct{}{}                                         // 3 free
	f.configSubs[mk(noop, 2, 2)] = struct{}{}                                         // 0 free (saturated)
	f.configSubs[mk(noop, 5, 6)] = struct{}{}                                         // over-subscribed → 0, NOT -1
	f.configSubs[mk(model.KindVersion{Kind: "other", Version: 1}, 9, 0)] = struct{}{} // different kind → ignored
	got := f.creditForLocked(noop)
	f.mu.Unlock()
	if got != 3 {
		t.Fatalf("creditForLocked(noop) = %d; want 3 (3 + 0 + 0, other-kind ignored, never negative)", got)
	}
}

// ── non-blocking config broadcast (never wedge the config loop) ───────────────────

// TestBroadcastConfigDropsOnFullSubChannel: broadcastConfig pushes onto a sub's
// buffered ch with room, and silently DROPS (never blocks) when that ch is full — the
// worker's periodic GetProviderConfig poll is the backstop. Never blocks the caller.
func TestBroadcastConfigDropsOnFullSubChannel(t *testing.T) {
	f := newFanoutExecutor()
	sub := f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 1, peerIdentity{id: "w"})
	// Fill sub.ch to capacity (addSubscriber makes it cap 64).
	f.mu.Lock()
	capCh := cap(sub.ch)
	f.mu.Unlock()
	for i := 0; i < capCh; i++ {
		sub.ch <- &workerpb.WorkStreamServerMsg{}
	}
	done := make(chan struct{})
	go func() { f.broadcastProviderConfig("noop", 1, []byte(`{"v":1}`), nil); close(done) }()
	select {
	case <-done: // must return promptly despite the full channel
	case <-time.After(2 * time.Second):
		t.Fatal("broadcastConfig blocked on a full sub channel")
	}
	if len(sub.ch) != capCh {
		t.Fatalf("sub.ch len = %d; want %d (the broadcast was dropped, not enqueued)", len(sub.ch), capCh)
	}
	// Drain one and re-broadcast — now it lands.
	<-sub.ch
	f.broadcastProviderConfig("noop", 1, []byte(`{"v":2}`), nil)
	if len(sub.ch) != capCh {
		t.Fatalf("after draining one, a re-broadcast did not land: len=%d want %d", len(sub.ch), capCh)
	}
}

// TestBroadcastOnlyReachesAdvertisingSubs: broadcastConfig(K, 1) enqueues ONLY on
// subs advertising the EXACT (K, 1) — the strict per-(kind, kindVersion) fan-out filter.
func TestBroadcastOnlyReachesAdvertisingSubs(t *testing.T) {
	f := newFanoutExecutor()
	subK := f.addSubscriber([]model.KindVersion{{Kind: "K", Version: 1}}, 1, peerIdentity{id: "wk"})
	subL := f.addSubscriber([]model.KindVersion{{Kind: "L", Version: 1}}, 1, peerIdentity{id: "wl"})
	subKL := f.addSubscriber([]model.KindVersion{{Kind: "K", Version: 1}, {Kind: "L", Version: 1}}, 1, peerIdentity{id: "wkl"})

	f.broadcastProviderConfig("K", 1, []byte(`{}`), nil)

	if len(subK.ch) != 1 {
		t.Fatalf("sub advertising K got %d config pushes; want 1", len(subK.ch))
	}
	if len(subKL.ch) != 1 {
		t.Fatalf("sub advertising K,L got %d; want 1", len(subKL.ch))
	}
	if len(subL.ch) != 0 {
		t.Fatalf("sub advertising only L got %d config pushes for kind K; want 0", len(subL.ch))
	}
	// And it's a ConfigUpdate for the right kind.
	select {
	case resp := <-subK.ch:
		if resp.GetConfig().GetKind() != "K" {
			t.Fatalf("config kind = %q; want K", resp.GetConfig().GetKind())
		}
	default:
		t.Fatal("no config on subK.ch")
	}
}

// ── requeue / forward stop-branch (return work, don't leak, on drop) ──────────────

// TestRequeueReturnsLiveStageDropsResolved: requeue re-buffers a stage still in
// f.inflight (so another worker gets it); a stage no longer inflight (already resolved)
// is silently dropped.
func TestRequeueReturnsLiveStageDropsResolved(t *testing.T) {
	f := newFanoutExecutor()
	stop := make(chan struct{})

	// Live: token in inflight → requeue re-appears on the kind buffer.
	live := &pendingStage{task: f.baseTask("noop", 1, 0, "work", &model.Env{}, uuid.New(), 1)}
	f.mu.Lock()
	f.inflight[live.task.GetLeaseToken()] = live
	f.mu.Unlock()
	f.requeue(live, stop)
	select {
	case ps := <-f.queueFor(model.KindVersion{Kind: "noop", Version: 1}):
		if ps != live {
			t.Fatal("requeue put a different stage on the buffer")
		}
	default:
		t.Fatal("requeue did not re-buffer a still-inflight stage")
	}

	// Resolved: token absent from inflight → requeue drops it (buffer stays empty).
	resolved := &pendingStage{task: f.baseTask("noop", 1, 0, "work", &model.Env{}, uuid.New(), 1)}
	f.requeue(resolved, stop)
	select {
	case <-f.queueFor(model.KindVersion{Kind: "noop", Version: 1}):
		t.Fatal("requeue re-buffered an already-resolved stage; want drop")
	default:
	}
}

// TestForwardStopBranchSplitsLocalVsForeign: forward()'s stop arm (worker stream tore
// down before hand-off) splits by ps.resultCh. A LOCAL stage (resultCh non-nil) is
// routed to requeue so another worker gets it; a FORWARDED reference (resultCh nil) is
// simply dropped — the forwarding peer keeps the work_queue row, whose heartbeat lapses
// and the reaper re-issues it. Both return false. We assert the deterministic parts:
// forward returns false without panicking on both, and a dropped reference leaves no
// state behind (this broker holds no lease/inflight entry for it). (The local branch's
// re-buffer is NOT asserted here: forward passes the SAME stop to requeue, and with a
// closed stop + a ready buffer Go's select picks nondeterministically — the reliable
// requeue-lands behaviour is covered by TestRequeueReturnsLiveStageDropsResolved with an
// OPEN stop.)
func TestForwardStopBranchSplitsLocalVsForeign(t *testing.T) {
	f := newFanoutExecutor()
	merged := make(chan *pendingStage) // unbuffered, no reader → forward must take the stop arm
	stop := make(chan struct{})
	close(stop) // force the stop arm of forward()'s select (a shutdown)

	// LOCAL: resultCh non-nil → forward returns false without panicking.
	local := &pendingStage{task: f.baseTask("noop", 1, 0, "work", &model.Env{}, uuid.New(), 1), resultCh: make(chan *workerpb.StageComplete, 1)}
	f.mu.Lock()
	f.inflight[local.task.GetLeaseToken()] = local
	f.mu.Unlock()
	if forward(local, merged, stop, f) {
		t.Fatal("forward returned true on a closed stop; want false")
	}

	// FORWARDED reference: no resultCh (the forwarding peer owns the row). forward drops
	// it on stop — no requeue, no state, no panic — and returns false.
	ftask := f.baseTask("noop", 1, 0, "work", &model.Env{}, uuid.New(), 1)
	ftoken := ftask.GetLeaseToken()
	foreign := &pendingStage{task: ftask} // resultCh nil = a peer's reference
	if forward(foreign, merged, stop, f) {
		t.Fatal("forward returned true for a forwarded reference on stop; want false")
	}
	f.mu.Lock()
	_, live := f.inflight[ftoken]
	f.mu.Unlock()
	if live {
		t.Fatal("a dropped forwarded reference left an inflight entry; this broker holds no lease for it")
	}
}
