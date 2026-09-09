package broker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/meshpb"
	"github.com/salesforce/converge/internal/meshpb/meshpbconnect"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/workerpb"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// h2cClient is the HTTP/2 client the mesh + worker use for the bidi Route stream in
// tests. The fixtures below serve HTTP/2 over TLS (httptest EnableHTTP2 + StartTLS),
// so this is an ALPN-h2 client that skips cert verification (a single shared client
// lets any broker dial any peer — the per-server httptest certs differ). Using the
// TLS h2 path (not h2c) sidesteps a stdlib data race in http2ConfigureServer when
// many cleartext-HTTP/2 httptest servers start concurrently under -race.
func h2cClient() connect.HTTPClient {
	var protos http.Protocols
	protos.SetHTTP2(true)
	return &http.Client{Transport: &http.Transport{
		Protocols:       &protos,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}}, //nolint:gosec // test-only
	}}
}

// brokerFixture is one broker: a fanout executor behind an httptest broker Connect server.
// The httptest server speaks h2c so the bidi Route stream works over plaintext.
type brokerFixture struct {
	f   *fanoutExecutor
	srv *httptest.Server
}

func newBrokerFixture(t *testing.T) *brokerFixture {
	t.Helper()
	f := newFanoutExecutor()
	h := &connectHandler{fanout: f}
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(h))
	mux.Handle(meshpbconnect.NewMeshServiceHandler(h))
	// Serve HTTP/2 over TLS (ALPN h2) so the bidi Route stream works. h2cClient()
	// dials with InsecureSkipVerify. (Prod uses h2c on the plaintext path; the TLS
	// path is used here to avoid a stdlib race in http2ConfigureServer under -race.)
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return &brokerFixture{f: f, srv: srv}
}

func mustAdvertise(url string) json.RawMessage { return AdvertiseAddr(nil, url) }

// TestPushKeepsLeaseOnOwner proves that pushing a task to a peer leaves the lease
// parked in the OWNER's inflight (ownership never transfers), so the owner still
// resolves + writes the fenced outbox. Fast + network-free: a mesh with one in-
// memory credited peer (big out buffer), then dispatchStage on a kind with no local
// worker → it must PUSH (a frame lands on the peer's out) AND keep the token in the
// owner's inflight.
func TestPushKeepsLeaseOnOwner(t *testing.T) {
	f := newFanoutExecutor()
	peer := &peerClient{memberID: "peer", out: make(chan *meshpb.RouteFrame, 16), credit: map[model.KindVersion]int32{{Kind: "noop", Version: 1}: 4}, serves: map[model.KindVersion]bool{{Kind: "noop", Version: 1}: true}}
	peers := map[string]*peerClient{"peer": peer}
	m := &peerMesh{selfID: "owner", sink: f, selfCredit: map[model.KindVersion]int32{}}
	m.peers.Store(&peers)
	f.mesh = m

	// dispatchStage with a ctx that we cancel right after asserting, so the parked
	// stage (whose pushed task never Completes here) unblocks promptly.
	ctx, cancel := context.WithCancel(context.Background())
	task := f.baseTask("noop", 1, 0, "work", &model.Env{}, uuid.New(), 1)
	token := task.GetLeaseToken()
	go func() { _, _ = f.dispatchStage(ctx, "noop", task) }()

	// The task must be PUSHED to the credited peer (a frame on its out) …
	select {
	case fr := <-peer.out:
		if fr.GetTask().GetLeaseToken() != token {
			t.Fatalf("pushed frame token = %q, want %q", fr.GetTask().GetLeaseToken(), token)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task was not pushed to the credited peer")
	}
	// … and the lease MUST still be parked in the OWNER's inflight (ownership never
	// transfers — the owner resolves it + does the fenced write when the peer's
	// Complete arrives home).
	f.mu.Lock()
	_, live := f.inflight[token]
	f.mu.Unlock()
	if !live {
		t.Fatal("owner dropped the lease after pushing — ownership must NOT transfer (fence would break)")
	}
	cancel()
}

// TestRelayDisabledIsInert proves the HEAD path: with no mesh wired, relayEnabled
// is false and dispatchStage serves only the LOCAL buffer — no push.
func TestRelayDisabledIsInert(t *testing.T) {
	f := newFanoutExecutor()
	if f.relayEnabled() {
		t.Fatal("mesh must be OFF by default (nil)")
	}
	if f.mesh != nil {
		t.Fatal("mesh must be nil by default")
	}
}

// TestPushTaskWeightedByCredit proves the distribution rule: pushTask picks a peer
// with probability proportional to its advertised credit (NATS queue-group weighted
// distribution), and never picks a peer with zero credit. pushTask returns only whether
// a peer took the frame, so we identify the LANDING peer by draining each peer's out
// channel after every push (exactly one lands per push here).
func TestPushTaskWeightedByCredit(t *testing.T) {
	f := newFanoutExecutor()
	// A mesh with three peers of credit 10 / 2 / 0 for "noop" and big send buffers.
	m := &peerMesh{selfID: "self", selfCredit: map[model.KindVersion]int32{}, sink: &spySink{}}
	noop := model.KindVersion{Kind: "noop", Version: 1}
	mk := func(id string, credit int32) *peerClient {
		p := &peerClient{memberID: id, out: make(chan *meshpb.RouteFrame, 100000), credit: map[model.KindVersion]int32{}, serves: map[model.KindVersion]bool{}}
		if credit > 0 {
			p.credit[noop] = credit
			p.serves[noop] = true
		}
		return p
	}
	peers := map[string]*peerClient{"a": mk("a", 10), "b": mk("b", 2), "c": mk("c", 0)}
	m.peers.Store(&peers)
	f.mesh = m

	// Push many tasks; count landings per peer (the peer whose out channel drained a
	// frame). Re-set credit each round so the optimistic decrement doesn't drain it to
	// zero (we're testing the pick weight, not the depletion — depletion is exercised by
	// the integration fairness test).
	const N = 6000
	counts := map[string]int{}
	for i := 0; i < N; i++ {
		peers["a"].setInterest(noop, 10, true)
		peers["b"].setInterest(noop, 2, true)
		if _, ok := m.pushTask(noop, &workerpb.StageTask{Kind: "noop", LeaseToken: uuid.NewString()}); !ok {
			t.Fatal("pushTask returned false despite credited peers")
		}
		// Find + drain the peer that took this push (exactly one lands per push).
		for id, p := range peers {
			select {
			case <-p.out:
				counts[id]++
			default:
			}
		}
	}
	if counts["c"] != 0 {
		t.Fatalf("peer c (credit 0) received %d tasks, want 0", counts["c"])
	}
	// a:b credit is 10:2 = 5:1. Assert a got materially more than b (loose bound to
	// avoid flakiness): a should be > 3× b.
	if !(counts["a"] > counts["b"]*3) {
		t.Fatalf("weighted split off: a=%d b=%d (want a ≈ 5×b, at least a > 3×b)", counts["a"], counts["b"])
	}
}

// syncLister is a MemberLister whose set the test can swap at runtime (from the
// test goroutine while the mesh reconcile reads it), to simulate a broker JOINING
// or LEAVING cluster_members between reconciles. Mutex-guarded because the swap
// and the read race across goroutines (unlike the read-only mutableLister).
type syncLister struct {
	mu      sync.Mutex
	members []store.ClusterMember
}

func (l *syncLister) ListLiveClusterMembers(context.Context, time.Duration) ([]store.ClusterMember, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]store.ClusterMember(nil), l.members...), nil
}
func (l *syncLister) set(members []store.ClusterMember) {
	l.mu.Lock()
	l.members = members
	l.mu.Unlock()
}

// triggerListener is a fake runtime.Listener the test drives by hand: Listen
// captures onNotify (and closes registered so fire() can't race ahead of the
// listener goroutine) then blocks until ctx ends, matching the real contract; the
// test calls fire() to simulate a cluster_changed NOTIFY.
type triggerListener struct {
	mu         sync.Mutex
	onNotify   func()
	registered chan struct{} // closed once Listen has captured onNotify
}

func newTriggerListener() *triggerListener {
	return &triggerListener{registered: make(chan struct{})}
}

func (l *triggerListener) Listen(ctx context.Context, _, _ string, onNotify func(), onReady ...func()) {
	l.mu.Lock()
	l.onNotify = onNotify
	l.mu.Unlock()
	close(l.registered)
	for _, r := range onReady {
		r()
	}
	<-ctx.Done()
}

// fire simulates a cluster_changed NOTIFY, waiting (bounded) for Listen to have
// registered so a fire() issued right after Run starts can't be dropped as a
// no-op against a not-yet-captured onNotify.
func (l *triggerListener) fire() {
	select {
	case <-l.registered:
	case <-time.After(2 * time.Second):
	}
	l.mu.Lock()
	fn := l.onNotify
	l.mu.Unlock()
	if fn != nil {
		fn()
	}
}

// TestMeshReactsToClusterChangedNotify proves the peerMesh reconciles its peer
// routes on a cluster_changed NOTIFY (the reactivity added so a broker JOIN is
// picked up promptly), NOT only on the slow failsafe poll. The failsafe here is
// deliberately LONG (1h) and the debounce SHORT, so a prompt reconcile can ONLY
// come from the NOTIFY path: a peer that appears in cluster_members after boot is
// dialed within the debounce window, well before the failsafe could fire.
func TestMeshReactsToClusterChangedNotify(t *testing.T) {
	self := newBrokerFixture(t) // provides a running broker (unused as a peer; we only need self's mesh)
	peer := newBrokerFixture(t)

	// Boot with an EMPTY peer set (only self, which the mesh excludes) and a long
	// failsafe so only the NOTIFY can drive a prompt reconcile.
	lister := &syncLister{members: []store.ClusterMember{
		{MemberID: "self", Role: "broker", Config: mustAdvertise(self.srv.URL)},
	}}
	tl := newTriggerListener()
	self.f.mesh = newPeerMesh("self", h2cClient(), lister, tl, time.Hour, time.Hour, self.f)
	self.f.mesh.debounceWindow = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go self.f.mesh.Run(ctx)

	// Initially no peers (self is excluded).
	waitFor(t, 2*time.Second, "boot with zero peers", func() bool {
		return len(self.f.mesh.loadPeers()) == 0
	})

	// A broker JOINS cluster_members. The failsafe is 1h, so a peer appearing within
	// the (2s) test window can ONLY come from the NOTIFY path: set the new member,
	// fire cluster_changed, and the mesh reconciles (after the 50ms debounce) and
	// dials the peer — long before the 1h failsafe could ever run.
	lister.set([]store.ClusterMember{
		{MemberID: "self", Role: "broker", Config: mustAdvertise(self.srv.URL)},
		{MemberID: "peer", Role: "broker", Config: mustAdvertise(peer.srv.URL)},
	})
	tl.fire()
	waitFor(t, 5*time.Second, "peer dialed after cluster_changed NOTIFY (failsafe is 1h, so this proves the NOTIFY path)", func() bool {
		_, ok := self.f.mesh.loadPeers()["peer"]
		return ok
	})

	// And a LEAVE reacts the same way: drop the peer, fire, assert it's torn down.
	lister.set([]store.ClusterMember{
		{MemberID: "self", Role: "broker", Config: mustAdvertise(self.srv.URL)},
	})
	tl.fire()
	waitFor(t, 5*time.Second, "peer dropped after cluster_changed NOTIFY", func() bool {
		_, ok := self.f.mesh.loadPeers()["peer"]
		return !ok
	})
}

// waitFor polls cond until true or the deadline, failing the test with msg.
func waitFor(t *testing.T, d time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.After(d)
	for {
		if cond() {
			return
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out waiting for: %s", msg)
		}
	}
}
