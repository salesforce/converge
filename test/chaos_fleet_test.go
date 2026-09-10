package test

// chaos_fleet.go — a supervised fleet of REAL brokers and workers for the
// fault-injection chaos test (broker_chaos_test.go). Each broker and worker is a
// managed "process": a goroutine tree with its own context that can be started,
// gracefully drained (SIGTERM-equivalent), hard-killed (crash-equivalent), and
// replaced — so the chaos driver can churn cluster membership the way k8s does on
// a rollout / node loss, while the workload keeps flowing over the real Connect path.
//
// Everything here exercises the PRODUCTION seams: brokers serve broker.Server over
// a real net.Listener; workers are real converge.RunWorker loops dialing over a
// real *http.Client through a fault-injecting transport (see faultDialer). Nothing
// is in-process-shortcut — a task travels resource → broker claim → Connect WorkStream
// (bidi) → worker handler → StageComplete UP the same stream → fenced AppendOutbox,
// exactly as in prod.

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// ── fault-injecting transport ────────────────────────────────────────────────
//
// faultConn wraps a net.Conn so the chaos driver can, per-connection:
//   - CUT it abruptly (RST-like): every subsequent Read/Write errors immediately,
//     modelling a broker crash / LB drop / cable pull the peer notices fast.
//   - BLACKHOLE it (half-open partition): Reads block forever and Writes silently
//     succeed-then-vanish, modelling a silent NAT/firewall drop with no FIN — the
//     exact case the keepalive must detect (a naive stream would hang forever).
//
// A single *faultGate shared by a worker's transport flips all its live conns at
// once, so "partition worker W from the fleet" is one atomic store.

type faultMode int32

const (
	faultNone      faultMode = iota
	faultCut                 // hard RST: reads+writes fail now
	faultBlackhole           // silent half-open: reads hang, writes vanish
	faultRecvOnly            // asymmetric: reads hang (can't receive), writes ok
	faultSendOnly            // asymmetric: writes vanish (can't send), reads ok
	faultLatency             // degraded-not-cut: every I/O delayed, none fail
)

// faultLatencyDelay is the per-I/O delay injected by faultLatency (models a slow
// link that flirts with RPC timeouts without a clean break).
const faultLatencyDelay = 150 * time.Millisecond

// faultGate is the shared switch a worker's transport consults. Atomic so the
// chaos goroutine flips it without locking the data path.
type faultGate struct{ mode atomic.Int32 }

func (g *faultGate) set(m faultMode) { g.mode.Store(int32(m)) }
func (g *faultGate) get() faultMode  { return faultMode(g.mode.Load()) }

// faultConn applies the gate's current mode to every I/O op on one connection.
type faultConn struct {
	net.Conn
	gate    *faultGate
	blocked chan struct{} // closed once; unblocks a parked blackhole Read on heal/close
	once    sync.Once
}

var errChaosCut = fmt.Errorf("chaos: connection cut (simulated RST)")

func (c *faultConn) Read(b []byte) (int, error) {
	switch c.gate.get() {
	case faultCut:
		return 0, errChaosCut
	case faultBlackhole, faultRecvOnly:
		// Reads park as if no bytes ever arrive (half-open / recv-only). They wake only
		// when the conn is closed (heal/teardown), then report the cut — mirroring a
		// real socket whose keepalive eventually tears the dead connection down.
		<-c.blocked
		return 0, errChaosCut
	case faultLatency:
		time.Sleep(faultLatencyDelay)
		return c.Conn.Read(b)
	default: // faultNone, faultSendOnly (reads ok)
		return c.Conn.Read(b)
	}
}

func (c *faultConn) Write(b []byte) (int, error) {
	switch c.gate.get() {
	case faultCut:
		return 0, errChaosCut
	case faultBlackhole, faultSendOnly:
		return len(b), nil // vanishes: the peer never receives it, but Write "succeeds"
	case faultLatency:
		time.Sleep(faultLatencyDelay)
		return c.Conn.Write(b)
	default: // faultNone, faultRecvOnly (writes ok)
		return c.Conn.Write(b)
	}
}

func (c *faultConn) Close() error {
	c.once.Do(func() { close(c.blocked) })
	return c.Conn.Close()
}

// enableH2C flips a plaintext broker http.Server to accept cleartext HTTP/2 (h2c)
// via the Go 1.24+ stdlib http.Protocols — the mirror of the production
// configureBrokerH2C. The bidirectional WorkStream (a worker's Subscribe + the
// broker's task pushes on ONE long-lived request) requires HTTP/2; on this
// non-TLS listener that means h2c. HTTP/1 is kept on so a non-stream health/probe
// request still works, but the stream negotiates h2. Without this the fleet's
// workers can't subscribe and nothing is ever claimed.
func enableH2C(srv *http.Server) {
	var protos http.Protocols
	protos.SetHTTP1(true)
	protos.SetHTTP2(true)
	protos.SetUnencryptedHTTP2(true)
	srv.Protocols = &protos
}

// enableH2CTransport configures a worker's http.Transport for PRIOR-KNOWLEDGE
// cleartext HTTP/2 (h2c) on a plaintext connection — the client mirror of enableH2C
// and of the production converge.Serve transport. HTTP/1 is OFF so every dial is h2c
// (the broker listener speaks it); the caller's custom DialContext is preserved
// (Protocols governs the protocol, DialContext the raw connection).
func enableH2CTransport(tr *http.Transport) {
	var protos http.Protocols
	protos.SetUnencryptedHTTP2(true)
	tr.Protocols = &protos
	tr.ForceAttemptHTTP2 = false // Protocols governs; avoid the h1+ForceHTTP2 ambiguity
}

// faultDialer builds a DialContext that wraps every dialed connection in a
// faultConn bound to the given gate, so the chaos driver can partition this
// client from the fleet by flipping the gate. Live conns are tracked so a heal
// can force-close the blackholed ones (the worker then reconnects clean).
type faultDialer struct {
	gate *faultGate
	base net.Dialer

	mu    sync.Mutex
	conns map[*faultConn]struct{}
}

func newFaultDialer(gate *faultGate) *faultDialer {
	return &faultDialer{
		gate:  gate,
		base:  net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second},
		conns: make(map[*faultConn]struct{}),
	}
}

func (d *faultDialer) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	// A CUT gate refuses NEW dials too (the broker is unreachable), so a reconnect
	// during a partition fails fast instead of connecting to a "dead" peer.
	if d.gate.get() == faultCut {
		return nil, errChaosCut
	}
	raw, err := d.base.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	fc := &faultConn{Conn: raw, gate: d.gate, blocked: make(chan struct{})}
	d.mu.Lock()
	d.conns[fc] = struct{}{}
	d.mu.Unlock()
	return fc, nil
}

// forceCloseAll tears down every tracked conn — used on HEAL so a worker parked in
// a blackholed Read wakes and reconnects over a fresh, healthy connection (the
// keepalive would do this in prod; the test forces it deterministically on heal).
func (d *faultDialer) forceCloseAll() {
	d.mu.Lock()
	conns := make([]*faultConn, 0, len(d.conns))
	for c := range d.conns {
		conns = append(conns, c)
	}
	d.conns = make(map[*faultConn]struct{})
	d.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// ── managed broker ───────────────────────────────────────────────────────────

// managedBroker is one restartable broker process: a broker.Server on a real
// net.Listener, its dispatcher + relay loops, and cluster_members registration.
// Stop() drains gracefully (release claims); Kill() severs the listener abruptly
// (crash — no release, the reaper/keepalive must recover its leases).
type managedBroker struct {
	id        string
	pool      *pgxpool.Pool
	mc        *runtime.KindManifestCache
	shards    *runtime.ShardSet
	relayHTTP connect.HTTPClient
	relayGate *faultGate // cuts THIS broker's peer-relay reachability (Route/RelayComplete)
	lister    broker.MemberLister
	reactors  bool // also run a ReactorDispatcher (drains lifecycle_outbox over STAGE_REACT)

	mu      sync.Mutex
	srv     *broker.Server
	httpSrv *http.Server
	ln      net.Listener
	addr    string
	cancel  context.CancelFunc
	done    chan struct{}
	running bool
}

// start brings the broker up: bind a fresh listener, build the Server, register in
// cluster_members with its advertised Connect addr, and run the dispatcher + relay.
func (b *managedBroker) start(t *testing.T, parent context.Context) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.running {
		return
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("broker %s listen: %v", b.id, err)
	}
	b.ln = ln
	b.addr = "http://" + ln.Addr().String()

	srv := broker.NewDispatch(b.pool, b.mc, b.id, 16, b.shards)
	// Short remote ceiling so a wedged/partitioned worker's task is reclaimed in
	// chaos-relevant time (not the 30-min prod default).
	srv.SetRemoteFanoutCeiling(45 * time.Second)
	if b.relayHTTP != nil {
		srv.EnableRelay(b.id, b.relayHTTP, b.lister, runtime.NewPgxListener(b.pool), 500*time.Millisecond, runtime.DefaultMeshLivenessWindow)
	}
	b.srv = srv

	mux := http.NewServeMux()
	mux.Handle(srv.Handler())
	b.httpSrv = &http.Server{Handler: mux}
	// The WorkStream is a BIDIRECTIONAL h2c stream (f56b25b): a worker's Subscribe
	// and the broker's task pushes ride one long-lived HTTP/2 request. On this
	// plaintext listener that needs cleartext HTTP/2 (h2c) — an HTTP/1.1 server
	// silently fails the bidi stream, so the worker never subscribes, HasSubscriber
	// never opens the claim gate, and nothing is ever claimed. Mirror the production
	// configureBrokerH2C so the chaos fleet speaks the same transport as prod.
	enableH2C(b.httpSrv)

	ctx, cancel := context.WithCancel(parent)
	b.cancel = cancel
	b.done = make(chan struct{})
	b.running = true

	// register the row so the relay mesh + resharder see this member.
	b.register(ctx)

	// With reactors on, this broker also runs a ReactorDispatcher wired to its OWN
	// fanout executor (StageDispatcher) + HasWorkerForKind gate — exactly how claimDuty
	// wires it in production. It drains lifecycle_outbox rows in this broker's shard range
	// and ships STAGE_REACT to a connected worker; it lives on the broker's ctx, so a
	// kill/drain tears it down and a restart brings a fresh one up — the reactor spine
	// churns with the broker. A short heartbeat + stale window so a killed broker's
	// in-flight reactor claim is reclaimed in chaos-relevant time.
	var rx *runtime.ReactorDispatcher
	if b.reactors {
		rx = runtime.NewReactorDispatcher(store.New(b.pool), runtime.NewPgxListener(b.pool), srv.StageDispatcher(), b.id)
		rx.Shards = b.shards
		rx.ReadyToDispatch = srv.HasWorkerForKind
		rx.HeartbeatEvery = 3 * time.Second
	}

	go func() {
		defer close(b.done)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); _ = b.httpSrv.Serve(ln) }()
		go func() { defer wg.Done(); _ = srv.Dispatcher().Run(ctx) }()
		go func() { defer wg.Done(); srv.StartRelay(ctx) }()
		if rx != nil {
			wg.Add(1)
			go func() { defer wg.Done(); _ = rx.Run(ctx) }()
		}
		// periodic heartbeat so cluster_members stays fresh + advertises live workers.
		go func() {
			tk := time.NewTicker(500 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tk.C:
					b.register(ctx)
				}
			}
		}()
		wg.Wait()
	}()
}

// register upserts this broker's cluster_members row advertising its Connect addr +
// live worker kinds (the DynamicConfig equivalent the prod reporter does).
func (b *managedBroker) register(ctx context.Context) {
	lo, hi := b.shards.Bounds()
	span := &[2]int16{lo, hi}
	if hi < lo {
		span = nil
	}
	cfg := broker.AdvertiseAddr(nil, b.addr)
	// BOUNDED: never block a heartbeat/start on pool-acquire — under heavy fleet
	// churn the pgx pool can be momentarily saturated, and an unbounded UpsertM here
	// would wedge the caller (and, if it's the chaos goroutine, stall cancellation).
	// A missed beat is harmless (the next one refreshes; the GC covers a truly dead row).
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = store.New(b.pool).UpsertClusterMember(rctx, store.ClusterMemberInfo{
		MemberID: b.id, Role: "broker", Shards: span,
		Config: cfg, StartedAt: time.Unix(0, 0),
	}, 0, nil)
}

// stop drains gracefully: stop claiming, release this broker's claims (fast
// handoff), deregister, then close the listener. Models a clean SIGTERM rollout.
func (b *managedBroker) stop(ctx context.Context) {
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return
	}
	b.running = false
	cancel, done, srv, httpSrv, id := b.cancel, b.done, b.srv, b.httpSrv, b.id
	b.mu.Unlock()

	cancel() // stops dispatcher + relay + heartbeat
	// Graceful drain window, then release + deregister (mirrors cmd/converge).
	relCtx, rc := context.WithTimeout(ctx, 5*time.Second)
	_, _ = srv.Dispatcher().ReleaseClaims(relCtx)
	rc()
	_ = store.New(b.pool).DeleteClusterMember(ctx, id)
	shutCtx, sc := context.WithTimeout(ctx, 3*time.Second)
	_ = httpSrv.Shutdown(shutCtx)
	sc()
	// Shutdown may not force-close a hung streaming conn; bound the join so a
	// wedged WorkStream stream can't pin cleanup. Close the listener to be sure.
	_ = b.ln.Close()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
	}
}

// kill CRASHES the broker: close the listener abruptly and cancel, but do NOT
// release claims or deregister — the reaper (StaleAfter) and keepalive must
// recover its stranded leases. Models an OOM-kill / node loss.
func (b *managedBroker) kill() {
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return
	}
	b.running = false
	cancel, done, ln := b.cancel, b.done, b.ln
	b.mu.Unlock()

	_ = ln.Close() // abrupt: in-flight WorkStream streams get a hard error
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
	}
	// NOTE: cluster_members row is left behind on purpose — the ClusterMemberGC /
	// stale reaper is what should reclaim a crashed broker, and we assert it does.
}

func (b *managedBroker) isRunning() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.running
}

// ── managed worker ───────────────────────────────────────────────────────────

// managedWorker is one restartable dumb worker: a converge.RunWorker loop dialing
// a target broker over a fault-injectable transport. Stop() drains (SIGTERM),
// Kill() cancels hard.
type managedWorker struct {
	id        string
	providers []converge.Provider
	dialAddr  string     // the broker URL it connects to
	gate      *faultGate // this worker's partition switch
	dialer    *faultDialer

	mu         sync.Mutex
	cancel     context.CancelFunc
	done       chan struct{}
	running    bool
	drainGrace time.Duration
}

func (w *managedWorker) start(t *testing.T, parent context.Context) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running {
		return
	}
	if w.gate == nil {
		w.gate = &faultGate{}
	}
	w.dialer = newFaultDialer(w.gate)
	tr := &http.Transport{
		DialContext:         w.dialer.dial,
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     30 * time.Second,
	}
	// The bidirectional WorkStream needs cleartext HTTP/2 (h2c) over this plaintext
	// transport (see the broker's enableH2C above + the production worker transport
	// in sdk-go/converge). HTTP/1.1 can't carry the bidi stream, so the worker would
	// never complete Subscribe. HTTP/1 OFF: every dial is h2c (prior-knowledge),
	// matching the broker listener. The custom DialContext (fault injection) is
	// preserved — Protocols governs the protocol, DialContext the raw connection.
	enableH2CTransport(tr)
	httpClient := &http.Client{Transport: tr}
	client := workerpbconnect.NewWorkerServiceClient(httpClient, w.dialAddr)

	ctx, cancel := context.WithCancel(parent)
	w.cancel = cancel
	w.done = make(chan struct{})
	w.running = true

	go func() {
		defer close(w.done)
		// RunWorker BLOCKS until ctx is cancelled and OWNS the reconnect-with-backoff
		// loop internally (subscribe/pull/dispatch/readiness/reconnect), so a stream
		// drop under fault injection re-establishes without an outer loop. A restart is
		// modelled by stop() cancelling this ctx + start() relaunching a fresh RunWorker.
		_ = converge.RunWorker(ctx, client, w.providers, converge.RunOptions{MaxInflight: 8, DrainGrace: w.drainGrace})
	}()
}

// stop drains (ctx cancel → Runner finishes in-flight within DrainGrace).
func (w *managedWorker) stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	cancel, done, dialer := w.cancel, w.done, w.dialer
	w.mu.Unlock()
	cancel()
	dialer.forceCloseAll() // unblock any parked blackholed read so Run returns
	// Bounded join: a worker wedged in a Complete-retry against a dead broker could
	// otherwise pin cleanup past the test timeout. The goroutine is already
	// cancelled + its conns closed, so give it a grace window then move on (it will
	// exit once its bounded retries drain; its leases fall to the reaper regardless).
	select {
	case <-done:
	case <-time.After(20 * time.Second):
	}
}

// kill is the same cancel here (the Runner's own drain is bounded); the crash-ness
// is modelled by cutting the gate FIRST (no clean Complete gets out). Callers that
// want a hard crash set the gate to faultCut, then kill.
func (w *managedWorker) kill() { w.stop() }

func (w *managedWorker) isRunning() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.running
}

// partition flips this worker's gate: faultCut (RST) or faultBlackhole (half-open).
func (w *managedWorker) partition(m faultMode) {
	if w.gate != nil {
		w.gate.set(m)
	}
}

// heal clears the gate and force-closes the poisoned conns so the worker
// reconnects clean.
func (w *managedWorker) heal() {
	if w.gate != nil {
		w.gate.set(faultNone)
	}
	w.mu.Lock()
	dialer := w.dialer
	w.mu.Unlock()
	if dialer != nil {
		dialer.forceCloseAll()
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func kindStrs(ks []model.Kind) []string {
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return out
}
