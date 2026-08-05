package broker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	"github.com/salesforce/converge/internal/meshpb"
	"github.com/salesforce/converge/internal/meshpb/meshpbconnect"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/pkg/wirelimits"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// relay.go implements the broker-to-broker MESH — a NATS-style presence router that
// decouples work PLACEMENT (which broker claims a shard tile) from CONSUMPTION
// (which worker runs it), WITHOUT a worker↔broker mesh and WITHOUT workers
// touching the DB. It is entirely OFF unless enabled (EnableRelay); with the mesh off
// (nil, single broker) the executor serves only LOCAL work.
//
// Model (NATS routes; owner-independent epoch fence):
//   - Persistent full mesh. Every broker holds ONE long-lived bidi Route stream to
//     every peer broker (the lower member_id dials, the higher accepts → exactly
//     one stream per pair). Peers are discovered leaderless from cluster_members
//     (each broker advertises its dial-able address under {"connect_addr": …}).
//   - Eager interest. Over the route each broker gossips its per-kind worker PRESENCE
//     (Interest frames = NATS RS+/RS-): has_worker is the correctness signal (does a
//     ready worker for the pair exist), and the credit int is a best-effort routing
//     HINT. Peers know the interest graph before any task arrives — no polling.
//   - Forward-on-claim. The instant a broker claims a task it has NO local worker for,
//     it forwards the DURABLE TASK REFERENCE down the route to a serving peer. The
//     reference carries its own (work_id, shard, generation, claim_epoch,
//     manifest_version) fence, so the peer can execute it without a lease transfer.
//   - Local-first = zero hop. A broker with its own worker for the kind serves it
//     off the local byKind buffer and never forwards (NATS queue-group locality).
//   - Owner-independent fence. The result write is fenced on claim_epoch, so any
//     broker holding the current-epoch token may write it — there is no home-route
//     hop and no lease transfer to track. A forwarded row that strands (peer gone,
//     lost result) is recovered by the reaper (heartbeat lapse → re-claim → re-run).

// MemberLister reads the registry so the mesh can discover peer brokers. The mesh
// uses the LIVENESS-FILTERED read (ListLiveClusterMembers) — only members whose
// heartbeat is within the window — so a crashed peer (row not yet GC'd) drops out of
// the peer set within one window instead of being dialed forever. This is the SAME
// liveness view the resharder uses for shard tiling, so routing and ownership agree
// on who is live. *store.Store satisfies it; kept as a seam for testing.
type MemberLister interface {
	ListLiveClusterMembers(ctx context.Context, window time.Duration) ([]store.ClusterMember, error)
}

// routeSink is the seam the mesh uses to hand INBOUND route frames to the owning
// fanout executor. The executor implements it. Keeping it an interface avoids a
// hard mesh→executor coupling and makes the mesh unit-testable with a fake sink.
type routeSink interface {
	// onRouteTask records a task REFERENCE forwarded to us by a peer and enqueues it
	// for a local worker to execute. This broker holds no lease for it (the forwarding
	// peer keeps the work_queue row); the reference carries its own owner-independent
	// (claim_epoch, generation, manifest_version) fence. Called from the route receive
	// loop.
	onRouteTask(peer *peerClient, task *workerpb.StageTask)
	// onRouteComplete resolves a task WE forwarded to a peer: the peer's worker ran it
	// and returned the StageComplete over the route. We hold the parked stage + the
	// lease, so this resolves it (deliver) and OUR runtime does the fenced write. A
	// stale/unknown token is a harmless no-op (the epoch fence rejects any dup). Called
	// from the route receive loop.
	onRouteComplete(sc *workerpb.StageComplete)
	// onPeerGone wakes every parked stage this broker pushed to `peer` (a synthetic
	// transient) when that peer departs the cluster, so the reaper-freed row re-claims
	// at once instead of riding the RemoteFanoutCeiling. Called from refreshOnce.
	onPeerGone(peer *peerClient)
}

// ── tunables ────────────────────────────────────────────────────────────────

// routeSendBuf bounds a peer route's outbound frame queue. The single per-route
// send goroutine drains it; a full queue means the peer is slow/stuck — the
// producer (pushTask / advertiseCredit) fails the non-blocking enqueue and picks
// another peer (or parks), rather than blocking the hot dispatch/claim path.
const routeSendBuf = 4096

// routeReconnectMin/routeReconnectMax bound the reconnect loop backoff after a
// route drops (doubling from min up to the max ceiling).
const (
	routeReconnectMin = 200 * time.Millisecond
	routeReconnectMax = 5 * time.Second
)

// ── peerMesh ──────────────────────────────────────────────────────────────

// peerMesh maintains persistent Route streams to peer brokers, discovered from
// cluster_members, and is the caller-facing forwarding surface: pushTask (forward a
// claimed task reference to a serving peer), advertiseCredit (broadcast our worker
// presence/credit), anyPeerServes (the cluster-aware claim gate). It reconciles its
// peer set on a cluster_changed NOTIFY (debounced) plus a failsafe poll — the same
// membership signal the Resharder reacts to, its own reaction being dial-new/drop-
// departed routes rather than re-tiling shards (see Run).
type peerMesh struct {
	selfID  string
	http    connect.HTTPClient // HTTP/2 transport built by cmd/converge (h2c/ALPN)
	lister  MemberLister
	refresh time.Duration
	sink    routeSink // the owning fanout executor (inbound task delivery)

	// livenessWindow is how recent a peer's heartbeat must be for the mesh to route
	// to it — the SAME window the resharder uses for shard tiling (both derive from
	// 3× the member-heartbeat cadence), so a crashed peer drops out of BOTH the shard
	// tiling and the mesh peer set within one window. Passed to ListLiveClusterMembers
	// in refreshOnce. Set at EnableRelay; a zero/negative value would return no peers,
	// so the caller always supplies a real window.
	livenessWindow time.Duration

	// listener wakes refreshOnce on a cluster_changed NOTIFY so a peer JOIN /
	// LEAVE / address-change is reflected in the mesh routes within ~one NOTIFY
	// window (plus debounce), instead of waiting out the `refresh` failsafe. The
	// same membership signal the Resharder reacts to — the mesh's independent
	// reaction is "dial/drop peer routes" (vs the Resharder's "re-tile my shards").
	// nil ⇒ failsafe poll only (the pre-listener behaviour; used by tests).
	listener runtime.Listener
	// debounceWindow collapses a scale event's burst of cluster_changed NOTIFYs
	// (one INSERT/DELETE per pod) into a SINGLE reconcile on the settled topology,
	// so a 1→50 broker scale-out dials the final peer set once, not N times
	// mid-flux. The `refresh` failsafe still backstops a crashed peer regardless.
	debounceWindow time.Duration

	// peers is an IMMUTABLE snapshot (memberID → client, excludes self), swapped
	// atomically by refreshOnce. Readers — anyPeerServes (the CLAIM hot path)
	// and pushTask — load it LOCK-FREE, so they never contend refreshOnce or each
	// other. The published map is never mutated after the swap; a refresh builds a
	// wholly new map (reusing a live peer's *peerClient*, incl. its route + credit).
	peers atomic.Pointer[map[string]*peerClient]

	// ctx bounds every route goroutine; cancelled when the mesh's Run returns.
	ctx    context.Context
	cancel context.CancelFunc

	// selfCredit is this broker's current per-(kind, kindVersion) worker credit, so a NEW
	// route coming up can bulk-send the full interest set (NATS sendSubsToRoute).
	// Updated by advertiseCredit before it fans the delta out to the live routes.
	mu         sync.Mutex
	selfCredit map[model.KindVersion]int32
}

// peerMeshDebounceWindow collapses a scale event's burst of cluster_changed
// NOTIFYs into one peer reconcile (matches the Resharder's 2s window — the same
// per-pod INSERT/DELETE burst drives both).
const peerMeshDebounceWindow = 2 * time.Second

// newPeerMesh builds a mesh dialing peers via httpClient (the HTTP/2 route
// transport). selfID excludes this broker from its own peer set + orders the
// dial/accept dedup. sink is the fanout executor receiving inbound pushed tasks.
// listener (may be nil in tests) wakes the peer reconcile on a cluster_changed
// NOTIFY; refresh is the failsafe re-reconcile cadence backing it.
func newPeerMesh(selfID string, httpClient connect.HTTPClient, lister MemberLister, listener runtime.Listener, refresh, livenessWindow time.Duration, sink routeSink) *peerMesh {
	if refresh <= 0 {
		refresh = 10 * time.Second
	}
	return &peerMesh{
		selfID:         selfID,
		http:           httpClient,
		lister:         lister,
		listener:       listener,
		refresh:        refresh,
		livenessWindow: livenessWindow,
		debounceWindow: peerMeshDebounceWindow,
		sink:           sink,
		selfCredit:     make(map[model.KindVersion]int32),
	}
}

// Run keeps the peer set synced with the LIVE cluster_members until ctx is cancelled.
// It reconciles on THREE edges (mirroring the Resharder's wake model — the SAME
// membership signal + the SAME liveness window, so mesh routing and shard tiling
// agree on who is alive):
//   - an initial reconcile at boot, so routes exist before the first forward;
//   - a cluster_changed NOTIFY (debounced), so a peer JOIN (INSERT) / graceful LEAVE
//     (DELETE) / address-change dials/drops its route within ~one NOTIFY window;
//   - the `refresh` failsafe tick — how a HARD-CRASHED peer is handled: a crash fires
//     no NOTIFY (the row lingers, only its heartbeat lapses), so the failsafe re-reads
//     the liveness-filtered members and the stale peer falls out within one liveness
//     window (NOT the ClusterMemberGC TTL). Also the backstop for a dropped NOTIFY.
//
// Each newly-discovered peer gets a persistent Route dial goroutine; a departed OR
// stale peer's route goroutine stops (refreshOnce cancels it + wakes parked pushes).
func (m *peerMesh) Run(ctx context.Context) {
	m.ctx, m.cancel = context.WithCancel(ctx)
	defer m.cancel()
	m.refreshOnce(m.ctx)

	// wakeup drives the reconcile: fed by the debounced listener (one wake per
	// settled membership burst) and consumed alongside the failsafe ticker.
	wakeup := make(chan struct{}, 1)
	if m.listener != nil {
		// raw carries every cluster_changed NOTIFY; Debounce collapses a burst into
		// one wakeup on the settled topology (trailing-edge). Same shape the
		// Resharder uses for the identical membership-burst problem.
		raw := make(chan struct{}, 1)
		go m.listener.Listen(m.ctx, "mesh cluster_changed", runtime.ClusterChangedChannel, func() { meshNotify(raw) })
		go runtime.Debounce(m.ctx, m.debounceWindow, raw, wakeup)
	}

	t := time.NewTicker(m.refresh)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-wakeup:
			m.refreshOnce(m.ctx)
		case <-t.C:
			m.refreshOnce(m.ctx)
		}
	}
}

// meshNotify does a non-blocking coalescing send on a buffered-1 wake channel: a
// wake already pending covers the new one, so a burst of NOTIFYs never blocks the
// listener. (The broker's local twin of runtime's own wake primitive — the mesh
// can't reach the unexported one across the package boundary.)
func meshNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// loadPeers returns the current immutable peer snapshot (never nil).
func (m *peerMesh) loadPeers() map[string]*peerClient {
	if p := m.peers.Load(); p != nil {
		return *p
	}
	return map[string]*peerClient{}
}

// refreshOnce reconciles the peer set against the LIVE cluster_members (heartbeat
// within livenessWindow): dial a Route to each newly-appeared peer, keep existing
// routes, and drop routes to departed OR STALE peers. Because the read is liveness-
// filtered, a hard-crashed peer (heartbeat lapsed, row not yet GC'd) falls out of
// `members` within one window → its route is cancelled + onPeerGone wakes parked
// pushes, so the mesh stops redialing a dead address in ~one window instead of at the
// ClusterMemberGC TTL. Copy-on-write publish so lock-free readers see a consistent
// snapshot.
func (m *peerMesh) refreshOnce(ctx context.Context) {
	members, err := m.lister.ListLiveClusterMembers(ctx, m.livenessWindow)
	if err != nil {
		slog.Warn("mesh: cluster_members refresh failed; keeping existing peer set", "err", err)
		return
	}
	prev := m.loadPeers()
	next := make(map[string]*peerClient, len(members))
	for _, mb := range members {
		if mb.MemberID == m.selfID {
			continue // never route to self
		}
		if mb.Role != "broker" && mb.Role != "all" {
			continue // only brokers participate in the mesh
		}
		addr := connectAddrFromConfig(mb.Config)
		if addr == "" {
			continue // peer hasn't advertised a dial-able address (mesh off there)
		}
		if existing, ok := prev[mb.MemberID]; ok && existing.addr == addr {
			next[mb.MemberID] = existing // keep the live route + credit + goroutines
			continue
		}
		pctx, pcancel := context.WithCancel(m.ctx)
		p := &peerClient{
			memberID: mb.MemberID,
			addr:     addr,
			ctx:      pctx,
			cancel:   pcancel,
			out:      make(chan *meshpb.RouteFrame, routeSendBuf),
			credit:   make(map[model.KindVersion]int32),
			serves:   make(map[model.KindVersion]bool),
			client: meshpbconnect.NewMeshServiceClient(m.http, addr,
				connect.WithSendGzip(), // compress large task/status payloads (bottleneck #4)
				connect.WithReadMaxBytes(wirelimits.MaxMessageBytes),
				connect.WithSendMaxBytes(wirelimits.MaxMessageBytes)),
		}
		next[mb.MemberID] = p
		// DEDUP: exactly one Route stream per pair. The lower member_id DIALS; the
		// higher one waits to ACCEPT the peer's dial (its Route handler attaches the
		// inbound stream). This keeps N(N-1)/2 connections, not N(N-1).
		if m.selfID < mb.MemberID {
			go m.runRoute(p)
		}
	}
	// CHURN: cancel every peer carried in prev but NOT in next (departed from
	// cluster_members, or changed address → replaced by a fresh peerClient above).
	// This stops its route reconnect loop promptly instead of leaking a goroutine and
	// a forever-redialing dial loop against a dead address. A task this broker forwarded
	// to that peer is woken at once (onPeerGone → synthetic transient) so its
	// reaper-freed row re-claims immediately instead of riding the RemoteFanoutCeiling;
	// the heartbeat lapse + reaper is the backstop if the wake races.
	for id, old := range prev {
		if next[id] != old && old.cancel != nil {
			old.cancel()
			if m.sink != nil {
				m.sink.onPeerGone(old)
			}
		}
	}
	m.peers.Store(&next)
}

// ── credit advertisement (RS+/RS-) ─────────────────────────────────────────

// advertiseCredit records this broker's absolute remaining FREE credit for the
// EXACT (kind, kindVersion) (hasWorker = does a local worker exist for it at all) and
// fans an Interest frame out to every live route (coalesced by the non-blocking
// enqueue). Called on every credit edge from the executor. hasWorker=false is the
// true RS- (no worker → peers stop claiming that (kind, kindVersion)); hasWorker=true,
// credit=0 keeps peers claiming (worker busy). The frame carries the kindVersion so a
// peer records it under the exact (kind, kindVersion) — STRICT: a v2 credit never opens a
// v1 claimer's gate.
func (m *peerMesh) advertiseCredit(kind model.Kind, kindVersion int, credit int32, hasWorker bool) {
	km := model.KindVersion{Kind: kind, Version: kindVersion}
	m.mu.Lock()
	if hasWorker {
		m.selfCredit[km] = credit // presence tracked by map membership; value = free slots
	} else {
		delete(m.selfCredit, km)
	}
	m.mu.Unlock()
	fr := &meshpb.RouteFrame{Body: &meshpb.RouteFrame_Interest{Interest: &workerpb.Interest{Kind: string(km.Kind), KindVersion: int32(km.Version), Credit: credit, HasWorker: hasWorker}}}
	for _, p := range m.loadPeers() {
		select {
		case p.out <- fr:
		default: // route queue full: drop; next edge re-sends the latest absolute state
		}
	}
}

// snapshotSelfCredit returns the (kind, kindVersion) pairs this broker currently has a
// worker for → their free credit. Presence = map membership (kept even at credit
// 0). Used for a new route's bulk interest send.
func (m *peerMesh) snapshotSelfCredit() map[model.KindVersion]int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[model.KindVersion]int32, len(m.selfCredit))
	for k, v := range m.selfCredit {
		out[k] = v
	}
	return out
}

// ── push (the hot path) ─────────────────────────────────────────────────────

// anyPeerServes reports whether ANY peer currently has a worker for the EXACT
// (kind, kindVersion) (presence, RS+/RS-), regardless of whether it has a FREE slot right
// now. The cluster-aware claim gate ORs this with the local HasSubscriber so a
// broker keeps claiming its tile whenever a worker for that exact (kind, kindVersion)
// exists somewhere in the fleet — even when that worker is momentarily full (the
// task then buffer-pushes to it). STRICT: a peer serving vpc/v2 does NOT open the
// gate for a vpc/v1 tile. Lock-free.
func (m *peerMesh) anyPeerServes(kind model.Kind, kindVersion int) bool {
	km := model.KindVersion{Kind: kind, Version: kindVersion}
	for _, p := range m.loadPeers() {
		if p.servesKindVersion(km) {
			return true
		}
	}
	return false
}

// pushTask picks ONE peer serving the EXACT (kind, kindVersion) and enqueues the
// durable task REFERENCE onto its route send queue. Peers with advertised free credit
// are preferred (weighted-random by that HINT — NATS queue-group cluster
// distribution, proportional to member count); if none has free credit a serving-but-
// full peer is chosen at random (its byKind buffer, 1024, absorbs the burst). Returns
// true if the reference was handed to a peer, false if no peer serves the pair or the
// chosen peer's queue is full (caller then retries / keeps parking — never dropped).
// STRICT: only peers serving the exact (kind, kindVersion) are candidates. O(peers),
// one draw.
func (m *peerMesh) pushTask(km model.KindVersion, task *workerpb.StageTask) (*peerClient, bool) {
	peers := m.loadPeers()
	// Collect candidates + total FREE credit (the routing HINT) in one pass; also
	// remember peers that SERVE the (kind, kindVersion) but are momentarily full
	// (credit 0) as the fallback set.
	type cand struct {
		p      *peerClient
		credit int32
	}
	cands := make([]cand, 0, len(peers))
	var total int32
	var serving []*peerClient // has a worker for this (kind, kindVersion), but no free credit right now
	for _, p := range peers {
		if c := p.getCredit(km); c > 0 {
			cands = append(cands, cand{p, c})
			total += c
		} else if p.servesKindVersion(km) {
			serving = append(serving, p)
		}
	}
	var chosen *peerClient
	switch {
	case total > 0:
		// Weighted-random pick proportional to the free-credit hint (NATS queue-group dist).
		r := int32(rand.IntN(int(total)))
		var sum int32
		for _, c := range cands {
			sum += c.credit
			if r < sum {
				chosen = c.p
				break
			}
		}
		if chosen == nil {
			chosen = cands[len(cands)-1].p // float guard (unreachable with ints)
		}
	case len(serving) > 0:
		// No free slot anywhere, but a worker exists — forward to one (random) so its
		// worker runs the reference when a slot frees. This keeps a worker-less broker
		// from stranding under credit exhaustion (it must not queue to its own dead local
		// buffer). The peer's byKind buffer (1024) absorbs the burst.
		chosen = serving[rand.IntN(len(serving))]
	default:
		return nil, false // no peer serves this (kind, kindVersion) at all → caller retries/parks
	}
	fr := &meshpb.RouteFrame{Body: &meshpb.RouteFrame_Task{Task: task}}
	select {
	case chosen.out <- fr:
		chosen.decCredit(km) // optimistic hint decrement; corrected by the peer's next Interest
		return chosen, true  // return the peer so the owner can wake this parked stage if it departs
	default:
		return nil, false // chosen peer's route queue full → caller retries
	}
}
