// Package broker is the server half of the universal work-distribution API.
//
// A broker pod OWNS a contiguous tile of the work_queue shard space (assigned
// by the Resharder under the "broker" role) and is the ONLY party that touches
// Postgres for work: it runs the FOR UPDATE SKIP LOCKED claim (stamping broker_id +
// bumping the strict monotonic claim_epoch), holds the lease, and performs the fenced
// AppendOutbox write. The write fence is the owner-independent claim_epoch (with
// generation + manifest_version), NOT broker_id — so any broker holding the current
// epoch's token may write the result. Dumb workers connect IN and pull tasks; they
// hold no shard, no DB handle, and no broker_id.
//
// A broker is a runtime.Dispatcher whose reaction executor is a FANOUT
// executor: instead of running a reaction's handler in-process, the executor
// parks the task in a per-kind buffer, streams it to a connected worker over
// WorkStream, and BLOCKS until that worker's Complete arrives — then the
// dispatcher's Loop resumes (rollup, condition fold, fenced AppendOutbox). The
// broker does all the DB reads/writes; the connected worker runs only the
// handler code.
package broker

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"

	"github.com/salesforce/converge/internal/meshpb/meshpbconnect"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/pkg/spiffeauthz"
	"github.com/salesforce/converge/pkg/wirelimits"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"

	"connectrpc.com/connect"
)

// ConfigReader is the minimal seam the broker needs to serve dumb workers their
// (kind, kindVersion)'s DEFAULT providerconfig docs + bundles over Connect (GetProviderConfig).
// The ProviderConfigCache satisfies it; nil = no defaults served (workers get only the
// per-resource override). Read LIVE on each call so an operator's edit reaches
// workers on their next refresh. PER-KIND-VERSION: defaults are per (kind, kindVersion), so the
// broker reads the exact kindVersion a worker advertises — a v2 worker gets vpc/v2's
// default, a v1 worker vpc/v1's.
type ConfigReader interface {
	Config(kind model.Kind, kindVersion int) json.RawMessage
	Bundle(kind model.Kind, kindVersion int) []byte
}

// Server is a broker: a Dispatcher driving a fanout executor plus the Connect
// handlers — the WorkerService workers connect to, and the MeshService peer brokers
// dial. Construct with New, start the claim loop with Run, and mount Handler() on an
// HTTP listener.
type Server struct {
	disp      *runtime.Dispatcher
	fanout    *fanoutExecutor
	manifests *runtime.KindManifestCache // kind_manifest cache; pair selection + AddKindLive
	configs   ConfigReader               // live default-config source served via GetProviderConfig; nil-safe

	// workerAuthz / meshAuthz are the per-service SPIFFE-ID allowlists Handler enforces
	// (set via SetAuthz before Handler): workerAuthz on WorkerService (WorkStream +
	// GetProviderConfig), meshAuthz on MeshService (Route). Both services share ONE mTLS
	// listener whose handshake admits the union of the two; these narrow each request to
	// its audience so a worker cert can't reach a mesh RPC (and vice versa). nil = no
	// allowlist for that service (chain trust only).
	workerAuthz spiffeauthz.Matcher
	meshAuthz   spiffeauthz.Matcher

	// peerSource selects how spiffeGate resolves a connection's OBSERVED identity
	// (peerIdentity) for the cluster view + "running on <worker>" attribution — the
	// mTLS client cert's SPIFFE ID (default) or a trusted service-mesh header. Set via
	// SetAuthz. Never a client self-report. See peeridentity.go.
	peerSource peerIdentitySource
}

// SetAuthz installs the per-service SPIFFE-ID allowlists Handler enforces and the
// peer-identity source spiffeGate resolves connections by. peerIdentitySource is the
// raw PEER_IDENTITY_SOURCE value ("" / "mtls" → the connection's client cert;
// "mesh-header" → a trusted service-mesh header). Call before Handler. nil matchers
// leave a service on chain trust only.
func (s *Server) SetAuthz(workerAuthz, meshAuthz spiffeauthz.Matcher, peerIdentitySrc string) {
	s.workerAuthz = workerAuthz
	s.meshAuthz = meshAuthz
	s.peerSource = resolvePeerIdentitySource(peerIdentitySrc)
}

// New builds a broker over pool. It claims work_queue rows and FANS each
// reaction out to a connected dumb worker (it runs NO handler code itself).
// manifests is the kind_manifest cache (drives pair selection + the engine);
// brokerID is the broker's lease identity (work_queue.broker_id at claim; the write
// fence is the claim_epoch, not this id). maxParallel bounds per-(kind,task_type)
// in-flight on this broker. Call SetShards/AddPair as for a Dispatcher before Run.
func New(pool *pgxpool.Pool, manifests *runtime.KindManifestCache, brokerID string, maxParallel int) *Server {
	disp := runtime.NewWithBrokerID(pool, brokerID) // broker runs no handlers in-process; SetDispatcher(fanout) below
	disp.SetMaxParallel(maxParallel)
	disp.SetManifests(manifests)
	// A wedged-but-connected worker holding a no-deadline (TaskDeadline=0) task
	// would otherwise park its dispatchStage slot forever (the stream never drops,
	// so the abandon-on-disconnect path can't fire). Bound the no-deadline REMOTE
	// wait with a generous ceiling so such a task is eventually cancelled and its
	// slot freed; a kind that sets an explicit TaskDeadline still uses that. Tune
	// up for very-long handlers (e.g. terraform applies) via SetRemoteFanoutCeiling.
	disp.RemoteFanoutCeiling = DefaultRemoteFanoutCeiling
	// Gate the lease heartbeat on worker-attested liveness: the broker refreshes a
	// task's lease only while a live worker keeps attesting its progress (WorkHeartbeat),
	// so a silent or wedged worker's lease goes stale within RequireWithin and the reaper
	// reclaims it — heartbeat_at attests worker EXECUTION, not mere broker liveness.
	disp.RequireWithin = runtime.DefaultRequireWithin
	f := newFanoutExecutor()
	disp.SetDispatcher(f) // ship every reaction to a dumb worker
	// Relay a worker's per-task liveness attestation (WorkHeartbeat) into the
	// dispatcher's heartbeat set: WorkStream maps the attested lease_tokens to their
	// work_ids and calls this, so ONLY still-progressing tasks keep their lease fresh
	// (a silent/hung worker's task goes stale → reaper). Wired at construction (before
	// Run launches the dispatcher goroutine → no data race).
	f.attest = disp.Attest
	// Only claim a (kind, kindVersion) that has a connected AND READY worker for that
	// EXACT kindVersion (G-BUFFER-STARVATION): no ready consumer → don't pull work we
	// can't deliver. "Ready" excludes a connected worker that sent a per-kind RS-
	// (its provider for that pair is degraded), so a kind whose only workers are all
	// degraded parks in work_queue for recovery instead of being claimed to fail.
	// STRICT — a v2 worker does not open the gate for a v1 row (and vice versa), so
	// a (kind, kindVersion) with resources but no ready worker parks for another broker
	// or a later poll rather than filling a buffer nobody drains.
	disp.ClaimGate = f.HasSubscriber
	// Scoped-release abandoned leases (dropped worker stream) for ms re-claim
	// instead of the reaper's StaleAfter (G-LEASE-RELEASE).
	st := store.New(pool)
	f.release = st.WorkQueueReleaseTasks
	// UI attribution: stamp worker_id with the worker each task is dispatched to.
	// Per-task events flow onto attrCh; runAttributionBatcher (launched alongside the
	// relay) coalesces them into batched UPDATEs off the dispatch path.
	f.markWorkerBatch = st.WorkQueueMarkWorkerBatch
	f.brokerID = brokerID
	f.attrCh = make(chan execAttr, attrChanBuf)
	return &Server{disp: disp, fanout: f, manifests: manifests}
}

// DefaultRemoteFanoutCeiling bounds a no-deadline (TaskDeadline=0) task on the
// broker's remote fanout so a wedged-but-connected worker can't park a
// dispatchStage slot indefinitely. Deliberately generous (well above any
// legitimate handler) so it only ever trips on a genuinely stuck worker; raise
// it on a broker that serves a kind with a longer legitimate runtime.
const DefaultRemoteFanoutCeiling = 30 * time.Minute

// SetRemoteFanoutCeiling overrides the no-deadline remote ceiling (0 disables
// it). Call before Run.
func (s *Server) SetRemoteFanoutCeiling(d time.Duration) { s.disp.RemoteFanoutCeiling = d }

// NewDispatch is New plus the boot wiring a dispatch surface needs: it sets
// shards and registers the (kind, task_type) pairs for EVERY already-applied
// kind's model. A broker always claims every manifested kind — it has no
// compiled-in kind list and no subset filter; it learns kinds LIVE from
// kind_manifest (the KindManifestCache). Kinds whose manifests land AFTER boot are
// registered via AddKindLive from the cache's reload callback. maxParallel ≤ 0
// defaults to 100.
func NewDispatch(pool *pgxpool.Pool, manifests *runtime.KindManifestCache, brokerID string, maxParallel int, shards *runtime.ShardSet) *Server {
	if maxParallel <= 0 {
		maxParallel = 100
	}
	s := New(pool, manifests, brokerID, maxParallel)
	if shards != nil {
		s.disp.Shards = shards
	}
	// Boot-seed the pairs of every kind already in the cache. Kinds applied later
	// arrive via AddKindLive (the KindManifestCache reload callback). The dispatcher
	// dedups, so a kind seeded here and re-seen live is registered once.
	for _, m := range manifests.All() {
		for _, tt := range manifestTaskTypes(m) {
			s.disp.AddPair(m.Kind, m.KindVersion, tt)
		}
	}
	return s
}

// AddKindLive registers a kind's task-type pairs on the running broker from its
// manifest (the ProviderRetrier / a manifest apply calls this), mirroring the
// dispatcher's live add.
func (s *Server) AddKindLive(m model.KindManifest) {
	for _, tt := range manifestTaskTypes(m) {
		s.disp.AddPairLive(m.Kind, m.KindVersion, tt)
	}
}

// manifestTaskTypes returns the work_queue task_type slots a kind's manifest opts
// into, derived from its declared reactions' triggers:
//   - specChange (compose/work) OR childrenSettled (rollup) OR resync → reconcile
//   - deleteRequested → delete
//   - operation        → operate
func manifestTaskTypes(m model.KindManifest) []store.TaskType {
	var reconcile, del, operate bool
	for _, rx := range m.Reactions {
		switch rx.Trigger {
		case model.TriggerSpecChange, model.TriggerChildrenSettled, model.TriggerResync:
			reconcile = true
		case model.TriggerDeleteRequested:
			del = true
		case model.TriggerOperation:
			operate = true
		case model.TriggerReactor:
			// Reactor reactions are delivered from lifecycle_outbox (STAGE_REACT),
			// not the work_queue — they produce no work task type here.
		}
	}
	var out []store.TaskType
	if reconcile {
		out = append(out, store.TaskReconcile)
	}
	if del {
		out = append(out, store.TaskDelete)
	}
	if operate {
		out = append(out, store.TaskOperate)
	}
	return out
}

// Dispatcher exposes the underlying dispatcher so the host can AddPair, set
// shards, run, and read InFlight — the broker reuses the dispatcher's API
// verbatim.
func (s *Server) Dispatcher() *runtime.Dispatcher { return s.disp }

// StageDispatcher returns the broker's fanout as a model.StageDispatcher — it
// does NOT execute, it routes a stage to a connected dumb worker and returns the
// worker's result. A ReactorDispatcher running on this broker uses it so a reactor
// reaction is shipped to a worker over the SAME WorkStream stream as every other
// stage (STAGE_REACT) — unifying reactor delivery with work delivery. The worker
// advertising the reactor kind runs the handler; the broker acks lifecycle_outbox
// only after a successful Complete.
func (s *Server) StageDispatcher() model.StageDispatcher { return s.fanout }

// HasWorkerForKind reports whether a connected worker advertises kind at ANY
// kindVersion. The ReactorDispatcher running on this broker uses it as its
// ReadyToDispatch gate so it skips (leaves claimed) a lifecycle delivery whose
// reactor kind has no worker yet, instead of parking it on the fanout buffer.
// Reactor delivery is keyed by KIND only (lifecycle_outbox carries no kindVersion), so
// the gate is deliberately the any-kindVersion form — a worker on any kindVersion of the
// reactor kind can run its (kind, reaction)-keyed handler. STRICT per-kindVersion
// routing still governs the WORK path (HasSubscriber(kind, kindVersion)); only this
// coarse reactor-readiness signal spans kind versions.
func (s *Server) HasWorkerForKind(kind model.Kind) bool { return s.fanout.HasAnyWorkerForKind(kind) }

// BroadcastConfig pushes a (kind, kindVersion)'s new default providerconfig — the whole
// monolith, spec + data/bundle together — to every connected worker advertising that
// EXACT (kind, kindVersion). Wire it to the ProviderConfigCache's OnKindConfigChange so an
// operator's edit reaches the right-kindVersion workers immediately over their open
// WorkStream (the push half of config delivery; the worker also pulls at boot/refresh).
func (s *Server) BroadcastConfig(kind model.Kind, kindVersion int, spec, data []byte) {
	s.fanout.broadcastProviderConfig(kind, kindVersion, spec, data)
}

// SetConfigReader wires the live default-config source the broker serves to
// dumb workers via GetProviderConfig. Call before Handler/Run. nil-safe (no
// defaults served).
func (s *Server) SetConfigReader(c ConfigReader) { s.configs = c }

// EnableRelay turns on the broker-to-broker MESH (NATS-style presence routing): this
// broker discovers peers from cluster_members (via lister), holds a persistent
// Route stream to each, advertises its live per-kind worker presence/credit over those
// streams, and — the instant it claims a task it has no LOCAL worker for — FORWARDS
// the durable task reference to a serving peer to execute. The result write is fenced
// on the owner-independent claim_epoch, so a forwarded reference needs no home-route
// hop. httpClient is the HTTP/2 (h2c/ALPN) route transport. selfID excludes self +
// orders the per-pair dial dedup. listener wakes the peer reconcile on a
// cluster_changed NOTIFY (nil ⇒ failsafe poll only). Call StartRelay to run the mesh.
// NOT called (mesh nil) → the executor is byte-for-byte the mesh-off path (claim +
// serve LOCAL work only). livenessWindow is how recent a peer's heartbeat must be for
// the mesh to route to it — the SAME window the resharder uses for shard tiling, so a
// crashed peer drops out of both within one window (not the ClusterMemberGC TTL). See relay.go.
func (s *Server) EnableRelay(selfID string, httpClient connect.HTTPClient, lister MemberLister, listener runtime.Listener, refresh, livenessWindow time.Duration) {
	s.fanout.mesh = newPeerMesh(selfID, httpClient, lister, listener, refresh, livenessWindow, s.fanout)
	// CLUSTER-AWARE CLAIM GATE: with the mesh on, a broker must claim its shard tile
	// whenever a READY worker for the EXACT (kind, kindVersion) exists ANYWHERE in the
	// fleet — not just when one is connected HERE — else a broker with no local worker
	// never claims its tile and there is nothing to forward (its keyspace strands). So OR
	// the local HasSubscriber(kind, kindVersion) with "does any peer advertise presence for
	// that exact (kind, kindVersion)?" (learned live over the route). Both terms are now
	// READINESS-aware: HasSubscriber reads readySubscribers, and a peer's advertised
	// presence/credit is retracted when its last ready worker for the pair goes RS- (the
	// mesh re-advertisement inside setStreamReadiness), so a fleet whose only workers for
	// a kind are all degraded stops claiming it everywhere. A (kind, kindVersion) with NO
	// ready worker anywhere is rejected (no wasted claim/park), and STRICT holds
	// end-to-end: a peer serving only vpc/v2 never opens the gate for a vpc/v1 tile.
	// Off → the gate stays the local-only HasSubscriber (HEAD).
	f := s.fanout
	s.disp.ClaimGate = func(kind model.Kind, kindVersion int) bool {
		return f.HasSubscriber(kind, kindVersion) || (f.mesh != nil && f.mesh.anyPeerServes(kind, kindVersion))
	}
}

// WorkerKindsSnapshot returns this broker's LIVE set of connected-worker kinds
// (kinds with ≥1 open WorkStream stream at ANY kindVersion). Used by the
// cluster-view/reporter for observability; worker CREDIT for the mesh is pushed
// live over the route, not via this snapshot. Order unspecified.
func (s *Server) WorkerKindsSnapshot() []model.Kind { return s.fanout.subscribedKinds() }

// SubscriberBreakdown returns a snapshot of this broker's connected-worker counts
// per (kind, kindVersion): kind → kindVersion → number of live WorkStream streams. The host
// surfaces the ZERO-WORKER-FOR-KINDVERSION signal with it — pairing it against the set of
// kind versions a kind actually has resources on reveals "kind vpc has resources on kind versions
// {1,2}, workers connected for {2}", the diagnostic for a v1 tile that STRICT
// routing will never drain (no v1 worker connected; v2 workers do not substitute).
// A deep copy; off the hot path (read on the reporter beat / a diagnostics call).
func (s *Server) SubscriberBreakdown() map[model.Kind]map[int]int {
	return s.fanout.SubscriberBreakdown()
}

// ConnectedWorkers snapshots the workers currently connected to THIS broker —
// one row per open WorkStream stream, each with its friendly id (hostname), the
// kinds it serves, and its live/ceiling slots. The cluster-member reporter
// publishes it into this broker's cluster_members.config so the cluster API can
// show "which workers are connected where and what they run". Off the hot path
// (read on the 30s reporter beat); a pure value snapshot, no live pointers.
func (s *Server) ConnectedWorkers() []ConnectedWorker { return s.fanout.connectedWorkers() }

// ConnectedWorkerCount is the number of live worker streams — a cheap metrics gauge
// source (no per-worker snapshot alloc). Satisfies metrics.BrokerSource.
func (s *Server) ConnectedWorkerCount() int { return s.fanout.subscriberCount() }

// MeshCounters satisfies metrics.BrokerSource. A departed OR stale peer (evicted by
// the topology watcher's liveness window) fires an onPeerGone wake that re-pends its
// forwarded rows at once, with the reaper as the backstop (a stranded forwarded row's
// heartbeat lapses → re-claim → re-run). These recovery edges are not individually
// counted yet, so the three gauges report zero.
func (s *Server) MeshCounters() (peerGoneWakes, foreignDropSignals, syntheticDelivered int64) {
	return 0, 0, 0
}

// ConfigPushDropped is the cumulative count of live config/bundle PUSH frames this
// broker dropped because a worker's push channel was full (broadcastPush). Normally
// ~0 — with prime-on-subscribe + the worker's reconnect re-pull + the periodic
// refresh all backing it up, a drop is only a transiently-wedged worker stream, so a
// sustained rise is the alertable signal. Read at scrape time; monotonic.
func (s *Server) ConfigPushDropped() int64 { return s.fanout.metrics.configPushDropped.Load() }

// SetMetrics wires the OTel dispatch counters the fanout increments (claimed on
// dispatch, completed on resolve). Called once at CONSTRUCTION (before the server
// serves), so the counters are read-only thereafter — no lock, no data race. Either
// may be nil (metrics off); a nil counter's Add is guarded at the call sites.
func (s *Server) SetMetrics(claimed, completed metric.Int64Counter) {
	s.fanout.claimed = claimed
	s.fanout.completed = completed
}

// StartRelay runs the peer-mesh loop (route dials + refresh) until ctx is
// cancelled. No-op if the mesh wasn't enabled. Launch it in a goroutine alongside
// the dispatcher's Run.
func (s *Server) StartRelay(ctx context.Context) {
	if s.fanout.mesh != nil {
		s.fanout.mesh.Run(ctx)
	}
}

// StartAttributionBatcher runs the coalescing loop that drains attrCh and writes
// worker_id in batches until ctx is cancelled (then it flushes what's buffered).
// No-op when attribution is disabled (markWorkerBatch/attrCh nil). Launch it in a
// goroutine alongside StartRelay — bound to the same loopCtx so it stops on Stop.
func (s *Server) StartAttributionBatcher(ctx context.Context) {
	s.fanout.runAttributionBatcher(ctx)
}

// AcceptRoute is the server side of the Route RPC (a peer dialed us). Delegates to
// the mesh; nil-safe when the mesh is off (returns nil so the handler closes the
// stream cleanly).
func (s *Server) AcceptRoute(ctx context.Context, stream routeBidi) error {
	if s.fanout.mesh == nil {
		return nil
	}
	return s.fanout.mesh.AcceptRoute(ctx, stream)
}

// Handler returns the Connect route ("/", http.Handler) for the broker's two
// services to mount on the broker's listener (the same mux/TLS the API server
// uses). The connectHandler implements BOTH the WorkerServiceHandler (WorkStream +
// GetProviderConfig, the worker-facing surface) and the MeshServiceHandler (Route, the
// broker↔broker mesh); the returned handler is an INTERNAL mux
// that carries both and routes by each service's full Connect path
// (/converge.broker.v1.WorkerService/… and /…MeshService/…). Splitting the service
// at the RPC layer means a worker (which dials only WorkerService) can never reach
// the mesh RPCs.
//
// The returned pattern is "/" (a root subtree) rather than a shared prefix: the two
// service paths share only "/converge.broker.v1." which does NOT end in "/", so it
// would register as an EXACT http.ServeMux pattern and never match the per-service
// subpaths. Returning "/" lets the caller's outer mux forward every request to this
// internal mux, which does the precise per-service routing. The broker listener
// serves ONLY these services, so a root mount is safe.
//
// The message-size cap (wirelimits.MaxMessageBytes) matches the worker client so a
// large-but-legitimate spec/subtree isn't rejected while a pathologically large one
// is bounded (vs Connect's default 0 = unlimited). The shared const lives in the
// leaf pkg pkg/wirelimits so the broker needn't import the worker client SDK
// (sdk-go/converge) — keeping that dependency edge cut for repo separability.
func (s *Server) Handler(opts ...connect.HandlerOption) (string, http.Handler) {
	opts = append([]connect.HandlerOption{
		connect.WithReadMaxBytes(wirelimits.MaxMessageBytes),
		connect.WithSendMaxBytes(wirelimits.MaxMessageBytes),
	}, opts...)
	h := &connectHandler{fanout: s.fanout, configs: s.configs}
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(h, opts...))
	mux.Handle(meshpbconnect.NewMeshServiceHandler(h, opts...))
	return "/", s.spiffeGate(mux)
}

// workerServicePrefix / meshServicePrefix are the Connect procedure-path prefixes
// ("/<fully-qualified-service>/") the two services mount under. spiffeGate routes
// per-service authz off them. Derived from the generated service-name constants so
// they stay in lockstep with the proto (no hand-typed literal).
var (
	workerServicePrefix = "/" + workerpbconnect.WorkerServiceName + "/"
	meshServicePrefix   = "/" + meshpbconnect.MeshServiceName + "/"
)

// spiffeGate wraps the two-service mux with PER-SERVICE SPIFFE-ID authz. Both
// services share one mTLS listener whose TLS handshake already admitted the UNION
// of the worker + mesh allowlists (cmd/converge), so any connection reaching here
// carries a chain-verified client cert that is SOME legitimate peer. This gate then
// narrows by request PATH: a WorkerService RPC must match the worker allowlist, a
// MeshService RPC the mesh allowlist — so a worker identity can't reach Route and a
// peer-broker identity can't pull work. Enforcement reads the
// SPIFFE ID from the verified client cert's URI SAN (r.TLS.VerifiedChains), the
// same source the handshake authorizer used.
//
// It does two things per request, in order:
//  1. AUTHZ (when a per-service matcher is set): the peer's verified client-cert
//     SPIFFE ID must be on that service's allowlist, else 403. A nil matcher = no
//     allowlist for that service (chain trust only) → skip.
//  2. IDENTITY: resolve the connection's OBSERVED identity (peerIdentity — SPIFFE ID
//     / trusted mesh header / peer IP, never a client self-report) and stash it on the
//     request context so the WorkStream handler can attribute + list the worker by it.
//     Always done, even with no allowlist, since attribution needs it regardless.
//
// The handler's ctx derives from this request's context (connect-go NewBidiStreamHandler),
// so the stashed peerIdentity flows straight through to WorkStream.
func (s *Server) spiffeGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var matcher spiffeauthz.Matcher
		switch {
		case strings.HasPrefix(r.URL.Path, workerServicePrefix):
			matcher = s.workerAuthz
		case strings.HasPrefix(r.URL.Path, meshServicePrefix):
			matcher = s.meshAuthz
		default:
			// Not one of our two services (the listener serves only these); refuse.
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		// (1) Authz — only when this service has an allowlist.
		if matcher != nil {
			var leaf *x509.Certificate
			if r.TLS != nil && len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
				leaf = r.TLS.VerifiedChains[0][0]
			}
			if err := spiffeauthz.CheckLeaf(matcher, leaf); err != nil {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		// (2) Identity — resolve + stash for the WorkStream handler (attribution + view).
		pi := resolvePeerIdentity(r, s.peerSource)
		next.ServeHTTP(w, r.WithContext(withPeerIdentity(r.Context(), pi)))
	})
}

// ConnectedWorker is one connected WorkStream stream, projected for the cluster
// view: its OBSERVED identity, the kinds it advertises (sorted, deduped), and
// its live/ceiling slot counts. A single struct crossing the broker→reporter
// boundary keeps the cluster-view read a pure snapshot (no live pointers).
type ConnectedWorker struct {
	// WorkerID is the identity the broker OBSERVED for this connection — the mTLS
	// client cert's SPIFFE ID, a trusted service-mesh header, or the peer IP — NEVER a
	// value the worker self-reported. IDSource says which; IDVerified is true only for a
	// cryptographically-verified source (cert / mesh header), false for a bare peer IP.
	// Empty id → the snapshot synthesizes a "worker-<seq>" fallback so the stream lists.
	WorkerID   string
	IDSource   string // "spiffe" | "mesh-header" | "peer-ip" | "" (synthesized)
	IDVerified bool
	Kinds      []model.Kind // the (kind, kindVersion) pairs this worker serves, sorted; one entry per pair
	// KindVersions is PARALLEL to Kinds: KindVersions[i] is the web-API kindVersion this
	// worker serves Kinds[i] at. A worker serving vpc/v1 AND vpc/v2 appears as
	// Kinds=[vpc, vpc], KindVersions=[1, 2] — the cluster view renders "vpc/v1",
	// "vpc/v2". The pairs are the real (kind, kindVersion) advertisement.
	KindVersions []int
	InFlight     int // tasks this stream currently holds
	MaxInflight  int // this stream's advertised concurrency ceiling
}
