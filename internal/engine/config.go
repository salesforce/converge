package engine

import (
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/metric"

	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/pkg/spiffeauthz"
)

// config.go holds the per-process boot configuration and the resolver that turns
// it into a duty list: EngineConfig (the role decision + tunable knobs main.go
// fills from env), the two per-duty config structs the resolver builds
// (ControlPlaneConfig, BrokerConfig), and DutiesFromConfig itself.

// Sweeper cadence defaults — the SINGLE source of truth; other code references
// THESE constants, never re-typing the literals. (StaleAfter and
// UnclaimedDeleteAfter default in the Reaper itself — see runtime.DefaultSweeper*.)
const (
	// DefaultRetryAfter is the delay before a failed task is re-pended by the reaper.
	DefaultRetryAfter = 2 * time.Second
	// DefaultSweeperInterval is the control-plane sweep cadence.
	DefaultSweeperInterval = 10 * time.Second
)

// EngineConfig is the resolved per-process boot configuration DutiesFromConfig
// turns into a duty list. It carries the role decision (RunControl/RunBroker +
// the dispatch identity) plus the tunable knobs the duties pass through to their
// runtime drivers. main.go fills it from env; tests fill it directly.
type EngineConfig struct {
	// RunControl requests the control sweeper bundle (drain + reap + specgc +
	// resync + membergc). False on a pure worker pod.
	RunControl bool

	// Control sweeper knobs (passed to the control bundle; zero = driver default).
	RetryAfter                  time.Duration
	SweeperStaleAfter           time.Duration
	SweeperInterval             time.Duration
	SweeperUnclaimedDeleteAfter time.Duration

	// Claim knobs (passed to the claim duty; zero = driver default).
	HeartbeatEvery time.Duration
	WorkerPollMin  time.Duration
	WorkerPollMax  time.Duration
	// ShutdownDrain is the graceful in-flight drain window the claim duty's
	// dispatcher + reactor honor on Stop (let fanned-out work land its result
	// before abandoning it). 0 → no drain.
	ShutdownDrain time.Duration

	// Shards is the pod's shard range, shared by every duty (control sweepers and
	// the broker). nil → each driver keeps its AllShards default.
	Shards *runtime.ShardSet

	// RunBroker requests the CLAIM duty: the pod owns its shard tile, claims
	// work_queue rows AND drains lifecycle_outbox, and fans each reaction (work or
	// lifecycle STAGE_REACT) out to dumb workers (the universal claim tier).
	// A broker claims EVERY manifested kind (learned live from kind_manifest).
	// BrokerID is the broker's stable lease identity (also the reactor
	// dispatcher's claim id).
	RunBroker bool
	BrokerID  string

	// Broker-to-broker MESH, passed to the claim duty. Enabled whenever
	// RelayHTTPClient is set (always, for a broker); a no-op with zero peers
	// (single-broker fleet). RelayHTTPClient is the HTTP/2 (h2c/ALPN) route transport.
	RelayHTTPClient connect.HTTPClient

	// MeshLivenessWindow is how recent a peer's heartbeat must be for the mesh to
	// route to it — the SAME window the Resharder uses for shard tiling, so a crashed
	// peer drops out of both within one window (not the ClusterMemberGC TTL). Derived
	// from 3× the member-heartbeat cadence at the composition root.
	MeshLivenessWindow time.Duration

	// WorkerAuthz / MeshAuthz are the parsed SPIFFE-ID allowlists for the broker's
	// two Connect services, which share ONE mTLS listener. The handshake admits the
	// UNION (main.go); these narrow each RPC per-service — WorkerAuthz gates
	// WorkStream/GetProviderConfig (the workers that pull work), MeshAuthz gates
	// Route (the PEER BROKERS on the internal mesh) — so a worker cert
	// can't reach a mesh RPC and vice versa. nil = no allowlist for that service.
	WorkerAuthz spiffeauthz.Matcher
	MeshAuthz   spiffeauthz.Matcher
	// PeerIdentitySource is the raw PEER_IDENTITY_SOURCE value ("" / "mtls" /
	// "mesh-header") — how the broker derives a connected client's OBSERVED identity
	// for the cluster view + attribution (never a self-report). See cmd/converge config.
	PeerIdentitySource string

	// OTel metric counters, wired into the duties AT CONSTRUCTION (before Start → no
	// data race). When metrics are on they're non-nil (real or no-op) instruments;
	// left nil (the zero value) the duties' Add call sites nil-guard. SweptCounter →
	// control sweepers; BrokerClaimed/BrokerCompleted → the broker dispatch path.
	SweptCounter    metric.Int64Counter
	BrokerClaimed   metric.Int64Counter
	BrokerCompleted metric.Int64Counter
}

// ControlPlaneConfig configures the control sweeper bundle. Pool is required;
// everything else has a sane default. There is deliberately NO Registry — the
// control plane reads its per-kind resync policy from the kind_config DB table,
// so it constructs no provider and dials no client.
type ControlPlaneConfig struct {
	Pool *pgxpool.Pool

	// Shards is the control pod's shard range, SHARED by every control sweeper
	// (drainer, reaper, specgc, resyncer) — a single *runtime.ShardSet so the
	// Resharder can swap the whole pod's range atomically on membership change
	// and all four sweepers pick it up together. nil → defaults to AllShards
	// (single-pod / unsharded: sweep everything). A member owns ONE contiguous
	// range for its role, used uniformly by all its sweepers.
	Shards *runtime.ShardSet

	SweeperStaleAfter time.Duration
	SweeperInterval   time.Duration
	RetryAfter        time.Duration

	// SweeperUnclaimedDeleteAfter overrides the reaper's abandoned-work GC window
	// (default 24h in runtime.NewReaper): a work_queue row unclaimed this long is
	// hard-deleted because no worker for its kind ever claimed it. Distinct from
	// SweeperStaleAfter (which recovers a dead CLAIMED task). 0 keeps the default.
	SweeperUnclaimedDeleteAfter time.Duration

	// ResyncSweepInterval overrides how often the drift Resyncer wakes to
	// look for due rows (default 30s). Test-only (not reachable from boot
	// config): lower it to observe drift detection within a short deadline;
	// leave zero in production.
	ResyncSweepInterval time.Duration

	// KindConfigRefreshInterval overrides how often the control plane re-reads
	// kind_config to live-refresh the Resyncer's kind-map (default 15s, the
	// informer relist). Test-only: lower it to observe a runtime resync edit
	// converge within a short deadline; leave zero in production.
	KindConfigRefreshInterval time.Duration

	// Swept is the optional OTel sweeper-rows counter, wired onto every sweeper AT
	// CONSTRUCTION (before Start → before any tick → no data race). nil → sweepers
	// stay metrics-free. Set from cmd/converge when metrics are enabled.
	Swept metric.Int64Counter
}

// BrokerConfig configures the claim duty (the server-side dispatch surface).
// It is the knob set a broker pod's runtime.Dispatcher reads: the
// per-(kind,task_type) concurrency ceiling, heartbeat/poll cadences, and its
// shard range + lease identity. A broker always claims EVERY manifested kind
// (learned live from kind_manifest); there is no kind subset.
type BrokerConfig struct {
	Pool *pgxpool.Pool

	// WorkerMaxParallel caps how many tasks of EACH (kind, task_type) are in
	// flight (claimed + fanning out to workers) on this broker. Left 0 by
	// DutiesFromConfig → broker.NewDispatch applies its 100 default (no env knob;
	// the per-worker cap lives on the worker binary via WORKER_MAX_PARALLEL).
	WorkerMaxParallel int
	HeartbeatEvery    time.Duration
	WorkerPollMin     time.Duration
	WorkerPollMax     time.Duration

	// DrainGrace is the graceful in-flight drain window on Stop: the dispatcher +
	// reactor stop claiming immediately but let already-fanned-out tasks land their
	// worker Complete for up to this long before abandoning them. 0 → no drain.
	DrainGrace time.Duration

	// Shards is the broker's claim range: a *runtime.ShardSet so the Resharder
	// can swap it at runtime on membership change (read lock-free each sweep).
	// nil → the dispatcher keeps its AllShards default.
	Shards *runtime.ShardSet

	// BrokerID is the lease identity stamped at claim — the broker's stable,
	// externally-chosen id so the AppendOutbox fence + cluster_members + reaper
	// attribute its leases.
	BrokerID string

	// RelayHTTPClient is the HTTP/2 (h2c/ALPN) transport this broker dials PEER
	// brokers' persistent Route stream with. It advertises this broker's live worker
	// CREDIT to peers and pushes a claimed task it has no local worker for to a
	// credited peer, so any worker can run work claimed by any broker (decouples
	// placement from consumption). Set for every broker; the mesh is a no-op with zero
	// peers (single-broker fleet). nil → mesh disabled. See internal/broker/relay.go.
	RelayHTTPClient connect.HTTPClient

	// MeshLivenessWindow is how recent a peer's heartbeat must be for the mesh to
	// route to it — the SAME window the Resharder uses for shard tiling (both 3× the
	// member-heartbeat cadence), so a crashed peer drops out of both within one window
	// rather than lingering to the ClusterMemberGC TTL. 0 → the broker duty derives it
	// from the member-heartbeat cadence.
	MeshLivenessWindow time.Duration

	// Claimed / Completed are the OTel dispatch counters, wired onto the broker
	// server AT CONSTRUCTION (before it serves → no data race). When metrics are on
	// they're non-nil (real or no-op) instruments; left nil here (the zero value, e.g.
	// a directly-built config) the broker's call sites nil-guard the Add.
	Claimed   metric.Int64Counter
	Completed metric.Int64Counter

	// WorkerAuthz / MeshAuthz are the per-service SPIFFE-ID allowlists the broker's
	// Handler installs as Connect interceptors — WorkerAuthz on WorkerService,
	// MeshAuthz on MeshService — narrowing the shared listener's union-admitted peer to
	// the right audience per RPC. nil = no allowlist for that service (chain trust).
	WorkerAuthz spiffeauthz.Matcher
	MeshAuthz   spiffeauthz.Matcher
	// PeerIdentitySource is the raw PEER_IDENTITY_SOURCE value ("" / "mtls" /
	// "mesh-header") the broker resolves connected-client identity by. See SetAuthz.
	PeerIdentitySource string
}

// DutiesFromConfig resolves an EngineConfig into the ordered duty list the
// Engine runs, derived from the ROLE-set RunControl/RunBroker flags. The control
// bundle (if present) comes FIRST and the claim duty LAST, so on shutdown the
// broker (reverse order) drains before the sweepers that feed it stop:
//
//	RunControl → controlDuty
//	RunBroker  → claimDuty (claims work AND lifecycle_outbox; every manifested kind)
//
// A config that yields no duties is legal (the pod does nothing but heartbeat) —
// the caller decides whether that's acceptable.
func DutiesFromConfig(cfg EngineConfig) ([]Duty, error) {
	var out []Duty
	if cfg.RunControl {
		out = append(out, ControlDuty(&ControlPlaneConfig{
			Shards:                      cfg.Shards,
			RetryAfter:                  cfg.RetryAfter,
			SweeperStaleAfter:           cfg.SweeperStaleAfter,
			SweeperInterval:             cfg.SweeperInterval,
			SweeperUnclaimedDeleteAfter: cfg.SweeperUnclaimedDeleteAfter,
			Swept:                       cfg.SweptCounter,
		}))
	}
	// A broker claims EVERY manifested kind (it learns them live from the
	// KindManifestCache), so RunBroker alone builds the claim duty. Per-pair fanout
	// concurrency uses the broker dispatcher's built-in default (see
	// broker.NewDispatch). (cmd/converge launches a dumb remote worker, not a
	// claim duty, for the worker surface, so this is only ever a real broker.)
	if cfg.RunBroker {
		out = append(out, ClaimDuty(&BrokerConfig{
			HeartbeatEvery:     cfg.HeartbeatEvery,
			WorkerPollMin:      cfg.WorkerPollMin,
			WorkerPollMax:      cfg.WorkerPollMax,
			DrainGrace:         cfg.ShutdownDrain,
			Shards:             cfg.Shards,
			BrokerID:           cfg.BrokerID,
			RelayHTTPClient:    cfg.RelayHTTPClient,
			MeshLivenessWindow: cfg.MeshLivenessWindow,
			WorkerAuthz:        cfg.WorkerAuthz,
			MeshAuthz:          cfg.MeshAuthz,
			PeerIdentitySource: cfg.PeerIdentitySource,
			Claimed:            cfg.BrokerClaimed,
			Completed:          cfg.BrokerCompleted,
		}))
	}
	return out, nil
}
