package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/runtime"
)

// Config: process-level knobs read from the environment.
// Config is the process-wide boot configuration, read from the environment via
// envconfig. Fields are grouped by WHERE they take effect — SHARED (every pod,
// or both tiers), CONTROL PLANE (the API server + sweepers), then BROKER (the
// claim/dispatch tier) — and alphabetical within each group. Role selects which
// tiers this pod actually runs (see engine.DutiesFromConfig); a knob in a tier
// group is inert on a pod that doesn't run that tier.
type Config struct {
	// ── SHARED: every pod / both tiers ──────────────────────────────────────

	// DBIAMAuth enables AWS IAM database authentication (RDS / Aurora) for ALL
	// Postgres connections — the primary pool, the read-replica pool, and the
	// migrate path. When true, the DSN password is ignored and every new physical
	// connection is opened with a short-lived IAM auth token minted from the pod's
	// AWS credentials (the default chain: env vars, shared config, IRSA, IMDS).
	// The token rides on pgx's BeforeConnect hook, so the 30m connection recycle
	// and any health-check reconnect transparently pick up rotated credentials —
	// no restart. The DSN still supplies host/port/user/dbname and MUST use
	// sslmode=require (or stronger): Aurora IAM auth requires TLS. Default false →
	// the DSN password is used as-is (local/dev/test are unaffected).
	DBIAMAuth bool `envconfig:"DB_IAM_AUTH" default:"false"`
	// DatabaseReadURL: optional read-replica DSN. When set, the API server routes
	// UI read traffic (every GET) to this pool while writes (POST/PUT/DELETE and
	// all engine traffic) stay on the primary. Empty → all queries hit DatabaseURL.
	DatabaseReadURL string `envconfig:"DATABASE_READ_URL"`
	DatabaseURL     string `envconfig:"DATABASE_URL" default:"postgres://localhost:5432/converge?sslmode=disable"`
	// HealthAddr is where a pod with NO main API server (i.e. not control/all)
	// serves the k8s probes (/healthz /livez /readyz). Control/all pods serve the
	// probes on ListenAddr's main mux instead, so this listener only starts when
	// the API server doesn't. Empty disables the probe listener entirely. Plain
	// HTTP only (bare host:port) — kubelet probes a private port.
	HealthAddr string `envconfig:"HEALTH_ADDR" default:":8081"`
	// LogFormat selects the slog handler: "json" emits structured JSON logs
	// (one object per line — for k8s log shippers / Loki / CloudWatch); anything
	// else (default) keeps the human-readable text handler.
	LogFormat string `envconfig:"LOG_FORMAT" default:"text"`
	LogLevel  string `envconfig:"LOG_LEVEL"  default:"info"`
	// MemberHeartbeatEvery is the cadence at which EVERY process UPSERTs its row in
	// the cluster_members registry (the cluster view). Distinct from HeartbeatEvery
	// (the broker's work_queue task-lease heartbeat). Default 10s (runtime.
	// DefaultMemberHeartbeatEvery) — it is the ONE member-liveness cadence: both the
	// mesh peer-eviction window and the Resharder's shard-tiling window are 3× it (30s),
	// so a crashed member drops out of routing + tiling within ~one window; the API
	// marks a member NotReady after ~3 missed beats and the GC reclaims it after ~5m.
	MemberHeartbeatEvery time.Duration `envconfig:"MEMBER_HEARTBEAT_EVERY"`
	// MetricsEnabled turns on OpenTelemetry metrics, exported as a Prometheus
	// /metrics endpoint mounted on the health listener (HealthAddr, :8081) — a
	// plaintext port, so a scraper never needs a client cert (see the metrics docs).
	// Off by default. OTEL_SERVICE_NAME overrides the resource service.name.
	MetricsEnabled  bool   `envconfig:"OTEL_METRICS_ENABLED" default:"false"`
	OTELServiceName string `envconfig:"OTEL_SERVICE_NAME" default:"converge"`
	NoHTTP          bool   `envconfig:"NO_HTTP" default:"false"`
	PgPoolMaxConns  int    `envconfig:"PG_POOL_MAX_CONNS"`
	PgPoolMinConns  int    `envconfig:"PG_POOL_MIN_CONNS"`
	// Role: "all" | "control" | "broker". converge is ONLY control plane and/or
	// broker — NEVER a worker (all provider code lives in the worker binary; dumb
	// workers connect IN to a broker). "all" = control + broker; "control" =
	// sweepers + API only; "broker" = claim every manifested kind + serve the
	// broker Connect services (WorkerService + MeshService). Role resolves into a SET
	// OF DUTIES (engine.DutiesFromConfig).
	Role string `envconfig:"ROLE" default:"all"`
	// ShutdownDrain bounds the graceful-shutdown DRAIN: on SIGTERM each duty stops
	// claiming new work but lets in-flight work land — the claim duty waits for
	// already-fanned-out tasks' worker Completes to arrive (instead of abandoning
	// them) for up to this long, then releases stragglers. Keep it BELOW the k8s
	// terminationGracePeriodSeconds (60s) so the process exits before SIGKILL, and
	// AT/ABOVE the slowest remote handler so a node move drains real work. The
	// later ReleaseClaims/Deregister steps have their own short 5s deadlines on top.
	ShutdownDrain time.Duration `envconfig:"SHUTDOWN_DRAIN" default:"45s"`
	// TLS scaffold shared by the API listener (control) AND the broker Connect
	// listener (broker) — both build a hot-reloading certReloader from these:
	//   TLSCertFile / TLSKeyFile   PEM server cert + key; REQUIRED for an https://
	//                              listener, must be empty otherwise. Hot-reloaded
	//                              (cert-manager / mounted-secret rotation, no restart).
	//   TLSClientCAFile            OPTIONAL mTLS: require + verify a client cert
	//                              signed by this CA (tls.RequireAndVerifyClientCert);
	//                              empty → one-way TLS (clients unauthenticated).
	//   TLSReloadInterval          how often the files are re-read for rotation
	//                              (default 3m; content-compared, so no-op if same).
	//
	// SPIFFE-ID AUTHZ is PER-AUDIENCE — there are three mTLS listeners, each dialed by
	// a DISTINCT client population, so each carries its OWN allowlist rather than one
	// shared list (a worker cert must not be able to reach the API, a peer-broker cert
	// must not be able to reach the worker surface, and so on):
	//   APIAuthzSPIFFEIDs     the API listener (control) — conctl / UI / CI / humans.
	//   WorkerAuthzSPIFFEIDs  the broker's WorkerService — the dumb workers that pull work.
	//   MeshAuthzSPIFFEIDs    the broker's MeshService — PEER BROKERS on the internal mesh.
	// Each is an OPTIONAL comma-separated spiffe://… list layered on top of mTLS: an
	// accepted client cert must also carry a matching URI-SAN SPIFFE ID. Each requires
	// TLSClientCAFile (SPIFFE authz needs client certs to check). Empty → chain trust
	// only for that listener. The broker's WorkerService + MeshService share ONE
	// listener, so its TLS handshake admits the UNION of the two, and per-service
	// Connect interceptors then narrow each RPC to the correct allowlist.
	// mTLS + these SPIFFE allowlists are the ONLY authn/authz gating who reaches a listener.
	//
	// PeerIdentitySource selects how the BROKER derives a connected client's OBSERVED
	// identity (for the connected-worker cluster view + the "running on <worker>"
	// attribution) — NEVER a client self-report. "" / "mtls" (default): the client's mTLS
	// cert SPIFFE ID (correct when the broker terminates mTLS itself). "mesh-header": a
	// service mesh (Istio/Linkerd) terminates mTLS in a sidecar, so the workload identity
	// arrives in a TRUSTED header (Istio X-Forwarded-Client-Cert, Linkerd l5d-client-id)
	// rather than the connection cert. OPT-IN because a header is forgeable by a direct
	// client — set it ONLY when a mesh fronts the broker and sets/strips these headers.
	// Either way, if no verified identity is found the broker falls back to the peer IP.
	APIAuthzSPIFFEIDs    string        `envconfig:"API_AUTHZ_SPIFFE_IDS"`
	MeshAuthzSPIFFEIDs   string        `envconfig:"MESH_AUTHZ_SPIFFE_IDS"`
	PeerIdentitySource   string        `envconfig:"PEER_IDENTITY_SOURCE"`
	TLSCertFile          string        `envconfig:"TLS_CERT_FILE"`
	TLSClientCAFile      string        `envconfig:"TLS_CLIENT_CA_FILE"`
	TLSKeyFile           string        `envconfig:"TLS_KEY_FILE"`
	TLSReloadInterval    time.Duration `envconfig:"TLS_RELOAD_INTERVAL" default:"3m"`
	WorkerAuthzSPIFFEIDs string        `envconfig:"WORKER_AUTHZ_SPIFFE_IDS"`

	// ── CONTROL PLANE: the HTTP API server + sweepers (Role control/all) ────

	// APIRateLimitRPS enables per-client-IP rate limiting on the HTTP API when
	// > 0 (requests/sec per client). 0 (default) = OFF. APIRateLimitBurst is the
	// token-bucket burst size (defaults to the RPS when <= 0). APIRateLimitTrustProxy
	// keys the limit on X-Forwarded-For / X-Real-IP instead of the socket peer —
	// set it ONLY behind a proxy/LB that overwrites those headers, else a client
	// can forge them to dodge the limit. Applies to API operations only; the k8s
	// probes, raw downloads, and the SPA are exempt.
	APIRateLimitBurst      int     `envconfig:"API_RATE_LIMIT_BURST"       default:"0"`
	APIRateLimitRPS        float64 `envconfig:"API_RATE_LIMIT_RPS"         default:"0"`
	APIRateLimitTrustProxy bool    `envconfig:"API_RATE_LIMIT_TRUST_PROXY" default:"false"`
	// CORSAllowedOrigins is the cross-origin allowlist for the HTTP API
	// (comma-separated). Empty (default) sends NO CORS headers — same-origin only,
	// correct since the SPA is served from this same origin; a permissive default
	// would let any web page drive the (mTLS-gated) state-changing endpoints. List
	// explicit origins to allow cross-origin browsers, or "*" for dev allow-any.
	CORSAllowedOrigins []string `envconfig:"CORS_ALLOWED_ORIGINS"`
	// ListenAddr is the API server bind address: a bare host:port (":8080" — plain
	// HTTP) or a URL whose scheme selects the protocol (http:// plain, https:// →
	// TLS, which requires TLSCertFile + TLSKeyFile). The scheme is authoritative:
	// https without cert/key, or http/bare WITH cert/key, fails fast (parseListenAddr).
	ListenAddr string `envconfig:"LISTEN_ADDR" default:":8080"`
	// Sweeper knobs (the control bundle: drain/reap/specgc/resync/membergc).
	// RetryAfter re-pends a failed task after this delay; SweeperInterval is the
	// sweep cadence; SweeperStaleAfter recovers a dead CLAIMED task's lease;
	// SweeperUnclaimedDeleteAfter GCs abandoned UNCLAIMED work (a work_queue row no
	// worker for its kind ever picked up — default 24h, 0 disables). Zero = the
	// driver default (see runtime.NewReaper / control_plane).
	RetryAfter                  time.Duration `envconfig:"RETRY_AFTER"`
	SweeperInterval             time.Duration `envconfig:"SWEEPER_INTERVAL"`
	SweeperStaleAfter           time.Duration `envconfig:"SWEEPER_STALE_AFTER"`
	SweeperUnclaimedDeleteAfter time.Duration `envconfig:"SWEEPER_UNCLAIMED_DELETE_AFTER"`

	// ── BROKER: the claim/dispatch + Connect gateway tier (Role broker/all) ────

	// BrokerAddr is where a BROKER pod serves the WorkerService Connect API that dumb
	// workers connect to (and the internal MeshService peer brokers dial). Reuses the
	// shared TLS_* scaffold (https:// → TLS, +TLS_CLIENT_CA_FILE → mTLS); the handshake
	// admits the UNION of WORKER_AUTHZ_SPIFFE_IDS + MESH_AUTHZ_SPIFFE_IDS and per-service
	// interceptors narrow each RPC, so a worker presents its client cert / SPIFFE id to
	// pull work and a peer broker its own to route. Default :9090.
	BrokerAddr string `envconfig:"BROKER_ADDR_LISTEN" default:":9090"`
	// HeartbeatEvery is the broker's work_queue task-LEASE heartbeat cadence (the
	// dispatcher renews claimed leases). Distinct from MemberHeartbeatEvery (the
	// registry beat). Zero = the dispatcher default (runtime/loop.go).
	HeartbeatEvery time.Duration `envconfig:"HEARTBEAT_EVERY"`
	// RelayAdvertiseAddr is the dial-able MeshService URL PEER brokers use to
	// reach THIS broker for relay (published in cluster_members.Config). Broker-to-
	// broker relay decouples work placement from consumption: a worker connected to
	// ANY broker can run work another claimed, so nothing strands when a worker
	// lands on a broker that doesn't own its kind's tile. Relay is ALWAYS ON for a
	// broker (the only dispatch model) and a no-op for a single-broker fleet.
	// Required in a MULTI-broker fleet (e.g. "http://$POD_IP:9090"); empty → this
	// broker serves relay but advertises no address (peers can't reach it).
	RelayAdvertiseAddr string `envconfig:"RELAY_ADVERTISE_ADDR"`
	// WorkerPollMin / WorkerPollMax bound the dispatcher's claim-poll backoff.
	// Zero = the dispatcher default (runtime/loop.go).
	WorkerPollMax time.Duration `envconfig:"WORKER_POLL_MAX"`
	WorkerPollMin time.Duration `envconfig:"WORKER_POLL_MIN"`
}

// applyDefaults fills the tunable duration knobs an operator left unset (zero)
// with the SAME effective default each driver would apply downstream — sourced
// from the owning package's exported constant so the two never drift. This makes
// the resolved Config the single source of truth: the cluster-view report
// (memberConfigJSON) then shows the REAL running value instead of "0s", while the
// drivers' own `if x > 0 { override }` guards see the now-positive value and
// behave identically. An explicit env override (already non-zero) is preserved.
//
// NOTE: the pgx pool sizes (PgPoolMaxConns/MinConns) are NOT defaulted here —
// pgx computes them from the host at ParseConfig time (max = max(4, numCPU)), so
// there is no constant to mirror; run() reads the RESOLVED values back off the
// parsed pool config into cfg before reporting (see the pool-build path).
func (c *Config) applyDefaults() {
	if c.MemberHeartbeatEvery == 0 {
		c.MemberHeartbeatEvery = runtime.DefaultMemberHeartbeatEvery
	}
	if c.HeartbeatEvery == 0 {
		c.HeartbeatEvery = runtime.DefaultHeartbeatEvery
	}
	if c.WorkerPollMin == 0 {
		c.WorkerPollMin = runtime.DefaultPollMin
	}
	if c.WorkerPollMax == 0 {
		c.WorkerPollMax = runtime.DefaultPollMax
	}
	if c.SweeperStaleAfter == 0 {
		c.SweeperStaleAfter = runtime.DefaultSweeperStaleAfter
	}
	if c.SweeperUnclaimedDeleteAfter == 0 {
		c.SweeperUnclaimedDeleteAfter = runtime.DefaultSweeperUnclaimedDeleteAfter
	}
	if c.RetryAfter == 0 {
		c.RetryAfter = engine.DefaultRetryAfter
	}
	if c.SweeperInterval == 0 {
		c.SweeperInterval = engine.DefaultSweeperInterval
	}
}

type roleConfig struct {
	runControl bool
	// runBroker makes the pod a BROKER (the universal Connect claim tier): it owns
	// its shard tile, claims tasks (work AND lifecycle), selects each task's
	// reaction, and ships it to dumb WORKERS over the WorkerService. The
	// broker drains lifecycle_outbox too, so reactors fan out to the same workers.
	// A broker claims EVERY manifested kind (it learns them LIVE from kind_manifest
	// via the KindManifestCache, never from compiled-in providers — converge carries NO
	// provider code). converge is ONLY ever control and/or broker — never a worker;
	// all provider handler code lives exclusively in the worker binary.
	runBroker bool
}

// parseRole resolves the ROLE env var:
//
//	all      control plane + broker (for every kind) + API server
//	control  control plane only (sweepers + API; no claiming)
//	broker   broker for every kind (owns a shard tile, claims + fans to workers)
//
// converge is ONLY ever a control plane and/or a broker — NEVER a worker. All
// provider handler code lives exclusively in the worker binary; the dumb
// workers connect IN to a broker and pull work. So this binary links NO provider
// package, and a broker learns which kinds to claim LIVE from kind_manifest (the
// KindManifestCache), not from a compiled-in kind list — hence parseRole needs no
// provider/kind argument.
func parseRole(role string) (roleConfig, error) {
	switch role {
	case "", "all":
		return roleConfig{runControl: true, runBroker: true}, nil
	case "control":
		return roleConfig{runControl: true}, nil
	case "broker":
		return roleConfig{runBroker: true}, nil
	}
	return roleConfig{}, fmt.Errorf("unknown ROLE: %q (want all, control, or broker)", role)
}

// memberConfigJSON marshals the booted runtime knobs into the small JSONB
// blob shown in the cluster view's expandable config panel. Durations are
// rendered as human strings (e.g. "30s") via Duration.String(). Only the knobs
// an operator tunes are included; per-kind/per-cloud config (read by each
// provider's Setup) is deliberately omitted.
func memberConfigJSON(cfg Config) json.RawMessage {
	m := map[string]string{
		"heartbeat_every":                cfg.HeartbeatEvery.String(),
		"worker_poll_min":                cfg.WorkerPollMin.String(),
		"worker_poll_max":                cfg.WorkerPollMax.String(),
		"sweeper_stale_after":            cfg.SweeperStaleAfter.String(),
		"sweeper_interval":               cfg.SweeperInterval.String(),
		"sweeper_unclaimed_delete_after": cfg.SweeperUnclaimedDeleteAfter.String(),
		"retry_after":                    cfg.RetryAfter.String(),
		"pg_pool_max_conns":              strconv.Itoa(cfg.PgPoolMaxConns),
		"pg_pool_min_conns":              strconv.Itoa(cfg.PgPoolMinConns),
	}
	b, err := json.Marshal(m)
	if err != nil {
		return json.RawMessage("{}")
	}
	// RELAY: advertise this broker's dial-able MeshService address so PEER
	// brokers can reach it (the Route mesh); peers read it from
	// cluster_members.Config[connect_addr]. AdvertiseAddr is a no-op when the addr is
	// empty (non-broker / single-broker fleet), so this is unconditional.
	return broker.AdvertiseAddr(b, cfg.RelayAdvertiseAddr)
}

// listenConfig is the resolved HTTP server binding: the host:port to
// bind and, when TLS is enabled, the cert/key files to serve it with.
type listenConfig struct {
	// addr is a bare host:port suitable for http.Server.Addr / net.Listen
	// (the scheme, if any, has been stripped).
	addr string
	// tls reports whether the server should serve TLS (https scheme).
	tls bool
	// certFile / keyFile are the PEM server cert + key, set iff tls.
	certFile string
	keyFile  string
	// clientCAFile is the PEM client-CA bundle for mTLS, set iff tls AND a CA
	// was configured; empty leaves clients unauthenticated (one-way TLS).
	clientCAFile string
}

// parseListenAddr resolves a listen address into a listenConfig. The
// address is either a bare host:port (":8080", "0.0.0.0:8080" — plain
// HTTP) or a URL whose scheme picks the protocol:
//
//	http://host:port  → plain HTTP
//	https://host:port → TLS, using certFile + keyFile
//
// The SCHEME is authoritative and validated against the cert/key pair so
// a misconfiguration fails fast at boot rather than silently serving the
// wrong protocol:
//   - https:// requires BOTH certFile and keyFile.
//   - http:// (or a bare host:port) must NOT be given cert/key.
//
// certFile/keyFile come from TLS_CERT_FILE/TLS_KEY_FILE; pass "" when unset.
// clientCAFile comes from TLS_CLIENT_CA_FILE; it is meaningful only with
// https:// (it enables mTLS) and is rejected for http://bare to fail fast on a
// config that would silently NOT authenticate clients.
func parseListenAddr(raw, certFile, keyFile, clientCAFile string) (listenConfig, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return listenConfig{}, fmt.Errorf("listen address is empty")
	}
	hasTLSFiles := certFile != "" || keyFile != ""

	// A bare host:port has no "scheme://" prefix. Detect a URL only when a
	// scheme is actually present so ":8080" / "0.0.0.0:8080" stay literal
	// (url.Parse would otherwise misread the host as a scheme).
	scheme, addr := "", raw
	if i := strings.Index(raw, "://"); i >= 0 {
		u, err := url.Parse(raw)
		if err != nil {
			return listenConfig{}, fmt.Errorf("invalid listen address %q: %w", raw, err)
		}
		scheme = strings.ToLower(u.Scheme)
		addr = u.Host // host:port, scheme + path stripped
		if addr == "" {
			return listenConfig{}, fmt.Errorf("listen address %q has no host:port", raw)
		}
	}

	switch scheme {
	case "", "http":
		if hasTLSFiles {
			return listenConfig{}, fmt.Errorf(
				"TLS_CERT_FILE/TLS_KEY_FILE set but LISTEN_ADDR %q is not https:// — use https:// to enable TLS or unset the cert/key", raw)
		}
		if clientCAFile != "" {
			return listenConfig{}, fmt.Errorf(
				"TLS_CLIENT_CA_FILE set but LISTEN_ADDR %q is not https:// — mTLS requires https://", raw)
		}
		return listenConfig{addr: addr}, nil
	case "https":
		if certFile == "" || keyFile == "" {
			return listenConfig{}, fmt.Errorf(
				"LISTEN_ADDR %q is https:// but TLS_CERT_FILE/TLS_KEY_FILE are not both set", raw)
		}
		return listenConfig{addr: addr, tls: true, certFile: certFile, keyFile: keyFile, clientCAFile: clientCAFile}, nil
	default:
		return listenConfig{}, fmt.Errorf("unsupported LISTEN_ADDR scheme %q (want http or https)", scheme)
	}
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// newLogHandler builds the slog handler for the configured format/level.
// LOG_FORMAT=json gives structured JSON (one object per line) for log
// shippers; anything else keeps the human-readable text handler.
func newLogHandler(cfg Config) slog.Handler {
	opts := &slog.HandlerOptions{Level: parseLogLevel(cfg.LogLevel)}
	if strings.EqualFold(cfg.LogFormat, "json") {
		return slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.NewTextHandler(os.Stdout, opts)
}
