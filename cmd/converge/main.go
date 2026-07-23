package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelseyhightower/envconfig"

	"github.com/salesforce/converge/db"
	"github.com/salesforce/converge/internal/api"
	"github.com/salesforce/converge/internal/awsauth"
	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/health"
	"github.com/salesforce/converge/internal/metrics"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/specschema"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/pkg/spiffeauthz"
)

// BuildVersion is the build/version string stamped into each cluster member's
// registry row (the cluster view's VERSION column). Defaults to "dev"; set at
// build time via the linker, e.g.:
//
//	go build -ldflags "-X main.BuildVersion=$(git describe --tags --always)" ...
var BuildVersion = "dev"

// configureServerTLS installs the DYNAMIC (hot-reloading) TLS material on srv
// when lc is a TLS listener, and returns whether TLS was configured (so the
// caller knows to serve via ListenAndServeTLS). It is the single wiring shared
// by BOTH the control-plane REST API and the broker Connect listener, so their
// TLS/mTLS/SPIFFE behaviour can't drift:
//
//   - builds a certReloader over lc's cert/key (+ client CA for mTLS) and the
//     optional SPIFFE-ID allowlist matcher — failing fast at boot on bad material;
//   - installs reloader.tlsConfig() as srv.TLSConfig, whose GetCertificate /
//     GetConfigForClient hooks read the CURRENT keypair + CA pool on every
//     handshake (so a rotated cert/CA takes effect with no restart);
//   - starts the reloader's poll loop on ctx (stops at shutdown).
//
// authz is the parsed SPIFFE-ID allowlist enforced at the handshake (nil = chain
// trust only); a listener serving several audiences passes their union and narrows
// per-service afterwards. On a plain-HTTP lc it does nothing and returns false.
// name tags the reloader's boot error so a failure is attributable to the right listener.
func configureServerTLS(ctx context.Context, srv *http.Server, lc listenConfig, authz spiffeauthz.Matcher, reloadInterval time.Duration, name string) (bool, error) {
	if !lc.tls {
		return false, nil
	}
	reloader, err := newCertReloader(lc.certFile, lc.keyFile, lc.clientCAFile, authz)
	if err != nil {
		return false, fmt.Errorf("%s TLS material: %w", name, err)
	}
	srv.TLSConfig = reloader.tlsConfig()
	go reloader.watch(ctx, reloadInterval)
	return true, nil
}

func main() {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	cfg.applyDefaults()

	slog.SetDefault(slog.New(newLogHandler(cfg)))

	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		if err := runMigrate(cfg); err != nil {
			slog.Error("migration failed", "error", err)
			os.Exit(1)
		}
		return
	}

	if err := run(cfg); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

// minPoolMaxConns is the floor applyPoolHealthDefaults enforces on the pgx pool's
// MaxConns when the operator hasn't set PG_POOL_MAX_CONNS. It must exceed the number
// of long-lived LISTEN subscribers a control+broker pod pins (~6-7; see the call site)
// with clear headroom for working queries — pgx's own default of max(4, NumCPU) can
// fall at or below the LISTEN count on a small node, which deadlocks the pool.
const minPoolMaxConns int32 = 20

// applyPoolHealthDefaults sets the connection-pool knobs that keep
// dead connections from lingering after a Postgres restart, OOM kill,
// or network blip:
//
//   - HealthCheckPeriod: background sweep that probes idle connections
//     and discards broken ones. Without this, a connection that was
//     open when the server restarted is reused on next acquire and
//     fails (or hangs, if the new postgres backend is in a bad state).
//   - MaxConnLifetime: hard recycle. Even healthy-looking connections
//     get rotated, so any latent state (broken auth, in-flight tx
//     that the server forgot about, etc.) is bounded.
//   - MaxConnIdleTime: idle connections close earlier than the
//     lifetime, so the pool stays small under bursty traffic and
//     broken-idle conns get pruned faster than the lifetime alone
//     would manage.
//
// LIFETIME/IDLE are deliberately COARSE (30m / 5m). The earlier 5m / 1m values
// made the whole fleet recycle its connections far too often: across 53 pods
// (hundreds of pooled conns) the recycles bunched into the health-check sweep
// and tore down + re-established a big batch at once — mass backend fork + auth
// + LISTEN re-arm, a real CPU burst on the primary that never appears as SQL
// exec time (so it's invisible to pg_stat_statements). Rotating every 30m
// instead of 5m cuts that churn ~6x; recycling is a hygiene backstop, not
// something that needs to happen every few minutes on a healthy connection.
//
// Also applies the per-process Min/MaxConns overrides from env so
// the helper is the single place pool config gets tuned.
//
// When iam is non-nil (DB_IAM_AUTH=true) it installs the IAM-token BeforeConnect
// hook so every new physical connection authenticates with a freshly minted RDS
// auth token instead of the DSN password — and the 30m recycle below doubles as
// the credential-refresh tick (a recycled connection re-runs BeforeConnect and
// re-signs with whatever creds are current).
func applyPoolHealthDefaults(p *pgxpool.Config, cfg Config, iam *awsauth.IAMAuthenticator) {
	if cfg.PgPoolMaxConns > 0 {
		p.MaxConns = int32(cfg.PgPoolMaxConns)
	}
	// Floor MaxConns so a pod NEVER runs with fewer connections than its long-lived
	// LISTEN subscribers pin. pgx's default is max(4, runtime.NumCPU()); on a small
	// node (≤4 vCPU) with PG_POOL_MAX_CONNS unset that yields as few as 4 — but a
	// control+broker pod holds ~6-7 connections PERMANENTLY, one per active LISTEN
	// (drainer outbox_ready + rollup_recheck, reactor lifecycle_ready, dispatcher
	// work_ready, topology + mesh cluster_changed, notify_refresher). Each Listen
	// Acquires a pooled conn and blocks on WaitForNotification for its lifetime
	// (internal/runtime/listen.go), so if MaxConns ≤ the LISTEN count the LISTENers
	// exhaust the pool and every working query blocks forever in Acquire — a silent
	// wedge, no error logged. minPoolMaxConns keeps clear headroom above the LISTENers
	// for working queries regardless of NumCPU. An explicit env override wins even if
	// lower — an operator who sets PG_POOL_MAX_CONNS owns the sizing.
	if cfg.PgPoolMaxConns == 0 && p.MaxConns < minPoolMaxConns {
		p.MaxConns = minPoolMaxConns
	}
	if cfg.PgPoolMinConns > 0 {
		p.MinConns = int32(cfg.PgPoolMinConns)
	}
	p.HealthCheckPeriod = 30 * time.Second
	p.MaxConnLifetime = 30 * time.Minute
	p.MaxConnIdleTime = 5 * time.Minute
	// On top of the longer lifetime, jitter it so conns opened together (the
	// whole pool at pod boot) don't all hit MaxConnLifetime in the SAME sweep and
	// recycle as one synchronized batch — that lockstep wave is the spike the
	// longer lifetime makes rarer; the jitter spreads each remaining recycle
	// across a window so the cost stays a trickle, never a burst.
	p.MaxConnLifetimeJitter = 5 * time.Minute
	if iam != nil {
		p.BeforeConnect = iam.BeforeConnect()
	}
}

func run(cfg Config) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Resolve the role. converge is control and/or broker ONLY — never a worker —
	// so it carries NO provider code and needs no kind list to resolve the role.
	role, err := parseRole(cfg.Role)
	if err != nil {
		return err
	}

	// Parse the three per-audience SPIFFE-ID allowlists up front so a malformed ID
	// fails fast at boot, before any listener starts. Each gates a DISTINCT client
	// population on its own mTLS listener: API (conctl/UI/CI), the broker's
	// WorkerService (workers), the broker's MeshService (peer brokers). nil = no
	// allowlist for that surface (chain trust only). The broker's two services share
	// ONE listener, so its handshake admits the UNION and per-service interceptors
	// (broker.NewDispatch) narrow each RPC to its own allowlist.
	apiAuthz, err := spiffeauthz.Parse(cfg.APIAuthzSPIFFEIDs)
	if err != nil {
		return fmt.Errorf("API_AUTHZ_SPIFFE_IDS: %w", err)
	}
	workerAuthz, err := spiffeauthz.Parse(cfg.WorkerAuthzSPIFFEIDs)
	if err != nil {
		return fmt.Errorf("WORKER_AUTHZ_SPIFFE_IDS: %w", err)
	}
	meshAuthz, err := spiffeauthz.Parse(cfg.MeshAuthzSPIFFEIDs)
	if err != nil {
		return fmt.Errorf("MESH_AUTHZ_SPIFFE_IDS: %w", err)
	}

	// This pod's stable, globally-unique identity: "<hostname>-<pid>-<uuid8>". It is
	// the single id the work_queue/lifecycle claims (EngineConfig.BrokerID), the
	// cluster-member registry row, the metrics resource, and logs all correlate to.
	// Computed here (no dependencies) so the metrics provider below can stamp it as
	// the per-pod service.instance.id.
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	memberID := fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), uuid.New().String()[:8])

	// OpenTelemetry metrics (Prometheus /metrics on the plaintext health listener).
	// Built early so instruments can register during startup; a no-op Provider when
	// disabled. Shut down late (after the duties that emit metrics stop) — deferred
	// here on context.Background() so a flush isn't cut short by the cancelled ctx.
	metricsProvider, err := metrics.New(metrics.Config{
		Enabled:        cfg.MetricsEnabled,
		ServiceName:    cfg.OTELServiceName,
		ServiceVersion: BuildVersion,
		Role:           cfg.Role,
		InstanceID:     memberID,
	})
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer scancel()
		if err := metricsProvider.Shutdown(sctx); err != nil {
			slog.Warn("metrics shutdown", "error", err)
		}
	}()

	// AWS IAM database auth (RDS / Aurora), resolved ONCE here and shared by the
	// primary pool, the read-replica pool, and the migrate path. nil when
	// DB_IAM_AUTH is off → the DSN password is used as-is. Building it eagerly
	// fails the pod fast on a missing region/credentials rather than on first
	// connect.
	var dbIAM *awsauth.IAMAuthenticator
	if cfg.DBIAMAuth {
		dbIAM, err = awsauth.New(ctx)
		if err != nil {
			return fmt.Errorf("init db iam auth: %w", err)
		}
		slog.Info("database iam auth enabled (rds/aurora)")
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	applyPoolHealthDefaults(poolCfg, cfg, dbIAM)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	// Migrate first — the sole migrate site, run for every role before anything reads
	// the schema. The config-plane cache and spec-validator rebuild below, and the
	// duties' sweepers in eng.Start, all read schema-dependent tables, so the schema
	// must exist first. Concurrent-safe via a Postgres session advisory lock (goose):
	// with no ordering between the control and broker Deployments, of the N pods
	// starting at once exactly one applies the pending migrations and the rest block
	// then no-op. Idempotent.
	if err := db.MigrateAllPool(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// Capture the RESOLVED primary pool sizes for the cluster-view report. Unlike
	// the duration knobs (Config.applyDefaults), these have no mirror-able constant:
	// pgx computes the default from the host at ParseConfig (MaxConns = max(4,
	// numCPU), MinConns = 0), and applyPoolHealthDefaults has already layered any
	// env override on top. We must NOT write these back into cfg yet: cfg is still
	// passed to applyPoolHealthDefaults for the READ-replica pool below, and a
	// now-positive PgPool*Conns would force the replica to the primary's size —
	// silently overriding a pool_max_conns in DATABASE_READ_URL. So stash them and
	// reflect into cfg only AFTER the read pool is built (before memberConfigJSON).
	resolvedPoolMaxConns := int(poolCfg.MaxConns)
	resolvedPoolMinConns := int(poolCfg.MinConns)

	// The process-wide spec validator is built from the kind MANIFESTS in the DB
	// (specschema.New decodes each manifest's JSON-Schema bytes into a huma
	// validator). A safe empty validator is installed up front so any pre-manifest
	// store.New has a non-nil default (validates everything OK); it is replaced
	// below once the manifests are in the DB and rebuilt on every manifest apply.
	store.SetDefaultValidator(specschema.New(nil))

	// memberID was computed above (near the metrics provider) — the broker's
	// work_queue + lifecycle_outbox claims (EngineConfig.BrokerID below), the
	// cluster-member registry row, logs, and the metrics resource all correlate to it.

	// The config plane serves each worker's DEFAULT providerconfig (the broker's
	// GetProviderConfig). converge carries NO providers, so it tracks the kind set
	// DYNAMICALLY from the DB (every applied manifest) rather than a compiled-in
	// list — a CRD applied after boot is config-served with no restart. A pure
	// control pod (no broker) still builds it cheaply; it just serves nothing.
	configPlane := runtime.NewProviderConfigCache(ctx, pool, nil)
	defer configPlane.Stop()

	manifestStore := store.New(pool)

	// CRDs (kind manifests) are applied via the API — PUT /api/kinds/{kind}/manifest
	// (conctl apply --type manifest). The server carries NO fixture-bootstrap: a
	// fresh cluster comes up empty and an operator (or a demo/test harness) applies
	// its kinds over the API. The broker learns claimable kinds LIVE from
	// kind_manifest (the KindManifestCache), so a CRD applied after boot registers with
	// no restart. The two AFTER triggers on kind_manifest derive kind_config +
	// reactor_bindings on apply.

	// Install the real spec validator now that the manifests are (or will be) in
	// the DB. The validator covers EVERY kind known to the cluster — not just this
	// pod's — so a control pod still validates writes for kinds another pod owns.
	// We read the full manifest set from kind_manifest (authoritative). It re-runs
	// on every manifest apply via SetOnManifestApply.
	rebuildValidator := func() {
		all, err := manifestStore.ListKindManifests(ctx)
		if err != nil {
			slog.Warn("rebuild spec validator: list kind manifests failed; keeping prior validator", "err", err)
			return
		}
		store.SetDefaultValidator(specschema.New(all))
	}
	rebuildValidator()

	// Optional read replica. The API server routes UI GETs here
	// when configured; engine traffic (composer, drainer, reaper,
	// workers) always stays on the primary.
	readPool := pool
	if cfg.DatabaseReadURL != "" {
		readCfg, err := pgxpool.ParseConfig(cfg.DatabaseReadURL)
		if err != nil {
			return fmt.Errorf("parse read database url: %w", err)
		}
		applyPoolHealthDefaults(readCfg, cfg, dbIAM)
		// Cap any single read-replica query server-side. This pool serves only
		// UI GETs (read-only, no long reconcile/drain txns), so a 30s
		// statement_timeout bounds a pathological query (e.g. a topology GROUP BY
		// over a huge subtree) even if the request-context deadline is somehow not
		// honored. NOT applied to the primary pool, which runs the seconds-long
		// drain tx and long reconcile work. When no replica is set, reads fall
		// back to the primary and the request-context deadline (the API
		// timeout middleware) is the only bound.
		if readCfg.ConnConfig.RuntimeParams == nil {
			readCfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		readCfg.ConnConfig.RuntimeParams["statement_timeout"] = "30000" // ms
		readPool, err = pgxpool.NewWithConfig(ctx, readCfg)
		if err != nil {
			return fmt.Errorf("connect to read replica: %w", err)
		}
		defer readPool.Close()
		slog.Info("read replica configured", "dsn_set", true)
	}

	// Now that the read-replica pool has been built from the ORIGINAL cfg, it's safe
	// to reflect the resolved PRIMARY pool sizes into cfg for the cluster-view report
	// (memberConfigJSON, below) — the replica pool already captured its own DSN-
	// resolved sizing and won't inherit the primary's.
	cfg.PgPoolMaxConns = resolvedPoolMaxConns
	cfg.PgPoolMinConns = resolvedPoolMinConns

	// ONE shard range per process, shared by every sharded driver it runs
	// (control sweepers and/or the broker dispatcher). Sharding is ALWAYS
	// DYNAMIC: the ShardSet starts owning NOTHING and the Resharder (below)
	// assigns its contiguous tile from live cluster_members, re-tiling as
	// members join/leave — no static pin, no StatefulSet ordinal.
	shardSet := runtime.NewShardSet(nil) // owns nothing until the Resharder assigns

	// WORKER pods are ALWAYS dumb Connect clients — there is NO native-SQL worker.
	// A pod that would run the dispatch SURFACE (it has dispatch kinds and is not
	// the control plane or the broker/gateway) MUST be a remote worker: it
	// requires BROKER_ADDR, runs NO in-process dispatch duty, and launches the
	// SDK worker runner (sdk-go/converge) that pulls stage tasks over Connect. The only DB-claiming
	// server-side roles are control + broker; everything that executes provider
	// code is a Connect client. dispatchKindsForEngine is therefore ALWAYS nil for a
	// worker pod (the in-process dispatch duty is never wired).
	// Resolve the pod's DUTY LIST from its role + knobs and run them through one
	// Engine: the control sweeper bundle (if any) and the claim duty (if any), as
	// uniform Duty values the Engine Start/Stops together. converge runs NO
	// in-process dispatch — all provider execution is remote (dumb workers
	// driven by the claim duty), so there is no DispatchKinds in-process path; the
	// broker learns its kinds LIVE from kind_manifest (ClaimKinds nil = all).
	// MESH: the transport this broker uses to dial PEER brokers' MeshService over
	// the persistent bidi Route stream (the broker's forwarding model). The Route RPC
	// is bidi-streaming, which REQUIRES end-to-end HTTP/2 — so routeHTTPClient dials
	// ALPN-h2 over the mTLS keypair when TLS is set, else h2c (HTTP/2 cleartext, via
	// the stdlib net/http Protocols) for the plain path, pooling a warm conn per peer.
	// A startup fail-fast probe (assertRouteH2, below) turns a silent HTTP/1.1
	// downgrade into a boot error.
	var relayClient connect.HTTPClient
	if role.runBroker {
		rc, err := routeHTTPClient(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile)
		if err != nil {
			return fmt.Errorf("mesh peer transport: %w", err)
		}
		relayClient = rc
		// GUARDRAIL: the mesh is the broker's ONLY way to serve work claimed on a tile
		// whose kind has no LOCAL worker (a worker landed on a different broker). With
		// no advertise address a broker publishes no connect_addr, so peers can't open a
		// Route to it — forwarding is SILENTLY dead and such work strands. Harmless for
		// a single-broker fleet; a latent stranding bug for a multi-broker one. Warn
		// loudly so a misconfigured multi-broker deploy is visible at boot.
		if cfg.RelayAdvertiseAddr == "" {
			slog.Warn("broker mesh: RELAY_ADVERTISE_ADDR is unset — this broker will NOT be reachable by peers for forwarding; " +
				"in a multi-broker fleet, work claimed on a tile whose kind has no local worker will STRAND. " +
				"Set it to this broker's dial-able MeshService URL (e.g. http://<pod-ip-or-host>:9090). Safe to ignore on a single-broker fleet.")
		}
	}
	// Register the SYNCHRONOUS metric counters BEFORE building the duties so they wire
	// into each duty AT CONSTRUCTION (before Start → no data race on the hot path). A
	// disabled Provider still returns non-nil NO-OP counters here, so the duties' Add
	// call sites run harmlessly; nothing is exported until metrics are enabled.
	brokerCounters := metricsProvider.RegisterBroker()
	controlCounters := metricsProvider.RegisterControl()
	duties, err := engine.DutiesFromConfig(engine.EngineConfig{
		RunControl:                  role.runControl,
		RunBroker:                   role.runBroker,
		BrokerID:                    memberID, // claim/reactor dispatcher claims under the pod's member id
		RetryAfter:                  cfg.RetryAfter,
		SweeperStaleAfter:           cfg.SweeperStaleAfter,
		SweeperInterval:             cfg.SweeperInterval,
		SweeperUnclaimedDeleteAfter: cfg.SweeperUnclaimedDeleteAfter,
		HeartbeatEvery:              cfg.HeartbeatEvery,
		WorkerPollMin:               cfg.WorkerPollMin,
		WorkerPollMax:               cfg.WorkerPollMax,
		ShutdownDrain:               cfg.ShutdownDrain,
		Shards:                      shardSet,
		RelayHTTPClient:             relayClient,
		MeshLivenessWindow:          3 * cfg.MemberHeartbeatEvery, // same window the Resharder uses; a crashed peer drops out of the mesh within it
		WorkerAuthz:                 workerAuthz,
		MeshAuthz:                   meshAuthz,
		PeerIdentitySource:          cfg.PeerIdentitySource,
		BrokerClaimed:               brokerCounters.Claimed,
		BrokerCompleted:             brokerCounters.Completed,
		SweptCounter:                controlCounters.Swept,
	})
	if err != nil {
		return fmt.Errorf("resolve duties: %w", err)
	}
	eng := engine.NewEngine(duties, engine.Deps{
		Pool:   pool,
		Shards: shardSet,
		// The claim duty builds its own KindManifestCache and ships every reaction —
		// work AND lifecycle (STAGE_REACT) — to dumb workers, so the engine
		// needs no in-process reaction handlers. Configs serves each worker's live
		// default providerconfig over GetProviderConfig.
		Configs: configPlane,
	})
	if err := eng.Start(ctx); err != nil {
		return fmt.Errorf("start engine: %w", err)
	}

	// Register the OBSERVABLE gauges against the now-live engine (they read it at
	// scrape time — no data race, no hot-path cost). Process + DB-pool always; broker
	// gauges only on a broker pod. (The synchronous counters were already wired into
	// the duties at construction, above.)
	if metricsProvider.Enabled() {
		metricsProvider.RegisterProcess(BuildVersion, cfg.Role)
		metricsProvider.RegisterDBPool(func() metrics.PoolStat { return pool.Stat() })
		if eng.RunsBroker() {
			metricsProvider.RegisterBrokerGauges(eng)
		}
	}

	// PUSH config to workers: when the ProviderConfigCache observes a default-providerconfig
	// edit (on EITHER axis — spec or bundle), the broker broadcasts the WHOLE new
	// providerconfig (spec + data together, one ProviderConfigUpdate) down every
	// connected worker's WorkStream so it applies live (no-op on a control-only pod).
	// The failsafe poll below + the worker's periodic GetProviderConfig still catch a
	// dropped push.
	configPlane.OnKindConfigChange = eng.BroadcastConfig

	// Begin live config reconfigure (providerconfig_changed listener + failsafe
	// poll) so an operator's default-config edit reaches workers with no restart.
	configPlane.Start(ctx)

	// ClusterMemberReporter — the running-fleet heartbeat. ONE per process
	// (here, not in the engine), so a role=all process running both the control
	// sweepers and a dispatch duty still writes a SINGLE cluster_members row.
	// It carries this member's static identity (role/kinds/owned shard span/
	// booted config/version/host/pid/started_at) and a live in-flight count read
	// from the engine's dispatch duty (0 on a control-only member with no
	// dispatch duty). On clean shutdown the member deregisters (deletes its own
	// row); the ControlPlane's ClusterMemberGC reclaims it otherwise.
	// hostname + memberID are computed once, earlier (shared with the react
	// dispatcher's BrokerID).
	reporter := runtime.NewClusterMemberReporter(store.New(pool), store.ClusterMemberInfo{
		MemberID:  memberID,
		Role:      cfg.Role,
		Config:    memberConfigJSON(cfg),
		Version:   BuildVersion,
		Hostname:  hostname,
		PID:       os.Getpid(),
		StartedAt: time.Now(),
	})
	// in_flight is a BROKER concept (tasks parked awaiting a worker); only a
	// broker pod reports it. Control/react pods have no dispatch duty, so the
	// hook stays nil and their in_flight is reported as 0 (the UI shows "—").
	if role.runBroker {
		reporter.InFlight = eng.InFlight
	}
	// The reporter reads the member's CURRENT shard span from the shared
	// ShardSet on each beat, so the registry's `shards` column tracks dynamic
	// reassignment (and reflects the pinned range in static mode). The first
	// beat (in Start) registers the row — which fires the cluster_changed
	// trigger that wakes every node's Resharder.
	reporter.Shards = shardSet
	// MESH: worker CREDIT is pushed LIVE over the persistent Route stream (Interest
	// frames), NOT advertised via cluster_members — so cluster_members carries only
	// the static config + connect_addr (via memberConfigJSON → AdvertiseAddr), which is
	// all a peer needs to DIAL a route; once dialed, credit flows over the route
	// sub-second.
	//
	// CLUSTER VIEW: a broker additionally publishes its LIVE connected-worker
	// snapshot (which workers are connected here + the kinds each runs) via the
	// reporter's Workers hook, written to the dedicated cluster_members.workers
	// column each beat — its own LIVE column, sibling to in_flight, NOT stuffed
	// into the static config blob. Observability only (not consulted for routing;
	// that rides the route's live credit). Only a broker pod has workers; a
	// control/react pod leaves the hook nil (the column is written "[]").
	if role.runBroker {
		reporter.Workers = func() json.RawMessage {
			return broker.WorkersJSON(eng.ConnectedWorkers())
		}
	}
	if cfg.MemberHeartbeatEvery > 0 {
		reporter.Interval = cfg.MemberHeartbeatEvery
	}
	reporter.Start(ctx)
	// Backstop for EARLY-RETURN paths (a boot error after this point): release the
	// reporter's LISTEN conn so the deferred pool.Close() can't hang on it. Stop is
	// idempotent, so the ordered happy-path Stop() below still runs first and this
	// is then a no-op. Registered AFTER `defer pool.Close()`, so LIFO runs it
	// BEFORE pool.Close — the conn is freed before puddle waits on it.
	defer reporter.Stop()

	// TopologyWatcher: the SINGLE reactor to a cluster-topology change (one LISTEN on
	// cluster_changed + debounce + failsafe poll). It drives the Resharder (recompute
	// this pod's shard tile) — the mesh's own peer-route reaction runs under the broker
	// duty but shares the SAME liveness window (3× the heartbeat cadence), so shard
	// tiling and mesh routing agree on who is live. Must start AFTER the reporter has
	// registered the member row (so assign_member_shards can rank this member). The
	// Resharder's initial assign is synchronous (run in Start), so by the time Start
	// returns the ShardSet already holds this member's computed range and the drivers
	// (already running, currently owning nothing) pick it up on their next tick. The
	// resharder's onChange nudges the reporter to re-beat immediately so the cluster
	// view's `shards` column reflects each reshard without waiting out the interval.
	// Sharding is always dynamic: every pod owns a contiguous tile assigned from live
	// cluster_members and re-tiled on join/leave.
	resharder := runtime.NewResharder(store.New(pool), memberID, shardSet)
	resharder.LivenessWindow = 3 * cfg.MemberHeartbeatEvery // same window the mesh uses; ≫ beat cadence, < GC TTL
	topology := runtime.NewTopologyWatcher(runtime.NewPgxListener(pool))
	topology.LivenessWindow = 3 * cfg.MemberHeartbeatEvery
	topology.Interval = cfg.MemberHeartbeatEvery // failsafe re-run matches the beat
	topology.Register("resharder", resharder.Reactor(), reporter.Trigger)
	topology.Start(ctx)
	// Same early-return backstop as the reporter: the watcher holds a LISTEN conn
	// (cluster_changed) for its whole run, so a boot error after this point would
	// otherwise wedge the deferred pool.Close() on that checked-out conn. Idempotent +
	// LIFO-before-pool.Close, like reporter above.
	defer topology.Stop()
	slog.Info("dynamic sharding enabled", "member", memberID, "role", cfg.Role)

	// The main API server runs on a control pod (it serves the operator UI +
	// write paths alongside the sweepers). A pure worker/dispatch pod gets only
	// the tiny health-probe listener below.
	//
	// apiServesPlaintextProbes tracks whether the main API listener is a plaintext
	// target the kubelet can probe directly. It is FALSE when there's no API server
	// (worker/dispatch or NO_HTTP pod) OR when the API serves TLS — because under
	// mTLS the API listener REQUIRES a client cert, which a kubelet httpGet probe
	// does not present, so the handshake (and thus /livez /readyz) would fail. In
	// both cases we start the separate plaintext health listener below so probes
	// have a cleartext target; the API stays TLS/mTLS on its own port.
	var httpServer *http.Server
	apiServesPlaintextProbes := false
	if !cfg.NoHTTP && role.runControl {
		lc, err := parseListenAddr(cfg.ListenAddr, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile)
		if err != nil {
			return fmt.Errorf("LISTEN_ADDR: %w", err)
		}
		// A plaintext API listener is itself a probe-able target; a TLS one is not
		// (see apiServesPlaintextProbes above) — so a plaintext API suppresses the
		// separate health listener, a TLS API does not.
		apiServesPlaintextProbes = !lc.tls
		srv := api.NewServer(api.DepsFromPools(pool, readPool))
		srv.SetBuildVersion(BuildVersion)
		// The kind schema surface (/docs + /api/kinds + the spec/config write-path
		// validation) is driven by the DB-backed kind_manifest — CRDs are applied via
		// the fixture bootstrap above (control pod) or PUT /api/kinds/{kind}/manifest,
		// and the API serves + applies them. We load the FULL manifest set
		// from the DB (authoritative — includes manifests other pods seeded, so a
		// control pod that Setups nothing still serves + validates every cluster kind),
		// split it into work vs reactor kinds (KindManifest.IsReactor), and feed both
		// the API schema cache and the store-wide spec validator. The same closure is
		// registered as SetOnManifestApply, so applying a manifest at runtime rebuilds
		// the surface with no restart.
		rebuildKindSchemaSurface := func() {
			all, err := manifestStore.ListKindManifests(ctx)
			if err != nil {
				slog.Warn("rebuild API schema surface: list kind manifests failed; keeping prior surface", "err", err)
				return
			}
			var workManifests, reactorManifests []model.KindManifest
			for _, m := range all {
				if m.IsReactor() {
					reactorManifests = append(reactorManifests, m)
				} else {
					workManifests = append(workManifests, m)
				}
			}
			srv.SetDeclaredSchemas(workManifests) // creatable-resource gate (work kinds)
			srv.SetReactorSchemas(reactorManifests)
			rebuildValidator() // spec/config write validation, all kinds (shared with boot)
			// Re-serve a fresh OpenAPI surface so /docs + /openapi reflect the new
			// kind's schemas live (huma caches the spec per API instance, so this
			// swaps in a new instance). No-op until Handler() is called below.
			srv.RebuildHandler()
		}
		rebuildKindSchemaSurface()
		// SetOnManifestApply fires rebuildKindSchemaSurface only on the pod that HANDLED
		// the PUT. In a multi-pod control plane, every OTHER API pod must also learn a
		// manifest changed — else its schema/validator surface stays stale until
		// restart, spuriously rejecting a resource that uses a newly-added field. Give
		// it the same discipline the KindManifestCache uses: a kind_manifest_changed LISTEN
		// (prompt on any pod's apply) + a failsafe re-read (a dropped NOTIFY still
		// converges). The refresher runs the SAME rebuild closure; SetOnManifestApply
		// stays for the zero-latency local path.
		kindSchemaRefresher := runtime.NewNotifyRefresher(runtime.NewPgxListener(pool),
			"apischema kind_manifest_changed", runtime.KindManifestChannel,
			runtime.ManifestReloadInterval, func(context.Context) { rebuildKindSchemaSurface() })
		kindSchemaRefresher.Start(ctx)
		defer kindSchemaRefresher.Stop()
		srv.SetOnManifestApply(rebuildKindSchemaSurface)
		// CORS is off by default (same-origin only — the SPA is served here). An
		// explicit allowlist (or "*" for dev) opts cross-origin browsers in.
		srv.SetCORSAllowedOrigins(cfg.CORSAllowedOrigins)
		// API rate limiting is off by default (RPS 0); a positive RPS enables a
		// per-client-IP token bucket on the API operations. Close() stops its
		// janitor on shutdown.
		srv.SetRateLimit(cfg.APIRateLimitRPS, cfg.APIRateLimitBurst, cfg.APIRateLimitTrustProxy)
		defer srv.Close()
		httpServer = &http.Server{
			Addr:    lc.addr,
			Handler: srv.Handler(),
			// Bound the connection lifecycle so a slow or idle client can't pin a
			// goroutine + FD indefinitely (Slowloris). ReadHeaderTimeout caps the
			// header read specifically (Huma's BodyReadTimeout only covers the body).
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      120 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    1 << 16, // 64 KiB
		}
		// Install the hot-reloading TLS/mTLS/SPIFFE material (shared with the broker
		// listener via configureServerTLS) — a no-op on a plain-HTTP listener.
		if _, err := configureServerTLS(ctx, httpServer, lc, apiAuthz, cfg.TLSReloadInterval, "api"); err != nil {
			return err
		}
		go func() {
			slog.Info("api server listening",
				"addr", lc.addr, "tls", lc.tls, "mtls", lc.clientCAFile != "")
			var serveErr error
			if lc.tls {
				// Empty cert/key paths: the keypair comes from TLSConfig.GetCertificate
				// (the reloader), not from files read once here.
				serveErr = httpServer.ListenAndServeTLS("", "")
			} else {
				serveErr = httpServer.ListenAndServe()
			}
			if serveErr != nil && serveErr != http.ErrServerClosed {
				slog.Error("http server error", "error", serveErr)
			}
		}()
	}

	// Start the plaintext HealthAddr listener (:8081) when EITHER:
	//   - the main API listener isn't itself a plaintext probe target — worker-only /
	//     NO_HTTP pods (no API server) and TLS control pods (a kubelet httpGet can't
	//     complete the mTLS handshake against the API port); OR
	//   - metrics are enabled — /metrics MUST live on this plaintext port so a
	//     Prometheus scraper never needs a client cert, even on a plaintext control/all
	//     pod that already serves probes on its API mux.
	// It hosts /livez /healthz /readyz and (when enabled) /metrics. Distinct from the
	// API port (:8080), so the two coexist. When the API is plaintext AND metrics are
	// off, we skip it to keep the single-probe-listener invariant.
	var healthServer *http.Server
	if (!apiServesPlaintextProbes || metricsProvider.Enabled()) && cfg.HealthAddr != "" {
		var metricsHandler http.Handler
		if metricsProvider.Enabled() {
			metricsHandler = metricsProvider.Handler()
		}
		healthServer = &http.Server{
			Addr:              cfg.HealthAddr,
			Handler:           health.Handler(pool, metricsHandler),
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    1 << 16,
		}
		go func() {
			slog.Info("health probe listener listening", "addr", cfg.HealthAddr, "metrics", metricsProvider.Enabled())
			if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("health server error", "error", err)
			}
		}()
	}

	// Broker pods serve the WorkerService Connect API that dumb workers connect to
	// (plus the internal MeshService for broker↔broker routing).
	// It mounts on its OWN listener (BROKER_ADDR_LISTEN), separate from the API,
	// reusing the same hot-reloading TLS/mTLS/SPIFFE scaffold — so a worker
	// presents its client cert to pull work. The handler comes from the claim
	// duty, available after eng.Start built its server.
	//
	// brokerMaxConcurrentStreams bounds HTTP/2 streams per client connection (see the
	// HTTP2Config below). Legitimate clients use 1-2; the ceiling is a transport-tier
	// abuse backstop, well above any real multiplexing need.
	const brokerMaxConcurrentStreams = 256
	var brokerServer *http.Server
	if role.runBroker && cfg.BrokerAddr != "" {
		path, handler, ok := eng.ClaimHandler()
		if !ok {
			return fmt.Errorf("ROLE is broker but the engine built no claim duty (no dispatch kinds?)")
		}
		lc, err := parseListenAddr(cfg.BrokerAddr, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSClientCAFile)
		if err != nil {
			return fmt.Errorf("BROKER_ADDR_LISTEN: %w", err)
		}
		mux := http.NewServeMux()
		mux.Handle(path, handler)
		brokerServer = &http.Server{
			Addr:              lc.addr,
			ReadHeaderTimeout: 10 * time.Second,
			// No WriteTimeout: WorkStream AND the broker↔broker Route stream are long-lived
			// server streams that hold the response open indefinitely; a write deadline
			// would kill them.
			IdleTimeout:    120 * time.Second,
			MaxHeaderBytes: 1 << 16,
			// Cap concurrent HTTP/2 streams PER CONNECTION. A well-behaved client uses
			// only 1-2 streams on its connection (a worker: one long-lived WorkStream; a
			// peer broker: one Route) — the remaining unary call (GetProviderConfig) is
			// short.
			// The generous ceiling bounds a single misbehaving/compromised client (each
			// WorkStream stream costs a 64-buffered channel + one fan-in goroutine per
			// advertised kind, and each Route stream a receive loop) without constraining
			// legitimate multiplexing. Admission is still primarily mTLS/SPIFFE; this is a
			// transport-tier backstop. Applies on both the TLS (ALPN h2) and h2c paths.
			HTTP2: &http.HTTP2Config{MaxConcurrentStreams: brokerMaxConcurrentStreams},
		}
		// Same hot-reloading TLS/mTLS/SPIFFE wiring as the API listener, via the
		// shared configureServerTLS — so the listeners can't drift. This ONE listener
		// serves both the WorkerService and the MeshService, so the handshake admits the
		// UNION of their allowlists (any legitimate worker OR peer broker); the broker's
		// per-service Connect interceptors (wired via EngineConfig → NewDispatch) then
		// narrow each RPC to its own audience so a worker cert can't reach a mesh RPC.
		serveTLS, err := configureServerTLS(ctx, brokerServer, lc, spiffeauthz.Union(workerAuthz, meshAuthz), cfg.TLSReloadInterval, "broker")
		if err != nil {
			return err
		}
		// MESH: the broker↔broker Route RPC is bidi-streaming, which needs end-to-end
		// HTTP/2. The TLS path negotiates "h2" via ALPN from the server's TLS config.
		// The PLAIN path would otherwise speak only HTTP/1.1 and break bidi — so flip
		// the server to also accept cleartext HTTP/2 (h2c) via the stdlib Protocols.
		// Both keep the unary RPCs working over the same server.
		brokerServer.Handler = mux
		if !serveTLS {
			configureBrokerH2C(brokerServer)
		}
		// Fail fast if the mesh transport can't negotiate HTTP/2 to our own listener
		// (a silent HTTP/1.1 downgrade would break Route at runtime). Best-effort:
		// tolerates a not-yet-listening self address (boot race), fails only on a
		// definitive non-H2 negotiation. Runs after the listener goroutine starts.
		if hc, ok := relayClient.(*http.Client); ok {
			go func() {
				// small delay so the listener is accepting before the self-probe
				select {
				case <-time.After(2 * time.Second):
				case <-ctx.Done():
					return
				}
				if err := assertRouteH2(ctx, hc, cfg.RelayAdvertiseAddr); err != nil {
					slog.Error("broker mesh HTTP/2 assertion FAILED — forwarding will not work", "error", err)
				}
			}()
		}
		go func() {
			slog.Info("broker Connect listening", "addr", lc.addr, "tls", lc.tls, "mtls", lc.clientCAFile != "")
			var serveErr error
			if lc.tls {
				serveErr = brokerServer.ListenAndServeTLS("", "")
			} else {
				serveErr = brokerServer.ListenAndServe()
			}
			if serveErr != nil && serveErr != http.ErrServerClosed {
				slog.Error("broker server error", "error", serveErr)
			}
		}()
	}

	slog.Info("converge running", "role", cfg.Role)
	<-ctx.Done()
	slog.Info("shutting down")

	// Flip /readyz to NotReady FIRST so k8s pulls this pod from its Service
	// endpoints (the API ClusterIP and the broker Connect Service) before we drain —
	// no new API request or WorkStream connection is routed to a pod that's tearing
	// down. Liveness stays 200 so the kubelet doesn't SIGKILL the draining process.
	// The probe listener is shut down later, after the drain.
	health.SetDraining()

	// Stop heartbeating FIRST so no in-flight beat can re-UPSERT the row after
	// the deregister below wins the race. The loop's context is already
	// cancelled; this just joins the goroutine and releases its pool use.
	reporter.Stop()

	// Stop the TopologyWatcher (joins its loop + reactors + releases its held LISTEN
	// conn) before the pool closes. It only reads/listens — no row to clean up — so
	// order relative to deregister doesn't matter, but it must finish before
	// pool.Close (deferred).
	if topology != nil {
		topology.Stop()
	}

	// Stop the API server first (it serves no work-completion path — only the
	// operator UI/API — so closing it early is safe and stops new operator writes).
	if httpServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("http shutdown error", "error", err)
		}
	}

	// DRAIN BEFORE closing the broker Connect server. eng.Stop drives each duty's
	// TWO-PHASE shutdown — for the claim duty: stop claiming, then DRAIN in-flight
	// fanout, letting already-dispatched tasks' worker Completes LAND (instead of
	// abandoning them), bounded by ShutdownDrain. This MUST run while the broker
	// Connect server is still up, because Complete is delivered over it — closing the
	// server first would strand every in-flight result. Sized below the k8s
	// terminationGracePeriod (60s) so the process exits before SIGKILL and a node
	// move drains in-flight remote work rather than re-running it. The control
	// duty's sweepers are idempotent/resumable so they stop promptly within it.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), cfg.ShutdownDrain)
	_ = eng.Stop(stopCtx)
	stopCancel()

	// NOW close the broker Connect server: in-flight work has drained (or hit the
	// budget), so no Complete is still expected. Shutdown also lets any open
	// WorkStream stream finish gracefully.
	if brokerServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := brokerServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("broker shutdown error", "error", err)
		}
	}
	if healthServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := healthServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("health shutdown error", "error", err)
		}
	}

	// Release this pod's in-flight work claims so a surviving pod re-claims the
	// work IMMEDIATELY, instead of it sitting worker_id-set until the reaper's
	// stale window elapses (the multi-minute stranding a rollout otherwise
	// causes — the dying pod stops heartbeating the moment its dispatcher
	// stops). Safe HERE and only here: eng.Stop above joined the dispatcher's
	// Run (wg.Wait), so it is fully quiesced — no goroutine can re-claim or
	// re-heartbeat after the release. A hard crash (no SIGTERM) still falls back
	// to the reaper's now-30s window. Bounded by a SHORT deadline on a FRESH
	// context (the signal ctx is already cancelled) so a DB blip can't hang a
	// shutting-down pod — we'd rather exit and let the reaper reclaim. The pool
	// is still open here (its Close is deferred and runs after we return).
	//
	// ORDERING INVARIANT: eng.Stop drained for ShutdownDrain (default 45s), which
	// MUST exceed a connected worker's MAX cumulative Complete-retry latency
	// (sdk-go/converge worker runner: 8 attempts, ≤2s capped backoff ≈ 5s, plus RPC time). While that
	// holds, a worker mid-retry during a clean shutdown lands its Complete on the
	// parked dispatchStage BEFORE this release NULLs the row — so no surviving broker
	// re-claims a task the original worker is still finishing (no double-execution).
	// If the drain genuinely timed out (task stuck the full window), releasing IS
	// correct — the task is unhealthy and should re-dispatch. Any late Complete after
	// release is fenced out by AppendOutbox (worker_id no longer matches) → no
	// double-apply. Keep SHUTDOWN_DRAIN comfortably above the worker retry budget.
	relCtx, relCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if freed, err := eng.ReleaseClaims(relCtx); err != nil {
		slog.Warn("release in-flight work claims on shutdown", "member_id", memberID, "err", err)
	} else if freed > 0 {
		slog.Info("released in-flight work claims on shutdown", "member_id", memberID, "freed", freed)
	}
	relCancel()

	// Deregister LAST: delete this member's cluster_members row so the cluster
	// view drops it immediately instead of waiting out the GC TTL. Bounded by a
	// short deadline on a FRESH context (the signal ctx is already cancelled) so
	// a DB/network blip can't hang a shutting-down member — we'd rather exit and
	// let the ClusterMemberGC reclaim the row than block on the network. The
	// pool is still open here (its Close is deferred and runs after we return).
	deregCtx, deregCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := reporter.Deregister(deregCtx); err != nil {
		slog.Warn("cluster member deregister", "member_id", reporter.Info.MemberID, "err", err)
	}
	deregCancel()
	return nil
}
