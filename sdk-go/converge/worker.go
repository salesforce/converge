package converge

import (
	"log/slog"
	"sync"
	"time"
)

// Worker configuration defaults. These back the env-configured worker Serve builds.
const (
	// defaultMaxInflight bounds concurrent task execution when WORKER_MAX_PARALLEL is unset.
	defaultMaxInflight = 16
	// defaultDrainGrace is the graceful in-flight drain window on ctx cancel when
	// WORKER_DRAIN_TIMEOUT is unset: the worker lets running handlers finish to their
	// TaskDeadline for this long before abandoning them (keep it below the deployment's
	// termination grace so the process exits on its own).
	defaultDrainGrace = 50 * time.Second
	// defaultHealthPollInterval is how often the worker polls each provider's Ready. Kept
	// well under the broker's WorkStream keepalive so a readiness flip reaches the broker
	// within one keepalive window; a cheap, non-blocking probe at this cadence is negligible.
	defaultHealthPollInterval = 2 * time.Second
	// defaultConfigRefresh is the interval at which the worker's configCache re-pulls each
	// (kind, kindVersion)'s default providerconfig from the broker — the worker-side half of
	// the live-config PULL model (an operator's edit reaches this worker within one tick),
	// the failsafe behind the broker's live push. The in-cluster ProviderConfigCache uses a similar
	// cadence.
	defaultConfigRefresh = 5 * time.Minute
	// defaultTLSReloadInterval is how often the mTLS cert/key/CA files are re-read for
	// rotation (matches the server side).
	defaultTLSReloadInterval = 3 * time.Minute
	// defaultReconnectCeiling caps the reconnect backoff on a dropped stream. Modest so a
	// single-worker kind's claim-gate stall (the broker won't claim a kind with no ready
	// subscriber) stays bounded while the worker is flapping.
	defaultReconnectCeiling = 30 * time.Second
)

// worker is the internal converge worker: Serve builds one from the environment,
// registers the given providers, and runs it. It owns the WHOLE broker relationship —
// dialing (h2c or mTLS with cert hot-reload), reconnect-with-backoff, the config cache,
// and the readiness poller. It is NOT part of the public SDK surface: an author registers
// providers and calls converge.Serve; the worker is the machinery behind it.
type worker struct {
	cfg config

	// client is the broker transport. Built lazily on the first run from cfg.brokerAddr
	// + the TLS options (so construction neither dials nor can fail), unless a test
	// injected one via cfg.client. Guarded by runnerMu (set once, under the runner lock).
	client transport
	cache  *configCache

	// Registration surfaces, all populated before run and read-only during it. Guarded
	// by mu only so a late registration (rare — registration is a boot concern) can't
	// race run's snapshot; the hot path takes no lock.
	mu       sync.Mutex
	handlers map[KindVersion]map[string]ReactionHandler // (kind,ver) → reaction → handler
	onConfig map[KindVersion]func(cfg ProviderConfig)   // per-(kind,ver) providerconfig-push callback (spec+data)
	health   map[KindVersion]func() bool                // per-(kind,ver) readiness probe

	// runner is the live serve engine, set at the top of each stream session so
	// SendReadiness (the health poller) can reach the current stream's send mux. Guarded
	// by runnerMu.
	runnerMu sync.Mutex
	runner   *runner
}

// config is the resolved option set. Group by functionality, then alphabetical.
type config struct {
	// Broker connection.
	brokerAddr        string
	client            transport // test-injected transport; nil ⇒ run dials from brokerAddr
	tlsCertFile       string    // mTLS client cert (empty ⊕ tlsKeyFile empty ⇒ cleartext h2c)
	tlsKeyFile        string
	tlsCAFile         string // pins the broker's server CA (empty ⇒ system roots)
	tlsReloadInterval time.Duration

	// Serving knobs.
	configRefresh    time.Duration
	drainGrace       time.Duration
	healthPoll       time.Duration
	logger           *slog.Logger
	maxInflight      int
	reconnectCeiling time.Duration
}

// option overrides a resolved config field. Serve reads everything from the environment;
// the sole internal option is withTransport (transport.go), for the SDK's own tests to
// inject an in-process broker. Env is the operator-facing config path (BROKER_ADDR,
// TLS_*, WORKER_*) — there is no public option surface; the worker API is converge.Serve.
type option func(*config)

// newWorker constructs an unstarted worker: it reads configuration from the ENVIRONMENT
// for defaults (BROKER_ADDR, TLS_CERT_FILE/TLS_KEY_FILE/BROKER_CA_FILE,
// WORKER_MAX_PARALLEL, WORKER_DRAIN_TIMEOUT, TLS_RELOAD_INTERVAL — see env.go), then
// applies any internal options (highest precedence). It dials nothing yet — the
// connection is made (and reconnected) inside run. It returns an error ONLY when a SET
// environment variable is malformed (a bad WORKER_DRAIN_TIMEOUT duration, a non-integer
// WORKER_MAX_PARALLEL): bad config fails loud at construction rather than silently
// falling back to a default the operator didn't intend.
func newWorker(opts ...option) (*worker, error) {
	cfg, err := resolveConfig(opts)
	if err != nil {
		return nil, err
	}
	return &worker{
		cfg:      cfg,
		client:   cfg.client,
		handlers: map[KindVersion]map[string]ReactionHandler{},
		onConfig: map[KindVersion]func(ProviderConfig){},
		health:   map[KindVersion]func() bool{},
	}, nil
}

// resolveConfig layers configuration cloud-SDK style: the built-in default* consts (the
// single source of truth for every default), then the environment (applyEnv overlays
// only the SET variables), then any internal options (highest precedence), then fills a
// nil logger. It errors only when a SET environment variable is malformed.
func resolveConfig(opts []option) (config, error) {
	cfg := config{
		tlsReloadInterval: defaultTLSReloadInterval,
		configRefresh:     defaultConfigRefresh,
		drainGrace:        defaultDrainGrace,
		healthPoll:        defaultHealthPollInterval,
		maxInflight:       defaultMaxInflight,
		reconnectCeiling:  defaultReconnectCeiling,
	}
	if err := applyEnv(&cfg); err != nil { // environment overlays the built-in defaults
		return config{}, err
	}
	for _, o := range opts {
		o(&cfg) // options override the environment
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	return cfg, nil
}
