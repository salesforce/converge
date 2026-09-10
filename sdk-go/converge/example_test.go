package converge_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"

	"github.com/salesforce/converge/sdk-go/converge"
)

// dbProvider is a minimal converge.Provider: one (kind, version), a Work that reconciles
// a resource's spec into status, no config, always ready. It is the shape every worker's
// business logic takes — the SDK calls only these four methods.
type dbProvider struct{}

func (dbProvider) Kind() converge.KindVersion {
	// The ONE pair this provider serves. Explicit version (>= 1); there is no implicit v1.
	return converge.KindVersion{Kind: converge.Kind("database"), Version: 1}
}

func (dbProvider) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	// Work is pure decision logic — it never touches a database. It reads the resource's
	// spec, decides the desired state, and returns an Outcome the core applies.
	var spec struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
		// A malformed spec can't be fixed by a retry → terminal (a plain error would retry).
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("decode spec: %w", err))
	}
	status, err := json.Marshal(map[string]string{"endpoint": spec.Name + ".db.internal:5432"})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: status}, nil
}

func (dbProvider) OnConfig(converge.ProviderConfig) {}              // no default providerconfig
func (dbProvider) Ready() bool                      { return true } // no downstream to dial

// cacheProvider is a second provider (kind "cache") in the same worker — a worker serves
// as many providers as it lists. Its Ready dials lazily: false until connected, so the
// broker sends it no work until it's up, then flips true (RS+). This is the whole
// bring-up contract — the SDK never calls a setup step.
type cacheProvider struct {
	mu     sync.RWMutex
	dialed bool // real code: guarded live state a background dial mutates
}

func (*cacheProvider) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind("cache"), Version: 1}
}
func (*cacheProvider) Work(context.Context, converge.ReactionRequest) (converge.Outcome, error) {
	return converge.Outcome{}, nil
}
func (*cacheProvider) OnConfig(converge.ProviderConfig) {}
func (c *cacheProvider) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.dialed // false until the downstream is up
}

// Example is the whole worker API: declare the providers, install a signal ctx, and call
// Serve. Serve reads config from the environment (BROKER_ADDR / TLS_* / WORKER_*), dials
// the broker, serves work, reconnects across drops, and drains on shutdown. The caller
// owns only the context and the exit decision.
func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := converge.Serve(ctx, []converge.Provider{dbProvider{}}); err != nil {
		log.Fatal(err)
	}
}

// Example_multipleProviders shows a worker serving several (kind, version) pairs: list one
// provider per pair. The SDK fans out, routing each claimed task to the matching provider.
// Two versions of one kind would likewise be two providers, each with its own Work.
func Example_multipleProviders() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err := converge.Serve(ctx, []converge.Provider{
		dbProvider{},
		&cacheProvider{}, // pointer: it holds live readiness state
	})
	if err != nil {
		log.Fatal(err)
	}
}

// configProvider reads a providerconfig. The SDK delivers the kind DEFAULT to OnConfig —
// once at startup with the primed default, then on every edit; the EFFECTIVE config a task
// runs on is default ⊕ that resource's per-resource override, resolved in Work.
type configProvider struct {
	mu  sync.RWMutex
	def converge.ProviderConfig // the kind default, kept current by OnConfig
}

type dbConfig struct {
	Endpoint string `json:"endpoint"`
	PoolSize int    `json:"pool_size"`
}

func (p *configProvider) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: "database", Version: 1}
}

// OnConfig stores the latest DEFAULT providerconfig (spec + bundle). It fires once at
// startup with the primed default, then on every operator edit — the place to react (e.g.
// redial). It carries no per-resource override, and it is not where the merge happens.
func (p *configProvider) OnConfig(cfg converge.ProviderConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.def = cfg
}

func (p *configProvider) Ready() bool { return true }

// Work resolves the EFFECTIVE config: EffectiveConfig overlays this resource's per-resource
// override (req.Env.ProviderConfig) on the live kind default — Spec deep-merges (override
// wins per key), while the bundle (Data) is a whole-artifact replace via EffectiveBundle.
func (p *configProvider) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	p.mu.RLock()
	def := p.def
	p.mu.RUnlock()

	cfg := converge.EffectiveConfig[dbConfig](def.Spec, req.Env.ProviderConfig)
	bundle := converge.EffectiveBundle(def.Data, req.Env.ProviderBundle)

	status, err := json.Marshal(map[string]any{"endpoint": cfg.Endpoint, "pool": cfg.PoolSize, "bundle_bytes": len(bundle)})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: status}, nil
}

// Example_effectiveConfig shows a provider that consumes a providerconfig: OnConfig keeps
// the kind DEFAULT current (fired at startup + on every edit), and Work resolves the
// EFFECTIVE config (default ⊕ per-resource override) with converge.EffectiveConfig.
func Example_effectiveConfig() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := converge.Serve(ctx, []converge.Provider{&configProvider{}}); err != nil {
		log.Fatal(err)
	}
}
