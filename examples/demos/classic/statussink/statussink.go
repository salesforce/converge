// Package statussink implements the REFERENCE reactor: it uploads a watched
// resource's rolled-up status to an object store when that resource crosses a
// subscribed lifecycle transition (typically 'synced' = "rolled up"). It is the
// reactive peer of fakeevent (a config-bootstrapped Worker) — same shape, same
// providerconfig two-tier split — but it is a REACT-ONLY kind whose KindRuntime
// carries only a TriggerReactor reaction + its handler, so it is driven by the
// ReactorDispatcher off lifecycle_outbox rather than the work dispatcher off
// work_queue.
//
// The statussink CRD only REGISTERS the reactor + its config schema — it names
// no kind or transition. Which resources it fires on is a separate, editable
// reactor_bindings SUBSCRIPTION:
//
//	{watch_kind:'classicbom', transition:'synced', reactor:'statussink'}
//
// It is the endpoint of the motivating saga: SQS → submit classicbom → WAIT
// until rolled up → upload the rolled-up status somewhere. The binding above
// makes this fire once each classicbom finishes rolling up; the broker ships
// STAGE_REACT to a worker advertising 'statussink', whose React uploads the
// ClassicBOMStatus to <endpoint>/<prefix>/<name>-<generation>.json (endpoint/prefix
// from statussink's DEFAULT providerconfig). Keying the object on generation
// makes an at-least-once redelivery an idempotent overwrite (no duplicate effect).
//
// Nothing real is touched — "dialing" validates the endpoint and "uploading"
// writes into an in-process store — so it exercises the whole reactor spine
// (binding match → claim → dispatch → ack, plus Setup-retry + live-reconfigure)
// end to end with no external infrastructure. Swap fakeStore for an *s3.Client
// to ship it for real.
package statussink

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Provider is the REFERENCE reactor as a converge.Provider (the ONE SDK contract) —
// and the reference for a provider that must DIAL a downstream to work. It does NOT
// dial at construction; instead it dials LAZILY and reports its state through Ready:
// Ready returns false until the object-store endpoint has arrived via OnConfig AND the
// dial succeeds, true after. So a statussink worker whose default providerconfig isn't
// applied yet simply advertises unready (the broker sends it no work) until the config
// lands — no crash, no strand — which is exactly the bring-up-retry the old Setup +
// ProviderRetrier synthesized, now written natively behind Ready.
type Provider struct {
	mu      sync.Mutex
	cfg     StatusSinkConfig // latest default providerconfig (from OnConfig)
	haveCfg bool
	store   *fakeStore // the dialed store; nil until a successful lazy dial
}

var _ converge.Provider = (*Provider)(nil)

// Kind is the single (kind, version) this reactor serves.
func (*Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// OnConfig stores the kind's default providerconfig (the endpoint + prefix); a change
// re-triggers the lazy dial on the next Ready poll (a new endpoint re-dials).
func (p *Provider) OnConfig(c converge.ProviderConfig) {
	var cfg StatusSinkConfig
	if len(c.Spec) > 0 {
		_ = json.Unmarshal(c.Spec, &cfg)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if cfg.Endpoint != p.cfg.Endpoint {
		p.store = nil // endpoint changed → drop the old dial, re-dial lazily
	}
	p.cfg, p.haveCfg = cfg, true
}

// Ready reports whether the store is dialed and usable — it attempts the lazy dial
// (idempotent, cached) and returns whether it succeeded. False until OnConfig has
// delivered a config with a valid endpoint; true once dialed. This is the whole
// bring-up contract: readiness, not a Setup the SDK orchestrates.
func (p *Provider) Ready() bool { return p.ensureStore() == nil }

// ensureStore dials the object store once from the current config, caching the client.
// Returns an error (leaving Ready false) until a config with a valid endpoint arrives
// and the dial succeeds. Concurrency-safe (Ready poll + Work both call it).
func (p *Provider) ensureStore() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store != nil {
		return nil
	}
	if !p.haveCfg {
		return fmt.Errorf("statussink: no default providerconfig yet (endpoint not applied)")
	}
	store, err := dialStore(p.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("statussink: %w", err)
	}
	p.store = store
	return nil
}

// Work runs the reactor's single 'react' reaction: it uploads the transitioned
// resource's rolled-up status to the dialed store. A binding fires it on a watched
// kind's transition; the transition + dedup token arrive on the request.
func (p *Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if err := p.ensureStore(); err != nil {
		return converge.Outcome{}, err // transient: retried; Ready has also gated dispatch
	}
	p.mu.Lock()
	store, def := p.store, p.cfg
	p.mu.Unlock()
	defaultSpec, _ := json.Marshal(def) // the kind default (endpoint + prefix) as bytes
	r := reactor{store: store, defaultSpec: defaultSpec}
	return r.React(ctx, req)
}

// ─────────────────────────────────────────────────────────────────────────
// Fake object store — the BOOTSTRAP "connection", dialed once and shared.
// ─────────────────────────────────────────────────────────────────────────

// fakeStore is an in-process stand-in for a real object store (S3 / GCS …).
// "Dialing" validates the endpoint; "uploading" records the object body keyed
// by its full key, so a test can assert what was uploaded. One instance per
// reactor at Setup (the BOOTSTRAP connection), shared by every React.
//
// putsByKey counts how many put calls each key received — the object-store PUT
// metric a real client would emit. It makes an at-least-once REDELIVERY observable
// (a key with count > 1 was delivered more than once) so a test can prove the effect
// stayed idempotent (one object per key) DESPITE redelivery, not merely that no
// redelivery happened. Cheap and always-on (it's just request accounting).
type fakeStore struct {
	endpoint  string
	mu        sync.Mutex
	objects   map[string][]byte
	putsByKey map[string]int
}

// dialStore is the fake "connect": it validates the store location and returns
// a ready client. A real reactor would build an *s3.Client here and could fail
// (bad endpoint, auth) — the boot-critical step that, on failure, keeps the
// kind pending until reconfigured.
func dialStore(endpoint string) (*fakeStore, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("no endpoint in providerconfig (BOOTSTRAP): apply the statussink default providerconfig with an endpoint")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid endpoint %q: want scheme://host[/path]", endpoint)
	}
	return &fakeStore{endpoint: endpoint, objects: map[string][]byte{}, putsByKey: map[string]int{}}, nil
}

// put records (or overwrites — idempotent) the object at key, counting the PUT.
// Honors cancellation so a slow upload stops on shutdown.
func (s *fakeStore) put(ctx context.Context, key string, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]byte(nil), body...)
	s.objects[key] = cp
	s.putsByKey[key]++
	return nil
}

// stats returns (distinct keys with an object, total put calls, max puts to any one
// key). distinct == the number of unique side effects; total > distinct means some
// deliveries redelivered (at-least-once); max is the worst-case redelivery count for
// a single key. Used to assert exactly-once EFFECT under at-least-once delivery.
func (s *fakeStore) stats() (distinct, totalPuts, maxPutsPerKey int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	distinct = len(s.objects)
	for _, n := range s.putsByKey {
		totalPuts += n
		if n > maxPutsPerKey {
			maxPutsPerKey = n
		}
	}
	return distinct, totalPuts, maxPutsPerKey
}

// Get returns a previously-uploaded object body and whether it exists. Exported
// for the integration test to assert the side effect happened.
func (s *fakeStore) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.objects[key]
	return b, ok
}

// ─────────────────────────────────────────────────────────────────────────
// Reactor
// ─────────────────────────────────────────────────────────────────────────

type reactor struct {
	store *fakeStore
	// defaultSpec is the kind DEFAULT config (endpoint + prefix) as bytes, from the
	// provider's latest OnConfig. Used to compute each delivery's effective config
	// (default ⊕ per-binding override).
	defaultSpec json.RawMessage
}

// React uploads the transitioned resource's status. Idempotent: the object key
// embeds the generation, so an at-least-once redelivery overwrites the same
// object rather than producing a duplicate.
func (r reactor) React(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	// Effective config = kind default ⊕ per-binding override (override wins per
	// field). The per-binding override now rides via Env.ProviderConfig (the sink
	// config travels on the delivery env). endpoint is BOOTSTRAP (dialed once at
	// Setup), so only the WORK-TIME prefix is taken from the effective config here.
	cfg := converge.EffectiveConfig[StatusSinkConfig](r.defaultSpec, req.Env.ProviderConfig)

	body := req.Resource.Status
	if len(body) == 0 {
		// A 'synced' resource with no status is legal (a leaf with no rollup);
		// upload an explicit empty object so the delivery is still observable.
		body = json.RawMessage(`{}`)
	}

	key := fmt.Sprintf("%s%s-%d.json", cfg.Prefix, req.Resource.Name, req.Generation)
	if err := r.store.put(ctx, key, body); err != nil {
		// Transient (cancellation / store error) → returned error leaves the
		// delivery unacked for at-least-once retry.
		return converge.Outcome{}, fmt.Errorf("statussink: put %s to %s: %w", key, r.store.endpoint, err)
	}

	req.Env.Logger.Info("uploaded rolled-up status",
		"endpoint", r.store.endpoint,
		"key", key,
		"transition", req.Transition,
		"dedup_token", req.DedupToken,
		"status_bytes", len(body),
	)
	return converge.Outcome{SideEffectDone: true}, nil
}
