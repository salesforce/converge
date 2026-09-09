package converge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"

	"github.com/salesforce/converge/sdk-go/workerpb"
)

// loadTimeout bounds a single GetProviderConfig RPC so a hung-but-connected broker (TCP
// up, never answers) can't wedge the worker's boot retry loop or its periodic refresh
// forever. A timeout surfaces as a returned error the boot loop retries with backoff and
// the refresh logs while keeping last-known docs; it never severs the long-lived
// WorkStream (that's a separate per-call connection), so we bound the unary call here
// rather than on the HTTP client.
const loadTimeout = 30 * time.Second

// configCache is the dumb worker's owner of every (kind, kindVersion)'s DEFAULT
// providerconfig — both the config document (spec) AND the opaque bundle (data). A worker
// has NO database, so it PULLS the defaults from the broker over Connect
// (GetProviderConfig) at boot and refreshes them periodically, and applies the broker's
// live PUSHes on the WorkStream. On any change (a boot prime, a refresh that moved, a
// push) it invokes onChange, which the SDK wires to fire the provider's OnConfig — so a
// provider always holds its current default (delivered at startup and on every edit), and
// computes the effective per-task config (default ⊕ per-resource override) itself in Work
// via EffectiveConfig / EffectiveBundle.
//
// Keyed strictly by (kind, kindVersion): defaults are per-kindVersion, so a v2 worker's
// fetch/push/onChange never mixes v1's document in.
type configCache struct {
	client  transport
	refresh time.Duration
	// onChange fires whenever a pair's stored default changes (boot prime, refresh, or
	// push), with the pair's CURRENT full {spec, data}. Set once at construction; nil is a
	// no-op (a test/config-less cache). The SDK wires it to the provider's OnConfig.
	onChange func(kind Kind, kindVersion int, spec json.RawMessage, data []byte)

	mu sync.RWMutex
	// pairs is the (kind, kindVersion) set Load fetches; set at construction and replaced
	// once by setPairs (post-registration, when the kindRuntimes reveal real kindVersions),
	// so guarded by mu.
	pairs   []KindVersion
	docs    map[KindVersion]json.RawMessage
	bundles map[KindVersion][]byte
}

// newConfigCache builds a cache that will fetch the given (kind, kindVersion) pairs'
// defaults from the broker. refresh ≤ 0 → defaultConfigRefresh. onChange (nil = no-op)
// fires whenever a pair's stored default changes — the SDK wires it to the provider's
// OnConfig. Call Load once at boot (which fires onChange for the primed defaults), then
// run for the periodic refresh. Each pair carries its EXPLICIT kindVersion as-is (no
// normalization); a pair at (kind, 0) stays at 0 and fetches nothing, surfacing an
// unversioned worker as broken.
func newConfigCache(client transport, pairs []KindVersion, refresh time.Duration, onChange func(kind Kind, kindVersion int, spec json.RawMessage, data []byte)) *configCache {
	if refresh <= 0 {
		refresh = defaultConfigRefresh
	}
	// Each pair carries its EXPLICIT web-API version (>= 1, advertised by the worker
	// from its kindRuntime.KindVersion). No normalization — a pair at (kind, 0) would
	// fetch (kind, 0)'s default (nothing), surfacing an unversioned worker as broken.
	norm := make([]KindVersion, len(pairs))
	copy(norm, pairs)
	return &configCache{
		client:   client,
		pairs:    norm,
		refresh:  refresh,
		onChange: onChange,
		docs:     map[KindVersion]json.RawMessage{},
		bundles:  map[KindVersion][]byte{},
	}
}

// setPairs replaces the (kind, kindVersion) set the cache fetches. The host calls it once
// after registration, when the kindRuntimes reveal each kind's REAL kindVersion (the boot
// load could only fetch the version-blind boot pairs). It does NOT re-fetch — the caller's
// next load / run refresh / onConnect re-pull picks up any newly-tracked kindVersion. Safe
// to call before run (setPairs runs on the boot goroutine before the stream opens).
// Idempotent.
func (c *configCache) setPairs(pairs []KindVersion) {
	norm := make([]KindVersion, len(pairs))
	copy(norm, pairs)
	c.mu.Lock()
	c.pairs = norm
	c.mu.Unlock()
}

// load fetches every tracked (kind, kindVersion)'s current default config from the broker
// and merges it into the cache. Call once at boot (before the first task) so a
// config-bootstrapped provider can dial its BOOTSTRAP client; a failure here is returned
// so the worker can retry rather than run against empty config.
func (c *configCache) load(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, loadTimeout)
	defer cancel()
	// Snapshot the tracked pair set under the lock (setPairs may swap it post-registration),
	// then ask per (kind, kindVersion): parallel kinds + kind_versions arrays (mirrors
	// Subscribe), so the broker serves each worker its OWN kindVersion's default.
	c.mu.RLock()
	pairs := c.pairs
	c.mu.RUnlock()
	kinds := make([]string, len(pairs))
	kindVersions := make([]int32, len(pairs))
	for i, p := range pairs {
		kinds[i] = string(p.Kind)
		kindVersions[i] = int32(p.Version)
	}
	resp, err := c.client.GetProviderConfig(ctx, connect.NewRequest(&workerpb.GetProviderConfigRequest{
		Kinds: kinds, KindVersions: kindVersions,
	}))
	if err != nil {
		return fmt.Errorf("converge: GetProviderConfig: %w", err)
	}
	// MERGE, don't replace: overwrite each (kind, kindVersion) the broker returned with its
	// latest config/bundle, but KEEP the last-known value for a pair the response OMITS.
	// A successful-but-empty answer is a real hazard — a broker whose ProviderConfigCache is
	// still cold on (re)connect, or a GetProviderConfig that wins the race against the
	// default landing — and a blanket replace would BLANK a bundle the worker already
	// holds, re-triggering the exact "no bundle yet" this cache exists to prevent (the
	// 5-min refresh is the sole backstop and must never make things worse). Overwriting
	// with the latest present value is still "load the latest"; only ABSENT pairs are
	// preserved. A genuine default DELETION arrives as an explicit push (apply with empty
	// → delete), not as an omission here.
	//
	// Track the pairs whose stored value actually MOVED this load, so onChange (→ the
	// provider's OnConfig) fires only on a real change — a steady 5-min refresh that
	// re-fetches identical bytes must not re-fire OnConfig every tick.
	var changed []KindVersion
	c.mu.Lock()
	for _, e := range resp.Msg.GetEntries() {
		key := KindVersion{Kind: Kind(e.GetKind()), Version: int(e.GetKindVersion())}
		moved := false
		if doc := e.GetConfig(); len(doc) > 0 && !bytes.Equal(doc, c.docs[key]) {
			c.docs[key] = doc
			moved = true
		}
		if b := e.GetBundle(); len(b) > 0 && !bytes.Equal(b, c.bundles[key]) {
			c.bundles[key] = b
			moved = true
		}
		if moved {
			changed = append(changed, key)
		}
	}
	ups := c.snapshotLocked(changed) // capture {spec,data} per changed pair under the lock
	c.mu.Unlock()
	c.fire(ups) // fire onChange (→ provider OnConfig) after unlock; only on a real change
	return nil
}

// apply installs a (kind, kindVersion)'s default providerconfig PUSHED by the broker (a
// ProviderConfigUpdate on the WorkStream — spec + bundle together, the whole monolith)
// into the live cache, so the next task observes an operator's edit immediately instead of
// on the next run tick, and fires onChange (→ the provider's OnConfig) with the new
// {spec, data}. An empty spec+bundle clears the pair (the default was deleted); onChange
// still fires (with nil, nil) so the provider observes the removal.
func (c *configCache) apply(kind Kind, kindVersion int, spec json.RawMessage, data []byte) {
	key := KindVersion{Kind: kind, Version: kindVersion}
	c.mu.Lock()
	if len(spec) == 0 {
		delete(c.docs, key)
	} else {
		c.docs[key] = spec
	}
	if len(data) == 0 {
		delete(c.bundles, key)
	} else {
		c.bundles[key] = data
	}
	ups := c.snapshotLocked([]KindVersion{key})
	c.mu.Unlock()
	c.fire(ups)
}

// changeUpdate is one pair's current {spec, data}, snapshotted for an onChange fire.
type changeUpdate struct {
	kv   KindVersion
	spec json.RawMessage
	data []byte
}

// snapshotLocked captures the CURRENT stored {spec, data} for each changed pair. The
// caller holds c.mu; it returns the snapshot so the caller can fire() AFTER unlocking,
// keeping the provider's OnConfig (which may take its own lock) off the held cache lock.
func (c *configCache) snapshotLocked(changed []KindVersion) []changeUpdate {
	if c.onChange == nil || len(changed) == 0 {
		return nil
	}
	ups := make([]changeUpdate, len(changed))
	for i, kv := range changed {
		ups[i] = changeUpdate{kv: kv, spec: c.docs[kv], data: c.bundles[kv]}
	}
	return ups
}

// fire invokes onChange for each snapshotted update, in order — called with c.mu NOT held.
func (c *configCache) fire(ups []changeUpdate) {
	for _, u := range ups {
		c.onChange(u.kv.Kind, u.kv.Version, u.spec, u.data)
	}
}

// run refreshes the cache immediately, then every refresh interval, until ctx is
// cancelled. A failed refresh keeps the last-known docs (a transient broker blip never
// wipes a worker's config — load also merges, see there) and is reported to onErr (nil →
// ignored).
//
// The IMMEDIATE first refresh (before the first ticker tick) is a resilience edge: the
// boot load runs once before the first task, but on a later broker RESTART the ProviderConfigCache
// that answered that boot load may have been cold, and after a worker RECONNECT the broker
// only PUSHES future changes — so without an eager re-load a config/bundle that changed
// during the gap would strand until the first +interval tick (default 5 min), long enough
// for a config/bundle-bootstrapped provider to exhaust its transient retries and
// terminal-fail. Refreshing at once on run start closes that window cheaply.
func (c *configCache) run(ctx context.Context, onErr func(error)) {
	load := func() {
		if err := c.load(ctx); err != nil && onErr != nil && ctx.Err() == nil {
			onErr(err)
		}
	}
	load() // eager: converge to current config now, don't wait a full interval
	t := time.NewTicker(c.refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			load()
		}
	}
}

// Get returns a (kind, kindVersion)'s CURRENT stored default {spec, data} (nil if none).
// Live — a later call after a refresh/push reflects an operator edit. It is the READ side
// of the cache; the PUSH delivery path a provider normally relies on is onChange → the
// provider's OnConfig (a provider keeps its own copy of the default from OnConfig and
// merges the per-resource override in Work via EffectiveConfig, so it rarely needs Get).
// Exported so the SDK's own tests can inspect the cache; it is a method on the unexported
// configCache, so it is not reachable from outside the package.
func (c *configCache) Get(kind Kind, kindVersion int) (spec json.RawMessage, data []byte) {
	key := KindVersion{Kind: kind, Version: kindVersion}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.docs[key], c.bundles[key]
}
