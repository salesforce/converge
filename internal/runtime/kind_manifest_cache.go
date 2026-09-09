package runtime

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// OrphanGraceForKind returns a (kind, kindVersion)'s orphan-grace window from its
// manifest (0 if no manifest / no grace). Satisfies store.ComposePolicy so
// ApplyComposeResult can decide soft-vs-hard prune. nil-safe.
func (c *KindManifestCache) OrphanGraceForKind(kind model.Kind, kindVersion int) time.Duration {
	if c == nil {
		return 0
	}
	if m, ok := c.Get(context.Background(), kind, kindVersion); ok {
		return time.Duration(m.OrphanGraceSecs) * time.Second
	}
	return 0
}

// FinalizerForKind returns a (kind, kindVersion)'s finalizer from its manifest (empty if
// none). Satisfies store.ComposePolicy. nil-safe.
func (c *KindManifestCache) FinalizerForKind(kind model.Kind, kindVersion int) string {
	if c == nil {
		return ""
	}
	if m, ok := c.Get(context.Background(), kind, kindVersion); ok {
		return m.FinalizerName
	}
	return ""
}

// KindManifestChannel is the LISTEN/NOTIFY channel the kind_manifest sync
// trigger fires (payload = kind) on every operator apply. Every in-memory view
// of kind_manifest LISTENs on it to refresh promptly — the KindManifestCache
// (reactions/policy) here, and the API's schema surface (spec/config schemas) in
// cmd/converge; a failsafe re-read converges even if a NOTIFY is dropped.
// Exported so those other consumers reference the ONE channel name, not a
// duplicated literal.
const KindManifestChannel = "kind_manifest_changed"

// ManifestReloadInterval is the failsafe cadence for kind_manifest views
// (mirrors configReloadInterval). Exported for the API schema-surface refresher.
const ManifestReloadInterval = 60 * time.Second

// kindManifestRepo is the store surface the cache reads (kept narrow for testing).
type kindManifestRepo interface {
	ListKindManifests(ctx context.Context) ([]model.KindManifest, error)
	GetKindManifest(ctx context.Context, kind model.Kind, kindVersion int) (model.KindManifest, bool, error)
}

// kindManifestSnapshot is the IMMUTABLE published state of the cache: once stored in
// the atomic pointer it is never mutated, so readers index its maps with NO lock.
// An update (boot Load, NOTIFY/failsafe reload, or a miss-fill) builds a brand-new
// snapshot and atomically swaps it in — copy-on-write, never in-place edit. Keyed
// by (kind, kindVersion) so v1 and v2 of a kind resolve independently.
type kindManifestSnapshot struct {
	byKind map[model.KindVersion]model.KindManifest
	// checked records (kind, kindVersion) resolved against the DB: a key present with
	// value false is a CACHED absence (Get returns absent with no DB read); a key
	// NOT in the map has never been checked → Get does a one-shot miss-fill read.
	checked map[model.KindVersion]bool
}

// KindManifestCache holds every kind's FULL model.KindManifest in memory, keyed by
// (kind, kind_version), so the DISPATCH + COMPOSE paths resolve a manifest with a
// LOCK-FREE read (an atomic pointer load + map index), not a per-task mutex or DB
// round-trip. Consumers pull the SLICE they need out of the returned manifest:
//   - the dispatch loop reads its REACTIONS (which reaction to run for a task);
//   - the composer/reaper read its POLICY (OrphanGraceForKind / FinalizerForKind).
//
// It is NOT the API's schema-validation view: a resource apply is validated against
// the schemas by a SEPARATE surface on the API pod (cmd/converge's
// rebuildKindSchemaSurface), which reads the same kind_manifest rows for their
// spec/config JSON Schemas. Two independent views of one table, each for its own
// consumer; both stay current via the same KindManifestChannel.
//
// Refresh (multi-pod convergent — same discipline as ProviderConfigCache):
//   - loaded fully at boot (Load) so the first reconcile already has its kind;
//   - refreshed on a kind_manifest_changed NOTIFY (prompt) AND a 60s failsafe
//     re-read (a dropped NOTIFY still converges) — the shared NotifyRefresher;
//   - DB-read-on-MISS (Get falls back to a single GetKindManifest), so a brand-
//     new kind applied between refreshes is never silently treated as absent.
//
// The published snapshot is immutable; every change swaps a fresh one in via
// atomic.Pointer. Manifests change only on control-plane events (a CRD apply), so
// the copy-on-write cost is rare while the per-task read is contention-free — the
// hot path (kind already resolved) takes no lock and does no DB read.
type KindManifestCache struct {
	repo      kindManifestRepo
	refresher *NotifyRefresher // kind_manifest_changed LISTEN + failsafe reload

	snap atomic.Pointer[kindManifestSnapshot]
	// fillMu serializes the rare copy-on-write paths (miss-fill, Load, reload) so
	// two concurrent updates can't clobber each other's snapshot. The hot read path
	// (Get for an already-resolved kind) never takes it.
	fillMu sync.Mutex

	// onReplace, if set, is called with the FULL current manifest set after every
	// whole-set refresh (boot Load + every NOTIFY/failsafe reload). The broker
	// uses it to register newly-applied kinds' (kind, task_type) claim pairs on the
	// running dispatcher — the live-registration path for a CRD applied AFTER the
	// broker booted (the common case: the fleet starts, then the operator applies
	// CRDs). Without it the broker would sit at "no pairs" and never claim a
	// just-applied kind until restart. Set once before Start; called from the
	// reload goroutine.
	onReplace func([]model.KindManifest)
}

// SetOnReplace registers a callback fired with the full manifest set after each
// whole-set refresh (Load + every reload). Set before Start.
func (c *KindManifestCache) SetOnReplace(fn func([]model.KindManifest)) { c.onReplace = fn }

// NewKindManifestCache builds the cache over the pool's store.
func NewKindManifestCache(pool *pgxpool.Pool) *KindManifestCache {
	c := &KindManifestCache{repo: store.New(pool)}
	c.snap.Store(&kindManifestSnapshot{
		byKind:  map[model.KindVersion]model.KindManifest{},
		checked: map[model.KindVersion]bool{},
	})
	// kind_manifest_changed LISTEN + 60s failsafe reload (Start launches it). A
	// dropped NOTIFY still converges on the failsafe tick.
	c.refresher = NewNotifyRefresher(NewPgxListener(pool), "manifestcache kind_manifest_changed",
		KindManifestChannel, ManifestReloadInterval, func(ctx context.Context) {
			if err := c.reload(ctx); err != nil {
				slog.Warn("manifest cache: failsafe reload", "err", err)
			}
		})
	return c
}

// Load reads every manifest into the cache synchronously (boot). A load error is
// returned so the caller can decide whether to proceed (the engine can run with
// an empty cache and miss-fill per kind, so a transient boot error is non-fatal).
func (c *KindManifestCache) Load(ctx context.Context) error {
	ms, err := c.repo.ListKindManifests(ctx)
	if err != nil {
		return err
	}
	c.replaceAll(ms)
	return nil
}

// Get returns a kind's manifest with a LOCK-FREE read on the hot path: an atomic
// pointer load + a map index, no mutex, no DB. On a cache MISS for a never-checked
// kind it does a single DB read and copy-on-write-fills the entry (so a just-
// applied kind is never wrongly reported absent). ok=false means the kind has no
// model. Safe for concurrent use.
func (c *KindManifestCache) Get(ctx context.Context, kind model.Kind, kindVersion int) (model.KindManifest, bool) {
	// kindVersion is the resource's explicit web-API version (>= 1, carried from the
	// row). No normalization: a 0 addresses (kind, 0), which no published manifest
	// occupies, so it resolves ABSENT — the omission surfaces, never a silent v1.
	key := model.KindVersion{Kind: kind, Version: kindVersion}
	s := c.snap.Load()
	if m, loaded := s.byKind[key]; loaded {
		return m, true
	}
	if _, wasChecked := s.checked[key]; wasChecked {
		// Resolved before and confirmed ABSENT (checked has the key, no byKind
		// entry) — return absent with no DB read until a reload. Key-presence is the
		// test, not the bool value (false here MEANS absent).
		return model.KindManifest{}, false
	}
	// Never checked: read-through once and copy-on-write the result into a fresh
	// snapshot. fillMu serializes concurrent miss-fills; the hot path above took
	// no lock. Re-check under fillMu in case a concurrent fill/reload resolved it.
	c.fillMu.Lock()
	defer c.fillMu.Unlock()
	cur := c.snap.Load()
	if m, loaded := cur.byKind[key]; loaded {
		return m, true
	}
	if _, wasChecked := cur.checked[key]; wasChecked {
		return model.KindManifest{}, false
	}
	m, ok, err := c.repo.GetKindManifest(ctx, kind, key.Version)
	if err != nil {
		slog.Warn("manifest cache: miss read", "kind", kind, "kind_version", key.Version, "err", err)
		return model.KindManifest{}, false
	}
	next := cur.clone()
	if ok {
		next.byKind[key] = m
	}
	next.checked[key] = ok
	c.snap.Store(next)
	return m, ok
}

// All returns every manifest currently in the cache (a fresh slice over the
// immutable snapshot — lock-free). A broker uses it to register the claim pairs
// of already-applied kinds at boot; kinds applied later arrive via the
// SetOnReplace reload callback. Order is unspecified.
func (c *KindManifestCache) All() []model.KindManifest {
	s := c.snap.Load()
	out := make([]model.KindManifest, 0, len(s.byKind))
	for _, m := range s.byKind {
		out = append(out, m)
	}
	return out
}

// clone makes a shallow copy of the snapshot's maps for copy-on-write. The
// KindManifest values are treated as immutable, so a shallow copy is safe.
func (s *kindManifestSnapshot) clone() *kindManifestSnapshot {
	byKind := make(map[model.KindVersion]model.KindManifest, len(s.byKind)+1)
	for k, v := range s.byKind {
		byKind[k] = v
	}
	checked := make(map[model.KindVersion]bool, len(s.checked)+1)
	for k, v := range s.checked {
		checked[k] = v
	}
	return &kindManifestSnapshot{byKind: byKind, checked: checked}
}

// replaceAll builds a fresh snapshot from the full manifest set and swaps it in.
// Shared by Load and reload; serialized by fillMu so it can't race a miss-fill.
func (c *KindManifestCache) replaceAll(ms []model.KindManifest) {
	next := &kindManifestSnapshot{
		byKind:  make(map[model.KindVersion]model.KindManifest, len(ms)),
		checked: make(map[model.KindVersion]bool, len(ms)),
	}
	for _, m := range ms {
		// A published manifest always carries an explicit kind_version (>= 1) — no
		// normalization; key by its exact (kind, kind_version).
		key := model.KindVersion{Kind: m.Kind, Version: m.KindVersion}
		next.byKind[key] = m
		next.checked[key] = true
	}
	c.fillMu.Lock()
	c.snap.Store(next)
	c.fillMu.Unlock()
	// Notify a subscriber (the broker) of the full current set so it can register
	// any newly-applied kind's claim pairs. Outside the lock — the callback may do
	// its own work and must not block other refreshers.
	if c.onReplace != nil {
		c.onReplace(ms)
	}
}

// Start launches the kind_manifest_changed listener + failsafe reload loop (the
// shared NotifyRefresher): the listener's onReady catch-up closes the subscribe-
// vs-apply race (a manifest inserted between boot Load and the LISTEN taking
// effect), the NOTIFY makes an operator's apply prompt, and the 60s failsafe
// converges even a dropped NOTIFY.
func (c *KindManifestCache) Start(ctx context.Context) { c.refresher.Start(ctx) }

// Stop cancels the listener + loop and joins them.
func (c *KindManifestCache) Stop() { c.refresher.Stop() }

// reload re-reads ALL manifests and atomically swaps in a fresh snapshot (a
// removed manifest drops out). Authoritative refresher; keeps the cache
// convergent regardless of NOTIFY delivery.
func (c *KindManifestCache) reload(ctx context.Context) error {
	ms, err := c.repo.ListKindManifests(ctx)
	if err != nil {
		return err
	}
	c.replaceAll(ms)
	return nil
}
