package api

import (
	"sort"
	"sync/atomic"

	"github.com/salesforce/converge/internal/model"
)

// schemaRegistry owns the API's PER-KIND SCHEMA SURFACE: the declared work-kind
// + reactor-kind manifests and the derived JSON-Schema cache. It answers every
// "does this deployment know this kind?" / "what shape does it have?" question
// the handlers ask, and has ONE reason to change — the set of kinds this
// deployment declares (driven by a CRD apply). The Server embeds it, so handlers
// call s.kindKnown(...) / s.schema() via promotion.
//
// CONCURRENCY: the whole schema surface is a single IMMUTABLE snapshot behind an
// atomic.Pointer. SetDeclaredSchemas / SetReactorSchemas build a fresh snapshot and
// swap it in one atomic store; every per-request read does one atomic load and then
// reads the snapshot's maps, which are never mutated after publication. This is what
// makes the reads safe: the maps handlers see are frozen, and the swap is a single
// word store. (A background refresher — runtime.NewNotifyRefresher, fired on every
// kind_manifest NOTIFY + a failsafe tick — drives the Set* calls WHILE the API serves
// traffic, so unsynchronized field reassignment would be a data race, possibly Go's
// fatal "concurrent map read and map write".) A registry with nothing wired (tests)
// has a nil snapshot and accepts any kind.
type schemaRegistry struct {
	snap atomic.Pointer[schemaSnapshot]
}

// schemaSnapshot is one immutable, atomically-published view of the schema surface.
// Every field is built once in a Set* call and never mutated after the store, so
// concurrent readers need no lock — only the atomic load of the pointer.
type schemaSnapshot struct {
	// schemaCache is the per-kind JSON-Schema cache (spec/status/config + verb
	// input), built ONCE per Set* and then immutable: every declared kind's shape
	// is pure type info known at boot, so it never needs a rebuild when a pending
	// kind registers later. All its methods are nil-receiver-safe.
	schemaCache *schemaCache

	// declared maps every DECLARED work kind to a REPRESENTATIVE manifest (the
	// highest published kindVersion) — INCLUDING kinds whose Setup is still PENDING.
	// It backs the single-key gates that don't care about the specific kindVersion:
	// the verb existence check, the delete finalizer, kindKnown, and the per-kind
	// operational block. Empty → no gate (tests that wire no manifests accept any
	// kind). The FULL per-kindVersion schema surface reads declaredList.
	declared map[model.Kind]model.KindManifest

	// declaredList is every declared (kind, kindVersion) manifest — the full schema
	// surface. A kind published at v1 AND v2 appears TWICE (one per kindVersion) so
	// registerOpenAPIComponents emits both VpcSpec and VpcSpecV2 and the Kinds list
	// documents every version. `declared` above collapses to one per kind.
	declaredList []model.KindManifest

	// declaredKindVersions lists the published web-API versions of each declared
	// work kind, ascending — the side-map that remembers a kind was published at v1
	// AND v2 (the Kinds page renders version chips + per-kindVersion /docs links).
	declaredKindVersions map[model.Kind][]int

	// reactorSchemas maps every declared REACTOR kind to its model. Kept SEPARATE
	// from `declared` on purpose: a reactor has no resource in the graph and is NOT
	// creatable, so it must not enter the work schema surface that gates spec/config
	// validation and kindKnown. Read by /api/kinds ONLY, to list reactors tagged
	// is_reactor.
	reactorSchemas map[model.Kind]model.KindManifest

	// reactorKindVersions is the reactor analogue of declaredKindVersions: the
	// published versions of each declared reactor kind, so the Kinds list can chip a
	// multi-version reactor exactly like a multi-version work kind.
	reactorKindVersions map[model.Kind][]int
}

// load returns the current snapshot, or a zero-value (empty) snapshot when nothing
// has been wired yet (tests) — so every accessor is nil-safe without a guard.
func (r *schemaRegistry) load() *schemaSnapshot {
	if s := r.snap.Load(); s != nil {
		return s
	}
	return &schemaSnapshot{}
}

// SetDeclaredSchemas records the WORK kinds this deployment declares (their
// applied manifests, incl. still-pending ones) and (re)builds the JSON-Schema
// cache, publishing a fresh snapshot atomically. Reactor state (if any) is carried
// over from the current snapshot, and the union cache is rebuilt to include it.
func (r *schemaRegistry) SetDeclaredSchemas(manifests []model.KindManifest) {
	m := make(map[model.Kind]model.KindManifest, len(manifests))
	kindVersions := make(map[model.Kind][]int, len(manifests))
	for _, km := range manifests {
		// A published manifest carries an explicit kind_version (>= 1) — no
		// normalization. `declared` keeps the HIGHEST kindVersion as the representative
		// manifest (a v2 re-publish's summary shows v2's shape); declaredKindVersions
		// remembers every published kindVersion so the Kinds page can chip them all.
		if prev, ok := m[km.Kind]; !ok || km.KindVersion >= prev.KindVersion {
			m[km.Kind] = km
		}
		kindVersions[km.Kind] = insertSortedUnique(kindVersions[km.Kind], km.KindVersion)
	}
	prev := r.load()
	declaredList := append([]model.KindManifest(nil), manifests...)
	// The cache holds every SERVABLE shape (work + carried-over reactor); work and
	// reactor kinds are disjoint, so no collision.
	reactorList := make([]model.KindManifest, 0, len(prev.reactorSchemas))
	for _, km := range prev.reactorSchemas {
		reactorList = append(reactorList, km)
	}
	r.snap.Store(&schemaSnapshot{
		schemaCache:          newSchemaCache(append(append([]model.KindManifest(nil), manifests...), reactorList...)),
		declared:             m,
		declaredList:         declaredList,
		declaredKindVersions: kindVersions,
		reactorSchemas:       prev.reactorSchemas,
		reactorKindVersions:  prev.reactorKindVersions,
	})
}

// insertSortedUnique inserts v into the ascending slice if absent.
func insertSortedUnique(xs []int, v int) []int {
	for _, x := range xs {
		if x == v {
			return xs
		}
	}
	xs = append(xs, v)
	sort.Ints(xs)
	return xs
}

// SetReactorSchemas records the REACTOR kinds this deployment declares and folds
// their shapes into the JSON-Schema cache so /api/kinds/{kind}/schema can serve a
// reactor's CONFIG schema. Reactors stay OUT of `declared` (the creatable-resource
// gate) because a reactor is bound to other kinds' transitions, not created
// directly. Carries the current work state forward and publishes a fresh snapshot
// atomically. Optional — a deployment with no reactors need not call it.
func (r *schemaRegistry) SetReactorSchemas(manifests []model.KindManifest) {
	m := make(map[model.Kind]model.KindManifest, len(manifests))
	kindVersions := make(map[model.Kind][]int, len(manifests))
	for _, km := range manifests {
		if prev, ok := m[km.Kind]; !ok || km.KindVersion >= prev.KindVersion {
			m[km.Kind] = km
		}
		kindVersions[km.Kind] = insertSortedUnique(kindVersions[km.Kind], km.KindVersion)
	}
	prev := r.load()
	// The cache holds every SERVABLE shape (carried-over work + reactor); `declared`
	// remains the work-only creatable gate. Work and reactor kinds are disjoint, so
	// no collision.
	r.snap.Store(&schemaSnapshot{
		schemaCache:          newSchemaCache(append(append([]model.KindManifest(nil), prev.declaredList...), manifests...)),
		declared:             prev.declared,
		declaredList:         prev.declaredList,
		declaredKindVersions: prev.declaredKindVersions,
		reactorSchemas:       m,
		reactorKindVersions:  kindVersions,
	})
}

// kindKnown reports whether kind is a declared WORK kind (registered or still
// pending). With no declared set wired (tests) it accepts anything.
func (r *schemaRegistry) kindKnown(kind model.Kind) bool {
	s := r.load()
	if len(s.declared) == 0 {
		return true
	}
	_, ok := s.declared[kind]
	return ok
}

// reactorKindKnown reports whether kind is a declared REACTOR kind. With no
// reactor set wired (tests) it accepts anything.
func (r *schemaRegistry) reactorKindKnown(kind model.Kind) bool {
	s := r.load()
	if len(s.reactorSchemas) == 0 {
		return true
	}
	_, ok := s.reactorSchemas[kind]
	return ok
}

// isReactorKind reports whether kind is POSITIVELY a declared reactor kind (a
// real membership hit, never the empty-set "accept anything"), so the
// resource-apply gate can REJECT a reactor kind applied as a resource.
func (r *schemaRegistry) isReactorKind(kind model.Kind) bool {
	_, ok := r.load().reactorSchemas[kind]
	return ok
}

// configurableKindKnown reports whether kind may carry a providerconfig — a
// declared WORK kind OR a declared REACTOR kind. Checks both maps DIRECTLY (not
// kindKnown||reactorKindKnown, whose per-map "accept anything" would open the
// gate the moment one map is empty); the empty-set fallback applies ONCE, over
// both maps together.
func (r *schemaRegistry) configurableKindKnown(kind model.Kind) bool {
	s := r.load()
	if len(s.declared) == 0 && len(s.reactorSchemas) == 0 {
		return true
	}
	if _, ok := s.declared[kind]; ok {
		return true
	}
	_, ok := s.reactorSchemas[kind]
	return ok
}

// declaredSchemas returns EVERY declared (kind, kindVersion) work manifest (incl.
// still-pending kinds); a multi-version kind appears once per version. Order
// follows insertion; callers sort. Returns a copy so callers may append.
func (r *schemaRegistry) declaredSchemas() []model.KindManifest {
	return append([]model.KindManifest(nil), r.load().declaredList...)
}

// kindVersionsForKind returns the ascending published kind versions of a kind, or
// [1] when unknown — every kind has at least v1. Falls back to the reactor
// side-map so a reactor published at v1+v2 chips both.
func (r *schemaRegistry) kindVersionsForKind(kind model.Kind) []int {
	s := r.load()
	if mm, ok := s.declaredKindVersions[kind]; ok && len(mm) > 0 {
		return append([]int(nil), mm...)
	}
	if mm, ok := s.reactorKindVersions[kind]; ok && len(mm) > 0 {
		return append([]int(nil), mm...)
	}
	return []int{1}
}

// reactorSchemasList returns the declared REACTOR kinds (empty when none). Order
// is unspecified; callers sort.
func (r *schemaRegistry) reactorSchemasList() []model.KindManifest {
	s := r.load()
	out := make([]model.KindManifest, 0, len(s.reactorSchemas))
	for _, km := range s.reactorSchemas {
		out = append(out, km)
	}
	return out
}

// servableSchemas returns every SERVABLE shape — work + reactor kinds — for the
// read/docs surface.
func (r *schemaRegistry) servableSchemas() []model.KindManifest {
	work := r.declaredSchemas()
	return append(work, r.reactorSchemasList()...)
}

// schemaForKind looks up one kind's declared WORK model.
func (r *schemaRegistry) schemaForKind(k model.Kind) (model.KindManifest, bool) {
	km, ok := r.load().declared[k]
	return km, ok
}

// servableSchema looks up one kind's manifest for the read surface: a work kind
// first, then a reactor kind. isReactor reports which set it came from.
func (r *schemaRegistry) servableSchema(k model.Kind) (km model.KindManifest, isReactor, ok bool) {
	s := r.load()
	if km, ok = s.declared[k]; ok {
		return km, false, true
	}
	km, ok = s.reactorSchemas[k]
	return km, ok, ok
}

// schema returns the immutable per-kind schema cache (nil until SetDeclaredSchemas;
// all schemaCache methods are nil-receiver-safe).
func (r *schemaRegistry) schema() *schemaCache { return r.load().schemaCache }
