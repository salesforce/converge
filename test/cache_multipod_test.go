package test

// Multi-pod convergence of the DB-view caches. Each control/broker pod holds its
// OWN in-memory view of a shared Postgres table (KindManifestCache over
// kind_manifest, ProviderConfigCache over providerconfigs); an operator's write
// lands on ONE pod's store but MUST become visible on EVERY pod's cache — promptly
// via the table's LISTEN/NOTIFY, and, if a NOTIFY is ever lost, still within the
// failsafe interval. These tests stand up a 3-pod fleet of each cache over one
// real pool (real triggers + real LISTEN/NOTIFY) and assert every pod converges
// on an apply, an edit, and a delete — the property that, if broken, silently
// leaves some pods dispatching/validating against stale state in a multi-pod
// deployment (the class of bug the API schema-surface refresh gap was).
//
// Scope of these vs the chaos suites:
//   - #5 ShardSet/Resharder and #6 peerMesh are MEMBERSHIP-driven and already
//     fault-tested by TestBrokerChaos / TestControlPlaneHA / TestBrokerMesh*.
//   - The FAILSAFE tick (a dropped NOTIFY still converges) is unit-proven for the
//     shared driver in TestNotifyRefresherFailsafeTick; both caches here are built
//     on that NotifyRefresher, so the failsafe backstop is covered there.
//   - These tests fill the remaining gap: the kind_manifest / providerconfigs
//     table-view caches converging across pods on the NOTIFY path, which no chaos
//     test asserts.

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/specschema"
	"github.com/salesforce/converge/internal/store"
)

// cacheConvergeTimeout bounds "every pod sees it" — comfortably above the NOTIFY
// window so a pass means genuine convergence, not a lucky poll.
const cacheConvergeTimeout = 20 * time.Second

// TestKindManifestCacheConvergesAcrossPods proves an apply/edit/delete on the
// shared kind_manifest table becomes visible on EVERY pod's KindManifestCache —
// the core multi-pod invariant for cache #1 (dispatch reads reactions/policy from
// it; a stale pod would dispatch the wrong reaction or miss a kind entirely).
func TestKindManifestCacheConvergesAcrossPods(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	// 3 pods, each with its OWN cache over the one shared table (real LISTEN +
	// failsafe on each).
	caches := startManifestCacheFleet(t, ctx, pool, 3)

	const kind = "mpcache"
	// APPLY: nobody has it yet. Upsert on "one pod's" store; every pod's cache must
	// converge to seeing the kind with the right finalizer.
	_, err := st.UpsertKindManifest(ctx, manifestForCacheTest(kind, 1, "grace-a"))
	require.NoError(t, err)
	requireAllManifestConverge(t, "apply visible on all pods", caches, func(mc *runtime.KindManifestCache) bool {
		m, ok := mc.Get(ctx, model.Kind(kind), 1)
		return ok && m.FinalizerName == "grace-a"
	})

	// EDIT: change the finalizer (a real content change → new manifest_version →
	// NOTIFY). Every pod must converge to the NEW value, not a cached old one — this
	// exercises the whole-set reload replacing an already-present entry.
	_, err = st.UpsertKindManifest(ctx, manifestForCacheTest(kind, 1, "grace-b"))
	require.NoError(t, err)
	requireAllManifestConverge(t, "edit visible on all pods", caches, func(mc *runtime.KindManifestCache) bool {
		m, ok := mc.Get(ctx, model.Kind(kind), 1)
		return ok && m.FinalizerName == "grace-b"
	})

	// DELETE: every pod must converge to ABSENT — the asymmetry only the whole-set
	// reload (not the positive-only miss-fill) can pick up, so this specifically
	// guards the delete-propagation path.
	found, _, err := st.DeleteKindManifest(ctx, model.Kind(kind), 1)
	require.NoError(t, err)
	require.True(t, found)
	requireAllManifestConverge(t, "delete visible on all pods", caches, func(mc *runtime.KindManifestCache) bool {
		_, ok := mc.Get(ctx, model.Kind(kind), 1)
		return !ok
	})

	t.Logf("SUCCESS: apply/edit/delete on kind_manifest converged across %d pods", len(caches))
}

// TestProviderConfigCacheConvergesAcrossPods proves cache #4: an operator's
// default-providerconfig upsert/edit on one pod becomes visible on every pod's
// ProviderConfigCache (the broker serves cp.Config(kind,ver) to workers, so a
// stale pod would hand a worker an old default). Uses the DYNAMIC plane (nil
// kinds) — the broker's production mode, which discovers the live (kind,ver) set
// from the DB.
func TestProviderConfigCacheConvergesAcrossPods(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	const kind = "pccache"
	// A manifest must exist so the DYNAMIC plane's ConfiguredKindVersions discovers
	// (kind,1) as a tracked pair (a kind_config row exists once the manifest lands).
	_, err := st.UpsertKindManifest(ctx, manifestForCacheTest(kind, 1, ""))
	require.NoError(t, err)

	// 3 dynamic-mode pods, each its own ProviderConfigCache over the shared table.
	caches := make([]*runtime.ProviderConfigCache, 3)
	for i := range caches {
		cp := runtime.NewProviderConfigCache(ctx, pool, nil)
		cp.Start(ctx)
		t.Cleanup(cp.Stop)
		caches[i] = cp
	}

	// APPLY a default config on "one pod's" store → every pod's cache must serve it.
	specA := json.RawMessage(`{"endpoint":"a"}`)
	_, _, err = st.UpsertProviderConfig(ctx, kind+"-default", model.Kind(kind), 1, true, specA, nil)
	require.NoError(t, err)
	requireAllConfigConverge(t, "config apply visible on all pods", caches, func(cp *runtime.ProviderConfigCache) bool {
		return jsonEq(cp.Config(model.Kind(kind), 1), specA)
	})

	// EDIT it → every pod converges to the new document (not the cached old one).
	specB := json.RawMessage(`{"endpoint":"b"}`)
	_, _, err = st.UpsertProviderConfig(ctx, kind+"-default", model.Kind(kind), 1, true, specB, nil)
	require.NoError(t, err)
	requireAllConfigConverge(t, "config edit visible on all pods", caches, func(cp *runtime.ProviderConfigCache) bool {
		return jsonEq(cp.Config(model.Kind(kind), 1), specB)
	})

	t.Logf("SUCCESS: providerconfig apply/edit converged across %d pods", len(caches))
}

// TestSchemaValidatorConvergesAcrossPods proves cache #2 — the API's spec-schema
// validation surface — converges across pods. This is the cache that actually had
// the bug: an API pod that didn't handle the manifest PUT kept validating against
// a STALE schema, spuriously rejecting a resource that used a newly-added field.
// The fix gave it a kind_manifest_changed NotifyRefresher (like #1/#4); this test
// reproduces the exact scenario over a shared DB with two pods' validators, each
// rebuilt by that refresher, and asserts the non-applying pod converges to the new
// schema. Deleting the refresh wiring regresses this test.
func TestSchemaValidatorConvergesAcrossPods(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	const kind = "schemacache"
	// v1 schema: an object requiring only "a".
	schemaA := json.RawMessage(`{"type":"object","required":["a"],"properties":{"a":{"type":"string"}},"additionalProperties":false}`)
	m := manifestForCacheTest(kind, 1, "")
	m.SpecSchema = schemaA
	_, err := st.UpsertKindManifest(ctx, m)
	require.NoError(t, err)

	// Two API-pod validators, each an independent specschema.Validator rebuilt from
	// the shared kind_manifest by its OWN kind_manifest_changed refresher — exactly
	// what cmd/converge's rebuildKindSchemaSurface does per API pod. Each holds its
	// current validator in an atomic pointer the refresher swaps.
	podA := newSchemaValidatorPod(t, ctx, pool)
	podB := newSchemaValidatorPod(t, ctx, pool)

	// Both pods must first converge to the v1 schema: "a" required, "b" rejected.
	requireValidatorConverge(t, "v1 schema on both pods", []*schemaValidatorPod{podA, podB}, func(p *schemaValidatorPod) bool {
		return p.validate(kind, 1, `{"a":"x"}`) == nil && // valid under v1
			p.validate(kind, 1, `{"a":"x","b":"y"}`) != nil // "b" unknown under v1 (additionalProperties:false)
	})

	// APPLY an ADDITIVE, backward-compatible edit on pod A's store: add optional "b".
	// Under the old (stale) schema pod B would REJECT {"a","b"} — the bug. After the
	// refresh, pod B (which never handled the write) must converge to ACCEPTING it.
	schemaB := json.RawMessage(`{"type":"object","required":["a"],"properties":{"a":{"type":"string"},"b":{"type":"string"}},"additionalProperties":false}`)
	m.SpecSchema = schemaB
	_, err = st.UpsertKindManifest(ctx, m)
	require.NoError(t, err)

	requireValidatorConverge(t, "additive field accepted on the non-applying pod", []*schemaValidatorPod{podA, podB}, func(p *schemaValidatorPod) bool {
		return p.validate(kind, 1, `{"a":"x","b":"y"}`) == nil // now valid on EVERY pod
	})

	t.Logf("SUCCESS: schema validator converged an additive edit across pods (no stale-reject)")
}

// ── helpers ──────────────────────────────────────────────────────────────────

// startManifestCacheFleet builds n independent KindManifestCaches over the shared
// pool — the multi-pod stand-in: each is a separate pod's cache with its own
// LISTEN + failsafe, all reading the one kind_manifest table. Started (Load +
// listener) and stopped via t.Cleanup.
func startManifestCacheFleet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int) []*runtime.KindManifestCache {
	t.Helper()
	caches := make([]*runtime.KindManifestCache, n)
	for i := range caches {
		mc := runtime.NewKindManifestCache(pool)
		require.NoError(t, mc.Load(ctx))
		mc.Start(ctx)
		t.Cleanup(mc.Stop)
		caches[i] = mc
	}
	return caches
}

// requireAllManifestConverge asserts pred holds on EVERY cache within the
// convergence timeout — the multi-pod property (not just "some pod saw it").
func requireAllManifestConverge(t *testing.T, what string, caches []*runtime.KindManifestCache, pred func(*runtime.KindManifestCache) bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, mc := range caches {
			if !pred(mc) {
				return false
			}
		}
		return true
	}, cacheConvergeTimeout, 100*time.Millisecond, what)
}

func requireAllConfigConverge(t *testing.T, what string, caches []*runtime.ProviderConfigCache, pred func(*runtime.ProviderConfigCache) bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, cp := range caches {
			if !pred(cp) {
				return false
			}
		}
		return true
	}, cacheConvergeTimeout, 100*time.Millisecond, what)
}

// schemaValidatorPod is one API pod's spec-validation surface: a specschema
// Validator held in an atomic pointer that a kind_manifest_changed NotifyRefresher
// rebuilds from the shared kind_manifest — the minimal faithful stand-in for
// cmd/converge's rebuildKindSchemaSurface (which does SetDeclaredSchemas +
// rebuildValidator on the same signal). If the refresher wiring were dropped, the
// pod would never rebuild and TestSchemaValidatorConvergesAcrossPods would fail.
type schemaValidatorPod struct {
	v         atomic.Pointer[specValidatorHolder]
	refresher *runtime.NotifyRefresher
}

type specValidatorHolder struct{ specschema.Validator }

// newSchemaValidatorPod builds a pod whose validator is rebuilt from all
// kind_manifest rows on every kind_manifest_changed NOTIFY + a short failsafe.
func newSchemaValidatorPod(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *schemaValidatorPod {
	t.Helper()
	st := store.New(pool)
	p := &schemaValidatorPod{}
	rebuild := func(ctx context.Context) {
		ms, err := st.ListKindManifests(ctx)
		if err != nil {
			return // keep prior validator on a transient read error (as main.go does)
		}
		p.v.Store(&specValidatorHolder{specschema.New(ms)})
	}
	// Same discipline as the production API pod: kind_manifest_changed LISTEN + a
	// failsafe (short here so the test doesn't wait the 60s prod cadence).
	p.refresher = runtime.NewNotifyRefresher(runtime.NewPgxListener(pool),
		"test apischema kind_manifest_changed", runtime.KindManifestChannel, 2*time.Second, rebuild)
	p.refresher.Start(ctx)
	t.Cleanup(p.refresher.Stop)
	return p
}

// validate runs the pod's CURRENT validator against a spec. A nil validator (not
// yet rebuilt) validates nothing → treat as "not converged" by returning an error.
func (p *schemaValidatorPod) validate(kind string, version int, spec string) error {
	h := p.v.Load()
	if h == nil {
		return errNotConverged
	}
	return h.ValidateSpec(model.Kind(kind), version, json.RawMessage(spec))
}

var errNotConverged = fmt.Errorf("validator not yet rebuilt")

// requireValidatorConverge asserts pred holds on EVERY pod within the timeout.
func requireValidatorConverge(t *testing.T, what string, pods []*schemaValidatorPod, pred func(*schemaValidatorPod) bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, p := range pods {
			if !pred(p) {
				return false
			}
		}
		return true
	}, cacheConvergeTimeout, 100*time.Millisecond, what)
}

// manifestForCacheTest builds a minimal valid KindManifest for a (kind,version)
// with a distinguishing finalizer (the field we mutate to observe convergence).
func manifestForCacheTest(kind string, version int, finalizer string) model.KindManifest {
	return model.KindManifest{
		Kind:          model.Kind(kind),
		KindVersion:   version,
		SpecSchema:    json.RawMessage(`{"type":"object"}`),
		FinalizerName: finalizer,
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
}

// jsonEq compares two JSON documents semantically (whitespace/key-order-insensitive).
func jsonEq(a, b json.RawMessage) bool {
	var ax, bx any
	if json.Unmarshal(a, &ax) != nil || json.Unmarshal(b, &bx) != nil {
		return false
	}
	ab, _ := json.Marshal(ax)
	bb, _ := json.Marshal(bx)
	return string(ab) == string(bb)
}
