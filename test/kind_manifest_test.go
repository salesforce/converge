package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/kindschema"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestKindManifestApplyDerivesAndGates proves the operator-facing manifest apply
// path end-to-end against a real DB: an UpsertKindManifest (a) derives the
// kind_config policy row via the DB trigger and does NOT project any
// reactor_bindings (a manifest no longer creates bindings — that wiring is a
// separate, explicit subscription), (b) yields a STABLE content-hash
// manifest_version (idempotent re-apply does not bump it), and (c) exposes the
// schema_hash the API 409 gate keys on (a schema change yields a different hash;
// a reactions-only change does not).
func TestKindManifestApplyDerivesAndGates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	m := model.KindManifest{
		Kind:          "widget",
		KindVersion:   1,
		FinalizerName: "converge.io/widget",
		SpecSchema:    json.RawMessage(`{"type":"object","properties":{"size":{"type":"integer"}}}`),
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}

	v1, err := st.UpsertKindManifest(ctx, m)
	require.NoError(t, err)
	require.NotZero(t, v1)

	// (a) Derived kind_config row (policy + finalizer + manifest_version mirrored).
	kc, ok, err := st.GetKindConfig(ctx, "widget", 1)
	require.NoError(t, err)
	require.True(t, ok, "trigger must derive a kind_config row")
	require.Equal(t, "converge.io/widget", kc.FinalizerName)

	// (a) A manifest apply creates NO reactor_bindings: bindings are explicit,
	// operator-applied subscriptions, never projected from a CRD.
	bindings, err := st.ListReactorBindings(ctx)
	require.NoError(t, err)
	require.Empty(t, bindings, "applying a kind manifest must NOT create any reactor_bindings (no projection)")

	// (b) Idempotent re-apply → SAME manifest_version (no spurious in-flight fence bump).
	v2, err := st.UpsertKindManifest(ctx, m)
	require.NoError(t, err)
	require.Equal(t, v1, v2, "re-applying an identical manifest must not change manifest_version")

	// A reactions-only change DOES bump the version (in-flight work must re-fence)...
	m2 := m
	m2.Reactions = append([]model.ReactionDecl{}, m.Reactions...)
	m2.Reactions[0] = model.ReactionDecl{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus, model.OutcomeConditions}}
	v3, err := st.UpsertKindManifest(ctx, m2)
	require.NoError(t, err)
	require.NotEqual(t, v1, v3, "a reactions change must bump manifest_version")

	// (c) schema_hash: unchanged across the reactions-only edit (the 409 gate must
	// NOT trip for a reactions change)...
	hashes1, ok, err := st.GetKindHashes(ctx, "widget", 1)
	require.NoError(t, err)
	require.True(t, ok)
	h1 := hashes1.SchemaHash
	require.Equal(t, kindschema.SchemaHashOf(m), h1, "stored schema_hash matches the canonical hash")

	// ...but a SCHEMA change yields a different hash (the 409 gate trips).
	m3 := m2
	m3.SpecSchema = json.RawMessage(`{"type":"object","properties":{"size":{"type":"string"}}}`) // int → string (incompatible)
	require.NotEqual(t, kindschema.SchemaHashOf(m), kindschema.SchemaHashOf(m3),
		"a spec schema change must produce a different hash (drives the API 409 gate)")
	_, err = st.UpsertKindManifest(ctx, m3)
	require.NoError(t, err)
	hashes3, _, err := st.GetKindHashes(ctx, "widget", 1)
	require.NoError(t, err)
	require.NotEqual(t, h1, hashes3.SchemaHash, "schema change must update the stored schema_hash")

	t.Logf("SUCCESS: manifest apply derives config (no binding projection), version stable on no-op, schema_hash gates schema edits")
}

// TestManifestVersionDenormalizedOntoResources proves the hot-path optimization's
// load-bearing invariant: manifest_version is denormalized onto resources (so the
// schedule/cascade INSERT reads it off the row with no kind_config join), and a
// CRD re-apply that BUMPS the version re-stamps every existing resource of that
// kind. Without this propagation a row created under v1 would forever enqueue work
// pinned to the stale v1 and the mid-flight fence would mis-fire.
func TestManifestVersionDenormalizedOntoResources(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	m := model.KindManifest{
		Kind:        "gadget",
		KindVersion: 1,
		SpecSchema:  json.RawMessage(`{"type":"object"}`),
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
	v1, err := st.UpsertKindManifest(ctx, m)
	require.NoError(t, err)
	require.NotZero(t, v1)

	// A resource created AFTER the manifest exists is stamped at v1 (engine.sql
	// reads kind_config at insert).
	res, err := st.ApplySpec(ctx, "gadget", "g1", json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	readVer := func() int64 {
		var got int64
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT manifest_version FROM resources WHERE id = $1`, res.ID).Scan(&got))
		return got
	}
	require.Equal(t, v1, readVer(), "a resource created under v1 must be stamped with v1")

	// A reactions change bumps the version; the AFTER trigger must re-stamp the
	// existing resource so its next scheduled task carries the new version.
	m2 := m
	m2.Reactions = []model.ReactionDecl{
		{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus, model.OutcomeConditions}},
	}
	v2, err := st.UpsertKindManifest(ctx, m2)
	require.NoError(t, err)
	require.NotEqual(t, v1, v2, "a reactions change must bump the version")
	require.Equal(t, v2, readVer(), "the version bump must re-stamp the existing resource (denormalized hot-path source)")

	// An idempotent re-apply (no version change) must NOT churn the resource row.
	v3, err := st.UpsertKindManifest(ctx, m2)
	require.NoError(t, err)
	require.Equal(t, v2, v3, "idempotent re-apply keeps the version")
	require.Equal(t, v2, readVer(), "a no-op re-apply leaves the resource version unchanged")

	t.Logf("SUCCESS: manifest_version denormalized onto resources; bump re-stamps existing rows, no-op re-apply does not")
}

// TestKindManifestRejectsIllegalReactions proves the DB-side legality CHECK (via
// the trigger) rejects a mis-declared manifest, matching the SDK ValidateManifest
// lattice — so an illegal manifest can't be persisted even bypassing the API.
func TestKindManifestRejectsIllegalReactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	// Two childrenSettled+status rollups — illegal (the rollup is singular).
	_, err := st.UpsertKindManifest(ctx, model.KindManifest{
		Kind:        "bad",
		KindVersion: 1,
		Reactions: []model.ReactionDecl{
			{Name: "r1", Trigger: model.TriggerChildrenSettled, Emits: model.OutcomeMask{model.OutcomeStatus}},
			{Name: "r2", Trigger: model.TriggerChildrenSettled, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	})
	require.Error(t, err, "two rollup reactions must be rejected")
}
