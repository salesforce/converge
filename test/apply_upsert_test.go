package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestApplySpecUpsertStates validates the single create-or-update
// Apply path (store.ApplySpec → UpsertResource), proving the three
// kubectl-style outcomes and the generation behavior behind them:
//
//  1. First apply of a new (kind,name) → created=true, changed=true,
//     generation=1.
//  2. Re-apply with an IDENTICAL spec → created=false, changed=false
//     (no-op); generation stays 1 — the UpsertResource body_changed
//     CTE's IS DISTINCT FROM gate skips the inline bump.
//  3. Re-apply with a DIFFERENT spec → created=false, changed=true
//     (configured); generation advances to 2.
//
// This is the backend behind the modal reporting "unchanged" vs
// "configured" and skipping the reconcile enqueue on a no-op.
func TestApplySpecUpsertStates(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	// Start the engine purely for its migration side-effect (apply the
	// schema). We don't need workers running to test the store write.
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	st := store.New(pool)
	const kind = model.Kind("widget")

	// 1. Create.
	r1, err := st.ApplySpec(ctx, kind, "w1", []byte(`{"size":1}`), nil)
	require.NoError(t, err)
	require.True(t, r1.Created, "first apply should report created")
	require.True(t, r1.Changed, "create is a change")
	require.EqualValues(t, 1, genOf(t, ctx, pool, r1.ID))

	// 2. No-op re-apply (identical spec).
	r2, err := st.ApplySpec(ctx, kind, "w1", []byte(`{"size":1}`), nil)
	require.NoError(t, err)
	require.Equal(t, r1.ID, r2.ID, "same (kind,name) → same row")
	require.False(t, r2.Created, "re-apply is not a create")
	require.False(t, r2.Changed, "identical spec must be a no-op")
	require.EqualValues(t, 1, genOf(t, ctx, pool, r2.ID), "no-op must NOT bump generation")

	// 2b. No-op even when JSON key order / whitespace differs — spec is
	// JSONB, so the comparison is structural, not byte-for-byte.
	r2b, err := st.ApplySpec(ctx, kind, "w1", []byte(`{ "size":  1 }`), nil)
	require.NoError(t, err)
	require.False(t, r2b.Changed, "reformatted-but-identical JSONB must be a no-op")
	require.EqualValues(t, 1, genOf(t, ctx, pool, r2b.ID))

	// 3. Real change.
	r3, err := st.ApplySpec(ctx, kind, "w1", []byte(`{"size":2}`), nil)
	require.NoError(t, err)
	require.False(t, r3.Created)
	require.True(t, r3.Changed, "different spec must report changed")
	require.EqualValues(t, 2, genOf(t, ctx, pool, r3.ID), "real change must bump generation")
}

// TestApplySpecToOwnedChildUpdatesInPlace is the regression for the bug
// where applying a spec to an OWNED CHILD mis-created a brand-new root
// with the same name (different uuid, owner_id NULL) instead of updating
// the child. With global (kind,name) uniqueness the upsert addresses the
// single existing row — so applying to a child must update THAT row in
// place: same id, owner preserved, no stray root, generation bumped.
func TestApplySpecToOwnedChildUpdatesInPlace(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	st := store.New(pool)

	// Seed a root and an owned child directly (no composer needed): the
	// child has owner_id = root.id and a globally-unique (kind,name).
	root, err := st.ApplySpec(ctx, model.Kind("widget"), "parent-root", []byte(`{}`), nil)
	require.NoError(t, err)
	// Children carry spec inline: seed directly with an explicit kind_version
	// (REQUIRED — the column is NOT NULL with no default).
	var childID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `
		INSERT INTO resources (owner_id, root_id, kind, kind_version, spec)
		VALUES ($1, $1, 'gadget', 1, '{"v":1}')
		RETURNING id`, root.ID).Scan(&childID))
	_, err = pool.Exec(ctx, `
		INSERT INTO resource_meta (id, kind, name, labels)
		VALUES ($1, 'gadget', 'child-1', '{}')`, childID)
	require.NoError(t, err)
	childGenBefore := genOf(t, ctx, pool, childID)

	rootCountBefore := countResources(t, ctx, pool, "gadget", "child-1")
	require.Equal(t, 1, rootCountBefore, "exactly one (gadget, child-1) exists pre-apply")

	// Apply a new spec to the CHILD's (kind,name). Must update the child
	// in place — NOT create a second root.
	res, err := st.ApplySpec(ctx, model.Kind("gadget"), "child-1", []byte(`{"v":2}`), nil)
	require.NoError(t, err)
	require.Equal(t, childID, res.ID, "apply must hit the existing child row, not a new one")
	require.False(t, res.Created, "applying to an existing child is not a create")
	require.True(t, res.Changed, "spec changed v:1 → v:2")

	// No stray root: still exactly one (gadget, child-1), and it still has
	// its owner.
	require.Equal(t, 1, countResources(t, ctx, pool, "gadget", "child-1"),
		"apply must NOT create a duplicate root with the same name")
	var ownerID *uuid.UUID
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT owner_id FROM resources WHERE id = $1`, childID).Scan(&ownerID))
	require.NotNil(t, ownerID, "child must keep its owner (no reparenting)")
	require.Equal(t, root.ID, *ownerID, "owner must still be the original root")

	require.Greater(t, genOf(t, ctx, pool, childID), childGenBefore,
		"child generation must advance on a real spec change")
}

// TestSpecHistoryCheckoutNavigation validates the immutable-spec rollback
// model: five authored versions, then free CHECKOUT navigation in any order
// (5 → 3 → 5 → 1 → 5) — proving (a) checkout moves the live cursor to an
// EXISTING revision without authoring a new version (the set stays 5),
// (b) the live body matches the checked-out version, (c) every authored
// version remains reachable forever, and (d) generation still advances on
// each checkout so the pipeline re-reconciles.
func TestSpecHistoryCheckoutNavigation(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	st := store.New(pool)
	const kind = model.Kind("widget")
	const name = "history-root"

	// Author 5 distinct versions (each a real spec change → a new revision).
	var id uuid.UUID
	for v := 1; v <= 5; v++ {
		r, err := st.ApplySpec(ctx, kind, name, []byte(fmt.Sprintf(`{"v":%d}`, v)), nil)
		require.NoError(t, err)
		id = r.ID
		require.True(t, r.Changed, "v%d should be a real change", v)
	}

	// History has exactly 5 versions (authored gens 1..5); newest-first; the
	// gen-5 version is current.
	hist, err := st.ListSpecHistory(ctx, kind, name, 100)
	require.NoError(t, err)
	require.Len(t, hist, 5, "five authored versions")
	require.True(t, hist[0].IsCurrent, "newest (gen 5) is current after the last apply")

	liveBody := func() string {
		var b string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT spec::text FROM resources WHERE id = $1`, id).Scan(&b))
		return b
	}
	versionCount := func() int {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM spec_versions WHERE resource_id = $1`, id).Scan(&n))
		return n
	}

	// Navigate in any order by AUTHORED generation: 3 → 5 → 1 → 5. Each
	// checkout must (1) make that version's body the live inline spec,
	// (2) NOT author a new version (the set stays 5), (3) bump the resource's
	// generation (re-reconcile). The version labels (authored gens 1..5) are
	// stable; the live generation counter advances past 5 with each checkout.
	for _, target := range []int64{3, 5, 1, 5} {
		genBefore := genOf(t, ctx, pool, id)
		newGen, err := st.RollbackSpec(ctx, id, target, "test")
		require.NoError(t, err)
		require.Greater(t, newGen, genBefore, "checkout to gen %d must bump generation", target)

		require.JSONEq(t, fmt.Sprintf(`{"v":%d}`, target), liveBody(),
			"after checkout the live body must equal v%d", target)
		require.Equal(t, 5, versionCount(),
			"checkout must NOT author a new version — set stays 5 (was checking out gen %d)", target)
	}

	// All five versions are still present and reachable after navigation.
	histAfter, err := st.ListSpecHistory(ctx, kind, name, 100)
	require.NoError(t, err)
	require.Len(t, histAfter, 5, "navigation never grows or shrinks the version set")
}

func genOf(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id any) int64 {
	t.Helper()
	var gen int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT generation FROM resources WHERE id = $1`, id).Scan(&gen))
	return gen
}

func countResources(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind, name string) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM resource_meta WHERE kind = $1 AND name = $2`, kind, name).Scan(&n))
	return n
}

// TestApplySpecConcurrentUpdatesNoLostWrite is the regression for the
// HIGH-severity update-vs-update race the apply-path audit found: two (here
// many) concurrent applies to the SAME existing (kind,name), each with a
// DISTINCT spec, must all serialize cleanly — no lost write, no
// spec_versions(resource_id, generation) PK collision surfaced as a 500,
// and a contiguous version log.
//
// The fix is the `FOR UPDATE OF r` lock in UpsertResource's prev CTE: the
// second apply blocks on the first, re-reads generation AFTER it commits,
// so ins_ver derives a fresh generation. Without it, both read generation=G,
// both target spec_versions(R, G+1), and the loser aborts (lost write +
// raw 500). Asserts: N concurrent applies → 0 errors, final generation = N
// (create + N-1 updates), and exactly N version rows at generations 1..N
// with no gaps or duplicates.
func TestApplySpecConcurrentUpdatesNoLostWrite(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	st := store.New(pool)
	const kind = model.Kind("widget")
	const name = "concurrent-root"

	// Seed gen 1.
	r0, err := st.ApplySpec(ctx, kind, name, []byte(`{"v":0}`), nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, r0.Generation)

	// Fire N concurrent applies, each with a distinct spec, at the SAME
	// (kind,name). Each must commit a new generation; none may be lost or
	// error out.
	const N = 12
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = st.ApplySpec(ctx, kind, name,
				[]byte(fmt.Sprintf(`{"v":%d}`, i+1)), nil)
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		require.NoError(t, e, "concurrent apply #%d must not error (no lost write / 500)", i)
	}

	// Final generation = 1 (seed) + N (each distinct apply bumped once).
	require.EqualValues(t, 1+N, genOf(t, ctx, pool, r0.ID),
		"every concurrent apply must have committed a distinct generation")

	// The version log is contiguous 1..(1+N): exactly one row per generation,
	// no gap (lost write) and no duplicate (PK collision that aborted a write).
	var count, minGen, maxGen, distinct int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*), min(generation), max(generation), count(DISTINCT generation)
		FROM spec_versions WHERE resource_id = $1`, r0.ID).
		Scan(&count, &minGen, &maxGen, &distinct))
	require.EqualValues(t, 1+N, count, "one version row per apply")
	require.EqualValues(t, 1+N, distinct, "no duplicate generations")
	require.EqualValues(t, 1, minGen)
	require.EqualValues(t, 1+N, maxGen, "version log is contiguous 1..1+N")
}

// TestGetResourceManifestRoundTrips proves the download/synthesize path:
// GetResourceManifest packs kind/name/labels/spec into one DB-built
// jsonb object (the same shape the Apply body accepts), and that object
// unmarshals into a model.ResourceManifest whose fields match what was
// applied — so a downloaded manifest re-applies verbatim and is a no-op
// (identical spec ⇒ no generation bump).
func TestGetResourceManifestRoundTrips(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	st := store.New(pool)
	const kind = model.Kind("widget")
	specIn := []byte(`{"size":3,"nested":{"a":[1,2,3]}}`)
	labelsIn := map[string]string{"team": "core", "env": "dev"}

	applied, err := st.ApplySpec(ctx, kind, "m1", specIn, labelsIn)
	require.NoError(t, err)

	// Synthesize the manifest DB-side and decode it.
	row, err := st.GetResourceManifest(ctx, kind, "m1")
	require.NoError(t, err)
	require.Equal(t, kind, row.Kind, "row.Kind column")
	require.Equal(t, "m1", row.Name, "row.Name column")

	var m model.ResourceManifest
	require.NoError(t, json.Unmarshal(row.Manifest, &m), "synthesized manifest must be valid JSON")
	require.Equal(t, kind, m.Kind, "manifest.kind")
	require.Equal(t, "m1", m.Name, "manifest.name")
	require.Equal(t, labelsIn, m.Labels, "manifest.labels must echo the applied set")
	require.JSONEq(t, string(specIn), string(m.Spec), "manifest.spec must equal the applied spec (JSONB-structural)")

	// Re-applying the downloaded manifest verbatim is a no-op: same spec,
	// same labels ⇒ no generation bump. This is the round-trip guarantee.
	re, err := st.ApplySpec(ctx, m.Kind, m.Name, m.Spec, m.Labels)
	require.NoError(t, err)
	require.Equal(t, applied.ID, re.ID, "round-trip apply hits the same row")
	require.False(t, re.Changed, "re-applying the downloaded manifest must be a no-op")
	require.EqualValues(t, genOf(t, ctx, pool, applied.ID), genOf(t, ctx, pool, re.ID),
		"no-op round-trip must not bump generation")
}

// TestResolveResourceIDByKindName proves the public-identity → internal-id
// boundary the name-keyed API handlers rely on: a resolve of an existing
// (kind, name) returns that row's id; a resolve of a missing pair returns the
// pgx.ErrNoRows sentinel (which the handlers map to 404), never a bogus id.
func TestResolveResourceIDByKindName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool) // apply the schema (no engine needed for a store write)
	st := store.New(pool)

	const kind = model.Kind("widget")
	applied, err := st.ApplySpec(ctx, kind, "resolvable", []byte(`{}`), nil)
	require.NoError(t, err)

	// Existing (kind, name) resolves to exactly that row's id.
	got, err := st.ResolveResourceIDByKindName(ctx, kind, "resolvable")
	require.NoError(t, err)
	require.Equal(t, applied.ID, got, "resolve must return the applied row's id")

	// A missing name (right kind) → ErrResourceNotFound sentinel, zero id (so the
	// API 404s absence but 500s a real read fault — no conflation).
	_, err = st.ResolveResourceIDByKindName(ctx, kind, "does-not-exist")
	require.ErrorIs(t, err, store.ErrResourceNotFound, "missing (kind,name) must surface ErrResourceNotFound")

	// A missing kind (right name) → also ErrResourceNotFound: identity is the PAIR.
	_, err = st.ResolveResourceIDByKindName(ctx, model.Kind("other"), "resolvable")
	require.ErrorIs(t, err, store.ErrResourceNotFound, "the (kind,name) pair is the key, not name alone")

	// Batch resolution returns only the refs that exist, keyed by public identity.
	resolved, err := st.ResolveResourceIDsByKindName(ctx, []store.ObjectRef{
		{Kind: string(kind), Name: "resolvable"},
		{Kind: string(kind), Name: "does-not-exist"},
	})
	require.NoError(t, err)
	require.Len(t, resolved, 1, "only the existing ref resolves")
	require.Equal(t, applied.ID, resolved[0].ID)
	require.Equal(t, "resolvable", resolved[0].Name)
}
