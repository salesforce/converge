package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestComposeConfigDiff exercises the provider-config half of the compose diff
// at the store layer (deterministic, no engine timing): a recompose must
//   - CREATE the configs a composer emits, owned by the root,
//   - UPSERT only a config whose spec/role changed (minimal diff),
//   - PRUNE a config the new compose no longer emits, and
//   - the owner FK must CASCADE-delete a root's configs when the root is deleted.
//
// Covers the new ApplyComposeResult configs path + BatchUpsertProviderConfigsSQL
// owner guard + idx_providerconfigs_owner by-owner read against real Postgres.
func TestComposeConfigDiff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	// startAllRolesEngine is used only for its migration side-effect (schema
	// creation); we drive ApplyComposeResult directly.
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	repo := store.New(pool)

	rootID, err := eng.CreateRoot(ctx, "compose-config-root-kind", "config-root",
		json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	cfg := func(name, kind string, isDefault bool, bucket string) model.ProviderConfigSpec {
		return model.ProviderConfigSpec{
			Name:        name,
			Kind:        model.Kind(kind),
			KindVersion: 1,
			IsDefault:   isDefault,
			Spec:        map[string]any{"bucket": bucket},
		}
	}

	// apply runs one ApplyComposeResult in its own tx (mirrors the worker). No
	// children/edges — this test is about the configs delta. ApplyComposeResult
	// is FENCED on the work_queue claim's claim_epoch at gen, so claim the root's
	// reconcile row at gen first (mirrors the dispatcher), then pass its work_id +
	// the row's claim_epoch. fenceClaimRoot leaves the freshly-enqueued row at the
	// default epoch 1, so the fence matches on claimEpoch=1 (manifest 0 = inert).
	const composeWorker = "compose-config-test-worker"
	apply := func(gen int64, configs []model.ProviderConfigSpec) store.ComposeCounts {
		wq := fenceClaimRoot(t, ctx, pool, rootID, composeWorker, gen)
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		counts, err := repo.WithTx(tx).ApplyComposeResult(ctx, rootID, gen, wq, 1, 0, nil, nil, configs, nil)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
		return counts
	}

	// ── gen 1: emit a default + a custom config → both CREATED, owned by root ──
	c1 := apply(1, []model.ProviderConfigSpec{
		cfg("leaf-default", "leaf", true, "a"),
		cfg("config-root-custom", "leaf", false, "b"),
	})
	require.Equal(t, 2, c1.ConfigsCreated, "both configs are new")
	require.Equal(t, 0, c1.ConfigsUpdated)
	require.Equal(t, 0, c1.ConfigsDeleted)

	owned, err := repo.ListProviderConfigsByOwner(ctx, rootID)
	require.NoError(t, err)
	require.Len(t, owned, 2, "root owns both emitted configs")

	def, found, err := repo.GetProviderConfig(ctx, "leaf-default")
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, def.IsDefault, "default flag persisted")
	require.NotNil(t, def.OwnerID)
	require.Equal(t, rootID, *def.OwnerID, "config is owned by the composing root")
	require.JSONEq(t, `{"bucket":"a"}`, string(def.Spec))

	// ── gen 2: change ONLY the default's spec → exactly 1 upsert, 0 created ──
	c2 := apply(2, []model.ProviderConfigSpec{
		cfg("leaf-default", "leaf", true, "z"),
		cfg("config-root-custom", "leaf", false, "b"),
	})
	require.Equal(t, 0, c2.ConfigsCreated)
	require.Equal(t, 1, c2.ConfigsUpdated, "only the changed config upserted")
	require.Equal(t, 0, c2.ConfigsDeleted)

	def, _, err = repo.GetProviderConfig(ctx, "leaf-default")
	require.NoError(t, err)
	require.JSONEq(t, `{"bucket":"z"}`, string(def.Spec), "spec change applied")

	// ── gen 2b: re-apply identical → minimal diff, nothing touched ──
	c2b := apply(2, []model.ProviderConfigSpec{
		cfg("leaf-default", "leaf", true, "z"),
		cfg("config-root-custom", "leaf", false, "b"),
	})
	require.Equal(t, 0, c2b.ConfigsCreated)
	require.Equal(t, 0, c2b.ConfigsUpdated, "unchanged configs must not re-upsert")
	require.Equal(t, 0, c2b.ConfigsDeleted)

	// ── gen 3: drop the custom config → it must be PRUNED ──
	c3 := apply(3, []model.ProviderConfigSpec{
		cfg("leaf-default", "leaf", true, "z"),
	})
	require.Equal(t, 0, c3.ConfigsCreated)
	require.Equal(t, 0, c3.ConfigsUpdated)
	require.Equal(t, 1, c3.ConfigsDeleted, "exactly the vanished custom config pruned")

	_, found, err = repo.GetProviderConfig(ctx, "config-root-custom")
	require.NoError(t, err)
	require.False(t, found, "pruned config is gone")

	// Paginated list + filters: the default-only filter returns the surviving default.
	defaults, err := repo.ListProviderConfigs(ctx, store.ProviderConfigFilter{
		Kind: "leaf", IsDefault: boolPtr(true),
	}, 50, 0)
	require.NoError(t, err)
	require.Len(t, defaults, 1)
	require.Equal(t, "leaf-default", defaults[0].Name)
	total, err := repo.CountProviderConfigs(ctx, store.ProviderConfigFilter{Kind: "leaf"})
	require.NoError(t, err)
	require.Equal(t, int64(1), total, "only the default remains for kind leaf")

	// ── owner CASCADE: deleting the root removes the configs it owns ──
	_, err = pool.Exec(ctx, `DELETE FROM resources WHERE id=$1`, rootID)
	require.NoError(t, err)
	_, found, err = repo.GetProviderConfig(ctx, "leaf-default")
	require.NoError(t, err)
	require.False(t, found, "ON DELETE CASCADE removes the root's configs")

	t.Logf("config diff: created/updated/pruned minimally, owner cascade GC works")
}

func boolPtr(b bool) *bool { return &b }

// TestUpsertProviderConfigNilBundle is the regression for a 500 on applying a
// providerconfig with NO bundle: data is BYTEA NOT NULL DEFAULT ”, and a nil
// []byte binds as SQL NULL (the column default does NOT apply when the column is
// present in the INSERT), tripping the NOT NULL constraint. Every bundle-less
// config POST (e.g. the celbom rule set, which uses spec not data) must succeed —
// UpsertProviderConfig coalesces nil data → empty bytes. Runs against real
// Postgres because the constraint only fires there.
func TestUpsertProviderConfigNilBundle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool) // migration side-effect (schema)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	repo := store.New(pool)

	// A config with spec only and NO bundle (data == nil) — the celbom/no-bundle case.
	pc, created, err := repo.UpsertProviderConfig(ctx, "nobundle", "leaf", 1, true,
		json.RawMessage(`{"rules":[]}`), nil)
	require.NoError(t, err, "a bundle-less providerconfig must insert (nil data -> empty bytes, not NULL)")
	require.True(t, created)
	require.Equal(t, "nobundle", pc.Name)

	// Read it back: spec round-trips, and the default-config getter returns an
	// empty (not nil-erroring) bundle.
	def, found, err := repo.GetDefaultProviderConfig(ctx, "leaf", 1)
	require.NoError(t, err)
	require.True(t, found)
	require.JSONEq(t, `{"rules":[]}`, string(def.Spec))
	require.Empty(t, def.Data, "no bundle -> empty data, no NULL")

	// Re-apply (the upsert ON CONFLICT path) also must not NULL-violate.
	_, created, err = repo.UpsertProviderConfig(ctx, "nobundle", "leaf", 1, true,
		json.RawMessage(`{"rules":[]}`), nil)
	require.NoError(t, err)
	require.False(t, created, "second apply is an update")
}

// TestComposeConfigDefaultCollision guards the poison-pill fix: when two
// composer roots each emit a DIFFERENT-named default for the SAME kind, the
// second compose must NOT fail (which, via uq_providerconfigs_default_per_kind,
// would abort and endlessly re-pend the whole compose) — its default is demoted
// to a custom override instead. The first claimant keeps the kind's default.
func TestComposeConfigDefaultCollision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	eng := startAllRolesEngine(t, ctx, pool)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	repo := store.New(pool)

	rootA, err := eng.CreateRoot(ctx, "compose-config-root-kind", "collide-a",
		json.RawMessage(`{}`), nil)
	require.NoError(t, err)
	rootB, err := eng.CreateRoot(ctx, "compose-config-root-kind", "collide-b",
		json.RawMessage(`{}`), nil)
	require.NoError(t, err)

	cfg := func(name, kind string, isDefault bool, bucket string) model.ProviderConfigSpec {
		return model.ProviderConfigSpec{
			Name:        name,
			Kind:        model.Kind(kind),
			KindVersion: 1,
			IsDefault:   isDefault,
			Spec:        map[string]any{"bucket": bucket},
		}
	}
	applyFor := func(root uuid.UUID, configs []model.ProviderConfigSpec) {
		wq := fenceClaimRoot(t, ctx, pool, root, "compose-collide-test-worker", 1)
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		_, err = repo.WithTx(tx).ApplyComposeResult(ctx, root, 1, wq, 1, 0, nil, nil, configs, nil)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
	}

	// Root A claims kind "collide" 's default.
	applyFor(rootA, []model.ProviderConfigSpec{cfg("collide-default-a", "collide", true, "a")})
	// Root B emits a DIFFERENT-named default for the same kind — must succeed
	// (no 23505 poison pill) with B's config demoted to a custom override.
	applyFor(rootB, []model.ProviderConfigSpec{cfg("collide-default-b", "collide", true, "b")})

	a, found, err := repo.GetProviderConfig(ctx, "collide-default-a")
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, a.IsDefault, "first claimant keeps the kind default")

	b, found, err := repo.GetProviderConfig(ctx, "collide-default-b")
	require.NoError(t, err)
	require.True(t, found, "the conflicting config is still created")
	require.False(t, b.IsDefault, "second default for the kind demoted to a custom override")

	defaults, err := repo.ListProviderConfigs(ctx, store.ProviderConfigFilter{
		Kind: "collide", IsDefault: boolPtr(true),
	}, 50, 0)
	require.NoError(t, err)
	require.Len(t, defaults, 1, "exactly one default survives for the kind")
	require.Equal(t, "collide-default-a", defaults[0].Name)
}
