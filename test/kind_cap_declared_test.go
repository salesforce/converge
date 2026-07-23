package test

// Tests the cap pipeline in the CURRENT architecture: a kind's manifest DECLARES
// its concurrency cap via KindManifest.MaxInflight; UPSERTing that manifest into
// kind_manifest DERIVES the single kind_config row (max_inflight) via the manifest
// derive-trigger (insert-if-absent). The work_queue claim reads max_inflight from
// kind_config DIRECTLY. An OPERATOR then re-caps LIVE by editing kind_config
// (UpsertKindConfig / the kinds API) — the declared value is a one-time seed, not
// the runtime truth. (The cap is NOT a provider-handler argument; the handler code
// carries no cap — it lives entirely in the manifest → kind_config.)

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// capManifest builds a minimal legal work-kind manifest with the given declared
// cap. A single specChange→status reaction (a plain worker) is the smallest kind
// the validator accepts; MaxInflight is what the derive-trigger seeds into
// kind_config.max_inflight.
func capManifest(kind model.Kind, maxInflight int) model.KindManifest {
	return model.KindManifest{
		Kind:        kind,
		KindVersion: 1,
		SpecSchema:  json.RawMessage(`{"type":"object"}`),
		MaxInflight: maxInflight,
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
}

// queryCap reads a kind's effective cap from kind_config; ok reports whether the
// kind is CAPPED (a row exists with max_inflight > 0), matching the "row-with-cap
// ⇔ capped" semantics the claim's budget CTE enforces.
func queryCap(t *testing.T, ctx context.Context, repo *store.Store, kind model.Kind) (int, bool) {
	t.Helper()
	kc, found, err := repo.GetKindConfig(ctx, kind, 1)
	require.NoError(t, err)
	return kc.MaxInflight, found && kc.MaxInflight > 0
}

// TestDeclaredCapSeededAtStartup: a kind manifest's declared MaxInflight lands in
// kind_config DIRECTLY via the derive-trigger (the claim reads it from there), and
// a kind whose manifest declares MaxInflight 0 is NOT treated as capped.
func TestDeclaredCapSeededAtStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	repo := store.New(pool)

	// Applying each manifest derives its kind_config (max_inflight) via the
	// manifest derive-trigger — that's the whole pipeline, no provider handler
	// involved.
	_, err := repo.UpsertKindManifest(ctx, capManifest("cap-vpc", 100)) // declared cap
	require.NoError(t, err)
	_, err = repo.UpsertKindManifest(ctx, capManifest("cap-tgw", 0)) // uncapped
	require.NoError(t, err)
	_, err = repo.UpsertKindManifest(ctx, capManifest("cap-route", 25)) // declared cap
	require.NoError(t, err)

	got, ok := queryCap(t, ctx, repo, "cap-vpc")
	require.True(t, ok)
	require.Equal(t, 100, got, "declared cap is live in kind_config")

	got, ok = queryCap(t, ctx, repo, "cap-route")
	require.True(t, ok)
	require.Equal(t, 25, got)

	_, ok = queryCap(t, ctx, repo, "cap-tgw")
	require.False(t, ok, "a kind declaring MaxInflight 0 is not treated as capped")
}

// TestCapChangesLiveOnEdit: an operator editing a kind's cap in kind_config
// (max_inflight) takes effect IMMEDIATELY — no reboot, because the claim reads
// kind_config directly. Clearing it (→ 0) makes the kind uncapped; raising it
// re-caps. Caps are runtime-editable in one place; the manifest's declared value
// is only the one-time seed.
func TestCapChangesLiveOnEdit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	repo := store.New(pool)

	// Seed a capped kind via its manifest (declared cap 100 → derived kind_config).
	_, err := repo.UpsertKindManifest(ctx, capManifest("cap-vpc", 100))
	require.NoError(t, err)

	got, ok := queryCap(t, ctx, repo, "cap-vpc")
	require.True(t, ok)
	require.Equal(t, 100, got)

	// Operator clears the cap → uncapped at once (the next claim sees it; the read
	// here proves the source flipped with no restart).
	require.NoError(t, repo.UpsertKindConfig(ctx, store.KindConfig{Kind: "cap-vpc", KindVersion: 1, MaxInflight: 0}))
	_, ok = queryCap(t, ctx, repo, "cap-vpc")
	require.False(t, ok, "cleared cap makes the kind uncapped live (no reboot)")

	// Operator raises a new cap → live again.
	require.NoError(t, repo.UpsertKindConfig(ctx, store.KindConfig{Kind: "cap-vpc", KindVersion: 1, MaxInflight: 50}))
	got, ok = queryCap(t, ctx, repo, "cap-vpc")
	require.True(t, ok)
	require.Equal(t, 50, got, "re-capped value is live")
}
