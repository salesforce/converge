package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/host"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
)

// TestBrokerRegistersKindAppliedAfterBoot reproduces the `just dev` ordering that
// regressed: the broker boots BEFORE any CRD is applied (the fleet starts, then
// the operator applies CRDs). NewDispatch therefore registers ZERO claim pairs;
// the fix is the KindManifestCache reload callback (SetOnReplace) that registers a
// kind's pairs the moment its manifest lands. Without it the broker sits at
// "no pairs; awaiting live registration" and never claims — so a classicbom root
// never composes, exactly the reported symptom.
//
// The test mirrors claimDuty.Start: build NewDispatch with an empty DB, wire
// SetOnReplace → AddKindLive (a broker claims EVERY manifested kind, no subset),
// Start the cache, THEN apply the CRD and assert the broker gains the
// (kind, reconcile) pair.
func TestBrokerRegistersKindAppliedAfterBoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	// A freshly-booted prod broker before the operator applies any CRD: no
	// manifest is applied yet.
	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx)) // empty: no kind_manifest rows yet

	const brokerID = "broker-live-crd"
	cl := broker.NewDispatch(pool, mc, brokerID, 8, nil)

	// At boot, with no manifest applied, the broker has NO pairs.
	require.Equal(t, 0, cl.Dispatcher().PairCount(),
		"a broker booted before any CRD is applied must start with zero pairs")

	// Wire the live-registration callback exactly as claimDuty.Start does: on a
	// cache reload, register EVERY kind's pairs (a broker claims all manifested
	// kinds — there is no subset filter).
	mc.SetOnReplace(func(ms []model.KindManifest) {
		for _, m := range ms {
			cl.AddKindLive(m)
		}
	})
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	claimCtx, claimCancel := context.WithCancel(ctx)
	t.Cleanup(claimCancel)
	go func() { _ = cl.Dispatcher().Run(claimCtx) }()

	// NOW apply the account CRD (the operator action). The kind_manifest_changed
	// NOTIFY wakes the cache, which reloads and fires SetOnReplace → AddKindLive →
	// the dispatcher registers the (account, reconcile) pair live.
	applyKindFixture(t, ctx, st, "account")

	require.Eventually(t, func() bool {
		return cl.Dispatcher().PairCount() > 0
	}, 15*time.Second, 100*time.Millisecond,
		"the broker must register a claim pair after the CRD is applied (live, no restart)")

	t.Logf("SUCCESS: broker registered %d pair(s) after a post-boot CRD apply", cl.Dispatcher().PairCount())
}

// applyKindFixture loads kind-<kind>.json from the shipped fixture dirs and
// UPSERTs it (the operator's CRD apply), firing the kind_manifest_changed
// NOTIFY. Uses the same host loader + fixtureDirs() the rest of the suite uses
// (test/testfixtures + every examples/demos/*/testfixtures), so it exercises the
// real demo fixtures wherever they live.
func applyKindFixture(t *testing.T, ctx context.Context, st *store.Store, kind string) {
	t.Helper()
	for _, dir := range fixtureDirs() {
		manifests, err := host.LoadKindFixtures(dir)
		require.NoError(t, err)
		for _, m := range manifests {
			if string(m.Kind) == kind {
				_, err := st.UpsertKindManifest(ctx, m)
				require.NoError(t, err)
				return
			}
		}
	}
	t.Fatalf("no fixture for kind %q", kind)
}
