package test

// Live-refresh of the Resyncer kind-map from kind_config: an operator editing a
// kind's resync interval via UpsertKindConfig must take effect on the RUNNING
// control plane with NO restart (the K8s informer relist —
// ControlPlane.refreshResyncKinds → Resyncer.SetKinds). This locks the headline
// claim of the kind_config redesign; without it, dropping the SetKinds wiring
// would fail no test.

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestResyncIntervalLiveRetune: the widget kind boots with resync ENABLED but
// SLOW (1h) — so the Resyncer goroutine exists, but drift won't be re-checked
// within the test deadline. An operator then UpsertKindConfigs the interval down
// to 1s; the control plane's refresh tick pushes that into the running Resyncer
// (SetKinds), and a subsequently-introduced drift IS demoted — proving the edit
// applied with no restart.
func TestResyncIntervalLiveRetune(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	repo := store.New(pool)

	// Boot with resync ENABLED but slow (1h) so the Resyncer exists at start —
	// the live-retune path retunes a RUNNING Resyncer (a kind that booted with
	// resync OFF would need a restart; not what we're testing).
	probe := newHealthProbeWorker(model.Kind("widget"), time.Hour)
	reg := newTReg()
	reg.AddKind(probe, probe.Manifest())

	// Seed the widget's MANIFEST (which derives kind_config via the DB triggers)
	// before NewControlPlane reads kind_config — the SeedKindConfig replacement.
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))
	cp, err := engine.NewControlPlane(ctx, &engine.ControlPlaneConfig{
		Pool:                pool,
		ResyncSweepInterval: 300 * time.Millisecond, // sweep often once a kind is due
		// Fast relist so an UpsertKindConfig edit is picked up within the deadline.
		KindConfigRefreshInterval: 300 * time.Millisecond,
		RetryAfter:                1 * time.Second,
	})
	require.NoError(t, err)
	require.NoError(t, cp.Start(ctx))
	wn := startInProcWorker(t, ctx, pool, reg, nil, 0, nil)
	defer func() { _ = wn.Stop(ctx); _ = cp.Stop(ctx) }()

	applied, err := repo.ApplySpec(ctx, model.Kind("widget"), "w1",
		mustJSON(map[string]string{"name": "w1", "k": "v"}), nil)
	require.NoError(t, err)
	rootID := applied.ID

	q := dbq.New(pool)
	require.Eventually(t, func() bool {
		r, err := q.GetResourceInfo(ctx, rootID)
		return err == nil && r.IsReady && r.HealthOk
	}, 15*time.Second, 100*time.Millisecond, "should reconcile to ready+healthy")

	// Flip unhealthy. With resync at 1h, NOTHING re-checks it — so it should stay
	// ready for a short window (the resync interval is far longer than this).
	probe.SetHealthy("w1", false)
	require.Never(t, func() bool {
		r, err := q.GetResourceInfo(ctx, rootID)
		return err == nil && !r.IsReady
	}, 3*time.Second, 300*time.Millisecond,
		"with resync at 1h, drift must NOT be re-checked yet (proves the boot interval is in effect)")

	// Operator retunes resync down to 1s — LIVE, no restart. The control plane's
	// refresh tick relists kind_config and pushes it into the running Resyncer.
	require.NoError(t, repo.UpsertKindConfig(ctx, store.KindConfig{
		Kind:           model.Kind("widget"),
		KindVersion:    1,
		ResyncInterval: 1 * time.Second,
	}))

	// Now the (still-unhealthy) widget IS re-checked and demoted — proving the
	// edit reached the running Resyncer with no restart.
	require.Eventually(t, func() bool {
		r, err := q.GetResourceInfo(ctx, rootID)
		return err == nil && !r.IsReady && !r.HealthOk
	}, 20*time.Second, 200*time.Millisecond,
		"after the live resync retune, drift should be demoted with no restart")
}
