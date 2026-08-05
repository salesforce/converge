package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

// TestReactionEngineReconcilesViaManifest proves the reaction engine drives a
// reconcile off the kind_manifest's declared reactions. A plain worker kind
// (account) carries a single specChange→status (work) reaction in its manifest;
// the engine selects it by mask, runs it, and the resource syncs.
func TestReactionEngineReconcilesViaManifest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)

	// The account runtime's manifest declares exactly one specChange→status
	// "work" reaction — the dispatch DATA the engine reconciles from. startInProcWorker
	// seeds it into kind_manifest (deriving kind_config + bindings via the DB
	// triggers) and enables the reaction engine off that manifest.
	reg := newTReg()
	reg.Add(demoruntime.Account(0, 0, fault.Injector{}))

	// Control duty (drainer/reaper) applies the outbox → synced_gen.
	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{RunControl: true})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	// In-proc worker: seeds the manifest + runs the (always-on) reaction engine.
	w := startInProcWorker(t, ctx, pool, reg, []model.Kind{model.Kind(account.Kind)}, 4, nil)
	t.Cleanup(func() { _ = w.Stop(context.Background()) })

	// Create an account root and wait for it to sync THROUGH the engine path.
	rootID, err := (&testEngine{Pool: pool}).CreateRoot(ctx, model.Kind(account.Kind), "acct-engine",
		json.RawMessage(`{"team_name":"alpha"}`), nil)
	require.NoError(t, err)

	status := waitSyncedStatus(t, ctx, pool, rootID)
	var s struct {
		AccountID string `json:"account_id"`
	}
	require.NoError(t, json.Unmarshal(status, &s), "status: %s", status)
	require.Equal(t, "acc-stub-alpha", s.AccountID, "engine must have run the account work reaction")
	t.Logf("SUCCESS: account synced via the reaction engine: %s", status)
}
