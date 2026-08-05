package test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestWorkFailedEventCarriesWorkerLocator validates that a work-failed
// event records the locator fields an operator needs to find the logs:
// the actor/worker id (host-pid-uuid → the slog `worker=` key) plus
// task_type and attempts. So "this resource failed" is traceable to the
// exact process and log filter after the fact.
func TestWorkFailedEventCarriesWorkerLocator(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	worker.SetFailsBefore("doomed", 1000) // never succeeds
	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())
	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "evt-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "doomed"}), nil)
	require.NoError(t, err)

	q := dbq.New(pool)
	repo := store.New(pool)

	// Find the doomed child + wait for it to reach phase=Failed.
	var childID [16]byte
	require.Eventually(t, func() bool {
		kids, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil {
			return false
		}
		for _, c := range kids {
			if c.Name == "doomed" {
				childID = c.ID
				return c.Phase == "Failed"
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond, "doomed child reaches phase=Failed")

	// Read its events; find a work-failed one and assert it locates the worker.
	var failedEvt *store.EventRow
	require.Eventually(t, func() bool {
		evs, err := repo.ListEvents(ctx, childID, time.Time{}, 0, 100)
		if err != nil {
			return false
		}
		for i := range evs {
			if evs[i].Type == "work-failed" {
				failedEvt = &evs[i]
				return true
			}
		}
		return false
	}, 15*time.Second, 200*time.Millisecond, "a work-failed event is recorded")

	// Actor is the broker id (the primary log key — the broker that ran the dispatch).
	require.NotEmpty(t, failedEvt.Actor, "work-failed event must record the broker as actor")

	// Detail carries the explicit locator breakdown.
	var detail map[string]any
	require.NoError(t, json.Unmarshal(failedEvt.Detail, &detail))
	require.Equal(t, failedEvt.Actor, detail["broker"], "detail.broker must match the actor")
	require.NotEmpty(t, detail["broker"], "detail.broker (host-pid-uuid log key) must be present")
	require.Equal(t, "reconcile", detail["task_type"])
	require.Contains(t, detail, "attempts", "detail must record which attempt failed")
	require.Contains(t, detail, "generation")

	t.Logf("work-failed event located to worker=%v task=%v attempts=%v",
		detail["worker"], detail["task_type"], detail["attempts"])
}
