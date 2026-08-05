package test

import (
	"context"
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
)

// TestTransientWorkerFailureRetries validates that a Worker that
// returns an error on first invocation gets retried by the reaper and
// eventually succeeds.
//
//  1. Submit root with one child.
//  2. Worker fails the first 2 calls, then succeeds.
//  3. Each failure writes WorkSucceeded=False with reason+message via
//     the outbox drainer; is_ready stays false.
//  4. requeue_failed_and_pending re-pends the failed row after
//     RetryAfter (1s in this test).
//  5. Worker re-runs; second failure repeats; third call succeeds and
//     flips WorkSucceeded=True; is_ready becomes true.
//
// Tunings: RetryAfter 1s. Total wall time ~3-5s.
func TestTransientWorkerFailureRetries(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))
	worker.SetFailsBefore("flaky-1", 2) // first 2 calls fail; 3rd succeeds

	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())

	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{
		HeartbeatEvery: 200 * time.Millisecond,
		RetryAfter:     1 * time.Second,
	})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "transient-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "flaky-1"}),
		nil,
	)
	require.NoError(t, err)

	q := dbq.New(pool)

	// Wait for the resource to reach is_ready=true despite 2 transient failures.
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 1 {
			return false
		}
		return children[0].IsReady
	}, 20*time.Second, 100*time.Millisecond, "transient failures should resolve after retries")

	// Final state: status populated. GetChildrenByOwner is now a slim
	// projection (no spec/status); re-fetch via GetResource to read
	// the populated status field.
	children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
	require.NoError(t, err)
	require.Len(t, children, 1)
	require.True(t, children[0].IsReady)
	full, err := q.GetResource(ctx, dbq.GetResourceParams{Kind: string(children[0].Kind), Name: children[0].Name})
	require.NoError(t, err)
	require.NotEmpty(t, full.Status, "status should be populated")

	// Worker must have run exactly 3 times (2 failures + 1 success).
	require.Equal(t, 3, worker.Calls("flaky-1"),
		"flaky-1 should have run 3 times: 2 failures + 1 success")
	t.Logf("flaky-1 reached ready after %d calls", worker.Calls("flaky-1"))
}
