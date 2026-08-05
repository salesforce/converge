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

// TestWorkerCrashRecovery validates the heartbeat-lapse recovery path.
//
// Simulating a true pod crash in a test is awkward — the dispatcher's
// heartbeat goroutine keeps running as long as the process is alive,
// even if Work() blocks. So we directly model the post-crash state:
// claim a work_queue row by setting worker_id and poison its
// heartbeat_at to far in the past. That's exactly what Postgres sees
// after a real pod dies mid-Work. The reaper then:
//
//  1. reap_stale_work: detects the lapsed heartbeat, writes
//     WorkSucceeded=False with reason=stale_heartbeat, deletes the
//     work_queue row.
//  2. requeue_failed_and_pending: after RetryAfter, re-pends the
//     resource and calls schedule_eligible.
//  3. The worker (now responsive) re-runs Work; resource → is_ready=true.
//
// Tunings: SweeperStaleAfter 1s, RetryAfter 1s. Reaper ticks 200ms.
// Total wall time ~3-5s.
func TestWorkerCrashRecovery(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	composer := newControllableComposer()
	worker := newFlakyWorker(model.Kind(account.Kind))

	reg := newTReg()
	reg.AddKind(composer, composer.Manifest())
	reg.AddKind(worker, worker.Manifest())

	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{
		HeartbeatEvery:    200 * time.Millisecond,
		SweeperStaleAfter: 1 * time.Second,
		RetryAfter:        1 * time.Second,
	})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	rootID, err := eng.CreateRoot(ctx, model.Kind(classicbom.Kind), "crash-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "crashed-1"}),
		nil,
	)
	require.NoError(t, err)

	q := dbq.New(pool)

	// Wait for the child to become ready under normal flow first. This
	// validates the happy path baseline — child created, worker ran,
	// status flipped, is_ready=true.
	require.Eventually(t, func() bool {
		children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
		if err != nil || len(children) != 1 {
			return false
		}
		return children[0].IsReady
	}, 10*time.Second, 50*time.Millisecond, "initial reconcile")

	children, err := q.GetChildrenByOwner(ctx, dbq.GetChildrenByOwnerParams{OwnerID: pgtype.UUID{Bytes: rootID, Valid: true}, Limit: 1000000})
	require.NoError(t, err)
	childID := children[0].ID

	// Now simulate "worker died mid-Work":
	//   1. Insert a work_queue row claimed by a fake worker_id with a
	//      stale heartbeat (older than SweeperStaleAfter).
	// resources.is_ready stays true (the prior successful run is still
	// the latest signal); the reaper's job is to time out the in-flight
	// claim and re-pend so a new attempt runs.
	_, err = pool.Exec(ctx, `
		INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, worker_id, heartbeat_at, shard_id)
		SELECT id, 'reconcile', kind, kind_version, generation, spec, 'dead-worker', now() - INTERVAL '1 hour', shard_of(id)
		FROM resources WHERE id = $1`,
		childID)
	require.NoError(t, err)

	t.Log("simulated worker crash; in-flight work_queue row has a 1h-old heartbeat")

	// Snapshot the current call count so we can assert "ran again".
	priorCalls := worker.Calls("crashed-1")

	// The reaper should:
	//   (a) detect the stale heartbeat → reap_stale_work writes
	//       WorkSucceeded=False, deletes the work_queue row.
	//   (b) requeue_failed_and_pending after RetryAfter → re-pend +
	//       schedule.
	//   (c) Worker re-runs (this time without crash) → resource ready.
	require.Eventually(t, func() bool {
		return worker.Calls("crashed-1") > priorCalls
	}, 30*time.Second, 100*time.Millisecond, "crashed resource should recover via reaper")

	// And it should converge back to ready.
	require.Eventually(t, func() bool {
		r, err := q.GetResourceInfo(ctx, childID)
		if err != nil {
			return false
		}
		return r.IsReady
	}, 30*time.Second, 100*time.Millisecond, "resource should converge back to ready")

	t.Logf("recovered: worker called %d times pre-crash, %d total",
		priorCalls, worker.Calls("crashed-1"))
}
