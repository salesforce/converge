package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

// TestWorkQueueReleaseBroker validates the graceful-shutdown claim-release path
// (WorkQueueReleaseBroker, called from cmd/converge's shutdown sequence):
// a pod that is terminating frees ONLY its own in-flight claims, so a surviving
// pod re-claims the work immediately instead of waiting out the reaper's stale
// window. The key correctness points:
//
//   - it nulls broker_id for EVERY row this worker holds (regardless of how
//     fresh the heartbeat is — a gracefully-stopped pod's heartbeat is recent),
//   - it leaves OTHER pods' claims untouched (scoped by broker_id),
//   - a freed row is immediately re-claimable (broker_id IS NULL).
//
// This is the millisecond fast-handoff that fixes the multi-minute rollout
// stranding; the reaper's now-30s window is only the crash backstop.
func TestWorkQueueReleaseBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)

	// Seed the kinds' manifests so they resolve (the claim/release paths read
	// kind_config, derived from kind_manifest).
	reg := newTReg()
	reg.Add(demoruntime.ClassicBOM(0))
	reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
	require.NoError(t, reg.seed(ctx, pool))

	st := store.New(pool)

	// Two resources, each with a FRESH-heartbeat claim — one held by the pod
	// that is shutting down, one by a surviving pod. A graceful shutdown's
	// heartbeat is recent (the dispatcher only just stopped), so the release
	// must NOT depend on a stale heartbeat the way the reaper does.
	dyingRoot, err := st.ApplySpec(ctx, model.Kind(classicbom.Kind), "dying-pod-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "held-by-dying"}), nil)
	require.NoError(t, err)
	survivorRoot, err := st.ApplySpec(ctx, model.Kind(classicbom.Kind), "survivor-pod-root",
		buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: "held-by-survivor"}), nil)
	require.NoError(t, err)

	const dyingWorker = "dying-pod-host-111-aaaaaaaa"
	const survivorWorker = "survivor-pod-host-222-bbbbbbbb"

	// ApplySpec already enqueued each root's reconcile work_queue row (via
	// schedule_eligible). CLAIM those existing rows with FRESH heartbeats
	// (now()), one per worker — exactly the state at the instant a rollout
	// SIGTERMs one pod: the dispatcher had claimed the row and was heartbeating
	// it. (An INSERT here would collide on the UNIQUE(resource_id, task_type,
	// shard_id) the enqueue already occupies.)
	claimRow := func(resourceID interface{}, worker string) {
		t.Helper()
		ct, err := pool.Exec(ctx, `
			UPDATE work_queue SET broker_id = $2, heartbeat_at = now()
			WHERE resource_id = $1 AND task_type = 'reconcile'`, resourceID, worker)
		require.NoError(t, err)
		require.Equal(t, int64(1), ct.RowsAffected(), "expected one pending reconcile row to claim")
	}
	claimRow(dyingRoot.ID, dyingWorker)
	claimRow(survivorRoot.ID, survivorWorker)

	// The dying pod releases its claims on shutdown.
	freed, err := st.WorkQueueReleaseBroker(ctx, dyingWorker)
	require.NoError(t, err)
	require.Equal(t, int64(1), freed, "release frees exactly the dying pod's one claim")

	// The dying pod's row is now unclaimed (re-claimable immediately).
	var dyingWorkerID *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT broker_id FROM work_queue WHERE resource_id = $1`, dyingRoot.ID).Scan(&dyingWorkerID))
	require.Nil(t, dyingWorkerID, "dying pod's claim must be released (broker_id NULL)")

	// The survivor's claim is untouched — release is scoped to the caller's id.
	var survivorWorkerID *string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT broker_id FROM work_queue WHERE resource_id = $1`, survivorRoot.ID).Scan(&survivorWorkerID))
	require.NotNil(t, survivorWorkerID, "other pods' claims must be left alone")
	require.Equal(t, survivorWorker, *survivorWorkerID)

	// Releasing again is a clean no-op (idempotent: nothing left to free).
	freed, err = st.WorkQueueReleaseBroker(ctx, dyingWorker)
	require.NoError(t, err)
	require.Equal(t, int64(0), freed, "second release frees nothing")
}
