package test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

// claimedRow is one seeded, claimed work_queue row's fencing identity — the tuple the
// batch writers key on (WorkQueueHeartbeat by (id, claim_epoch); the release paths by
// (broker_id, id)).
type claimedRow struct {
	id    uuid.UUID
	epoch int64
	shard int16
}

func isPGDeadlock(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "40P01"
}

// seedClaimedWorkQueue seeds n resources (each enqueues a reconcile row), claims every
// row under brokerID with a fresh heartbeat, and returns their (id, claim_epoch, shard)
// tuples — the state a live fleet is in when a DB blip/restart makes every pod reconnect
// and replay its batches at once.
func seedClaimedWorkQueue(t *testing.T, ctx context.Context, pool *pgxpool.Pool, st *store.Store, n int, brokerID string) []claimedRow {
	t.Helper()
	rows := make([]claimedRow, 0, n)
	for i := 0; i < n; i++ {
		root, err := st.ApplySpec(ctx, model.Kind(classicbom.Kind), fmt.Sprintf("dl-root-%03d", i),
			buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: fmt.Sprintf("dl-child-%03d", i)}), nil)
		require.NoError(t, err)
		var r claimedRow
		require.NoError(t, pool.QueryRow(ctx, `
			UPDATE work_queue SET broker_id = $2, heartbeat_at = now()
			WHERE resource_id = $1 AND task_type = 'reconcile'
			RETURNING id, claim_epoch, shard_id`, root.ID, brokerID).Scan(&r.id, &r.epoch, &r.shard))
		rows = append(rows, r)
	}
	return rows
}

// TestWorkQueueBatchWritersNoDeadlock is the regression guard for the ordered
// SKIP-LOCKED pre-lock on the work_queue batch writers (WorkQueueHeartbeat +
// WorkQueueReleaseTasks + WorkQueueReleaseBroker). Before the fix these were bare,
// UNORDERED multi-row writes; a full Postgres restart (persistent data, every pod's pool
// breaks + reconnects, then all replay their batches against a DB also running the
// drainer/reaper) made two of them grab the same rows' locks in opposite order and
// 40P01-deadlock. The fix pre-locks each batch's rows FOR NO KEY UPDATE ... SKIP LOCKED
// ORDER BY (shard_id, id), so lock acquisition follows one global order and no cycle can
// form; a row another writer holds is skipped this tick (best-effort + idempotent).
//
// This drives the real contention: a pool of claimed rows, then many goroutines hammering
// ALL the batch writers plus the reaper (reap_stale_work) over overlapping, differently-
// ORDERED subsets for a fixed window — the exact "storm on a recovering DB" that surfaced
// the bug. It is a VALIDATED guard, not a false-green: reverting any of the three writers
// to its bare unordered form makes this FAIL with that writer's 40P01 within ~10s.
//
// Assertions: no writer ever returns 40P01, and no row is lost under the churn.
func TestWorkQueueBatchWritersNoDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	reg := newTReg()
	reg.Add(demoruntime.ClassicBOM(0))
	reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
	require.NoError(t, reg.seed(ctx, pool))
	st := store.New(pool)

	const rowCount = 120
	const brokerID = "dl-broker-host-000-bbbbbbbb"
	rows := seedClaimedWorkQueue(t, ctx, pool, st, rowCount, brokerID)
	allIDs := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		allIDs[i] = r.id
	}

	var dlOnce sync.Once
	var dlErr error
	noteDeadlock := func(op string, err error) {
		if isPGDeadlock(err) {
			dlOnce.Do(func() { dlErr = fmt.Errorf("%s deadlocked (40P01): %w", op, err) })
		}
	}
	// rotate(k) returns the rows rotated by k so each goroutine presents its batch in a
	// different order — the precondition for a cycle if a SKIP-LOCKED pre-lock were removed.
	rotate := func(k int) []claimedRow {
		out := make([]claimedRow, len(rows))
		for i := range rows {
			out[i] = rows[(i+k)%len(rows)]
		}
		return out
	}

	runCtx, runCancel := context.WithTimeout(ctx, 8*time.Second)
	defer runCancel()
	var wg sync.WaitGroup

	// Heartbeat attesters: each attests the whole set from a different offset.
	const heartbeaters = 12
	for g := 0; g < heartbeaters; g++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			for runCtx.Err() == nil {
				view := rotate(k)
				ids := make([]uuid.UUID, len(view))
				eps := make([]int64, len(view))
				for i, r := range view {
					ids[i], eps[i] = r.id, r.epoch
				}
				noteDeadlock("heartbeat", st.WorkQueueHeartbeat(runCtx, ids, eps, 0, 255))
			}
		}(g * 7)
	}
	// Release-tasks: re-claim (so there's always a target) then release a rotated subset.
	const releasers = 4
	for g := 0; g < releasers; g++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			for runCtx.Err() == nil {
				_, _ = pool.Exec(runCtx, `UPDATE work_queue SET broker_id = $1 WHERE broker_id IS NULL`, brokerID)
				view := rotate(k)
				ids := make([]uuid.UUID, 0, len(view)/2)
				for i := 0; i < len(view); i += 2 {
					ids = append(ids, view[i].id)
				}
				_, err := st.WorkQueueReleaseTasks(runCtx, brokerID, ids, 0, 255)
				noteDeadlock("release-tasks", err)
			}
		}(g*13 + 3)
	}
	// Release-broker: the whole-pod shutdown release (the writer that reproduced the
	// deadlock fastest against the buggy form).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for runCtx.Err() == nil {
			_, _ = pool.Exec(runCtx, `UPDATE work_queue SET broker_id = $1 WHERE broker_id IS NULL`, brokerID)
			_, err := st.WorkQueueReleaseBroker(runCtx, brokerID)
			noteDeadlock("release-broker", err)
		}
	}()
	// The reaper's reap_stale_work — the OTHER real work_queue writer the batch writers
	// can cycle with. staleAfter=0 so it considers everything reclaimable (max lock overlap).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for runCtx.Err() == nil {
			_, err := st.ReapStaleWork(runCtx, 0, 500, nil)
			noteDeadlock("reap-stale-work", err)
		}
	}()
	wg.Wait()

	require.NoError(t, dlErr, "no work_queue batch writer may 40P01 under shuffled-order concurrency (ordered SKIP-LOCKED pre-lock)")

	var present int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM work_queue WHERE id = ANY($1::uuid[])`, allIDs).Scan(&present))
	require.Equal(t, rowCount, present, "no row lost under the concurrency")
}

// lifecycleRow is one seeded lifecycle_outbox delivery's key — the full per-binding
// tuple the reactor writers key on (claim/ack by the PK, heartbeat/reap by broker_id +
// shard range). shard is carried so a caller can present rows in a shuffled order.
type lifecycleRow struct {
	resourceID uuid.UUID
	transition string
	generation int64
	binding    string
	shard      int16
}

// seedClaimedLifecycleOutbox creates n resources and, for each, inserts a
// lifecycle_outbox delivery already CLAIMED under brokerID with a fresh heartbeat —
// the state a fleet is in when a DB blip makes every reactor dispatcher reconnect and
// replay its heartbeat/ack batches against a DB also running the reaper. Rows are
// inserted directly (not via the emit path) so the seed is deterministic and doesn't
// depend on a binding fan-out. Returns their key tuples.
func seedClaimedLifecycleOutbox(t *testing.T, ctx context.Context, pool *pgxpool.Pool, st *store.Store, n int, brokerID, binding string) []lifecycleRow {
	t.Helper()
	rows := make([]lifecycleRow, 0, n)
	for i := 0; i < n; i++ {
		root, err := st.ApplySpec(ctx, model.Kind(classicbom.Kind), fmt.Sprintf("rx-root-%03d", i),
			buildSpec(controllableChildSpec{Kind: string(account.Kind), Name: fmt.Sprintf("rx-child-%03d", i)}), nil)
		require.NoError(t, err)
		r := lifecycleRow{resourceID: root.ID, transition: "synced", generation: 1, binding: binding}
		require.NoError(t, pool.QueryRow(ctx, `
			INSERT INTO lifecycle_outbox (resource_id, kind, transition, generation, binding_name, shard_id, broker_id, heartbeat_at, claim_epoch)
			VALUES ($1, $2, $3, $4, $5, shard_of($1), $6, now(), 2)
			RETURNING shard_id`,
			r.resourceID, string(classicbom.Kind), r.transition, r.generation, r.binding, brokerID).Scan(&r.shard))
		rows = append(rows, r)
	}
	return rows
}

// TestReactorOutboxWritersNoDeadlock is the reactor-path twin of
// TestWorkQueueBatchWritersNoDeadlock: it guards the ordered SKIP-LOCKED pre-lock on
// the lifecycle_outbox batch writers. The reactor spine is TWINNED code (its own table,
// claim, ack, reap — not the reconcile path's), and HeartbeatReactorClaims used to be a
// bare, UNORDERED multi-row UPDATE (broker_id + shard range) — the exact hazard the
// work_queue heartbeat had. It can cycle with reap_stale_lifecycle, which pre-locks its
// rows ORDER BY heartbeat_at (a DIFFERENT order), so under a recovering-DB storm two
// writers grab the same rows' locks in opposite order and 40P01-deadlock. The fix gives
// the heartbeat the same `pick` CTE pre-lock (FOR NO KEY UPDATE ... SKIP LOCKED ordered
// by the lifecycle_outbox key) as work_queue; the claim + reap already pre-lock.
//
// This drives the real contention: a pool of claimed deliveries, then many goroutines
// hammering HeartbeatReactorClaims + ClaimReactorDeliveries + AckReactorDelivery +
// ReapStaleLifecycle over overlapping subsets for a fixed window. It is a VALIDATED
// guard: reverting HeartbeatReactorClaims to its bare unordered form makes this FAIL
// with a heartbeat 40P01. Assertion: no reactor writer ever returns 40P01.
func TestReactorOutboxWritersNoDeadlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	reg := newTReg()
	reg.Add(demoruntime.ClassicBOM(0))
	reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
	require.NoError(t, reg.seed(ctx, pool))
	st := store.New(pool)

	const rowCount = 120
	const brokerID = "rx-broker-host-000-bbbbbbbb"
	const binding = "rx-deadlock-binding"
	rows := seedClaimedLifecycleOutbox(t, ctx, pool, st, rowCount, brokerID, binding)

	var dlOnce sync.Once
	var dlErr error
	noteDeadlock := func(op string, err error) {
		if isPGDeadlock(err) {
			dlOnce.Do(func() { dlErr = fmt.Errorf("%s deadlocked (40P01): %w", op, err) })
		}
	}
	// reclaimUnclaimed re-stamps only the rows a claimer/reaper just RELEASED (broker_id IS
	// NULL) back to brokerID, so heartbeat/ack keep a live target as the pool churns. It
	// re-claims by the `broker_id IS NULL` predicate (NOT an explicit id array) so it locks
	// a set DISJOINT from the rows the heartbeat is refreshing — exactly the discipline the
	// work_queue re-claim above uses. A test writer that instead re-stamped ALL rows by an
	// id array would lock in array order and manufacture a cross-writer cycle that no
	// PRODUCTION writer has (nothing in the reactor spine updates lifecycle_outbox by an
	// arbitrary id list) — a false positive. This keeps the test honest to the real writers.
	reclaimUnclaimed := func() {
		_, _ = pool.Exec(ctx, `UPDATE lifecycle_outbox SET broker_id = $1, heartbeat_at = now()
			WHERE broker_id IS NULL`, brokerID)
	}

	runCtx, runCancel := context.WithTimeout(ctx, 8*time.Second)
	defer runCancel()
	var wg sync.WaitGroup

	// Heartbeat attesters: each refreshes THIS broker's whole claimed set — the writer
	// that carried the bug. Full range so it scans every partition, maximizing overlap.
	// leaseKeys are the (key, claim_epoch) tuples a dispatcher "holds" — the seeded rows at
	// their claim epoch (2, the seed value). A claimer that re-issues a row bumps its epoch,
	// so that key stops matching and the heartbeat skips it (the epoch-pin property under
	// test): the heartbeat must contend on the LIVE rows without chasing a re-claimed row's
	// updated tuple version — the exact 40P01 the epoch pin prevents.
	leaseKeys := make([]store.ReactorLeaseKey, len(rows))
	for i, r := range rows {
		leaseKeys[i] = store.ReactorLeaseKey{ResourceID: r.resourceID, Transition: r.transition, Generation: r.generation, BindingName: r.binding, ClaimEpoch: 2}
	}
	const heartbeaters = 12
	for g := 0; g < heartbeaters; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for runCtx.Err() == nil {
				noteDeadlock("reactor-heartbeat", st.HeartbeatReactorClaims(runCtx, leaseKeys, []int16{0, 255}))
			}
		}()
	}
	// Claimers: release a subset then re-claim via the real claim (its own SKIP-LOCKED
	// pick), contending on the same rows the heartbeat refreshes. The release is by the
	// `broker_id IS NOT NULL` predicate over the reaper's own path — no id-array update.
	const claimers = 3
	for g := 0; g < claimers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for runCtx.Err() == nil {
				// Release everything this broker holds (predicate, not an id array), then let
				// the real claim re-take it — the production release→claim contention.
				_, _ = pool.Exec(runCtx, `UPDATE lifecycle_outbox SET broker_id = NULL WHERE broker_id = $1`, brokerID)
				_, err := st.ClaimReactorDeliveries(runCtx, brokerID, rowCount, []int16{0, 255})
				noteDeadlock("reactor-claim", err)
			}
		}()
	}
	// Ackers: delete a rotated subset by its full key + the seeded epoch, then re-seed the
	// pool so the set stays full for the other writers (ack is a DELETE).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for runCtx.Err() == nil {
			reclaimUnclaimed() // keep rows claimed so the ack's epoch/broker target exists
			for i := 0; i < len(rows); i += 3 {
				r := rows[i]
				// epoch 2 is what the seed stamped; a claimer may have bumped it, in which
				// case the fenced ack simply matches 0 rows (correct) — never a deadlock.
				noteDeadlock("reactor-ack", st.AckReactorDelivery(runCtx, r.resourceID, r.transition, r.generation, r.binding, 2))
			}
			// Re-seed any acked-away rows so the set stays full for the other writers.
			for _, r := range rows {
				_, _ = pool.Exec(runCtx, `INSERT INTO lifecycle_outbox (resource_id, kind, transition, generation, binding_name, shard_id, broker_id, heartbeat_at, claim_epoch)
					VALUES ($1, $2, $3, $4, $5, shard_of($1), $6, now(), 2) ON CONFLICT DO NOTHING`,
					r.resourceID, string(classicbom.Kind), r.transition, r.generation, r.binding, brokerID)
			}
		}
	}()
	// The reaper's reap_stale_lifecycle — the OTHER real lifecycle_outbox writer the
	// heartbeat can cycle with: it pre-locks its rows ORDER BY heartbeat_at (a DIFFERENT
	// order than the heartbeat's key order), so an unordered heartbeat cycles with it.
	// staleAfter=0 so it considers everything reclaimable, over the FULL shard range
	// (0..255) so it contends on the same rows the heartbeat refreshes (max overlap).
	const allShards = 256
	fullRange := make([]int16, allShards)
	for i := range fullRange {
		fullRange[i] = int16(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for runCtx.Err() == nil {
			noteDeadlock("reap-stale-lifecycle", st.ReapStaleLifecycle(runCtx, 0, 500, fullRange))
		}
	}()
	wg.Wait()

	require.NoError(t, dlErr, "no lifecycle_outbox reactor writer may 40P01 under shuffled-order concurrency (ordered SKIP-LOCKED pre-lock)")
}

// lifecycleIDs projects the resource_ids of a lifecycle row set for a bulk re-stamp.
func lifecycleIDs(rows []lifecycleRow) []uuid.UUID {
	ids := make([]uuid.UUID, len(rows))
	for i, r := range rows {
		ids[i] = r.resourceID
	}
	return ids
}
