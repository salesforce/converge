package dbq

import (
	"context"

	"github.com/google/uuid"
)

// WorkQueueMarkWorkerBatch stamps work_queue.worker_id (the dumb worker that ran the
// stage) for a BATCH of dispatched tasks in a single UPDATE — the coalesced
// UI-attribution write ("running on <worker>"). Hand-written because sqlc's analyzer
// can't infer the multi-column unnest($2::uuid[], $3::text[], $4::smallint[]) signature
// (see the note in db/queries/work_queue.sql).
//
// ATTRIBUTION ONLY: each row is scoped to brokerID (this broker's own claim /
// broker_id), so it can only touch a row this broker still owns; a reclaimed/released
// row is a harmless no-op. Never fenced on, never read by the reaper. The three
// slices are positional (ids[i] ↔ workerIDs[i] ↔ shards[i]); callers pass equal
// lengths. Empty input is a no-op.
//
// A `pick` CTE locks the target rows with FOR NO KEY UPDATE SKIP LOCKED before the
// UPDATE: this best-effort UI write must never contend with drain_outbox_batch's
// work_queue DELETE, which locks the same rows in its own scan order. SKIP LOCKED
// means a row the drainer currently holds is simply skipped (its attribution lands
// on a later tick, or not at all — display-only), so the two can never form a
// wait cycle. Ordering the pick by (shard_id, id) also matches work_queue's
// (id, shard_id) partitioned key so concurrent attribution batches agree on a
// lock order among themselves.
func (q *Queries) WorkQueueMarkWorkerBatch(ctx context.Context, brokerID string, ids []uuid.UUID, workerIDs []string, shards []int16) error {
	if len(ids) == 0 {
		return nil
	}
	const sql = `WITH v AS (
    SELECT id, worker_id, shard_id
    FROM unnest($2::uuid[], $3::text[], $4::smallint[]) AS t(id, worker_id, shard_id)
), pick AS (
    SELECT q.id, q.shard_id
    FROM work_queue q JOIN v ON q.id = v.id AND q.shard_id = v.shard_id
    WHERE q.broker_id = $1
    ORDER BY q.shard_id, q.id
    FOR NO KEY UPDATE OF q SKIP LOCKED
)
UPDATE work_queue q
   SET worker_id = v.worker_id
  FROM v JOIN pick p ON p.id = v.id AND p.shard_id = v.shard_id
 WHERE q.id = p.id AND q.shard_id = p.shard_id`
	_, err := q.db.Exec(ctx, sql, brokerID, ids, workerIDs, shards)
	return err
}
