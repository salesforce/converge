package dbq

import (
	"context"

	"github.com/google/uuid"
)

// WorkQueueHeartbeat refreshes work_queue.heartbeat_at for the (id, claim_epoch)
// pairs a live worker ATTESTED within the liveness window — the broker relays a
// worker's WorkHeartbeat frame here, so heartbeat_at means "a live worker is
// executing this task", not merely "the broker process is up". A task whose worker
// went silent or wedged is absent from the attested set, so its row stops being
// refreshed and the reaper reclaims it. Hand-written because sqlc's analyzer can't
// infer the multi-column unnest($1::uuid[], $2::bigint[]) signature (see the note
// in db/queries/work_queue.sql).
//
// FENCED on claim_epoch: the epoch both scopes the refresh to THIS claim and
// rejects a stale extension — a lease that was reaped and re-issued (which bumped
// the epoch) can no longer be kept alive by its prior holder. The two slices are
// positional (ids[i] ↔ epochs[i]); callers pass equal lengths. shard_lo/shard_hi
// bound the range so the UPDATE prunes to the worker's partition(s) instead of all
// 16 (range predicate → runtime pruning; see WorkQueueTakeBatch). Empty input is a
// no-op.
//
// DEADLOCK-SAFE: the refresh pre-locks its rows in a `pick` CTE with FOR NO KEY
// UPDATE ... SKIP LOCKED ordered (shard_id, id), so this best-effort liveness UPDATE
// can NEVER form a 40P01 wait cycle with another work_queue writer (the drainer's
// DELETE, a claim, another broker's heartbeat replaying a differently-ordered unnest)
// — a hazard that surfaces when the DB comes back and many brokers slam work_queue at
// once. A row locked by another writer is simply SKIPPED this tick; the next
// attestation (~5s) refreshes it, well within the reaper's StaleAfter, so a skipped
// refresh is harmless. Same fix as WorkQueueMarkWorkerBatch's attribution UPDATE.
func (q *Queries) WorkQueueHeartbeat(ctx context.Context, ids []uuid.UUID, epochs []int64, lo, hi int16) error {
	if len(ids) == 0 {
		return nil
	}
	const sql = `WITH a AS (
    SELECT id, claim_epoch FROM unnest($1::uuid[], $2::bigint[]) AS t(id, claim_epoch)
), pick AS (
    SELECT q.id, q.shard_id FROM work_queue q JOIN a ON q.id = a.id AND q.claim_epoch = a.claim_epoch
    WHERE q.shard_id BETWEEN $3::smallint AND $4::smallint
    ORDER BY q.shard_id, q.id FOR NO KEY UPDATE OF q SKIP LOCKED
)
UPDATE work_queue q SET heartbeat_at = now()
  FROM pick p WHERE q.id = p.id AND q.shard_id = p.shard_id`
	_, err := q.db.Exec(ctx, sql, ids, epochs, lo, hi)
	return err
}
