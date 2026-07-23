package dbq

import (
	"context"

	"github.com/google/uuid"
)

// HeartbeatReactorClaims refreshes lifecycle_outbox.heartbeat_at for the SPECIFIC
// in-flight deliveries a reactor dispatcher holds — one row per positional tuple
// (ids[i], transitions[i], generations[i], bindings[i]) fenced by epochs[i], the
// claim_epoch that delivery was claimed under. Hand-written for the SAME reason as
// WorkQueueHeartbeat: sqlc's analyzer can't infer the multi-column
// unnest($1::uuid[], $2::text[], …) signature.
//
// FENCED on claim_epoch, and that fence is what makes it DEADLOCK-SAFE. An earlier form
// refreshed by a bare `broker_id = me` predicate; under concurrent claim/reap (which
// mutate the same rows and bump claim_epoch) that form 40P01-deadlocked — two writers
// each held a lock while the other's outer UPDATE chased the row's just-updated tuple
// version ("while locking updated version of tuple"). Pinning claim_epoch here means a
// row a concurrent claim/reap RE-ISSUED (bumped epoch) no longer matches the join, so
// the heartbeat SKIPS it instead of waiting on the re-claimer — no cycle can form. This
// mirrors WorkQueueHeartbeat's (id, claim_epoch) contract exactly.
//
// The `pick` CTE also pre-locks FOR NO KEY UPDATE ... SKIP LOCKED in one global key
// order (belt-and-suspenders against a cycle with reap_stale_lifecycle, which locks
// ORDER BY heartbeat_at). shard_lo/shard_hi bound the pod's partitions (range prune).
// Empty input is a no-op. A skipped row is harmless: the delivery either finished
// (epoch moot) or the next heartbeat refreshes it, well within the reaper's stale window.
func (q *Queries) HeartbeatReactorClaims(ctx context.Context, ids []uuid.UUID, transitions []string, generations []int64, bindings []string, epochs []int64, lo, hi int16) error {
	if len(ids) == 0 {
		return nil
	}
	const sql = `WITH held AS (
    SELECT * FROM unnest($1::uuid[], $2::text[], $3::bigint[], $4::text[], $5::bigint[])
        AS t(resource_id, transition, generation, binding_name, claim_epoch)
), pick AS (
    SELECT lo.resource_id, lo.transition, lo.generation, lo.binding_name, lo.shard_id
      FROM lifecycle_outbox lo
      JOIN held h
        ON lo.resource_id = h.resource_id AND lo.transition = h.transition
       AND lo.generation = h.generation AND lo.binding_name = h.binding_name
       AND lo.claim_epoch = h.claim_epoch
     WHERE lo.shard_id BETWEEN $6::smallint AND $7::smallint
     ORDER BY lo.shard_id, lo.resource_id, lo.transition, lo.generation, lo.binding_name
       FOR NO KEY UPDATE OF lo SKIP LOCKED
)
UPDATE lifecycle_outbox lo SET heartbeat_at = now()
  FROM pick p
 WHERE lo.resource_id = p.resource_id AND lo.transition = p.transition
   AND lo.generation = p.generation AND lo.binding_name = p.binding_name
   AND lo.shard_id = p.shard_id`
	_, err := q.db.Exec(ctx, sql, ids, transitions, generations, bindings, epochs, lo, hi)
	return err
}
