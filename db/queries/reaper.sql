-- Go-side wrappers for the SQL functions called by the reaper.

-- name: ReapStaleWork :one
-- Frees task rows whose worker stopped heartbeating so the next claim
-- picks them up. No condition writes — failures surface via
-- resource_events + work_outbox.error_message. shard_lo/shard_hi bound
-- the reaper's contiguous shard range (range → partition pruning on the
-- RANGE-partitioned work_queue).
SELECT reap_stale_work($1::int, $2::int, $3::smallint, $4::smallint)::int AS reaped;

-- name: RequeueFailedAndPending :one
-- Re-schedules pending/failed/working-orphan rows whose dependencies
-- are now ready and that have aged past retry_seconds.
SELECT requeue_failed_and_pending($1::int, $2::int, $3::smallint[])::int AS rescheduled;

-- name: RecountInflight :exec
-- Self-healing per-kind in-flight recount over this reaper's contiguous
-- shard range. Reclaims worker hint partials in [lo,hi] and writes one
-- authoritative per-range count. Range-pruned, off the hot claim path.
SELECT recount_inflight($1::smallint, $2::smallint);

-- name: DeleteUnclaimedWork :one
-- Hard-deletes work_queue rows that have been UNCLAIMED for longer than
-- delete_seconds — abandoned tasks no worker ever picked up (kind has no
-- worker / decommissioned). Garbage collection, not recovery: if a worker
-- for the kind returns, requeue_failed_and_pending re-enqueues a fresh row.
-- shard_lo/shard_hi bound the reaper's contiguous range (partition pruning).
SELECT delete_unclaimed_work($1::int, $2::int, $3::smallint, $4::smallint)::int AS deleted;

-- name: SweepExpiredOrphans :one
-- Tears down composer-dropped children whose orphan-grace window has elapsed
-- without a re-emit (frozen_until < now(), not yet draining): finalizer kinds →
-- soft-delete (request_resource_deletion semantics, set-based), leaf kinds →
-- hard delete. Per-kind finalizer read from kind_config in-DB (the reaper has no
-- Go registry). Grace is baked into frozen_until by the composer, so no grace
-- arg. A quarantined row (frozen_until = 'infinity') is never < now(), so it is
-- never swept. shard_lo/shard_hi bound the reaper's range (partition pruning on
-- the delete-task work_queue inserts; the resources scan rides idx_resources_frozen_sweep).
SELECT sweep_expired_orphans($1::int, $2::smallint, $3::smallint)::int AS swept;

-- name: SweepDeletable :one
-- Level-triggered backstop for reverse-dependency cascade delete: hard-deletes any
-- deletion_requested row whose finalizers are empty (teardown done, or none) AND
-- that has no remaining OWNED child AND no remaining DEPENDENT (resource_deps). The
-- marked teardown tree collapses inward one layer per reaper tick — removing a leaf
-- unblocks its parent for the next tick. STRICT: a node still holding a finalizer
-- keeps blocking its parent (no unblock). shard_lo/shard_hi bound the reaper range;
-- the scan rides idx_resources_deleting and the function carries its own idle gate.
SELECT sweep_deletable($1::int, $2::smallint, $3::smallint)::int AS removed;
