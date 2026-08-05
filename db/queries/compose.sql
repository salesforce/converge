-- Composer bulk operations.
--
-- BatchUpsertResources and BatchUpsertResourceDeps use multi-arg
-- unnest() that sqlc 1.31 doesn't parse — they live in
-- internal/dbq/compose_manual.go.

-- name: AnalyzeComposeTables :exec
-- Refresh planner stats on the two tables a post-compose
-- ScheduleEligible call joins against. After a composer-class
-- composer commits ~1M children + ~1M edges in one shot, autoanalyze
-- has not yet fired.
ANALYZE resources, resource_deps;

-- name: GetResourceRootID :one
-- Returns COALESCE(root_id, id) for a resource — the value composer
-- stamps on every inserted child so resources.root_id is set without
-- a per-row BEFORE INSERT trigger.
SELECT COALESCE(root_id, id) FROM resources WHERE id = $1;

-- name: ListExistingChildren :many
-- Children currently owned by parent_id, returned with the columns
-- composer needs to diff against the latest desired set.
-- deletion_requested_at lets the prune skip children already in the
-- soft-delete drain (so a recompose doesn't re-request deletion every
-- tick while finalizers run). frozen_until lets a re-emit detect a child
-- currently in the orphan-grace window (a FINITE frozen_until) so it CLEARS
-- the grace mark (re-adoption) — even when the re-emitted spec is unchanged.
-- A quarantined child (frozen_until = 'infinity') is NOT re-adopted by a
-- re-emit (only an explicit un-quarantine lifts it); the composer checks the
-- value to tell them apart.
SELECT r.id, r.kind, r.kind_version, m.name, r.spec, m.labels, r.is_ready, r.deletion_requested_at, r.frozen_until
FROM resources r JOIN resource_meta m ON m.id = r.id
WHERE r.owner_id = $1;

-- name: ApplyValueFlowsForDependents :exec
-- Bootstrap pass for value flows on freshly-composed dependents.
-- See apply_value_flows_for_dependents in 00001_schema.sql.
SELECT apply_value_flows_for_dependents($1::uuid[]);

-- name: StampComposedGen :execrows
-- Records that the composer ran for the given generation on this
-- resource. The worker uses composed_gen >= generation to skip the
-- compose reaction on re-pends that are waiting on descendants (rollup
-- gate). Must run inside the same tx that wrote children + edges.
--
-- FENCED — the LAST write of the compose tx, so the WHOLE compose commit is gated
-- on the claim STILL bearing the epoch this compose was dispatched under, at the
-- generation it composed. If a network-isolated / reaper-reassigned composer lost
-- the claim mid-compose (its epoch was bumped), this UPDATE matches 0 rows; the
-- caller sees rows_affected=0 and ROLLS BACK the entire tx — no stale-generation
-- child graph / composed_gen is committed over a newer generation's. work_id ($3)
-- = work_queue.id; claim_epoch ($4) = the epoch the StageTask carried; the
-- generation guard (q.generation = $2) is the belt-and-suspenders staleness axis.
-- One cached PK probe at commit — once per compose, OFF the per-child path.
UPDATE resources r SET composed_gen = $2
 WHERE r.id = $1
   AND EXISTS (
       SELECT 1 FROM work_queue q
        WHERE q.id = sqlc.arg('work_id')::uuid AND q.shard_id = shard_of($1)
          AND q.claim_epoch = sqlc.arg('claim_epoch')::bigint
          AND q.generation = $2
          -- manifest fence (belt-and-suspenders; see AppendOutbox): a compose run
          -- under an old manifest cannot stamp composed_gen if the row was
          -- re-enqueued under a new manifest version mid-flight. Inert when either
          -- side is 0 (the no-manifest path / a worker that never learned a version).
          AND (sqlc.arg('manifest_version')::bigint = 0
               OR q.manifest_version = 0
               OR q.manifest_version = sqlc.arg('manifest_version')::bigint)
   );

-- name: GetComposedGen :one
-- Reads the composer's current high-water mark for this resource.
-- Worker calls this before running compose; if composed_gen >=
-- generation, it skips the compose reaction (children + edges already
-- match the desired spec).
SELECT composed_gen FROM resources WHERE id = $1;
