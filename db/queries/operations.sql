-- Subresource operation queries: durable record of verb invocations
-- (e.g. a kind-specific imperative verb). The verb runs through work_queue
-- with task_type='operate' and op_id pointing at a row here.

-- name: CreateOperation :one
-- Inserts a pending operation row. The API handler then enqueues a
-- work_queue row (task_type='operate', op_id pointing here) so the
-- next loop tick dispatches the verb.
INSERT INTO resource_operations (resource_id, verb, input, requested_by)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: EnqueueOperationWork :execrows
-- Inserts the work_queue row that runs the operation. Spec is the
-- resource spec snapshot (inline) at request time; the operator sees
-- stable input even if reconciliation work updates the spec mid-flight.
-- ON CONFLICT no-ops if an operate task is already pending for this resource.
-- Returns the affected-row count so the caller can distinguish a real enqueue
-- (1) from a no-op — a frozen resource (0 selected rows) or a duplicate operate
-- task already queued (0 by ON CONFLICT) — and fail closed instead of recording
-- an operation with no backing task.
INSERT INTO work_queue (resource_id, task_type, op_id, kind, kind_version, generation, spec, provider_config, provider_bundle, manifest_version, shard_id)
SELECT r.id, 'operate'::task_type, sqlc.arg(op_id), r.kind, r.kind_version, r.generation, r.spec,
       (SELECT pc.spec FROM providerconfigs pc WHERE pc.id = r.provider_config_id),  -- clone custom config spec
       (SELECT pc.data FROM providerconfigs pc WHERE pc.id = r.provider_config_id),  -- clone custom config bundle
       r.manifest_version,  -- fence the result write on the manifest the verb was enqueued under (like every other path)
       shard_of(r.id)
FROM resources r WHERE r.id = sqlc.arg(resource_id)
  -- FROZEN states bar operate (subresource-verb) work too: an operator who set a
  -- resource aside (quarantined) or a child pending teardown (orphaned) must not
  -- have a verb run against the live provider. The freeze is enqueue-side (the
  -- claim path doesn't see resource state), so guard here exactly like the 7
  -- reconcile schedulers. A frozen resource selects 0 rows → no operate enqueued;
  -- the API rejects up front (handlers_ops) so the operation row isn't left
  -- stuck pending. delete tasks are deliberately NOT gated (teardown must proceed).
  AND r.frozen_until IS NULL          -- orphaned OR quarantined = frozen
ON CONFLICT (resource_id, task_type, shard_id) DO NOTHING;

-- name: GetOperation :one
SELECT * FROM resource_operations WHERE id = $1;

-- name: ListOperationsByResource :many
-- Newest-first listing, optionally filtered by state. Used by the UI
-- Operations tab.
SELECT * FROM resource_operations
WHERE resource_id = $1
  AND (sqlc.narg('states')::text[] IS NULL OR state = ANY(sqlc.narg('states')::text[]))
ORDER BY requested_at DESC, id DESC
LIMIT sqlc.arg('lim')::int;

-- name: SetOperationRunning :exec
-- The dispatcher calls this when it claims an operate task. Best-effort; the
-- drainer-side state writes (succeeded/failed) are authoritative.
UPDATE resource_operations
SET state = 'running',
    attempts = attempts + 1
WHERE id = $1 AND state IN ('pending', 'running');
