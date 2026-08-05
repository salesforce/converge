-- Resource event queries. The K8s "Events" API analogue: append-only
-- audit log, queried by resource id (newest first) for the UI's
-- Events tab.

-- name: AppendResourceEvent :exec
-- created_at uses clock_timestamp() (REAL wall-clock at INSERT), not the column's
-- DEFAULT now() (= TRANSACTION-START time). An event emitted inside a long tx —
-- e.g. compose-succeeded, written mid-way through a 1M-child compose tx that runs
-- ~1 min before it commits — would otherwise be stamped at the tx's start and only
-- become visible on commit: it appears ~1 min late AND with a timestamp ~1 min
-- behind wall-clock (the UI's "spec-composed shows up a minute later with the wrong
-- time" bug). clock_timestamp() records when the row was actually written.
INSERT INTO resource_events (resource_id, type, actor, reason, message, detail, created_at)
VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp());

-- name: ListResourceEvents :many
-- Newest events first, paginated via (created_at, id) cursor.
-- Empty cursor (zero time / zero id) means "from the top."
SELECT id, resource_id, type, actor, reason, message, detail, created_at
FROM resource_events
WHERE resource_id = $1
  AND (
        sqlc.arg(cursor_at)::timestamptz = '0001-01-01 00:00:00+00'::timestamptz
     OR (created_at, id) < (sqlc.arg(cursor_at)::timestamptz, sqlc.arg(cursor_id)::bigint)
  )
ORDER BY created_at DESC, id DESC
LIMIT sqlc.arg(lim)::int;
