-- Lifecycle reactor spine: claim/ack lifecycle_outbox deliveries, reap stale
-- claims, and CRUD the reactor_bindings rule table. See db/migrations for the
-- table + function definitions (claim_reactor_deliveries / ack_reactor_delivery
-- / reap_stale_lifecycle live in plpgsql so the claim's FOR UPDATE SKIP LOCKED
-- + in-DB binding JOIN never round-trip).

-- NOTE: ClaimReactorDeliveries is NOT here — sqlc can't infer column types from
-- a plpgsql RETURNS TABLE function, so it lives as a hand-written query in
-- internal/store/reactor.go.

-- name: AckReactorDelivery :exec
-- A delivery succeeded → delete its PER-BINDING outbox row so it is never
-- re-delivered. binding_name is part of the key so acking one binding's delivery
-- never touches a sibling binding's row for the same transition. FENCED on
-- claim_epoch ($5, the epoch the delivery was claimed under): a dispatcher whose
-- claim was reaped and re-issued carries a stale epoch, so its ack matches 0 rows
-- and cannot delete the live re-delivery (at-least-once preserved).
SELECT ack_reactor_delivery($1::uuid, $2::text, $3::bigint, $4::text, $5::bigint);

-- name: ReapStaleLifecycle :exec
-- Free lifecycle_outbox rows whose dispatcher stopped heartbeating, so the next
-- claim re-delivers them (at-least-once crash recovery). Clone of ReapStaleWork.
SELECT reap_stale_lifecycle($1::int, $2::int, $3::smallint, $4::smallint);

-- NOTE: HeartbeatReactorClaims is NOT here — like WorkQueueHeartbeat, its multi-column
-- unnest($1::uuid[], $2::text[], …) signature can't be inferred by sqlc's analyzer, so it
-- lives as a hand-written query in internal/dbq/reactor_heartbeat.go.

-- name: EmitLifecycleCreated :exec
-- Emit a 'created' delivery PER MATCHING enabled 'created' binding for a
-- freshly-applied resource (fan-out + label_match at emit, like the cascade).
-- Called inside the apply tx so it commits with the resource. ON CONFLICT DO
-- NOTHING dedups a re-apply at the same generation, per binding. The DIRECT
-- pg_notify('lifecycle_ready') (NOT notify_gated — the work_ready scar) wakes
-- the dispatcher; it fires per-apply, low rate, commit-atomic, and only when a
-- row was actually inserted (the ins CTE gate). Apply is NOT the 1M hot path
-- (it is one root submission), so the binding join here is free; when no
-- 'created' binding exists the join yields no rows → no INSERT, no NOTIFY.
-- watch_kind_version ($4) scopes a binding to ONE version of the watched kind
-- (NULL on the binding = all versions); AND it against the applied resource's
-- kind_version so a 'created' row exists only for a binding that matches the
-- version too.
WITH ins AS (
    INSERT INTO lifecycle_outbox (resource_id, kind, transition, generation, binding_name, shard_id)
    SELECT $1::uuid, $2::text, 'created', $3::bigint, b.name, shard_of($1::uuid)
      FROM reactor_bindings b
      LEFT JOIN resource_meta m ON m.id = $1::uuid
     WHERE b.enabled AND b.watch_kind = $2::text AND b.transition = 'created'
       AND (b.watch_kind_version IS NULL OR b.watch_kind_version = sqlc.arg('kind_version')::smallint)
       AND (b.label_match = '{}'::jsonb OR m.labels @> b.label_match)
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT pg_notify('lifecycle_ready', '') FROM ins;

-- name: UpsertReactorBinding :exec
-- Create or replace a subscription (runtime-editable; picked up on the
-- dispatcher's next claim — no Go-side cache, no restart).
-- watch_kind_version + reactor_version are BOTH nullable (sqlc.narg):
--   watch_kind_version — scope the subscription to ONE version of the WATCHED
--     kind (NULL = all versions).
--   reactor_version — pin which version of the REACTOR runs (NULL = highest
--     published, resolved at claim time).
INSERT INTO reactor_bindings (name, watch_kind, watch_kind_version, transition, label_match, reactor, reactor_version, enabled)
VALUES ($1, $2, sqlc.narg('watch_kind_version')::smallint, $3, sqlc.arg('label_match')::jsonb, sqlc.arg('reactor')::text, sqlc.narg('reactor_version')::smallint, sqlc.arg('enabled')::boolean)
ON CONFLICT (name) DO UPDATE SET
    watch_kind = EXCLUDED.watch_kind,
    watch_kind_version = EXCLUDED.watch_kind_version,
    transition = EXCLUDED.transition,
    label_match = EXCLUDED.label_match,
    reactor = EXCLUDED.reactor,
    reactor_version = EXCLUDED.reactor_version,
    enabled = EXCLUDED.enabled;

-- name: ListReactorBindings :many
SELECT name, watch_kind, watch_kind_version, transition, label_match, reactor, reactor_version, enabled, created_at
FROM reactor_bindings
ORDER BY name;

-- name: DeleteReactorBinding :one
-- Remove a binding by name. RETURNING name so the store reports whether a row
-- went, so the API can 404 a missing name (matching DeleteProviderConfig).
DELETE FROM reactor_bindings WHERE name = $1
RETURNING name;
