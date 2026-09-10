-- name: UpsertKindManifest :exec
-- Operator-applied (CRD-style) kind definition, per (kind, kind_version). Inserts or
-- replaces the manifest row for THIS kind_version; the AFTER trigger
-- sync_kind_config_from_manifest derives the (kind, kind_version) kind_config policy row
-- and scopes its resource restamp to kind_version. Publishing a NEW kind_version inserts a new
-- row and touches no existing-kind_version resource. (Reactor wiring is NOT projected
-- from the manifest — it's a separate editable reactor_bindings subscription.)
-- validate_kind_manifest() runs in the config trigger and RAISEs on an illegal
-- reaction set, rolling back the apply. manifest_version is the caller-computed
-- content hash (reactions+schemas); the publish-time (kind, kind_version)→content-hash
-- invariant is enforced app-side (handlers_kind_manifest) BEFORE this upsert.
INSERT INTO kind_manifest (
    kind, kind_version, description, spec_schema, status_schema, config_schema,
    reactions, finalizer_name, max_inflight, task_deadline_secs, resync_interval_secs,
    resync_recomposes, orphan_grace_secs, max_transient_attempts, retired, schema_hash, manifest_version, updated_at
) VALUES (
    $1, sqlc.arg('kind_version')::int, $2,
    sqlc.narg('spec_schema')::jsonb, sqlc.narg('status_schema')::jsonb, sqlc.narg('config_schema')::jsonb,
    sqlc.arg('reactions')::jsonb, sqlc.narg('finalizer_name')::text,
    sqlc.arg('max_inflight')::int, sqlc.arg('task_deadline_secs')::int, sqlc.arg('resync_interval_secs')::int,
    sqlc.arg('resync_recomposes')::boolean, sqlc.arg('orphan_grace_secs')::int, sqlc.arg('max_transient_attempts')::int,
    sqlc.arg('retired')::boolean, sqlc.narg('schema_hash')::text, sqlc.arg('manifest_version')::bigint, now()
)
ON CONFLICT (kind, kind_version) DO UPDATE SET
    description          = EXCLUDED.description,
    spec_schema          = EXCLUDED.spec_schema,
    status_schema        = EXCLUDED.status_schema,
    config_schema        = EXCLUDED.config_schema,
    reactions            = EXCLUDED.reactions,
    finalizer_name       = EXCLUDED.finalizer_name,
    max_inflight         = EXCLUDED.max_inflight,
    task_deadline_secs   = EXCLUDED.task_deadline_secs,
    resync_interval_secs = EXCLUDED.resync_interval_secs,
    resync_recomposes    = EXCLUDED.resync_recomposes,
    orphan_grace_secs    = EXCLUDED.orphan_grace_secs,
    max_transient_attempts = EXCLUDED.max_transient_attempts,
    retired              = EXCLUDED.retired,
    schema_hash          = EXCLUDED.schema_hash,
    manifest_version     = EXCLUDED.manifest_version,
    updated_at           = now();

-- name: GetKindManifest :one
-- Reads one (kind, kind_version) manifest (API/UI, KindManifestCache miss-fill). OFF the hot
-- path. PK probe on (kind, kind_version).
SELECT kind, kind_version, description, spec_schema, status_schema, config_schema,
       reactions, finalizer_name, max_inflight, task_deadline_secs, resync_interval_secs,
       resync_recomposes, orphan_grace_secs, max_transient_attempts, retired, schema_hash, manifest_version, created_at, updated_at
FROM kind_manifest WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::int;

-- name: ListKindManifests :many
-- Reads every manifest across all (kind, kind_version) (KindManifestCache full load at boot +
-- failsafe re-read, API list, schema-validator rebuild). OFF the hot path.
SELECT kind, kind_version, description, spec_schema, status_schema, config_schema,
       reactions, finalizer_name, max_inflight, task_deadline_secs, resync_interval_secs,
       resync_recomposes, orphan_grace_secs, max_transient_attempts, retired, schema_hash, manifest_version, created_at, updated_at
FROM kind_manifest ORDER BY kind, kind_version;

-- name: GetKindSchemaHash :one
-- Reads the stored schema hash + content-hash for a (kind, kind_version) — the
-- publish-time (kind, kind_version)→content-hash invariant gate compares an incoming
-- manifest against these before allowing an in-place same-kind_version overwrite. Cheap
-- PK probe.
SELECT schema_hash, manifest_version FROM kind_manifest WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::int;

-- name: KindVersionHasResources :one
-- Existence check (NOT a count): is ANY resource pinned to this (kind, kind_version)?
-- The FIRST delete-guard for DeleteKindManifest. EXISTS short-circuits at the first
-- matching row (the kind-leading index locates it) instead of scanning + counting
-- every match — we only need >0-or-0, never the tally. INCLUDES soft-deleting/draining
-- rows (deletion_requested_at set): a version still tearing anything down is NOT
-- deletable. Off every hot path (operator delete only).
SELECT EXISTS (
    SELECT 1 FROM resources
    WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::smallint
);

-- name: KindVersionPinnedByBinding :one
-- Existence check (NOT a count): is this (kind, kind_version) EXACTLY pinned by any
-- reactor binding — either as the WATCHED kind (watch_kind_version = V) or as the
-- REACTOR that runs (reactor_version = V)? The SECOND delete-guard. EXISTS
-- short-circuits at the first match. An UNPINNED binding (NULL watch/reactor version)
-- is deliberately NOT matched: it resolves to the remaining versions, so dropping one
-- version never strands it. Off the hot path.
SELECT EXISTS (
    SELECT 1 FROM reactor_bindings
    WHERE (watch_kind = $1 AND watch_kind_version = sqlc.arg('kind_version')::smallint)
       OR (reactor    = $1 AND reactor_version    = sqlc.arg('kind_version')::smallint)
);

-- name: DeleteProviderConfigsAtKindVersion :execrows
-- Cascade-deletes every provider config at a (kind, kind_version) as part of the
-- manifest delete — once no resource references the version, its configs (which only
-- ever serve resources of their own (kind, kind_version)) are dead. Runs in the same
-- transaction as the manifest/config delete. Returns the row count deleted.
DELETE FROM providerconfigs
WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::smallint;

-- name: DeleteKindConfig :execrows
-- Deletes the (kind, kind_version) operational-config row alongside the manifest
-- (kind_config is derived from the manifest and joined by value, not FK, so it is
-- removed explicitly). Returns the row count (0 if none — idempotent).
DELETE FROM kind_config
WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::smallint;

-- name: DeleteKindManifest :execrows
-- Deletes the (kind, kind_version) manifest row. The AFTER DELETE trigger fires the
-- kind_manifest_changed NOTIFY so caches drop the pair. Returns the row count (0 =
-- nothing there → the API maps it to 404, idempotent). Blocking checks + the paired
-- kind_config/providerconfigs deletes are enforced by store.DeleteKindManifest in one
-- transaction; this is the final statement.
DELETE FROM kind_manifest
WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::smallint;

-- name: BatchKindPolicy :many
-- Batch-reads (kind, kind_version, orphan_grace_secs, finalizer_name) for a set of kinds
-- in ONE query — the composer's orphan-prune loop uses this to build an in-memory
-- (kind, kind_version) → policy map BEFORE the per-child loop, so the grace/finalizer
-- lookup never becomes N per-child DB round-trips at 1M children. Returns EVERY
-- kind_version of each requested kind (a handful of rows per kind); the composer indexes
-- the map by the child's concrete (kind, kind_version). Reads kind_config (the policy
-- table the manifest derives into).
SELECT kc.kind, kc.kind_version, kc.orphan_grace_secs, kc.finalizer_name
FROM kind_config kc
WHERE kc.kind = ANY($1::text[]);
