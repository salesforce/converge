-- Queries for the providerconfigs table: the runtime-editable per-kind config
-- store. Defaults (is_default=TRUE, one per kind) are captured by workers at
-- boot and live-reconfigured; customs (is_default=FALSE) are referenced by
-- resources.provider_config_id and cloned into the work queue at schedule.

-- name: GetDefaultProviderConfig :one
-- The (kind, kind_version)'s DEFAULT config document + bundle, loaded at boot and on every
-- reconfigure by the config plane. Per-kind_version: a vpc/v2 resource merges over vpc/v2's
-- default, not v1's. spec is the operational config (merged with a per-resource
-- override downstream); data is the opaque provider bundle. At most one row matches
-- (uq_providerconfigs_default_per_kind on (kind, kind_version)), an index probe.
SELECT id, spec, data
FROM providerconfigs
WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::int AND is_default;

-- name: GetProviderConfigByName :one
-- Resolve a custom config NAME (a manifest's provider_config_ref) to its id, kind
-- AND kind_version, so Apply can persist resources.provider_config_id and reject a config
-- whose (kind, kind_version) doesn't match the resource. Indexed point read on UNIQUE name.
SELECT id, kind, kind_version
FROM providerconfigs
WHERE name = $1;

-- name: GetDefaultProviderConfigName :one
-- The NAME of a (kind, kind_version)'s current DEFAULT config (if any). The composer config
-- diff uses it to detect a default collision and demote a conflicting emitted
-- default to a custom override BEFORE the upsert, so a second default never trips
-- uq_providerconfigs_default_per_kind mid-compose. Index probe.
SELECT name
FROM providerconfigs
WHERE kind = $1 AND kind_version = sqlc.arg('kind_version')::int AND is_default;

-- name: UpsertProviderConfig :one
-- Create-or-update a config by NAME (the global handle). kind + kind_version + is_default
-- + spec + data are overwritten on update. A second default for a (kind, kind_version) is
-- rejected by uq_providerconfigs_default_per_kind (surfaced as a conflict). name
-- stays globally unique, so a config is pinned to one kind_version — a v2 config uses a
-- distinct name. created distinguishes insert (xmax=0) from update.
INSERT INTO providerconfigs (name, kind, kind_version, is_default, spec, data)
VALUES ($1, $2, sqlc.arg('kind_version')::int, $3, $4, $5)
ON CONFLICT (name) DO UPDATE
    SET kind       = EXCLUDED.kind,
        kind_version      = EXCLUDED.kind_version,
        is_default = EXCLUDED.is_default,
        spec       = EXCLUDED.spec,
        data       = EXCLUDED.data,
        updated_at = now()
RETURNING id, name, kind, kind_version, is_default, spec, data, owner_id, created_at, updated_at, (xmax = 0) AS created;

-- name: GetProviderConfig :one
-- One config by name for the read/detail API, with its owner's kind+name (if a
-- composer owns it) resolved via resource_meta. owner_* are NULL for unowned configs.
SELECT pc.id, pc.name, pc.kind, pc.kind_version, pc.is_default, pc.spec, pc.data, pc.owner_id,
       om.kind AS owner_kind, om.name AS owner_name,
       pc.created_at, pc.updated_at
FROM providerconfigs pc
LEFT JOIN resource_meta om ON om.id = pc.owner_id
WHERE pc.name = $1;

-- name: ListProviderConfigs :many
-- PAGINATED admin list. Filters (all optional, AND-combined):
--   kind_filter    : exact kind ('' = any)
--   kind_version_filter   : exact kind_version (0 = any)
--   name_filter    : substring match on name ('' = any), trigram-indexed ILIKE
--   default_filter : tri-state is_default (NULL = any, true/false = that role)
-- Owner kind+name joined for the "owner, if any" column. Ordered (kind, kind_version, name)
-- for a stable page. SLIM row: spec + the opaque data bundle are NOT selected (the
-- list renders identity/role/owner only); the full manifest comes from GetProviderConfig.
SELECT pc.id, pc.name, pc.kind, pc.kind_version, pc.is_default, pc.owner_id,
       om.kind AS owner_kind, om.name AS owner_name,
       pc.created_at, pc.updated_at
FROM providerconfigs pc
LEFT JOIN resource_meta om ON om.id = pc.owner_id
WHERE (sqlc.arg(kind_filter)::text = '' OR pc.kind = sqlc.arg(kind_filter)::text)
  AND (sqlc.arg(kind_version_filter)::int = 0 OR pc.kind_version = sqlc.arg(kind_version_filter)::int)
  AND (sqlc.arg(name_filter)::text = '' OR pc.name ILIKE '%' || sqlc.arg(name_filter)::text || '%')
  AND (sqlc.narg(default_filter)::bool IS NULL OR pc.is_default = sqlc.narg(default_filter)::bool)
ORDER BY pc.kind, pc.kind_version, pc.name
LIMIT sqlc.arg(lim)::int OFFSET sqlc.arg(off)::int;

-- name: CountProviderConfigs :one
-- Total rows matching the same filter set as ListProviderConfigs. Kept in lockstep.
SELECT count(*)
FROM providerconfigs pc
WHERE (sqlc.arg(kind_filter)::text = '' OR pc.kind = sqlc.arg(kind_filter)::text)
  AND (sqlc.arg(kind_version_filter)::int = 0 OR pc.kind_version = sqlc.arg(kind_version_filter)::int)
  AND (sqlc.arg(name_filter)::text = '' OR pc.name ILIKE '%' || sqlc.arg(name_filter)::text || '%')
  AND (sqlc.narg(default_filter)::bool IS NULL OR pc.is_default = sqlc.narg(default_filter)::bool);

-- name: ListProviderConfigsByOwner :many
-- Every config a given composer root owns — the set ApplyComposeResult diffs a
-- fresh compose's emitted configs against. Index probe on idx_providerconfigs_owner.
SELECT id, name, kind, kind_version, is_default, spec
FROM providerconfigs
WHERE owner_id = $1;

-- name: DeleteProviderConfig :one
-- Remove a config by name. ON DELETE SET NULL on resources.provider_config_id
-- demotes any consumers back to the kind default. Returns whether a row went.
DELETE FROM providerconfigs WHERE name = $1
RETURNING id;
