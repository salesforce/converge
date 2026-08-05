-- Resource queries. Every entity lives in `resources`; root resources
-- are rows with owner_id IS NULL, owned resources point at a parent.
--
-- State is two orthogonal axes:
--   synced_gen vs generation -> the Synced axis (spec reconciled)
--   health_ok                -> the Ready/health axis (observed healthy)
-- is_ready GENERATED = synced AND healthy AND not deleting. Rich K8s/
-- Crossplane-style conditions (Ready/custom, with reason+message+
-- lastTransitionTime) live in resource_conditions; the Synced condition
-- + happy-path Ready=True are synthesized API-side from the two scalars.

-- name: ResolveResourceIDByKindName :one
-- Resolve the internal resource id (uuid) from the PUBLIC (kind, name)
-- identity. This is the single translation the name-keyed API handlers do at
-- their top before delegating to the id-based reads below — so the whole SQL
-- layer stays internally id-based while nothing but this probe touches an id.
-- Served by uniq_resource_meta (UNIQUE btree (kind, name)): a single index seek.
SELECT id FROM resource_meta WHERE kind = $1 AND name = $2;

-- name: ResolveResourceIDKindVersionByKindName :one
-- Resolve (id, kind_version) from the PUBLIC (kind, name) — the apply path's flip-detect
-- probe: if the incoming apply's kind_version differs from the resource's pinned kind_version,
-- the apply is a breaking KIND-VERSION FLIP routed to FlipResourceKindVersion instead of
-- the ordinary in-place update. One index seek (uniq_resource_meta) + the row's
-- kind_version. No row → the resource doesn't exist yet (a plain create at the applied
-- kind_version).
SELECT m.id, r.kind_version FROM resource_meta m JOIN resources r ON r.id = m.id
WHERE m.kind = $1 AND m.name = $2;

-- name: ResolveResourceIDsByKindName :many
-- Batch (kind, name) -> id resolution for the cross-owner scoping params
-- (?owner=kind/name repeated). Zipped over two parallel arrays by ordinality,
-- matched against uniq_resource_meta. Returns the resolved identity so the
-- caller can map a requested ref back to its id (unresolved refs don't appear).
SELECT m.id, m.kind, m.name
FROM unnest($1::text[]) WITH ORDINALITY AS k(kind, ord)
JOIN unnest($2::text[]) WITH ORDINALITY AS n(name, ord) ON k.ord = n.ord
JOIN resource_meta m ON m.kind = k.kind AND m.name = n.name;

-- name: GetResource :one
-- Per-row read addressed by the PUBLIC (kind, name) — NO separate id-resolve
-- probe: the resource_meta join that already materializes name is also the
-- lookup key (uniq_resource_meta = one index seek). r.id is returned for the
-- detail handler's own follow-ups (conditions, work_queue) so those reuse it
-- rather than re-resolving. spec/status are elided server-side when they exceed
-- the inline limit (100 KB). owner_id/root_id stay INTERNAL (cache keys); the
-- API surfaces owner/root only as (kind, name) refs, resolved here by the two
-- LEFT JOINs on resource_meta so the detail read needs no extra round-trip.
SELECT
    r.id, r.kind, r.kind_version, m.name, r.owner_id, r.root_id,
    om.kind AS owner_kind, om.name AS owner_name,
    rm.kind AS root_kind,  rm.name AS root_name,
    CASE WHEN pg_column_size(r.spec)   > 102400 THEN NULL::jsonb ELSE r.spec   END AS spec,
    CASE WHEN pg_column_size(r.status) > 102400 THEN NULL::jsonb ELSE r.status END AS status,
    COALESCE(pg_column_size(r.spec), 0)::bigint   AS spec_size,
    COALESCE(pg_column_size(r.status), 0)::bigint AS status_size,
    r.generation, r.synced_gen,
    r.is_ready, r.health_ok, r.phase,
    r.finalizers, r.deletion_requested_at,
    m.labels,
    r.created_at, r.updated_at,
    r.shard_id,
    -- Failure classification for the detail page: failure_terminal = the current-gen
    -- failure won't auto-retry (provider-terminal OR the poison-pill escalated a
    -- persistently-transient failure at the cap → DEAD-LETTERED); failure_attempts =
    -- the durable consecutive-transient-failure count climbing toward the kind's
    -- max_transient_attempts (also returned, from kc) so the UI can show "attempt N of
    -- CAP" and distinguish approaching-dead-letter from already-dead-lettered.
    r.failure_terminal, r.failure_attempts,
    kc.max_transient_attempts AS max_transient_attempts,
    -- manifest_version drift: the version this resource is PINNED to (stamped at
    -- create/compose, carried onto its work) vs the kind's CURRENT applied
    -- version (kind_config). When they differ, a CRD was re-applied after this
    -- resource last scheduled — the detail page surfaces it as a drift signal.
    -- current is NULL when the kind has no manifest yet.
    r.manifest_version AS manifest_version,
    kc.manifest_version AS kind_manifest_version,
    -- Attached CUSTOM provider config (if any), so the detail page can render a
    -- link to it. NULL for the common no-custom-config case (LEFT JOIN). name is
    -- the global handle the provider-configs page resolves by; kind == r.kind.
    pc.name AS provider_config_name,
    pc.kind AS provider_config_kind
FROM resources r
JOIN resource_meta m ON m.id = r.id
LEFT JOIN resource_meta om ON om.id = r.owner_id
LEFT JOIN resource_meta rm ON rm.id = r.root_id
LEFT JOIN providerconfigs pc ON pc.id = r.provider_config_id
LEFT JOIN kind_config kc ON kc.kind = r.kind AND kc.kind_version = r.kind_version
WHERE m.kind = $1 AND m.name = $2;

-- name: GetResourceManifest :one
-- Synthesize the downloadable ResourceManifest entirely in the DB:
-- one jsonb_build_object packs identity (kind+name), labels, and the
-- live spec into a single column so the bytes never get reassembled
-- app-side. Pairs with the Apply body shape (model.ResourceManifest)
-- — a downloaded file re-applies as-is. provider_config_ref is included
-- ONLY when the resource has a custom config attached, so a download→reapply
-- preserves the attachment without polluting the common no-config manifest
-- (a CASE rather than jsonb_strip_nulls, which would recursively strip nested
-- nulls inside spec). m.name is also returned so the download handler can name
-- the attachment without re-parsing the blob.
SELECT
    (CASE WHEN pc.name IS NULL THEN
        jsonb_build_object(
            'kind',   r.kind,
            'kind_version',  r.kind_version,
            'name',   m.name,
            'labels', m.labels,
            'spec',   r.spec
        )
    ELSE
        jsonb_build_object(
            'kind',                r.kind,
            'kind_version',               r.kind_version,
            'name',                m.name,
            'labels',              m.labels,
            'spec',                r.spec,
            'provider_config_ref', pc.name
        )
    END)::jsonb AS manifest,
    r.kind,
    m.name
FROM resources r
JOIN resource_meta m ON m.id = r.id
LEFT JOIN providerconfigs pc ON pc.id = r.provider_config_id
WHERE m.kind = $1 AND m.name = $2;

-- name: GetResourceStatus :one
SELECT r.status, r.kind, m.name FROM resources r JOIN resource_meta m ON m.id = r.id WHERE m.kind = $1 AND m.name = $2;

-- name: GetResourceInfo :one
-- Lightweight metadata only — omits spec/status. By internal id (used by paths
-- that already hold one: apply read-back, owner-cache batch fan-in).
SELECT r.id, r.kind, r.kind_version, m.name, r.owner_id, r.generation, r.synced_gen,
       r.is_ready, r.health_ok, r.phase, r.deletion_requested_at,
       m.labels, r.created_at, r.updated_at
FROM resources r JOIN resource_meta m ON m.id = r.id WHERE r.id = $1;

-- name: GetResourceInfoByName :one
-- Same lightweight metadata addressed by the PUBLIC (kind, name). Owner-scoped
-- handlers use this for the owner envelope: it returns r.id, so the follow-up
-- owner-scoped query reuses it — one indexed seek here, no separate resolve.
SELECT r.id, r.kind, m.name, r.owner_id, r.generation, r.synced_gen,
       r.is_ready, r.health_ok, r.phase, r.deletion_requested_at,
       m.labels, r.created_at, r.updated_at
FROM resources r JOIN resource_meta m ON m.id = r.id
WHERE m.kind = $1 AND m.name = $2;

-- name: GetResourcesInfo :many
SELECT r.id, r.kind, m.name, r.owner_id, r.generation, r.synced_gen,
       r.is_ready, r.health_ok, r.phase, r.deletion_requested_at,
       m.labels, r.created_at, r.updated_at
FROM resources r JOIN resource_meta m ON m.id = r.id WHERE r.id = ANY($1::uuid[]);

-- name: ListRootResourcesPage :many
SELECT
    r.id, r.kind, r.kind_version, m.name, r.generation, r.synced_gen,
    r.is_ready, r.health_ok, r.phase, r.deletion_requested_at,
    m.labels, r.created_at, r.updated_at,
    (SELECT count(*) FROM resources c WHERE c.owner_id = r.id)::bigint AS child_count
FROM resources r JOIN resource_meta m ON m.id = r.id
WHERE r.owner_id IS NULL
  AND (sqlc.narg('kinds')::text[]      IS NULL OR r.kind = ANY(sqlc.narg('kinds')::text[]))
  -- (kind, kind_version) PAIR filter — the Resources-page kind/version chips
  -- (vpc/v1, account/v2, …). SARGABLE: `r.kind = ANY(kv_kinds)` prunes to the
  -- selected kinds' slices via the leading `kind` column of
  -- idx_resources_kind_pagination FIRST, then the composite `kind:version` equality
  -- is an EXACT per-pair match WITHIN that pruned slice — no whole-table scan, no
  -- new hot-path index. kv_kinds is the distinct kind list (for pruning) and
  -- kv_pairs the "kind:version" strings (for the exact match); the app builds both
  -- from the same chip set. NULL = no pair filter. AND-composed with the plain
  -- `kinds` filter (kinds = any version of these kinds; pairs = these exact versions).
  AND (sqlc.narg('kv_kinds')::text[] IS NULL
       OR (r.kind = ANY(sqlc.narg('kv_kinds')::text[])
           AND (r.kind || ':' || r.kind_version::text) = ANY(sqlc.narg('kv_pairs')::text[])))
  AND (sqlc.narg('name_like')::text    IS NULL OR m.name ILIKE '%' || sqlc.narg('name_like')::text || '%')
  AND (sqlc.narg('created_after')::timestamptz  IS NULL OR r.created_at >= sqlc.narg('created_after')::timestamptz)
  AND (sqlc.narg('created_before')::timestamptz IS NULL OR r.created_at <= sqlc.narg('created_before')::timestamptz)
  AND (sqlc.narg('updated_after')::timestamptz  IS NULL OR r.updated_at >= sqlc.narg('updated_after')::timestamptz)
  AND (sqlc.narg('updated_before')::timestamptz IS NULL OR r.updated_at <= sqlc.narg('updated_before')::timestamptz)
  AND (sqlc.narg('gen_min')::bigint IS NULL OR r.generation >= sqlc.narg('gen_min')::bigint)
  AND (sqlc.narg('gen_max')::bigint IS NULL OR r.generation <= sqlc.narg('gen_max')::bigint)
  AND (sqlc.narg('count_min')::bigint IS NULL
       OR (SELECT count(*) FROM resources c WHERE c.owner_id = r.id) >= sqlc.narg('count_min')::bigint)
  AND (sqlc.narg('count_max')::bigint IS NULL
       OR (SELECT count(*) FROM resources c WHERE c.owner_id = r.id) <= sqlc.narg('count_max')::bigint)
  -- phase filter: a single optional value matched against the generated
  -- `phase` scalar (one indexed predicate; 'Failed' hits idx_resources_failed).
  AND (sqlc.narg('phase')::text IS NULL OR r.phase = sqlc.narg('phase')::text)
  AND (
      sqlc.narg('cursor_after_at')::timestamptz IS NULL
      OR (r.created_at, r.id) < (sqlc.narg('cursor_after_at')::timestamptz, sqlc.narg('cursor_after_id')::uuid)
  )
ORDER BY r.created_at DESC, r.id DESC
LIMIT $1;

-- name: CountRootResourcesFiltered :one
SELECT count(*) FROM resources r JOIN resource_meta m ON m.id = r.id
WHERE r.owner_id IS NULL
  AND (sqlc.narg('kinds')::text[]      IS NULL OR r.kind = ANY(sqlc.narg('kinds')::text[]))
  -- (kind, kind_version) PAIR filter — mirrors ListRootResourcesPage so the count
  -- matches the page. SARGABLE (kind-prune then composite equality); NULL = none.
  AND (sqlc.narg('kv_kinds')::text[] IS NULL
       OR (r.kind = ANY(sqlc.narg('kv_kinds')::text[])
           AND (r.kind || ':' || r.kind_version::text) = ANY(sqlc.narg('kv_pairs')::text[])))
  AND (sqlc.narg('name_like')::text    IS NULL OR m.name ILIKE '%' || sqlc.narg('name_like')::text || '%')
  AND (sqlc.narg('created_after')::timestamptz  IS NULL OR r.created_at >= sqlc.narg('created_after')::timestamptz)
  AND (sqlc.narg('created_before')::timestamptz IS NULL OR r.created_at <= sqlc.narg('created_before')::timestamptz)
  AND (sqlc.narg('updated_after')::timestamptz  IS NULL OR r.updated_at >= sqlc.narg('updated_after')::timestamptz)
  AND (sqlc.narg('updated_before')::timestamptz IS NULL OR r.updated_at <= sqlc.narg('updated_before')::timestamptz)
  AND (sqlc.narg('gen_min')::bigint IS NULL OR r.generation >= sqlc.narg('gen_min')::bigint)
  AND (sqlc.narg('gen_max')::bigint IS NULL OR r.generation <= sqlc.narg('gen_max')::bigint)
  AND (sqlc.narg('count_min')::bigint IS NULL
       OR (SELECT count(*) FROM resources c WHERE c.owner_id = r.id) >= sqlc.narg('count_min')::bigint)
  AND (sqlc.narg('count_max')::bigint IS NULL
       OR (SELECT count(*) FROM resources c WHERE c.owner_id = r.id) <= sqlc.narg('count_max')::bigint)
  AND (sqlc.narg('phase')::text IS NULL OR r.phase = sqlc.narg('phase')::text);

-- name: GetChildrenByOwner :many
-- Non-paginated children read for the flat /children + /graph views. LIMIT-capped
-- ($2) so an owner with millions of children (a composer root) can never stream an
-- unbounded response / blow up the API's memory. Walk large sets via the keyset-
-- paginated ListResourcesPage (/children/page) instead.
SELECT
    r.id, r.kind, m.name, r.owner_id,
    r.generation, r.synced_gen,
    r.is_ready, r.phase,
    r.deletion_requested_at,
    m.labels,
    r.created_at, r.updated_at
FROM resources r JOIN resource_meta m ON m.id = r.id WHERE r.owner_id = $1
ORDER BY r.created_at, r.id
LIMIT $2;

-- name: GetDepsByOwner :many
-- Dependency edges under one owner, addressed by the PUBLIC (kind, name) of
-- each endpoint (not the internal id) so the graph joins nodes by ref. Both
-- endpoints live under the same owner, so the meta joins are PK probes.
-- LIMIT-capped ($2) for the same reason as GetChildrenByOwner: the graph view
-- must not stream an unbounded edge set for a huge owner.
SELECT
    dent.kind AS dependent_kind, dent.name AS dependent_name,
    dep.kind  AS dependency_kind, dep.name AS dependency_name
FROM resource_deps d
JOIN resources r ON r.id = d.dependent_id
JOIN resource_meta dent ON dent.id = d.dependent_id
JOIN resource_meta dep  ON dep.id  = d.dependency_id
WHERE r.owner_id = $1
LIMIT $2;

-- name: CountChildrenByReadiness :many
-- Per-kind phase breakdown. Six FILTER buckets over the generated `phase`
-- scalar — one GROUP BY scan, no join.
SELECT kind,
       count(*) FILTER (WHERE phase = 'Ready')::bigint       AS ready,
       count(*) FILTER (WHERE phase = 'Reconciling')::bigint AS reconciling,
       count(*) FILTER (WHERE phase = 'Degraded')::bigint    AS degraded,
       count(*) FILTER (WHERE phase = 'Failed')::bigint      AS failed,
       count(*) FILTER (WHERE phase = 'Deleting')::bigint    AS deleting,
       count(*) FILTER (WHERE phase = 'Orphaned')::bigint    AS orphaned,
       count(*) FILTER (WHERE phase = 'Quarantined')::bigint AS quarantined
FROM resources WHERE owner_id = $1
GROUP BY kind;

-- name: CountResourcesByReadinessScoped :many
SELECT kind,
       count(*) FILTER (WHERE phase = 'Ready')::bigint       AS ready,
       count(*) FILTER (WHERE phase = 'Reconciling')::bigint AS reconciling,
       count(*) FILTER (WHERE phase = 'Degraded')::bigint    AS degraded,
       count(*) FILTER (WHERE phase = 'Failed')::bigint      AS failed,
       count(*) FILTER (WHERE phase = 'Deleting')::bigint    AS deleting,
       count(*) FILTER (WHERE phase = 'Orphaned')::bigint    AS orphaned,
       count(*) FILTER (WHERE phase = 'Quarantined')::bigint AS quarantined
FROM resources
WHERE (sqlc.narg('owner_ids')::uuid[] IS NULL
       OR owner_id = ANY(sqlc.narg('owner_ids')::uuid[]))
GROUP BY kind;

-- name: GetResourcesChangedSince :many
SELECT
    r.id, r.kind, m.name, r.owner_id,
    r.generation, r.synced_gen,
    r.is_ready, r.phase,
    r.deletion_requested_at,
    m.labels,
    r.created_at, r.updated_at
FROM resources r JOIN resource_meta m ON m.id = r.id
WHERE r.owner_id = $1 AND r.updated_at > $2
ORDER BY r.updated_at
LIMIT 1000;

-- name: ListResourcesPage :many
-- The meta JOIN is a PK probe per row; it only materializes name/labels for
-- the LIMIT'd page (and for the name_like/label_match filters when present).
SELECT
    r.id, r.kind, r.kind_version, m.name, r.owner_id,
    r.generation, r.synced_gen,
    r.is_ready, r.phase,
    r.deletion_requested_at,
    m.labels,
    r.created_at, r.updated_at
FROM resources r JOIN resource_meta m ON m.id = r.id
WHERE
      (sqlc.narg('owner_ids')::uuid[] IS NULL
       OR r.owner_id = ANY(sqlc.narg('owner_ids')::uuid[]))
  AND (sqlc.narg('kinds')::text[]    IS NULL OR r.kind = ANY(sqlc.narg('kinds')::text[]))
  -- (kind, kind_version) PAIR filter — the kind/version chips (vpc/v1, account/v2).
  -- SARGABLE: `r.kind = ANY(kv_kinds)` prunes per kind via idx_resources_kind_pagination,
  -- then the composite `kind:version` equality is the exact per-pair match within
  -- that slice (no whole-table scan, no new hot-path index). NULL = no pair filter.
  AND (sqlc.narg('kv_kinds')::text[] IS NULL
       OR (r.kind = ANY(sqlc.narg('kv_kinds')::text[])
           AND (r.kind || ':' || r.kind_version::text) = ANY(sqlc.narg('kv_pairs')::text[])))
  -- Phase filter — a SET (union of selected phases), not a single value.
  -- The UI lets the operator multi-select phases; we match any of them so
  -- the page is the union, computed and paginated server-side (never a
  -- client-side sliver of the first page). 'Failed' is served by
  -- idx_resources_failed; the others by the generated scalar — no
  -- condition join, ever.
  AND (sqlc.narg('phases')::text[] IS NULL OR r.phase = ANY(sqlc.narg('phases')::text[]))
  AND (sqlc.narg('name_like')::text  IS NULL OR m.name ILIKE '%' || sqlc.narg('name_like')::text || '%')
  AND (sqlc.narg('label_match')::jsonb IS NULL OR m.labels @> sqlc.narg('label_match')::jsonb)
  AND (
      sqlc.narg('cursor_after_at')::timestamptz IS NULL
      OR (r.created_at, r.id) < (sqlc.narg('cursor_after_at')::timestamptz, sqlc.narg('cursor_after_id')::uuid)
  )
ORDER BY r.created_at DESC, r.id DESC
LIMIT $1;

-- name: CountResourcesFiltered :one
SELECT count(*) FROM resources r JOIN resource_meta m ON m.id = r.id
WHERE
      (sqlc.narg('owner_ids')::uuid[] IS NULL
       OR r.owner_id = ANY(sqlc.narg('owner_ids')::uuid[]))
  AND (sqlc.narg('kinds')::text[]    IS NULL OR r.kind = ANY(sqlc.narg('kinds')::text[]))
  -- (kind, kind_version) PAIR filter — mirrors ListResourcesPage so the count matches.
  AND (sqlc.narg('kv_kinds')::text[] IS NULL
       OR (r.kind = ANY(sqlc.narg('kv_kinds')::text[])
           AND (r.kind || ':' || r.kind_version::text) = ANY(sqlc.narg('kv_pairs')::text[])))
  AND (sqlc.narg('phases')::text[] IS NULL OR r.phase = ANY(sqlc.narg('phases')::text[]))
  AND (sqlc.narg('name_like')::text  IS NULL OR m.name ILIKE '%' || sqlc.narg('name_like')::text || '%')
  AND (sqlc.narg('label_match')::jsonb IS NULL OR m.labels @> sqlc.narg('label_match')::jsonb);

-- name: GetResourceDependencies :many
-- Lists every upstream this resource depends on, with the upstream's
-- id/kind/name/is_ready/health_ok/updated_at and the value_flows array.
SELECT
    u.id   AS upstream_id,
    u.kind AS upstream_kind,
    um.name AS upstream_name,
    u.is_ready AS upstream_ready,
    u.health_ok AS upstream_health_ok,
    u.updated_at AS upstream_updated_at,
    d.value_flows
FROM resource_deps d
JOIN resources u ON u.id = d.dependency_id
JOIN resource_meta um ON um.id = u.id
-- Address the DEPENDENT by its public (kind, name) via one indexed subquery, so
-- there's no separate id-resolve round-trip.
WHERE d.dependent_id = (SELECT sm.id FROM resource_meta sm WHERE sm.kind = $1 AND sm.name = $2)
ORDER BY u.kind, um.name;

-- name: ListDescendants :many
-- The rollup reaction reads each descendant's labels (fd/team) + name, so
-- join meta. The meta join is a PK probe; root_id drives the scan via
-- idx_resources_root_descendants.
SELECT r.*, m.name, m.labels
FROM resources r JOIN resource_meta m ON m.id = r.id
WHERE r.root_id = $1
ORDER BY r.created_at, r.id;

-- name: DescendantsSettled :one
-- The dispatcher's rollup-readiness gate: true when NO live descendant of root
-- is still lagging. Its predicates MUST match schedule_eligible / the cascade +
-- resync paths EXACTLY, or the worker's rollup-stage gate and the scheduler
-- disagree and a frozen (orphaned/quarantined) child would wedge the root's
-- rollup. A settled child is one that is synced, or terminally failed at this
-- generation, or delete-requested, or frozen (orphan grace / quarantine set-
-- aside). Uses idx_resources_root_lagging.
SELECT NOT EXISTS (
    SELECT 1 FROM resources d
    WHERE d.root_id = $1
      AND d.synced_gen < d.generation
      AND d.failure_gen <> d.generation     -- failed children are settled
      AND d.deletion_requested_at IS NULL
      AND d.frozen_until IS NULL            -- frozen (orphaned grace / quarantined) children are settled
) AS settled;

-- name: ScheduleEligible :one
SELECT schedule_eligible($1::uuid[])::int AS scheduled;

-- name: RequestResourceDeletion :one
-- Soft-delete by internal id. The rare operator mutations (delete/quarantine/
-- reconcile/rollback) intentionally stay id-based: the API handler resolves
-- (kind,name)→id once (an indexed point-read) and calls this. Folding the
-- resolve into each function via a *ByName twin would double the SQL/store
-- surface to save a microsecond on a manual click — not worth it. The READ
-- paths (hot) are folded to a single query; these (cold) are not. Internal
-- callers (composer teardown) also hold an id, so id-based is the natural form.
SELECT request_resource_deletion($1, $2, $3)::boolean AS deleted;

-- name: QuarantineResource :one
-- Operator sets a failed/stuck resource ASIDE: freezes it (no retry/resync/
-- resurrection) and unblocks its root's rollup, without deleting it. Returns
-- true if the row existed and was quarantinable. See quarantine_resource.
SELECT quarantine_resource($1, $2)::boolean AS quarantined;

-- name: UnquarantineResource :one
-- Clears a quarantine and re-arms the resource (schedule_eligible runs on it).
-- Returns true if the row existed and was quarantined. See unquarantine_resource.
SELECT unquarantine_resource($1, $2)::boolean AS released;

-- name: RequeueForResync :one
-- Drift-detection sweep for one kind. See requeue_for_resync in
-- 00001_schema.sql. Called by the control plane only for kinds whose
-- ResyncInterval > 0. $5 (recompose) forces a composer re-run on the
-- re-pend — opt-in per kind via the manifest's resync_recomposes setting.
SELECT requeue_for_resync($1::text, $2::int, $3::int, $4::smallint[], $5::boolean)::int AS requeued;

-- name: ListSpecHistory :many
-- Spec revision history for a ROOT resource, newest-first, WITHOUT the
-- bodies (the UI lazy-fetches a body via the raw endpoint). is_current
-- flags the revision whose body matches the live inline resources.spec (the
-- checked-out one — exactly one, found by a JSONB compare over the small
-- ≤keep_n history). Served by idx_spec_versions_history. spec_versions holds
-- only roots; the API gates the endpoint to roots.
SELECT s.generation, s.source, s.created_at,
       pg_column_size(s.spec)::bigint AS size_bytes,
       (s.spec IS NOT DISTINCT FROM r.spec) AS is_current
FROM spec_versions s
JOIN resources r ON r.id = s.resource_id
-- Address the root by its public (kind, name) via one indexed subquery — single
-- round-trip, no separate id-resolve.
WHERE s.resource_id = (SELECT sm.id FROM resource_meta sm WHERE sm.kind = $1 AND sm.name = $2)
ORDER BY s.generation DESC
LIMIT $3;

-- name: GetSpecRevision :one
-- One historic spec body by (kind, name, generation) — the raw revision
-- download + the rollback prefill. Name-scoped so another resource's version
-- can't be fetched through this resource's identity.
SELECT spec FROM spec_versions
WHERE resource_id = (SELECT sm.id FROM resource_meta sm WHERE sm.kind = $1 AND sm.name = $2)
  AND generation = $3;

-- name: RollbackToSpec :one
-- Check out a historic root revision (by authored generation) as the live
-- spec. See rollback_to_spec in 00001_schema.sql. Returns the resource's
-- generation after checkout, or -1 if (id, generation) doesn't exist.
SELECT rollback_to_spec($1, $2, $3)::bigint AS generation;

-- name: GCSpecHistory :one
-- Trim the spec_versions root history to keep_n newest per root, for one
-- pod's shards. See gc_spec_history in 00001_schema.sql.
SELECT gc_spec_history($1::int, $2::int, $3::smallint[])::int AS collected;

-- name: ListResourceConditions :many
-- Every stored condition row for one resource (the Ready/custom axes the
-- provider actually reported). The Synced condition and happy-path
-- Ready=True are NOT stored here — the API synthesizes them from the
-- resource's scalar axes. Ordered Ready-first then by type for stable UI.
SELECT resource_id, type, status, reason, message,
       observed_generation, last_transition_at
FROM resource_conditions
WHERE resource_id = $1
ORDER BY (type = 'Ready') DESC, type;
