-- Read-side helpers for the resource graph view.

-- name: SubgraphDeps :many
-- Edges among the subgraph's node set, addressed by the PUBLIC (kind, name) of
-- each endpoint (not the internal id) so the graph joins by ref. Both endpoints
-- are already in the node set, so the meta joins are PK probes.
SELECT
    dent.kind AS dependent_kind, dent.name AS dependent_name,
    dep.kind  AS dependency_kind, dep.name AS dependency_name
FROM resource_deps d
JOIN resource_meta dent ON dent.id = d.dependent_id
JOIN resource_meta dep  ON dep.id  = d.dependency_id
WHERE d.dependent_id  = ANY($1::uuid[])
  AND d.dependency_id = ANY($1::uuid[]);

-- name: SubgraphTransitive :many
-- Center the walk on the resource addressed by (kind, name) — resolved in one
-- indexed seed subquery, so there's no separate id-resolve round-trip. The two
-- depth bounds and the row LIMIT follow.
WITH RECURSIVE
center(id) AS (
    SELECT sm.id FROM resource_meta sm WHERE sm.kind = $1 AND sm.name = $2
),
ancestors(id, depth) AS (
    SELECT id, 0 FROM center
    UNION
    SELECT d.dependency_id, a.depth + 1
    FROM ancestors a
    JOIN resource_deps d ON d.dependent_id = a.id
    WHERE a.depth < $3::int
),
descendants(id, depth) AS (
    SELECT id, 0 FROM center
    UNION
    SELECT d.dependent_id, dd.depth + 1
    FROM descendants dd
    JOIN resource_deps d ON d.dependency_id = dd.id
    WHERE dd.depth < $4::int
),
all_nodes AS (
    SELECT id, MIN(signed_depth) AS min_depth
    FROM (
        SELECT id, -depth AS signed_depth FROM ancestors
        UNION ALL
        SELECT id, depth AS signed_depth FROM descendants
    ) u
    GROUP BY id
)
SELECT
    r.id, r.kind, m.name, r.owner_id,
    r.generation, r.synced_gen,
    r.is_ready, r.phase,
    r.deletion_requested_at,
    m.labels,
    r.created_at, r.updated_at,
    a.min_depth::int AS min_depth
FROM all_nodes a
JOIN resources r ON r.id = a.id
JOIN resource_meta m ON m.id = r.id
ORDER BY a.min_depth, r.kind, m.name
LIMIT $5::int;
