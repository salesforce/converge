-- Topology queries for the hierarchy view. Free-form (group-by arbitrary
-- label keys with variable WHERE chains) queries live as hand-written
-- SQL constants in internal/dbq/topology_manual.go.

-- name: TopologyLabelKeys :many
-- Union of label keys actually present on resources in this owner subtree.
SELECT DISTINCT k
FROM resources r
JOIN resource_meta m ON m.id = r.id,
     jsonb_object_keys(m.labels) AS k
WHERE r.owner_id = $1
ORDER BY k
LIMIT $2;
