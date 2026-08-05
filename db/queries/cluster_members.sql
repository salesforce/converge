-- Go-side wrappers for the cluster_members running-fleet registry.
--
-- The shards column is an int4range; sqlc has no Go mapping for it, so we
-- cast it to/from text at the query boundary (`$N::int4range` on write,
-- `shards::text` on read) and the store wrapper owns the [lo,hi] <-> [lo,hi+1)
-- conversion — the same "store does the typed conversion" pattern as
-- shardBounds. That keeps internal/dbq free of any int4range type.

-- name: UpsertClusterMember :exec
-- One cheap PK UPSERT per ~30s beat. last_heartbeat is stamped now() in SQL
-- (DB clock, never an app timestamp) so liveness is skew-free; started_at is
-- written on INSERT only — the DO UPDATE deliberately omits it so uptime is
-- stable across beats. Every other field is refreshed: in_flight + workers are
-- the LIVE per-beat state (a broker's claimed-task count + its connected-worker
-- snapshot); config/version/identity are static-but-cheap to re-set.
INSERT INTO cluster_members (
    member_id, role, shards, config, in_flight, workers,
    version, hostname, pid, started_at, last_heartbeat
) VALUES (
    $1, $2, $3::int4range, $4, $5, $6::jsonb,
    $7, $8, $9, $10, now()
)
ON CONFLICT (member_id) DO UPDATE SET
    role           = EXCLUDED.role,
    shards         = EXCLUDED.shards,
    config         = EXCLUDED.config,
    in_flight      = EXCLUDED.in_flight,
    workers        = EXCLUDED.workers,
    version        = EXCLUDED.version,
    hostname       = EXCLUDED.hostname,
    pid            = EXCLUDED.pid,
    last_heartbeat = now();

-- name: DeleteClusterMember :exec
-- Deregister: delete this member's row on a clean shutdown so the cluster
-- view drops it immediately (instead of waiting for the GC TTL). Called as the
-- LAST shutdown step under a bounded context so a DB/network blip can't hang
-- the process — the GC then reclaims the row as the crash/timeout fallback.
DELETE FROM cluster_members WHERE member_id = $1;

-- name: GCStaleClusterMembers :one
-- Hard-delete members whose heartbeat is older than ttl_seconds; returns the
-- number reclaimed. Driven by the ControlPlane ClusterMemberGC sweeper.
SELECT gc_stale_cluster_members($1::int)::int AS deleted;

-- NOTE: the cluster-view listing (ListClusterMembers) AND the dynamic-sharding
-- assignment (AssignMemberShards, calling the assign_member_shards() function)
-- are HAND-WRITTEN in internal/store/store.go, NOT generated here. sqlc can't
-- reliably type the int4range result (a bare `shards::text` cast is non-nullable
-- string and fails to scan a NULL; `lower()/upper()` infer as
-- string/interface{}), so the store scans the range bounds into pgtype.Int4
-- directly — the "write manual SQL in store when sqlc can't express it"
-- convention.
