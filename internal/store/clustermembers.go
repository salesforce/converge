package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/salesforce/converge/internal/dbq"
)

// ─────────────────────────────────────────────────────────────────────────
// Cluster members registry (running-fleet view — not on any hot path).
//
// One row per live PROCESS (pod) — a cluster member, i.e. one instance of the
// Converge binary identified by its role: the ClusterMemberReporter UPSERTs
// it every ~30s; the API lists it for the cluster view; a clean shutdown
// deletes its own row; the ClusterMemberGC sweeper hard-deletes stale rows as
// the fallback. See cluster_members in 00001_schema.sql.
// ─────────────────────────────────────────────────────────────────────────

// ClusterMemberInfo is the STATIC per-process payload the reporter gathers once
// (in cmd/converge/main.go, where role/shards/config are known). The LIVE state
// that changes every beat — the in-flight count and the connected-worker
// snapshot — is passed separately to UpsertClusterMember, so this struct stays
// immutable across beats.
type ClusterMemberInfo struct {
	MemberID string
	Role     string
	// Shards is the member's owned contiguous shard span [lo, hi] INCLUSIVE, or
	// nil when it owns none (e.g. a control-only member with no reaper slice).
	// Stored as the half-open int4range [lo, hi+1).
	Shards    *[2]int16
	Config    json.RawMessage // small STATIC JSONB blob (connect_addr + runtime knobs); nil → "{}"
	Version   string
	Hostname  string
	PID       int
	StartedAt time.Time
}

// ClusterMember is one row of the registry as read for the cluster view.
// Carries the parsed shard span (*[2]int16) the generated row can't express, so
// it's a hand-defined struct (NOT a dbq alias). Liveness (Ready/NotReady) is
// derived by the API from LastHeartbeat age — there is no stored status.
type ClusterMember struct {
	MemberID string
	Role     string
	Shards   *[2]int16 // [lo, hi] inclusive, or nil
	Config   json.RawMessage
	InFlight int64
	// Workers is the LIVE connected-worker snapshot (the `workers` JSONB column):
	// a JSON array of {worker_id,kinds,inflight,max_inflight}, populated on broker
	// rows only. "[]" on control/react and on a broker with none. The API decodes
	// it into the typed cluster DTO.
	Workers       json.RawMessage
	Version       string
	Hostname      string
	PID           int32
	StartedAt     pgtype.Timestamptz
	LastHeartbeat pgtype.Timestamptz
}

// shardRange builds the half-open int4range [lo, hi+1) pgx value for the
// inclusive [lo, hi] span, or a NULL range when the pod owns no shards.
func shardRange(s *[2]int16) pgtype.Range[pgtype.Int4] {
	if s == nil {
		return pgtype.Range[pgtype.Int4]{Valid: false}
	}
	return pgtype.Range[pgtype.Int4]{
		Lower:     pgtype.Int4{Int32: int32(s[0]), Valid: true},
		Upper:     pgtype.Int4{Int32: int32(s[1]) + 1, Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}
}

// shardSpanFromBounds rebuilds the inclusive [lo, hi] span from a Postgres
// range's lower/upper bounds. The range is stored half-open ([lo, hi+1)), so
// the inclusive hi is upper-1. A NULL lower OR upper (a pod owning no slice)
// → nil.
func shardSpanFromBounds(lo, hi pgtype.Int4) *[2]int16 {
	if !lo.Valid || !hi.Valid {
		return nil
	}
	return &[2]int16{int16(lo.Int32), int16(hi.Int32) - 1}
}

// UpsertClusterMember writes the member's heartbeat row: a single PK UPSERT that
// refreshes last_heartbeat (stamped now() in SQL — DB clock) plus the LIVE state
// (inFlight + the connected-worker snapshot), preserving started_at. inFlight and
// workers are passed separately from the static info because they change every
// beat. workers is a JSON array ("[]" when empty). Off every hot path.
func (s *Store) UpsertClusterMember(ctx context.Context, info ClusterMemberInfo, inFlight int, workers json.RawMessage) error {
	config := info.Config
	if len(config) == 0 {
		config = json.RawMessage("{}")
	}
	if len(workers) == 0 {
		workers = json.RawMessage("[]")
	}
	return s.queries().UpsertClusterMember(ctx, dbq.UpsertClusterMemberParams{
		MemberID:  info.MemberID,
		Role:      info.Role,
		Column3:   shardRange(info.Shards),
		Config:    config,
		InFlight:  int32(inFlight),
		Column6:   workers,
		Version:   info.Version,
		Hostname:  info.Hostname,
		Pid:       int32(info.PID),
		StartedAt: pgtype.Timestamptz{Time: info.StartedAt, Valid: true},
	})
}

// DeleteClusterMember deregisters a member by deleting its registry row. Called
// as the last step of a clean shutdown (under a bounded context) so the cluster
// view drops the member immediately; the ClusterMemberGC reclaims it otherwise.
func (s *Store) DeleteClusterMember(ctx context.Context, memberID string) error {
	return s.queries().DeleteClusterMember(ctx, memberID)
}

// GCStaleClusterMembers hard-deletes registry rows whose last heartbeat is older
// than ttl, returning the count reclaimed. Driven by the ClusterMemberGC
// sweeper as the fallback to a clean shutdown's own deregister.
func (s *Store) GCStaleClusterMembers(ctx context.Context, ttl time.Duration) (int, error) {
	ttlSec := secsAtLeast1(ttl)
	n, err := s.queries().GCStaleClusterMembers(ctx, ttlSec)
	return int(n), err
}

// ListClusterMembers returns the full registry for the cluster view, newest
// heartbeat first — the UI/history view: EVERY row, including members whose
// heartbeat has lapsed (the API derives Ready/NotReady from its age, and shows a
// crashed member as NotReady until the GC deletes it). For the ROUTING view (only
// members alive within a liveness window) use ListLiveClusterMembers. The SQL is
// hand-written in dbq (dbq.ListClusterMembersSQL) — sqlc can't type the int4range
// shards column.
func (s *Store) ListClusterMembers(ctx context.Context) ([]ClusterMember, error) {
	rows, err := s.db.Query(ctx, dbq.ListClusterMembersSQL)
	if err != nil {
		return nil, err
	}
	return scanClusterMembers(rows)
}

// ListLiveClusterMembers returns only members whose heartbeat is within `window`
// — the ROUTING view of the registry (vs ListClusterMembers' full UI/history view).
// The TopologyWatcher reads it so both the resharder's shard tiling AND the mesh's
// peer set derive from ONE definition of "alive": a crashed peer (heartbeat lapsed,
// row not yet GC'd) is excluded within one window, so the mesh stops routing to its
// dead dial address without waiting out the ClusterMemberGC TTL. Same liveness
// predicate as assign_member_shards, so shard ownership and mesh routing never
// disagree on who is live.
func (s *Store) ListLiveClusterMembers(ctx context.Context, window time.Duration) ([]ClusterMember, error) {
	rows, err := s.db.Query(ctx, dbq.ListLiveClusterMembersSQL, int32(window.Seconds()))
	if err != nil {
		return nil, err
	}
	return scanClusterMembers(rows)
}

// scanClusterMembers drains a cluster_members result set (the identical column list
// of ListClusterMembersSQL / ListLiveClusterMembersSQL) into ClusterMembers, closing
// the rows. Shared so the full-view and live-view reads scan identically.
func scanClusterMembers(rows pgx.Rows) ([]ClusterMember, error) {
	defer rows.Close()
	var out []ClusterMember
	for rows.Next() {
		var c ClusterMember
		var lo, hi pgtype.Int4
		if err := rows.Scan(
			&c.MemberID, &c.Role, &lo, &hi, &c.Config,
			&c.InFlight, &c.Workers, &c.Version, &c.Hostname, &c.PID,
			&c.StartedAt, &c.LastHeartbeat,
		); err != nil {
			return nil, err
		}
		c.Shards = shardSpanFromBounds(lo, hi)
		out = append(out, c)
	}
	return out, rows.Err()
}

// AssignMemberShards computes the contiguous shard span this member owns RIGHT
// NOW from the live cluster_members view — the COMPUTE half of dynamic sharding
// the Resharder calls each tick. It ranks the member among the LIVE members of
// its OWN role and range-partitions [0, total) the same way shardutil.ShardsForPod
// does (see assign_member_shards in 00001_schema.sql). Returns the inclusive
// [lo, hi] span and ok=true, or ok=false when the member owns nothing: not yet
// registered, its own heartbeat aged past livenessWindow, or more live members
// than shards. livenessWindow bounds how recent a peer's heartbeat must be to
// count toward the tiling — wide enough (≫ the beat cadence) that a slow beat
// doesn't drop a live peer and churn the assignment.
//
// Unwraps lower()/upper() into the inclusive [lo, hi], mirroring
// shardSpanFromBounds on the cluster-view read path (the range is stored
// half-open [lo, hi+1), so inclusive hi = upper-1).
func (s *Store) AssignMemberShards(ctx context.Context, memberID string, total int, livenessWindow time.Duration) (lo, hi int16, ok bool, err error) {
	secs := secsAtLeast1(livenessWindow)
	var lower, upper pgtype.Int4
	if err := s.db.QueryRow(ctx, dbq.AssignMemberShardsSQL, memberID, int32(total), secs).Scan(&lower, &upper); err != nil {
		return 0, -1, false, err
	}
	if !lower.Valid || !upper.Valid {
		return 0, -1, false, nil // owns nothing this tick
	}
	return int16(lower.Int32), int16(upper.Int32) - 1, true, nil
}
