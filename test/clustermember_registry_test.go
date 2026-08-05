package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
)

// TestClusterMemberRegistry exercises the cluster_members running-fleet registry
// end to end against a real Postgres: the store UPSERT/list/GC/deregister
// wrappers, the shard-range round-trip, the started_at-preserving re-beat, the
// TTL GC, a live ClusterMemberReporter driver writing on its own cadence, and
// the clean-shutdown deregister DELETE. It does NOT run the engine — the
// registry is independent of the reconcile path — but it does need the schema,
// so it migrates via a throwaway control plane first.
func TestClusterMemberRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	// startAllRolesEngine migrates the pool (via migrateForSeed) as it builds the
	// harness — the cheapest way to get the schema applied in this package. We
	// don't Start it; we only need the migrated pool.
	_ = startAllRolesEngine(t, ctx, pool)

	repo := store.New(pool)

	// ── 1. Upsert + list: a fresh row round-trips, shards parse back to the
	//       inclusive [lo, hi] span, in_flight is recorded. ──
	started := time.Now().Add(-90 * time.Minute)
	info := store.ClusterMemberInfo{
		MemberID:  "host-1-aaaabbbb",
		Role:      "all",
		Shards:    &[2]int16{0, 127},
		Config:    json.RawMessage(`{"worker_max_parallel":"100"}`),
		Version:   "v-test",
		Hostname:  "host-1",
		PID:       4242,
		StartedAt: started,
	}
	workersJSON := json.RawMessage(`[{"worker_id":"fleet-a","kinds":["noop"],"inflight":2,"max_inflight":10}]`)
	require.NoError(t, repo.UpsertClusterMember(ctx, info, 7, workersJSON))

	rows, err := repo.ListClusterMembers(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	got := rows[0]
	require.Equal(t, "host-1-aaaabbbb", got.MemberID)
	require.Equal(t, "all", got.Role)
	require.NotNil(t, got.Shards)
	require.Equal(t, [2]int16{0, 127}, *got.Shards)
	require.Equal(t, int64(7), got.InFlight)
	require.JSONEq(t, string(workersJSON), string(got.Workers), "workers column must round-trip")
	require.Equal(t, "v-test", got.Version)
	require.Equal(t, int32(4242), got.PID)
	require.True(t, got.StartedAt.Valid)
	require.True(t, got.LastHeartbeat.Valid)
	firstBeat := got.LastHeartbeat.Time

	// ── 2. Re-beat: same id with a new in_flight UPSERTs in place (still one
	//       row), refreshes last_heartbeat, and PRESERVES started_at. ──
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, repo.UpsertClusterMember(ctx, info, 3, nil)) // nil workers → "[]"
	rows, err = repo.ListClusterMembers(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1, "re-beat must update in place, not insert")
	got = rows[0]
	require.Equal(t, int64(3), got.InFlight)
	require.JSONEq(t, "[]", string(got.Workers), "nil workers on re-beat clears to []")
	require.WithinDuration(t, started, got.StartedAt.Time, time.Second, "started_at must be preserved across beats")
	require.True(t, got.LastHeartbeat.Time.After(firstBeat) || got.LastHeartbeat.Time.Equal(firstBeat))

	// ── 3. A control-only member with no shard slice stores a NULL range. ──
	require.NoError(t, repo.UpsertClusterMember(ctx, store.ClusterMemberInfo{
		MemberID:  "host-2-cccddd",
		Role:      "control",
		Shards:    nil,
		StartedAt: time.Now(),
	}, 0, nil))
	rows, err = repo.ListClusterMembers(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	for _, r := range rows {
		if r.MemberID == "host-2-cccddd" {
			require.Nil(t, r.Shards, "control member with no slice → NULL range → nil")
			require.Equal(t, int64(0), r.InFlight)
		}
	}

	// ── 4. GC: fresh rows survive a 5m TTL; an artificially-aged row is
	//       reclaimed. ──
	deleted, err := repo.GCStaleClusterMembers(ctx, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, 0, deleted, "fresh rows are not GC'd")

	// Age host-2 past the TTL directly (the UPSERT always stamps now()).
	_, err = pool.Exec(ctx,
		`UPDATE cluster_members SET last_heartbeat = now() - interval '10 minutes' WHERE member_id = $1`,
		"host-2-cccddd")
	require.NoError(t, err)
	deleted, err = repo.GCStaleClusterMembers(ctx, 5*time.Minute)
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "row aged past TTL is reclaimed")
	rows, err = repo.ListClusterMembers(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "host-1-aaaabbbb", rows[0].MemberID)

	// ── 5. The live ClusterMemberReporter writes on its own cadence and reports
	//       the in-flight count from its hook. Stop() leaves the row (it only
	//       halts beating); deregistration is a separate explicit step. ──
	reporter := runtime.NewClusterMemberReporter(store.New(pool), store.ClusterMemberInfo{
		MemberID:  "host-3-eeefff",
		Role:      "worker",
		Shards:    &[2]int16{0, 63},
		Version:   "v-test",
		Hostname:  "host-3",
		PID:       9001,
		StartedAt: time.Now(),
	})
	reporter.Interval = 100 * time.Millisecond
	reporter.InFlight = func() int { return 5 }
	reporter.Start(ctx) // beats once synchronously

	require.Eventually(t, func() bool {
		rs, e := repo.ListClusterMembers(ctx)
		if e != nil {
			return false
		}
		for _, r := range rs {
			if r.MemberID == "host-3-eeefff" && r.InFlight == 5 {
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond, "reporter should write its row with in_flight=5")

	reporter.Stop()
	// Clean Stop does NOT delete the row — that's Deregister's job (section 6).
	rows, err = repo.ListClusterMembers(ctx)
	require.NoError(t, err)
	found := false
	for _, r := range rows {
		if r.MemberID == "host-3-eeefff" {
			found = true
		}
	}
	require.True(t, found, "Stop must not delete the registry row")

	// ── 6. Deregister: the clean-shutdown DELETE removes this member's own row
	//       immediately, with a deadline-bounded context. The other member's
	//       row is untouched. ──
	deregCtx, deregCancel := context.WithTimeout(ctx, 5*time.Second)
	defer deregCancel()
	require.NoError(t, reporter.Deregister(deregCtx))
	rows, err = repo.ListClusterMembers(ctx)
	require.NoError(t, err)
	for _, r := range rows {
		require.NotEqual(t, "host-3-eeefff", r.MemberID, "Deregister must delete this member's row")
	}
	require.Len(t, rows, 1, "only the deregistered member's row is removed")
	require.Equal(t, "host-1-aaaabbbb", rows[0].MemberID)

	// Deregister is idempotent: deleting an already-gone row is a no-op, not an
	// error (a crash-then-restart with a fresh id must never wedge shutdown).
	require.NoError(t, reporter.Deregister(deregCtx))
}

// TestListLiveVsFullClusterMembers pins the deliberate divergence between the two
// registry reads that the mesh-liveness fix depends on: ListClusterMembers is the
// UI/history view (EVERY row, so the API can show a just-crashed member as NotReady),
// while ListLiveClusterMembers(window) is the ROUTING view (only members whose
// heartbeat is within the window). The mesh reads the live one so a crashed peer drops
// out of the peer set within one window instead of lingering to the 5m GC; the
// resharder's assign_member_shards applies the SAME predicate, so tiling and routing
// agree on who is alive. This proves, in isolation, that the two reads split exactly at
// the window boundary.
func TestListLiveVsFullClusterMembers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	_ = startAllRolesEngine(t, ctx, pool) // migrate the schema (not started)
	repo := store.New(pool)

	// Three members; all UPSERT stamps now(), then we age two of them.
	for _, id := range []string{"live-fresh", "stale-just-over", "stale-way-over"} {
		require.NoError(t, repo.UpsertClusterMember(ctx, store.ClusterMemberInfo{
			MemberID: id, Role: "broker", Version: "v-test", Hostname: id, PID: 1, StartedAt: time.Now(),
		}, 0, nil))
	}
	const window = 30 * time.Second
	// Age one member just past the window, another far past it. The third stays fresh.
	_, err := pool.Exec(ctx,
		`UPDATE cluster_members SET last_heartbeat = now() - interval '45 seconds' WHERE member_id = 'stale-just-over'`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`UPDATE cluster_members SET last_heartbeat = now() - interval '5 minutes' WHERE member_id = 'stale-way-over'`)
	require.NoError(t, err)

	// The FULL read (UI/history) returns ALL three — a stale member stays visible so the
	// API can render it NotReady; the GC (5m) is what eventually removes it, NOT this read.
	full, err := repo.ListClusterMembers(ctx)
	require.NoError(t, err)
	fullIDs := memberIDSet(full)
	require.Len(t, fullIDs, 3, "the full read keeps stale rows (UI/history view)")
	require.Contains(t, fullIDs, "stale-just-over")
	require.Contains(t, fullIDs, "stale-way-over")

	// The LIVE read (routing) returns ONLY the fresh member — both stale ones are excluded
	// within one window, which is exactly why the mesh stops routing to a crashed peer
	// without waiting for the GC.
	live, err := repo.ListLiveClusterMembers(ctx, window)
	require.NoError(t, err)
	liveIDs := memberIDSet(live)
	require.Len(t, liveIDs, 1, "the live read excludes everything past the window (routing view)")
	require.Contains(t, liveIDs, "live-fresh")
	require.NotContains(t, liveIDs, "stale-just-over", "a member just past the window must not route")
	require.NotContains(t, liveIDs, "stale-way-over")

	// A wide-enough window re-includes the just-over member — proving the boundary is the
	// window, not some other property.
	wide, err := repo.ListLiveClusterMembers(ctx, time.Minute)
	require.NoError(t, err)
	wideIDs := memberIDSet(wide)
	require.Contains(t, wideIDs, "stale-just-over", "a 60s window includes the 45s-old member")
	require.NotContains(t, wideIDs, "stale-way-over", "but still excludes the 5m-old one")
}

func memberIDSet(rows []store.ClusterMember) map[string]struct{} {
	out := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		out[r.MemberID] = struct{}{}
	}
	return out
}
