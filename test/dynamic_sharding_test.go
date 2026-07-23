package test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/shardutil"
	"github.com/salesforce/converge/internal/store"
)

// TestDynamicSharding exercises dynamic, membership-driven sharding end to end
// against a real Postgres: the assign_member_shards() SQL function (the COMPUTE
// half), the cluster_members INSERT/DELETE trigger → cluster_changed NOTIFY (the
// REACT half), and the Go runtime.Resharder + ShardSet that turn those into a
// live, lock-free shard assignment every driver reads.
//
// It does NOT run the reconcile engine — dynamic sharding is independent of the
// work path — but it needs the schema, so it migrates via a throwaway control
// plane first (same pattern as the cluster-member registry test).
func TestDynamicSharding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	basePool := setupPostgres(t, ctx)
	_ = startAllRolesEngine(t, ctx, basePool) // migrates the schema

	// The Resharders each hold ONE pooled connection for their lifetime (the
	// cluster_changed LISTEN), so a handful of them would exhaust setupPostgres'
	// default-sized pool. Build a generously-sized pool from the same DSN.
	bigCfg, err := pgxpool.ParseConfig(basePool.Config().ConnString())
	require.NoError(t, err)
	bigCfg.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(ctx, bigCfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	repo := store.New(pool)

	t.Run("AssignMemberShardsTilesRoleExactlyLikeShardsForPod", func(t *testing.T) {
		resetMembers(t, ctx, pool)
		const n = 5
		ids := make([]string, n)
		for i := 0; i < n; i++ {
			ids[i] = fmt.Sprintf("worker-%02d", i) // sort order == rank order
			upsertMember(t, ctx, repo, ids[i], "worker")
		}
		// Each member's computed span must equal ShardsForPod(rank, n) — the
		// member_id sort order IS the rank order — and the spans must tile
		// [0,256) with no overlap and no gap.
		var spans [][2]int16
		for rank, id := range ids {
			lo, hi, ok, err := repo.AssignMemberShards(ctx, id, shardutil.NumShards, 90*time.Second)
			require.NoError(t, err)
			require.True(t, ok, "member %s of %d should own a slice", id, n)

			want := shardutil.ShardsForPod(rank, n, shardutil.NumShards)
			require.Equal(t, want[0], lo, "member %s lo", id)
			require.Equal(t, want[len(want)-1], hi, "member %s hi", id)
			spans = append(spans, [2]int16{lo, hi})
		}
		require.True(t, isFullDisjointTiling(spans, shardutil.NumShards),
			"the %d members must tile [0,%d) exactly: %v", n, shardutil.NumShards, spans)
	})

	t.Run("RolesTileIndependently", func(t *testing.T) {
		resetMembers(t, ctx, pool)
		// 2 control + 3 worker: each ROLE tiles the full space among ITS OWN
		// members, independent of the other role (a control member and a worker
		// member can both own shard 0 — they do different things with it).
		upsertMember(t, ctx, repo, "control-0", "control")
		upsertMember(t, ctx, repo, "control-1", "control")
		for i := 0; i < 3; i++ {
			upsertMember(t, ctx, repo, fmt.Sprintf("worker-%d", i), "worker")
		}

		ctrl := collectSpans(t, ctx, repo, []string{"control-0", "control-1"})
		require.True(t, isFullDisjointTiling(ctrl, shardutil.NumShards),
			"control role must tile [0,256) 2-way: %v", ctrl)

		wrk := collectSpans(t, ctx, repo, []string{"worker-0", "worker-1", "worker-2"})
		require.True(t, isFullDisjointTiling(wrk, shardutil.NumShards),
			"worker role must tile [0,256) 3-way: %v", wrk)

		// control-0 (rank 0 of 2) owns the first HALF; worker-0 (rank 0 of 3)
		// owns the first THIRD — different tilings, proving role independence.
		require.Equal(t, [2]int16{0, 127}, ctrl[0])
		require.Equal(t, [2]int16{0, 84}, wrk[0])
	})

	t.Run("LivenessWindowExcludesStaleMembers", func(t *testing.T) {
		resetMembers(t, ctx, pool)
		upsertMember(t, ctx, repo, "worker-fresh", "worker")
		upsertMember(t, ctx, repo, "worker-stale", "worker")
		// Age one member's heartbeat past a tight liveness window. It must drop
		// out of the tiling (owns nothing) and the fresh one must take the whole
		// space — this is how a crashed peer's shards get reclaimed.
		_, err := pool.Exec(ctx,
			`UPDATE cluster_members SET last_heartbeat = now() - interval '10 minutes' WHERE member_id = $1`,
			"worker-stale")
		require.NoError(t, err)

		lo, hi, ok, err := repo.AssignMemberShards(ctx, "worker-fresh", shardutil.NumShards, 60*time.Second)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, [2]int16{0, 255}, [2]int16{lo, hi}, "fresh member alone owns everything")

		_, _, ok, err = repo.AssignMemberShards(ctx, "worker-stale", shardutil.NumShards, 60*time.Second)
		require.NoError(t, err)
		require.False(t, ok, "stale member is excluded → owns nothing")
	})

	t.Run("UnregisteredMemberOwnsNothing", func(t *testing.T) {
		resetMembers(t, ctx, pool)
		_, _, ok, err := repo.AssignMemberShards(ctx, "ghost", shardutil.NumShards, 90*time.Second)
		require.NoError(t, err)
		require.False(t, ok, "a member with no registry row owns nothing")
	})

	t.Run("ReshardersReactToJoinAndLeave", func(t *testing.T) {
		resetMembers(t, ctx, pool)

		// A live "pod": its registry row, the ShardSet its drivers would read,
		// and the Resharder keeping that ShardSet in sync with membership.
		type pod struct {
			id    string
			set   *runtime.ShardSet
			watch *runtime.TopologyWatcher
			start func()
		}
		newPod := func(i int) *pod {
			id := fmt.Sprintf("worker-%02d", i)
			set := runtime.NewShardSet(nil) // owns nothing until the resharder assigns
			resh := runtime.NewResharder(store.New(pool), id, set)
			resh.LivenessWindow = 90 * time.Second
			// The TopologyWatcher drives the resharder reactor on the shared
			// cluster_changed wake + failsafe poll (the same shape production uses).
			watch := runtime.NewTopologyWatcher(runtime.NewPgxListener(pool))
			watch.Interval = 1 * time.Second // snappy failsafe for the test
			watch.LivenessWindow = 90 * time.Second
			watch.Register("resharder", resh.Reactor(), nil)
			return &pod{
				id:    id,
				set:   set,
				watch: watch,
				start: func() {
					// Register the row FIRST (its INSERT fires cluster_changed,
					// waking the other pods' watchers), THEN start this pod's watcher
					// (its synchronous initial run sees the full set).
					upsertMember(t, ctx, repo, id, "worker")
					watch.Start(ctx)
				},
			}
		}

		const initial = 4
		pods := make([]*pod, 0, initial+1)
		for i := 0; i < initial; i++ {
			p := newPod(i)
			pods = append(pods, p)
			p.start()
		}
		t.Cleanup(func() {
			for _, p := range pods {
				p.watch.Stop()
			}
		})

		liveSets := func() []*runtime.ShardSet {
			out := make([]*runtime.ShardSet, len(pods))
			for i, p := range pods {
				out[i] = p.set
			}
			return out
		}

		// They converge to a 4-way tiling of [0,256).
		requireEventualTiling(t, liveSets, shardutil.NumShards, "initial 4-way tiling")

		// JOIN: a 5th pod registers (INSERT → cluster_changed). The 4 running
		// resharders shrink and the newcomer takes its slice → 5-way tiling.
		p5 := newPod(initial)
		pods = append(pods, p5)
		p5.start()
		requireEventualTiling(t, liveSets, shardutil.NumShards, "5-way tiling after join")

		// LEAVE: the 5th pod deregisters (DELETE → cluster_changed) and stops.
		// The remaining 4 grow back to cover the space → 4-way tiling again.
		p5.watch.Stop()
		require.NoError(t, repo.DeleteClusterMember(ctx, p5.id))
		pods = pods[:initial]
		requireEventualTiling(t, liveSets, shardutil.NumShards, "4-way tiling after leave")
	})

	t.Run("ReporterReportsDynamicSpanFromShardSet", func(t *testing.T) {
		resetMembers(t, ctx, pool)
		set := runtime.NewShardSet(nil)
		rep := runtime.NewClusterMemberReporter(store.New(pool), store.ClusterMemberInfo{
			MemberID:  "worker-dyn",
			Role:      "worker",
			StartedAt: time.Now(),
		})
		rep.Shards = set // report the live span, not a static Info.Shards
		rep.Interval = 100 * time.Millisecond
		rep.Start(ctx)
		t.Cleanup(rep.Stop)

		// Initially the set is empty → the registry row's shards is NULL.
		require.Eventually(t, func() bool {
			m := findMember(t, ctx, repo, "worker-dyn")
			return m != nil && m.Shards == nil
		}, 2*time.Second, 50*time.Millisecond, "empty ShardSet → NULL shards")

		// A reshard swaps in [0,127]; Trigger() pushes an immediate beat, so the
		// registry's shards column reflects it without waiting out the interval.
		set.Store(contiguous(0, 127))
		rep.Trigger()
		require.Eventually(t, func() bool {
			m := findMember(t, ctx, repo, "worker-dyn")
			return m != nil && m.Shards != nil && *m.Shards == [2]int16{0, 127}
		}, 2*time.Second, 50*time.Millisecond, "reshard → registry shards = [0,127]")
	})
}

// ── helpers ──────────────────────────────────────────────────────────────

func resetMembers(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, "DELETE FROM cluster_members")
	require.NoError(t, err)
}

func upsertMember(t *testing.T, ctx context.Context, repo *store.Store, id, role string) {
	t.Helper()
	require.NoError(t, repo.UpsertClusterMember(ctx, store.ClusterMemberInfo{
		MemberID:  id,
		Role:      role,
		StartedAt: time.Now(),
	}, 0, nil))
}

func collectSpans(t *testing.T, ctx context.Context, repo *store.Store, ids []string) [][2]int16 {
	t.Helper()
	out := make([][2]int16, 0, len(ids))
	for _, id := range ids {
		lo, hi, ok, err := repo.AssignMemberShards(ctx, id, shardutil.NumShards, 90*time.Second)
		require.NoError(t, err)
		require.True(t, ok, "member %s should own a slice", id)
		out = append(out, [2]int16{lo, hi})
	}
	return out
}

func findMember(t *testing.T, ctx context.Context, repo *store.Store, id string) *store.ClusterMember {
	t.Helper()
	rows, err := repo.ListClusterMembers(ctx)
	require.NoError(t, err)
	for i := range rows {
		if rows[i].MemberID == id {
			return &rows[i]
		}
	}
	return nil
}

func contiguous(lo, hi int16) []int16 {
	out := make([]int16, 0, int(hi-lo)+1)
	for s := lo; s <= hi; s++ {
		out = append(out, s)
	}
	return out
}

// isFullDisjointTiling reports whether the inclusive [lo,hi] spans (ignoring
// empty ones, hi<lo) partition [0,total) exactly: sorted by lo, the first
// starts at 0, the last ends at total-1, and each span begins right after the
// previous ends — no overlap, no gap.
func isFullDisjointTiling(spans [][2]int16, total int) bool {
	nonEmpty := make([][2]int16, 0, len(spans))
	for _, s := range spans {
		if s[1] >= s[0] {
			nonEmpty = append(nonEmpty, s)
		}
	}
	if len(nonEmpty) == 0 {
		return false
	}
	sort.Slice(nonEmpty, func(i, j int) bool { return nonEmpty[i][0] < nonEmpty[j][0] })
	if nonEmpty[0][0] != 0 || int(nonEmpty[len(nonEmpty)-1][1]) != total-1 {
		return false
	}
	for i := 1; i < len(nonEmpty); i++ {
		if nonEmpty[i][0] != nonEmpty[i-1][1]+1 {
			return false
		}
	}
	return true
}

// requireEventualTiling waits until the given ShardSets' current bounds form a
// full disjoint tiling of [0,total). Resharders converge via the cluster_changed
// NOTIFY (prompt) backed by the failsafe interval, so a few seconds is ample.
func requireEventualTiling(t *testing.T, sets func() []*runtime.ShardSet, total int, msg string) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		live := sets()
		spans := make([][2]int16, len(live))
		for i, s := range live {
			lo, hi := s.Bounds()
			spans[i] = [2]int16{lo, hi}
		}
		return isFullDisjointTiling(spans, total)
	}, 15*time.Second, 100*time.Millisecond, "%s: ShardSets never converged to a full disjoint tiling", msg)
}
