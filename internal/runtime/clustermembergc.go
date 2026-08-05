package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// ClusterMemberGC is the delayed garbage collector for the cluster_members
// running-fleet registry: it hard-deletes rows whose last heartbeat is older
// than TTL (a member that crashed, or whose clean-shutdown deregister timed
// out). It runs on the ControlPlane only — like the Reaper/SpecGC sweepers —
// and is the Postgres-side cron the user asked for, implemented as a Go-driven
// sweeper calling a SQL function (gc_stale_cluster_members) rather than
// pg_cron, which isn't enabled here.
//
// It is the FALLBACK to a clean shutdown's own deregister (the
// ClusterMemberReporter deletes its row on exit). k8s-style soft liveness with
// delayed GC: a member that vanished without deregistering is shown NotReady in
// the cluster view (the API derives that from heartbeat age, ~3 missed beats) for a
// while BEFORE this sweeper reclaims it at the longer TTL — so a just-crashed member
// stays visible (greyed) instead of vanishing. Cadence invariant:
// beat (DefaultMemberHeartbeatEvery) ≤ NotReady/3 ≤ TTL 5m.
//
// THIS IS UI/HISTORY CLEANUP ONLY — it is NOT a routing signal. Routing liveness
// (which brokers reshard onto, which mesh peers to dial) is the SHORT liveness window
// (3× the heartbeat cadence) the TopologyWatcher applies when it re-reads the LIVE
// members; a crashed member drops out of routing there, within one window, long
// before this GC deletes its row. The GC only decides when the greyed row disappears.
//
// Entirely off the hot path: a coarse cadence, a single-statement DELETE over
// a dozens-of-rows table, no shard scoping, no work_queue/outbox coupling.
type ClusterMemberGC struct {
	Repo ClusterMemberGCRepo

	// TTL: a member unheard-from for longer than this is reclaimed.
	TTL time.Duration

	Interval     time.Duration
	IdleInterval time.Duration

	// OnSwept is an optional metrics observer (sweep, n>0). nil → no-op. Off the hot path.
	OnSwept func(sweep string, n int64)
}

// NewClusterMemberGC builds the sweeper with production-sane defaults: a 5m TTL
// (10 missed 30s beats) and a coarse 60s cadence.
func NewClusterMemberGC(repo ClusterMemberGCRepo) *ClusterMemberGC {
	return &ClusterMemberGC{
		Repo:         repo,
		TTL:          5 * time.Minute,
		Interval:     60 * time.Second,
		IdleInterval: 60 * time.Second,
	}
}

func (g *ClusterMemberGC) Run(ctx context.Context) error {
	// repollOnWork=false: a coarse-cadence rebroker on a fixed interval.
	// nil wakeup: no NOTIFY source. hotWindow=0: no post-work hot poll.
	pollLoop(ctx, "clustermembergc", g.Interval, g.IdleInterval, 0, false, nil, g.tick)
	return nil
}

// tick reclaims every member past the TTL in one bounded DELETE.
func (g *ClusterMemberGC) tick(ctx context.Context) (bool, error) {
	deleted, err := g.Repo.GCStaleClusterMembers(ctx, g.TTL)
	if err != nil {
		return false, fmt.Errorf("gc stale cluster members: %w", err)
	}
	if deleted > 0 {
		slog.Debug("clustermembergc reclaimed stale members", "count", deleted)
		if g.OnSwept != nil {
			g.OnSwept("members_reclaimed", int64(deleted))
		}
	}
	return deleted > 0, nil
}
