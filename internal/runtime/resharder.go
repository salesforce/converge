package runtime

import (
	"context"
	"log/slog"
	"time"
)

// ClusterChangedChannel is the LISTEN/NOTIFY channel the cluster_members
// INSERT/DELETE trigger fires (gated, via notify_cluster_changed → notify_gated)
// when a member JOINS or LEAVES. The TopologyWatcher LISTENs on it (once, for all
// its reactors) to react to a membership change promptly instead of waiting out a
// failsafe interval. Exported so the mesh references the ONE channel name.
const ClusterChangedChannel = "cluster_changed"

// Resharder is the per-PROCESS dynamic-sharding COMPUTE unit: given the live
// cluster_members view it computes this member's contiguous shard range (store.
// AssignMemberShards → assign_member_shards(), which ranks this member among the
// LIVE members of its OWN role and range-partitions [0,total)) and atomically swaps
// it into the ShardSet the drivers read. It carries NO loop of its own — the
// TopologyWatcher drives it (via Reactor) on the shared cluster_changed wake +
// failsafe poll, alongside the mesh, so both react to the same membership snapshot.
//
// CORRECTNESS WITHOUT CONSENSUS: every node computes its OWN range from the same
// shared registry with the same deterministic math, so in steady state the ranges
// tile [0,total) with no overlap and no gap. DURING a membership change different
// nodes observe it at slightly different instants and may briefly compute
// overlapping ranges — SAFE: work_queue claims are FOR UPDATE SKIP LOCKED (two pods
// racing a shard take disjoint rows), the drainer is idempotent, and the next tick
// reconciles everyone onto the new tiling. The explicit "transient overlap is fine"
// design — no lock, no leader.
type Resharder struct {
	Repo   ResharderRepo
	Shards *ShardSet

	// MemberID is THIS process's cluster_members id — the row the assignment is
	// computed for. Must match the ClusterMemberReporter's MemberID so the member
	// ranks itself within its registered role.
	MemberID string

	// Total is the shard-space cardinality the range partitions (NumShards).
	Total int

	// LivenessWindow bounds how recent a peer's heartbeat must be to count as a live
	// member in the tiling — the SAME window the mesh uses for peer routing (both
	// set from the TopologyWatcher), so shard ownership and mesh routing never
	// disagree on who is live. Must be ≫ the member-heartbeat cadence so a slow beat
	// never drops a live peer, and ≤ the GC TTL so an excluded member is also on its
	// way to being reclaimed.
	LivenessWindow time.Duration
}

// NewResharder builds a Resharder for the given member id and the ShardSet the
// drivers read. Total defaults to NumShards; the caller sets LivenessWindow (from
// the TopologyWatcher's shared window) before registering the reactor.
func NewResharder(repo ResharderRepo, memberID string, shards *ShardSet) *Resharder {
	return &Resharder{
		Repo:     repo,
		Shards:   shards,
		MemberID: memberID,
		Total:    NumShards,
	}
}

// Reactor returns this resharder's TopologyReactor — the tick the TopologyWatcher
// runs on every cluster_changed wake + failsafe poll. It recomputes this member's
// shard span and swaps it into the ShardSet, reporting whether the span CHANGED (so
// the watcher can fire the resharder's onChange — wired to the reporter's Trigger so
// the new span lands in the registry promptly).
func (r *Resharder) Reactor() TopologyReactor {
	return r.assignOnce
}

// assignOnce computes this member's current span and swaps it into the ShardSet,
// returning whether the assignment CHANGED. Builds the contiguous []int16 from the
// [lo,hi] the DB returns; ok=false (owns nothing) stores an empty set so the drivers
// cleanly no-op until the member is assigned a range. Best-effort: a DB error
// leaves the ShardSet at its prior value and the watcher retries next tick.
func (r *Resharder) assignOnce(ctx context.Context) (bool, error) {
	lo, hi, ok, err := r.Repo.AssignMemberShards(ctx, r.MemberID, r.Total, r.LivenessWindow)
	if err != nil {
		return false, err
	}
	var shards []int16
	if ok {
		shards = make([]int16, 0, int(hi-lo)+1)
		for s := lo; s <= hi; s++ {
			shards = append(shards, s)
		}
	}
	changed := r.Shards.Store(shards)
	if changed {
		if ok {
			slog.Info("resharded", "member", r.MemberID, "shard_lo", lo, "shard_hi", hi, "count", len(shards))
		} else {
			slog.Info("resharded: member owns no shards", "member", r.MemberID)
		}
	}
	return changed, nil
}
