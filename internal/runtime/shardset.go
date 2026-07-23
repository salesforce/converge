package runtime

import "sync/atomic"

// ShardSnapshot is an immutable view of the shard range a process currently
// owns. Shards is a contiguous ascending slice; Lo/Hi are its inclusive bounds
// (Hi < Lo — i.e. (0, -1) — means "owns nothing", matching store.shardBounds'
// empty range that prunes to no partition). Once published a snapshot is never
// mutated, so every reader holds it lock-free and a reshard is a single atomic
// pointer store with no torn reads.
type ShardSnapshot struct {
	Shards []int16
	Lo, Hi int16
}

// ShardSet is a lock-free, atomically-swappable holder of the shard range a
// process owns. It is the seam that lets the Resharder swap the shard range
// at runtime when cluster membership changes.
//
// The hot loops — the dispatcher's claim sweep, the drainer bands, and the
// reaper/specgc/resyncer ticks — read the current snapshot every iteration via
// Snapshot() (a single atomic.Pointer load, no lock, no allocation). The
// Resharder publishes a new range with Store(). Because each snapshot is
// immutable and swapped with one atomic store, a reader either sees the whole
// old range or the whole new one — never a mix — so a reshard can overlap a
// claim/drain without corrupting ownership (and overlap is safe anyway: the
// work_queue claim is FOR UPDATE SKIP LOCKED, so two pods racing the same shard
// each take disjoint rows).
type ShardSet struct {
	p atomic.Pointer[ShardSnapshot]
}

// NewShardSet builds a set initialised to the given contiguous shard slice
// (nil/empty → owns nothing until the first Store, the dynamic-bootstrap case).
// Takes ownership of shards; the caller must not mutate it afterwards.
func NewShardSet(shards []int16) *ShardSet {
	s := &ShardSet{}
	s.p.Store(snapshotOf(shards))
	return s
}

func snapshotOf(shards []int16) *ShardSnapshot {
	lo, hi := boundsOf(shards)
	return &ShardSnapshot{Shards: shards, Lo: lo, Hi: hi}
}

// boundsOf is the ShardSet-local twin of store.shardBounds: the inclusive
// [lo, hi] of a contiguous set, or (0, -1) for empty (matches nothing). Kept
// here because store.shardBounds is unexported and the snapshot caches the
// bounds so the compare in Store is a cheap two-int check.
func boundsOf(shards []int16) (lo, hi int16) {
	if len(shards) == 0 {
		return 0, -1
	}
	lo, hi = shards[0], shards[0]
	for _, s := range shards[1:] {
		if s < lo {
			lo = s
		}
		if s > hi {
			hi = s
		}
	}
	return lo, hi
}

// Snapshot returns the current immutable shard view. Lock-free (one atomic
// load). Callers MUST treat the returned snapshot and its Shards slice as
// read-only — it may be shared with other goroutines.
func (s *ShardSet) Snapshot() *ShardSnapshot { return s.p.Load() }

// Bounds returns the current inclusive [lo, hi] span (hi < lo when empty), for
// the cluster-view reporter that records each member's owned span.
func (s *ShardSet) Bounds() (lo, hi int16) {
	snap := s.p.Load()
	return snap.Lo, snap.Hi
}

// Store publishes a new contiguous shard range and reports whether it actually
// changed. A contiguous set is fully determined by its [lo, hi] bounds, so the
// compare is two ints: an unchanged tick is a no-op that neither swaps the
// pointer nor invalidates the drainer bands' snapshot-keyed split cache. Takes
// ownership of shards; the caller must not mutate it afterwards.
func (s *ShardSet) Store(shards []int16) bool {
	next := snapshotOf(shards)
	if cur := s.p.Load(); cur != nil && cur.Lo == next.Lo && cur.Hi == next.Hi {
		return false
	}
	s.p.Store(next)
	return true
}
