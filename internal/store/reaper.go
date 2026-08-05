package store

import (
	"context"
	"time"

	"github.com/salesforce/converge/internal/dbq"
)

// ─────────────────────────────────────────────────────────────────────────
// Reaper wrappers.
// ─────────────────────────────────────────────────────────────────────────

// ReapStaleWork frees work_queue rows whose worker stopped heartbeating.
func (s *Store) ReapStaleWork(ctx context.Context, staleAfter time.Duration, limit int, shards []int16) (int, error) {
	staleSec := secsAtLeast1(staleAfter)
	lo, hi := shardBounds(shards)
	n, err := s.queries().ReapStaleWork(ctx, dbq.ReapStaleWorkParams{
		Column1: staleSec,
		Column2: int32(limit),
		Column3: lo,
		Column4: hi,
	})
	return int(n), err
}

// RecountInflight rebuilds the per-kind concurrency-cap tally for this
// reaper's shard range: it reclaims worker hint partials in the range and
// writes one authoritative per-range count. The self-healing edge of the
// cap — keeps the claim's optimistic +N honest. Called on the reaper tick;
// shards is the reaper's owned (contiguous) shard set.
func (s *Store) RecountInflight(ctx context.Context, shards []int16) error {
	lo, hi := shardBounds(shards)
	return s.queries().RecountInflight(ctx, dbq.RecountInflightParams{
		Column1: lo,
		Column2: hi,
	})
}

// DeleteUnclaimedWork hard-deletes work_queue rows that have been UNCLAIMED
// for longer than deleteAfter — abandoned tasks no worker ever picked up
// (the kind lost its worker / was decommissioned). Garbage collection, not
// recovery: the resource stays lagging, so requeue_failed_and_pending
// re-enqueues a fresh row if a worker for the kind returns. Called on the
// reaper tick over its owned (contiguous) shard range; returns the row count.
func (s *Store) DeleteUnclaimedWork(ctx context.Context, deleteAfter time.Duration, limit int, shards []int16) (int, error) {
	deleteSec := secsAtLeast1(deleteAfter)
	lo, hi := shardBounds(shards)
	n, err := s.queries().DeleteUnclaimedWork(ctx, dbq.DeleteUnclaimedWorkParams{
		Column1: deleteSec,
		Column2: int32(limit),
		Column3: lo,
		Column4: hi,
	})
	return int(n), err
}

// SweepExpiredOrphans tears down composer-dropped children whose orphan-grace
// window has elapsed without a re-emit: finalizer kinds escalate to soft-delete
// (the kind's Deleter runs), leaf kinds are hard-deleted. The grace deadline is
// baked into resources.frozen_until by the composer at prune time, so there is
// no duration arg — the sweep only compares frozen_until to now() (a quarantined
// row's 'infinity' is never < now(), so it is never swept). Called on the
// reaper tick over its owned (contiguous) shard range; returns the row count.
func (s *Store) SweepExpiredOrphans(ctx context.Context, limit int, shards []int16) (int, error) {
	lo, hi := shardBounds(shards)
	n, err := s.queries().SweepExpiredOrphans(ctx, dbq.SweepExpiredOrphansParams{
		Column1: int32(limit),
		Column2: lo,
		Column3: hi,
	})
	return int(n), err
}

// SweepDeletable is the level-triggered backstop for reverse-dependency cascade
// delete: it hard-deletes any deletion_requested row whose finalizers are empty AND
// that has no remaining owned child AND no remaining dependent, so a marked teardown
// tree collapses bottom-up (a parent waits for its children/dependents). Bounded by
// limit, scoped to the shard range; carries its own zero-cost idle gate.
func (s *Store) SweepDeletable(ctx context.Context, limit int, shards []int16) (int, error) {
	lo, hi := shardBounds(shards)
	n, err := s.queries().SweepDeletable(ctx, dbq.SweepDeletableParams{
		Column1: int32(limit),
		Column2: lo,
		Column3: hi,
	})
	return int(n), err
}
