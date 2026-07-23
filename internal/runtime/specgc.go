package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// SpecGC bounds the spec_versions root-history log. The live spec is inline
// on resources.spec; only ROOT Apply events append a copy to spec_versions
// (the user-facing rollback/checkout history). Composed children carry spec
// inline and are never versioned, so they never appear here. This sweeper
// trims each root's log to KeepN newest authored revisions.
//
// Entirely off the hot path: a coarse cadence, shard-scoped to this pod's
// slice, LIMIT-bounded per tick. It needs NO work_queue/work_outbox coupling
// and no live-body guard — the live spec is a separate inline column, so
// trimming an aged-out log row never touches what a worker is reconciling.
// A version that a checkout previously copied into the live spec keeps
// working even if its log row ages out (the bytes already live on
// resources.spec); only the navigable log entry is reclaimed.
type SpecGC struct {
	Repo      SpecGCRepo
	KeepN     int
	Interval  time.Duration
	BatchSize int

	IdleInterval time.Duration

	// Shards is the control pod's CURRENT sweep range, read lock-free each tick
	// via Shards.Snapshot(). A *ShardSet so the Resharder can swap it at runtime.
	Shards *ShardSet

	// OnSwept is an optional metrics observer (sweep, n>0). nil → no-op. Off the hot path.
	OnSwept func(sweep string, n int64)
}

// NewSpecGC builds the sweeper with production-sane defaults. KeepN=20
// matches the agreed root retention depth.
func NewSpecGC(repo SpecGCRepo) *SpecGC {
	return &SpecGC{
		Repo:         repo,
		KeepN:        20,
		Shards:       NewShardSet(AllShards()),
		Interval:     60 * time.Second,
		IdleInterval: 60 * time.Second,
		BatchSize:    500,
	}
}

func (g *SpecGC) Run(ctx context.Context) error {
	// repollOnWork=false: the tick BURSTS its batches internally (see tick)
	// until the log is trimmed, so it needs no pollLoop-level re-poll and stays
	// on its coarse interval once caught up. nil wakeup: no NOTIFY source.
	// hotWindow=0: no post-work hot poll.
	pollLoop(ctx, "specgc", g.Interval, g.IdleInterval, 0, false, nil, g.tick)
	return nil
}

// tick reclaims superseded spec bodies in a BURST: it loops batches back-to-
// back until one comes back short of BatchSize, so a large accumulated log
// (churny re-applies that pushed many roots past KeepN) is trimmed in one
// tick instead of one batch per Interval. At the coarse 60s cadence, draining
// a backlog one 500-row batch at a time would take hours and leave
// spec_versions (hence disk) bloated meanwhile. A saturated batch
// (== BatchSize) is the signal there's more to trim; a short batch means we
// caught up, so pollLoop returns to its paced Interval. Common case (no
// backlog) is unchanged: the first batch comes back short and the loop exits.
//
// retryOnDeadlock guards against the rare cycle with a concurrent repoint (the
// FOR UPDATE SKIP LOCKED on candidates makes that vanishingly unlikely, but
// the repoint UPDATE and this DELETE both touch resource_specs).
func (g *SpecGC) tick(ctx context.Context) (bool, error) {
	shards := g.Shards.Snapshot().Shards
	didWork := false
	for {
		collected, err := retryOnDeadlock(ctx, "specgc", func(ctx context.Context) (int, error) {
			return g.Repo.GCSpecHistory(ctx, g.KeepN, g.BatchSize, shards)
		})
		if err != nil {
			return didWork, fmt.Errorf("gc spec history: %w", err)
		}
		if collected > 0 {
			slog.Debug("specgc reclaimed superseded spec bodies", "count", collected)
			if g.OnSwept != nil {
				g.OnSwept("specgc_reclaimed", int64(collected))
			}
		}
		didWork = didWork || collected > 0
		if collected < g.BatchSize {
			break
		}
	}
	return didWork, nil
}
