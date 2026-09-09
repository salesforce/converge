package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/salesforce/converge/internal/model"
)

// Resyncer is the drift-detection engine (Crossplane poll-loop parity).
// For each kind that opted into resync (the manifest's resync_interval_secs > 0,
// read from kind_config) it periodically re-pends SETTLED resources of that kind
// — without bumping generation — so their provider re-observes live health. A
// leaf worker re-probes and may report Ready=False (→ health_ok=false →
// is_ready demotes); a composite re-runs its rollup and aggregates
// descendant readiness into its own health_ok. Kinds that additionally
// set the manifest's resync_recomposes (carried here as ResyncKind.Recompose)
// also re-run their composer on each re-pend, drift-correcting composed
// children — see requeue_for_resync's p_recompose in 00001_schema.sql.
//
// Kinds with ResyncInterval == 0 are never registered here, so a
// deployment with no resync-enabled kind starts no Resyncer goroutine,
// never calls requeue_for_resync, and leaves resources.last_reconciled_at
// NULL on every row — zero cost, which keeps the 1M stress path (all kinds
// ResyncInterval=0) free of any resync work.

// ResyncKind is the per-kind resync configuration the sweeper carries:
// how often to re-pend, and whether the re-pend also forces a composer
// re-run (drift-correcting composed children). Built from kind_config by
// the control plane.
type ResyncKind struct {
	Interval  time.Duration
	Recompose bool
}

type Resyncer struct {
	Repo ResyncerRepo

	// kinds maps each opted-in kind to its resync config. Seeded at construction
	// from kind_config and LIVE-REFRESHED by the control plane on its tick
	// (SetKinds) so an operator's resync-interval/recompose edit converges with no
	// restart — the informer relist. Guarded by mu because tick() reads it on the
	// sweeper goroutine while SetKinds writes from the control loop. NOTE: this
	// retunes/adds/removes kinds AMONG a running Resyncer; a deployment that booted
	// with ZERO resync kinds has no Resyncer goroutine at all (NewResyncer returned
	// nil to keep the no-resync path zero-cost), so enabling resync on a
	// previously-none kind still needs a restart — a rare action.
	mu    sync.RWMutex
	kinds map[model.Kind]ResyncKind

	// Interval is how often the sweeper wakes to look for due rows. It
	// is the resolution of resync timing, not the interval itself — a
	// kind with ResyncInterval=5m is swept on whichever tick first finds
	// its rows older than 5m. Kept coarse so the due-row scan (one
	// indexed query per kind) is cheap relative to real work.
	Interval  time.Duration
	BatchSize int

	// Shards is the control pod's CURRENT sweep range, read lock-free each tick
	// via Shards.Snapshot(). A *ShardSet so the Resharder can swap it at runtime.
	Shards *ShardSet

	// OnSwept is an optional metrics observer (sweep, n>0). nil → no-op. Off the hot path.
	OnSwept func(sweep string, n int64)
}

// NewResyncer constructs a Resyncer for the given opted-in kinds. Returns
// nil when no kind opted in, so callers can skip starting a goroutine.
func NewResyncer(repo ResyncerRepo, kinds map[model.Kind]ResyncKind) *Resyncer {
	enabled := map[model.Kind]ResyncKind{}
	for k, rk := range kinds {
		if rk.Interval > 0 {
			enabled[k] = rk
		}
	}
	if len(enabled) == 0 {
		return nil
	}
	return &Resyncer{
		Repo:      repo,
		kinds:     enabled,
		Interval:  30 * time.Second,
		BatchSize: 500,
		Shards:    NewShardSet(AllShards()),
	}
}

// SetKinds atomically replaces the resync kind-map — the control plane calls it
// on its tick after re-reading kind_config, so an operator's resync edit
// (interval/recompose) takes effect on a running Resyncer with no restart. A
// kind dropped from the map simply stops being re-pended; one added (that was
// already resync-enabled at boot, hence this Resyncer exists) starts on the next
// tick. Safe to call concurrently with tick().
func (r *Resyncer) SetKinds(kinds map[model.Kind]ResyncKind) {
	enabled := make(map[model.Kind]ResyncKind, len(kinds))
	for k, rk := range kinds {
		if rk.Interval > 0 {
			enabled[k] = rk
		}
	}
	r.mu.Lock()
	r.kinds = enabled
	r.mu.Unlock()
}

// kindsSnapshot returns a stable copy of the kind-map for one tick, so the sweep
// iterates without holding the lock across its (DB-bound) per-kind work.
func (r *Resyncer) kindsSnapshot() map[model.Kind]ResyncKind {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[model.Kind]ResyncKind, len(r.kinds))
	for k, v := range r.kinds {
		out[k] = v
	}
	return out
}

func (r *Resyncer) Run(ctx context.Context) error {
	// No work_ready NOTIFY: resync is a periodic drift re-check; its re-pends
	// are picked up by the worker's failsafe poll (not latency-critical).
	// Idle == busy: the sweep cadence is fixed by Interval regardless of
	// whether a tick found due rows, because "due" is time-driven, not
	// backlog-driven — there's no burst to drain faster.
	// repollOnWork=false: a paced, time-driven sweep ("due" rows on Interval),
	// not a backlog drain. nil wakeup: no NOTIFY source to react to. hotWindow=0:
	// no post-work hot polling — "due" is time-driven, there's no burst to chase.
	pollLoop(ctx, "resyncer", r.Interval, r.Interval, 0, false, nil, r.tick)
	return nil
}

func (r *Resyncer) tick(ctx context.Context) (bool, error) {
	shards := r.Shards.Snapshot().Shards
	total := 0
	for kind, rk := range r.kindsSnapshot() {
		n, err := retryOnDeadlock(ctx, "resyncer", func(ctx context.Context) (int, error) {
			return r.Repo.RequeueForResync(ctx, kind, rk.Interval, r.BatchSize, shards, rk.Recompose)
		})
		if err != nil {
			return total > 0, fmt.Errorf("requeue for resync (kind=%s): %w", kind, err)
		}
		if n > 0 {
			slog.Debug("resyncer re-pended for drift check", "kind", kind, "count", n)
			total += n
			if r.OnSwept != nil {
				r.OnSwept("resync_repended", int64(n))
			}
		}
	}
	return total > 0, nil
}
