package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
)

// ControlPlane runs the schema-owning, sweeper-running parts of the
// system:
//
//   - runtime.Reaper:   reaps stale work_queue rows; requeues failed/pending.
//   - runtime.Drainer:  drains work_outbox into resources + work_queue.
//   - runtime.Resyncer: per-kind drift detection (only when some kind
//     opted into drift via kind_config.resync_interval_secs). nil otherwise.
//   - runtime.SpecGC:   reclaims superseded immutable spec bodies (roots keep
//     last-N, children keep latest-1); always on.
//   - runtime.ClusterMemberGC: reclaims stale cluster_members registry rows past
//     their TTL (cluster-wide, not shard-scoped).
//
// It owns the schema but NOT any provider: it reads the per-kind resync policy
// from the kind_config DB table, never from a constructed registry — so a
// control pod dials no external client. (The cap half of kind_config is read by
// the work_queue claim directly; the control plane never touches it.)

// kindConfigLister is the ONE data read the ControlPlane itself does (beyond
// what its sweepers do): re-listing kind_config on a cadence to live-refresh the
// Resyncer's kind-map. Injected as this narrow role interface — the plane holds
// no pool or concrete store. *store.Store satisfies it.
type kindConfigLister interface {
	ListKindConfig(ctx context.Context) ([]store.KindConfig, error)
}

type ControlPlane struct {
	kindConfigs kindConfigLister

	reaper          *runtime.Reaper
	drainer         *runtime.Drainer
	resyncer        *runtime.Resyncer
	specgc          *runtime.SpecGC
	clusterMemberGC *runtime.ClusterMemberGC

	// kindConfigRefresh is the cadence of the Resyncer kind-map relist (see
	// refreshResyncKinds). Defaults to kindConfigRefreshInterval.
	kindConfigRefresh time.Duration

	// swept is the OTel sweeper-rows counter, set ONCE in NewControlPlane from
	// cfg.Swept (in the struct literal, BEFORE Start launches the sweeper goroutines)
	// — so it's race-free and the sweepers' OnSwept hooks see it from their first
	// tick. nil when metrics are off; onSwept nil-guards the Add. Do NOT add an
	// after-Start setter: that WOULD race the running sweepers reading this field.
	swept metric.Int64Counter

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// onSwept records n rows a sweeper acted on under the given sweep label. Safe when
// the counter is unset (nil → no-op) and for n<=0 (skipped). cp.swept is written
// once at construction and only read here, so no synchronization is needed.
func (cp *ControlPlane) onSwept(sweep string, n int64) {
	if cp.swept == nil || n <= 0 {
		return
	}
	cp.swept.Add(context.Background(), n, metric.WithAttributes(attribute.String("sweep", sweep)))
}

func NewControlPlane(ctx context.Context, cfg *ControlPlaneConfig) (*ControlPlane, error) {
	if cfg.Pool == nil {
		return nil, fmt.Errorf("control plane: Pool is required")
	}
	if cfg.RetryAfter == 0 {
		cfg.RetryAfter = DefaultRetryAfter
	}
	if cfg.SweeperInterval == 0 {
		cfg.SweeperInterval = DefaultSweeperInterval
	}

	if err := verifyShardModulus(ctx, cfg.Pool); err != nil {
		return nil, fmt.Errorf("shard modulus check: %w", err)
	}

	// Read the per-kind settings from kind_config — DATA derived from each applied
	// manifest by the kind_manifest trigger, NOT a constructed registry. This is
	// what lets a control pod dial NO external client: it never calls
	// Provider.Setup, it just SELECTs. NOTE the caps are NOT copied anywhere: the
	// work_queue claim reads max_inflight from kind_config DIRECTLY (one PK probe),
	// so there is no kind_caps projection to reconcile. The control plane reads
	// kind_config only for the Resyncer kind map below — and re-reads it on a
	// cadence so an operator edit takes effect with no restart (the informer relist
	// pattern; see refreshResyncKinds).
	// The control plane is a construction site: build the shared store + LISTEN
	// adapter ONCE from the pool here and inject the role interfaces into each
	// sweeper factory (store.New is a root/construction-only call; the sweepers
	// themselves hold only their narrow *Repo interface + a Listener, never the
	// pool). *store.Store satisfies every sweeper's role interface.
	st := store.New(cfg.Pool)
	listener := runtime.NewPgxListener(cfg.Pool)

	cfgByKind, err := st.ListKindConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("list kind_config: %w", err)
	}

	// All four control sweepers share ONE shard range (cfg.Shards): a control
	// member owns a single contiguous tile of [0,NumShards) for its role, swept
	// uniformly by drain/reap/specgc/resync. When cfg.Shards is set (the normal
	// case — dynamic ShardSet or an explicit pin) every sweeper points at the
	// SAME *ShardSet, so a Resharder swap re-points all of them at once; when
	// nil they keep the constructor default (AllShards).
	wr := runtime.NewReaper(st)
	// NewReaper carries the default StaleAfter; only override when configured.
	if cfg.SweeperStaleAfter > 0 {
		wr.StaleAfter = cfg.SweeperStaleAfter
	}
	// Same pattern for the abandoned-work GC window: NewReaper carries the 24h
	// default; only override when explicitly configured (>0). A 0 keeps the
	// default; set SWEEPER_UNCLAIMED_DELETE_AFTER negative is not supported.
	if cfg.SweeperUnclaimedDeleteAfter > 0 {
		wr.UnclaimedDeleteAfter = cfg.SweeperUnclaimedDeleteAfter
	}
	wr.RetryAfter = cfg.RetryAfter
	if cfg.Shards != nil {
		wr.Shards = cfg.Shards
	}

	dr := runtime.NewDrainer(st, listener)
	if cfg.Shards != nil {
		dr.Shards = cfg.Shards
	}

	// Resyncer is built only from kinds that opted into drift detection
	// (kind_config.resync_interval_secs > 0). NewResyncer returns nil when no
	// kind opted in, so no goroutine starts and the resync SQL path is
	// never touched. It shares the control pod's shard range (cfg.Shards) so
	// disjoint control pods sweep disjoint resources. The kinds come from the
	// kind_config read above — no registry, no client.
	kinds := map[model.Kind]runtime.ResyncKind{}
	for _, kc := range cfgByKind {
		if kc.ResyncInterval > 0 {
			kinds[kc.Kind] = runtime.ResyncKind{
				Interval:  kc.ResyncInterval,
				Recompose: kc.ResyncRecomposes,
			}
		}
	}
	rs := runtime.NewResyncer(st, kinds)
	if rs != nil {
		if cfg.Shards != nil {
			rs.Shards = cfg.Shards
		}
		if cfg.ResyncSweepInterval > 0 {
			rs.Interval = cfg.ResyncSweepInterval
		}
	}

	// SpecGC reclaims superseded immutable spec bodies (roots keep last-N,
	// children keep latest-1). Always on — every deployment writes specs —
	// but cheap: coarse cadence, the control pod's shard range, off the hot
	// path.
	sg := runtime.NewSpecGC(st)
	if cfg.Shards != nil {
		sg.Shards = cfg.Shards
	}

	// ClusterMemberGC reclaims stale cluster_members registry rows (members
	// that stopped heartbeating without deregistering) past a TTL — the delayed
	// garbage collector for the running-fleet view and the fallback to a clean
	// shutdown's own deregister. Cluster-wide (no shard scoping; the registry is
	// a dozens-of-rows table, one row per member), so every control pod runs the
	// same idempotent DELETE; whichever fires first reclaims, the rest no-op.
	cg := runtime.NewClusterMemberGC(st)

	refresh := cfg.KindConfigRefreshInterval
	if refresh <= 0 {
		refresh = kindConfigRefreshInterval
	}

	cp := &ControlPlane{
		kindConfigs:       st,
		reaper:            wr,
		drainer:           dr,
		resyncer:          rs,
		specgc:            sg,
		clusterMemberGC:   cg,
		kindConfigRefresh: refresh,
		swept:             cfg.Swept,
	}
	// Wire the OTel sweeper-rows observer AT CONSTRUCTION (before Start launches the
	// sweeper goroutines → no data race on the OnSwept field). onSwept is a no-op when
	// cfg.Swept is nil (metrics off). The drainer stays un-instrumented — its per-batch
	// count is the hot outbox drain, not a "swept" row set.
	wr.OnSwept = cp.onSwept
	sg.OnSwept = cp.onSwept
	cg.OnSwept = cp.onSwept
	if rs != nil {
		rs.OnSwept = cp.onSwept
	}
	return cp, nil
}

func (cp *ControlPlane) Start(ctx context.Context) error {
	loopCtx, cancel := context.WithCancel(ctx)
	cp.cancel = cancel

	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		_ = cp.reaper.Run(loopCtx)
	}()
	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		_ = cp.drainer.Run(loopCtx)
	}()
	if cp.resyncer != nil {
		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			_ = cp.resyncer.Run(loopCtx)
		}()
		// Live-refresh the Resyncer's kind-map from kind_config on a coarse
		// cadence (the informer "relist"): an operator editing a kind's
		// resync_interval_secs / resync_recomposes is picked up within one
		// interval with NO restart. Off the hot path — one tiny SELECT every
		// kindConfigRefreshInterval. Only retunes/adds/removes kinds among a
		// RUNNING Resyncer (a deployment that booted with zero resync kinds has
		// no resyncer here; enabling resync on a none kind still needs a restart).
		cp.wg.Add(1)
		go func() {
			defer cp.wg.Done()
			cp.refreshResyncKinds(loopCtx)
		}()
	}
	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		_ = cp.specgc.Run(loopCtx)
	}()
	cp.wg.Add(1)
	go func() {
		defer cp.wg.Done()
		_ = cp.clusterMemberGC.Run(loopCtx)
	}()
	return nil
}

func (cp *ControlPlane) Stop(_ context.Context) error {
	if cp.cancel != nil {
		cp.cancel()
	}
	cp.wg.Wait()
	return nil
}

// kindConfigRefreshInterval is the DEFAULT cadence for re-reading kind_config to
// live-refresh the Resyncer's kind-map. Coarse — resync re-tuning is a rare
// operator action and not latency-critical; the read is one tiny SELECT.
// Overridable per-ControlPlane via ControlPlaneConfig.KindConfigRefreshInterval
// (cp.kindConfigRefresh) for tests.
const kindConfigRefreshInterval = 15 * time.Second

// refreshResyncKinds periodically re-reads kind_config and pushes the resync
// kind-map into the running Resyncer (the informer relist), so an operator's
// resync edit converges with no restart. Cheap and off the hot path; a transient
// read error keeps the prior map (a DB blip never wipes resync config).
func (cp *ControlPlane) refreshResyncKinds(ctx context.Context) {
	t := time.NewTicker(cp.kindConfigRefresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		rows, err := cp.kindConfigs.ListKindConfig(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("control: refresh resync kinds", "err", err)
			continue
		}
		kinds := map[model.Kind]runtime.ResyncKind{}
		for _, kc := range rows {
			if kc.ResyncInterval > 0 {
				kinds[kc.Kind] = runtime.ResyncKind{Interval: kc.ResyncInterval, Recompose: kc.ResyncRecomposes}
			}
		}
		cp.resyncer.SetKinds(kinds)
	}
}
