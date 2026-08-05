package runtime

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/salesforce/converge/internal/store"
)

// DefaultMemberHeartbeatEvery is the cadence at which every process UPSERTs its
// cluster_members row (the running-fleet view). It is the ONE authoritative
// member-liveness cadence: DefaultMeshLivenessWindow (mesh peer eviction) and the
// Resharder's shard-tiling liveness window both derive from 3× this, and the API
// marks a member NotReady after ~3 missed beats. Short enough that a crashed member
// is excluded from routing + tiling within ~one window (3× = 30s), not the
// minutes-long ClusterMemberGC TTL; the UPSERT is a single PK write on a tiny table,
// so a 10s cadence is negligible even at fleet scale. cmd/converge's
// MEMBER_HEARTBEAT_EVERY overrides it.
const DefaultMemberHeartbeatEvery = 10 * time.Second

// DefaultMeshLivenessWindow is how recent a member's heartbeat must be to count as
// LIVE for routing/tiling — 3× the heartbeat cadence, so a single slow/lost beat
// never drops a live member (which would churn the mesh + tiling) but a genuinely
// crashed one is excluded within one window. The SINGLE liveness window the mesh
// (peer eviction) and the Resharder (shard tiling) share, so they never disagree on
// who is alive. Kept well below the ClusterMemberGC TTL so an excluded member is also
// on its way to being reclaimed from the registry.
const DefaultMeshLivenessWindow = 3 * DefaultMemberHeartbeatEvery

// ClusterMemberReporter is the per-PROCESS heartbeat into the cluster_members
// registry — the running-fleet view ("like kubectl get nodes", but each row is
// a cluster MEMBER: one instance of the app identified by its role). Exactly
// ONE runs per process (constructed in cmd/converge/main.go, NOT in the
// engine), so a role=all process — which runs both the control duties and the
// broker duty — still writes a SINGLE row.
//
// Each beat UPSERTs the member's STATIC identity (Info: id/role/shards/
// config/version/host/pid/started_at) plus the LIVE in-flight count read via
// the InFlight hook (a BROKER concept; nil → 0 on control/react). The UPSERT
// stamps last_heartbeat with the DB clock
// (now()), so liveness is skew-free; started_at is preserved across beats.
//
// Off every hot path: a single PK UPSERT on a dozens-of-rows table on a 30s
// cadence from this one goroutine — it touches no work_queue/outbox/resources
// row and is never on a per-task path.
type ClusterMemberReporter struct {
	Repo ClusterMemberReporterRepo

	// Info is the static per-process payload, gathered once at construction.
	Info store.ClusterMemberInfo

	// InFlight returns the member's current in-flight task count for the beat.
	// nil on a control-only member (no dispatcher) → reported as 0.
	InFlight func() int

	// Shards, when set, is the live source of this member's owned span: each
	// beat reads its current [lo, hi] bounds and stamps Info.Shards before the
	// UPSERT, so the registry's `shards` column tracks DYNAMIC reassignment
	// (the Resharder swaps the same ShardSet). nil → Info.Shards is reported
	// as-is (static/explicit-override members, and the unit tests).
	Shards *ShardSet

	// Workers, when set, is the live source of this member's connected-worker
	// snapshot: each beat calls it and writes the result to the workers JSONB
	// column, so the cluster view tracks dumb workers connecting/dropping. A
	// broker wires it to its ConnectedWorkers snapshot; nil (control/react members
	// and the unit tests) → the column is written "[]". Distinct from the STATIC
	// Info.Config (connect_addr + runtime knobs), which never changes across beats.
	Workers func() json.RawMessage

	// Interval is the beat cadence (default 30s — 3 beats inside the API's 90s
	// NotReady window, well under the GC's 5m TTL).
	Interval time.Duration

	// wakeup is a buffered-1 coalescing channel: Trigger() signals it to force
	// an off-cadence beat (used by the Resharder to push a fresh `shards` span
	// the instant a reshard lands, instead of waiting out Interval).
	wakeup chan struct{}

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewClusterMemberReporter builds a reporter for the given static identity. The
// caller sets InFlight (and may override Interval) before Start.
func NewClusterMemberReporter(repo ClusterMemberReporterRepo, info store.ClusterMemberInfo) *ClusterMemberReporter {
	return &ClusterMemberReporter{
		Repo:     repo,
		Info:     info,
		Interval: DefaultMemberHeartbeatEvery,
		wakeup:   make(chan struct{}, 1),
	}
}

// Trigger requests an immediate off-cadence beat (non-blocking, coalescing).
// Called by the Resharder's OnChange so a reshard's new shard span lands in the
// registry promptly rather than after the next Interval. Safe before Start (the
// buffered slot just holds the request until the loop runs).
func (r *ClusterMemberReporter) Trigger() { notify(r.wakeup) }

// Start fires one beat synchronously (so the row appears immediately, not
// after the first interval), then runs the periodic beat in a tracked
// goroutine until Stop. The synchronous first beat is best-effort — a failure
// is logged, not fatal; the next tick retries.
func (r *ClusterMemberReporter) Start(ctx context.Context) {
	if err := r.beat(ctx); err != nil {
		slog.Warn("cluster member reporter initial beat", "member", r.Info.MemberID, "err", err)
	}
	loopCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		// repollOnWork=false, hotWindow=0: a fixed-cadence reporter. Interval==
		// IdleInterval so the beat stays uniform (tick always returns "no work").
		// wakeup is Trigger()'s off-cadence beat (a reshard pushing its new
		// shard span) — not a backlog source, just a "beat now" nudge.
		pollLoop(loopCtx, "cluster-member-reporter", r.Interval, r.Interval, 0, false, r.wakeup, r.tick)
	}()
}

// Stop cancels the beat loop and joins the goroutine. It does NOT delete the
// registry row — call Deregister for that. Stopping the beat FIRST guarantees
// no beat can re-UPSERT the row after a subsequent Deregister wins the race.
func (r *ClusterMemberReporter) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

// Deregister deletes this member's registry row so the cluster view drops it
// immediately on a clean shutdown (instead of waiting out the GC TTL). Call it
// AFTER Stop, as the last shutdown step, with a deadline-bounded ctx: a
// shutting-down member must never hang waiting on the DB if there's a network
// blip — on error/timeout we just exit and let the ClusterMemberGC reclaim the
// row. Best-effort by contract; the caller logs the error.
func (r *ClusterMemberReporter) Deregister(ctx context.Context) error {
	return r.Repo.DeleteClusterMember(ctx, r.Info.MemberID)
}

// tick adapts beat to the pollLoop signature. The reporter never "finds work"
// in the sweeper sense, so it always reports (false, err) and paces at
// Interval regardless of outcome.
func (r *ClusterMemberReporter) tick(ctx context.Context) (bool, error) {
	return false, r.beat(ctx)
}

// beat reads the live in-flight count (and, in dynamic mode, the live owned
// shard span) and UPSERTs the registry row. Single-writer: only ever called by
// the reporter's own loop goroutine (and the synchronous initial beat before it
// launches), so mutating Info.Shards here needs no lock.
func (r *ClusterMemberReporter) beat(ctx context.Context) error {
	inFlight := 0
	if r.InFlight != nil {
		inFlight = r.InFlight()
	}
	if r.Shards != nil {
		// Reflect the CURRENT dynamic assignment: an empty range (the member
		// owns nothing — e.g. before the Resharder's first assign, or when
		// there are more live members than shards) reports NULL shards.
		lo, hi := r.Shards.Bounds()
		if hi < lo {
			r.Info.Shards = nil
		} else {
			r.Info.Shards = &[2]int16{lo, hi}
		}
	}
	// The connected-worker snapshot is LIVE (changes as streams connect/drop), so
	// it's a per-beat argument to the UPSERT — written to the workers column, NOT
	// stamped onto the static Info.Config. nil hook (control/react) → "[]".
	var workers json.RawMessage
	if r.Workers != nil {
		workers = r.Workers()
	}
	return r.Repo.UpsertClusterMember(ctx, r.Info, inFlight, workers)
}
