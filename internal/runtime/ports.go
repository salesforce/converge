package runtime

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// Ports — narrow interfaces the periodic drivers depend on. *store.Store
// satisfies all of them. Tests can substitute fakes by implementing
// these interfaces directly, without pgx or sqlc.

// ReaperRepo is what the Reaper calls each tick.
type ReaperRepo interface {
	ReapStaleWork(ctx context.Context, staleAfter time.Duration, limit int, shards []int16) (int, error)
	RequeueFailedAndPending(ctx context.Context, retryAfter time.Duration, limit int, shards []int16) (int, error)
	// RecountInflight rebuilds the per-kind concurrency-cap tally for the
	// reaper's shard range — the self-healing edge of the cap.
	RecountInflight(ctx context.Context, shards []int16) error
	// ReapStaleLifecycle frees lifecycle_outbox claims whose dispatcher stopped
	// heartbeating — the at-least-once backstop for reactor deliveries (the
	// dispatcher's own poll is the primary path).
	ReapStaleLifecycle(ctx context.Context, staleAfter time.Duration, limit int, shards []int16) error
	// DeleteUnclaimedWork hard-deletes work_queue rows unclaimed past deleteAfter
	// — abandoned tasks no worker ever picked up. GC, not recovery: a returning
	// worker's fleet gets a fresh row from requeue_failed_and_pending.
	DeleteUnclaimedWork(ctx context.Context, deleteAfter time.Duration, limit int, shards []int16) (int, error)
	// SweepExpiredOrphans tears down composer-dropped children whose orphan-grace
	// window elapsed without a re-emit (frozen_until < now()): finalizer kinds →
	// soft-delete, leaf kinds → hard delete. The deadline is baked into
	// frozen_until by the composer, so there is no duration arg.
	SweepExpiredOrphans(ctx context.Context, limit int, shards []int16) (int, error)
	// SweepDeletable is the level-triggered backstop for reverse-dependency cascade
	// delete: hard-deletes any deletion_requested row whose finalizers are empty AND
	// that has no remaining owned child AND no remaining dependent, so a marked
	// teardown tree collapses bottom-up (a parent waits for its children/dependents).
	SweepDeletable(ctx context.Context, limit int, shards []int16) (int, error)
}

// DrainerRepo is what the Drainer calls each tick. The cascade trigger
// in 00001_schema.sql handles downstream scheduling inside the drainer
// transaction, so the Go side has no per-batch follow-up work.
type DrainerRepo interface {
	DrainOutboxBatch(ctx context.Context, limit int, shards []int16) (int, error)
	// DrainRollupRechecks re-pends, at a fresh snapshot, the straddled rollup
	// roots the cascade queued in rollup_recheck — the deferred half that closes
	// the rollup straddle race. Empty + cheap on the common no-straddle path.
	DrainRollupRechecks(ctx context.Context, limit int, shards []int16) (int, error)
	// DrainScheduleRechecks schedules a band-scoped batch of the children a wide
	// compose armed in schedule_recheck (OPT-A) — the deferred, drain-paced
	// replacement for the post-commit ScheduleEligible that deadlocked. Empty +
	// cheap once a compose's children are all scheduled.
	DrainScheduleRechecks(ctx context.Context, limit int, shards []int16) (int, error)
}

// ResyncerRepo is what the Resyncer calls each tick: re-pend settled
// resources of one kind for a drift re-check.
type ResyncerRepo interface {
	RequeueForResync(ctx context.Context, kind model.Kind, resyncAfter time.Duration, limit int, shards []int16, recompose bool) (int, error)
}

// ReactorRepo is what the ReactorDispatcher calls each tick: claim due
// lifecycle transitions with matching bindings, and ack a delivered one. The
// claim_reactor_deliveries plpgsql function does the FOR UPDATE SKIP LOCKED +
// binding JOIN + resource/config resolution IN-DB, so the Go side just
// dispatches to the reactor and acks.
type ReactorRepo interface {
	ClaimReactorDeliveries(ctx context.Context, brokerID string, limit int, shards []int16) ([]store.ReactorDelivery, error)
	AckReactorDelivery(ctx context.Context, resourceID uuid.UUID, transition string, generation int64, bindingName string, claimEpoch int64) error
	// HeartbeatReactorClaims refreshes heartbeat_at on the SPECIFIC in-flight deliveries
	// the dispatcher holds (keyed by identity + the claim_epoch each was claimed under)
	// so a slow reactor isn't reaped mid-delivery. Epoch-fenced like the work
	// dispatcher's heartbeat — a row a concurrent claim/reap re-issued (bumped epoch)
	// drops out, so the refresh never 40P01-cycles with the re-claimer. Scoped to the
	// pod's shard range for partition pruning.
	HeartbeatReactorClaims(ctx context.Context, keys []store.ReactorLeaseKey, shards []int16) error
}

// SpecGCRepo is what the SpecGC sweeper calls each tick: reclaim
// superseded immutable spec bodies (roots keep keepN, children keep
// latest-1).
type SpecGCRepo interface {
	GCSpecHistory(ctx context.Context, keepN, limit int, shards []int16) (int, error)
}

// ClusterMemberReporterRepo is what the per-process ClusterMemberReporter calls:
// each beat UPSERTs this member's row in the cluster_members registry (static
// identity plus the LIVE in-flight count and connected-worker snapshot), and a
// clean shutdown deletes it (deregister).
type ClusterMemberReporterRepo interface {
	UpsertClusterMember(ctx context.Context, info store.ClusterMemberInfo, inFlight int, workers json.RawMessage) error
	DeleteClusterMember(ctx context.Context, memberID string) error
}

// ClusterMemberGCRepo is what the ControlPlane's ClusterMemberGC sweeper calls
// each tick: hard-delete registry rows whose heartbeat is older than ttl.
type ClusterMemberGCRepo interface {
	GCStaleClusterMembers(ctx context.Context, ttl time.Duration) (int, error)
}

// ResharderRepo is what the per-process Resharder calls each tick: compute this
// member's contiguous shard span from the live cluster_members view (its rank
// among same-role live members × range-partition of [0,total)). ok=false means
// the member owns nothing this tick (not yet registered, beat aged out, or more
// live members than shards).
type ResharderRepo interface {
	AssignMemberShards(ctx context.Context, memberID string, total int, livenessWindow time.Duration) (lo, hi int16, ok bool, err error)
}

// txBeginner is the ONE raw-pool capability the dispatch Loop needs beyond its
// Repo: opening a transaction for the compose fence (ApplyComposeResult runs in
// a single tx via Repo.WithTx). The Loop depends on this capability, not the
// concrete pool. pgx.Tx is the transaction contract itself, so the interface
// names it directly rather than re-abstracting it.
type txBeginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// DispatcherRepo is the data surface the work-dispatch Loop uses — the full set
// of reads/writes a task's lifecycle touches (claim/heartbeat/release, compose
// apply, outbox append, event/trace emit, rollup-readiness gate). WithTx returns
// a DispatcherRepo bound to tx so the compose fence stays a single transaction.
// *store.Store satisfies it (see the conformance block below), but the Loop
// depends only on this interface — the dispatch core carries no concrete store
// or pgx pool.
type DispatcherRepo interface {
	// Claim / lease lifecycle.
	WorkQueueTakeBatch(ctx context.Context, kind model.Kind, kindVersion int, taskType store.TaskType, brokerID string, limit int, shards []int16) ([]store.WorkTask, error)
	WorkQueueHeartbeat(ctx context.Context, ids []uuid.UUID, epochs []int64, lo, hi int16) error
	WorkQueueReleaseBroker(ctx context.Context, brokerID string) (int64, error)
	WorkQueueIncrementAttempts(ctx context.Context, id uuid.UUID, shardID int16) error

	// Reaction reads + rollup gate.
	GetComposedGen(ctx context.Context, id uuid.UUID) (int64, error)
	ListDescendants(ctx context.Context, rootID uuid.UUID) ([]store.ResourceRow, error)
	DescendantsSettled(ctx context.Context, rootID uuid.UUID) (bool, error)
	GetOperation(ctx context.Context, id uuid.UUID) (store.ResourceOperation, error)

	// Writes.
	AppendOutbox(ctx context.Context, a store.OutboxAppend) error
	EmitEvent(ctx context.Context, e store.Event) error
	ApplyComposeResult(ctx context.Context, parentID uuid.UUID, parentGeneration int64, workID uuid.UUID, claimEpoch int64, manifestVersion int64, children []model.ChildSpec, edges []model.DepEdge, configs []model.ProviderConfigSpec, pol store.ComposePolicy) (store.ComposeCounts, error)
	ArmScheduleRecheck(ctx context.Context, candidateIDs []uuid.UUID) error
	AnalyzeComposeTables(ctx context.Context) error

	// WithTx returns a DispatcherRepo whose queries run inside tx (the compose
	// fence). The concrete store's WithTx returns *store.Store; dispatcherRepo
	// (loop.go) adapts it to this interface.
	WithTx(tx pgx.Tx) DispatcherRepo
}

// dispatcherRepo adapts *store.Store to DispatcherRepo. The only method the
// concrete store can't satisfy structurally is WithTx: store.WithTx returns
// *store.Store (Go has no covariant returns), so this wrapper re-wraps the
// tx-bound store as a DispatcherRepo. Every other method is promoted from the
// embedded *store.Store unchanged (zero-cost — it's an embedded pointer).
type dispatcherRepo struct{ *store.Store }

// NewDispatcherRepo wraps a store as a DispatcherRepo for the work-dispatch Loop.
// The root builds the store once and hands the Loop this interface, so the
// dispatch core carries no concrete store or pgx pool.
func NewDispatcherRepo(s *store.Store) DispatcherRepo { return dispatcherRepo{s} }

// WithTx returns a DispatcherRepo whose queries run inside tx (the compose
// fence), re-wrapping the tx-bound *store.Store.
func (d dispatcherRepo) WithTx(tx pgx.Tx) DispatcherRepo {
	return dispatcherRepo{d.Store.WithTx(tx)}
}

var (
	_ ReaperRepo                = (*store.Store)(nil)
	_ DrainerRepo               = (*store.Store)(nil)
	_ ResyncerRepo              = (*store.Store)(nil)
	_ ReactorRepo               = (*store.Store)(nil)
	_ SpecGCRepo                = (*store.Store)(nil)
	_ ClusterMemberReporterRepo = (*store.Store)(nil)
	_ ClusterMemberGCRepo       = (*store.Store)(nil)
	_ ResharderRepo             = (*store.Store)(nil)
	_ DispatcherRepo            = dispatcherRepo{}
	_ txBeginner                = (*pgxpool.Pool)(nil)
)
