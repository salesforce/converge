package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// ─────────────────────────────────────────────────────────────────────────
// Work queue: claim batch, heartbeat, append outbox results.
// ─────────────────────────────────────────────────────────────────────────

// TaskType mirrors the schema's task_type enum.
type TaskType string

const (
	TaskReconcile TaskType = "reconcile"
	TaskDelete    TaskType = "delete"
	TaskOperate   TaskType = "operate"
)

// WorkTask is one in-flight unit of work the Loop owns.
type WorkTask struct {
	ID         uuid.UUID
	ResourceID uuid.UUID
	Kind       model.Kind
	// KindVersion is the web-API version this task was claimed for (work_queue.kindVersion,
	// carried from the claim). The broker routed it here because a worker serving
	// this (kind, kindVersion) is connected; the reaction engine resolves the reaction
	// from the (kind, kindVersion) manifest and carries it to the worker in StageTask.
	KindVersion int
	TaskType    TaskType
	OpID        *uuid.UUID
	Generation  int64
	Attempts    int
	Spec        json.RawMessage
	// ProviderConfig is the per-task CLONE of the resource's CUSTOM config
	// (work_queue.provider_config), snapshotted at schedule time. nil means
	// "no custom override — the provider uses its kind default config only".
	// The runtime hands it to the worker, which merges kind-default ⊕ this
	// (this wins per field) to get the effective config for the task.
	ProviderConfig json.RawMessage
	// ProviderBundle is the per-task CLONE of the resource's CUSTOM config BUNDLE
	// (work_queue.provider_bundle — the opaque artifact), snapshotted at schedule
	// time. nil means "no bundle override — use the kind default bundle". Unlike
	// ProviderConfig it REPLACES the default wholesale at the worker (opaque bytes
	// can't merge); see converge.EffectiveBundle.
	ProviderBundle []byte
	// ShardID is the row's partition key (hash of ResourceID). Carried
	// from the claim's RETURNING so heartbeat / increment / delete /
	// re-pend can prune to the row's single partition — work_queue is
	// RANGE-partitioned by shard_id.
	ShardID int16
	// TaskDeadline is the kind's per-task timeout, read LIVE from kind_config
	// in the claim (not the in-memory registry), so an operator's edit takes
	// effect on the next claim with no restart. 0 means no deadline.
	TaskDeadline time.Duration
	// ManifestVersion is the kind_manifest content hash this task was enqueued
	// under (work_queue.manifest_version, carried from the claim). The reaction
	// engine passes it to the fenced AppendOutbox so a result produced under a
	// stale manifest can't commit. 0 = no manifest version (inert fence).
	ManifestVersion int64
	// ClaimEpoch is the strict monotonic fencing token stamped on this claim
	// (work_queue.claim_epoch, carried from the claim's RETURNING). It rides the
	// StageTask to the worker and is echoed on the result; the fenced AppendOutbox /
	// StampComposedGen land the write only if the row still bears this exact epoch,
	// so a stale/zombie/misrouted result no-ops. Owner-independent: any broker
	// holding this token writes the fenced result.
	ClaimEpoch int64
}

// WorkQueueTakeBatch claims up to limit pending tasks of the given
// (kind, kindVersion, taskType) from the worker's assigned shards. kindVersion is an exact
// filter so a claim only takes rows of the kindVersion the calling dispatcher pair
// serves — the broker/dispatcher never mixes kind versions (kindVersion is NOT in the pending
// index; the in-RAM subscriber gate decides eligibility, this filter is the
// correctness backstop and rides the fetched tuples, sargable on (kind,
// task_type, shard_id)).
func (s *Store) WorkQueueTakeBatch(ctx context.Context, kind model.Kind, kindVersion int, taskType TaskType, brokerID string, limit int, shards []int16) ([]WorkTask, error) {
	lo, hi := shardBounds(shards)
	rows, err := s.queries().WorkQueueTakeBatch(ctx, dbq.WorkQueueTakeBatchParams{
		Kind:        string(kind),
		KindVersion: int32(kindVersion),
		BrokerID:    &brokerID,
		TaskType:    dbq.TaskType(taskType),
		ShardLo:     lo,
		ShardHi:     hi,
		Lim:         int32(limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]WorkTask, len(rows))
	for i, r := range rows {
		var opID *uuid.UUID
		if r.OpID.Valid {
			id := uuid.UUID(r.OpID.Bytes)
			opID = &id
		}
		out[i] = WorkTask{
			ID:              r.ID,
			ResourceID:      r.ResourceID,
			Kind:            model.Kind(r.Kind),
			KindVersion:     int(r.KindVersion),
			TaskType:        TaskType(r.TaskType),
			OpID:            opID,
			Generation:      r.Generation,
			Attempts:        int(r.Attempts),
			Spec:            r.Spec,
			ProviderConfig:  json.RawMessage(r.ProviderConfig),
			ProviderBundle:  r.ProviderBundle,
			ShardID:         r.ShardID,
			TaskDeadline:    secsToDur(r.TaskDeadlineSecs),
			ManifestVersion: r.ManifestVersion,
			ClaimEpoch:      r.ClaimEpoch,
		}
	}
	return out, nil
}

// WorkQueueHeartbeat refreshes heartbeat_at for the (taskID, epoch) pairs a live
// worker attested within the liveness window (the broker relays the worker's
// WorkHeartbeat here). The two slices are positional (taskIDs[i] ↔ epochs[i]). lo/hi
// bound the shard span the heartbeat must cover for partition pruning (work_queue is
// RANGE-partitioned by shard_id) — the (id, epoch) pairs are the real filter, the
// BETWEEN just prunes the Append.
//
// FENCED on claim_epoch: the epoch both scopes the refresh to THIS claim and rejects
// a stale extension — a lease reaped and re-issued (bumping the epoch) can't be kept
// alive by its prior holder. heartbeat_at therefore attests that a LIVE worker is
// executing the task, not merely that the broker process is up.
//
// CONTRACT: lo/hi are the min/max shard_id of the ACTUAL attested tasks (see the
// dispatcher's inFlightSet.snapshot), NOT the dispatcher's current OWNERSHIP range.
// This decouples the lease-refresh from ownership so a reshard that SHRINKS this
// pod's range mid-task can't strand an already-claimed task outside the pruning
// window and let the reaper reclaim it while it's still running — the heartbeat keeps
// covering every task this pod actually holds, wherever its shard now falls. Empty id
// list → no-op.
func (s *Store) WorkQueueHeartbeat(ctx context.Context, taskIDs []uuid.UUID, epochs []int64, lo, hi int16) error {
	if len(taskIDs) == 0 {
		return nil
	}
	return s.queries().WorkQueueHeartbeat(ctx, taskIDs, epochs, lo, hi)
}

// WorkQueueIncrementAttempts bumps the row's attempts counter. shardID
// is the row's partition key, for partition pruning.
func (s *Store) WorkQueueIncrementAttempts(ctx context.Context, id uuid.UUID, shardID int16) error {
	return s.queries().WorkQueueIncrementAttempts(ctx, dbq.WorkQueueIncrementAttemptsParams{
		ID:      id,
		ShardID: shardID,
	})
}

// WorkQueueReleaseBroker frees every claim this pod (brokerID) holds, so a
// surviving pod can re-claim the work immediately on graceful shutdown instead
// of waiting out the reaper's stale window. Returns the number of rows freed.
// See the WorkQueueReleaseBroker query: it nulls broker_id only and leaves the
// kind_inflight cap tally to the reaper's recount_inflight self-heal.
func (s *Store) WorkQueueReleaseBroker(ctx context.Context, brokerID string) (int64, error) {
	return s.queries().WorkQueueReleaseBroker(ctx, &brokerID)
}

// WorkQueueReleaseTasks frees a SPECIFIC subset (taskIDs) of the claims this
// broker (brokerID) holds — the broker-tier fast path for a dropped worker
// stream: the broker NULLs broker_id for exactly that stream's in-flight tasks
// so a surviving worker re-claims in milliseconds instead of waiting out the
// reaper's stale window, while the broker's OTHER leases stay intact. lo/hi
// bound the shard range for partition pruning (mirrors WorkQueueHeartbeat).
// Returns the number of rows freed. No-op for an empty id list.
func (s *Store) WorkQueueReleaseTasks(ctx context.Context, brokerID string, taskIDs []uuid.UUID, lo, hi int16) (int64, error) {
	if len(taskIDs) == 0 {
		return 0, nil
	}
	return s.queries().WorkQueueReleaseTasks(ctx, dbq.WorkQueueReleaseTasksParams{
		BrokerID: &brokerID,
		Column2:  taskIDs,
		Column3:  lo,
		Column4:  hi,
	})
}

// WorkQueueMarkWorkerBatch stamps worker_id (the executing worker) onto a BATCH
// of claimed rows for UI attribution ("running on <worker>"). brokerID is this
// broker's claim (broker_id) so each scoped UPDATE only touches a row it still owns;
// a lost race is a harmless no-op. The three slices are positional + equal length.
// Best-effort, coalesced — never on a fence/hot path.
func (s *Store) WorkQueueMarkWorkerBatch(ctx context.Context, brokerID string, ids []uuid.UUID, workerIDs []string, shards []int16) error {
	return s.queries().WorkQueueMarkWorkerBatch(ctx, brokerID, ids, workerIDs, shards)
}

// AppendOutbox is the polymorphic outbox append.
//
//	reconcile -> coalesced status + synced_gen + health_ok + conditions
//	             + value-flow substitution
//	delete    -> strip finalizer string; hard-delete if last
//	operate   -> update resource_operations.{state, output, error_message}
//
// HealthOK is the Ready/health axis: nil means "the worker said nothing
// about health" (drainer leaves resources.health_ok untouched), non-nil
// sets the scalar. Conditions is the optional pre-marshaled JSONB array
// of {type,status,reason,message} the drainer upserts into
// resource_conditions on transition (nil/empty => nothing to write).
type OutboxAppend struct {
	WorkID             uuid.UUID
	ResourceID         uuid.UUID
	TaskType           TaskType
	Succeeded          bool
	ObservedGeneration int64
	// ClaimEpoch is the fencing token the task carried (work_queue.claim_epoch at
	// claim). The append is FENCED on it: the row lands only if work_queue still
	// bears this exact epoch for this work_id. A reaped/re-issued claim bumped the
	// epoch, so a stale/zombie/misrouted result no longer matches and writes nothing
	// — see the AppendOutbox query's fencing-token comment. Owner-independent: ANY
	// broker holding this token may write.
	ClaimEpoch int64
	// ShardID is the row's partition key (task.ShardID = shard_of(ResourceID)),
	// passed EXPLICITLY so the fenced INSERT prunes to ONE work_queue/work_outbox
	// partition instead of locking all 16 (the shard_of($resource) function form
	// defeats prepared-plan partition pruning). See the AppendOutbox query.
	ShardID          int16
	AdvanceSyncedGen bool
	HealthOK         *bool
	Conditions       json.RawMessage
	// Failed marks a hard reconcile failure: the drainer stamps
	// resources.failure_gen = ObservedGeneration (→ phase='Failed'). Set
	// by recordFailed for reconcile tasks; false on every success.
	Failed bool
	// Terminal marks that failure non-retryable (model.Terminal): the
	// drainer sets failure_terminal so the scheduler stops re-queuing it.
	// Only meaningful when Failed.
	Terminal      bool
	OpID          *uuid.UUID
	Payload       json.RawMessage
	ErrorMessage  string
	FinalizerName string
	// ManifestVersion is the kind_manifest content hash the worker ran the
	// reaction under (0 = unknown). The AppendOutbox fence checks it
	// against the claimed work_queue row's manifest_version so a result produced
	// under a stale manifest can't commit (inert when 0 — see the query).
	ManifestVersion int64
}

func (s *Store) AppendOutbox(ctx context.Context, a OutboxAppend) error {
	params := dbq.AppendOutboxParams{
		WorkID:             a.WorkID,
		ResourceID:         a.ResourceID,
		TaskType:           dbq.TaskType(a.TaskType),
		Succeeded:          a.Succeeded,
		ObservedGeneration: a.ObservedGeneration,
		AdvanceSyncedGen:   a.AdvanceSyncedGen,
		HealthOk:           a.HealthOK,
		Conditions:         a.Conditions,
		Failed:             a.Failed,
		Terminal:           a.Terminal,
		Payload:            a.Payload,
		ClaimEpoch:         a.ClaimEpoch,
		ShardID:            a.ShardID,
		ManifestVersion:    a.ManifestVersion,
	}
	if a.OpID != nil {
		params.OpID = toUUID(*a.OpID)
	}
	if a.ErrorMessage != "" {
		em := a.ErrorMessage
		params.ErrorMessage = &em
	}
	if a.FinalizerName != "" {
		fn := a.FinalizerName
		params.FinalizerName = &fn
	}
	return s.queries().AppendOutbox(ctx, params)
}

// (Worker/drainer latency wakes are fired in the DB via notify_gated() from
// schedule_eligible / AppendOutbox — globally rate-limited and lock-free in
// the common case — not from Go. A Go-side bare pg_notify from many backends
// serialized them all on the async-notification queue's AccessExclusiveLock
// at 1M scale, so it was removed.)

// DrainOutboxBatch invokes the plpgsql drainer for a single tick. The
// inline cascade_ready_change trigger handles downstream scheduling
// when synced_gen flips.
func (s *Store) DrainOutboxBatch(ctx context.Context, limit int, shards []int16) (int, error) {
	lo, hi := shardBounds(shards)
	n, err := s.queries().DrainOutboxBatch(ctx, dbq.DrainOutboxBatchParams{
		Column1: int32(limit),
		Column2: lo,
		Column3: hi,
	})
	return int(n), err
}

// DrainRollupRechecks re-pends, at a FRESH snapshot, the straddled rollup roots
// the cascade trigger queued in rollup_recheck — closing the rollup straddle
// race without waiting for the reaper. Called by the drainer each tick for its
// shard band. Returns how many queued roots it processed (0 on the common
// no-straddle path, where the queue is empty).
func (s *Store) DrainRollupRechecks(ctx context.Context, limit int, shards []int16) (int, error) {
	lo, hi := shardBounds(shards)
	n, err := s.queries().DrainRollupRechecks(ctx, dbq.DrainRollupRechecksParams{
		Column1: int32(limit),
		Column2: lo,
		Column3: hi,
	})
	return int(n), err
}

// ArmScheduleRecheck queues composed children into schedule_recheck so the
// drainer schedules them (deferred, paced with drains) instead of a single
// deadlock-prone post-commit ScheduleEligible. MUST run inside the compose tx
// (via the caller's WithTx store) so the arm commits atomically with the
// children — the schedule wake then can never be lost.
func (s *Store) ArmScheduleRecheck(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.db.Exec(ctx, dbq.ArmScheduleRecheckSQL, ids)
	return err
}

// DrainScheduleRechecks schedules a bounded band-scoped batch of the children the
// compose armed in schedule_recheck (fires work_ready for the runnable ones,
// drops the batch — dependents ride the cascade). Called by the drainer each tick
// for its shard band; returns how many rows it processed (0 when the queue is
// empty, the steady state).
func (s *Store) DrainScheduleRechecks(ctx context.Context, limit int, shards []int16) (int, error) {
	lo, hi := shardBounds(shards)
	n, err := s.queries().DrainScheduleRechecks(ctx, dbq.DrainScheduleRechecksParams{
		Column1: int32(limit),
		Column2: lo,
		Column3: hi,
	})
	return int(n), err
}
