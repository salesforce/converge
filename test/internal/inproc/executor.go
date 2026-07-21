// Package inproc holds the TEST-ONLY in-process reaction executor. Production runs exactly
// one model.StageDispatcher — the broker's remote fanout, which ships every reaction to
// a dumb worker over Connect. The integration suite exercises the SAME
// runtime.Dispatcher/Loop reconcile engine WITHOUT a broker or Connect: it injects this
// Executor (via Dispatcher.SetDispatcher) to run the registered providers directly in the
// test process.
//
// There is ONE dispatch path — not a dual stack. This Executor's DispatchStage is
// byte-for-byte the broker fanout's: it maps a reaction's (Trigger, Emits) to a proto
// Stage, encodes the model.ReactionRequest into a proto StageTask via internal/wire,
// and decodes the proto StageComplete back into a model.Outcome — the EXACT wire codec
// the broker uses. The only difference from the broker is the last hop: instead of
// shipping the StageTask over Connect and awaiting a worker, it runs the task in-process
// via converge.ExecuteTask (the SDK's own decode → Provider.Work → encode, the same code a
// shipped worker's WorkStream runner executes). So a provider's Work is reached through the
// SAME proto decode whether the bytes arrived over Connect (prod) or from this in-process
// encode (test) — there is no second Go dispatch path, no hand-written model⇄converge
// type bridge, and the two type worlds meet ONLY at the two existing, tested codecs
// (internal/wire on the core side, the SDK's convert on the worker side).
//
// It lives under test/internal so it is unreachable from production and from the public
// SDK.
package inproc

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/internal/wire"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// Attribution batcher tuning — the in-process twin of the broker fanout's
// (internal/broker/fanout.go): coalesce per-task "running on <worker>" stamps into
// ONE batched work_queue UPDATE per tick, so attribution never adds a per-task write
// to the hot dispatch path (a per-task synchronous UPDATE contends with
// DrainOutboxBatch/AppendOutbox on the same work_queue partitions — the LockManager
// convoy that halves 1M throughput). Small + frequent so the label appears promptly,
// deduped by work id so a re-dispatch collapses to one row. Best-effort throughout.
const (
	attrFlushEvery = 100 * time.Millisecond
	attrFlushMax   = 500
	attrChanBuf    = 2048
)

// attrEvent is one attribution stamp: work_id (on shard) is running on workerID.
// Deduped by id in the batcher's pending map.
type attrEvent struct {
	id     uuid.UUID
	shard  int16
	broker string
	worker string
}

// Executor runs reactions in-process against a set of converge.Providers — the test
// harness's model.StageDispatcher. Providers are indexed by (kind, kindVersion); a
// claimed task routes to the matching provider, exactly as the broker routes to a worker
// advertising that pair.
type Executor struct {
	providers map[model.KindVersion]converge.Provider
	// store + attribID stamp work_queue.worker_id ("running on <worker>") when set — the
	// in-process twin of the broker fanout's attribution. In this model one process is
	// BOTH broker and worker, so the executing worker's id is the pod's own attribID.
	// nil store → attribution off (a plain provider-only Executor).
	store    *store.Store
	attribID string
	// attrCh carries per-task attribution events to the coalescing batcher goroutine
	// (attributionBatcher), started by NewWithAttribution. Buffered + non-blocking send:
	// a full channel drops the event (the UI keeps the broker id — attribution is
	// best-effort, never worth blocking or slowing dispatch). nil → attribution off.
	// A reference type, so an Executor value copy (it is passed by value into
	// SetDispatcher) still shares the one channel + its single batcher.
	attrCh chan attrEvent
}

var _ model.StageDispatcher = Executor{}

// New builds an Executor from the providers the test registered, indexed by their
// (kind, kindVersion). Attribution is off; use NewWithAttribution to stamp worker_id.
func New(providers []converge.Provider) Executor {
	return newExecutor(providers, nil, "")
}

// NewWithAttribution is New plus attribution: at dispatch it enqueues a "running on
// <worker>" stamp onto a coalescing batcher (mirroring the broker fanout, which
// attributes off a channel when it hands a task to a worker) so the API work-block
// surfaces the executing worker in the in-process path too — even while a long/hung
// stage is still running — WITHOUT a per-task work_queue UPDATE on the hot path. The
// batcher goroutine runs until ctx is cancelled (the worker's lifecycle ctx), then
// flushes once and exits.
func NewWithAttribution(ctx context.Context, providers []converge.Provider, pool *pgxpool.Pool, attribID string) Executor {
	e := newExecutor(providers, store.New(pool), attribID)
	if e.store != nil && e.attribID != "" {
		e.attrCh = make(chan attrEvent, attrChanBuf)
		go e.attributionBatcher(ctx)
	}
	return e
}

func newExecutor(providers []converge.Provider, st *store.Store, attribID string) Executor {
	idx := make(map[model.KindVersion]converge.Provider, len(providers))
	for _, p := range providers {
		kv := p.Kind()
		idx[model.KindVersion{Kind: model.Kind(kv.Kind), Version: kv.Version}] = p
	}
	return Executor{providers: idx, store: st, attribID: attribID}
}

// attributionBatcher coalesces per-task attribution events off attrCh into ONE batched
// WorkQueueMarkWorkerBatch per tick (or when attrFlushMax is buffered), deduped by work
// id so a re-dispatch emits one row. It runs until ctx is cancelled, then flushes
// whatever is buffered once and returns. Best-effort: a DB error is ignored (attribution
// is display-only; the lease/fence use broker_id). This is the same discipline as the
// broker fanout's runAttributionBatcher, so the in-process path never adds a per-task
// work_queue write to the hot dispatch path.
func (e Executor) attributionBatcher(ctx context.Context) {
	t := time.NewTicker(attrFlushEvery)
	defer t.Stop()
	pending := make(map[uuid.UUID]attrEvent, attrFlushMax)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		ids := make([]uuid.UUID, 0, len(pending))
		workers := make([]string, 0, len(pending))
		shards := make([]int16, 0, len(pending))
		broker := ""
		for _, a := range pending {
			ids = append(ids, a.id)
			workers = append(workers, a.worker)
			shards = append(shards, a.shard)
			broker = a.broker // all events on one pod share the pod's broker id
		}
		clear(pending)
		wctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = e.store.WorkQueueMarkWorkerBatch(wctx, broker, ids, workers, shards)
		cancel()
	}
	for {
		select {
		case <-ctx.Done():
			flush() // best-effort final flush of whatever's buffered
			return
		case a := <-e.attrCh:
			pending[a.id] = a
			if len(pending) >= attrFlushMax {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

// stampAttribution enqueues a "running on <worker>" stamp for task's lease
// (work_queue.worker_id → attribID), keyed by the lease the dispatcher put on ctx. The
// send is NON-BLOCKING onto the coalescing batcher's channel: a full channel drops the
// event (attribution is best-effort + display-only — it never gates anything; the
// lease/fence use broker_id). No-op when attribution is off. This keeps the per-task
// work_queue UPDATE OFF the hot dispatch path — the batcher does one coalesced UPDATE
// per tick instead.
func (e Executor) stampAttribution(ctx context.Context) {
	if e.attrCh == nil {
		return
	}
	lease, ok := runtime.LeaseFromContext(ctx)
	if !ok || lease.WorkID == uuid.Nil {
		return
	}
	select {
	case e.attrCh <- attrEvent{id: lease.WorkID, shard: lease.ShardID, broker: lease.BrokerID, worker: e.attribID}:
	default: // batcher busy / channel full — drop (best-effort, UI keeps the broker id)
	}
}

// DispatchStage maps the reaction's (Trigger, Emits) to a proto Stage, encodes the request
// into a StageTask (wire), runs it in-process (converge.ExecuteTask), and decodes the
// StageComplete back — mirroring the broker fanout so in-process tests and the Connect path
// can never diverge on the wire shape or Outcome threading.
func (e Executor) DispatchStage(ctx context.Context, kind model.Kind, kindVersion int, rx model.ReactionDecl, req model.ReactionRequest) (model.Outcome, int, error) {
	p, ok := e.providers[model.KindVersion{Kind: kind, Version: kindVersion}]
	if !ok {
		return model.Outcome{}, 0, model.Terminal(fmt.Errorf("inproc: no provider for %s/v%d", kind, kindVersion))
	}
	// Attribute the task to this in-process worker (work_queue.worker_id) at DISPATCH —
	// before the stage runs, since the stage may block (a long/hung handler) and the
	// "running on <worker>" work-block must be visible while it's in flight. This mirrors
	// the broker fanout, which stamps when it hands the task to a worker, not on Complete.
	e.stampAttribution(ctx)
	run := func(t *workerpb.StageTask) *workerpb.StageComplete {
		return converge.ExecuteTask(ctx, []converge.Provider{p}, t)
	}

	switch {
	case rx.Emits.Has(model.OutcomeChildren):
		t := baseTask(kind, kindVersion, workerpb.Stage_STAGE_COMPOSE, rx.Name, req)
		creq, err := wire.ComposeReqToProto(model.ComposeRequest{Root: req.Resource, Observed: req.Observed, Env: req.Env})
		if err != nil {
			return model.Outcome{}, 0, model.Terminal(fmt.Errorf("encode compose request: %w", err))
		}
		t.ComposeReq = creq
		sc := run(t)
		if out, fs, err := stageErr(sc); err != nil {
			return out, fs, err
		}
		r := wire.ComposeResultFromProto(sc.GetCompose())
		return model.Outcome{Children: r.Desired, Edges: r.Edges, Configs: r.Configs, Status: r.Status, Conditions: r.Conditions}, 0, nil

	case rx.Trigger == model.TriggerChildrenSettled:
		t := baseTask(kind, kindVersion, workerpb.Stage_STAGE_ROLLUP, rx.Name, req)
		t.RollupReq = wire.RollupReqToProto(model.RollupRequest{Root: req.Resource, Descendants: req.Descendants, Status: req.Status, Env: req.Env})
		sc := run(t)
		if out, fs, err := stageErr(sc); err != nil {
			return out, fs, err
		}
		return model.Outcome{Status: wire.RawOrNil(sc.GetStatus()), Conditions: wire.ConditionsFromProto(sc.GetConditions())}, 0, nil

	case rx.Emits.Has(model.OutcomeFinalizer):
		t := baseTask(kind, kindVersion, workerpb.Stage_STAGE_DELETE, rx.Name, req)
		t.DeleteReq = wire.DeleteReqToProto(model.DeleteRequest{Resource: req.Resource, Env: req.Env})
		sc := run(t)
		if out, fs, err := stageErr(sc); err != nil {
			return out, fs, err
		}
		return model.Outcome{}, 0, nil

	case rx.Emits.Has(model.OutcomeOperationOutput):
		var op model.Operation
		if req.Operation != nil {
			op = *req.Operation
		}
		t := baseTask(kind, kindVersion, workerpb.Stage_STAGE_OPERATE, rx.Name, req)
		t.OperateReq = wire.OperateReqToProto(model.OperateRequest{Resource: req.Resource, Operation: op, Env: req.Env})
		sc := run(t)
		if out, fs, err := stageErr(sc); err != nil {
			return out, fs, err
		}
		return model.Outcome{OperationOutput: wire.RawOrNil(sc.GetOutput())}, 0, nil

	case rx.Trigger == model.TriggerReactor:
		t := baseTask(kind, kindVersion, workerpb.Stage_STAGE_REACT, rx.Name, req)
		t.ReactReq = wire.ReactReqToProto(model.ReactRequest{Resource: req.Resource, Transition: req.Transition, DedupToken: req.DedupToken, Env: req.Env})
		sc := run(t)
		if out, fs, err := stageErr(sc); err != nil {
			return out, fs, err
		}
		return model.Outcome{Status: wire.RawOrNil(sc.GetStatus()), Conditions: wire.ConditionsFromProto(sc.GetConditions())}, 0, nil

	default: // SpecChange/Resync work (status only)
		t := baseTask(kind, kindVersion, workerpb.Stage_STAGE_WORK, rx.Name, req)
		t.WorkReq = wire.WorkReqToProto(model.WorkRequest{Resource: req.Resource, Status: req.Status, Env: req.Env})
		sc := run(t)
		if out, fs, err := stageErr(sc); err != nil {
			return out, fs, err
		}
		return model.Outcome{Status: wire.RawOrNil(sc.GetStatus()), Conditions: wire.ConditionsFromProto(sc.GetConditions())}, 0, nil
	}
}

// baseTask builds the StageTask skeleton for a stage — the in-process twin of the broker's
// baseTask (minus the lease token, which only the broker's fence needs; ExecuteTask ignores
// it). The kindVersion is stamped explicitly (>= 1) for the (kind, kindVersion) the task
// was claimed for.
func baseTask(kind model.Kind, kindVersion int, stage workerpb.Stage, reaction string, req model.ReactionRequest) *workerpb.StageTask {
	var pc, pb []byte
	if req.Env != nil {
		pc = req.Env.ProviderConfig
		pb = req.Env.ProviderBundle
	}
	resID := req.Resource.ID
	return &workerpb.StageTask{
		Stage:          stage,
		Kind:           string(kind),
		KindVersion:    int32(kindVersion),
		Reaction:       reaction,
		ResourceId:     resID[:],
		Generation:     req.Resource.Generation,
		ProviderConfig: pc,
		ProviderBundle: pb,
	}
}

// stageErr turns a failed StageComplete into the (Outcome, failedStage, error) the engine
// expects, preserving terminal-ness so the dispatcher records the same terminal/transient
// failure the broker path would. A successful completion returns (_, _, nil) so the caller
// proceeds to decode the result.
func stageErr(sc *workerpb.StageComplete) (model.Outcome, int, error) {
	if msg := sc.GetErrorMessage(); msg != "" {
		err := fmt.Errorf("%s", msg)
		if sc.GetTerminal() {
			err = model.Terminal(err)
		}
		return model.Outcome{}, int(sc.GetFailedStage()), err
	}
	return model.Outcome{}, 0, nil
}
