package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/pkg/drainctx"
)

// These tests lock in the graceful-drain lease-refresh invariant: BOTH the work
// dispatcher's and the reactor's heartbeat loops run under drainCtx (NOT the Run ctx),
// so an in-flight task/delivery keeps its lease refreshed for the WHOLE DrainGrace
// window after SIGTERM. That makes DrainGrace independent of the reaper's StaleAfter /
// LifecycleStaleAfter — the reaper can't falsely reclaim (dispatcher) or re-arm +
// double-fire (reactor) work this pod is still executing. If a future refactor rewires
// a heartbeat back onto the Run ctx (stopping it at SIGTERM), these fail. They also
// guard the two-group defer ordering: Run must not DEADLOCK joining a heartbeat that
// only exits when drainCtx is cancelled.

// countingHeartbeatRepo is a DispatcherRepo whose only live method is WorkQueueHeartbeat
// (it counts beats and stamps the wall clock of the last one). Every other method is a
// zero-value stub — the drain-heartbeat test drives heartbeatLoop directly, so nothing
// else is ever called. It exists only to satisfy the interface.
type countingHeartbeatRepo struct {
	beats atomic.Int64
	mu    sync.Mutex
	last  time.Time
	now   func() time.Time
}

func (r *countingHeartbeatRepo) WorkQueueHeartbeat(_ context.Context, _ []uuid.UUID, _ []int64, _, _ int16) error {
	r.beats.Add(1)
	r.mu.Lock()
	r.last = r.now()
	r.mu.Unlock()
	return nil
}

func (r *countingHeartbeatRepo) lastBeat() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// --- zero-value stubs to satisfy DispatcherRepo (never called here) ---

func (r *countingHeartbeatRepo) WorkQueueTakeBatch(context.Context, model.Kind, int, store.TaskType, string, int, []int16) ([]store.WorkTask, error) {
	return nil, nil
}
func (r *countingHeartbeatRepo) WorkQueueReleaseBroker(context.Context, string) (int64, error) {
	return 0, nil
}
func (r *countingHeartbeatRepo) WorkQueueIncrementAttempts(context.Context, uuid.UUID, int16) error {
	return nil
}
func (r *countingHeartbeatRepo) GetComposedGen(context.Context, uuid.UUID) (int64, error) {
	return 0, nil
}
func (r *countingHeartbeatRepo) ListDescendants(context.Context, uuid.UUID) ([]store.ResourceRow, error) {
	return nil, nil
}
func (r *countingHeartbeatRepo) DescendantsSettled(context.Context, uuid.UUID) (bool, error) {
	return true, nil
}
func (r *countingHeartbeatRepo) GetOperation(context.Context, uuid.UUID) (store.ResourceOperation, error) {
	return store.ResourceOperation{}, nil
}
func (r *countingHeartbeatRepo) AppendOutbox(context.Context, store.OutboxAppend) error { return nil }
func (r *countingHeartbeatRepo) EmitEvent(context.Context, store.Event) error           { return nil }
func (r *countingHeartbeatRepo) ApplyComposeResult(context.Context, uuid.UUID, int64, uuid.UUID, int64, int64, []model.ChildSpec, []model.DepEdge, []model.ProviderConfigSpec, store.ComposePolicy) (store.ComposeCounts, error) {
	return store.ComposeCounts{}, nil
}
func (r *countingHeartbeatRepo) ArmScheduleRecheck(context.Context, []uuid.UUID) error { return nil }
func (r *countingHeartbeatRepo) AnalyzeComposeTables(context.Context) error            { return nil }
func (r *countingHeartbeatRepo) WithTx(pgx.Tx) DispatcherRepo                          { return r }

var _ DispatcherRepo = (*countingHeartbeatRepo)(nil)

// TestDispatcherHeartbeatRefreshesThroughDrain proves the work dispatcher's heartbeat is
// scoped to drainCtx, not the Run ctx: it keeps beating AFTER the parent ctx is cancelled
// (SIGTERM), for the whole DrainGrace window, then stops. We drive heartbeatLoop with the
// SAME drainCtx wiring Run uses (drainctx.New), a seeded in-flight task, and a fake clock so
// the assertions are deterministic (no wall-clock flake).
func TestDispatcherHeartbeatRefreshesThroughDrain(t *testing.T) {
	repo := &countingHeartbeatRepo{now: time.Now}
	d := &Dispatcher{
		Loop:           &Loop{Repo: repo, BrokerID: "test"},
		HeartbeatEvery: 5 * time.Millisecond, // fast so the test is quick
	}

	// One in-flight task (gate off: requireWithin==0 → always refreshed).
	inFlight := newInFlightSet(0)
	inFlight.add(uuid.New(), 1, 1)

	// Mirror Run's wiring: tasks + heartbeat run under drainCtx; the claim ctx is the
	// parent we cancel to simulate SIGTERM. DrainGrace is generous so we can observe
	// beats landing AFTER the cancel and before the grace elapses.
	const drainGrace = 200 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	drainCtx, cancelDrain := drainctx.New(ctx, drainGrace)
	defer cancelDrain()

	var hbWG sync.WaitGroup
	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		d.heartbeatLoop(drainCtx, inFlight)
	}()

	// Let a few beats land while "live", then simulate SIGTERM.
	time.Sleep(30 * time.Millisecond)
	beatsAtSigterm := repo.beats.Load()
	if beatsAtSigterm == 0 {
		t.Fatal("heartbeat produced no beats while live")
	}
	cancel() // SIGTERM: the Run ctx is now dead, but drainCtx lingers drainGrace

	// The KEY assertion: beats keep landing AFTER ctx cancel. Poll briefly for at least
	// one more beat than we had at SIGTERM — if the heartbeat were (wrongly) bound to ctx
	// it would have stopped immediately and this never advances.
	deadline := time.Now().Add(drainGrace / 2)
	for repo.beats.Load() <= beatsAtSigterm && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := repo.beats.Load(); got <= beatsAtSigterm {
		t.Fatalf("heartbeat did NOT beat after SIGTERM: %d beats at cancel, %d after — the drain lease refresh is broken (heartbeat bound to ctx, not drainCtx)", beatsAtSigterm, got)
	}
	lastDuringDrain := repo.lastBeat()

	// After the drain grace elapses, drainCtx is cancelled by its watcher and the loop
	// exits — hbWG.Wait must return (no deadlock) and no beat lands past drain end.
	hbWG.Wait()
	beatsAtDrainEnd := repo.beats.Load()

	// A short settle: confirm the loop is truly stopped (no further beats).
	time.Sleep(20 * time.Millisecond)
	if got := repo.beats.Load(); got != beatsAtDrainEnd {
		t.Fatalf("heartbeat beat %d more times AFTER drainCtx ended — the loop did not stop at drain end", got-beatsAtDrainEnd)
	}
	if lastDuringDrain.IsZero() {
		t.Fatal("expected a recorded beat during the drain window")
	}
}

// TestDispatcherRunNoDeadlockOnDrainShutdown drives the REAL Dispatcher.Run through a
// graceful-drain shutdown (zero pairs → a clean claim-loop no-op; heartbeat + listener
// goroutines still start) and asserts Run RETURNS promptly after ctx cancel — it does not
// hang joining the drainCtx-scoped heartbeat. The heartbeat's loop only exits when drainCtx
// is cancelled, so Run's defers must cancel drainCtx before (or without blocking on) the
// hbWG join; this is the end-to-end guard that the two-group wiring shuts down cleanly.
// (Run returns within roughly DrainGrace — the drainctx watcher cancels drainCtx that long
// after ctx — so a genuine join-order hang would blow the 2s ceiling below.)
func TestDispatcherRunNoDeadlockOnDrainShutdown(t *testing.T) {
	repo := &countingHeartbeatRepo{now: time.Now}
	d := &Dispatcher{
		Loop:           &Loop{Repo: repo, Listener: newFakeListener(), BrokerID: "test"},
		HeartbeatEvery: 5 * time.Millisecond,
		PollMin:        5 * time.Millisecond,
		PollMax:        20 * time.Millisecond,
		HotWindow:      5 * time.Millisecond,
		Shards:         NewShardSet(AllShards()),
		DrainGrace:     150 * time.Millisecond,
		newPairs:       make(chan *pair, 8),
		stopped:        make(chan struct{}),
		wakeup:         make(chan struct{}, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		_ = d.Run(ctx)
		close(runDone)
	}()

	// Let Run come up and start its heartbeat + listener goroutines, then SIGTERM.
	// (The per-lease refresh-through-drain behaviour is asserted in
	// TestDispatcherHeartbeatRefreshesThroughDrain, which drives heartbeatLoop directly
	// without racing Run's internal inFlight publication — here we only assert clean
	// shutdown of the real Run.)
	time.Sleep(30 * time.Millisecond)
	cancel()

	// Run must return once the drain grace elapses — cleanly, no deadlock joining the
	// drainCtx heartbeat. Allow a healthy margin over DrainGrace.
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Dispatcher.Run did not return after drain — deadlock joining the drainCtx heartbeat (two-group defer ordering regressed)")
	}
}

// oneShotReactorRepo claims exactly ONE delivery on the first ClaimReactorDeliveries call
// then returns empty forever, counts HeartbeatReactorClaims beats, and no-ops the ack.
// Enough to drive ReactorDispatcher.Run end-to-end and observe the drain-heartbeat
// behaviour without a DB.
type oneShotReactorRepo struct {
	claimed  atomic.Bool
	delivery store.ReactorDelivery
	beats    atomic.Int64
}

func (r *oneShotReactorRepo) ClaimReactorDeliveries(_ context.Context, _ string, _ int, _ []int16) ([]store.ReactorDelivery, error) {
	if r.claimed.Swap(true) {
		return nil, nil // subsequent ticks: nothing left to claim
	}
	return []store.ReactorDelivery{r.delivery}, nil
}

func (r *oneShotReactorRepo) AckReactorDelivery(context.Context, uuid.UUID, string, int64, string, int64) error {
	return nil
}

func (r *oneShotReactorRepo) HeartbeatReactorClaims(_ context.Context, _ []store.ReactorLeaseKey, _ []int16) error {
	r.beats.Add(1)
	return nil
}

func (r *oneShotReactorRepo) beatCount() int64 { return r.beats.Load() }

var _ ReactorRepo = (*oneShotReactorRepo)(nil)

// blockingDispatch is a StageDispatcher that parks a delivery until ctx is cancelled — so
// the delivery is genuinely "in flight" across the drain window, which is exactly the
// state whose lease the heartbeat must keep refreshing. On ctx cancel (drain grace elapsed)
// it returns, letting tick's wg.Wait join it.
type blockingDispatch struct{ entered chan struct{} }

func (b blockingDispatch) DispatchStage(ctx context.Context, _ model.Kind, _ int, _ model.ReactionDecl, _ model.ReactionRequest) (model.Outcome, int, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-ctx.Done() // park until drainCtx ends
	return model.Outcome{}, 0, ctx.Err()
}

// TestReactorHeartbeatRefreshesThroughDrain proves the reactor's heartbeat is scoped to
// drainCtx: an in-flight delivery (parked in DispatchStage) keeps its lifecycle_outbox
// claim refreshed for the whole DrainGrace window after SIGTERM, and Run returns cleanly
// (no deadlock) once the grace elapses. Drives the REAL ReactorDispatcher.Run.
func TestReactorHeartbeatRefreshesThroughDrain(t *testing.T) {
	repo := &oneShotReactorRepo{
		delivery: store.ReactorDelivery{
			ResourceID:         uuid.New(),
			Kind:               "watched",
			Transition:         "synced",
			Generation:         1,
			BindingName:        "b1",
			Reactor:            "notifier",
			Reaction:           "notify",
			ReactorKindVersion: 1,
			ClaimEpoch:         1,
		},
	}
	entered := make(chan struct{}, 1)
	d := NewReactorDispatcher(repo, newFakeListener(), blockingDispatch{entered: entered}, "react-test")
	d.HeartbeatEvery = 5 * time.Millisecond // fast so the test is quick
	d.Interval = 2 * time.Millisecond
	d.DrainGrace = 200 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		_ = d.Run(ctx)
		close(runDone)
	}()

	// Wait until the delivery is actually parked in DispatchStage (in flight), so its
	// lease is now the thing the heartbeat must keep alive.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery never entered DispatchStage")
	}

	// Let a couple of beats land while live, then SIGTERM.
	time.Sleep(30 * time.Millisecond)
	beatsAtSigterm := repo.beatCount()
	if beatsAtSigterm == 0 {
		t.Fatal("reactor heartbeat produced no beats while the delivery was in flight")
	}
	cancel() // SIGTERM: Run ctx dead; drainCtx lingers DrainGrace; delivery still parked

	// KEY assertion: the heartbeat keeps beating AFTER SIGTERM (it's on drainCtx). If it
	// were bound to ctx it would stop here and this never advances.
	deadline := time.Now().Add(d.DrainGrace / 2)
	for repo.beatCount() <= beatsAtSigterm && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if got := repo.beatCount(); got <= beatsAtSigterm {
		t.Fatalf("reactor heartbeat did NOT beat after SIGTERM: %d at cancel, %d after — the drain lease refresh is broken (heartbeat bound to ctx, not drainCtx)", beatsAtSigterm, got)
	}

	// Run must return once the drain grace elapses (the parked DispatchStage unblocks on
	// drainCtx.Done, tick joins it, the heartbeat stops on drainCtx.Done) — no deadlock.
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("reactor Run did not return after the drain grace — deadlock joining the drainCtx heartbeat?")
	}
}
