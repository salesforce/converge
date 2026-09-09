package converge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	"github.com/salesforce/converge/sdk-go/workerpb"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// testWork is a trivial ReactionHandler: it echoes a fixed status and a Ready
// condition, and records the ReactionRequest it ran with.
type testWork struct {
	ran    chan ReactionRequest
	status json.RawMessage
}

func (w testWork) React(_ context.Context, req ReactionRequest) (Outcome, error) {
	w.ran <- req
	return Outcome{
		Status:     w.status,
		Conditions: []Condition{{Type: "Ready", Status: ConditionTrue, Reason: "OK"}},
	}, nil
}

// noopRuntime builds the kindRuntime the worker registers: a "noop" kind whose
// single "work" reaction is the leaf handler the runner maps a STAGE_WORK task
// to. The worker holds NO manifest — it looks up the handler by (kind, reaction)
// using the reaction NAME the broker ships in StageTask.Reaction. h runs it.
func noopRuntime(kind string, h ReactionHandler) kindRuntime {
	return kindRuntime{
		Kind:        Kind(kind),
		KindVersion: 1,
		Handlers:    map[string]ReactionHandler{"work": h},
	}
}

// panicWork is a ReactionHandler that always panics — the stand-in for a buggy
// (or dependency-faulting) provider handler. Its panic must fail only its own
// task, never crash the worker.
type panicWork struct{}

func (panicWork) React(context.Context, ReactionRequest) (Outcome, error) {
	panic("boom: handler dependency exploded")
}

// fakeBroker is an in-memory WorkerServiceHandler for the bidirectional WorkStream:
// it expects a Subscribe first, streams the tasks it was seeded with DOWN, reads
// StageCompletes UP the same stream (captured for assertions), and holds the stream
// open until ctx is cancelled — like a real broker.
type fakeBroker struct {
	tasks []*workerpb.StageTask

	mu        sync.Mutex
	completed []*workerpb.StageComplete
	done      chan struct{} // closed after the first Complete
	once      sync.Once
}

// waitCompletes blocks until at least n Completes have been captured or the
// deadline passes, returning a snapshot copy of them.
func (f *fakeBroker) waitCompletes(t *testing.T, n int, d time.Duration) []*workerpb.StageComplete {
	t.Helper()
	deadline := time.NewTimer(d)
	defer deadline.Stop()
	for {
		f.mu.Lock()
		if len(f.completed) >= n {
			out := append([]*workerpb.StageComplete(nil), f.completed...)
			f.mu.Unlock()
			return out
		}
		f.mu.Unlock()
		select {
		case <-deadline.C:
			f.mu.Lock()
			got := len(f.completed)
			f.mu.Unlock()
			t.Fatalf("got %d completes within %s, want %d", got, d, n)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// WorkStream is the bidi work channel: it expects a Subscribe first, sends the
// seeded tasks DOWN, and reads StageCompletes UP the same stream (capturing them).
// Runs the receive loop in a goroutine and holds the stream open until ctx is
// cancelled, like a real broker.
func (f *fakeBroker) WorkStream(ctx context.Context, stream *connect.BidiStream[workerpb.WorkStreamClientMsg, workerpb.WorkStreamServerMsg]) error {
	// First client message must be Subscribe.
	first, err := stream.Receive()
	if err != nil {
		return err
	}
	if first.GetSubscribe() == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("first message must be Subscribe"))
	}
	// Receive loop (worker → broker): capture StageCompletes; signal recvDone on EOF.
	// Mirrors the REAL broker (connect.go): when the worker HALF-CLOSES its send side
	// (CloseRequest during a graceful drain) the receive hits io.EOF, and the broker
	// returns — closing the response side so the worker's Receive() ends promptly. A
	// fake that ignored EOF would force the worker to wait out its full DrainGrace,
	// masking the prompt drain-exit.
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			msg, err := stream.Receive()
			if err != nil {
				return // io.EOF on the worker's CloseRequest, or a transport error
			}
			if c := msg.GetComplete(); c != nil {
				f.mu.Lock()
				f.completed = append(f.completed, c)
				f.mu.Unlock()
				f.once.Do(func() { close(f.done) })
			}
		}
	}()
	// Send the seeded tasks DOWN. connect flushes the h2 response writer on each Send,
	// so the task frames reach the worker promptly.
	for _, t := range f.tasks {
		if err := stream.Send(&workerpb.WorkStreamServerMsg{Body: &workerpb.WorkStreamServerMsg_Task{Task: t}}); err != nil {
			return err
		}
	}
	// Hold the stream open until the worker half-closes its send side (recvDone, a
	// graceful drain) or ctx is cancelled — like a real broker, which drives liveness
	// off the worker's UP-going WorkHeartbeat rather than a DOWN-going keepalive.
	select {
	case <-ctx.Done():
		return nil
	case <-recvDone:
		return nil
	}
}

// workTask builds a WORK StageTask carrying the TYPED work request (Resource +
// Status), matching what the broker fanout sends.
func workTask(kind string, resID uuid.UUID, gen int64, spec string, lease string) *workerpb.StageTask {
	return &workerpb.StageTask{
		Stage: workerpb.Stage_STAGE_WORK,
		Kind:  kind,
		// KindVersion is stamped explicitly (>= 1) — the worker looks the handler up by
		// (kind, kind_version, reaction) with NO implicit v1 default, so an unstamped
		// (0) task would be a hard "no handler" miss.
		KindVersion: 1,
		Reaction:    "work", // the broker ships the reaction NAME; the worker looks up (kind, kind_version, reaction)
		ResourceId:  resID[:],
		Generation:  gen,
		WorkReq: workReqToProto(workRequest{
			Resource: Resource{ID: resID, Kind: Kind(kind), Spec: json.RawMessage(spec), Generation: gen},
		}),
		LeaseToken: lease,
	}
}

func (f *fakeBroker) GetProviderConfig(context.Context, *connect.Request[workerpb.GetProviderConfigRequest]) (*connect.Response[workerpb.GetProviderConfigResponse], error) {
	return connect.NewResponse(&workerpb.GetProviderConfigResponse{}), nil
}

// TestRunnerExecutesAndCompletes proves the dumb worker pulls a streamed task,
// runs its registered Worker stage with the right WorkRequest, and reports the
// status + folded-back conditions to the broker via Complete.
func TestRunnerExecutesAndCompletes(t *testing.T) {
	ran := make(chan ReactionRequest, 1)
	wantStatus := json.RawMessage(`{"ok":true}`)

	kinds := []kindRuntime{noopRuntime("noop", testWork{ran: ran, status: wantStatus})}

	resID := uuid.New()
	fake := &fakeBroker{
		done:  make(chan struct{}),
		tasks: []*workerpb.StageTask{workTask("noop", resID, 7, `{"x":1}`, "lease-1")},
	}

	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(fake))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	r, err := newRunner(runnerConfig{Client: client, Kinds: kinds, MaxInflight: 4})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// The stage must run with the streamed spec/generation.
	select {
	case req := <-ran:
		if string(req.Resource.Spec) != `{"x":1}` {
			t.Fatalf("spec = %s, want {\"x\":1}", req.Resource.Spec)
		}
		if req.Resource.Generation != 7 {
			t.Fatalf("generation = %d, want 7", req.Resource.Generation)
		}
		if req.Resource.ID != resID {
			t.Fatalf("resource id = %s, want %s", req.Resource.ID, resID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker stage did not run within 5s")
	}

	// The Complete must carry the status + the raw Ready condition.
	select {
	case <-fake.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Complete not received within 5s")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.completed) != 1 {
		t.Fatalf("got %d completes, want 1", len(fake.completed))
	}
	c := fake.completed[0]
	if c.GetLeaseToken() != "lease-1" {
		t.Fatalf("lease token = %q, want lease-1", c.GetLeaseToken())
	}
	if string(c.GetStatus()) != string(wantStatus) {
		t.Fatalf("status = %s, want %s", c.GetStatus(), wantStatus)
	}
	if c.GetErrorMessage() != "" {
		t.Fatalf("unexpected error: %q", c.GetErrorMessage())
	}
	if len(c.GetConditions()) != 1 || c.GetConditions()[0].GetType() != "Ready" {
		t.Fatalf("conditions = %+v, want one Ready", c.GetConditions())
	}
}

// TestRunnerEchoesClaimEpoch proves the worker copies the task's claim_epoch onto its
// StageComplete. The epoch is the broker's fence: the fenced AppendOutbox lands the
// write ONLY if the row still bears this exact epoch, so a result from a reaped +
// re-issued lease (a stale epoch) no-ops instead of double-applying. If the echo were
// dropped the fence would reject EVERY live result.
func TestRunnerEchoesClaimEpoch(t *testing.T) {
	const wantEpoch = int64(42)
	resID := uuid.New()
	task := workTask("noop", resID, 1, `{}`, "lease-epoch")
	task.ClaimEpoch = wantEpoch

	fake := &fakeBroker{done: make(chan struct{}), tasks: []*workerpb.StageTask{task}}
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(fake))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	r, err := newRunner(runnerConfig{
		Client: client,
		Kinds:  []kindRuntime{noopRuntime("noop", testWork{ran: make(chan ReactionRequest, 1), status: json.RawMessage(`{}`)})},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	select {
	case <-fake.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Complete not received within 5s")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.completed[0].GetClaimEpoch(); got != wantEpoch {
		t.Fatalf("Complete claim_epoch = %d, want %d (echo dropped → broker fence would reject the write)", got, wantEpoch)
	}
}

// TestRunnerOnConnectFiresAfterSubscribe proves the reconnect-resilience hook: Run
// invokes Config.OnConnect once per (re)connect, right after the Subscribe. Production
// wires it to ConfigCache.Load so a reconnecting worker RE-PULLS the current default
// config/bundle instead of trusting a stale boot snapshot (the broker only pushes future
// changes). Without it a config that changed during a disconnect would strand until the
// periodic refresh — long enough for a bootstrapped provider to terminal-fail.
func TestRunnerOnConnectFiresAfterSubscribe(t *testing.T) {
	fake := &fakeBroker{done: make(chan struct{})} // no tasks; we only assert the hook
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(fake))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	connected := make(chan struct{}, 1)
	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	r, err := newRunner(runnerConfig{
		Client:      client,
		Kinds:       []kindRuntime{noopRuntime("noop", testWork{ran: make(chan ReactionRequest, 1)})},
		MaxInflight: 1,
		OnConnect: func(context.Context) {
			select {
			case connected <- struct{}{}:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("OnConnect did not fire after Subscribe within 5s (reconnect config re-pull is dead)")
	}
}

// TestRunnerUnknownKindFailsTerminal proves a task for a kind the worker does
// not run is completed with a terminal error rather than silently dropped.
func TestRunnerUnknownKindFailsTerminal(t *testing.T) {
	kinds := []kindRuntime{noopRuntime("noop", testWork{ran: make(chan ReactionRequest, 1), status: nil})}

	fake := &fakeBroker{
		done:  make(chan struct{}),
		tasks: []*workerpb.StageTask{workTask("unregistered", uuid.New(), 1, `{}`, "lease-x")},
	}

	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(fake))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	r, err := newRunner(runnerConfig{Client: client, Kinds: kinds})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	select {
	case <-fake.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Complete not received within 5s")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	c := fake.completed[0]
	if c.GetErrorMessage() == "" || !c.GetTerminal() {
		t.Fatalf("want terminal error for unknown kind, got err=%q terminal=%v", c.GetErrorMessage(), c.GetTerminal())
	}
}

// TestRunnerPanicIsolatedPerKind proves the worker's fault isolation: when one
// kind's handler PANICS, that task is completed with a terminal error and the
// process survives, so a SIBLING kind's task in the SAME runner still runs and
// completes normally. A panic in one provider must never take down the other
// kinds sharing the worker binary.
func TestRunnerPanicIsolatedPerKind(t *testing.T) {
	ran := make(chan ReactionRequest, 1)
	wantStatus := json.RawMessage(`{"ok":true}`)

	// Two kinds in ONE runner: "boom" panics, "noop" is healthy.
	kinds := []kindRuntime{
		noopRuntime("boom", panicWork{}),
		noopRuntime("noop", testWork{ran: ran, status: wantStatus}),
	}

	panicRes, healthyRes := uuid.New(), uuid.New()
	fake := &fakeBroker{
		done: make(chan struct{}),
		tasks: []*workerpb.StageTask{
			workTask("boom", panicRes, 1, `{}`, "lease-panic"),
			workTask("noop", healthyRes, 2, `{"x":1}`, "lease-ok"),
		},
	}

	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(fake))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	// MaxInflight 1 forces the two tasks through the SAME goroutine slot in turn,
	// so the panic's slot must be released for the healthy task to run at all —
	// proving the recover doesn't leak the semaphore either.
	r, err := newRunner(runnerConfig{Client: client, Kinds: kinds, MaxInflight: 1})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// The healthy sibling must have run despite the panic on the other kind.
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("healthy sibling kind did not run — a panic in another kind took the worker down")
	}

	completes := fake.waitCompletes(t, 2, 5*time.Second)
	byLease := map[string]*workerpb.StageComplete{}
	for _, c := range completes {
		byLease[c.GetLeaseToken()] = c
	}

	pc := byLease["lease-panic"]
	if pc == nil {
		t.Fatal("panicking task never reported a Complete (lease would be stranded)")
	}
	if !pc.GetTerminal() || pc.GetErrorMessage() == "" {
		t.Fatalf("panic task: want terminal error, got terminal=%v err=%q", pc.GetTerminal(), pc.GetErrorMessage())
	}

	hc := byLease["lease-ok"]
	if hc == nil {
		t.Fatal("healthy task never completed")
	}
	if hc.GetErrorMessage() != "" {
		t.Fatalf("healthy task: unexpected error %q", hc.GetErrorMessage())
	}
	if string(hc.GetStatus()) != string(wantStatus) {
		t.Fatalf("healthy task status = %s, want %s", hc.GetStatus(), wantStatus)
	}
}

// blockingWork starts (signals `started`), then blocks until `release` is closed
// — a stand-in for an in-flight handler we can hold across a shutdown. It does NOT
// abort on ctx cancel: a handler keeps computing through a SIGTERM (the drain lets
// in-flight work finish and land its result rather than be abandoned), so the test
// controls exactly when it finishes via `release`.
type blockingWork struct {
	started chan struct{}
	release chan struct{}
	status  json.RawMessage
}

func (w blockingWork) React(_ context.Context, _ ReactionRequest) (Outcome, error) {
	close(w.started)
	<-w.release
	return Outcome{Status: w.status}, nil
}

// TestRunnerDrainsInFlightOnShutdown proves the graceful drain: a handler that is
// in-flight when Run's ctx is cancelled (SIGTERM) keeps running and lands its
// result, instead of being abandoned — and Run only returns after it drains.
func TestRunnerDrainsInFlightOnShutdown(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	wantStatus := json.RawMessage(`{"drained":true}`)
	kinds := []kindRuntime{noopRuntime("noop", blockingWork{started: started, release: release, status: wantStatus})}

	fake := &fakeBroker{
		done:  make(chan struct{}),
		tasks: []*workerpb.StageTask{workTask("noop", uuid.New(), 1, `{}`, "lease-drain")},
	}
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(fake))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	// Generous DrainGrace so the in-flight handler finishes within it.
	r, err := newRunner(runnerConfig{Client: client, Kinds: kinds, MaxInflight: 1, DrainGrace: 5 * time.Second})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = r.Run(ctx); close(runDone) }()

	// Wait until the handler is genuinely in-flight, THEN cancel (SIGTERM).
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()

	// Run must NOT have returned yet — it's draining the in-flight handler.
	select {
	case <-runDone:
		t.Fatal("Run returned while a handler was still in-flight; drain abandoned it")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the handler: it finishes, its Complete lands, and Run returns.
	close(release)
	select {
	case <-fake.done:
	case <-time.After(5 * time.Second):
		t.Fatal("drained handler's Complete never arrived")
	}
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the in-flight handler drained")
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.completed) != 1 || string(fake.completed[0].GetStatus()) != string(wantStatus) {
		t.Fatalf("drained result not reported: %+v", fake.completed)
	}
}

// TestRunnerIdleDrainExitsPromptly is the regression guard for the SIGTERM idle-drain
// hang: with NO in-flight work, a worker must exit AT ONCE on SIGTERM — not idle out
// the full DrainGrace. On a rolling deploy the broker stays up and keeps sending
// keepalive frames; a receive loop that merely looped on them (never noticing ctx) would
// sit the entire grace before exiting, needlessly slowing every pod rollout. Run half-
// closes its send side once idle+cancelled, the broker ends the stream, and Run returns.
// We give a GENEROUS DrainGrace and assert Run returns in a small fraction of it.
func TestRunnerIdleDrainExitsPromptly(t *testing.T) {
	fake := &fakeBroker{done: make(chan struct{})} // no tasks — the worker is idle
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(fake))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	r, err := newRunner(runnerConfig{
		Client:      client,
		Kinds:       []kindRuntime{noopRuntime("noop", testWork{ran: make(chan ReactionRequest, 1)})},
		MaxInflight: 1,
		DrainGrace:  30 * time.Second, // generous: a prompt exit must be FAR under this
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { _ = r.Run(ctx); close(runDone) }()

	time.Sleep(300 * time.Millisecond) // let it connect + subscribe (be genuinely idle)
	start := time.Now()
	cancel() // SIGTERM

	select {
	case <-runDone:
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Fatalf("idle worker took %v to exit on SIGTERM — it idled out the DrainGrace instead of exiting promptly", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("idle worker did not exit within 10s of SIGTERM (drain-exit hang regressed)")
	}
}

// TestRunnerBuffersResultWhenStreamDown proves send() BUFFERS a computed result when
// there is no active stream (broker gone / mid-reconnect) instead of discarding it: the
// result lands in resultBuf and the discarded counter stays 0 (the compute is preserved
// for redelivery, not thrown away). Only a full buffer discards.
func TestRunnerBuffersResultWhenStreamDown(t *testing.T) {
	r, err := newRunner(runnerConfig{
		Client: nilTransport{}, // never dialed — we drive send() directly
		Kinds:  []kindRuntime{noopRuntime("noop", testWork{ran: make(chan ReactionRequest, 1)})},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	// No active stream: r.sendCh is nil, so send() must buffer.
	task := workTask("noop", uuid.New(), 1, `{}`, "lease-buf")
	sc := &workerpb.StageComplete{LeaseToken: "lease-buf", ClaimEpoch: 7}
	r.send(context.Background(), task, sc)

	r.mu.Lock()
	bufLen := len(r.resultBuf)
	buffered := len(r.resultBuf) == 1 && r.resultBuf[0] == sc
	r.mu.Unlock()
	if !buffered {
		t.Fatalf("result not buffered: resultBuf len=%d", bufLen)
	}
	if got := r.discarded.Load(); got != 0 {
		t.Fatalf("discarded = %d, want 0 (a buffered result is not discarded)", got)
	}
}

// TestRunnerRedeliversBufferedResult proves a result computed while the stream was down
// (buffered in resultBuf) is REDELIVERED on the next Run — to whatever broker the worker
// reconnects to — before any new work is pulled. This is what lets a worker that
// reconnects (to this or a different broker) land its already-computed result instead of
// re-executing it across a rollout.
func TestRunnerRedeliversBufferedResult(t *testing.T) {
	fake := &fakeBroker{done: make(chan struct{})} // no tasks — only the redelivered result
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(fake))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	r, err := newRunner(runnerConfig{
		Client: client,
		Kinds:  []kindRuntime{noopRuntime("noop", testWork{ran: make(chan ReactionRequest, 1)})},
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	// Seed a buffered result from a prior (dropped) session.
	buffered := &workerpb.StageComplete{LeaseToken: "lease-redeliver", ClaimEpoch: 9, Status: json.RawMessage(`{"prior":true}`)}
	r.mu.Lock()
	r.resultBuf = []*workerpb.StageComplete{buffered}
	r.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	// The next connect redelivers the buffered Complete before pulling new work.
	select {
	case <-fake.done:
	case <-time.After(5 * time.Second):
		t.Fatal("buffered result was not redelivered within 5s")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.completed) != 1 || fake.completed[0].GetLeaseToken() != "lease-redeliver" {
		t.Fatalf("redelivered completes = %+v, want the one buffered lease-redeliver", fake.completed)
	}
	if fake.completed[0].GetClaimEpoch() != 9 {
		t.Fatalf("redelivered claim_epoch = %d, want 9 (the epoch the broker fences on)", fake.completed[0].GetClaimEpoch())
	}
}

// nilTransport is a transport whose methods are never called — for a runner driven at
// the method level (send/buffer) rather than over a real stream.
type nilTransport struct{}

func (nilTransport) WorkStream(context.Context) *connect.BidiStreamForClient[workerpb.WorkStreamClientMsg, workerpb.WorkStreamServerMsg] {
	return nil
}
func (nilTransport) GetProviderConfig(context.Context, *connect.Request[workerpb.GetProviderConfigRequest]) (*connect.Response[workerpb.GetProviderConfigResponse], error) {
	return nil, nil
}
