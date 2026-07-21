package converge

// worker_test.go — DETERMINISTIC tests for the CLIENT SDK, in-package so they can drive
// the unexported worker (newWorker/register/run + the withTransport test seam). A worker
// registers a provider, serves a streamed task and returns its result, fires OnConfig on a
// broker config push, and — the readiness path — emits an Interest RS- up the WorkStream
// when a provider's Ready flips false (and RS+ when it recovers). The broker side is a
// real in-process Connect server (httptest + h2) that captures the client messages.

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

// captureBroker is an in-memory WorkerServiceHandler that records the client messages a
// worker sends UP the WorkStream (Subscribe / StageComplete / Interest) and can push a
// task or a ProviderConfigUpdate DOWN. It holds the stream open until ctx is cancelled.
type captureBroker struct {
	pushTasks  []*workerpb.StageTask
	pushConfig *workerpb.ProviderConfigUpdate // pushed once after Subscribe, if set

	mu        sync.Mutex
	subscribe *workerpb.Subscribe
	completes []*workerpb.StageComplete
	interests []*workerpb.Interest
}

func (b *captureBroker) snapshotInterests() []*workerpb.Interest {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*workerpb.Interest(nil), b.interests...)
}

func (b *captureBroker) WorkStream(ctx context.Context, stream *connect.BidiStream[workerpb.WorkStreamClientMsg, workerpb.WorkStreamServerMsg]) error {
	first, err := stream.Receive()
	if err != nil {
		return err
	}
	sub := first.GetSubscribe()
	if sub == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("first message must be Subscribe"))
	}
	b.mu.Lock()
	b.subscribe = sub
	b.mu.Unlock()

	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			msg, err := stream.Receive()
			if err != nil {
				return
			}
			switch {
			case msg.GetComplete() != nil:
				b.mu.Lock()
				b.completes = append(b.completes, msg.GetComplete())
				b.mu.Unlock()
			case msg.GetInterest() != nil:
				b.mu.Lock()
				b.interests = append(b.interests, msg.GetInterest())
				b.mu.Unlock()
			}
		}
	}()

	for _, t := range b.pushTasks {
		if err := stream.Send(&workerpb.WorkStreamServerMsg{Body: &workerpb.WorkStreamServerMsg_Task{Task: t}}); err != nil {
			return err
		}
	}
	if b.pushConfig != nil {
		if err := stream.Send(&workerpb.WorkStreamServerMsg{Body: &workerpb.WorkStreamServerMsg_Config{Config: b.pushConfig}}); err != nil {
			return err
		}
	}
	// Hold the stream open until the worker half-closes (recvDone) or ctx is cancelled.
	// The real broker drives liveness off the worker's UP-going WorkHeartbeat, so there
	// is nothing to push DOWN periodically.
	select {
	case <-ctx.Done():
		return nil
	case <-recvDone:
		return nil
	}
}

func (b *captureBroker) GetProviderConfig(context.Context, *connect.Request[workerpb.GetProviderConfigRequest]) (*connect.Response[workerpb.GetProviderConfigResponse], error) {
	return connect.NewResponse(&workerpb.GetProviderConfigResponse{}), nil
}

// serve stands the captureBroker up on an in-process h2 TLS server and returns a Transport
// client for it. captureBroker implements only the WorkerServiceHandler — the mesh
// (Route/RelayComplete) is a separate service a worker never dials.
func serve(t *testing.T, b *captureBroker) workerpbconnect.WorkerServiceClient {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(b))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
}

// newTestWorker builds a worker wired to an in-process broker + a fast health poll, with a
// fatal on the (env-config) error. The tests set no env, so newWorker never errors here.
func newTestWorker(t *testing.T, b *captureBroker) *worker {
	t.Helper()
	w, err := newWorker(withTransport(serve(t, b)))
	if err != nil {
		t.Fatalf("newWorker: %v", err)
	}
	w.cfg.healthPoll = 20 * time.Millisecond // snappy readiness edges for the RS-/RS+ test
	return w
}

// testProvider is a Provider the deterministic tests register. work + ready are closures
// so a test can observe a run and toggle readiness; onConfig forwards pushes to a channel.
type testProvider struct {
	kv       KindVersion
	work     func() Outcome
	ready    func() bool
	onConfig func(ProviderConfig)
}

func (p testProvider) Kind() KindVersion { return p.kv }
func (p testProvider) Work(context.Context, ReactionRequest) (Outcome, error) {
	if p.work != nil {
		return p.work(), nil
	}
	return Outcome{}, nil
}
func (p testProvider) OnConfig(cfg ProviderConfig) {
	if p.onConfig != nil {
		p.onConfig(cfg)
	}
}
func (p testProvider) Ready() bool { return p.ready == nil || p.ready() }

// TestRunRequiresRegistration: run refuses a worker with no registered provider (a worker
// that can execute nothing would poll forever).
func TestRunRequiresRegistration(t *testing.T) {
	w := newTestWorker(t, &captureBroker{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.run(ctx); err == nil {
		t.Fatal("run with no registrations returned nil; want an error")
	}
}

// TestServesTask: a registered provider runs a task pushed by the broker and its Outcome
// comes back as a StageComplete up the stream.
func TestServesTask(t *testing.T) {
	ran := make(chan struct{}, 1)
	resID := uuid.New()
	b := &captureBroker{pushTasks: []*workerpb.StageTask{{
		Stage: workerpb.Stage_STAGE_WORK, Kind: "noop", KindVersion: 1, Reaction: "work", LeaseToken: "lease-1",
		ResourceId: resID[:],
		WorkReq: workReqToProto(workRequest{
			Resource: Resource{ID: resID, Kind: Kind("noop"), Spec: json.RawMessage(`{}`), Generation: 1},
		}),
	}}}
	w := newTestWorker(t, b)
	w.register(testProvider{
		kv: KindVersion{Kind: "noop", Version: 1},
		work: func() Outcome {
			ran <- struct{}{}
			return Outcome{Status: json.RawMessage(`{"ok":true}`)}
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.run(ctx) }()

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not run within 5s")
	}
	deadline := time.After(5 * time.Second)
	for {
		b.mu.Lock()
		n := len(b.completes)
		b.mu.Unlock()
		if n >= 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("no StageComplete received within 5s")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestOnConfigFiresOnPush: a broker ProviderConfigUpdate push fires the provider's
// OnConfig with the whole {Spec, Data}.
func TestOnConfigFiresOnPush(t *testing.T) {
	got := make(chan ProviderConfig, 1)
	b := &captureBroker{pushConfig: &workerpb.ProviderConfigUpdate{Kind: "noop", KindVersion: 1, Config: []byte(`{"v":2}`), Bundle: []byte("B2")}}
	w := newTestWorker(t, b)
	w.register(testProvider{
		kv:       KindVersion{Kind: "noop", Version: 1},
		onConfig: func(cfg ProviderConfig) { got <- cfg },
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.run(ctx) }()

	select {
	case cfg := <-got:
		if string(cfg.Spec) != `{"v":2}` {
			t.Fatalf("OnConfig spec = %s, want {\"v\":2}", cfg.Spec)
		}
		if string(cfg.Data) != "B2" {
			t.Fatalf("OnConfig data = %s, want B2", cfg.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnConfig callback did not fire within 5s")
	}
}

// TestReadyEmitsRSMinusThenRSPlus: a provider whose Ready starts true then flips false
// emits an Interest RS- (has_worker=false) up the stream for the exact pair; on recovery
// it emits RS+ (has_worker=true). A sibling that stays ready never gets a frame.
func TestReadyEmitsRSMinusThenRSPlus(t *testing.T) {
	up := &atomicBool{v: true}
	b := &captureBroker{}
	w := newTestWorker(t, b)
	w.register(testProvider{kv: KindVersion{Kind: "kafka", Version: 1}, ready: up.get})
	w.register(testProvider{kv: KindVersion{Kind: "noop", Version: 1}}) // no probe → always ready, never a frame
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.run(ctx) }()

	// Flip kafka down → expect an RS- for (kafka,1) only.
	up.set(false)
	waitInterest(t, b, "kafka", 1, false)
	for _, in := range b.snapshotInterests() {
		if in.GetKind() == "noop" {
			t.Fatal("emitted an Interest for an always-ready kind (should never flip)")
		}
	}
	// Recover → expect an RS+ for (kafka,1).
	up.set(true)
	waitInterest(t, b, "kafka", 1, true)
}

// waitInterest blocks until an Interest for (kind,ver) with the given has_worker is
// captured, or fails after 5s.
func waitInterest(t *testing.T, b *captureBroker, kind string, ver int, wantReady bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		for _, in := range b.snapshotInterests() {
			if in.GetKind() == kind && int(in.GetKindVersion()) == ver && in.GetHasWorker() == wantReady {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("no Interest{kind=%s,ver=%d,has_worker=%v} within 5s", kind, ver, wantReady)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// atomicBool is a tiny goroutine-safe bool for the readiness probe (the poller reads it
// from its own goroutine while the test writes it).
type atomicBool struct {
	mu sync.Mutex
	v  bool
}

func (a *atomicBool) get() bool  { a.mu.Lock(); defer a.mu.Unlock(); return a.v }
func (a *atomicBool) set(v bool) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
