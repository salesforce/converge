package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/meshpb/meshpbconnect"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
)

// echoWork is the WORKER-side provider (converge.Provider) the remote worker runs: its
// Work returns a fixed status + Ready condition so the test can assert the worker's
// result flowed back through the lease bridge to the parked executor. onConfig, when
// set, forwards the primed default providerconfig so a test can observe the
// prime-on-subscribe push. It serves the single (noop, v1) pair.
type echoWork struct {
	status   json.RawMessage
	onConfig func(converge.ProviderConfig)
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = echoWork{}

// Kind is the single (noop, v1) pair this provider serves.
func (echoWork) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind("noop"), Version: 1}
}

// OnConfig forwards the primed default providerconfig to the test's observer (if any).
func (w echoWork) OnConfig(cfg converge.ProviderConfig) {
	if w.onConfig != nil {
		w.onConfig(cfg)
	}
}

// Ready is always true: this test provider has no downstream to dial.
func (echoWork) Ready() bool { return true }

func (w echoWork) Work(_ context.Context, _ converge.ReactionRequest) (converge.Outcome, error) {
	return converge.Outcome{
		Status:     w.status,
		Conditions: []converge.Condition{{Type: "Ready", Status: converge.ConditionTrue, Reason: "OK"}},
	}, nil
}

// TestFanoutLeaseBridge proves the core broker mechanism WITHOUT a database:
// the fanout executor parks a task (as the dispatcher's reconcile pipeline
// would), a real remote worker connected over the Connect handler pulls it, runs
// its stage, and Completes; the executor then returns that worker's status +
// conditions to the (would-be) pipeline. This is the broker↔worker round-trip
// the whole tier rests on.
func TestFanoutLeaseBridge(t *testing.T) {
	wantStatus := json.RawMessage(`{"done":true}`)

	// Broker-side fanout + Connect handlers, mounted on an httptest server. The
	// connectHandler serves both broker services; mount both like Server.Handler does.
	f := newFanoutExecutor()
	h := &connectHandler{fanout: f}
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(h))
	mux.Handle(meshpbconnect.NewMeshServiceHandler(h))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	// A real remote worker that runs the "noop" kind: its single specChange→status
	// reaction "work" is the leaf worker the runner maps a STAGE_WORK task to.
	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = converge.RunWorker(ctx, client, []converge.Provider{echoWork{status: wantStatus}}, converge.RunOptions{MaxInflight: 2})
	}()

	// Drive the executor as the parked reaction engine would: DispatchStage ships
	// the kind's specChange→status "work" reaction to the connected worker and
	// blocks until it Completes the lease. The worker's Outcome (status +
	// conditions) flows back through the lease bridge into the returned Outcome.
	resID := uuid.New()
	type result struct {
		out model.Outcome
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		out, _, err := f.DispatchStage(ctx, model.Kind("noop"), 1,
			model.ReactionDecl{
				Name:    "work",
				Trigger: model.TriggerSpecChange,
				Emits:   model.OutcomeMask{model.OutcomeStatus},
			},
			model.ReactionRequest{
				Resource: model.Resource{ID: resID, Kind: "noop", Spec: json.RawMessage(`{}`), Generation: 1},
				Env:      &model.Env{},
			})
		resultCh <- result{out: out, err: err}
	}()

	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("reaction returned error: %v", res.err)
		}
		if string(res.out.Status) != string(wantStatus) {
			t.Fatalf("status = %s, want %s", res.out.Status, wantStatus)
		}
		if len(res.out.Conditions) != 1 || res.out.Conditions[0].Type != "Ready" {
			t.Fatalf("conditions = %+v, want one Ready", res.out.Conditions)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor did not resolve the lease within 5s")
	}
}

// TestFanoutAbandonWakesParkedStage is the regression guard for the headline G1
// leak: a no-deadline (no ctx timeout) task parks DispatchStage on the lease's
// result channel; when the worker stream drops, abandon() must wake it with a
// TRANSIENT error so the slot/goroutine free, instead of leaking until shutdown.
// We drive the fanout directly: park a reaction, drain the buffered task off the
// kind queue (simulating a worker that PULLED it then died), then abandon its
// token as the WorkStream teardown does. DispatchStage must return promptly with a
// non-terminal error.
func TestFanoutAbandonWakesParkedStage(t *testing.T) {
	f := newFanoutExecutor()
	ctx := context.Background() // NO deadline — the leak only existed on this path

	resID := uuid.New()
	type result struct {
		out model.Outcome
		err error
	}
	resultCh := make(chan result, 1)
	go func() {
		out, _, err := f.DispatchStage(ctx, model.Kind("noop"), 1,
			model.ReactionDecl{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
			model.ReactionRequest{
				Resource: model.Resource{ID: resID, Kind: "noop", Spec: json.RawMessage(`{}`), Generation: 1},
				Env:      &model.Env{},
			})
		resultCh <- result{out: out, err: err}
	}()

	// Pull the parked task off its kind buffer (a worker would have done this) and
	// capture its token, then abandon it as the WorkStream teardown does on a drop.
	q := f.queueFor(model.KindVersion{Kind: "noop", Version: 1})
	var token string
	select {
	case ps := <-q:
		token = ps.task.GetLeaseToken()
	case <-time.After(5 * time.Second):
		t.Fatal("task was never parked on the kind buffer")
	}
	f.abandon([]string{token})

	select {
	case res := <-resultCh:
		if res.err == nil {
			t.Fatal("abandoned stage returned nil error; want a transient failure")
		}
		if model.IsTerminal(res.err) {
			t.Fatalf("abandoned stage error is TERMINAL (%v); want transient so the task re-dispatches", res.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DispatchStage did not wake after abandon — the parked-stage leak is back")
	}

	// The token must be dropped from inflight so a late worker Complete is a no-op.
	f.mu.Lock()
	_, stillInflight := f.inflight[token]
	f.mu.Unlock()
	if stillInflight {
		t.Fatal("token still inflight after abandon+resolve; a late Complete could double-deliver")
	}
}

// staticConfigReader is a fixed ConfigReader: each kind maps to a config doc and a
// bundle, served to a subscribing worker via WorkStream's prime-on-subscribe. It
// ignores the kindVersion (the tests exercise v1 only), so any (kind, *) resolves to the
// kind's fixed doc/bundle.
type staticConfigReader struct {
	docs    map[model.Kind][]byte
	bundles map[model.Kind][]byte
}

func (s staticConfigReader) Config(k model.Kind, _ int) json.RawMessage { return s.docs[k] }
func (s staticConfigReader) Bundle(k model.Kind, _ int) []byte          { return s.bundles[k] }

// TestWorkStreamPrimesConfigOnSubscribe is the regression guard for the terminal
// "no bundle yet" bug: the broker must push each subscribed kind's CURRENT default
// config + bundle down the stream IMMEDIATELY on Subscribe, so a worker that connects
// AFTER a default was applied has it before its first task — not only after a future
// broadcast or its 5-min refresh. We stand up a broker whose ConfigReader already holds
// a config+bundle for "noop", connect a real worker that records what it's pushed, and
// assert both arrive with no broadcast and no task in play.
func TestWorkStreamPrimesConfigOnSubscribe(t *testing.T) {
	wantDoc := json.RawMessage(`{"tuned":true}`)
	wantBundle := []byte("BUNDLE-PRIMED")
	f := newFanoutExecutor()
	cfgs := staticConfigReader{
		docs:    map[model.Kind][]byte{"noop": wantDoc},
		bundles: map[model.Kind][]byte{"noop": wantBundle},
	}
	h := &connectHandler{fanout: f, configs: cfgs}
	mux := http.NewServeMux()
	mux.Handle(workerpbconnect.NewWorkerServiceHandler(h))
	mux.Handle(meshpbconnect.NewMeshServiceHandler(h))
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	type primed struct {
		spec json.RawMessage
		data []byte
	}
	got := make(chan primed, 1)
	// A providerconfig arrives WHOLE — spec + data in one push — through the
	// provider's OnConfig, which the SDK fires with the primed default on subscribe.
	provider := echoWork{
		status: json.RawMessage(`{}`),
		onConfig: func(cfg converge.ProviderConfig) {
			select {
			case got <- primed{spec: cfg.Spec, data: cfg.Data}:
			default:
			}
		},
	}
	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = converge.RunWorker(ctx, client, []converge.Provider{provider}, converge.RunOptions{MaxInflight: 1})
	}()

	// The config AND bundle must arrive TOGETHER in ONE push on connect — with NO task
	// dispatched and NO broadcast fired. That is the prime-on-subscribe contract.
	select {
	case p := <-got:
		if string(p.spec) != string(wantDoc) {
			t.Fatalf("primed config = %s, want %s", p.spec, wantDoc)
		}
		if string(p.data) != string(wantBundle) {
			t.Fatalf("primed bundle = %s, want %s", p.data, wantBundle)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not prime config+bundle on subscribe within 5s (the 'no bundle yet' regression)")
	}
}

// TestBroadcastConfigAndBundleReachSubscriber proves an operator's LIVE default edit
// (BroadcastConfig / BroadcastBundle) reaches an already-connected worker over its open
// stream — the push half of live config delivery (the pull half being the periodic
// GetProviderConfig refresh). A subscriber is registered directly on the fanout; each
// broadcast must land on its push channel.
func TestBroadcastConfigAndBundleReachSubscriber(t *testing.T) {
	f := newFanoutExecutor()
	sub := f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 4, peerIdentity{id: "w1"})
	defer f.removeSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, sub)

	// A providerconfig broadcasts WHOLE — spec + data in one ProviderConfigUpdate.
	f.broadcastProviderConfig("noop", 1, []byte(`{"v":2}`), []byte("B2"))

	select {
	case msg := <-sub.ch:
		c := msg.GetConfig()
		if c == nil {
			t.Fatalf("broadcast did not carry a ProviderConfigUpdate: %v", msg)
		}
		if string(c.GetConfig()) != `{"v":2}` {
			t.Fatalf("broadcast config = %s, want {\"v\":2}", c.GetConfig())
		}
		if string(c.GetBundle()) != "B2" {
			t.Fatalf("broadcast bundle = %s, want B2", c.GetBundle())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast did not reach the subscriber's push channel")
	}
}

// TestBroadcastPushDropIncrementsCounter proves a config/bundle push DROPPED because a
// worker's push channel is full is counted by ConfigPushDropped — the observable signal
// for the otherwise-silent drop (backstopped by the 5-min refresh + reconnect re-pull).
// We saturate the sub channel's buffer, then broadcast once more and assert the counter.
func TestBroadcastPushDropIncrementsCounter(t *testing.T) {
	f := newFanoutExecutor()
	sub := f.addSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, 1, peerIdentity{id: "w1"})
	defer f.removeSubscriber([]model.KindVersion{{Kind: "noop", Version: 1}}, sub)

	// Fill the buffered push channel (cap 64) so the next push has nowhere to go.
	for len(sub.ch) < cap(sub.ch) {
		f.broadcastProviderConfig("noop", 1, []byte(`{"fill":1}`), nil)
	}
	before := f.metrics.configPushDropped.Load()
	f.broadcastProviderConfig("noop", 1, []byte(`{"overflow":1}`), nil) // must drop
	if got := f.metrics.configPushDropped.Load(); got <= before {
		t.Fatalf("configPushDropped = %d, want > %d (a full-buffer drop must be counted)", got, before)
	}
}
