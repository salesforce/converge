package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

// TestBrokerReactorRemote proves the UNIFIED reactor path: a lifecycle reactor
// runs in a DUMB worker over the SAME broker→WorkStream→worker stream as
// every other stage (the new STAGE_REACT), not in an in-process react pod.
//
// Topology mirrors production exactly:
//   - control duty (drainer/reaper) only — no in-process execution,
//   - a broker that runs BOTH the work dispatcher AND the ReactorDispatcher,
//     the latter wired to the broker's fanout executor (Dispatcher) + the
//     HasWorkerForKind gate — exactly what claimDuty.Start does,
//   - ONE dumb worker advertising the work kinds AND the statussink reactor
//     kind, so the broker fans STAGE_REACT to it.
//
// Saga: submit a classicbom → it composes + rolls up (all remote) → crossing
// 'synced' fires the classicbom→statussink binding → the broker claims the
// lifecycle_outbox row and ships STAGE_REACT to the worker → the worker runs
// statussink's 'react' handler (uploading the rolled-up status) → on a clean
// Complete the broker acks the outbox. If the reactor ran in-process anywhere,
// the worker's object store would stay empty; it doesn't.
func TestBrokerReactorRemote(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// The statussink reactor backed by an inspectable in-memory object store, and
	// the work kinds a classicbom composes into. The reactor KindRuntime carries a
	// TriggerReactor 'react' reaction; seeding it puts statussink in kind_manifest
	// so the broker treats it as a real (subscribable, claimable-from-outbox) kind
	// and the claim can resolve its reaction name.
	const prefix = "rolled-up/"
	reactorKR, getObject := demoruntime.StatusSink(
		"mem://test-bucket",
		fmt.Sprintf(`{"endpoint":"mem://test-bucket","prefix":%q}`, prefix),
	)

	reg := newTReg()
	reg.Add(demoruntime.ClassicBOM(0))
	reg.Add(demoruntime.Account(0, 0, fault.Injector{}))
	reg.Add(networking.AllRuntimes(0, fault.Injector{})...)
	reg.Add(reactorKR) // the statussink reactor — its manifest seeds alongside the work kinds

	// Control duty only: the drainer applies the outbox → synced_gen and the
	// cascade trigger emits the 'synced' lifecycle_outbox row; nothing executes
	// provider/reactor code in-process. Seed manifests first (the broker + reactor
	// dispatcher read from kind_manifest; the subscription is applied below).
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))
	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{RunControl: true})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	// The subscription: when a classicbom crosses 'synced', run the statussink
	// reactor. The binding name is arbitrary; the dispatcher resolves the reaction
	// to run from the statussink CRD (its single `reactor` reaction).
	require.NoError(t, store.New(pool).UpsertReactorBinding(ctx, store.ReactorBinding{
		Name:       "classicbom-synced-to-statussink",
		WatchKind:  model.Kind(classicbom.Kind),
		Transition: string(model.TransitionSynced),
		Reactor:    model.Kind(statussink.Kind),
		Enabled:    true,
	}))

	// The manifest cache the broker reads pairs + policy from.
	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	// The broker: owns all shards, claims the reconcile pair for every WORK kind.
	// statussink declares only a reactor reaction, so it has NO work_queue pair —
	// it is delivered from lifecycle_outbox by the reactor dispatcher, not claimed
	// from work_queue. (AddPair for it would be a no-op; we don't add one.)
	const brokerID = "broker-reactor"
	cl := broker.New(pool, mc, brokerID, 32)
	for _, k := range reg.kinds() {
		if k == model.Kind(statussink.Kind) {
			continue
		}
		cl.Dispatcher().AddPair(k, 1, store.TaskReconcile)
	}
	mux := http.NewServeMux()
	mux.Handle(cl.Handler())
	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	claimCtx, claimCancel := context.WithCancel(ctx)
	t.Cleanup(claimCancel)
	go func() { _ = cl.Dispatcher().Run(claimCtx) }()

	// The reactor dispatcher running ON the broker, wired to the fanout executor —
	// EXACTLY as claimDuty.Start wires it in production. It drains lifecycle_outbox
	// and ships STAGE_REACT through the fanout to a connected worker; HasWorkerForKind
	// gates a kind with no worker.
	reactor := runtime.NewReactorDispatcher(store.New(pool), runtime.NewPgxListener(pool), cl.StageDispatcher(), brokerID)
	reactor.ReadyToDispatch = cl.HasWorkerForKind
	go func() { _ = reactor.Run(claimCtx) }()

	// ONE dumb worker advertising the work kinds AND statussink. The reactor's
	// handler runs HERE, over Connect — never in-process on the broker/control pod.
	workerProviders := []converge.Provider{demoruntime.ClassicBOM(0), demoruntime.Account(0, 0, fault.Injector{})}
	workerProviders = append(workerProviders, networking.AllRuntimes(0, fault.Injector{})...)
	workerProviders = append(workerProviders, reactorKR)
	client := workerpbconnect.NewWorkerServiceClient(srv.Client(), srv.URL)
	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)
	go func() {
		_ = converge.RunWorker(workerCtx, client, workerProviders, converge.RunOptions{MaxInflight: 16})
	}()

	// Submit a small classicbom: one FD with three teams → 11 children.
	bom := smallBOM("reactor-remote", map[string][]string{
		"alpha-fd": {"team-alpha", "team-beta", "team-gamma"},
	})
	specBytes, err := json.Marshal(bom)
	require.NoError(t, err)
	rootID, err := (&testEngine{Pool: pool}).CreateRoot(ctx, model.Kind(classicbom.Kind), "reactor-remote", specBytes, nil)
	require.NoError(t, err)

	q := dbq.New(pool)

	// Wait until the classicbom rolls up (synced) — all compose/work/rollup ran remotely.
	deadline := time.Now().Add(90 * time.Second)
	var rolledUp bool
	for time.Now().Before(deadline) {
		root, err := q.GetResourceInfo(ctx, rootID)
		if err == nil && root.IsReady && root.SyncedGen >= 1 {
			rolledUp = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !rolledUp {
		dumpResourceState(t, ctx, pool, rootID)
		t.Fatal("classicbom should roll up (synced) before the reactor can fire")
	}

	// The reactor delivered OVER Connect: the object lands under the expected key.
	root, err := q.GetResourceInfo(ctx, rootID)
	require.NoError(t, err)
	wantKey := fmt.Sprintf("%s%s-%d.json", prefix, "reactor-remote", root.Generation)

	delivered := false
	for time.Now().Before(deadline) {
		if _, ok := getObject(wantKey); ok {
			delivered = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !delivered {
		dumpLifecycleState(t, ctx, pool)
		t.Fatalf("statussink reactor (running in the REMOTE worker via STAGE_REACT) should have uploaded to %q", wantKey)
	}

	t.Logf("SUCCESS: classicbom synced → STAGE_REACT shipped to the dumb worker → statussink uploaded %s (unified reactor path, no in-process react)", wantKey)
}
