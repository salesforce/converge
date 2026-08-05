package test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/db"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/host"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/test/internal/demoruntime"
	"github.com/salesforce/converge/test/internal/inproc"
	"github.com/salesforce/converge/test/internal/noop"
)

// migrateForSeed runs the schema migrations so kind_config exists BEFORE a test
// seeds it. Mirrors cmd/converge, where the pool's migrations run before the
// kind manifests are applied.
func migrateForSeed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	sqlDB, err := sql.Open("pgx", pool.Config().ConnString())
	require.NoError(t, err)
	defer sqlDB.Close()
	require.NoError(t, db.MigrateAll(ctx, sqlDB))
}

// testEngine bundles the control sweeper bundle (engine.Engine) PLUS a test-only
// in-process worker that executes the provider reactions, with the registry + pool
// the test drives directly. Production splits these (control pods + broker tier +
// dumb workers); the test harness fuses them into one in-process unit via
// startInProcWorker so the tests can drive the reconcile pipeline without standing
// up a broker per test. Tests that specifically exercise the broker tier use
// broker.Server + converge.RunWorker (see broker_e2e_test.go /
// broker_classicbom_test.go).
type testEngine struct {
	Engine   *engine.Engine
	worker   *inProcWorker // nil for a control-only test engine
	Registry *tReg
	Pool     *pgxpool.Pool

	// reactor runs the lifecycle-reactor spine over an in-process executor when a
	// test wires engineOpts.Reactors (mirrors the broker running it over the Connect
	// fanout in prod). nil when no reactors are configured.
	reactor       *runtime.ReactorDispatcher
	reactorCancel context.CancelFunc
	reactorDone   chan struct{}
}

// CreateRoot / Reconcile are test-convenience wrappers over the store's
// resource commands (internal/store/commands.go). They live on the test
// harness because store is the real mutation gateway (the HTTP API uses it
// via the ResourceCommands port); a convenience facade belongs in tests,
// not on a production type.
//
// CreateRoot wraps the (kind,name)-keyed ApplySpec upsert and discards the
// created flag — tests that just need a root id.
//
// It stamps the root's name into the spec as a top-level "name" field.
// resource.Name lives in resource_meta, off the reconcile hot tuples, so the
// test providers read their bookkeeping name from the spec via nameFromSpec —
// this is where that name gets into the spec for roots. Best-effort: only when
// the spec is a JSON object and doesn't already set "name".
func (te *testEngine) CreateRoot(ctx context.Context, kind model.Kind, name string, spec json.RawMessage, labels map[string]string) (uuid.UUID, error) {
	spec = injectSpecName(spec, name)
	res, err := store.New(te.Pool).ApplySpec(ctx, kind, name, spec, labels)
	return res.ID, err
}

// injectSpecName sets a top-level "name" on a JSON-object spec if absent,
// so test providers can recover the resource's logical name from the
// spec (resource.Name lives in resource_meta, off the reconcile hot path).
func injectSpecName(spec json.RawMessage, name string) json.RawMessage {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(spec, &m); err != nil || m == nil {
		return spec // not a JSON object — leave as-is
	}
	if _, ok := m["name"]; ok {
		return spec
	}
	nb, _ := json.Marshal(name)
	m["name"] = nb
	out, err := json.Marshal(m)
	if err != nil {
		return spec
	}
	return out
}

// PatchSpecByID patches an EXISTING resource's spec by id and re-pends
// it — an id-keyed update the production store does not expose (it applies
// by (kind,name) via ApplySpec). Tests that already hold a resource's id
// (e.g. a composed child resolved from GetChildrenByOwner) mutate its spec
// directly through this test-only plumbing.
//
// Bumps generation EXPLICITLY (gated on IS DISTINCT FROM) — there is no
// bump_generation trigger; every spec-writing UPDATE owns its own bump.
// schedule_eligible then re-pends.
func (te *testEngine) PatchSpecByID(ctx context.Context, id uuid.UUID, spec json.RawMessage) error {
	if _, err := te.Pool.Exec(ctx,
		`UPDATE resources SET spec = $2, generation = generation + 1, updated_at = now()
		 WHERE id = $1 AND spec IS DISTINCT FROM $2::jsonb`, id, spec); err != nil {
		return err
	}
	_, err := dbq.New(te.Pool).ScheduleEligible(ctx, []uuid.UUID{id})
	return err
}

func (te *testEngine) Start(ctx context.Context) error {
	if err := te.Engine.Start(ctx); err != nil {
		return err
	}
	if te.worker != nil {
		return te.worker.Start(ctx)
	}
	return nil
}

func (te *testEngine) Stop(ctx context.Context) error {
	// Stop the reactor dispatcher, then the worker (drains in-flight), then the
	// control sweepers they feed.
	if te.reactorCancel != nil {
		te.reactorCancel()
		if te.reactorDone != nil {
			<-te.reactorDone
		}
	}
	if te.worker != nil {
		_ = te.worker.Stop(ctx)
	}
	return te.Engine.Stop(ctx)
}

// AddKindLive forwards a late-registered kind to the test worker (mirrors the
// engine's ProviderRetrier sink), so tests that register kinds after Start keep
// dispatching them.
func (te *testEngine) AddKindLive(m model.KindManifest) {
	if te.worker != nil {
		te.worker.AddKindLive(m)
	}
}

// Reconcile re-pends the resource so its provider re-runs.
func (te *testEngine) Reconcile(ctx context.Context, id uuid.UUID) error {
	return store.New(te.Pool).Reconcile(ctx, id)
}

// inProcWorker is a TEST-ONLY in-process dispatcher: it wraps a
// runtime.Dispatcher (the same primitive a broker wraps) with a Start/Stop
// lifecycle, so integration tests can drive the reconcile pipeline in-process
// without standing up a broker + remote worker. It is NOT a production
// path — production has no native-SQL worker; the broker tier + dumb
// workers are the only execution surface (see internal/broker). Tests that
// specifically exercise the broker tier use broker.Server + converge.RunWorker
// directly (see broker_e2e_test.go).
type inProcWorker struct {
	disp     *runtime.Dispatcher
	manifest *runtime.KindManifestCache
	cancel   context.CancelFunc
	done     chan struct{}
}

// newInProcWorker BUILDS (does not start) an in-process worker for the given
// kinds (every kind in reg if kinds is empty): it wires the registry's providers into an
// inproc.Executor, a KindManifestCache (the registry's manifests must already be seeded into
// the DB — callers do that via reg.seed), enables the reaction engine, and registers each
// kind's task-type pairs from its model. Test-only; production uses the broker tier +
// dumb workers. Call Start to run it.
//
// attribute turns on the "running on <worker>" work_queue.worker_id stamp (the in-process
// twin of the broker fanout's attribution). It is display-only and coalesced by a batcher,
// but it is still ~one work_queue write per task, so it is ON only for the small functional
// tests that assert the work-block (startEngineWithRegistry) and OFF for the scale/throughput
// path (startInProcWorker), whose baseline never stamped worker_id in-process.
func newInProcWorker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reg *tReg, kinds []model.Kind, maxParallel int, shards *runtime.ShardSet, attribute bool) *inProcWorker {
	t.Helper()
	d := runtime.New(pool)
	// In-process execution (no broker/Connect). When attribute is set, attribute work to this
	// pod's identity so the API work-block surfaces "running on <worker>" in-process too (one
	// process is both broker and worker here), matching the broker fanout's attribution. The
	// stamp is coalesced by a batcher goroutine bound to ctx (the worker's lifecycle), so it
	// never adds a per-task work_queue UPDATE to the hot dispatch path. When unset, the
	// executor runs providers only (no worker_id write) — the scale/throughput default.
	if attribute {
		d.SetDispatcher(inproc.NewWithAttribution(ctx, reg.providerList(), pool, d.BrokerID))
	} else {
		d.SetDispatcher(inproc.New(reg.providerList()))
	}
	if shards != nil {
		d.Shards = shards
	}
	if maxParallel <= 0 {
		maxParallel = 100
	}
	d.SetMaxParallel(maxParallel)
	if len(kinds) == 0 {
		kinds = reg.kinds()
	}
	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	d.SetManifests(mc)
	// Register a claim pair per (kind, kindVersion) the registry seeded — a kind published
	// at two kind versions (vpc/v1 + vpc/v2) contributes a pair for EACH, so the dispatcher
	// claims both. Filtered to the requested kind set when one was given.
	want := map[model.Kind]bool{}
	for _, k := range kinds {
		want[k] = true
	}
	for km, m := range reg.manifests {
		if !want[km.Kind] {
			continue
		}
		for _, tt := range manifestTaskTypes(m) {
			d.AddPair(km.Kind, km.Version, tt)
		}
	}
	return &inProcWorker{disp: d, manifest: mc}
}

// startInProcWorker seeds the registry's manifests, builds, and runs an
// in-process worker (the build-and-start convenience the multi-pod tests use).
// Attribution is OFF here: this is the scale/throughput path, whose baseline
// never stamped work_queue.worker_id in-process (a per-task write, even coalesced,
// is not free at 1M — see newInProcWorker).
func startInProcWorker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reg *tReg, kinds []model.Kind, maxParallel int, shards *runtime.ShardSet) *inProcWorker {
	t.Helper()
	require.NoError(t, reg.seed(ctx, pool))
	w := newInProcWorker(t, ctx, pool, reg, kinds, maxParallel, shards, false)
	require.NoError(t, w.Start(ctx))
	return w
}

func (w *inProcWorker) Start(ctx context.Context) error {
	loopCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.done = make(chan struct{})
	go func() {
		defer close(w.done)
		_ = w.disp.Run(loopCtx)
	}()
	return nil
}

func (w *inProcWorker) Stop(_ context.Context) error {
	if w.cancel != nil {
		w.cancel()
	}
	if w.done != nil {
		<-w.done
	}
	if w.manifest != nil {
		w.manifest.Stop()
	}
	return nil
}

func (w *inProcWorker) AddKindLive(m model.KindManifest) {
	for _, tt := range manifestTaskTypes(m) {
		w.disp.AddPairLive(m.Kind, m.KindVersion, tt)
	}
}

// manifestTaskTypes returns the work_queue task_type slots a kind's manifest opts
// into, derived from its declared reactions (the test-harness mirror of
// broker.manifestTaskTypes).
func manifestTaskTypes(m model.KindManifest) []store.TaskType {
	var reconcile, del, operate bool
	for _, rx := range m.Reactions {
		switch rx.Trigger {
		case model.TriggerSpecChange, model.TriggerChildrenSettled, model.TriggerResync:
			reconcile = true
		case model.TriggerDeleteRequested:
			del = true
		case model.TriggerOperation:
			operate = true
		}
	}
	var out []store.TaskType
	if reconcile {
		out = append(out, store.TaskReconcile)
	}
	if del {
		out = append(out, store.TaskDelete)
	}
	if operate {
		out = append(out, store.TaskOperate)
	}
	return out
}

// tReg is the TEST-ONLY kind registry: a slice of converge.Providers a test builds, plus
// helpers to (a) seed their manifests into kind_manifest and (b) list the kinds. The
// in-process executor (inproc.New) runs the providers directly — the same proto codec +
// Provider.Work path a shipped worker uses, with no framework⇄converge bridge.
type tReg struct {
	providers []converge.Provider
	manifests map[model.KindVersion]model.KindManifest // CRDs keyed by (kind, kindVersion): fixture-loaded (real kinds) or inline (test kinds)
	inline    map[model.KindVersion]model.KindManifest // test-only CRDs supplied via AddKind (no fixture file)
}

// newTReg builds an empty test registry.
func newTReg() *tReg {
	return &tReg{
		manifests: map[model.KindVersion]model.KindManifest{},
		inline:    map[model.KindVersion]model.KindManifest{},
	}
}

// kmOf is the (kind, kindVersion) key for a runtime/manifest, normalizing an unset
// kindVersion to v1 so a single-version kind keys identically whether it sets KindVersion or
// not. A kind published at two kind versions (vpc/v1 + vpc/v2) keys to two DISTINCT
// entries, so the harness seeds/dispatches BOTH — mirroring production, where each
// CRD is applied independently rather than deduped by kind.
func kmOf(kind model.Kind, kindVersion int) model.KindVersion {
	if kindVersion <= 0 {
		kindVersion = 1
	}
	return model.KindVersion{Kind: kind, Version: kindVersion}
}

// Add registers one (or more) converge.Provider (Work + the pair its Kind names). The
// kind's MANIFEST (CRD) is not here — it is loaded from the testfixtures/*.kind.json
// fixtures at seed() time, mirroring the k8s-CRD model where the manifest is applied out
// of band from the provider's handler code.
func (r *tReg) Add(ps ...converge.Provider) { r.providers = append(r.providers, ps...) }

// AddKind registers a TEST-ONLY kind: a converge.Provider plus an INLINE manifest (CRD)
// that has no testfixtures/*.kind.json file. seed() applies the inline manifest directly.
// Use this for ad-hoc controllers a test defines (controllableComposer, flakyWorker, …);
// shipped provider kinds use Add + the fixture. The k8s model holds — the manifest is
// applied separately from the handler code; it just comes from a Go literal instead of a
// file.
func (r *tReg) AddKind(p converge.Provider, m model.KindManifest) {
	r.providers = append(r.providers, p)
	kv := p.Kind()
	r.inline[kmOf(model.Kind(kv.Kind), kv.Version)] = m
}

// providerList returns the registry's providers for the in-process executor (inproc.New).
func (r *tReg) providerList() []converge.Provider { return r.providers }

// kinds lists the kinds the registry holds.
func (r *tReg) kinds() []model.Kind {
	out := make([]model.Kind, 0, len(r.providers))
	for _, p := range r.providers {
		out = append(out, model.Kind(p.Kind().Kind))
	}
	return out
}

// seed loads the CRD manifest fixture for each kind the registry holds from
// testfixtures/kind-<kind>.json and UPSERTs it into kind_manifest (which derives
// kind_config + reactor_bindings via the DB triggers). It also caches the loaded
// manifests for manifest()/AddKindLive. Call after migrateForSeed. This is the
// test analogue of the operator/bootstrap applying CRDs: the provider runtimes
// carry only handlers; the manifest comes from the fixture.
func (r *tReg) seed(ctx context.Context, pool *pgxpool.Pool) error {
	var byKM map[model.KindVersion]model.KindManifest
	// Load the shipped fixtures lazily — only if some runtime needs one (a test
	// using only inline AddKind kinds touches no files).
	loadFixtures := func() error {
		if byKM != nil {
			return nil
		}
		// Load the core testfixtures/ PLUS every example-demo fixture dir
		// (examples/demos/*/testfixtures/), so a test can register a demo kind
		// (celbom/fakevpc/…) whose CRD now lives with its demo. Each demo ships its
		// own CRDs; a (kind, kindVersion) two independent demos both define (each is
		// its own cluster) resolves to the first dir's copy — LoadKindFixtures merges
		// across dirs first-dir-wins, so this lookup is unambiguous.
		dirs := fixtureDirs()
		all, err := host.LoadKindFixtures(dirs...)
		if err != nil {
			return fmt.Errorf("load kind fixtures from %v: %w", dirs, err)
		}
		byKM = make(map[model.KindVersion]model.KindManifest, len(all))
		for _, m := range all {
			byKM[kmOf(m.Kind, m.KindVersion)] = m
		}
		return nil
	}
	st := store.New(pool)
	// Dedup identical (kind, kindVersion) providers: a kind may appear as several providers
	// (e.g. one per kindVersion), but its CRD is one row per (kind, kindVersion). Seeding the
	// SAME (kind, kindVersion) twice is a harmless upsert; the seen set just skips the
	// repeat DB round-trip and keeps r.manifests one entry per pair.
	seeded := map[model.KindVersion]bool{}
	for _, p := range r.providers {
		kv := p.Kind()
		kind := model.Kind(kv.Kind)
		km := kmOf(kind, kv.Version)
		if seeded[km] {
			continue
		}
		m, ok := r.inline[km] // test-only CRD supplied via AddKind
		if !ok {
			if err := loadFixtures(); err != nil {
				return err
			}
			if m, ok = byKM[km]; !ok {
				return fmt.Errorf("no CRD for kind %q/v%d: neither an inline manifest (AddKind) nor a fixture testfixtures/kind-%s.json at that kindVersion", kind, km.Version, kind)
			}
		}
		if _, err := st.UpsertKindManifest(ctx, m); err != nil {
			return err
		}
		r.manifests[km] = m
		seeded[km] = true
	}
	return nil
}

// manifest returns a (kind, kindVersion)'s manifest (loaded by seed) for AddKindLive etc.
func (r *tReg) manifest(kind model.Kind, kindVersion int) (model.KindManifest, bool) {
	m, ok := r.manifests[kmOf(kind, kindVersion)]
	return m, ok
}

// manifestList returns every seeded (kind, kindVersion) manifest — one per published
// kindVersion, so a kind at v1 AND v2 yields BOTH. Order is unspecified.
func (r *tReg) manifestList() []model.KindManifest {
	out := make([]model.KindManifest, 0, len(r.manifests))
	for _, m := range r.manifests {
		out = append(out, m)
	}
	return out
}

// repoRootFromCwd walks up from the working directory to the dir holding go.mod.
// A testing.T-free sibling of mustRepoRoot, so tReg.seed (which has no *testing.T)
// can locate testfixtures/.
func repoRootFromCwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	for root := wd; ; {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			return root
		}
		parent := filepath.Dir(root)
		if parent == root {
			return wd
		}
		root = parent
	}
}

// fixtureDirs returns the CRD-fixture directories seed() loads: the test-only
// testfixtures (test/testfixtures/, e.g. the generic noop CRD the engine tests use)
// plus every example-demo fixture dir (examples/demos/*/testfixtures/), each shipping
// its own demo's CRDs. Globbed from the repo root so a test runs from any working
// directory. A missing examples/ tree simply yields just test/testfixtures/ — no error.
func fixtureDirs() []string {
	root := repoRootFromCwd()
	dirs := []string{filepath.Join(root, "test", "testfixtures")}
	demoDirs, _ := filepath.Glob(filepath.Join(root, "examples", "demos", "*", "testfixtures"))
	sort.Strings(demoDirs) // deterministic load order
	return append(dirs, demoDirs...)
}

// kindFixtureFiles returns every *.kind.json across fixtureDirs() (test-only +
// all demo dirs). The canonical way to enumerate the shipped CRDs from a test,
// after the top-level testfixtures/ dir was removed in favour of per-demo dirs.
func kindFixtureFiles() []string {
	var out []string
	for _, dir := range fixtureDirs() {
		matches, _ := filepath.Glob(filepath.Join(dir, "*.kind.json"))
		out = append(out, matches...)
	}
	sort.Strings(out)
	return out
}

// readKindFixture returns the raw bytes of <kind>.kind.json found across
// fixtureDirs(). Fails the test if no such fixture exists — the same "real
// shipped fixtures, wherever they live" resolution the rest of the suite uses.
func readKindFixture(t *testing.T, kind string) []byte {
	t.Helper()
	for _, dir := range fixtureDirs() {
		p := filepath.Join(dir, kind+".kind.json")
		if raw, err := os.ReadFile(p); err == nil {
			return raw
		}
	}
	t.Fatalf("no %s.kind.json fixture found across %v", kind, fixtureDirs())
	return nil
}

// defaultRegistry returns the standard set of providers used by integration
// tests: classicbom (composer+rollup) + account/networking/noop workers. Built from
// the pure provider constructors (delay 0), hermetic (no env, no clients).
func defaultRegistry() *tReg {
	r := newTReg()
	r.Add(demoruntime.ClassicBOM(0))
	r.Add(demoruntime.Account(0, 0, fault.Injector{}))
	r.Add(noop.New(0))
	r.Add(networking.AllRuntimes(0, fault.Injector{})...)
	return r
}

// startAllRolesEngine constructs the control sweeper bundle + an in-process
// worker with the default registry. Most integration tests use this.
func startAllRolesEngine(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *testEngine {
	t.Helper()
	return startEngineWithRegistry(t, ctx, pool, defaultRegistry())
}

// startEngineWithRegistry lets tests provide their own registry (e.g.
// fake provisioners with controllable failure behavior).
func startEngineWithRegistry(t *testing.T, ctx context.Context, pool *pgxpool.Pool, reg *tReg) *testEngine {
	t.Helper()
	return startEngineWithConfig(t, ctx, pool, reg, engineOpts{})
}

// engineOpts threads optional engine knobs (heartbeat interval, rate
// limits, etc.) without growing the helper signatures every time a
// test wants to tune one.
type engineOpts struct {
	HeartbeatEvery              time.Duration
	SweeperStaleAfter           time.Duration
	SweeperUnclaimedDeleteAfter time.Duration
	RetryAfter                  time.Duration
	WorkerMaxParallel           int

	// WithReactors, when true, starts the reactor dispatch duty over an
	// in-process executor of the registry's handlers — so a test can drive the
	// full subscribe → submit → wait → react saga (see lifecycle_reactor_test.go).
	// The reactor KINDS are ordinary registry members (Add / AddKind): their
	// manifests seed into kind_manifest like any kind, and the claim resolves each
	// reactor's reaction name from its CRD. Mirrors prod, where the broker runs
	// the same dispatcher over its Connect fanout.
	WithReactors bool
}

func startEngineWithConfig(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	reg *tReg,
	opts engineOpts,
) *testEngine {
	t.Helper()
	// Migrate, then seed the registry's MANIFESTS into kind_manifest (which derives
	// kind_config + reactor_bindings via the DB triggers) before the engine starts.
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))

	// The control sweeper bundle runs through the Engine; the provider reactions
	// run in a test-only in-process worker attached below.
	duties, err := engine.DutiesFromConfig(engine.EngineConfig{
		RunControl:                  true,
		SweeperStaleAfter:           opts.SweeperStaleAfter,
		SweeperUnclaimedDeleteAfter: opts.SweeperUnclaimedDeleteAfter,
		RetryAfter:                  opts.RetryAfter,
		HeartbeatEvery:              opts.HeartbeatEvery,
	})
	require.NoError(t, err)
	eng := engine.NewEngine(duties, engine.Deps{Pool: pool})
	// Attribution ON: the small functional tests use this path, and one asserts the
	// "running on <worker>" work-block (TestResourceWorkBlock). The stamp is coalesced;
	// at these sizes its cost is negligible.
	worker := newInProcWorker(t, ctx, pool, reg, nil, opts.WorkerMaxParallel, nil, true)

	te := &testEngine{Engine: eng, worker: worker, Registry: reg, Pool: pool}

	// Reactors run through the SAME ReactorDispatcher → StageDispatcher seam as
	// production; the only difference is the executor (in-process here, the
	// broker's Connect fanout in prod). When a test wires WithReactors, run a reactor
	// dispatcher over an in-process executor of the registry's handlers (the
	// reactor kinds are ordinary registry members, so their handlers are already
	// registered and their manifests already seeded). ReadyToDispatch is left nil
	// (in-process always has the handler; a missing one fails Terminal and re-arms).
	if opts.WithReactors {
		te.reactor = runtime.NewReactorDispatcher(store.New(pool), runtime.NewPgxListener(pool), inproc.New(reg.providerList()), "")
		reactCtx, reactCancel := context.WithCancel(ctx)
		te.reactorCancel = reactCancel
		te.reactorDone = make(chan struct{})
		go func() { defer close(te.reactorDone); _ = te.reactor.Run(reactCtx) }()
	}
	return te
}

// dumpResourceState prints the root + every descendant with its
// is_ready, conditions, work_queue rows, and resource_deps. Called
// when a "wait for ready" loop times out so the failure shows what
// got stuck instead of just "false".
func dumpResourceState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rootID uuid.UUID) {
	t.Helper()
	rows, err := pool.Query(ctx, `
		WITH RECURSIVE descs(id, parent_id) AS (
		    SELECT id, owner_id FROM resources WHERE id = $1
		    UNION ALL
		    SELECT r.id, r.owner_id FROM resources r JOIN descs d ON r.owner_id = d.id
		)
		SELECT r.id, r.kind, r.kind_version, m.name, r.is_ready, r.generation, r.synced_gen,
		       r.deletion_requested_at IS NOT NULL AS deleting,
		       r.failure_terminal, r.failure_gen, r.failure_attempts, r.health_ok
		FROM resources r JOIN descs d ON r.id = d.id JOIN resource_meta m ON m.id = r.id
		ORDER BY r.kind, m.name`, rootID)
	if err != nil {
		t.Logf("dump query failed: %v", err)
		return
	}
	defer rows.Close()
	t.Logf("=== resources tree for root %s ===", rootID)
	for rows.Next() {
		var id uuid.UUID
		var kind, name string
		var kindVersion int
		var isReady, deleting, failTerminal, healthOK bool
		var gen, syncedGen, failGen int64
		var failAttempts int
		if err := rows.Scan(&id, &kind, &kindVersion, &name, &isReady, &gen, &syncedGen, &deleting,
			&failTerminal, &failGen, &failAttempts, &healthOK); err != nil {
			t.Logf("scan failed: %v", err)
			continue
		}
		t.Logf("  %-10s v%d %-30s ready=%v gen=%d/ready=%d del=%v health=%v failTerm=%v failGen=%d failAtt=%d",
			kind, kindVersion, name, isReady, gen, syncedGen, deleting, healthOK, failTerminal, failGen, failAttempts)
	}
	wqRows, err := pool.Query(ctx, `
		SELECT wq.resource_id, r.kind, m.name, wq.task_type, wq.attempts, wq.created_at,
		       wq.worker_id IS NOT NULL AS claimed
		FROM work_queue wq JOIN resources r ON r.id = wq.resource_id JOIN resource_meta m ON m.id = r.id
		ORDER BY wq.created_at`)
	if err != nil {
		t.Logf("wq query failed: %v", err)
		return
	}
	defer wqRows.Close()
	t.Logf("=== work_queue ===")
	for wqRows.Next() {
		var rid uuid.UUID
		var kind, name, taskType string
		var attempts int
		var created time.Time
		var claimed bool
		if err := wqRows.Scan(&rid, &kind, &name, &taskType, &attempts, &created, &claimed); err != nil {
			t.Logf("scan failed: %v", err)
			continue
		}
		t.Logf("  %-10s %-30s task=%-8s attempts=%d claimed=%v",
			kind, name, taskType, attempts, claimed)
	}
	oboxRows, err := pool.Query(ctx, `
		SELECT wo.resource_id, wo.task_type, wo.succeeded, wo.error_message,
		       wo.observed_generation
		FROM work_outbox wo
		ORDER BY wo.resource_id LIMIT 20`)
	if err == nil {
		defer oboxRows.Close()
		t.Logf("=== work_outbox (latest 20) ===")
		for oboxRows.Next() {
			var rid uuid.UUID
			var taskType string
			var succeeded bool
			var errMsg *string
			var obsGen int64
			if err := oboxRows.Scan(&rid, &taskType, &succeeded, &errMsg, &obsGen); err != nil {
				continue
			}
			es := ""
			if errMsg != nil {
				es = *errMsg
			}
			t.Logf("  res=%s task=%s ok=%v obs=%d err=%q", rid, taskType, succeeded, obsGen, es)
		}
	}
	depRows, err := pool.Query(ctx, `
		SELECT d.dependent_id, dep.kind, dep_m.name, d.dependency_id, p.kind, p_m.name,
		       d.value_flows::text
		FROM resource_deps d
		JOIN resources dep ON dep.id = d.dependent_id
		JOIN resource_meta dep_m ON dep_m.id = d.dependent_id
		JOIN resources p ON p.id = d.dependency_id
		JOIN resource_meta p_m ON p_m.id = d.dependency_id
		WHERE dep.id = $1 OR dep.owner_id IN (
		    WITH RECURSIVE descs(id) AS (
		        SELECT $1::uuid UNION ALL
		        SELECT r.id FROM resources r JOIN descs ON r.owner_id = descs.id
		    ) SELECT id FROM descs)
		ORDER BY dep.kind, dep_m.name`, rootID)
	if err != nil {
		t.Logf("deps query failed: %v", err)
		return
	}
	defer depRows.Close()
	t.Logf("=== resource_deps ===")
	for depRows.Next() {
		var depID, parentID uuid.UUID
		var depKind, depName, parentKind, parentName, valueFlows string
		if err := depRows.Scan(&depID, &depKind, &depName, &parentID, &parentKind, &parentName, &valueFlows); err != nil {
			t.Logf("scan failed: %v", err)
			continue
		}
		t.Logf("  %s/%s depends on %s/%s flows=%s",
			depKind, depName, parentKind, parentName, valueFlows)
	}
	// resource_events: the durable per-resource event log carries reconcile
	// failure messages (the transient error that keeps a task churning), which the
	// drained work_outbox no longer holds. Dump the latest events for the subtree so
	// a stuck-child failure reason is visible.
	evRows, err := pool.Query(ctx, `
		WITH RECURSIVE descs(id) AS (
		    SELECT $1::uuid UNION ALL
		    SELECT r.id FROM resources r JOIN descs ON r.owner_id = descs.id
		)
		SELECT r.kind, m.name, e.type, COALESCE(e.message, '')
		FROM resource_events e
		JOIN resources r ON r.id = e.resource_id
		JOIN resource_meta m ON m.id = e.resource_id
		WHERE e.resource_id IN (SELECT id FROM descs)
		ORDER BY e.created_at DESC LIMIT 20`, rootID)
	if err != nil {
		t.Logf("events query failed: %v", err)
		return
	}
	defer evRows.Close()
	t.Logf("=== resource_events (latest 20) ===")
	for evRows.Next() {
		var kind, name, evType, msg string
		if err := evRows.Scan(&kind, &name, &evType, &msg); err != nil {
			continue
		}
		t.Logf("  %-10s %-30s %-20s %q", kind, name, evType, msg)
	}
}

// buildSpec constructs the JSON spec the controllableComposer reads —
// trivial helper used by the spec-driven tests.
func buildSpec(children ...controllableChildSpec) []byte {
	out, err := json.Marshal(controllableSpec{Children: children})
	if err != nil {
		panic(err)
	}
	return out
}

// buildSpecWithDeps adds dep edges between children: depsOn[childName]
// = list of (kind, name) the child depends on.
func buildSpecWithDeps(children []controllableChildSpec, depsOn map[string][]controllableChildSpec) []byte {
	out, err := json.Marshal(controllableSpec{Children: children, DepsOn: depsOn})
	if err != nil {
		panic(err)
	}
	return out
}

// fenceClaimRoot makes a root's reconcile work_queue row owned by `worker` at
// `gen`, so a test driving ApplyComposeResult directly satisfies its commit
// fence (StampComposedGen requires the caller to still own the claim at the
// composed generation — see db/queries/compose.sql). Mirrors what the
// dispatcher's claim + a generation bump would establish. Returns the row's id
// (= work_id) to pass into ApplyComposeResult. The reconcile row was enqueued
// by CreateRoot/ApplySpec; we just stamp ownership + the target generation onto
// it.
func fenceClaimRoot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rootID uuid.UUID, broker string, gen int64) uuid.UUID {
	t.Helper()
	var workID uuid.UUID
	// Stamp the LEASE identity (broker_id) — the column the compose fence
	// (StampComposedGen) and the reaper key on. worker_id is display-only attribution.
	err := pool.QueryRow(ctx, `
		UPDATE work_queue SET broker_id = $2, generation = $3, heartbeat_at = now()
		WHERE resource_id = $1 AND task_type = 'reconcile'
		RETURNING id`, rootID, broker, gen).Scan(&workID)
	require.NoError(t, err, "root must have a reconcile work_queue row to claim (enqueued at CreateRoot)")
	return workID
}

// waitSyncedStatus waits until the resource's synced_gen catches its generation
// and returns its status JSON (or fails the test on timeout).
func waitSyncedStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rootID uuid.UUID) json.RawMessage {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		var gen, syncedGen int64
		var status json.RawMessage
		err := pool.QueryRow(ctx,
			`SELECT generation, synced_gen, COALESCE(status, '{}'::jsonb) FROM resources WHERE id = $1`, rootID,
		).Scan(&gen, &syncedGen, &status)
		require.NoError(t, err)
		if gen > 0 && syncedGen >= gen {
			return status
		}
		time.Sleep(300 * time.Millisecond)
	}
	dumpResourceState(t, ctx, pool, rootID)
	t.Fatal("resource never synced within 60s")
	return nil
}
