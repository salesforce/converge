package test

// Configurable providers for the integration tests. These replace
// the stub real providers (account/networking) when a test wants to
// inject failures, count invocations, or vary spec → children behavior
// dynamically.
//
// Each one is a kind: a model.KindManifest (the CRD — schemas + declared
// reactions + policy, core-side internal/model types) plus a
// converge.Provider implementation whose Work switches on req.Reaction. Tests add
// the provider to a tReg via reg.AddKind(controller, controller.Manifest()).
// Reaction-name convention mirrors the reference providers
// (account/networking/classicbom): composer="compose", worker="work",
// rollup="rollup", delete="teardown", operate="operate:<verb>".

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/kindschema"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/converge"
)

// ─── controllableComposer ────────────────────────────────────────────────
//
// A composer for kind=classicbom whose "compose" reaction produces children
// determined by the latest spec. Tests use it to validate spec updates →
// generation bumps → re-reconcile and orphan detection. With withRollup set
// it also declares a "rollup" reaction for composite demotion.

type controllableComposer struct {
	mu sync.Mutex
	// produce maps the root resource's name → desired children. Tests
	// flip the value before submitting an update to model "user updated
	// the spec".
	produce func(name string, spec controllableSpec) []converge.ChildSpec
	// composeCalls counts each compose invocation per (root name).
	composeCalls map[string]int
	// failNextN, when > 0, makes the compose reaction return an error and decrement.
	failNextN int
	// withRollup adds a "rollup" reaction that reports Ready=False/
	// ChildrenNotReady when any descendant is not is_ready — so a test can
	// exercise composite demotion (a root going Degraded when a child fails)
	// rather than the rollup-less composer that just stays not-ready.
	withRollup bool
}

// compile-time proof each test controller satisfies the one provider contract.
var (
	_ converge.Provider = (*controllableComposer)(nil)
	_ converge.Provider = (*flakyWorker)(nil)
	_ converge.Provider = (*deletableWorker)(nil)
	_ converge.Provider = (*statusBumpWorker)(nil)
	_ converge.Provider = (*healthProbeWorker)(nil)
	_ converge.Provider = (*slowWorker)(nil)
)

// controllableSpec is the JSON shape we use as the classicbom spec for
// these tests. Keeps the surface tiny — children + optional edges.
type controllableSpec struct {
	Children []controllableChildSpec `json:"children"`
	// DepsOn maps a child name → list of (kind, name) that child
	// depends on. Used to set up multi-level dep graphs in tests.
	DepsOn map[string][]controllableChildSpec `json:"deps_on,omitempty"`
}

type controllableChildSpec struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// nameFromSpec extracts the child's logical name from its spec's "name"
// field. The vertical split removed resource.Name from the reconcile hot
// path (it lives in resource_meta, fetched only by the operate verb), so
// test providers that key bookkeeping by name read it from the spec —
// which the controllable composer always stamps as {"name": <childName>}
// and the account/networking specs carry too. Returns "" if absent.
func nameFromSpec(spec []byte) string {
	var s struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(spec, &s)
	return s.Name
}

func newControllableComposer() *controllableComposer {
	c := &controllableComposer{composeCalls: map[string]int{}}
	c.produce = func(name string, spec controllableSpec) []converge.ChildSpec {
		out := make([]converge.ChildSpec, 0, len(spec.Children))
		for _, ch := range spec.Children {
			out = append(out, converge.ChildSpec{
				Kind:        converge.Kind(ch.Kind),
				KindVersion: 1,
				Name:        ch.Name,
				Spec:        map[string]string{"name": ch.Name},
				Labels:      map[string]string{"composed_by": "controllable"},
			})
		}
		return out
	}
	return c
}

// compose is the controllableComposer's "compose" reaction: it expands the root spec
// into children + dep edges.
func (c *controllableComposer) compose(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	// Root name isn't on the reconcile hot path (vertical split). The
	// controllable root spec carries it as {"name": <rootName>}; key
	// compose-call bookkeeping off that so tests can still assert by name.
	rootName := nameFromSpec(req.Resource.Spec)
	c.mu.Lock()
	c.composeCalls[rootName]++
	if c.failNextN > 0 {
		c.failNextN--
		c.mu.Unlock()
		return converge.Outcome{}, fmt.Errorf("controllable: simulated compose failure")
	}
	c.mu.Unlock()

	var spec controllableSpec
	if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
		return converge.Outcome{}, fmt.Errorf("decode controllable spec: %w", err)
	}
	c.mu.Lock()
	produce := c.produce
	c.mu.Unlock()
	children := produce(rootName, spec)

	// Build edges from spec.DepsOn. Each entry is "this child depends
	// on these other children", so the edge is (from=child, to=dep).
	var edges []converge.DepEdge
	for childName, deps := range spec.DepsOn {
		for _, dep := range deps {
			var childKind converge.Kind
			for _, ch := range children {
				if ch.Name == childName {
					childKind = ch.Kind
					break
				}
			}
			if childKind == "" {
				continue
			}
			edges = append(edges, converge.DepEdge{
				From: converge.ResourceRef{Kind: childKind, Name: childName},
				To:   converge.ResourceRef{Kind: converge.Kind(dep.Kind), Name: dep.Name},
			})
		}
	}
	return converge.Outcome{Children: children, Edges: edges}, nil
}

func (c *controllableComposer) Calls(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.composeCalls[name]
}

// rollup is the controllableComposer's "rollup" reaction: it reports the composite's
// aggregate health: Ready=False/ChildrenNotReady when any descendant is not is_ready,
// else Ready=True. Mirrors classicbom's rollup so tests can exercise composite demotion.
// Only reached when withRollup is set (the manifest declares no rollup reaction otherwise).
func (c *controllableComposer) rollup(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	unready := 0
	for _, d := range req.Descendants {
		if d.OwnerID == nil { // skip the root itself if present
			continue
		}
		// Ignore children leaving/set aside (deleting, or frozen = orphaned-grace
		// or quarantined) — mirrors the classicbom rollup so the test controller matches.
		if !d.IsReady && d.DeletionRequestedAt == nil && !d.IsFrozen() {
			unready++
		}
	}
	ready := converge.Condition{Type: converge.TypeReady, Status: converge.ConditionTrue, Reason: "Available"}
	if unready > 0 {
		ready = converge.Condition{
			Type:    converge.TypeReady,
			Status:  converge.ConditionFalse,
			Reason:  "ChildrenNotReady",
			Message: fmt.Sprintf("%d descendant(s) not ready", unready),
		}
	}
	return converge.Outcome{Status: req.Status, Conditions: []converge.Condition{ready}}, nil
}

// Kind is the single (kind, version) this composer serves.
func (*controllableComposer) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(classicbom.Kind), Version: 1}
}

// Work dispatches on req.Reaction: "compose" (spec change → children/edges) and, when
// withRollup is set, "rollup" (children settled → status/conditions, composite-demotion).
func (c *controllableComposer) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	switch req.Reaction {
	case "rollup":
		return c.rollup(ctx, req)
	default:
		return c.compose(ctx, req)
	}
}

// OnConfig is a no-op: this test composer reads no default providerconfig.
func (*controllableComposer) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test controller has no downstream to dial.
func (*controllableComposer) Ready() bool { return true }

// Manifest is the inline CRD a test seeds via reg.AddKind. It declares a
// "compose" reaction (spec change → children/edges); when withRollup is set it
// also declares a "rollup" reaction (children settled → status/conditions,
// composite-demotion) — kept consistent with the reactions Work dispatches.
func (c *controllableComposer) Manifest() model.KindManifest {
	reactions := []model.ReactionDecl{
		{Name: "compose", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{
			model.OutcomeChildren, model.OutcomeEdges, model.OutcomeConfigs,
			model.OutcomeStatus, model.OutcomeConditions,
		}},
	}
	if c.withRollup {
		reactions = append(reactions, model.ReactionDecl{
			Name: "rollup", Trigger: model.TriggerChildrenSettled,
			Emits: model.OutcomeMask{model.OutcomeStatus, model.OutcomeConditions},
		})
	}
	return model.KindManifest{
		Kind:        model.Kind(classicbom.Kind),
		KindVersion: 1,
		SpecSchema:  kindschema.Of[controllableSpec](),
		Reactions:   reactions,
	}
}

// ─── flakyWorker ─────────────────────────────────────────────────────────
//
// A "work" reaction that fails the first N times per resource name, then
// succeeds. Used to validate retry semantics and reaper recovery.

type flakyWorker struct {
	mu          sync.Mutex
	kind        model.Kind
	failsBefore map[string]int // resource name → remaining failures
	calls       map[string]int // resource name → invocation count
	workCalls   atomic.Int64

	// hangNextN: the next React() calls (per resource name) will block until
	// ctx cancels — simulates a stuck worker. Used by the crash-recovery test.
	hangNames map[string]bool

	// terminalNames: Work returns converge.Terminal(err) for these —
	// a non-retryable failure (the scheduler must STOP re-queuing them).
	terminalNames map[string]bool

	// taskDeadline, when > 0, is surfaced on the manifest (as TaskDeadlineSecs)
	// so the dispatcher enforces a per-task timeout for this kind. Used by the
	// deadline test together with SetHang.
	taskDeadline time.Duration
}

func newFlakyWorker(kind model.Kind) *flakyWorker {
	return &flakyWorker{
		kind:          kind,
		failsBefore:   map[string]int{},
		calls:         map[string]int{},
		hangNames:     map[string]bool{},
		terminalNames: map[string]bool{},
	}
}

// Kind is the single (kind, version) this worker serves.
func (w *flakyWorker) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(w.kind), Version: 1}
}

// OnConfig is a no-op: this test worker reads no default providerconfig.
func (*flakyWorker) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test controller has no downstream to dial.
func (*flakyWorker) Ready() bool { return true }

// Work runs the kind's one "work" reaction (spec change → status), with the test's
// per-name transient/terminal/hang knobs applied.
func (w *flakyWorker) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	name := nameFromSpec(req.Resource.Spec)
	w.mu.Lock()
	w.calls[name]++
	w.workCalls.Add(1)
	if w.terminalNames[name] {
		w.mu.Unlock()
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("flaky: simulated TERMINAL failure"))
	}
	failsLeft := w.failsBefore[name]
	if failsLeft > 0 {
		w.failsBefore[name] = failsLeft - 1
		w.mu.Unlock()
		return converge.Outcome{}, fmt.Errorf("flaky: simulated transient failure")
	}
	hang := w.hangNames[name]
	w.mu.Unlock()

	if hang {
		<-ctx.Done()
		return converge.Outcome{}, ctx.Err()
	}

	out, err := json.Marshal(map[string]string{
		"kind":      string(w.kind),
		"name":      name,
		"output_id": fmt.Sprintf("%s-%s", w.kind, name),
	})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

func (w *flakyWorker) SetFailsBefore(name string, n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failsBefore[name] = n
}

func (w *flakyWorker) SetHang(name string, hang bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.hangNames[name] = hang
}

// SetTerminal makes Work return a non-retryable converge.Terminal
// error for the named resource (the scheduler stops re-queuing it).
func (w *flakyWorker) SetTerminal(name string, terminal bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.terminalNames[name] = terminal
}

func (w *flakyWorker) Calls(name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls[name]
}

// SetTaskDeadline declares a per-task deadline on this kind's manifest
// (TaskDeadlineSecs) so the dispatcher races each task against it. Pair with
// SetHang to drive a deadline-exceeded → cancel → retry flow.
func (w *flakyWorker) SetTaskDeadline(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.taskDeadline = d
}

// Manifest is the inline CRD a test seeds via reg.AddKind: a "work" reaction
// (spec change → status). When SetTaskDeadline was called, the manifest carries
// TaskDeadlineSecs so the dispatcher enforces a per-task timeout.
func (w *flakyWorker) Manifest() model.KindManifest {
	w.mu.Lock()
	defer w.mu.Unlock()
	return model.KindManifest{
		Kind:             w.kind,
		KindVersion:      1,
		TaskDeadlineSecs: secs(w.taskDeadline),
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
}

// secs converts a deadline duration to the manifest's whole-second knob,
// rounding UP so a sub-second deadline still enforces a >=1s window (matching
// the deadline test's 1s setting). 0 stays 0 (no deadline).
func secs(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	n := int(d / time.Second)
	if d%time.Second != 0 {
		n++
	}
	return n
}

// ─── deletableWorker ─────────────────────────────────────────────────────
//
// A "work" + "teardown" kind with a FinalizerName, so a child of this kind
// owns external cleanup. Used to validate that the composer PRUNE
// soft-deletes (deletion_requested_at + finalizer + delete task → teardown
// runs) rather than hard-deleting the row out from under the deleter.

type deletableWorker struct {
	mu          sync.Mutex
	kind        model.Kind
	deleteCalls map[string]int
}

func newDeletableWorker(kind model.Kind) *deletableWorker {
	return &deletableWorker{kind: kind, deleteCalls: map[string]int{}}
}

// Kind is the single (kind, version) this worker serves.
func (w *deletableWorker) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(w.kind), Version: 1}
}

// OnConfig is a no-op: this test worker reads no default providerconfig.
func (*deletableWorker) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test controller has no downstream to dial.
func (*deletableWorker) Ready() bool { return true }

// Work dispatches on req.Reaction: "work" (spec change → status) and "teardown" (delete
// requested → count the delete + strip the finalizer via the manifest's Emits).
func (w *deletableWorker) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if req.Reaction == "teardown" {
		w.mu.Lock()
		w.deleteCalls[nameFromSpec(req.Resource.Spec)]++
		w.mu.Unlock()
		return converge.Outcome{}, nil
	}
	out, _ := json.Marshal(map[string]string{"name": nameFromSpec(req.Resource.Spec)})
	return converge.Outcome{Status: out}, nil
}

func (w *deletableWorker) DeleteCalls(name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deleteCalls[name]
}

// finalizerName is this kind's finalizer string (exposed so a test composing
// these children can assert the finalizer survives the soft-delete).
func (w *deletableWorker) finalizerName() string { return "test.io/" + string(w.kind) }

// Manifest is the inline CRD a test seeds via reg.AddKind: a "work" reaction
// (spec change → status) plus a "teardown" reaction (delete requested → strip
// the finalizer). The manifest's FinalizerName makes deletes a two-phase teardown.
func (w *deletableWorker) Manifest() model.KindManifest {
	return model.KindManifest{
		Kind:          w.kind,
		KindVersion:   1,
		FinalizerName: w.finalizerName(),
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
			{Name: "teardown", Trigger: model.TriggerDeleteRequested, Emits: model.OutcomeMask{model.OutcomeFinalizer}, Finalizer: w.finalizerName()},
		},
	}
}

// ─── statusBumpWorker ────────────────────────────────────────────────────
//
// A "work" reaction whose status output the test can change between runs.
// Models a real upstream whose value is observed and propagates to
// dependents via the value-flow cascade. Used to validate the
// status-cascade trigger.

type statusBumpWorker struct {
	mu     sync.Mutex
	kind   model.Kind
	values map[string]string // resource name → current "value" stamped into status
	calls  map[string]int
}

func newStatusBumpWorker(kind model.Kind) *statusBumpWorker {
	return &statusBumpWorker{
		kind:   kind,
		values: map[string]string{},
		calls:  map[string]int{},
	}
}

// Kind is the single (kind, version) this worker serves.
func (w *statusBumpWorker) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(w.kind), Version: 1}
}

// OnConfig is a no-op: this test worker reads no default providerconfig.
func (*statusBumpWorker) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test controller has no downstream to dial.
func (*statusBumpWorker) Ready() bool { return true }

// Work runs the kind's one "work" reaction (spec change → status), stamping the test's
// current per-name value so a dependent picks it up via the value-flow cascade.
func (w *statusBumpWorker) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	name := nameFromSpec(req.Resource.Spec)
	w.mu.Lock()
	w.calls[name]++
	val := w.values[name]
	if val == "" {
		val = "v1"
	}
	w.mu.Unlock()
	out, err := json.Marshal(map[string]string{"value": val, "name": name})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

func (w *statusBumpWorker) SetValue(name, value string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.values[name] = value
}

func (w *statusBumpWorker) Calls(name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls[name]
}

// Manifest is the inline CRD a test seeds via reg.AddKind: a "work" reaction
// (spec change → status).
func (w *statusBumpWorker) Manifest() model.KindManifest {
	return model.KindManifest{
		Kind:        w.kind,
		KindVersion: 1,
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
}

// ─── healthProbeWorker ───────────────────────────────────────────────────
//
// A "work" reaction that always succeeds (advances Synced) but reports a
// Ready condition driven by a test-settable per-resource health flag.
// Combined with a short ResyncInterval, it models the headline case:
// a resource that reconciled fine, then a later drift probe finds it
// unhealthy — flipping Ready=False (→ health_ok=false → is_ready=false)
// with NO spec/generation change.

type healthProbeWorker struct {
	mu      sync.Mutex
	kind    model.Kind
	healthy map[string]bool // resource name → currently healthy (default true)
	calls   map[string]int
	resync  time.Duration
}

func newHealthProbeWorker(kind model.Kind, resync time.Duration) *healthProbeWorker {
	return &healthProbeWorker{
		kind:    kind,
		healthy: map[string]bool{},
		calls:   map[string]int{},
		resync:  resync,
	}
}

// Kind is the single (kind, version) this worker serves.
func (w *healthProbeWorker) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(w.kind), Version: 1}
}

// OnConfig is a no-op: this test worker reads no default providerconfig.
func (*healthProbeWorker) OnConfig(converge.ProviderConfig) {}

// Ready is always true: the per-resource health this worker models is reported PER TASK
// via the Ready CONDITION in Work's Outcome (which flips health_ok), NOT via the
// provider-level readiness gate that sheds a whole (kind, version) from the broker.
func (*healthProbeWorker) Ready() bool { return true }

// Work runs the kind's one "work" reaction: it always advances Synced (writes status) but
// emits a Ready condition driven by the test-settable per-resource health flag, so a
// later resync probe can flip Ready=False with no spec/generation change.
func (w *healthProbeWorker) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	name := nameFromSpec(req.Resource.Spec)
	w.mu.Lock()
	w.calls[name]++
	healthy, seen := w.healthy[name]
	if !seen {
		healthy = true // default healthy until a test marks it down
	}
	w.mu.Unlock()

	out, err := json.Marshal(map[string]string{"name": name})
	if err != nil {
		return converge.Outcome{}, err
	}
	cond := converge.Condition{Type: converge.TypeReady, Status: converge.ConditionTrue, Reason: "Available"}
	if !healthy {
		cond = converge.Condition{
			Type:    converge.TypeReady,
			Status:  converge.ConditionFalse,
			Reason:  "ProbeFailed",
			Message: "health probe detected degradation",
		}
	}
	return converge.Outcome{Status: out, Conditions: []converge.Condition{cond}}, nil
}

func (w *healthProbeWorker) SetHealthy(name string, healthy bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.healthy[name] = healthy
}

func (w *healthProbeWorker) Calls(name string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls[name]
}

// Manifest is the inline CRD a test seeds via reg.AddKind: a "work" reaction
// that emits BOTH status and conditions (the Ready health axis), plus a
// ResyncIntervalSecs so the drift engine re-runs the probe on its cadence.
func (w *healthProbeWorker) Manifest() model.KindManifest {
	return model.KindManifest{
		Kind:               w.kind,
		KindVersion:        1,
		ResyncIntervalSecs: secs(w.resync),
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus, model.OutcomeConditions}},
		},
	}
}

// ─── slowWorker ──────────────────────────────────────────────────────────
//
// A "work" reaction that sleeps for a configured duration before
// returning. Used to validate rate-limit caps (resources accumulate in
// 'working' while we measure the in-flight count).

type slowWorker struct {
	kind  model.Kind
	delay func() time.Duration
	calls atomic.Int64
}

// Kind is the single (kind, version) this worker serves.
func (w *slowWorker) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: converge.Kind(w.kind), Version: 1}
}

// OnConfig is a no-op: this test worker reads no default providerconfig.
func (*slowWorker) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure in-process test controller has no downstream to dial.
func (*slowWorker) Ready() bool { return true }

// Work runs the kind's one "work" reaction (spec change → status), sleeping the
// configured delay first so a test can measure the in-flight count against a rate cap.
func (w *slowWorker) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	w.calls.Add(1)
	timer := time.NewTimer(w.delay())
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return converge.Outcome{}, ctx.Err()
	case <-timer.C:
	}
	out, err := json.Marshal(map[string]string{"name": nameFromSpec(req.Resource.Spec)})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

// Manifest is the inline CRD a test seeds via reg.AddKind: a "work" reaction
// (spec change → status).
func (w *slowWorker) Manifest() model.KindManifest {
	return model.KindManifest{
		Kind:        w.kind,
		KindVersion: 1,
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
}
