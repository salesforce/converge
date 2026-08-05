// Package networking implements the three networking kinds — VPC, TGW, Route —
// as providers: each is a Manifest (pure data) + reaction Handlers (code),
// sharing one stub backend.
//
// VPC additionally exposes one Operator verb (enable_flow_logs) so the
// API can drive subresource invocations without bumping the parent's
// generation. The verb runs concurrently with the kind's reconcile
// work_queue slot — work_queue's UNIQUE(resource_id, task_type) gives
// operate its own row.
package networking

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/sdk-go/converge"
)

// backend is the networking stubs' shared "environment": a simulated-latency delay, a
// teardown delay, and an optional transient-fault rate. Each single-pair provider below
// closes over one backend.
type backend struct {
	delay       time.Duration
	deleteDelay time.Duration
	faults      fault.Injector
}

// provider is ONE networking (kind, version) as a converge.Provider — the single-pair
// contract. work is the pair's reconcile handler (vpc/v1 vs vpc/v2 differ), selected by
// Providers below; teardown + the operate verb are handled uniformly by dispatch on
// req.Reaction. A worker that serves several networking kinds registers several of these
// (w.Add per element of Providers()); the SDK owns the fan-out.
type provider struct {
	kv      converge.KindVersion
	be      backend
	work    converge.ReactionHandler // the pair's reconcile handler
	hasVerb bool                     // vpc exposes the enable_flow_logs operate verb
}

var _ converge.Provider = provider{}

func (p provider) Kind() converge.KindVersion     { return p.kv }
func (provider) OnConfig(converge.ProviderConfig) {} // pure stubs: no providerconfig
func (provider) Ready() bool                      { return true }

// Work dispatches one task for this pair on req.Reaction: teardown → the paced
// finalizer, the operate verb (vpc only) → enable_flow_logs, else the pair's reconcile
// handler. Every reaction the core dispatches for this pair lands here.
func (p provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	switch req.Reaction {
	case "teardown":
		return fault.Teardown{Delay: p.be.deleteDelay}.React(ctx, req)
	case "operate:enable_flow_logs":
		if p.hasVerb {
			return enableFlowLogs{delay: p.be.delay}.React(ctx, req)
		}
	}
	return p.work.React(ctx, req)
}

// New reads the demo env knobs (FAKE_WORK_DELAY / FAKE_FAULT_RATE / FAKE_DELETE_DELAY)
// and returns the configured networking providers — one per (kind, version): vpc/v1,
// vpc/v2, tgw/v1, route/v1. The worker lists them in the converge.Serve slice (or ranges
// Providers()); the SDK fans out. A demo deletion visibly cascades with the env teardown
// delay (default 5s); tests build the pairs directly with zero delays via Runtime().
func New() ([]converge.Provider, error) {
	var cfg struct {
		Delay       time.Duration `envconfig:"FAKE_WORK_DELAY"`
		FaultRate   float64       `envconfig:"FAKE_FAULT_RATE"`
		DeleteDelay string        `envconfig:"FAKE_DELETE_DELAY"`
	}
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}
	be := backend{
		delay:       cfg.Delay,
		deleteDelay: fault.DeleteDelayFromEnv(cfg.DeleteDelay),
		faults:      fault.Injector{Rate: cfg.FaultRate},
	}
	return providers(be), nil
}

// providers builds the four single-pair networking providers over a backend — vpc at v1
// AND v2 (their reconcile handlers differ), plus tgw/v1 and route/v1. Each version is
// explicit (no implicit default).
func providers(be backend) []converge.Provider {
	return []converge.Provider{
		provider{kv: converge.KindVersion{Kind: KindVPC, Version: 1}, be: be, work: vpcWorker{delay: be.delay, faults: be.faults}, hasVerb: true},
		provider{kv: converge.KindVersion{Kind: KindVPC, Version: 2}, be: be, work: vpcV2Worker{delay: be.delay, faults: be.faults}, hasVerb: true},
		provider{kv: converge.KindVersion{Kind: KindTGW, Version: 1}, be: be, work: tgwWorker{delay: be.delay, faults: be.faults}},
		provider{kv: converge.KindVersion{Kind: KindRoute, Version: 1}, be: be, work: routeWorker{delay: be.delay, faults: be.faults}},
	}
}

// VPCRuntime is the vpc/v1 provider: work + a simulated teardown (finalizer) + the
// enable_flow_logs operate verb, all dispatched by provider.Work on req.Reaction. PURE —
// no I/O, so tests build it with VPCRuntime(0, 0, fault.Injector{}). deleteDelay paces the
// simulated teardown (0 = instant); faults simulates transient reconcile failures (zero
// value = never). The enable_flow_logs verb and the teardown are NOT faulted — fault
// injection targets reconcile.
func VPCRuntime(delay, deleteDelay time.Duration, faults fault.Injector) converge.Provider {
	be := backend{delay: delay, deleteDelay: deleteDelay, faults: faults}
	return provider{kv: converge.KindVersion{Kind: KindVPC, Version: 1}, be: be, work: vpcWorker{delay: delay, faults: faults}, hasVerb: true}
}

// VPCv2Runtime is the vpc/v2 provider served by the SAME worker binary. It's the
// BREAKING kindVersion: v2's spec requires `region` (VPCv2Spec), so the v2 worker reads
// and echoes region into status — a resource pinned to vpc/v2 routes here, a
// vpc/v1 resource never does (strict (kind, kindVersion) routing). Provider authors
// declare a new kindVersion exactly this way: a provider with a v2 KindVersion + the v2
// reconcile handler. Same teardown + flow-logs verb as v1.
func VPCv2Runtime(delay, deleteDelay time.Duration, faults fault.Injector) converge.Provider {
	be := backend{delay: delay, deleteDelay: deleteDelay, faults: faults}
	return provider{kv: converge.KindVersion{Kind: KindVPC, Version: 2}, be: be, work: vpcV2Worker{delay: delay, faults: faults}, hasVerb: true}
}

// TGWRuntime is the tgw/v1 provider: work + simulated teardown.
func TGWRuntime(delay, deleteDelay time.Duration, faults fault.Injector) converge.Provider {
	be := backend{delay: delay, deleteDelay: deleteDelay, faults: faults}
	return provider{kv: converge.KindVersion{Kind: KindTGW, Version: 1}, be: be, work: tgwWorker{delay: delay, faults: faults}}
}

// RouteRuntime is the route/v1 provider: work + simulated teardown.
func RouteRuntime(delay, deleteDelay time.Duration, faults fault.Injector) converge.Provider {
	be := backend{delay: delay, deleteDelay: deleteDelay, faults: faults}
	return provider{kv: converge.KindVersion{Kind: KindRoute, Version: 1}, be: be, work: routeWorker{delay: delay, faults: faults}}
}

// AllRuntimes is a convenience for wiring up all four networking pairs in one go
// (tests, fixtures) — uncapped, with instant (deleteDelay=0) teardown. The worker's
// New builds them directly with the env teardown delay instead.
func AllRuntimes(delay time.Duration, faults fault.Injector) []converge.Provider {
	return []converge.Provider{
		VPCRuntime(delay, 0, faults),   // vpc/v1
		VPCv2Runtime(delay, 0, faults), // vpc/v2
		TGWRuntime(delay, 0, faults),
		RouteRuntime(delay, 0, faults),
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Worker handlers — one per kind, sharing a stubbed backend.
// ─────────────────────────────────────────────────────────────────────────

// sleepCtx blocks for d, returning ctx.Err() if the context is cancelled
// first. The stub workers' only "environment" is a simulated-latency
// delay; a bare time.Sleep would block an orphaned goroutine past a
// TaskDeadline cancel or pod shutdown, so the wait is made cancelable.
// d<=0 is a no-op.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type vpcWorker struct {
	delay  time.Duration
	faults fault.Injector
}

func (w vpcWorker) React(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if err := sleepCtx(ctx, w.delay); err != nil {
		return converge.Outcome{}, err
	}
	if w.faults.Roll() {
		return converge.Outcome{}, w.faults.Fail(string(KindVPC), req.Resource.Name)
	}
	var spec VPCSpec
	if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
		return converge.Outcome{}, fmt.Errorf("decode vpc spec: %w", err)
	}
	b, err := json.Marshal(VPCStatus{VPCID: "vpc-" + spec.AccountID})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: b}, nil
}

// vpcV2Worker is the vpc/v2 reconcile handler. It reads the v2 spec (which
// REQUIRES region) and echoes the region into status — so an operator watching a
// vpc/v2 resource SEES that the v2 worker ran it (a v1 worker, which doesn't
// understand region, never gets a v2 task). This is the whole point of a versioned
// worker: v2 code for v2 resources.
type vpcV2Worker struct {
	delay  time.Duration
	faults fault.Injector
}

func (w vpcV2Worker) React(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if err := sleepCtx(ctx, w.delay); err != nil {
		return converge.Outcome{}, err
	}
	if w.faults.Roll() {
		return converge.Outcome{}, w.faults.Fail(string(KindVPC)+"/v2", req.Resource.Name)
	}
	var spec VPCv2Spec
	if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
		return converge.Outcome{}, fmt.Errorf("decode vpc/v2 spec: %w", err)
	}
	b, err := json.Marshal(VPCv2Status{VPCID: "vpc-" + spec.AccountID, Region: spec.Region})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: b}, nil
}

type tgwWorker struct {
	delay  time.Duration
	faults fault.Injector
}

func (w tgwWorker) React(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if err := sleepCtx(ctx, w.delay); err != nil {
		return converge.Outcome{}, err
	}
	if w.faults.Roll() {
		return converge.Outcome{}, w.faults.Fail(string(KindTGW), req.Resource.Name)
	}
	var spec TGWSpec
	if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
		return converge.Outcome{}, fmt.Errorf("decode tgw spec: %w", err)
	}
	b, err := json.Marshal(TGWStatus{TGWID: "tgw-" + spec.Name})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: b}, nil
}

type routeWorker struct {
	delay  time.Duration
	faults fault.Injector
}

func (w routeWorker) React(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if err := sleepCtx(ctx, w.delay); err != nil {
		return converge.Outcome{}, err
	}
	if w.faults.Roll() {
		return converge.Outcome{}, w.faults.Fail(string(KindRoute), req.Resource.Name)
	}
	var spec RouteSpec
	if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
		return converge.Outcome{}, fmt.Errorf("decode route spec: %w", err)
	}
	b, err := json.Marshal(RouteStatus{RouteID: "rt-" + spec.VPCID + "-" + spec.TGWID})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: b}, nil
}

// ─────────────────────────────────────────────────────────────────────────
// Operator verb — enable_flow_logs (VPC only).
//
// Declared as a TriggerOperation reaction on the VPC kind. The reaction runs
// in its own work_queue slot so the API can dispatch the verb while a regular
// reconcile is in flight. The core writes the resource_operations row's
// terminal state from the outbox after React returns; the verb can be invoked
// again as a fresh op row.
// ─────────────────────────────────────────────────────────────────────────

// EnableFlowLogsInput is the API request body the verb accepts.
type EnableFlowLogsInput struct {
	S3Bucket string `json:"s3_bucket" minLength:"1" doc:"Destination S3 bucket for flow logs."`
}

// EnableFlowLogsOutput is what the verb writes back.
type EnableFlowLogsOutput struct {
	FlowLogsID string `json:"flow_logs_id" doc:"AWS flow logs id."`
}

type enableFlowLogs struct{ delay time.Duration }

func (op enableFlowLogs) React(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if err := sleepCtx(ctx, op.delay); err != nil {
		return converge.Outcome{}, err
	}
	if req.Operation == nil {
		return converge.Outcome{}, fmt.Errorf("enable_flow_logs: missing operation")
	}
	var in EnableFlowLogsInput
	if len(req.Operation.Input) > 0 {
		if err := json.Unmarshal(req.Operation.Input, &in); err != nil {
			return converge.Outcome{}, fmt.Errorf("decode enable_flow_logs input: %w", err)
		}
	}
	if in.S3Bucket == "" {
		return converge.Outcome{}, fmt.Errorf("enable_flow_logs: s3_bucket is required")
	}
	out, err := json.Marshal(EnableFlowLogsOutput{FlowLogsID: "fl-" + req.Resource.Name + "-" + in.S3Bucket})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{OperationOutput: out}, nil
}
