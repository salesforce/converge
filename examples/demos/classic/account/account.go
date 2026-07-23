// Package account implements the account kind under the ONE converge.Provider
// contract: a single provider that serves account/v1 and routes every reaction the
// kind declares through Work. Stub implementation suitable for tests and demos; a real
// account worker replaces the simulated AWS calls with a cloud client it dials on its
// own schedule (constructor or lazily) and reports through Ready.
package account

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/sdk-go/converge"
)

// Reaction names the account kind declares in its manifest (account.kind.json). Work
// switches on req.Reaction to route to the matching body — these consts are the link
// between the manifest's ReactionDecl.Name and the dispatch here; keep them in sync.
const (
	reactionWork     = "work"     // specChange → write status
	reactionTeardown = "teardown" // deleteRequested → simulate finalizer cleanup
)

// Provider is the worker-side logic for the account kind under the converge.Provider
// contract. A pure simulation leaf: it dials no downstream, needs no config, and is
// always ready. The stub's only "environment" is a simulated-latency delay, an optional
// transient-fault rate (see package fault), and a paced teardown — all carried as fields
// so Work can reproduce the old handlers' behavior. A real account worker would hold its
// AWS client here and report bring-up through Ready.
type Provider struct {
	// delay simulates per-reconcile latency (FAKE_WORK_DELAY); 0 = instant.
	delay time.Duration
	// deleteDelay paces the simulated teardown (FAKE_DELETE_DELAY) so a demo deletion
	// visibly cascades bottom-up.
	deleteDelay time.Duration
	// faults injects transient failures (FAKE_FAULT_RATE); zero value = never.
	faults fault.Injector
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = Provider{}

// Kind is the single (kind, version) this provider serves — explicit web-API version
// (no implicit v1 default).
func (Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// Work runs one task, dispatching on the manifest-selected reaction: "work" reconciles
// the account (spec → status, with the simulated delay + transient-fault knob) and
// "teardown" simulates the finalizer cleanup (paced by deleteDelay) so the deletion
// cascade is watchable. The union of the old work + teardown handler bodies.
func (p Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	switch req.Reaction {
	case reactionTeardown:
		return p.teardown(ctx, req)
	default:
		// Any other reaction (including the manifest's "work") reconciles the account;
		// the default keeps a single-reaction manifest working without naming "work".
		return p.reconcile(ctx, req)
	}
}

// reconcile is the "work" reaction: honor the simulated delay, roll the transient-fault
// knob, then derive the stub account id from the spec and write it to status.
func (p Provider) reconcile(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	if p.delay > 0 {
		// Respect cancellation so the simulated delay doesn't outlive the task's
		// deadline or a pod shutdown.
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return converge.Outcome{}, ctx.Err()
		}
	}
	// Simulated transient failure (no-op unless FAKE_FAULT_RATE>0): a plain error
	// is retryable, so the resource flaps Reconciling→Failed→Reconciling→Ready.
	if p.faults.Roll() {
		return converge.Outcome{}, p.faults.Fail(string(Kind), req.Resource.Name)
	}
	var spec AccountSpec
	if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
		return converge.Outcome{}, fmt.Errorf("decode account spec: %w", err)
	}
	out, err := json.Marshal(AccountStatus{AccountID: "acc-stub-" + spec.TeamName})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

// teardown is the "teardown" reaction: the shared simulated finalizer/delete handler,
// paced by deleteDelay so the reverse-dependency cascade is watchable.
func (p Provider) teardown(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	return fault.Teardown{Delay: p.deleteDelay}.React(ctx, req)
}

// OnConfig is a no-op: this leaf has no default providerconfig to react to.
func (Provider) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure simulation leaf has no downstream to dial.
func (Provider) Ready() bool { return true }

// NewFromEnv builds a Provider from the classic-demo env knobs: FAKE_WORK_DELAY paces
// each reconcile, FAKE_FAULT_RATE injects transient failures (see package fault), and
// FAKE_DELETE_DELAY (default 5s) paces the simulated teardown. This is where the worker
// reads its "environment"; a real account worker would also build its AWS client here.
func NewFromEnv() (Provider, error) {
	var cfg struct {
		Delay       time.Duration `envconfig:"FAKE_WORK_DELAY"`
		FaultRate   float64       `envconfig:"FAKE_FAULT_RATE"`
		DeleteDelay string        `envconfig:"FAKE_DELETE_DELAY"`
	}
	if err := envconfig.Process("", &cfg); err != nil {
		return Provider{}, err
	}
	return Provider{
		delay:       cfg.Delay,
		deleteDelay: fault.DeleteDelayFromEnv(cfg.DeleteDelay),
		faults:      fault.Injector{Rate: cfg.FaultRate},
	}, nil
}

// New builds a configured Provider from explicit knobs (delay paces each reconcile,
// deleteDelay paces the simulated teardown, faults injects transient failures). It is
// the programmatic sibling of NewFromEnv — the in-process test harness constructs the
// kind with it (via the test-only demoruntime bridge), and the dumb worker runs the
// same Provider via converge.Serve. PURE — no I/O, so tests build it with
// New(0, 0, fault.Injector{}).
func New(delay, deleteDelay time.Duration, faults fault.Injector) Provider {
	return Provider{delay: delay, deleteDelay: deleteDelay, faults: faults}
}
