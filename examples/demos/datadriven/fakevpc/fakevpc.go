// Package fakevpc is a simulation leaf that PRODUCES an id in its status, the
// upstream half of a dependency + value-flow demo (its consumer is providers/fakeapp).
// It does no external work: on reconcile it derives a deterministic fake VPC id
// from its spec's account_id and writes it to status. A downstream resource then
// takes that id via a composer-emitted ValueFlow.
//
// This exists to exercise the orchestrator's REAL dependency machinery end to end:
// a DepEdge gates the consumer until this producer is Ready, and a ValueFlow copies
// status.vpc_id into the consumer's spec.vpc_id the moment it becomes available.
// (noop can't play this role — it writes an empty status, producing no id.)
package fakevpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Kind is the producer kind; its CRD lives in testfixtures/fakevpc.kind.json.
const Kind converge.Kind = "fakevpc"

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). A pure simulation leaf, so it needs
// no config and is always ready. Imports only sdk/* — no internal/*.
type Provider struct{}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = Provider{}

// Kind is the single (kind, version) this provider serves.
func (Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// Work derives a deterministic fake VPC id from the resource's spec and writes it to
// status — the one reaction (spec change → status) this kind declares.
func (Provider) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	var spec Spec
	if len(req.Resource.Spec) > 0 {
		if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
			// A bad spec won't fix itself on retry — terminal.
			return converge.Outcome{}, converge.Terminal(fmt.Errorf("fakevpc: decode spec: %w", err))
		}
	}
	if spec.AccountID == "" {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("fakevpc: spec.account_id is required"))
	}
	// Deterministic fake id: same account → same vpc_id across runs (stable status,
	// so the drainer skips redundant downstream patches).
	out, err := json.Marshal(Status{VPCID: "vpc-" + spec.AccountID})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

// OnConfig is a no-op: this leaf has no default providerconfig.
func (Provider) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure simulation leaf has no downstream to dial.
func (Provider) Ready() bool { return true }

// Spec is fakevpc's desired state: the account it "creates a VPC in". The fake id
// is derived from it, so the producer is deterministic.
type Spec struct {
	AccountID string `json:"account_id" doc:"The account this fake VPC lives in; the produced vpc_id is derived from it."`
	// account_id is the only field the composer stamps in; allow extra keys (e.g.
	// the demo's "policy") without tripping huma's additionalProperties:false.
	_ struct{} `additionalProperties:"true"`
}

// Status is what fakevpc reports once "created": the id downstream resources consume.
type Status struct {
	VPCID string `json:"vpc_id"`
}
