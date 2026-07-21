// Package fakeapp is the simulation leaf that CONSUMES values flowed from TWO
// upstream resources — the downstream half of the dependency + value-flow demo. Its
// producers are providers/fakevpc (vpc_id) and providers/fakedb (db_endpoint). The
// composer emits fakeapp with BOTH fields LEFT EMPTY plus two DepEdges, each
// carrying a ValueFlow; the engine schedules it only after BOTH upstreams are Ready
// and fills spec.vpc_id + spec.db_endpoint from their statuses first.
//
// Work is also the ASSERTION that BOTH flows happened: it fails (terminal) if
// either field is still empty when it runs, and otherwise records what it was wired
// to — so a green fakeapp proves the two dependency edges gated it AND both values
// flowed.
package fakeapp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Kind is the consumer kind; its CRD lives in testfixtures/fakeapp.kind.json.
const Kind converge.Kind = "fakeapp"

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). A pure simulation leaf, so it needs
// no config and is always ready. Imports only sdk/* — no internal/*.
type Provider struct{}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = Provider{}

// Kind is the single (kind, version) this provider serves.
func (Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// Work is the one reaction (spec change → status) this kind declares. It asserts both
// value flows delivered: if a value flow had NOT delivered an upstream value, that field
// would still be empty here. Terminal (not transient) is CORRECT, not a footgun: the
// engine's dep-gate guarantees a dependent is never scheduled while ANY of its upstreams
// lag (resource_deps dep-gate `synced_gen < generation` + a FOR SHARE during compose-time
// inline substitution — see db schema). So reaching this handler at all means BOTH flows
// already ran; an empty field here is a genuine misconfiguration that a same-generation
// retry can't fix. Terminal makes a broken edge LOUD instead of silently spinning.
func (Provider) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	var spec Spec
	if len(req.Resource.Spec) > 0 {
		if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
			return converge.Outcome{}, converge.Terminal(fmt.Errorf("fakeapp: decode spec: %w", err))
		}
	}
	if spec.VPCID == "" {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("fakeapp: vpc_id empty — the fakevpc value flow did not deliver"))
	}
	if spec.DBEndpoint == "" {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("fakeapp: db_endpoint empty — the fakedb value flow did not deliver"))
	}
	out, err := json.Marshal(Status{WiredTo: spec.VPCID, DB: spec.DBEndpoint})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

// OnConfig is a no-op: this leaf has no default providerconfig.
func (Provider) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure simulation leaf has no downstream to dial.
func (Provider) Ready() bool { return true }

// Spec is fakeapp's desired state. VPCID + DBEndpoint are filled by the framework
// from the upstream fakevpc's + fakedb's statuses (composer-emitted ValueFlows into
// /vpc_id + /db_endpoint), so the composer emits them ABSENT — child validation
// skips the flowed paths (store.ApplyComposeResult → ValidateSpecPartial), exactly
// like networking.RouteSpec.VPCID. A user creating a fakeapp directly would set them.
type Spec struct {
	VPCID      string   `json:"vpc_id" doc:"Upstream VPC id; flowed in from a fakevpc's status by a composer ValueFlow."`
	DBEndpoint string   `json:"db_endpoint" doc:"Upstream DB endpoint; flowed in from a fakedb's status by a composer ValueFlow."`
	_          struct{} `additionalProperties:"true"`
}

// Status records what the app was wired to — green status = BOTH flows delivered.
type Status struct {
	WiredTo string `json:"wired_to"` // the vpc_id
	DB      string `json:"db"`       // the db_endpoint
}
