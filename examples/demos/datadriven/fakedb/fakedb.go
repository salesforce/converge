// Package fakedb is a second simulation PRODUCER leaf (sibling of fakevpc): on
// reconcile it derives a deterministic fake DB endpoint from its spec's account_id
// and writes it to status. It exists so a downstream consumer (fakeapp) can depend
// on TWO upstreams at once — fakevpc's vpc_id AND fakedb's db_endpoint — exercising
// the engine's multi-edge value-flow gating (a dependent gated until BOTH producers
// are Ready, with both values flowed into its spec).
package fakedb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Kind is the producer kind; its CRD lives in testfixtures/fakedb.kind.json.
const Kind converge.Kind = "fakedb"

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). A pure simulation leaf, so it needs no
// config. Ready is normally true; the demo's readiness knob sets Unhealthy to advertise
// this kind UNREADY (its "database" is unreachable) so the broker parks fakedb work —
// see the datadriven worker main's FAKEDB_UNHEALTHY.
type Provider struct {
	// Unhealthy, when set, makes Ready report false — the demo's stand-in for a provider
	// whose downstream is unreachable. Zero value (false) = always ready, the normal leaf.
	Unhealthy bool
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = Provider{}

// Kind is the single (kind, version) this provider serves.
func (Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// Work derives a deterministic fake DB endpoint from the resource's spec and writes
// it to status — the one reaction (spec change → status) this kind declares.
func (Provider) Work(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	var spec Spec
	if len(req.Resource.Spec) > 0 {
		if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
			return converge.Outcome{}, converge.Terminal(fmt.Errorf("fakedb: decode spec: %w", err))
		}
	}
	if spec.AccountID == "" {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("fakedb: spec.account_id is required"))
	}
	// Deterministic fake endpoint: same account → same db_endpoint across runs.
	out, err := json.Marshal(Status{DBEndpoint: "db-" + spec.AccountID + ".internal:5432"})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

// OnConfig is a no-op: this leaf has no default providerconfig.
func (Provider) OnConfig(converge.ProviderConfig) {}

// Ready reports readiness: true for a normal leaf (no downstream to dial), false when
// Unhealthy is set (the demo's "database unreachable" knob → the broker parks this kind).
func (p Provider) Ready() bool { return !p.Unhealthy }

// Spec is fakedb's desired state: the account it "creates a DB in". The fake
// endpoint is derived from it, so the producer is deterministic.
type Spec struct {
	AccountID string   `json:"account_id" doc:"The account this fake DB lives in; the produced db_endpoint is derived from it."`
	_         struct{} `additionalProperties:"true"`
}

// Status is what fakedb reports once "created": the endpoint downstream resources consume.
type Status struct {
	DBEndpoint string `json:"db_endpoint"`
}
