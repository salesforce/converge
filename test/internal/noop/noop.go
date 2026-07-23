// Package noop implements a Worker-only kind that does no external work: it
// sleeps for a configured delay and reports success. It is the REFERENCE
// converge.Provider (the ONE SDK provider contract — Kinds/Work/OnConfig/Ready):
// the per-worker "environment" — here just a delay — is a field on the Provider
// struct set by its constructor, NOT through converge.Env or a Setup callback
// the SDK orchestrates. The provider self-sources its delay from NOOP_DELAY at
// construction; the host holds a uniform list of providers and never names
// NOOP_DELAY itself.
//
// Because it touches nothing external, it emits a single "work" reaction on a
// spec change (no children, no rollup, no finalizer, no operations) — a pure
// leaf that reaches Ready by writing an empty status after the delay. It needs
// no providerconfig (OnConfig is a no-op) and is always Ready.
package noop

import (
	"context"
	"encoding/json"
	"time"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Provider is the worker-side logic for the noop kind: it implements
// converge.Provider. Its only environment is the delay it sleeps before
// reporting success — captured as a field by the constructor, so the SDK never
// runs a bring-up step. A pure leaf, so it needs no config and is always ready.
// Imports only sdk/* — no internal/*.
type Provider struct {
	// delay is how long Work sleeps before reporting an empty status. Any real
	// worker (AWS, GCP, Kafka) would hold its client/creds here the same way,
	// captured from its own constructor argument, never from converge.Env.
	delay time.Duration
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = Provider{}

// Kinds is the single (kind, version) this provider serves.
func (Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// Work runs the kind's one reaction: sleep the configured delay (respecting
// cancellation), then write an empty status. There is a single reaction, so it
// does not dispatch on req.Reaction.
func (p Provider) Work(ctx context.Context, _ converge.ReactionRequest) (converge.Outcome, error) {
	if p.delay > 0 {
		// Respect cancellation so a long delay doesn't outlive the task's
		// claim or a pod shutdown.
		select {
		case <-time.After(p.delay):
		case <-ctx.Done():
			return converge.Outcome{}, ctx.Err()
		}
	}
	// No external state to report; an empty object is a stable, byte-identical
	// status across runs (the drainer skips redundant downstream patches when
	// the encoded status is unchanged).
	out, err := json.Marshal(NoopStatus{})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

// OnConfig is a no-op: this leaf has no default providerconfig.
func (Provider) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure leaf has no downstream to dial.
func (Provider) Ready() bool { return true }

// New builds a noop provider capturing the given delay — the programmatic sibling of
// NewProvider (which sources the delay from NOOP_DELAY). PURE: it does no I/O, so the
// in-process test harness builds the kind with New(0) and passes the resulting provider
// straight to the executor (the same provider a dumb worker runs via converge.Serve).
// The kind's global concurrency cap is enforced core-side via kind_config, not here, so
// New takes no cap argument.
func New(delay time.Duration) Provider {
	return Provider{delay: delay}
}
