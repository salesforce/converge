package converge

import (
	"context"
	"encoding/json"
)

// ProviderConfig is a kind's default providerconfig as the provider sees it: a JSON
// Spec and opaque raw Data bytes. Both axes travel together here — Spec is the
// structured config document (unmarshal into your own type), Data is the opaque bundle
// (e.g. a zip of Starlark files, a template archive). Data is RAW bytes end to end; the
// base64 encoding you see on the public/REST API is only how []byte crosses JSON on the
// wire to/from that API — it is decoded before it reaches here. Either may be empty: a
// spec-only config has nil Data, a bundle-only config has nil Spec, and a deleted
// default arrives as both nil.
type ProviderConfig struct {
	Spec json.RawMessage
	Data []byte
}

// Provider is the CLIENT-SDK contract: everything the SDK needs from a worker's
// business logic for ONE (kind, version), and nothing more. The SDK calls ONLY these
// methods — it NEVER runs a setup/bring-up step. A provider dials its own downstreams
// (Kafka, a DB pool, a cloud client) on its OWN schedule (in its constructor, a
// background goroutine, lazily on first Work — its choice) and simply REPORTS through
// Ready whether it can currently do work. That is the whole bring-up contract:
// readiness, not a Setup callback the SDK orchestrates.
//
// ONE Provider serves ONE (kind, version). A worker that serves several — a single kind
// at v1 AND v2 (their specs differ, so their Work differs), or several distinct kinds —
// lists ONE Provider per pair in the slice passed to converge.Serve; the SDK owns the fan-out and
// routes each claimed task to the matching provider. There is no in-provider switch on
// kind/version: the pair is fixed by Kind(), so Work/OnConfig/Ready need no kind/version
// argument.
//
// The four methods:
//
//   - Kind returns the single (kind, version) this provider serves. Pure — called once
//     at registration; no I/O.
//   - Work runs one task for that pair. The returned Outcome is applied by the core
//     (status, children, etc.); an error fails the task (wrap with Terminal to stop
//     retries — a transient error re-dispatches). A kind that declares several reactions
//     (compose + rollup, say) switches on req.Reaction inside Work. The config Work should
//     act on is the EFFECTIVE config: the kind default overlaid with this resource's
//     optional per-resource override, which rides on the task in req.Env — resolve it with
//     EffectiveConfig (see OnConfig for the two config axes).
//   - OnConfig receives the kind's DEFAULT providerconfig — NOT a per-resource override.
//     The SDK delivers the full {Spec, Data} monolith here ONCE at startup (the primed
//     default, before the first task) and again whenever an operator edits it, so OnConfig
//     is the hook to react to that config (dial a client, recompile a bundle); an empty cfg
//     (nil Spec + nil Data) = the default was deleted. The EFFECTIVE config a given task
//     runs on is default ⊕ that resource's per-resource override, and that merge happens
//     PER TASK in Work (via EffectiveConfig over req.Env.ProviderConfig), not here: Spec is
//     a deep JSON merge (the override wins per key), while Data (the opaque bundle) is a
//     whole-artifact REPLACE (a bundle can't be deep-merged).
//   - Ready reports whether this provider can currently do work. The SDK polls it and, on
//     a true→false edge, tells the broker to STOP sending this pair's work (RS-);
//     false→true resumes it (RS+). A provider still dialing its downstream returns false
//     until connected; one whose Kafka broke returns false until it recovers. This is how
//     a worker sheds a single degraded pair over its shared stream.
type Provider interface {
	Kind() KindVersion
	Work(ctx context.Context, req ReactionRequest) (Outcome, error)
	OnConfig(cfg ProviderConfig)
	Ready() bool
}

// providerHandler adapts a Provider's Work to ReactionHandler (the runner's per-task
// seam). Every reaction the core dispatches for the provider's pair lands in Work; the
// provider switches on req.Reaction if its kind declares more than one.
type providerHandler struct{ p Provider }

func (h providerHandler) React(ctx context.Context, req ReactionRequest) (Outcome, error) {
	return h.p.Work(ctx, req)
}
