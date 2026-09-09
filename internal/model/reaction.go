package model

import "encoding/json"

// ReactionRequest carries everything a reaction may need. The core fills only
// the fields a given reaction needs (driven by its Trigger): SpecChange/Work
// gets Resource+Status; a composer also reads Observed; a rollup gets
// Descendants; operate gets Operation; lifecycle gets Transition+DedupToken.
// Reaction + Trigger tell the worker which handler to run and why.
type ReactionRequest struct {
	Reaction    string  // the ReactionDecl.Name — selects the handler
	KindVersion int     // the resource's web-API version (>= 1) — lets a Work serving several versions of one kind route (vpc/v1 vs vpc/v2)
	Trigger     Trigger // why it fired

	Resource    Resource        // the subject resource, with its current Spec/Status
	Status      json.RawMessage // accumulated status (work/compose/rollup); nil for the first
	Observed    []Resource      // composer: current owned children + statuses
	Descendants []Resource      // rollup: the whole settled subtree
	Operation   *Operation      // operate: the resource_operations row
	Transition  Transition      // reactor: the lifecycle edge that fired (a TransitionX constant)
	Generation  int64           // the generation that fired (pin reads/keys to it)
	DedupToken  string          // lifecycle: stable "<id>:<transition>:<gen>:<reaction>" for non-idempotent sinks
	Env         *Env            // logger + this resource's per-resource config override (merge with the default in the handler)
}

// Outcome carries everything a reaction may produce. A handler fills only the
// parts its reaction declared in Emits; the core applies exactly those (a part
// set without the matching Emits bit is ignored, and the manifest legality
// lattice forbids the dangerous combinations). Status is last-writer-wins;
// Conditions accumulate.
type Outcome struct {
	Status     json.RawMessage      // work/compose/rollup status
	Conditions []Condition          // health/custom axes
	Children   []ChildSpec          // composer: desired children
	Edges      []DepEdge            // composer: dep edges (+ value flows)
	Configs    []ProviderConfigSpec // composer: provider configs it owns

	OperationOutput json.RawMessage // operate: verb output
	SideEffectDone  bool            // lifecycle: the side effect completed (informational)
}
