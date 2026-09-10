package model

import "encoding/json"

// Wire DTOs — the per-stage request/response shapes the Connect wire layer
// (internal/wire ↔ sdk-go/workerpb) and the broker fanout / remote worker use to ship
// a reaction to a dumb worker. They are PLAIN DATA (no behaviour): dispatch runs
// via ReactionRequest/Outcome + StageDispatcher, but the typed proto
// envelopes still mirror these per-stage shapes on the wire (compose carries
// children/edges/configs; work/rollup carry status; operate carries output), so
// keeping them as DTOs lets the wire stay typed without re-deriving the proto.
// They are NOT part of the authoring contract (a provider implements a handler,
// not these).

// ComposeRequest is the compose wire input: the root + its observed children
// (Desired/Edges/Configs are accumulator fields the wire carries; the
// single-handler model leaves them empty on input and fills the Outcome instead).
type ComposeRequest struct {
	Root     Resource
	Observed []Resource
	Desired  []ChildSpec
	Edges    []DepEdge
	Configs  []ProviderConfigSpec
	Env      *Env
}

// WorkRequest is the work wire input: the resource + accumulated status.
type WorkRequest struct {
	Resource Resource
	Status   json.RawMessage
	Env      *Env
}

// RollupRequest is the rollup wire input: the root + its settled subtree.
type RollupRequest struct {
	Root        Resource
	Descendants []Resource
	Status      json.RawMessage
	Env         *Env
}

// DeleteRequest is the delete wire input.
type DeleteRequest struct {
	Resource Resource
	Env      *Env
}

// OperateRequest is the operate wire input: the resource + the operation row.
type OperateRequest struct {
	Resource  Resource
	Operation Operation
	Env       *Env
}

// ReactRequest is the lifecycle-reactor wire input: the resource that crossed a
// transition, the transition name, and the dedup token (the stable per-binding
// key an idempotent reactor uses to no-op a redelivery). Status rides in the
// Resource; the sink config rides in Env.ProviderConfig (like every other stage).
type ReactRequest struct {
	Resource   Resource
	Transition Transition
	DedupToken string
	Env        *Env
}

// ComposePipelineResult is the compose wire output: desired children/edges/configs
// + status/conditions (and an error with the failing stage index). It is the
// only *PipelineResult still carried on the wire — work/rollup/delete/operate
// results ride back as the bare Outcome fields (status/conditions/output) in
// StageComplete, so they need no dedicated result DTO.
type ComposePipelineResult struct {
	Desired     []ChildSpec
	Edges       []DepEdge
	Configs     []ProviderConfigSpec
	Status      json.RawMessage
	Conditions  []Condition
	FailedStage int
	Err         error
}
