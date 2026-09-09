package converge

// types.go holds the worker SDK's contract types — the shapes a provider author reads and
// returns, wrapping the generated worker proto (sdk-go/workerpb). The broker speaks proto;
// convert.go translates proto ⇄ these types at the SDK edge. The core keeps its own
// equivalents (internal/model, converted by internal/wire). Kept name-for-name aligned with
// sdk-ts/src/types.ts (see docs/spec/sdk-spec.md).

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"dario.cat/mergo"
	"github.com/google/uuid"
)

// ─────────────────────────────────────────────────────────────────────────
// Kind identity.
// ─────────────────────────────────────────────────────────────────────────

// Kind names a resource kind. String-aliased so call sites can use untyped string
// literals or typed constants interchangeably.
type Kind string

// KindVersion is a (kind, web-API version) pair — the addressable unit of everything
// versioned: what a provider serves, what a worker advertises, what the broker routes.
// Version is EXPLICIT (>= 1); there is no implicit v1.
type KindVersion struct {
	Kind    Kind
	Version int
}

// ResourceRef identifies a sibling resource by (Kind, Name) — the natural key under a
// single owner. Used in DepEdge and ChildSpec.
type ResourceRef struct {
	Kind Kind
	Name string
}

const refSep = "/"

func (r ResourceRef) String() string { return string(r.Kind) + refSep + r.Name }

// ─────────────────────────────────────────────────────────────────────────
// Trigger / Transition — the two author-facing lifecycle enums a reaction reads off
// its ReactionRequest.
// ─────────────────────────────────────────────────────────────────────────

// Trigger is the condition that fired a reaction — why Work was called.
type Trigger string

const (
	TriggerSpecChange      Trigger = "specChange"
	TriggerChildrenSettled Trigger = "childrenSettled"
	TriggerDeleteRequested Trigger = "deleteRequested"
	TriggerOperation       Trigger = "operation"
	TriggerReactor         Trigger = "reactor"
	TriggerResync          Trigger = "resync"
)

// Transition is the resource lifecycle edge a reactor delivery fired on (delivered to
// the reactor handler as data on ReactionRequest.Transition).
type Transition string

const (
	TransitionCreated  Transition = "created"
	TransitionSynced   Transition = "synced"
	TransitionDegraded Transition = "degraded"
	TransitionFailed   Transition = "failed"
	TransitionDeleted  Transition = "deleted"
)

// ─────────────────────────────────────────────────────────────────────────
// Conditions — K8s/Crossplane multi-axis status.
// ─────────────────────────────────────────────────────────────────────────

// ConditionStatus mirrors metav1.ConditionStatus.
type ConditionStatus string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// Standard condition types. Providers may also emit their own custom axes (e.g.
// "Bound", "Healthy"); only TypeReady feeds the health_ok scalar that gates is_ready.
const (
	// TypeReady is the health/availability axis (Crossplane "Ready"). A reaction that
	// returns TypeReady=False sets the resource's health_ok=false, demoting is_ready even
	// when fully synced. A reaction that emits no Ready condition is treated as healthy.
	TypeReady = "Ready"
	// TypeSynced is the spec-reconciled axis (Crossplane "Synced"). DERIVED from
	// synced_gen vs generation and synthesized by the API; providers do not normally emit it.
	TypeSynced = "Synced"
)

// Condition is one axis of a resource's status — K8s/Crossplane shape. Returned by a
// reaction in its Outcome's Conditions slice; the core upserts them on transition and,
// for Type==TypeReady, folds Status into the health_ok scalar.
type Condition struct {
	Type    string          // "Ready", "Synced", or a custom axis
	Status  ConditionStatus // True | False | Unknown
	Reason  string          // CamelCase machine code, e.g. "Available"
	Message string          // human-readable detail
}

// ─────────────────────────────────────────────────────────────────────────
// Terminal — mark a reaction failure non-retryable.
// ─────────────────────────────────────────────────────────────────────────

// terminalError marks a reaction failure as TERMINAL: it will not succeed on retry
// without a spec change, so the scheduler stops re-queuing it. A plain error is treated
// as TRANSIENT (retried).
type terminalError struct{ err error }

func (e *terminalError) Error() string { return e.err.Error() }
func (e *terminalError) Unwrap() error { return e.err }

// Terminal wraps err to signal a non-retryable (terminal) failure. nil in → nil out.
func Terminal(err error) error {
	if err == nil {
		return nil
	}
	return &terminalError{err: err}
}

// IsTerminal reports whether err (or anything it wraps) was marked terminal.
func IsTerminal(err error) bool {
	var te *terminalError
	return errors.As(err, &te)
}

// ─────────────────────────────────────────────────────────────────────────
// ChildSpec / DepEdge / ValueFlow / ProviderConfigSpec — what a composer emits.
// ─────────────────────────────────────────────────────────────────────────

// ChildSpec describes one child a composer wants to exist under a parent. Spec is the
// typed Go value; the core JSON-marshals at the DB boundary. KindVersion is REQUIRED and
// explicit (>= 1) — a composer emits each child at the version it wants.
type ChildSpec struct {
	Kind        Kind
	KindVersion int
	Name        string
	Spec        any
	Labels      map[string]string
}

// ProviderConfigSpec is one provider config a composer emits alongside its children and
// edges. KindVersion is the web-API version of the consumer Kind this config is for
// (REQUIRED, >= 1).
type ProviderConfigSpec struct {
	Name        string
	Kind        Kind
	KindVersion int
	IsDefault   bool
	Spec        any
}

// DepEdge: dependent (From) depends on dependency (To), optionally carrying value flows
// that fill the dependent's spec from the upstream's status when the upstream is ready.
type DepEdge struct {
	From   ResourceRef
	To     ResourceRef
	Values []ValueFlow
}

// ValueFlow declares one field-level flow on a DepEdge: the dependent's
// spec[DependentField] is filled from the upstream's status[SourceField] (both RFC 6901
// JSON pointers) when the upstream's status becomes available.
type ValueFlow struct {
	DependentField string
	SourceField    string
}

// ─────────────────────────────────────────────────────────────────────────
// Resource — the provider-side view of a row from the resources table.
// ─────────────────────────────────────────────────────────────────────────

// Resource is what a reaction handler receives. JSON columns are json.RawMessage so
// handlers re-marshal without conversion. State is observed via IsReady (synced AND
// healthy AND not deleting).
type Resource struct {
	ID                  uuid.UUID
	Kind                Kind
	Name                string
	OwnerID             *uuid.UUID
	RootID              *uuid.UUID
	Spec                json.RawMessage
	Status              json.RawMessage
	Generation          int64
	SyncedGen           int64
	IsReady             bool
	Finalizers          []string
	DeletionRequestedAt *time.Time
	// FrozenUntil (orphan-grace deadline) and Quarantined (operator set-aside) are the
	// two faces of the resources frozen_until column; a rollup must skip either (IsFrozen).
	FrozenUntil *time.Time
	Quarantined bool
	Labels      map[string]string
}

// IsFrozen reports whether the resource is set aside from scheduling and rollup —
// orphaned (in the grace window) OR quarantined.
func (r Resource) IsFrozen() bool { return r.FrozenUntil != nil || r.Quarantined }

// Operation is the provider-side view of a resource_operations row, passed to an
// operation reaction's handler. ResourceID (the resource's public uuid) identifies what
// the verb runs against; there is no internal row-id.
type Operation struct {
	ResourceID  uuid.UUID
	Verb        string
	Input       json.RawMessage
	Attempts    int
	RequestedBy string
}

// ─────────────────────────────────────────────────────────────────────────
// Env — per-call environment a reaction handler receives.
// ─────────────────────────────────────────────────────────────────────────

// Env is intentionally narrow: a handler describes what it wants done and lets the core
// persist it (handlers issue no raw SQL). Logger is scoped slog with task fields.
//
// ProviderConfig is the per-task CUSTOM config override (nil when the resource carries no
// custom override); the kind DEFAULT reaches the provider through OnConfig. A handler
// computes its EFFECTIVE config by overlaying this override on the default via
// EffectiveConfig. ProviderBundle is the per-resource CUSTOM bundle override; use
// EffectiveBundle (whole-replace, not a merge — opaque bytes can't deep-merge).
type Env struct {
	Logger         *slog.Logger
	ProviderConfig json.RawMessage
	ProviderBundle []byte
	// Heartbeat is an OPTIONAL progress hint a long-running handler MAY call; it is NOT
	// required to keep the task's lease alive. Liveness is attested for as long as the
	// handler goroutine runs (the worker's WorkHeartbeat lists every still-running task), so
	// a straight-line handler that never calls Heartbeat stays attested for its whole run.
	// It is a no-op when nil, which is the common case (shipped code wires no hook; the
	// in-process test path has no broker to attest to). Reserved for future finer-grained
	// liveness. Safe to call from any goroutine; cheap and non-blocking.
	Heartbeat func()
}

// ─────────────────────────────────────────────────────────────────────────
// ReactionRequest / Outcome — the request/response a handler speaks.
// ─────────────────────────────────────────────────────────────────────────

// ReactionRequest carries everything a reaction may need. The core fills only the fields
// a given reaction needs (driven by its Trigger). Reaction + Trigger tell the worker
// which handler to run and why.
type ReactionRequest struct {
	Reaction    string
	KindVersion int
	Trigger     Trigger

	Resource    Resource
	Status      json.RawMessage
	Observed    []Resource // composer: current owned children + statuses
	Descendants []Resource // rollup: the whole settled subtree
	Operation   *Operation // operate: the resource_operations row
	Transition  Transition // reactor: the lifecycle edge that fired
	Generation  int64
	DedupToken  string
	Env         *Env
}

// Outcome carries everything a reaction may produce. A handler fills only the parts its
// reaction declared in Emits; the core applies exactly those. Status is
// last-writer-wins; Conditions accumulate.
type Outcome struct {
	Status     json.RawMessage
	Conditions []Condition
	Children   []ChildSpec          // composer: desired children
	Edges      []DepEdge            // composer: dep edges (+ value flows)
	Configs    []ProviderConfigSpec // composer: provider configs it owns

	OperationOutput json.RawMessage // operate: verb output
	SideEffectDone  bool            // lifecycle: the side effect completed (informational)
}

// ─────────────────────────────────────────────────────────────────────────
// EffectiveConfig / EffectiveBundle — merge the kind default with a per-resource
// override.
// ─────────────────────────────────────────────────────────────────────────

// EffectiveConfig computes a task's effective config of type T: it overlays the
// per-resource override on the kind default via a deep JSON merge (override wins per
// key), then decodes into T. defaultSpec is the provider's last OnConfig spec; override
// is req.Env.ProviderConfig.
func EffectiveConfig[T any](defaultSpec, override json.RawMessage) T {
	merged := mergeConfigDoc(defaultSpec, override)
	var cfg T
	if len(merged) > 0 {
		_ = json.Unmarshal(merged, &cfg)
	}
	return cfg
}

// EffectiveBundle resolves a task's effective opaque bundle: the per-resource override
// when non-empty, else the kind default. A whole-artifact REPLACE (opaque bytes can't
// deep-merge). The returned slice is one of the inputs verbatim; treat it as read-only.
func EffectiveBundle(defaultData, override []byte) []byte {
	if len(override) > 0 {
		return override
	}
	return defaultData
}

// mergeConfigDoc overlays a per-resource override on the kind default config document.
// The default is the base; the override wins per top-level key and nested objects are
// deep-merged. An empty override returns the default verbatim (the hot path); a malformed
// override falls back to the default (never wipes config).
func mergeConfigDoc(base, override json.RawMessage) json.RawMessage {
	if len(override) == 0 {
		return base
	}
	if len(base) == 0 {
		return override
	}
	var baseMap, overMap map[string]any
	if err := json.Unmarshal(base, &baseMap); err != nil || baseMap == nil {
		return override
	}
	if err := json.Unmarshal(override, &overMap); err != nil || overMap == nil {
		return base
	}
	if err := mergo.Merge(&baseMap, overMap, mergo.WithOverride); err != nil {
		return base
	}
	merged, err := json.Marshal(baseMap)
	if err != nil {
		return base
	}
	return merged
}

// ─────────────────────────────────────────────────────────────────────────
// ReactionHandler / ReactionRegistry / KindRuntime — worker-side dispatch plumbing.
// The SDK builds a registry from a provider's handler and looks up by
// (kind, kindVersion, reaction). Purely worker-local.
// ─────────────────────────────────────────────────────────────────────────

// ReactionHandler runs one reaction — the single seam every handler implements. A
// returned error fails the reaction (wrap with Terminal to stop retries).
type ReactionHandler interface {
	React(ctx context.Context, req ReactionRequest) (Outcome, error)
}

// runReactionStages runs a sequence of handlers for one reaction, threading Status
// (last-writer-wins) and accumulating Conditions/Children/Edges/Configs. A single
// handler (the common case) is just len==1. On error it returns the partial outcome
// plus the failing handler index. Worker-local: the runner (per streamed task) and the
// in-process ExecuteTask seam both drive it through computeComplete; a provider author
// implements Provider.Work, never this.
func runReactionStages(ctx context.Context, handlers []ReactionHandler, req ReactionRequest) (Outcome, int, error) {
	out := Outcome{Status: req.Status}
	for i, h := range handlers {
		req.Status = out.Status
		resp, err := h.React(ctx, req)
		if err != nil {
			return out, i, err
		}
		if len(resp.Status) > 0 {
			out.Status = resp.Status
		}
		out.Conditions = append(out.Conditions, resp.Conditions...)
		if len(resp.Children) > 0 {
			out.Children = resp.Children
		}
		if len(resp.Edges) > 0 {
			out.Edges = resp.Edges
		}
		if len(resp.Configs) > 0 {
			out.Configs = resp.Configs
		}
		if len(resp.OperationOutput) > 0 {
			out.OperationOutput = resp.OperationOutput
		}
		if resp.SideEffectDone {
			out.SideEffectDone = true
		}
	}
	return out, 0, nil
}

// reactionRegistry maps (kind, kindVersion, reactionName) → handler chain, worker-side.
// The kind version is part of the key so one worker binary can serve several web-API
// versions of the same kind, each with its own handlers. Purely worker-local.
type reactionRegistry struct {
	handlers map[reactionKey][]ReactionHandler
}

type reactionKey struct {
	kind        string
	kindVersion int
	reaction    string
}

// reactionWildcard is the reaction name a handler registers under to serve EVERY reaction
// the core dispatches for a (kind, kindVersion) — the catch-all Lookup falls back to. The
// SDK's Provider registers its Work here (the manifest is operator-applied, so the
// provider need not enumerate reaction names; it switches on req.Reaction if it serves
// several). Cannot collide with a real reaction name.
const reactionWildcard = "*"

// newReactionRegistry constructs an empty registry.
func newReactionRegistry() *reactionRegistry {
	return &reactionRegistry{handlers: map[reactionKey][]ReactionHandler{}}
}

// register binds a handler chain to (kind, kindVersion, reactionName). kindVersion MUST
// be explicit (>= 1). Pass reactionWildcard to serve every reaction for the pair.
func (r *reactionRegistry) register(kind Kind, kindVersion int, reaction string, handlers ...ReactionHandler) {
	r.handlers[reactionKey{kind: string(kind), kindVersion: kindVersion, reaction: reaction}] = handlers
}

// lookup returns the handler chain for (kind, kindVersion, reactionName). It tries the
// EXACT reaction first, then the (kind, kindVersion, reactionWildcard) catch-all.
// kindVersion is matched EXACTLY (no 0→1 normalization).
func (r *reactionRegistry) lookup(kind Kind, kindVersion int, reaction string) ([]ReactionHandler, bool) {
	if h, ok := r.handlers[reactionKey{kind: string(kind), kindVersion: kindVersion, reaction: reaction}]; ok {
		return h, true
	}
	h, ok := r.handlers[reactionKey{kind: string(kind), kindVersion: kindVersion, reaction: reactionWildcard}]
	return h, ok
}

// kindRuntime is the worker-side record for ONE (kind, kindVersion): its handlers keyed
// by reaction name. The SDK builds one per registered Provider and registers its handlers
// into the reactionRegistry. Unexported — an implementation detail of the worker loop.
type kindRuntime struct {
	Kind        Kind
	KindVersion int
	Handlers    map[string]ReactionHandler
}

// registerInto adds this kind's handlers to a worker registry.
func (k kindRuntime) registerInto(reg *reactionRegistry) {
	for name, h := range k.Handlers {
		reg.register(k.Kind, k.KindVersion, name, h)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Wire DTOs — the per-stage request/response shapes convert.go decodes/encodes off the
// proto. PLAIN DATA (no behaviour). Unexported: a provider implements ReactionHandler,
// never these.
// ─────────────────────────────────────────────────────────────────────────

type composeRequest struct {
	Root     Resource
	Observed []Resource
	Desired  []ChildSpec
	Edges    []DepEdge
	Configs  []ProviderConfigSpec
	Env      *Env
}

type workRequest struct {
	Resource Resource
	Status   json.RawMessage
	Env      *Env
}

type rollupRequest struct {
	Root        Resource
	Descendants []Resource
	Status      json.RawMessage
	Env         *Env
}

type deleteRequest struct {
	Resource Resource
	Env      *Env
}

type operateRequest struct {
	Resource  Resource
	Operation Operation
	Env       *Env
}

type reactRequest struct {
	Resource   Resource
	Transition Transition
	DedupToken string
	Env        *Env
}

// composePipelineResult is the compose wire output: desired children/edges/configs +
// status/conditions (and an error with the failing stage index).
type composePipelineResult struct {
	Desired     []ChildSpec
	Edges       []DepEdge
	Configs     []ProviderConfigSpec
	Status      json.RawMessage
	Conditions  []Condition
	FailedStage int
	Err         error
}
