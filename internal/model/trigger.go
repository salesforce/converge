package model

// trigger.go holds the two author-facing lifecycle enums a reaction handler
// reads off its ReactionRequest: the Trigger that fired it and (for a reactor) the
// Transition edge it is reacting to. They are part of the shared wire contract — the
// core stamps them onto the request, the worker reads them. The control-plane
// vocabulary that pairs with them (the OutcomeMask a manifest declares, the
// KindManifest/ReactionDecl a manifest is built from, the validation lattice) lives
// in manifest.go; a worker author never declares any of it.

// Trigger is the condition that fires a reaction — the FIRST half of a
// reaction's identity; the SECOND half is the Emits mask (OutcomeMask).
// The core matches a claimed task / lifecycle delivery to the reaction(s) whose Trigger
// applies, ships the reaction, and applies the Outcome ∩ Emits.
type Trigger string

const (
	// TriggerSpecChange fires when generation bumps (a spec edit / first apply).
	// Emits=Children → compose; Emits=Status → work. A kind can declare both.
	TriggerSpecChange Trigger = "specChange"
	// TriggerChildrenSettled fires when a root is re-pended after its whole
	// subtree settled (the cascade edge). Emits=Status → rollup. The ONLY trigger
	// for which the core loads the descendant subtree.
	TriggerChildrenSettled Trigger = "childrenSettled"
	// TriggerDeleteRequested fires when a finalizer-bearing resource is being torn
	// down. Emits=Finalizer → the core strips the finalizer string on success.
	TriggerDeleteRequested Trigger = "deleteRequested"
	// TriggerOperation fires when a resource_operations row is enqueued for a Verb.
	// Emits=OperationOutput → the core writes the verb output.
	TriggerOperation Trigger = "operation"
	// TriggerReactor marks a REACTOR reaction: a pure side effect that a
	// reactor_bindings SUBSCRIPTION delivers when a WATCHED kind crosses a
	// transition (created|synced|degraded|failed|deleted). Emits=SideEffect only.
	// The reactor CRD declares just this reaction (name + emits) — NO transition,
	// kind, or label; those live on the binding. At delivery the transition that
	// fired is passed to the handler as DATA (ReactionRequest.Transition), not
	// baked into the reaction's identity. The core acks the lifecycle_outbox row
	// on success (at-least-once).
	TriggerReactor Trigger = "reactor"
	// TriggerResync fires on a drift tick (no generation bump). Re-runs the
	// SpecChange-class reactions to re-observe health; not a distinct handler.
	TriggerResync Trigger = "resync"
)

// Transition is a resource lifecycle edge a reactor_bindings subscription may
// watch — the set a `reactor` reaction fires on. It is delivered to the reactor
// handler as data (ReactionRequest.Transition). The constants are the ONLY legal
// values; they mirror the DB CHECK on reactor_bindings.transition /
// lifecycle_outbox.transition (both must agree), and the huma enum tag on the
// reactor-binding apply body validates an incoming value against this same set at
// the API edge. Use these — never a string literal — so a typo is a compile error,
// not a binding that silently never fires.
type Transition string

const (
	// TransitionCreated fires when a resource is first applied (from the apply tx).
	TransitionCreated Transition = "created"
	// TransitionSynced fires when a resource reaches synced_gen >= generation (the
	// durable "rolled up / reconciled" edge — the cascade computes it).
	TransitionSynced Transition = "synced"
	// TransitionDegraded fires when a resource's health flips true→false.
	TransitionDegraded Transition = "degraded"
	// TransitionFailed fires when a resource newly hard-fails (failure_gen == generation).
	TransitionFailed Transition = "failed"
	// TransitionDeleted fires when a resource is torn down.
	TransitionDeleted Transition = "deleted"
)
