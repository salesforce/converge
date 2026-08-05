package model

import (
	"encoding/json"
	"fmt"
	"slices"
)

// KindManifest is the CONTROL-PLANE half of a kind's definition: the CRD-style
// document an operator applies (its schemas, declared reactions, and lifecycle policy),
// validated by the legality lattice below and dispatched off by the outcome-mask
// vocabulary. A worker author never declares schemas, reactions, or policy — the
// manifest is applied out of band into the kind_manifest table, so none of this is on
// the public SDK surface.
//
// The KindManifest IS the kind definition: the operator applies it as a CRD into the
// kind_manifest table and that row is the sole source of a kind's schemas, reactions, and
// policy. Its reactions are a single list of ReactionDecl whose (Trigger, Emits) pair
// encodes each reaction's behaviour.
//
// KindManifest is the serializable CRD definition of a kind. It round-trips to
// the kind_manifest DB row (schemas as JSONB, reactions as JSONB) and to the
// API/UI manifest editor. JSON tags match the DB column names and the wire
// contract.
type KindManifest struct {
	Kind Kind `json:"kind"`
	// KindVersion is the web-API-style user-facing version (vpc/v1, vpc/v2, … — no
	// minor/micro; 1–32767, the smallint ceiling of the resources/work_queue
	// columns it is denormalized onto). A manifest is published PER (kind,
	// kindVersion); the core routes work by (kind, kindVersion) exact equality. The
	// kind string stays bare `vpc`; the kindVersion is this orthogonal field, never
	// encoded into Kind. REQUIRED and explicit (>= 1): there is no implicit v1
	// default — an un-versioned manifest is rejected at publish, never silently
	// treated as v1 (no `omitempty`, so it always serializes).
	KindVersion int    `json:"kind_version"`
	Description string `json:"description,omitempty"`

	// JSON Schemas (RFC draft 2020-12) authored for the kind, so the runtime stores
	// schema bytes, never a reflect.Type. A nil schema means "untyped" — the kind
	// opts out of validation for that axis.
	SpecSchema   json.RawMessage `json:"spec_schema,omitempty"`
	StatusSchema json.RawMessage `json:"status_schema,omitempty"`
	ConfigSchema json.RawMessage `json:"config_schema,omitempty"`

	// Reactions is the declared set of (Trigger, Emits) reactions. The core
	// dispatches purely off these (it never reads a kind name, only the masks).
	Reactions []ReactionDecl `json:"reactions"`

	// FinalizerName, when set, makes deletes a two-phase teardown (the reaper
	// soft-deletes and waits for the DeleteRequested reaction to strip it). Empty
	// = immediate hard-delete.
	FinalizerName string `json:"finalizer_name,omitempty"`

	// Operational policy (today's kind_config knobs). Seeded insert-if-absent on
	// apply so an operator's live /api/kinds/{kind}/config edits are never
	// clobbered by a re-apply.
	MaxInflight        int  `json:"max_inflight,omitempty"`
	TaskDeadlineSecs   int  `json:"task_deadline_secs,omitempty"`
	ResyncIntervalSecs int  `json:"resync_interval_secs,omitempty"`
	ResyncRecomposes   bool `json:"resync_recomposes,omitempty"`
	OrphanGraceSecs    int  `json:"orphan_grace_secs,omitempty"`
	// MaxTransientAttempts caps consecutive TRANSIENT reconcile failures before the
	// failure is escalated to terminal (dead-lettered), so a persistently-broken
	// handler stops retrying forever. 0 (the default) = unbounded — set it to opt a
	// kind into the poison-pill.
	MaxTransientAttempts int `json:"max_transient_attempts,omitempty"`

	// Retired SUNSETS this web-API version: a publish declaring it true (or an
	// operator toggling it via /api/kinds/{kind}/config) freezes NEW creates/flips
	// onto (kind, KindVersion) while EXISTING resources keep reconciling so they can
	// drain or be migrated off — never a forced teardown. Seeded from the manifest
	// but operator-editable at runtime, so it is preserved across a re-apply (the
	// sync trigger never un-retires). Off the claim hot path.
	Retired bool `json:"retired,omitempty"`
}

// OutcomeMask is the set of Outcome parts the core will APPLY for a reaction —
// the SECOND half of a reaction's identity. A reaction author declares it so the
// core knows which fields of the returned Outcome are meaningful (and validates
// legality, e.g. only a Children-emitting reaction can mutate the graph). It
// serializes as a JSON array of OutcomeBit strings.
type OutcomeMask []OutcomeBit

// OutcomeBit names one applicable Outcome part.
type OutcomeBit string

const (
	OutcomeChildren        OutcomeBit = "children"        // ApplyComposeResult (graph upsert/prune)
	OutcomeEdges           OutcomeBit = "edges"           // dep edges (with value-flow)
	OutcomeConfigs         OutcomeBit = "configs"         // provider configs the composer owns
	OutcomeStatus          OutcomeBit = "status"          // resources.status
	OutcomeConditions      OutcomeBit = "conditions"      // resource_conditions / health_ok
	OutcomeFinalizer       OutcomeBit = "finalizer"       // strip the finalizer (delete path)
	OutcomeOperationOutput OutcomeBit = "operationOutput" // resource_operations.output
	OutcomeSideEffect      OutcomeBit = "sideEffect"      // external side effect only; ack the delivery
)

// Has reports whether the mask includes bit.
func (m OutcomeMask) Has(bit OutcomeBit) bool {
	return slices.Contains(m, bit)
}

// ReactionDecl is one declared reaction. name is the stable handle the worker
// keys its handler by (and the lifecycle_outbox binding name).
// trigger + emits drive the core's dispatch and apply. The optional fields
// refine specific triggers. Trigger is the author-facing Trigger enum;
// Emits is the control-plane OutcomeMask.
type ReactionDecl struct {
	Name    string      `json:"name"`
	Trigger Trigger     `json:"trigger"`
	Emits   OutcomeMask `json:"emits"`

	// Verb refines TriggerOperation: the subresource verb this reaction handles.
	Verb string `json:"verb,omitempty"`
	// Finalizer refines TriggerDeleteRequested: the finalizer string this teardown
	// strips (defaults to the manifest's FinalizerName when empty).
	Finalizer string `json:"finalizer,omitempty"`
	// NOTE: a reactor reaction (TriggerReactor) carries NO transition, kind, or
	// label — those live on the reactor_bindings subscription, not the CRD. The
	// transition that fired reaches the handler at delivery as
	// ReactionRequest.Transition (data), never as part of the declaration.
}

// ValidateManifest enforces the CLOSED legality lattice on a manifest's
// reactions — the SDK half of the DB validate_kind_manifest() CHECK; both must
// agree. The reaction rules reject a mis-declared manifest at AUTHORING/APPLY
// time, before a task ever runs against bad data. Returns a descriptive error on
// the first violation, nil if legal.
func ValidateManifest(m KindManifest) error {
	if m.Kind == "" {
		return fmt.Errorf("manifest: kind is required")
	}
	// KindVersion is REQUIRED and explicit (>= 1); 0/unset is rejected here (never
	// normalized to v1), matching the DB CHECK (kind_version >= 1) and the store's
	// own guard. Keeping this in lockstep is what lets ValidateManifest be the
	// authoring/apply-time gate an OSS provider author trusts — a 0 must fail here,
	// not survive to a raw SQL constraint error deep in the write path.
	if m.KindVersion < 1 {
		return fmt.Errorf("manifest %q: kindVersion is required and must be >= 1 (no implicit v1 default; got %d)", m.Kind, m.KindVersion)
	}
	nChildren, nRollup, nReactor := 0, 0, 0
	for _, rx := range m.Reactions {
		switch rx.Trigger {
		case TriggerSpecChange, TriggerChildrenSettled, TriggerDeleteRequested,
			TriggerOperation, TriggerReactor, TriggerResync:
		case "":
			return fmt.Errorf("reaction %q: missing trigger", rx.Name)
		default:
			return fmt.Errorf("reaction %q: unknown trigger %q", rx.Name, rx.Trigger)
		}
		hasChildren := rx.Emits.Has(OutcomeChildren)
		hasStatus := rx.Emits.Has(OutcomeStatus) || rx.Emits.Has(OutcomeConditions)
		hasSide := rx.Emits.Has(OutcomeSideEffect)

		if rx.Trigger == TriggerSpecChange && hasChildren {
			nChildren++
			if !hasStatus {
				return fmt.Errorf("reaction %q: a specChange reaction emitting children must also emit status/conditions", rx.Name)
			}
			if hasSide {
				return fmt.Errorf("reaction %q: a children-emitting reaction may not also emit sideEffect", rx.Name)
			}
		}
		if rx.Trigger == TriggerChildrenSettled && hasStatus {
			nRollup++
		}
		if rx.Trigger == TriggerDeleteRequested && rx.Finalizer == "" && m.FinalizerName == "" {
			return fmt.Errorf("reaction %q: deleteRequested requires a finalizer (on the reaction or the manifest)", rx.Name)
		}
		if rx.Trigger == TriggerOperation && rx.Verb == "" {
			return fmt.Errorf("reaction %q: operation requires a verb", rx.Name)
		}
		// A reactor reaction is a pure side effect delivered via a binding: it must
		// emit sideEffect and NOTHING that mutates the resource (it acts on OTHER
		// kinds, not itself). It declares no transition — the binding does.
		if rx.Trigger == TriggerReactor {
			nReactor++
			if !hasSide {
				return fmt.Errorf("reaction %q: a reactor reaction must emit sideEffect", rx.Name)
			}
			if hasChildren || hasStatus {
				return fmt.Errorf("reaction %q: a reactor reaction may only emit sideEffect", rx.Name)
			}
		}
	}
	if nChildren > 1 {
		return fmt.Errorf("at most one specChange reaction may emit children (got %d)", nChildren)
	}
	if nRollup > 1 {
		return fmt.Errorf("at most one childrenSettled reaction may emit status (got %d)", nRollup)
	}
	// A reactor kind declares EXACTLY ONE reactor reaction: the claim resolves the
	// reaction to run from the reactor's CRD (there's no per-binding reaction
	// name), so two would make the resolution ambiguous. Multiple reactors on the
	// same event is expressed with multiple KINDS + multiple bindings, not multiple
	// reactions on one kind.
	if nReactor > 1 {
		return fmt.Errorf("at most one reactor reaction per kind (got %d)", nReactor)
	}
	return nil
}

// ComposerReaction returns the (single, by the lattice) reaction that emits
// children on a spec change, and whether one exists. The core uses this to know
// a kind is a composer without reading its name.
func (m KindManifest) ComposerReaction() (ReactionDecl, bool) {
	for _, rx := range m.Reactions {
		if rx.Trigger == TriggerSpecChange && rx.Emits.Has(OutcomeChildren) {
			return rx, true
		}
	}
	return ReactionDecl{}, false
}

// RollupReaction returns the (single, by the lattice) childrenSettled+status
// reaction, and whether one exists — i.e. whether this kind is a rollup-er.
func (m KindManifest) RollupReaction() (ReactionDecl, bool) {
	for _, rx := range m.Reactions {
		if rx.Trigger == TriggerChildrenSettled &&
			(rx.Emits.Has(OutcomeStatus) || rx.Emits.Has(OutcomeConditions)) {
			return rx, true
		}
	}
	return ReactionDecl{}, false
}

// WorkerReaction returns the specChange reaction that emits status WITHOUT
// children (a plain leaf worker), and whether one exists.
func (m KindManifest) WorkerReaction() (ReactionDecl, bool) {
	for _, rx := range m.Reactions {
		if rx.Trigger == TriggerSpecChange &&
			!rx.Emits.Has(OutcomeChildren) &&
			(rx.Emits.Has(OutcomeStatus) || rx.Emits.Has(OutcomeConditions)) {
			return rx, true
		}
	}
	return ReactionDecl{}, false
}

// DeleteReaction returns the deleteRequested (finalizer teardown) reaction, and
// whether one exists — i.e. whether this kind runs a teardown on delete.
func (m KindManifest) DeleteReaction() (ReactionDecl, bool) {
	for _, rx := range m.Reactions {
		if rx.Trigger == TriggerDeleteRequested {
			return rx, true
		}
	}
	return ReactionDecl{}, false
}

// OperationReaction returns the operation reaction bound to verb, and whether one
// exists — i.e. whether this kind exposes that subresource verb.
func (m KindManifest) OperationReaction(verb string) (ReactionDecl, bool) {
	for _, rx := range m.Reactions {
		if rx.Trigger == TriggerOperation && rx.Verb == verb {
			return rx, true
		}
	}
	return ReactionDecl{}, false
}

// ReactorReaction returns the (single) reactor reaction and whether one exists.
// A reactor CRD declares exactly one TriggerReactor reaction; its name is the
// handle the worker keys its handler by (the reaction the dispatcher
// ships when a binding to this reactor fires).
func (m KindManifest) ReactorReaction() (ReactionDecl, bool) {
	for _, rx := range m.Reactions {
		if rx.Trigger == TriggerReactor {
			return rx, true
		}
	}
	return ReactionDecl{}, false
}

// IsReactor reports whether this kind is a REACTOR — a side-effect handler a
// binding SUBSCRIBES to other kinds' transitions (e.g. a status-upload sink),
// not a directly-creatable resource. A reactor declares ONLY reactor reactions:
// at least one TriggerReactor reaction and no specChange/childrenSettled/
// deleteRequested/operation reaction. The API uses it to keep reactors out of
// the creatable-resource gate while still serving their config schema.
func (m KindManifest) IsReactor() bool {
	hasReactor := false
	for _, rx := range m.Reactions {
		if rx.Trigger == TriggerReactor {
			hasReactor = true
			continue
		}
		// Any non-reactor reaction means this is a work kind, not a reactor.
		return false
	}
	return hasReactor
}
