package model

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
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

// Standard condition types. Providers may also emit their own custom
// axes (e.g. "Bound", "Healthy"); only TypeReady feeds the health_ok
// scalar that gates is_ready.
const (
	// TypeReady is the health/availability axis (Crossplane "Ready").
	// A reaction that returns TypeReady=False sets the resource's
	// health_ok=false, demoting is_ready even when fully synced. A
	// reaction that emits no Ready condition is treated as healthy
	// (health_ok stays true) — the common case.
	TypeReady = "Ready"
	// TypeSynced is the spec-reconciled axis (Crossplane "Synced"). It
	// is DERIVED from synced_gen vs generation and synthesized by the
	// API; providers do not normally emit it. The core writes a
	// Synced=False row on reconcile failure so the failure reason is
	// queryable as current truth.
	TypeSynced = "Synced"
)

// Condition is one axis of a resource's status — K8s/Crossplane shape.
// Returned by a reaction in its Outcome's Conditions slice; the core
// upserts them into resource_conditions on transition and, for
// Type==TypeReady, folds Status into the health_ok scalar.
type Condition struct {
	Type    string          // "Ready", "Synced", or a custom axis
	Status  ConditionStatus // True | False | Unknown
	Reason  string          // CamelCase machine code, e.g. "Available"
	Message string          // human-readable detail
}

// ─────────────────────────────────────────────────────────────────────────
// Resource — the provider-side view of a row from the resources table.
// ─────────────────────────────────────────────────────────────────────────

// Resource is what a reaction handler receives. JSON columns are
// json.RawMessage so handlers can re-marshal without conversion.
//
// State is observed via IsReady (GENERATED from synced_gen >= generation
// AND health_ok AND no pending deletion) — the Synced and Ready axes
// folded into one queryable flag. There is no conditions array on the row and
// no separate observed_generation; synced_gen carries the synced axis (plus
// composed_gen for the compose-skip gate).
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
	// FrozenUntil and Quarantined are the two faces of the single resources
	// frozen_until column (orphaned = a finite future instant; quarantined =
	// 'infinity'), split here so providers read each unambiguously:
	//
	//   FrozenUntil — non-nil means the composer dropped this child and it is in
	//   the orphan-grace window (phase=Orphaned), awaiting a re-emit (which clears
	//   it) or the reaper's sweep. It is the finite teardown deadline.
	//
	//   Quarantined — true when an operator set this failed resource ASIDE
	//   (phase=Quarantined, frozen_until='infinity').
	//
	// A rollup MUST treat either as "ignore this child" (use IsFrozen): an
	// orphaned child has left the composition, and a quarantined child is
	// deliberately excluded so one bad child can't block the root.
	FrozenUntil *time.Time
	Quarantined bool
	Labels      map[string]string
}

// IsFrozen reports whether the resource is set aside from scheduling and the
// rollup — orphaned (in the grace window) OR quarantined. Both faces of the
// resources frozen_until column; a rollup must skip a frozen child.
func (r Resource) IsFrozen() bool { return r.FrozenUntil != nil || r.Quarantined }

// Operation is the provider-side view of a resource_operations row,
// passed to an operation reaction's handler.
type Operation struct {
	// No ID field: the resource_operations row id is a pure internal DB key the
	// broker uses to correlate the result; a provider never needs it. ResourceID
	// (the resource's public uuid) identifies what the verb runs against.
	ResourceID  uuid.UUID
	Verb        string
	Input       json.RawMessage
	Attempts    int
	RequestedBy string
}

// ─────────────────────────────────────────────────────────────────────────
// Env — per-call environment a reaction handler receives.
// ─────────────────────────────────────────────────────────────────────────

// Env is intentionally narrow: a handler describes what it wants done
// (children, status, output) and lets the core persist it. Handlers do not —
// and must not — issue raw SQL. Logger is scoped slog with task-specific fields
// (kind, resource id, generation).
//
// ProviderConfig is the per-task CUSTOM config override: the clone of the
// resource's referenced providerconfig (work_queue.provider_config), or nil
// when the resource carries no custom override. It is pure read-only data
// (not a DB handle), so it fits Env's "per-call environment" role without
// widening what a handler can DO. Every handler of a task sees the same value.
// The kind DEFAULT config reaches the provider through the SDK (its OnConfig
// hook, primed at startup); a handler computes its EFFECTIVE config by overlaying
// this override onto that default (an object merge — the custom override wins per
// field, the default fills the rest) via the SDK's converge.EffectiveConfig. Spec is
// a separate axis and never participates.
type Env struct {
	Logger         *slog.Logger
	ProviderConfig json.RawMessage
	// ProviderBundle is the per-resource CUSTOM bundle override (the clone of the
	// referenced custom config's `data` — the opaque artifact, e.g. a zip of
	// Starlark .star files), or nil when the resource carries no custom bundle.
	// Unlike ProviderConfig it does NOT field-merge: a handler computes its
	// EFFECTIVE bundle by taking this override whole when non-empty, else the kind
	// DEFAULT bundle (from the SDK's OnConfig/startup prime) — opaque bytes can't
	// deep-merge. Use the SDK's converge.EffectiveBundle helper.
	ProviderBundle []byte
}
