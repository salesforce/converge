package model

// ─────────────────────────────────────────────────────────────────────────
// ChildSpec / DepEdge / ValueFlow — what a composer reaction emits.
// ─────────────────────────────────────────────────────────────────────────

// ChildSpec describes one child a composer wants to exist under a
// parent. Spec is the typed Go value (e.g. AccountSpec); the core
// JSON-marshals at the DB boundary.
type ChildSpec struct {
	Kind Kind
	// KindVersion is the web-API version the child is emitted at. A versioned composer
	// (classicbom/v2) emits its children at the new kindVersion; because the (kind,
	// name) address is stable, ApplyComposeResult ADOPTS the existing child BY ID
	// and flips its kindVersion in place — a safe in-place upgrade, never a
	// delete-and-recreate of live infra. REQUIRED and explicit (>= 1): there is no
	// implicit v1 default — a composer MUST emit each child with the version it
	// wants (a 0 fails the DB NOT-NULL/CHECK at ApplyComposeResult, never silently v1).
	KindVersion int
	Name        string
	Spec        any
	Labels      map[string]string
}

// ProviderConfigSpec is one provider config a composer emits, alongside
// its children and edges. The handler chain threads accumulated configs
// (like Children/Edges) and ApplyComposeResult diffs them against the
// configs this root already OWNS — upserting the changed/new delta and
// pruning what vanished, all owned by the root so they GC with it (a
// composer's emitted configs never outlive the composer root). Spec is the
// config document; its shape is the consumer kind's manifest config_schema.
//
//   - IsDefault=true  → the consumer kind's single live-reconfigurable
//     DEFAULT (editing it pushes to every worker; one per kind globally,
//     enforced by uq_providerconfigs_default_per_kind).
//   - IsDefault=false → a named CUSTOM config a resource references by Name
//     via provider_config_ref.
//
// Names are a GLOBAL handle, so a composer must name its configs uniquely
// (e.g. prefixed by the root) exactly like children — a name already owned
// by a different root is left untouched (the owner-guarded upsert no-ops).
type ProviderConfigSpec struct {
	Name string
	Kind Kind
	// KindVersion is the web-API version of the consumer Kind this config is for. A
	// composer configuring a v2 child emits its config at KindVersion 2 so the config's
	// (kind, kindVersion) matches the child it attaches to (the resource-apply guard
	// enforces the match). REQUIRED and explicit (>= 1): there is no implicit v1
	// default — a composer MUST set the version its config is for.
	KindVersion int
	IsDefault   bool
	Spec        any
}

// DepEdge: dependent depends on dependency. Persisted to resource_deps.
// An edge optionally carries value flows that fill the dependent's
// spec from the upstream's status when the upstream becomes ready.
type DepEdge struct {
	From   ResourceRef // dependent
	To     ResourceRef // dependency
	Values []ValueFlow
}

// ValueFlow declares one field-level flow on a DepEdge: the
// dependent's spec[DependentField] is filled from the upstream's
// status[SourceField] when the upstream's status becomes available.
//
// Both fields are RFC 6901 JSON Pointers ("/account_id"); a bare key
// without a leading slash is treated as a single-segment pointer. The flow is
// applied as a JSON-pointer COPY (status[SourceField] → spec[DependentField]),
// guarded by present-and-non-null — an absent source field simply does not fire
// (no error), and the copied value is trusted as-is (it is the upstream's own
// status). The fields are NOT individually schema-checked; the guarantee is the
// DEPENDENT's own spec_schema, which the store validates when the composer emits
// the child (with DependentField relaxed, since the flow fills it later). So a
// flow into a kind with a closed spec_schema is bounded by that schema (minus the
// deferred field); a flow into an opaque (additionalProperties:true) kind is
// trusted by design.
type ValueFlow struct {
	DependentField string
	SourceField    string
}
