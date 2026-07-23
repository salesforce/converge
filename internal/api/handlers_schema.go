package api

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// handlers_schema.go: the read + write endpoints that surface a kind's schema
// and OPERATIONAL config to the UI — GET /api/kinds (list), GET
// /api/kinds/{kind}/schema (one kind's shapes + docs refs + default config), and
// POST /api/kinds/{kind}/config (edit the kind_config cap/resync knobs). The
// schema cache + validators live in schema_cache.go; OpenAPI naming in
// schema_openapi.go.

// ── /api/kinds and /api/kinds/{kind}/schema ────────────────────────────

type kindSummary struct {
	Kind        string `json:"kind"`
	Description string `json:"description,omitempty"`
	// KindVersions is the ascending list of published web-API versions of this kind
	// (e.g. [1, 2]). The UI renders a version chip per kindVersion and builds per-kindVersion
	// /docs schema links + a per-kindVersion config view from it. Always ≥ [1].
	KindVersions []int `json:"kind_versions,omitempty"`
	HasSpec      bool  `json:"has_spec"`
	HasStatus    bool  `json:"has_status"`
	HasConfig    bool  `json:"has_config"`
	// IsReactor marks a REACTOR kind: a side-effect handler a reactor_bindings
	// subscription runs on other kinds' transitions, NOT a creatable resource.
	// The UI lists these without operational config or a create action; their
	// wiring lives entirely in editable bindings.
	IsReactor bool `json:"is_reactor,omitempty"`
}

// kindListEntry is one row of the /api/kinds list: the kind's summary plus its
// OPERATIONAL config (cap + resync) so the UI's Kinds page renders the whole
// table in ONE request instead of N per-kind fetches. Operational is nil when
// the kind has no kind_config row (uncapped, no resync).
type kindListEntry struct {
	kindSummary
	Operational *kindOperational `json:"operational,omitempty"`
}

type listKindsOutput struct {
	Body struct {
		Kinds []kindListEntry `json:"kinds"`
	}
}

func (s *Server) listKindSchemas(ctx context.Context, _ *struct{}) (*listKindsOutput, error) {
	// DECLARED schemas: every kind with an applied manifest — a kind's shape is
	// documented from its manifest, not from whether its provider is up yet.
	all := s.declaredSchemas()

	// One read of all kind_config rows → index by kind, so the per-kind loop adds
	// the operational block without N queries. A read error is non-fatal: the
	// schema list is the primary payload, so we degrade to "no operational info".
	ops := map[model.Kind]kindOperational{}
	if s.kindConfigs != nil {
		if rows, err := s.kindConfigs.ListKindConfig(ctx); err == nil {
			for _, kc := range rows {
				ops[kc.Kind] = kindOperational{
					MaxInflight:           kc.MaxInflight,
					TaskDeadlineSeconds:   int(kc.TaskDeadline.Seconds()),
					ResyncIntervalSeconds: int(kc.ResyncInterval.Seconds()),
					ResyncRecomposes:      kc.ResyncRecomposes,
					OrphanGraceSeconds:    int(kc.OrphanGracePeriod.Seconds()),
					MaxTransientAttempts:  kc.MaxTransientAttempts,
					Retired:               kc.Retired,
				}
			}
		}
	}

	reactors := s.reactorSchemasList()

	// One list ENTRY per kind (not per kindVersion): the entry summarises the kind's
	// highest-kindVersion shape and its KindVersions chip lists every published version, so the
	// UI links per-kindVersion /docs schemas from a single row. `all` carries one manifest
	// per (kind, version), so collapse to the highest kindVersion as the representative.
	rep := make(map[model.Kind]model.KindManifest, len(all))
	for _, ks := range all {
		if prev, seen := rep[ks.Kind]; !seen || ks.KindVersion >= prev.KindVersion {
			rep[ks.Kind] = ks
		}
	}
	out := &listKindsOutput{}
	out.Body.Kinds = make([]kindListEntry, 0, len(rep)+len(reactors))
	for _, ks := range rep {
		e := kindListEntry{kindSummary: summariseKind(ks, false)}
		e.KindVersions = s.kindVersionsForKind(ks.Kind)
		if op, ok := ops[ks.Kind]; ok {
			opCopy := op
			e.Operational = &opCopy
		}
		out.Body.Kinds = append(out.Body.Kinds, e)
	}
	// Reactor kinds are listed read-only: no kind_config (uncapped, no resync),
	// so no Operational block — only their shape, tagged is_reactor. Like work
	// kinds, chip EVERY published version (reactorSchemasList is one representative
	// per reactor kind; KindVersions comes from the reactor side-map) so a reactor
	// published at v1+v2 shows both chips, not a single collapsed row.
	for _, ks := range reactors {
		e := kindListEntry{kindSummary: summariseKind(ks, true)}
		e.KindVersions = s.kindVersionsForKind(ks.Kind)
		out.Body.Kinds = append(out.Body.Kinds, e)
	}
	sort.Slice(out.Body.Kinds, func(i, j int) bool {
		return out.Body.Kinds[i].Kind < out.Body.Kinds[j].Kind
	})
	return out, nil
}

type kindSchemaInput struct {
	Kind        string `path:"kind" doc:"Resource kind."`
	KindVersion int    `query:"kind_version" doc:"Web-API version (v1, v2, …) whose /docs schema refs to resolve; omitted resolves the LOWEST published version, an over-large value clamps to the highest. A /docs display convenience only (no data is written), so it has no v1 default."`
}

// kindOperational is the per-kind OPERATIONAL config (kind_config row) the UI
// shows alongside the schema — the platform tuning knobs, distinct from the
// kind's config DOCUMENT (providerconfigs). MaxInflight 0 = uncapped;
// ResyncIntervalSeconds 0 = no drift resync. These are runtime-editable.
type kindOperational struct {
	MaxInflight           int  `json:"max_inflight" doc:"Global in-flight cap across all pods; 0 = uncapped."`
	TaskDeadlineSeconds   int  `json:"task_deadline_seconds" doc:"Per-task timeout; 0 = no deadline."`
	ResyncIntervalSeconds int  `json:"resync_interval_seconds" doc:"Drift re-check interval; 0 = disabled."`
	ResyncRecomposes      bool `json:"resync_recomposes" doc:"Whether a resync also re-runs the composer."`
	OrphanGraceSeconds    int  `json:"orphan_grace_seconds" doc:"Grace window before a composer-dropped child of this kind is torn down; 0 = prune immediately."`
	MaxTransientAttempts  int  `json:"max_transient_attempts" doc:"Consecutive transient reconcile failures before a task dead-letters (marked terminal); 0 = unbounded (retry forever)."`
	Retired               bool `json:"retired,omitempty" doc:"Whether this web-API version is RETIRED: new creates/flips onto it are frozen (422) while existing resources drain."`
}

type kindSchemaOutput struct {
	Body struct {
		kindSummary
		SpecSchema   any `json:"spec_schema,omitempty"`
		StatusSchema any `json:"status_schema,omitempty"`
		ConfigSchema any `json:"config_schema,omitempty"`
		// *SchemaRef is the /docs OpenAPI component NAME for each shape (minted
		// from the kind + axis, e.g. "WidgetSpec", "ReactorConfig"). The UI
		// links to the concrete shape at /docs#/schemas/{ref} instead of rendering
		// the JSON Schema as text. Empty when the kind has no such shape.
		SpecSchemaRef   string `json:"spec_schema_ref,omitempty"`
		StatusSchemaRef string `json:"status_schema_ref,omitempty"`
		ConfigSchemaRef string `json:"config_schema_ref,omitempty"`
		// Operational is the kind's cap + resync policy (kind_config). Present
		// when the kind has a kind_config row; absent → uncapped, no resync.
		Operational *kindOperational `json:"operational,omitempty"`
		// DefaultConfig is the kind's live DEFAULT config DOCUMENT (the
		// providerconfigs is_default row's spec), shaped by ConfigSchema. Absent
		// when the kind has no default config. This + Operational give the UI one
		// panel for everything per-kind-tunable, while the two stay separate tables.
		DefaultConfig json.RawMessage `json:"default_config,omitempty"`
	}
}

func (s *Server) getKindSchema(ctx context.Context, in *kindSchemaInput) (*kindSchemaOutput, error) {
	ks, isReactor, ok := s.servableSchema(model.Kind(in.Kind))
	if !ok {
		return nil, huma.Error404NotFound("unknown kind: " + in.Kind)
	}
	cache := s.schema()
	out := &kindSchemaOutput{}
	out.Body.kindSummary = summariseKind(ks, isReactor)
	out.Body.KindVersions = s.kindVersionsForKind(ks.Kind)
	// Each shape is returned both as the raw JSON Schema (for programmatic
	// clients) and as its /docs component name (*SchemaRef) so the UI can link to
	// the concrete shape at /docs#/schemas/{ref}. Resolve the /docs ref kindVersion to a
	// PUBLISHED kindVersion so the link never dangles: a client asking for ?kindVersion=99
	// (or 0) on a kind published at [1,2] gets refs to the highest published
	// kindVersion ≤ requested (v2 here), not a VpcSpecV99 component that doesn't exist.
	// The inline schema bytes are kind-keyed (kindVersion-agnostic); only the /docs ref is
	// versioned, so clamping keeps it resolvable.
	kindVersion := resolveDocKindVersion(in.KindVersion, out.Body.KindVersions)
	if specSchema := cache.specSchema(ks.Kind); specSchema != nil {
		out.Body.SpecSchema = specSchema
		out.Body.SpecSchemaRef = componentName(ks.Kind, kindVersion, "Spec")
	}
	if statusSchema := cache.statusSchema(ks.Kind); statusSchema != nil {
		out.Body.StatusSchema = statusSchema
		out.Body.StatusSchemaRef = componentName(ks.Kind, kindVersion, "Status")
	}
	// ConfigSchema is served for work AND reactor kinds — a reactor's config
	// (e.g. an upload endpoint/prefix) is authored as a providerconfig just
	// like a work kind's.
	if configSchema := cache.configSchema(ks.Kind); configSchema != nil {
		out.Body.ConfigSchema = configSchema
		out.Body.ConfigSchemaRef = componentName(ks.Kind, kindVersion, "Config")
	}

	// Unify the per-kind config sources for the operator view WITHOUT fusing the
	// tables. Both reads are tiny index probes off the read pool; a read error is
	// non-fatal (the schema is the primary payload) — degrade to "no
	// operational/default info". A reactor has no kind_config row (uncapped, no
	// deadline/resync), so skip that probe; it still has a default providerconfig.
	if s.kindConfigs != nil {
		if !isReactor && kindVersion >= 1 {
			// Show the operational config for the RESOLVED (kind, version) — the same
			// version the /docs schema ref points at (highest published ≤ requested).
			// No implicit v1: a kind with no published version (kindVersion < 1) shows
			// no operational block. The claim reads the concrete (kind, version)
			// directly regardless.
			if kc, found, err := s.kindConfigs.GetKindConfig(ctx, ks.Kind, kindVersion); err == nil && found {
				out.Body.Operational = &kindOperational{
					MaxInflight:           kc.MaxInflight,
					TaskDeadlineSeconds:   int(kc.TaskDeadline.Seconds()),
					ResyncIntervalSeconds: int(kc.ResyncInterval.Seconds()),
					ResyncRecomposes:      kc.ResyncRecomposes,
					OrphanGraceSeconds:    int(kc.OrphanGracePeriod.Seconds()),
					MaxTransientAttempts:  kc.MaxTransientAttempts,
					Retired:               kc.Retired,
				}
			}
		}
		// The kinds panel shows the RESOLVED (kind, version)'s default config (the
		// full per-version default set is on the providerconfigs page). spec only;
		// the opaque bundle (data) is provider-internal and not surfaced here. No
		// implicit v1 — a kind with no published version shows no default.
		if kindVersion >= 1 {
			if doc, found, err := s.kindConfigs.GetDefaultProviderConfig(ctx, ks.Kind, kindVersion); err == nil && found {
				out.Body.DefaultConfig = doc.Spec
			}
		}
	}
	return out, nil
}

func summariseKind(m model.KindManifest, isReactor bool) kindSummary {
	return kindSummary{
		Kind:        string(m.Kind),
		Description: m.Description,
		HasSpec:     len(m.SpecSchema) > 0,
		HasStatus:   len(m.StatusSchema) > 0,
		HasConfig:   len(m.ConfigSchema) > 0,
		IsReactor:   isReactor,
	}
}

// ── /api/kinds/{kind}/config — per-kind OPERATIONAL config (kind_config) ──
//
// The runtime-editable cap + resync for a kind. Distinct from
// /api/providerconfigs (the kind's config DOCUMENT): this is the platform's
// operational tuning, stored in the single-row-per-kind kind_config table the
// claim reads directly. A cap edit takes effect LIVE (the next claim reads it);
// a resync edit is picked up by the control plane's next refresh tick.

// applyKindConfigInput sets a kind's operational config. All fields are the
// effective values (not deltas); 0 / false mean uncapped / no-resync.
type applyKindConfigInput struct {
	Kind string `path:"kind" doc:"Resource kind."`
	// kind_version is IDENTITY (which published version's config to replace) → PATH
	// segment. Inherently required; minimum:"1" rejects a 0 with a 422.
	KindVersion int `path:"kind_version" minimum:"1" maximum:"32767" doc:"Web-API version (v1, v2, …; 1–32767) whose operational config to edit."`
	Body        struct {
		MaxInflight           int  `json:"max_inflight" minimum:"0" doc:"Global in-flight cap across all pods; 0 = uncapped."`
		TaskDeadlineSeconds   int  `json:"task_deadline_seconds" minimum:"0" doc:"Per-task timeout in seconds; 0 = no deadline."`
		ResyncIntervalSeconds int  `json:"resync_interval_seconds" minimum:"0" doc:"Drift re-check interval in seconds; 0 = disabled."`
		ResyncRecomposes      bool `json:"resync_recomposes,omitempty" doc:"Whether a resync also re-runs the composer (only meaningful when resync is enabled)."`
		OrphanGraceSeconds    int  `json:"orphan_grace_seconds" minimum:"0" doc:"Grace window in seconds before a composer-dropped child of this kind is torn down; 0 = prune immediately."`
		MaxTransientAttempts  int  `json:"max_transient_attempts" minimum:"0" doc:"Consecutive transient reconcile failures before a task dead-letters (marked terminal); 0 = unbounded (retry forever)."`
		Retired               bool `json:"retired,omitempty" doc:"RETIRE this web-API version: freeze NEW creates/flips onto it (422) while existing resources keep reconciling so they can drain/migrate off. Runtime-editable; effective values (not deltas), so send the current value to preserve it."`
	}
}

type kindConfigOutput struct {
	Status int `header:"-"`
	Body   struct {
		Kind        string `json:"kind"`
		KindVersion int    `json:"kind_version"`
		kindOperational
	}
}

func (s *Server) applyKindConfig(ctx context.Context, in *applyKindConfigInput) (*kindConfigOutput, error) {
	kind := model.Kind(in.Kind)
	// Operational config is only meaningful for a kind this deployment DECLARES
	// (registered or pending) — gating on the declared set mirrors the
	// providerconfig write path.
	if !s.kindKnown(kind) {
		return nil, huma.Error422UnprocessableEntity("unknown kind: " + in.Kind)
	}
	if s.kindConfigs == nil {
		return nil, huma.Error500InternalServerError("kind config store not configured")
	}
	// FinalizerName is intentionally NOT set here: it is declared in the kind's
	// manifest (the kind_manifest trigger seeds it), not operator-editable. The
	// empty FinalizerName maps to a NULL param that UpsertKindConfig's query
	// COALESCEs to the existing seeded value — so this operator edit can't wipe it
	// and break the orphan sweep's soft-vs-hard delete decision.
	// kind_version is IDENTITY carried in the PATH (>= 1); minimum:"1" rejects a 0 at
	// the edge before this runs. This is an idempotent full-replace of the addressed
	// (kind, kind_version) config row — hence PUT (see the route registration).
	kindVersion := in.KindVersion
	if err := s.kindConfigs.UpsertKindConfig(ctx, store.KindConfig{
		Kind: kind,
		// Edit the operational config of the requested (kind, version). The claim
		// reads this concrete (kind, version) directly, so a per-kindVersion
		// cap/deadline edit takes effect live.
		KindVersion:          kindVersion,
		MaxInflight:          in.Body.MaxInflight,
		TaskDeadline:         time.Duration(in.Body.TaskDeadlineSeconds) * time.Second,
		ResyncInterval:       time.Duration(in.Body.ResyncIntervalSeconds) * time.Second,
		ResyncRecomposes:     in.Body.ResyncRecomposes,
		OrphanGracePeriod:    time.Duration(in.Body.OrphanGraceSeconds) * time.Second,
		MaxTransientAttempts: in.Body.MaxTransientAttempts,
		// retired is operator-editable here (effective value, not a delta): the
		// runtime retire/un-retire path (freeze-new / drain-existing).
		Retired: in.Body.Retired,
	}); err != nil {
		return nil, internalError(ctx, err)
	}
	out := &kindConfigOutput{Status: http.StatusOK}
	out.Body.Kind = in.Kind
	out.Body.KindVersion = kindVersion
	out.Body.kindOperational = kindOperational{
		MaxInflight:           in.Body.MaxInflight,
		TaskDeadlineSeconds:   in.Body.TaskDeadlineSeconds,
		ResyncIntervalSeconds: in.Body.ResyncIntervalSeconds,
		ResyncRecomposes:      in.Body.ResyncRecomposes,
		OrphanGraceSeconds:    in.Body.OrphanGraceSeconds,
		MaxTransientAttempts:  in.Body.MaxTransientAttempts,
		Retired:               in.Body.Retired,
	}
	return out, nil
}
