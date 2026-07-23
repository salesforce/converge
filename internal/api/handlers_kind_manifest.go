package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/kindschema"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// itoa is a tiny local helper for composing (kind, kindVersion) error strings.
func itoa(n int) string { return strconv.Itoa(n) }

// requireManifests guards the manifest handlers: the store must be wired (it is
// in every real deployment; nil only in a mis-wired test Server). Returns a 500
// StatusError to return as-is, or nil when configured.
func (s *Server) requireManifests() error {
	if s.manifests == nil {
		return huma.Error500InternalServerError("kind manifest store not configured")
	}
	return nil
}

// Handlers for /api/kinds/{kind}/manifest — the CRD surface. An operator applies
// a KindManifest (JSON Schemas + declared reactions + finalizer + policy); the
// two DB triggers derive kind_config + reactor_bindings, and the KindManifestCache +
// reaction engine pick it up. This is the "define what this kind IS" surface: a
// kind exists once its manifest is applied and a worker connects, with no
// compile-time provider registry in the core.

// manifestBody is the wire form of model.KindManifest — the FULL CRD: the
// schemas (raw JSON Schema documents), the typed reaction list, the finalizer,
// AND the operational policy knobs (max_inflight / task_deadline_secs / resync /
// orphan_grace). All fields are accepted so a manifest downloaded/edited as one
// JSON document (e.g. testfixtures/*.kind.json) round-trips verbatim. Fields are
// declared inline (NOT an embedded struct) so huma flattens them into the body
// schema; an embedded struct here makes huma reject every field as "unexpected".
type manifestBody struct {
	Kind string `json:"kind"`
	// KindVersion is the web-API version this manifest defines (vpc/v1, vpc/v2, …).
	// REQUIRED and explicit (>= 1): there is no implicit v1 default. A given
	// (kind, kindVersion) is bound to one content hash at publish (the invariant
	// enforced below), so a kindVersion can never silently mean two manifests.
	KindVersion        int                  `json:"kind_version" minimum:"1" maximum:"32767" doc:"Web-API version (v1, v2, …; 1–32767). REQUIRED — no implicit v1 default."`
	Description        string               `json:"description,omitempty"`
	SpecSchema         json.RawMessage      `json:"spec_schema,omitempty"`
	StatusSchema       json.RawMessage      `json:"status_schema,omitempty"`
	ConfigSchema       json.RawMessage      `json:"config_schema,omitempty"`
	Reactions          []model.ReactionDecl `json:"reactions"`
	Finalizer          string               `json:"finalizer_name,omitempty"`
	MaxInflight        int                  `json:"max_inflight,omitempty"`
	TaskDeadlineSecs   int                  `json:"task_deadline_secs,omitempty"`
	ResyncIntervalSecs int                  `json:"resync_interval_secs,omitempty"`
	ResyncRecomposes   bool                 `json:"resync_recomposes,omitempty"`
	OrphanGraceSecs    int                  `json:"orphan_grace_secs,omitempty"`
	// MaxTransientAttempts caps consecutive transient reconcile failures before the
	// failure dead-letters (marked terminal). 0 = unbounded (retry forever).
	MaxTransientAttempts int `json:"max_transient_attempts,omitempty"`
	// Retired declares this web-API version SUNSET at publish: new creates/flips
	// onto it are frozen while existing resources drain. Operator-editable at
	// runtime too (POST /api/kinds/{kind}/config), and preserved across a re-apply
	// (the sync trigger never un-retires) — so a re-publish need not resend it.
	Retired bool `json:"retired,omitempty"`
}

func manifestBodyFromStore(m model.KindManifest) manifestBody {
	return manifestBody{
		Kind:                 string(m.Kind),
		KindVersion:          m.KindVersion,
		Description:          m.Description,
		SpecSchema:           m.SpecSchema,
		StatusSchema:         m.StatusSchema,
		ConfigSchema:         m.ConfigSchema,
		Reactions:            m.Reactions,
		Finalizer:            m.FinalizerName,
		MaxInflight:          m.MaxInflight,
		TaskDeadlineSecs:     m.TaskDeadlineSecs,
		ResyncIntervalSecs:   m.ResyncIntervalSecs,
		ResyncRecomposes:     m.ResyncRecomposes,
		OrphanGraceSecs:      m.OrphanGraceSecs,
		MaxTransientAttempts: m.MaxTransientAttempts,
		Retired:              m.Retired,
	}
}

// toManifest builds the model.KindManifest from the wire body, with the path
// kind authoritative. KindVersion comes from the body verbatim (REQUIRED, >= 1 —
// no normalization; the handler rejects a missing/0 before this is called).
func (b manifestBody) toManifest(kind model.Kind) model.KindManifest {
	return model.KindManifest{
		Kind:                 kind,
		KindVersion:          b.KindVersion,
		Description:          b.Description,
		SpecSchema:           b.SpecSchema,
		StatusSchema:         b.StatusSchema,
		ConfigSchema:         b.ConfigSchema,
		Reactions:            b.Reactions,
		FinalizerName:        b.Finalizer,
		MaxInflight:          b.MaxInflight,
		TaskDeadlineSecs:     b.TaskDeadlineSecs,
		ResyncIntervalSecs:   b.ResyncIntervalSecs,
		ResyncRecomposes:     b.ResyncRecomposes,
		OrphanGraceSecs:      b.OrphanGraceSecs,
		MaxTransientAttempts: b.MaxTransientAttempts,
		Retired:              b.Retired,
	}
}

// ── PUT /api/kinds/{kind}/manifest ──

// putManifestBody is the PUT request body: the full manifest fields inline
// (huma does NOT flatten an embedded struct into the body schema — embedding
// here makes it reject every field as "unexpected") plus the schema-change gate.
type putManifestBody struct {
	// Type is the OPTIONAL self-describing object tag (applyType* in dto_resource.go).
	// Only "manifest" here; accepted + validated, never affects routing (the
	// /kinds/{kind}/manifest URL already selects this endpoint). Lets a directory-sync
	// client tag a CRD file so it routes without a filename convention.
	Type string `json:"type,omitempty" enum:"manifest" doc:"Optional self-describing tag; must be \"manifest\" if set. Ignored for routing."`
	Kind string `json:"kind"`
	// KindVersion is the web-API version this apply defines (vpc/v1, vpc/v2, …).
	// REQUIRED and explicit (>= 1): no implicit v1 default. Publishing a NEW
	// kindVersion inserts a new (kind, kindVersion); an in-place re-publish of an
	// EXISTING kindVersion is bound by the content-hash invariant enforced below.
	KindVersion        int                  `json:"kind_version" minimum:"1" maximum:"32767" doc:"Web-API version (v1, v2, …; 1–32767). REQUIRED — no implicit v1 default."`
	Description        string               `json:"description,omitempty"`
	SpecSchema         json.RawMessage      `json:"spec_schema,omitempty"`
	StatusSchema       json.RawMessage      `json:"status_schema,omitempty"`
	ConfigSchema       json.RawMessage      `json:"config_schema,omitempty"`
	Reactions          []model.ReactionDecl `json:"reactions"`
	Finalizer          string               `json:"finalizer_name,omitempty"`
	MaxInflight        int                  `json:"max_inflight,omitempty"`
	TaskDeadlineSecs   int                  `json:"task_deadline_secs,omitempty"`
	ResyncIntervalSecs int                  `json:"resync_interval_secs,omitempty"`
	ResyncRecomposes   bool                 `json:"resync_recomposes,omitempty"`
	OrphanGraceSecs    int                  `json:"orphan_grace_secs,omitempty"`
	// MaxTransientAttempts caps consecutive transient reconcile failures before the
	// failure dead-letters (marked terminal). 0 = unbounded (retry forever).
	MaxTransientAttempts int `json:"max_transient_attempts,omitempty"`
}

func (b putManifestBody) toManifest(kind model.Kind) model.KindManifest {
	return manifestBody{
		Kind:                 b.Kind,
		KindVersion:          b.KindVersion,
		Description:          b.Description,
		SpecSchema:           b.SpecSchema,
		StatusSchema:         b.StatusSchema,
		ConfigSchema:         b.ConfigSchema,
		Reactions:            b.Reactions,
		Finalizer:            b.Finalizer,
		MaxInflight:          b.MaxInflight,
		TaskDeadlineSecs:     b.TaskDeadlineSecs,
		ResyncIntervalSecs:   b.ResyncIntervalSecs,
		ResyncRecomposes:     b.ResyncRecomposes,
		OrphanGraceSecs:      b.OrphanGraceSecs,
		MaxTransientAttempts: b.MaxTransientAttempts,
	}.toManifest(kind)
}

type putManifestInput struct {
	Kind string `path:"kind" doc:"Resource kind."`
	Body putManifestBody
}

type manifestOutput struct {
	Status int `header:"-"`
	Body   struct {
		manifestBody
		ManifestVersion int64 `json:"manifest_version"`
	}
}

func (s *Server) putKindManifest(ctx context.Context, in *putManifestInput) (*manifestOutput, error) {
	if err := s.requireManifests(); err != nil {
		return nil, err
	}
	kind := model.Kind(in.Kind)
	// Path kind is authoritative (the body's kind, if present, is ignored).
	m := in.Body.toManifest(kind)

	// Validate the reaction legality lattice up front (the DB CHECK re-validates,
	// but a 422 with the precise reason is friendlier than a raw SQL exception).
	if err := model.ValidateManifest(m); err != nil {
		return nil, huma.Error422UnprocessableEntity(err.Error())
	}

	// Publish-time (kind, kindVersion) → content-hash INVARIANT, LINTED. "Any
	// vN worker serves any vN resource" is safe ONLY if vN means exactly one thing
	// across the fleet. So an in-place re-publish of an EXISTING (kind, kindVersion) is
	// gated against the STORED manifest for THAT kindVersion, with the additive-vs-
	// breaking linter (kindschema.ManifestSchemaCompat) making the call machine-side
	// instead of trusting a human flag:
	//   - identical schema hash          → idempotent no-op re-apply (allowed).
	//   - schema changed, COMPATIBLE      → additive/backward-compatible (a new
	//                                       optional field, a widened bound, a
	//                                       removed requirement) → AUTO-ALLOWED; any
	//                                       vN worker stays interchangeable. No flag
	//                                       needed.
	//   - schema changed, BREAKING        → rejected 409 ALWAYS. A backward-
	//                                       INCOMPATIBLE edit MUST ship as a NEW
	//                                       kindVersion so live resources keep validating
	//                                       against the schema they were applied under —
	//                                       an in-place breaking mutation silently
	//                                       reinterprets every existing resource of this
	//                                       (kind, kindVersion). There is NO override: the
	//                                       linter is sound (may over-report, never
	//                                       under-report), so a false-positive breaking
	//                                       verdict is resolved by bumping the kindVersion.
	// Publishing a brand-new (kind, kindVersion) has no prior manifest → always allowed.
	// kind_version is REQUIRED and explicit (>= 1) — no implicit v1 default. The body
	// field's `minimum:"1"` tag rejects a missing/0 value with a 422 at the edge
	// (an omitted int defaults to 0, which minimum:"1" still rejects).
	kindVersion := in.Body.KindVersion
	if prevManifest, ok, err := s.manifests.GetKindManifest(ctx, kind, kindVersion); err != nil {
		return nil, internalError(ctx, err)
	} else if ok && kindschema.SchemaHashOf(m) != kindschema.SchemaHashOf(prevManifest) {
		compat := kindschema.ManifestSchemaCompat(prevManifest, m)
		if !compat.Compatible {
			// BREAKING: never unlockable in place — a new kindVersion is mandatory
			// (there is no override).
			return nil, huma.Error409Conflict(
				"BREAKING schema change for existing " + in.Kind + "/v" + itoa(kindVersion) +
					" — ship it as a NEW kindVersion (a breaking edit cannot be applied in place; live resources of this version were validated against the current schema). Breaking edits: " +
					strings.Join(compat.Reasons, "; "))
		}
	}

	version, err := s.manifests.UpsertKindManifest(ctx, m)
	if err != nil {
		return nil, internalError(ctx, err)
	}

	// Rebuild the API schema validator from the new manifest set so apply
	// validation immediately reflects the change (the claim path's KindManifestCache,
	// on the control/broker side, refreshes independently via its own NOTIFY/failsafe).
	if s.onManifestApply != nil {
		s.onManifestApply()
	}

	out := &manifestOutput{Status: http.StatusOK}
	out.Body.manifestBody = manifestBodyFromStore(m)
	out.Body.ManifestVersion = version
	return out, nil
}

// ── GET /api/kinds/{kind}/manifest ──

type getManifestInput struct {
	Kind string `path:"kind" doc:"Resource kind."`
	// kind_version is IDENTITY (which published version to read), so it is a PATH
	// segment, not a query filter. A path param is inherently required; minimum:"1"
	// still rejects a 0 with a 422.
	KindVersion int `path:"kind_version" minimum:"1" maximum:"32767" doc:"Web-API version (v1, v2, …; 1–32767)."`
}

func (s *Server) getKindManifest(ctx context.Context, in *getManifestInput) (*manifestOutput, error) {
	if err := s.requireManifests(); err != nil {
		return nil, err
	}
	// kind_version is IDENTITY carried in the PATH (>= 1); minimum:"1" rejects a 0
	// with a 422 at the edge before this handler runs. No implicit v1 default.
	kindVersion := in.KindVersion
	m, ok, err := s.manifests.GetKindManifest(ctx, model.Kind(in.Kind), kindVersion)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	if !ok {
		return nil, huma.Error404NotFound("no manifest for kind " + in.Kind + "/v" + itoa(kindVersion))
	}
	out := &manifestOutput{Status: http.StatusOK}
	out.Body.manifestBody = manifestBodyFromStore(m)
	return out, nil
}

// ── DELETE /api/kinds/{kind}/manifest ──

type deleteManifestInput struct {
	Kind string `path:"kind" doc:"Resource kind."`
	// kind_version is IDENTITY (which published version to delete) → PATH segment.
	// Inherently required; minimum:"1" still rejects a 0 with a 422.
	KindVersion int `path:"kind_version" minimum:"1" maximum:"32767" doc:"Web-API version (v1, v2, …; 1–32767) to delete."`
}

type deleteManifestOutput struct {
	Status int `header:"-"`
	Body   struct {
		Kind           string `json:"kind"`
		KindVersion    int    `json:"kind_version"`
		DeletedConfigs int64  `json:"deleted_configs" doc:"Provider configs cascade-deleted with the version."`
	}
}

// deleteKindManifest removes one (kind, kind_version) — the CRD, its derived
// kind_config, and its provider configs — but ONLY when nothing references the
// version: it is refused 409 if any resource (incl. draining) is pinned to it, or a
// reactor binding EXACTLY pins it (an unpinned binding resolves to the other
// versions and does not block). Idempotent: deleting a version that is already gone
// is a 404. On success the AFTER DELETE trigger NOTIFYs, and we rebuild the local
// schema validator so an apply at the deleted version immediately 404s/validates
// against the remaining set.
func (s *Server) deleteKindManifest(ctx context.Context, in *deleteManifestInput) (*deleteManifestOutput, error) {
	if err := s.requireManifests(); err != nil {
		return nil, err
	}
	// kind_version is IDENTITY carried in the PATH (>= 1); minimum:"1" rejects a 0
	// with a 422 at the edge before this handler runs. No implicit v1 default.
	kindVersion := in.KindVersion
	found, deletedConfigs, err := s.manifests.DeleteKindManifest(ctx, model.Kind(in.Kind), kindVersion)
	if err != nil {
		if errors.Is(err, store.ErrKindVersionInUse) {
			// Still referenced — a client-fixable conflict, not a server fault.
			return nil, huma.Error409Conflict(err.Error())
		}
		return nil, internalError(ctx, err)
	}
	if !found {
		return nil, huma.Error404NotFound("no manifest for kind " + in.Kind + "/v" + itoa(kindVersion))
	}
	// Rebuild the API schema validator so the deleted (kind, kindVersion) drops out
	// immediately (the claim path's KindManifestCache, on the control/broker side, refreshes
	// via the DELETE NOTIFY / failsafe).
	if s.onManifestApply != nil {
		s.onManifestApply()
	}
	out := &deleteManifestOutput{Status: http.StatusOK}
	out.Body.Kind = in.Kind
	out.Body.KindVersion = kindVersion
	out.Body.DeletedConfigs = deletedConfigs
	return out, nil
}

// ── GET /api/kinds/manifests (list) ──

type listManifestsOutput struct {
	Body struct {
		Manifests []manifestBody `json:"manifests"`
	}
}

func (s *Server) listKindManifests(ctx context.Context, _ *struct{}) (*listManifestsOutput, error) {
	if err := s.requireManifests(); err != nil {
		return nil, err
	}
	ms, err := s.manifests.ListKindManifests(ctx)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	ms = capList(ctx, "kind_manifests", ms)
	out := &listManifestsOutput{}
	out.Body.Manifests = make([]manifestBody, 0, len(ms))
	for _, m := range ms {
		out.Body.Manifests = append(out.Body.Manifests, manifestBodyFromStore(m))
	}
	return out, nil
}
