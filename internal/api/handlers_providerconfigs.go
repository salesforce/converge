package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// Handlers for /api/providerconfigs — the runtime-editable per-kind config
// store (Crossplane's ProviderConfig, adapted). A providerconfig is NOT a
// resource: it lives in its own table, never flows through the work pipeline,
// and is read by providers (the kind DEFAULT, live-reconfigured) or cloned
// into the work queue at schedule (a per-resource CUSTOM override).

type providerConfigBody struct {
	// A providerconfig is addressed by its NAME (unique); its internal uuid is not
	// surfaced. When a composer owns it, the owner is shown as owner_kind/owner_name
	// (public identity), never an owner id.
	Name string `json:"name"`
	Kind string `json:"kind"`
	// KindVersion is the consumer kind's web-API version this config is for (v1, v2, …).
	// A config is per-(kind, version); a resource may only attach a config whose
	// (kind, version) matches its own. Always explicit (>= 1) — a stored config
	// always has a version, so it always serializes.
	KindVersion int  `json:"kind_version"`
	IsDefault   bool `json:"is_default"`
	// Spec + Data are the config DOCUMENT (the manifest body). They are returned
	// ONLY by the detail read (GetProviderConfig); the paginated LIST omits both
	// (its store rows don't select the document — Spec/Data are nil there, and
	// omitempty drops them). Spec is the operational config; Data is the opaque
	// provider BUNDLE (bytea providerconfigs.data — a .star zip etc.),
	// base64-encoded so it can ride in JSON. A config always has a non-empty spec
	// ({} minimum), so omitempty never hides it on the detail read.
	Spec json.RawMessage `json:"spec,omitempty"`
	Data string          `json:"data,omitempty"`
	// Owner identifies the composer root that owns this config (a config a
	// Composer emitted), so the UI can show "owned by <kind>/<name>". Empty
	// for unowned user/API-created configs.
	OwnerKind string    `json:"owner_kind,omitempty"`
	OwnerName string    `json:"owner_name,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func providerConfigBodyFromStore(pc store.ProviderConfig) providerConfigBody {
	b := providerConfigBody{
		Name:        pc.Name,
		Kind:        string(pc.Kind),
		KindVersion: pc.KindVersion,
		IsDefault:   pc.IsDefault,
		Spec:        pc.Spec,
		OwnerKind:   pc.OwnerKind,
		OwnerName:   pc.OwnerName,
		CreatedAt:   pc.CreatedAt,
		UpdatedAt:   pc.UpdatedAt,
	}
	// Echo the bundle base64-encoded (omitempty drops the field for a bundle-less
	// config, so the common spec-only case stays clean on the wire).
	if len(pc.Data) > 0 {
		b.Data = base64.StdEncoding.EncodeToString(pc.Data)
	}
	return b
}

// applyProviderConfigInput upserts a config by name. is_default=true makes it
// the kind's single live-reconfigurable default (at most one per kind);
// otherwise it's a custom override a resource attaches via provider_config_ref.
type applyProviderConfigInput struct {
	Body struct {
		// Type is the OPTIONAL self-describing object tag (applyType*). Only
		// "providerconfig" here; accepted + validated, never affects routing.
		Type string `json:"type,omitempty" enum:"providerconfig" doc:"Optional self-describing tag; must be \"providerconfig\" if set. Ignored for routing."`
		Name string `json:"name" minLength:"1" doc:"Global config name; provider_config_ref resolves by it."`
		Kind string `json:"kind" minLength:"1" doc:"Consumer kind this config parameterises."`
		// KindVersion is the consumer kind's web-API version this config is for (v1,
		// v2, …). REQUIRED and explicit (>= 1): no implicit v1 default. The config is
		// validated against THIS (kind, version)'s config_schema, and default-ness is
		// per-(kind, version). A resource may only attach a config whose (kind,
		// version) matches its own.
		KindVersion int             `json:"kind_version" minimum:"1" maximum:"32767" doc:"Consumer kind web-API version (v1, v2, …; 1–32767). REQUIRED — no implicit v1 default."`
		IsDefault   bool            `json:"is_default,omitempty" doc:"True = this (kind, version)'s single live-reconfigurable DEFAULT; false = a CUSTOM override referenced via provider_config_ref."`
		Spec        json.RawMessage `json:"spec" doc:"Config document; shape is the kind's config_schema (GET /api/kinds/{kind}/schema)."`
		// Data is the OPAQUE provider bundle (e.g. a zip of Starlark .star files)
		// base64-encoded — bytea can't ride raw in JSON. The server decodes it into
		// providerconfigs.data (capped at maxProviderConfigDataBytes decoded). Empty
		// leaves the bundle empty. Kind-wide logic, so it only makes sense on a
		// DEFAULT config (no per-resource override).
		Data string `json:"data,omitempty" doc:"base64-encoded opaque provider bundle (e.g. a zip of Starlark .star files); decoded into the config's data bytes. Max 10 MiB decoded."`
	}
}

type providerConfigOutput struct {
	Status int                `header:"-"`
	Apply  string             `header:"X-Apply-Result"`
	Body   providerConfigBody `json:"body"`
}

func (s *Server) applyProviderConfig(ctx context.Context, in *applyProviderConfigInput) (*providerConfigOutput, error) {
	kind := model.Kind(in.Body.Kind)
	// The config must parameterise a kind this deployment DECLARES — but NOT
	// necessarily one that's registered yet: a kind whose Setup is pending on a
	// missing/invalid default config is exactly the one an operator needs to
	// write a config for, so gating on the declared set (not the registry) is
	// what lets the retrier bring it online.
	//
	// A REACTOR kind is accepted too. Reactors are deliberately kept OUT of
	// `declared` (they own no creatable resource — see SetReactorSchemas), but a
	// reactor legitimately needs a default providerconfig for its BOOTSTRAP
	// parameters (e.g. statussink's object-store endpoint). Without this, a reactor
	// whose Setup dials that endpoint stays pending forever: the worker defers Setup
	// until the config arrives, but the config could never be applied. Its
	// config_schema lives in reactorSchemas, so validateConfig below still applies.
	if !s.configurableKindKnown(kind) {
		return nil, huma.Error422UnprocessableEntity("unknown kind: " + in.Body.Kind)
	}
	// kind_version is REQUIRED and explicit (>= 1) — a providerconfig must name the
	// (kind, version) it configures, with no implicit v1 default. The body field's
	// `minimum:"1"` tag rejects a missing/0 value with a 422 at the edge (an omitted
	// int defaults to 0, which minimum:"1" still rejects).
	kindVersion := in.Body.KindVersion
	// Validate the document against THIS (kind, version)'s config_schema so a
	// malformed config is rejected at write time, not silently ignored by the
	// worker. The config is per-kindVersion now (a vpc/v2 config validates against v2's
	// config_schema), and validateConfig no-ops for a (kind, version) whose manifest
	// declares no config_schema. A pending kind's schema is still known (pure data
	// in the manifest), so this is the exact path that un-sticks a pending kind.
	if err := s.schema().validateConfig(kind, kindVersion, in.Body.Spec); err != nil {
		return nil, err
	}

	// Decode the optional opaque bundle (the worker materialises it; the DB stores
	// it byte-exact). A malformed base64 is a client error, not a 500. There is no
	// separate size cap on the decoded `data` vs the rest of the body: the whole
	// providerconfig apply is bounded at 50 MB by configBody (server.go), enforced
	// by Huma at ingress before the body is buffered — one simple limit covers it.
	var data []byte
	if in.Body.Data != "" {
		d, derr := base64.StdEncoding.DecodeString(in.Body.Data)
		if derr != nil {
			return nil, huma.Error422UnprocessableEntity("data is not valid base64: " + derr.Error())
		}
		data = d
	}

	pc, created, err := s.configs.UpsertProviderConfig(ctx, in.Body.Name, kind, kindVersion, in.Body.IsDefault, in.Body.Spec, data)
	if err != nil {
		// Second default for a (kind, version): the one-default-per-(kind,kindVersion)
		// invariant. 409 so the caller un-defaults the existing one (or picks
		// custom) and retries.
		if errors.Is(err, store.ErrConfigDefaultExists) {
			return nil, huma.Error409Conflict("kind " + in.Body.Kind + " already has a default provider_config for that kindVersion")
		}
		return nil, internalError(ctx, err)
	}

	out := &providerConfigOutput{Status: http.StatusOK, Apply: applyResultConfigured}
	if created {
		out.Status, out.Apply = http.StatusCreated, applyResultCreated
	}
	out.Body = providerConfigBodyFromStore(pc)
	return out, nil
}

type providerConfigNameInput struct {
	Name string `path:"name" doc:"Config name."`
}

func (s *Server) getProviderConfig(ctx context.Context, in *providerConfigNameInput) (*providerConfigOutput, error) {
	pc, found, err := s.configs.GetProviderConfig(ctx, in.Name)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	if !found {
		return nil, huma.Error404NotFound("no such provider_config: " + in.Name)
	}
	out := &providerConfigOutput{Status: http.StatusOK}
	out.Body = providerConfigBodyFromStore(pc)
	return out, nil
}

// listProviderConfigsInput is the PAGINATED list query. All filters optional
// and AND-combined: kind (exact), name (case-insensitive substring), default
// (tri-state — omit for any, true/false to filter by role).
type listProviderConfigsInput struct {
	Kind        string `query:"kind" doc:"Optional exact consumer-kind filter; omit for all."`
	KindVersion int    `query:"kind_version" minimum:"0" maximum:"32767" doc:"Optional exact consumer-kind version (v1, v2, …; 1–32767) filter; 0/omit for any."`
	Name        string `query:"name" doc:"Optional case-insensitive substring match on name."`
	Default     string `query:"default" enum:"true,false" doc:"Optional is_default filter; omit for any, true/false to filter."`
	Limit       int    `query:"limit" default:"100" minimum:"1" maximum:"500"`
	Offset      int    `query:"offset" default:"0" minimum:"0"`
}

type listProviderConfigsOutput struct {
	Body struct {
		ProviderConfigs []providerConfigBody `json:"provider_configs"`
		Total           int64                `json:"total"`
		Limit           int                  `json:"limit"`
		Offset          int                  `json:"offset"`
	}
}

func (s *Server) listProviderConfigs(ctx context.Context, in *listProviderConfigsInput) (*listProviderConfigsOutput, error) {
	f := store.ProviderConfigFilter{Kind: in.Kind, KindVersion: in.KindVersion, Name: in.Name, IsDefault: parseTriBool(in.Default)}
	rows, err := s.configs.ListProviderConfigs(ctx, f, in.Limit, in.Offset)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	total, err := s.configs.CountProviderConfigs(ctx, f)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	out := &listProviderConfigsOutput{}
	out.Body.ProviderConfigs = make([]providerConfigBody, len(rows))
	for i, r := range rows {
		out.Body.ProviderConfigs[i] = providerConfigBodyFromStore(r)
	}
	out.Body.Total = total
	out.Body.Limit = in.Limit
	out.Body.Offset = in.Offset
	return out, nil
}

// parseTriBool maps the tri-state `default` query value to *bool: "true"/"false"
// → that value, anything else (omitted) → nil ("any").
func parseTriBool(v string) *bool {
	switch v {
	case "true":
		t := true
		return &t
	case "false":
		f := false
		return &f
	default:
		return nil
	}
}

type deleteProviderConfigOutput struct {
	Status int `header:"-"`
	Body   struct {
		Deleted bool `json:"deleted"`
	}
}

func (s *Server) deleteProviderConfig(ctx context.Context, in *providerConfigNameInput) (*deleteProviderConfigOutput, error) {
	deleted, err := s.configs.DeleteProviderConfig(ctx, in.Name)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	if !deleted {
		return nil, huma.Error404NotFound("no such provider_config: " + in.Name)
	}
	out := &deleteProviderConfigOutput{Status: http.StatusOK}
	out.Body.Deleted = true
	return out, nil
}
