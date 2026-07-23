// Package specschema is the SINGLE server-side schema validator for resource
// specs, providerconfig documents, and operate-verb inputs. It exists so
// validation is not pinned to the HTTP boundary: every entry point that authors
// a resource — the HTTP API, a custom ingestion duty calling store.ApplySpec, a
// reactor chaining a downstream resource, and the composer emitting children —
// validates through THIS one engine, at the data gateway, against the same
// rules.
//
// It is built ONCE from the DECLARED kind manifests (the CRD →
// []model.KindManifest), each carrying the kind's JSON Schema BYTES for
// every declared axis. The Validator it returns is IMMUTABLE and goroutine-safe.
//
// DECOUPLING: callers depend only on the Validator interface and a plain
// *Error — NO huma types leak out. huma is merely today's IMPLEMENTATION of the
// schema build + validate (reusing the engine the API already shipped, so the
// HTTP path and the internal paths can never disagree on the rules). Swapping
// the underlying schema engine later (a different web framework, a reflect
// validator) means reimplementing ONLY this package; the store/composer/api call
// sites are unchanged because they hold the interface.
package specschema

import (
	"encoding/json"
	"fmt"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/kindschema"
	"github.com/salesforce/converge/internal/model"
)

// Validator validates raw JSON documents against a kind's declared shape. The
// interface is huma-free: callers (store, composer, api) import only this. A nil
// Validator validates nothing (returns nil) — the safe no-op for tests / a pod
// built without schemas.
type Validator interface {
	// ValidateSpec checks a resource spec against the (kind, kindVersion)'s SpecSchema.
	// kindVersion is the web-API version the spec is authored against (a user apply / a
	// kindVersion flip / a composed child): the schema is per-(kind, kindVersion), so a v2
	// spec is checked against v2's schema, not v1's. kindVersion is always explicit
	// (>= 1); callers reject a missing/0 version at the edge before reaching here — it
	// is NEVER normalized to v1. A (kind, kindVersion) with no registered SpecSchema
	// (untyped opt-out, an unpublished kindVersion, or a stray 0) matches nothing and
	// passes as an untyped opt-out. An empty spec for a (kind, kindVersion) that HAS a
	// SpecSchema is rejected.
	ValidateSpec(kind model.Kind, kindVersion int, raw json.RawMessage) error
	// ValidateSpecPartial is ValidateSpec for an INTENTIONALLY incomplete spec:
	// the composer emits a child whose flowed fields are filled later from an
	// upstream's status (see resource_deps.value_flows). Those fields are
	// logically required AND keep their value constraints (minLength/enum) — but
	// they are absent at emit time, so a strict check would reject a valid
	// composition. skipPaths lists the flowed top-level field names (e.g.
	// ["account_id"]); for those fields ONLY, presence and value are not checked.
	// EVERY other field is validated exactly as ValidateSpec — a bad cidr still
	// fails. Empty skipPaths ⇒ identical to ValidateSpec. The strict
	// ValidateSpec (API edge, user create) is unchanged, so a hand-authored spec
	// with an empty flowed field is still rejected there. Validated against the
	// child's (kind, kindVersion) schema.
	ValidateSpecPartial(kind model.Kind, kindVersion int, raw json.RawMessage, skipPaths []string) error
	// ValidateConfig checks a providerconfig document against the (kind, kindVersion)'s
	// ConfigSchema. An empty doc is validated as {} (a config that omits required
	// fields must be rejected, not skipped).
	ValidateConfig(kind model.Kind, kindVersion int, raw json.RawMessage) error
	// ValidateVerbInput checks an operate-verb input. The KindManifest stores
	// NO per-verb input schema (ReactionDecl carries only the verb name), so this
	// is now a no-op (always nil) — operate-input validation is best-effort and
	// delegated to the worker. Retained on the interface so callers (api) keep the
	// same call path; drop it once no caller invokes it.
	ValidateVerbInput(kind model.Kind, verb string, raw json.RawMessage) error
}

// Error is the typed validation failure callers map to their own boundary error
// (the API → huma.Error400BadRequest; the store → model.Terminal). It never
// carries a huma type, so importing it pulls in no web model.
type Error struct {
	Kind    model.Kind
	What    string   // "spec" | "config" | "input"
	Details []string // per-field messages from the underlying validator
}

func (e *Error) Error() string {
	if len(e.Details) == 0 {
		return fmt.Sprintf("%s for kind %q failed validation", e.What, e.Kind)
	}
	return fmt.Sprintf("%s for kind %q failed validation: %v", e.What, e.Kind, e.Details)
}

// cache is the huma-backed Validator implementation: the per-kind *huma.Schema
// validators (decoded from each manifest's JSON Schema bytes) + the registry
// huma.Validate needs, built once and never mutated. There is no verb-input map:
// the manifest stores no per-verb input schema (see ValidateVerbInput).
type cache struct {
	registry huma.Registry
	specs    map[model.KindVersion]*huma.Schema
	configs  map[model.KindVersion]*huma.Schema
}

// New builds the Validator from the declared kind manifests. nil/empty → a
// Validator that validates everything as OK (no schemas to check against).
//
// Each manifest carries JSON Schema BYTES (not a reflect.Type): we decode the
// SpecSchema/ConfigSchema bytes into a *huma.Schema and PrecomputeMessages so
// the cached schema feeds the identical huma.Validate path. A nil/empty schema
// for an axis (the manifest opted out) → no validator for that axis (validates
// as OK), exactly as a nil SpecType did before.
func New(manifests []model.KindManifest) Validator {
	hr := huma.NewMapRegistry("#/components/schemas/", huma.DefaultSchemaNamer)
	c := &cache{
		registry: hr,
		specs:    map[model.KindVersion]*huma.Schema{},
		configs:  map[model.KindVersion]*huma.Schema{},
	}
	for _, m := range manifests {
		km := model.KindVersion{Kind: m.Kind, Version: m.KindVersion}
		if s := decodeSchema(m.SpecSchema); s != nil {
			c.specs[km] = s
		}
		if s := decodeSchema(m.ConfigSchema); s != nil {
			c.configs[km] = s
		}
		// Verb-input validation is intentionally dropped: the manifest's
		// ReactionDecl carries only a verb name, no per-verb input schema. See
		// ValidateVerbInput (now a no-op) — the worker validates verb input.
	}
	return c
}

// decodeSchema turns a manifest's JSON Schema bytes into a ready-to-validate
// *huma.Schema. Empty/nil bytes → nil (the axis opted out of validation).
func decodeSchema(raw json.RawMessage) *huma.Schema {
	return kindschema.DecodeSchema(raw)
}

func (c *cache) ValidateSpec(kind model.Kind, kindVersion int, raw json.RawMessage) error {
	schema := c.specs[model.KindVersion{Kind: kind, Version: kindVersion}]
	if schema == nil {
		return nil // (kind, kindVersion) opted out of typed validation / unpublished kindVersion
	}
	if len(raw) == 0 {
		return &Error{Kind: kind, What: "spec", Details: []string{"spec is required"}}
	}
	return c.validate(kind, "spec", schema, raw)
}

// ValidateSpecPartial validates raw against kind's spec schema with the
// skipPaths fields treated as absent (presence + value unchecked) — for the
// composer emitting a child whose flowed fields are filled later. See the
// Validator interface for the contract. Implementation: clone the cached
// schema, drop the skipped names from its Required set, delete them from the
// decoded doc, then run the shared validate core; every other field is checked
// exactly as ValidateSpec. The cached schema is never mutated (the clone owns a
// fresh Required slice), so this stays goroutine-safe.
func (c *cache) ValidateSpecPartial(kind model.Kind, kindVersion int, raw json.RawMessage, skipPaths []string) error {
	schema := c.specs[model.KindVersion{Kind: kind, Version: kindVersion}]
	if schema == nil {
		return nil // (kind, kindVersion) opted out of typed validation / unpublished kindVersion
	}
	if len(skipPaths) == 0 {
		return c.ValidateSpec(kind, kindVersion, raw) // nothing to relax
	}
	if len(raw) == 0 {
		return &Error{Kind: kind, What: "spec", Details: []string{"spec is required"}}
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return &Error{Kind: kind, What: "spec", Details: []string{"spec is not valid JSON: " + err.Error()}}
	}
	// Drop each flowed field from BOTH the document and the schema's required
	// set, then validate. Deleting the key alone is not enough: a non-omitempty
	// Go field is in huma's `required` list, so an absent flowed field would
	// fail "required property ... to be present". And clearing it to a zero
	// value is not enough either: a flowed field that ALSO carries a value
	// constraint (e.g. account_id minLength:"12") would fail that constraint on
	// "". Removing the path from required + from the doc treats the flowed field
	// as genuinely absent for this one check, regardless of its constraints,
	// while every other field is validated normally.
	doc, _ := v.(map[string]any)
	relaxed := *schema // shallow copy; we only rewrite Required
	relaxed.Required = make([]string, 0, len(schema.Required))
	skip := make(map[string]struct{}, len(skipPaths))
	for _, p := range skipPaths {
		skip[p] = struct{}{}
	}
	for _, req := range schema.Required {
		if _, skipped := skip[req]; !skipped {
			relaxed.Required = append(relaxed.Required, req)
		}
	}
	if doc != nil {
		for p := range skip {
			delete(doc, p)
		}
	}
	// huma derives an internal required-lookup from Required during build/Precompute;
	// rebuild it for the copy so the dropped fields are actually un-required.
	relaxed.PrecomputeMessages()
	return c.validateValue(kind, "spec", &relaxed, v)
}

func (c *cache) ValidateConfig(kind model.Kind, kindVersion int, raw json.RawMessage) error {
	schema := c.configs[model.KindVersion{Kind: kind, Version: kindVersion}]
	if schema == nil {
		return nil
	}
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`) // empty config validates as {} so required fields still fail
	}
	return c.validate(kind, "config", schema, raw)
}

// ValidateVerbInput is a no-op: a KindManifest stores no
// per-verb input schema, so there is nothing to validate here. Operate-input
// validation is best-effort and performed by the worker. Always returns nil.
func (c *cache) ValidateVerbInput(_ model.Kind, _ string, _ json.RawMessage) error {
	return nil
}

// validate is the shared huma-validate core. Unmarshal failures and schema
// violations both surface as a typed *Error (never a huma error).
func (c *cache) validate(kind model.Kind, what string, schema *huma.Schema, raw json.RawMessage) error {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return &Error{Kind: kind, What: what, Details: []string{what + " is not valid JSON: " + err.Error()}}
	}
	return c.validateValue(kind, what, schema, v)
}

// validateValue is the validate core for an already-decoded document — used by
// validate (after unmarshal) and by ValidateSpecPartial (which mutates the
// decoded map before validating).
func (c *cache) validateValue(kind model.Kind, what string, schema *huma.Schema, v any) error {
	res := &huma.ValidateResult{}
	pb := huma.NewPathBuffer([]byte(what), 0)
	huma.Validate(c.registry, schema, pb, huma.ModeWriteToServer, v, res)
	if len(res.Errors) == 0 {
		return nil
	}
	details := make([]string, 0, len(res.Errors))
	for _, e := range res.Errors {
		details = append(details, e.Error())
	}
	return &Error{Kind: kind, What: what, Details: details}
}
