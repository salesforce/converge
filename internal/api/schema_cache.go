package api

import (
	"encoding/json"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/kindschema"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/specschema"
)

// schema_cache.go: the per-kind JSON-Schema cache + the server-side validate*
// delegators. OpenAPI component naming/registration lives in schema_openapi.go;
// the /api/kinds* read endpoints in handlers_schema.go.
//
// schemaCache holds the per-kind JSON Schemas (decoded from the applied
// manifests) and the shared validator, built ONCE from the declared manifest
// set. A kind's shape is pure data carried in its manifest, so the cache never
// depends on a live registry or a successful provider bring-up. No locks, no
// atomics, no generation gate — the surface is rebuilt wholesale when the
// manifest set changes (SetDeclaredSchemas).

type schemaCache struct {
	specs    map[model.Kind]*huma.Schema
	statuses map[model.Kind]*huma.Schema
	configs  map[model.Kind]*huma.Schema
	// validator is the SHARED server-side validator (internal/specschema) the
	// store/composer/duty also use, so the HTTP boundary and every internal
	// author path validate through ONE engine with one rule-set — no drift. The
	// schemaCache still holds the huma.Schema maps above for /docs + /api/kinds
	// SCHEMA SERVING; only the validate* methods delegate here.
	validator specschema.Validator
}

// newSchemaCache builds the JSON-Schema cache entirely from the applied kind
// manifests — Spec/Status/Config. Each manifest carries JSON Schema BYTES, so
// each axis is decoded into a *huma.Schema the same way the validator does. The
// manifest stores no per-verb input schema, so verb input is not cached. Every
// shape is pure data carried in the manifest for EVERY declared kind, so the
// cache never depends on the live registry.
func newSchemaCache(manifests []model.KindManifest) *schemaCache {
	c := &schemaCache{
		specs:    map[model.Kind]*huma.Schema{},
		statuses: map[model.Kind]*huma.Schema{},
		configs:  map[model.Kind]*huma.Schema{},
	}
	for _, m := range manifests {
		if s := decodeSchema(m.SpecSchema); s != nil {
			c.specs[m.Kind] = s
		}
		if s := decodeSchema(m.StatusSchema); s != nil {
			c.statuses[m.Kind] = s
		}
		// ConfigSchema is the shape of the providerconfig document this kind
		// consumes (dump destination, broker URL, …). Publishing it lets an
		// operator author a correct default/override config for the kind.
		if s := decodeSchema(m.ConfigSchema); s != nil {
			c.configs[m.Kind] = s
		}
	}
	// The shared validator is built from the SAME manifests, so HTTP validation
	// and the store/composer data-gateway validation are identical.
	c.validator = specschema.New(manifests)
	return c
}

// decodeSchema turns a manifest's JSON Schema bytes into a ready *huma.Schema
// (PrecomputeMessages, so it feeds huma.Validate / serializes for /docs).
// Empty/nil bytes or malformed JSON → nil (the kind opted out of that axis).
func decodeSchema(raw json.RawMessage) *huma.Schema {
	return kindschema.DecodeSchema(raw)
}

func (c *schemaCache) specSchema(kind model.Kind) *huma.Schema {
	if c == nil {
		return nil
	}
	return c.specs[kind]
}

func (c *schemaCache) statusSchema(kind model.Kind) *huma.Schema {
	if c == nil {
		return nil
	}
	return c.statuses[kind]
}

func (c *schemaCache) configSchema(kind model.Kind) *huma.Schema {
	if c == nil {
		return nil
	}
	return c.configs[kind]
}

// validateSpec / validateConfig / validateVerbInput delegate to the SHARED
// validator (internal/specschema) so the HTTP boundary validates with the exact
// same engine + rules as the store/composer data gateway — no drift. They map
// the typed *specschema.Error onto huma's 400, preserving the response shape. A
// nil cache (no schemas wired) validates nothing.
func (c *schemaCache) validateSpec(kind model.Kind, kindVersion int, raw json.RawMessage) error {
	if c == nil || c.validator == nil {
		return nil
	}
	return validationError(c.validator.ValidateSpec(kind, kindVersion, raw), "spec", kind)
}

func (c *schemaCache) validateConfig(kind model.Kind, kindVersion int, raw json.RawMessage) error {
	if c == nil || c.validator == nil {
		return nil
	}
	return validationError(c.validator.ValidateConfig(kind, kindVersion, raw), "config", kind)
}

func (c *schemaCache) validateVerbInput(kind model.Kind, verb string, raw json.RawMessage) error {
	if c == nil || c.validator == nil {
		return nil
	}
	return validationError(c.validator.ValidateVerbInput(kind, verb, raw), "input", kind)
}
