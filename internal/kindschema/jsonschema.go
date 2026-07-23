// Package kindschema is the server-side schema tooling for kind manifests: it
// derives a Go type's JSON Schema (authoring), decodes stored schema bytes back
// into a validator (the round-trip huma can't do on its own output), hashes a
// manifest's schemas for the stability gate, and lints an in-place re-publish for
// backward compatibility. It operates ON model.KindManifest but is NOT part of
// the public worker SDK — only the control-plane internals (api, store,
// specschema) consume it. A provider author supplies schema BYTES in the applied
// manifest, so nothing here is on a worker's path.
package kindschema

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"

	"github.com/danielgtaylor/huma/v2"
	"github.com/salesforce/converge/internal/model"
)

// DecodeSchema parses JSON Schema bytes (as produced by Of/SchemaJSON) back into a
// *huma.Schema ready for validation (PrecomputeMessages called). It is the READER
// half of the schema round-trip: huma.Schema.MarshalJSON emits a nullable field's
// type as a UNION ["T","null"], but huma.Schema.Type is a plain string with no
// custom UnmarshalJSON, so a naive json.Unmarshal fails on its OWN output.
// DecodeSchema normalizes every `"type":[...,"null"]` to `"type":"T","nullable":true`
// (recursively, in properties/items) before unmarshaling, so a manifest's stored
// schema validates correctly. Returns nil for empty/garbage input.
func DecodeSchema(raw json.RawMessage) *huma.Schema {
	if len(raw) == 0 {
		return nil
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	normalizeNullableType(doc)
	fixed, err := json.Marshal(doc)
	if err != nil {
		return nil
	}
	var s huma.Schema
	if err := json.Unmarshal(fixed, &s); err != nil {
		return nil
	}
	s.PrecomputeMessages()
	return &s
}

// normalizeNullableType collapses a JSON-Schema type UNION (["T","null"]) into a
// scalar type + nullable:true, recursively, so huma.Schema (scalar Type) can
// unmarshal its own marshaled output.
func normalizeNullableType(node map[string]any) {
	if t, ok := node["type"].([]any); ok {
		var scalar string
		nullable := false
		for _, v := range t {
			if s, _ := v.(string); s == "null" {
				nullable = true
			} else if s != "" {
				scalar = s
			}
		}
		if scalar != "" {
			node["type"] = scalar
			if nullable {
				node["nullable"] = true
			}
		}
	}
	if props, ok := node["properties"].(map[string]any); ok {
		for _, v := range props {
			if child, ok := v.(map[string]any); ok {
				normalizeNullableType(child)
			}
		}
	}
	if items, ok := node["items"].(map[string]any); ok {
		normalizeNullableType(items)
	}
	if ap, ok := node["additionalProperties"].(map[string]any); ok {
		normalizeNullableType(ap)
	}
}

// SchemaHashOf is the canonical content hash of a manifest's three JSON Schemas
// (spec/status/config), used by the schema-stability gate: the API rejects a
// schema change to an existing kind unless the operator explicitly accepts the
// new hash. Shared by the API gate and the store's persisted schema_hash so both
// compute the SAME value for the same schemas.
func SchemaHashOf(m model.KindManifest) string {
	h := sha256.New()
	for _, s := range [][]byte{m.SpecSchema, m.StatusSchema, m.ConfigSchema} {
		h.Write(s)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Of generates the JSON Schema (RFC draft 2020-12) for a Go type T, as the raw
// bytes a KindManifest stores for spec/status/config. It is the AUTHORING-time
// canonical schema: a manifest author writes their typed Go structs and calls
// Of[MySpec]() to embed the schema in the manifest they apply — so the runtime
// stores schema bytes, never a reflect.Type, and the schema declared is
// byte-identical to the one the core validator (internal/specschema) builds from
// the same huma reflector. The reflect-based path stays a build-time convenience
// only.
//
// Returns nil for the zero/empty struct{} type so a kind can opt OUT of an axis
// (an untyped schema).
func Of[T any]() json.RawMessage {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		return nil
	}
	// An empty struct (struct{}{}) means "no typed schema" — opt out.
	if t.Kind() == reflect.Struct && t.NumField() == 0 {
		return nil
	}
	return SchemaJSON(t)
}

// SchemaJSON marshals the huma JSON Schema for a reflect.Type to bytes using the
// SAME registry construction as internal/specschema.New, so an authored manifest
// schema and a validator-built schema for the same type are identical. Exposed so
// a caller holding a reflect.Type can derive the manifest schema without
// re-deriving the type.
func SchemaJSON(t reflect.Type) json.RawMessage {
	if t == nil {
		return nil
	}
	hr := huma.NewMapRegistry("#/components/schemas/", huma.DefaultSchemaNamer)
	s := huma.SchemaFromType(hr, t)
	b, err := json.Marshal(s)
	if err != nil {
		// SchemaFromType produces a well-formed *huma.Schema; a marshal failure
		// would be a programming error in huma, not user input. Return nil rather
		// than panic so an authoring helper never crashes a boot.
		return nil
	}
	return b
}
