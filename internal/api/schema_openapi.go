package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/specschema"
)

// schema_openapi.go: minting the synthetic /docs OpenAPI component names for each
// (kind, version) axis, registering those components on the huma document, the
// /docs display-version resolver, and mapping a validator error onto huma's 400.

// componentName is the synthetic /docs OpenAPI component name for a (kind, version)
// axis. The manifest carries JSON Schema BYTES with no type name, so we mint a
// stable name from the kind + kindVersion + axis, used identically by
// registerOpenAPIComponents (registration) and getKindSchema (the *SchemaRef the
// UI links to at /docs#/schemas/{ref}). v1 is UNSUFFIXED (e.g. "WidgetSpec") so
// existing links stay valid; v2+ append the version ("WidgetSpecV2") so each
// published kindVersion of a kind gets its OWN distinct schema entry in /docs (a v2
// manifest must not clobber v1's schema in the docs tree).
func componentName(kind model.Kind, kindVersion int, axis string) string {
	k := string(kind)
	if k == "" {
		return axis
	}
	name := strings.ToUpper(k[:1]) + k[1:] + axis
	if kindVersion > 1 {
		name += "V" + strconv.Itoa(kindVersion)
	}
	return name
}

// resolveDocKindVersion picks the /docs component kindVersion to reference for a REQUESTED
// kindVersion, given the kind's ascending published kind versions. It returns the highest
// published kindVersion ≤ requested (so ?kindVersion=0/omitted → the lowest published, a
// normal request → its exact version), and clamps an over-large request
// (?kindVersion=99) to the highest published — every /docs ref then names a component
// that actually exists. This is a /docs DISPLAY convenience (which schema component a
// doc link resolves to), NOT a data path — it never invents a v1 default for a kind
// with no published versions; it just echoes the request back so the ref is a plain
// (harmless) miss. published is assumed sorted ascending (kindVersionsForKind
// guarantees it).
func resolveDocKindVersion(requested int, published []int) int {
	if len(published) == 0 {
		return requested
	}
	if requested <= 0 {
		return published[0]
	}
	best := published[0]
	for _, m := range published {
		if m <= requested {
			best = m
		} else {
			break // ascending — no further match possible
		}
	}
	return best
}

// validationError maps a typed *specschema.Error (or any validator error) onto
// huma's 422 Unprocessable Entity — the body PARSED (huma bound it) but its
// content violates the kind's schema, which is 422, not 400 (400 is huma's own
// code for unparseable/mis-typed input). what is the field label
// ("spec"/"config"/"input") for the top-line message.
func validationError(err error, what string, kind model.Kind) error {
	if err == nil {
		return nil
	}
	var ve *specschema.Error
	if errors.As(err, &ve) {
		return huma.Error422UnprocessableEntity(
			fmt.Sprintf("%s for kind %q failed validation", what, kind),
			fmt.Errorf("%v", ve.Details),
		)
	}
	return huma.Error422UnprocessableEntity(err.Error())
}

// registerOpenAPIComponents adds every DECLARED kind's Spec/Status/Config JSON
// Schema as a named schema component on the huma API's OpenAPI document. After
// this runs, the OpenAPI document's #/components/schemas/ table contains entries
// like kind-specific *Spec / *Status / *Config (named via componentName), which
// Stoplight Elements (huma's built-in /docs renderer) lists in its schemas tree.
//
// It is fed the applied manifests, NOT the live registry, so a kind's shape
// shows in /docs whether or not its provider is up — schema documentation is
// pure data and must not depend on runtime/DB state (a bootstrap-gated kind —
// one that dials a client from its config — would otherwise be invisible until
// its default config exists).
//
// The manifest carries JSON Schema BYTES, so we decode each axis and insert the
// *huma.Schema straight into the registry's backing map (Registry.Map()) under
// the synthetic component name — the map registry both serializes from and
// resolves $refs against that same map.
func registerOpenAPIComponents(api huma.API, manifests []model.KindManifest) {
	target := api.OpenAPI().Components.Schemas.Map()
	put := func(kind model.Kind, kindVersion int, axis string, raw json.RawMessage) {
		if s := decodeSchema(raw); s != nil {
			target[componentName(kind, kindVersion, axis)] = s
		}
	}
	for _, m := range manifests {
		put(m.Kind, m.KindVersion, "Spec", m.SpecSchema)
		put(m.Kind, m.KindVersion, "Status", m.StatusSchema)
		put(m.Kind, m.KindVersion, "Config", m.ConfigSchema)
	}
}
