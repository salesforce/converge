package test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/host"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/specschema"
)

// TestStdcelFixtureValidatesAgainstSchema is the same drift guard as
// TestCelbomFixtureValidatesAgainstSchema, for the datadriven demo's REUSE of the
// shipped stdcel (kro-style graph) provider: the demo's graph providerconfig and
// instance resource must satisfy the shipped stdcel.kind.json config_schema /
// spec_schema — the exact validation the API POST runs — so a graph field the CRD
// doesn't declare (config_schema is additionalProperties:false) is caught at unit
// speed instead of as a live 400.
func TestStdcelFixtureValidatesAgainstSchema(t *testing.T) {
	// stdcel's CRD ships WITH the provider (not in a demo testfixtures/ dir), so load
	// its provider dir alongside the demo dirs — the validator must know the stdcel
	// kind for the checks below (and for the negative control to actually reject).
	stdcelDir := filepath.Join(repoRootFromCwd(), "internal", "providers", "stdcel")
	manifests, err := host.LoadKindFixtures(append(fixtureDirs(), stdcelDir)...)
	require.NoError(t, err)
	v := specschema.New(manifests)

	demoDir := filepath.Join(repoRootFromCwd(), "examples", "demos", "datadriven", "testfixtures")

	// BOTH shipped graph providerconfigs — the CUSTOM one (referenced by an instance)
	// and the DEFAULT one (the shared template) — must validate against stdcel's
	// config_schema, exactly the check the API POST runs.
	for _, cfg := range []string{"providerconfig-stdcel.json", "providerconfig-stdcel-default.json"} {
		raw, err := os.ReadFile(filepath.Join(demoDir, cfg))
		require.NoError(t, err)
		var pc struct {
			Kind string          `json:"kind"`
			Spec json.RawMessage `json:"spec"`
		}
		require.NoError(t, json.Unmarshal(raw, &pc))
		require.Equal(t, "stdcel", pc.Kind)
		require.NoError(t, v.ValidateConfig(model.Kind(pc.Kind), 1, pc.Spec),
			cfg+" must satisfy stdcel.kind.json config_schema")
	}

	// BOTH shipped instances' specs (the ${schema.spec.*} inputs) must validate against
	// stdcel's spec_schema (open object — proves the fixtures are well-formed and the
	// kind resolves).
	for _, res := range []string{"resource-stdcel.json", "resource-stdcel-default.json"} {
		raw, err := os.ReadFile(filepath.Join(demoDir, res))
		require.NoError(t, err)
		var r struct {
			Kind string          `json:"kind"`
			Spec json.RawMessage `json:"spec"`
		}
		require.NoError(t, json.Unmarshal(raw, &r))
		require.Equal(t, "stdcel", r.Kind)
		require.NoError(t, v.ValidateSpec(model.Kind(r.Kind), 1, r.Spec),
			res+" must satisfy stdcel.kind.json spec_schema")
	}

	// Negative control: a graph resource with an undeclared property MUST be rejected,
	// proving config_schema enforces additionalProperties:false (so the positive checks
	// are meaningful).
	bogus := json.RawMessage(`{"resources":[{"id":"a","template":{"kind":"k","kind_version":1,"name":"n"},"bogus_field":true}]}`)
	require.Error(t, v.ValidateConfig("stdcel", 1, bogus),
		"stdcel config_schema should reject an undeclared resource property")
}
