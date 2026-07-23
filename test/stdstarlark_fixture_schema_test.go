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

// TestStdstarlarkFixtureValidatesAgainstSchema is the drift guard for the datadriven
// demo's REUSE of the shipped stdstarlark provider: the demo's providerconfig spec
// (the policies) and the instance resource's spec (the BOM) must satisfy the shipped
// stdstarlark.kind.json config_schema / spec_schema — the exact validation the API
// POST runs — so a fixture that would 400 in prod is caught at unit speed. (Both
// schemas are open objects, so this mainly proves the fixtures are well-formed and the
// kind resolves; the .star program's OUTPUT is validated by the stdstarlark package's
// own TestDemoBundleComposesBOM.)
func TestStdstarlarkFixtureValidatesAgainstSchema(t *testing.T) {
	// stdstarlark's CRD ships WITH the provider (not in a demo testfixtures/ dir), so
	// load its provider dir alongside the demo dirs — the validator must know the kind.
	stdstarlarkDir := filepath.Join(repoRootFromCwd(), "internal", "providers", "stdstarlark")
	manifests, err := host.LoadKindFixtures(append(fixtureDirs(), stdstarlarkDir)...)
	require.NoError(t, err)
	v := specschema.New(manifests)

	demoDir := filepath.Join(repoRootFromCwd(), "examples", "demos", "datadriven", "testfixtures")

	// The providerconfig spec (the policies) must validate against config_schema.
	pcRaw, err := os.ReadFile(filepath.Join(demoDir, "providerconfig-stdstarlark.json"))
	require.NoError(t, err)
	var pc struct {
		Kind string          `json:"kind"`
		Spec json.RawMessage `json:"spec"`
	}
	require.NoError(t, json.Unmarshal(pcRaw, &pc))
	require.Equal(t, "stdstarlark", pc.Kind)
	require.NoError(t, v.ValidateConfig(model.Kind(pc.Kind), 1, pc.Spec),
		"providerconfig-stdstarlark.json must satisfy stdstarlark.kind.json config_schema")

	// The instance resource's spec (the BOM) must validate against spec_schema.
	resRaw, err := os.ReadFile(filepath.Join(demoDir, "resource-stdstarlark.json"))
	require.NoError(t, err)
	var res struct {
		Kind string          `json:"kind"`
		Spec json.RawMessage `json:"spec"`
	}
	require.NoError(t, json.Unmarshal(resRaw, &res))
	require.Equal(t, "stdstarlark", res.Kind)
	require.NoError(t, v.ValidateSpec(model.Kind(res.Kind), 1, res.Spec),
		"resource-stdstarlark.json must satisfy stdstarlark.kind.json spec_schema")
}
