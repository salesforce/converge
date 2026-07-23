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

// TestCelbomFixtureValidatesAgainstSchema closes the coverage gap that hid the
// depends_on schema-drift bug: the value-flow integration test seeds the rule set
// via UpsertProviderConfig (store layer), BYPASSING the API's config_schema
// validation — so a field the Go struct/fixture used but the CRD's config_schema
// didn't declare (additionalProperties:false) slipped through tests yet 400'd in
// production.
//
// This test runs the EXACT validator the API uses (internal/specschema) on the
// SHIPPED providerconfig-celbom.json against the SHIPPED celbom.kind.json
// config_schema. It is a pure, no-DB drift guard: if the rule struct grows a field
// that isn't added to the CRD schema (or vice-versa), this fails — catching the
// drift at unit speed instead of as a live 400.
func TestCelbomFixtureValidatesAgainstSchema(t *testing.T) {
	// Build the validator from ALL shipped CRDs across the core testfixtures/ AND
	// every example-demo fixture dir (fixtureDirs() = the same set the test seed +
	// the demo's KIND_FIXTURES_DIR bootstrap), so celbom's config_schema — which now
	// lives in examples/demos/datadriven/testfixtures/ — is loaded as in production.
	manifests, err := host.LoadKindFixtures(fixtureDirs()...)
	require.NoError(t, err)
	v := specschema.New(manifests)

	// Pull the providerconfig fixture's spec (the rule set) — the bytes that get
	// POSTed and validated against celbom's config_schema — from the datadriven demo.
	raw, err := os.ReadFile(filepath.Join(repoRootFromCwd(),
		"examples", "demos", "datadriven", "testfixtures", "providerconfig-celbom.json"))
	require.NoError(t, err)
	var pc struct {
		Kind string          `json:"kind"`
		Spec json.RawMessage `json:"spec"`
	}
	require.NoError(t, json.Unmarshal(raw, &pc))
	require.Equal(t, "celbom", pc.Kind)

	// The shipped rule set (incl. the depends_on rule) must validate against the
	// shipped CRD config_schema — exactly the check the API POST runs.
	require.NoError(t, v.ValidateConfig(model.Kind(pc.Kind), 1, pc.Spec),
		"providerconfig-celbom.json must satisfy celbom.kind.json config_schema "+
			"(a celbom rule field missing from the CRD config_schema would 400 in prod)")

	// Negative control: a rule with an undeclared property MUST be rejected, proving
	// the schema actually enforces additionalProperties:false (so the positive check
	// above is meaningful, not a schema that accepts anything).
	bogus := json.RawMessage(`{"rules":[{"name":"x","child":{"kind":"noop","kind_version":1,"name":"n"},"bogus_field":true}]}`)
	require.Error(t, v.ValidateConfig("celbom", 1, bogus),
		"celbom config_schema should reject an undeclared rule property")
}
