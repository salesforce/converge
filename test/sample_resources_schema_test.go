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

// TestSampleResourceFixturesValidate guards the hand-authored, manual-apply sample
// resources the demos ship (examples/demos/*/testfixtures/resource-<kind>.json for
// the LEAF kinds — account, vpc, route, tgw, fakeapp, fakevpc, fakedb). A user is
// told to `curl ... --data-binary @resource-vpc.json`; if a sample's spec drifts
// from its kind's spec_schema (e.g. vpc.account_id not exactly 12 chars, or a
// required field missing), the POST 400s. This runs the EXACT validator the API
// uses (internal/specschema) on each sample against the SHIPPED CRD — a pure,
// no-DB drift guard catching it at unit speed instead of as a live 400.
//
// Only LEAF samples are checked: composer roots (classicbom/celbom/stdstarlark)
// have their own coverage, and their child specs are validated partially (flowed
// fields skipped) — a manual leaf apply has no composer to flow values, so the
// sample must satisfy the FULL schema, which is exactly what ValidateSpec asserts.
func TestSampleResourceFixturesValidate(t *testing.T) {
	manifests, err := host.LoadKindFixtures(fixtureDirs()...)
	require.NoError(t, err)
	v := specschema.New(manifests)

	root := repoRootFromCwd()
	// (demo dir, kind, sample file) — the leaf samples a user applies by hand.
	samples := []struct{ dir, kind, file string }{
		{"classic", "account", "resource-account.json"},
		{"classic", "vpc", "resource-vpc.json"},
		{"classic", "tgw", "resource-tgw.json"},
		{"classic", "route", "resource-route.json"},
		{"datadriven", "fakevpc", "resource-fakevpc.json"},
		{"datadriven", "fakedb", "resource-fakedb.json"},
		{"datadriven", "fakeapp", "resource-fakeapp.json"},
		{"datadriven", "faketerraform", "resource-faketerraform.json"},
		{"datadriven", "fakek8sjob", "resource-fakek8sjob.json"},
	}

	for _, s := range samples {
		t.Run(s.kind, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(root, "examples", "demos", s.dir, "testfixtures", s.file))
			require.NoError(t, err)
			var rsc struct {
				Kind        string          `json:"kind"`
				KindVersion int             `json:"kind_version"`
				Spec        json.RawMessage `json:"spec"`
			}
			require.NoError(t, json.Unmarshal(raw, &rsc))
			require.Equal(t, s.kind, rsc.Kind, "%s: kind field mismatch", s.file)
			// kind_version is REQUIRED and explicit (>= 1) in every fixture — no implicit
			// v1 default. Guard it here so a fixture that drops it fails at unit speed.
			require.GreaterOrEqual(t, rsc.KindVersion, 1, "%s must carry an explicit kind_version (>= 1)", s.file)

			// The sample's spec must satisfy the FULL kind spec_schema at the fixture's
			// explicit version — exactly the check POST /api/v1/resources runs. A manual
			// leaf apply has no composer to flow upstream values, so every required field
			// must be present.
			require.NoError(t, v.ValidateSpec(model.Kind(rsc.Kind), rsc.KindVersion, rsc.Spec),
				"%s must satisfy kind-%s.json spec_schema (a manual apply would 400 otherwise)", s.file, s.kind)
		})
	}

	// The classic demo's versioning walkthrough (README "Versioning") ships a set of
	// per-(kind, kindVersion) fixtures a user applies by hand: vpc/v1 and vpc/v2 resources,
	// per-kindVersion provider-config DEFAULTs and CUSTOM overrides, and a deliberate
	// (kind, kindVersion)-mismatch resource. This guards each against the SHIPPED CRD at the
	// right kindVersion — the same validators the API edge runs — so a copy-paste from the
	// README never 400s/422s on drift, and it pins the config-kindVersion-mismatch invariant
	// (a v1 resource may not attach a v2 config) at demo-fixture level.
	t.Run("versioning_walkthrough", func(t *testing.T) {
		verDir := filepath.Join(root, "examples", "demos", "classic", "testfixtures", "versioning")

		// resource specs — validated at the kindVersion the fixture pins.
		resFixtures := []struct {
			file        string
			kindVersion int
		}{
			{"resource-vpc-v1-tagged.json", 1},          // v1 + optional tags
			{"resource-vpc-v1-with-custom.json", 1},     // v1 attaching a MATCHING v1 config
			{"resource-vpc-v1-config-mismatch.json", 1}, // v1 whose spec is valid; the CONFIG ref is the bug
			{"resource-vpc-flip-to-v2.json", 2},         // the live v1→v2 flip target (complete v2 spec)
		}
		for _, r := range resFixtures {
			raw, err := os.ReadFile(filepath.Join(verDir, r.file))
			require.NoError(t, err, "read %s", r.file)
			var rsc struct {
				Kind string          `json:"kind"`
				Spec json.RawMessage `json:"spec"`
			}
			require.NoError(t, json.Unmarshal(raw, &rsc))
			require.NoError(t, v.ValidateSpec(model.Kind(rsc.Kind), r.kindVersion, rsc.Spec),
				"%s must satisfy vpc/v%d spec_schema", r.file, r.kindVersion)
		}
		// The demo's v2 resource lives at the top of the demo testfixtures dir.
		{
			raw, err := os.ReadFile(filepath.Join(root, "examples", "demos", "classic", "testfixtures", "resource-vpc-v2.json"))
			require.NoError(t, err)
			var rsc struct {
				Kind string          `json:"kind"`
				Spec json.RawMessage `json:"spec"`
			}
			require.NoError(t, json.Unmarshal(raw, &rsc))
			require.NoError(t, v.ValidateSpec(model.Kind(rsc.Kind), 2, rsc.Spec),
				"resource-vpc-v2.json must satisfy vpc/v2 spec_schema")
		}

		// provider-config specs — validated against the (kind, kindVersion)'s config_schema.
		// v1's config_schema has flow_logs_bucket; v2's adds default_region — so a v2
		// config carrying default_region is only valid at kindVersion 2.
		cfgFixtures := []struct {
			file        string
			kindVersion int
		}{
			{"providerconfig-vpc-v1-default.json", 1},
			{"providerconfig-vpc-v1-custom.json", 1},
			{"providerconfig-vpc-v2-default.json", 2},
			{"providerconfig-vpc-v2-custom.json", 2},
		}
		cfgKindVersionByName := map[string]int{}
		for _, c := range cfgFixtures {
			raw, err := os.ReadFile(filepath.Join(verDir, c.file))
			require.NoError(t, err, "read %s", c.file)
			var cfg struct {
				Kind        string          `json:"kind"`
				KindVersion int             `json:"kind_version"`
				Name        string          `json:"name"`
				Spec        json.RawMessage `json:"spec"`
			}
			require.NoError(t, json.Unmarshal(raw, &cfg))
			require.Equal(t, c.kindVersion, cfg.KindVersion, "%s: kindVersion field mismatch", c.file)
			require.NoError(t, v.ValidateConfig(model.Kind(cfg.Kind), c.kindVersion, cfg.Spec),
				"%s must satisfy vpc/v%d config_schema", c.file, c.kindVersion)
			cfgKindVersionByName[cfg.Name] = cfg.KindVersion
		}

		// The config-kindVersion-mismatch invariant at demo-fixture level: a resource may
		// only attach a config on its OWN (kind, kindVersion). The mismatch fixture is a
		// vpc/v1 resource referencing a vpc/v2 config — the exact case applySpecOnce
		// rejects (ErrConfigKindMismatch). Assert the fixture actually encodes the
		// mismatch (else the demo would silently stop demonstrating the guard), and
		// that the matching fixture lines up.
		readResRef := func(file string) (kindVersion int, ref string) {
			raw, err := os.ReadFile(filepath.Join(verDir, file))
			require.NoError(t, err, "read %s", file)
			var rsc struct {
				KindVersion       int    `json:"kind_version"`
				ProviderConfigRef string `json:"provider_config_ref"`
			}
			require.NoError(t, json.Unmarshal(raw, &rsc))
			return rsc.KindVersion, rsc.ProviderConfigRef
		}
		okKindVersion, okRef := readResRef("resource-vpc-v1-with-custom.json")
		require.Equal(t, okKindVersion, cfgKindVersionByName[okRef],
			"resource-vpc-v1-with-custom.json must attach a config on its OWN kindVersion (the guard would else reject a valid demo apply)")
		badKindVersion, badRef := readResRef("resource-vpc-v1-config-mismatch.json")
		require.NotEqual(t, badKindVersion, cfgKindVersionByName[badRef],
			"resource-vpc-v1-config-mismatch.json must attach a config on a DIFFERENT kindVersion (else it stops demonstrating ErrConfigKindMismatch)")
	})
}
