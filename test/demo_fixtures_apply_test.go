package test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/host"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/specschema"
)

// TestAllDemoFixturesApplyClean is the no-DB proof that EVERY demo applies clean
// under the "kind_version is REQUIRED and explicit — no implicit v1 default" model.
// It mirrors what each demo's `just demo` does with `conctl apply`, at unit speed:
//
//  1. Loads every *.kind.json CRD across all demo dirs through host.LoadKindFixtures
//     — the loader now REJECTS a manifest with no kind_version, so a green load
//     proves every demo CRD carries an explicit version.
//  2. Asserts every resource-*.json and providerconfig-*.json across the demo dirs
//     carries an explicit kind_version (>= 1) and VALIDATES against its declared
//     kind's schema AT THAT VERSION — the exact checks the API edge runs on apply,
//     so a `conctl apply` of any demo fixture would not 400/422 on version/schema
//     drift.
//
// Composer roots (classicbom/celbom/stdcel/stdstarlark/stdio-composer) emit their
// children WITH the flowed fields absent, so their child specs are validated
// partially elsewhere; here we validate the manually-applied fixtures (roots +
// leaves + provider configs) against the FULL schema, which is what a hand apply
// faces. The end-to-end compose paths themselves are covered by the broker E2E and
// provider unit tests.
func TestAllDemoFixturesApplyClean(t *testing.T) {
	root := repoRootFromCwd()
	demoDirs, err := filepath.Glob(filepath.Join(root, "examples", "demos", "*", "testfixtures"))
	require.NoError(t, err)
	require.NotEmpty(t, demoDirs, "expected demo fixture dirs under examples/demos/*/testfixtures")
	// The stdio/stdshell/stdcel/stdstarlark/stdterraform providers ship their CRD next
	// to the provider code (internal/providers/*/<kind>.kind.json), not under the demo's
	// testfixtures/ — the demo justfiles apply CRDs from BOTH. Load both so the
	// validator knows every (kind, version) a demo publishes.
	providerCRDDirs, err := filepath.Glob(filepath.Join(root, "internal", "providers", "*"))
	require.NoError(t, err)
	crdDirs := append(append([]string{}, demoDirs...), providerCRDDirs...)

	// (1) Load EVERY demo + provider CRD through the required-version loader. A missing
	// kind_version is a hard error there, so this alone proves every CRD is explicitly
	// versioned; it also builds the validator keyed by (kind, version). The uniform
	// <kind>.kind.json naming means the loader's single glob catches every provider CRD
	// (the fixed-kind providers no longer need a separate singular pass).
	manifests, err := host.LoadKindFixtures(crdDirs...)
	require.NoError(t, err, "every demo/provider *.kind.json must load (explicit kind_version required)")
	v := specschema.New(manifests)

	// The set of (kind, version) a manifest was published at — a resource/config
	// fixture may only reference a published pair.
	published := map[string]bool{}
	for _, m := range manifests {
		published[string(m.Kind)+"/"+itoaLocal(m.KindVersion)] = true
	}

	// (2) Walk every resource/providerconfig fixture (recursively, to include the
	// classic versioning/ walkthrough subdir) and assert explicit version + schema.
	for _, dir := range demoDirs {
		_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, werr error) error {
			require.NoError(t, werr)
			if d.IsDir() {
				return nil
			}
			base := filepath.Base(path)
			isResource := strings.HasPrefix(base, "resource-")
			isConfig := strings.HasPrefix(base, "providerconfig-")
			if !isResource && !isConfig {
				return nil
			}
			t.Run(relFromRoot(root, path), func(t *testing.T) {
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				var doc struct {
					Kind        string          `json:"kind"`
					KindVersion int             `json:"kind_version"`
					Spec        json.RawMessage `json:"spec"`
				}
				require.NoError(t, json.Unmarshal(raw, &doc))
				// kind_version is REQUIRED and explicit (>= 1) — no implicit v1 default.
				require.GreaterOrEqual(t, doc.KindVersion, 1,
					"%s must carry an explicit kind_version (>= 1)", base)
				// The fixture must reference a (kind, version) the demo actually publishes.
				require.True(t, published[doc.Kind+"/"+itoaLocal(doc.KindVersion)],
					"%s references %s/v%d, which has no published CRD in the demo dirs",
					base, doc.Kind, doc.KindVersion)
				// A providerconfig validates against the kind's config_schema; a resource
				// against its spec_schema — both at the fixture's explicit version, exactly
				// as the API edge does on apply. (A composer root's own spec is validated
				// here too; its emitted children are validated partially elsewhere.)
				if isConfig {
					require.NoError(t, v.ValidateConfig(model.Kind(doc.Kind), doc.KindVersion, doc.Spec),
						"%s must satisfy %s/v%d config_schema (conctl apply would 400 otherwise)", base, doc.Kind, doc.KindVersion)
				} else {
					require.NoError(t, v.ValidateSpec(model.Kind(doc.Kind), doc.KindVersion, doc.Spec),
						"%s must satisfy %s/v%d spec_schema (conctl apply would 400 otherwise)", base, doc.Kind, doc.KindVersion)
				}
			})
			return nil
		})
	}
}

// itoaLocal is a tiny int→string for building (kind, version) keys without pulling
// in strconv at the call sites.
func itoaLocal(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [12]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// relFromRoot renders a repo-relative path for a readable subtest name.
func relFromRoot(root, path string) string {
	if r, err := filepath.Rel(root, path); err == nil {
		return r
	}
	return filepath.Base(path)
}
