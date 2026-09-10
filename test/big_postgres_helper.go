package test

// Helpers for the 1M-resource real-BOM stress test:
//
//   - loadRealBOM: reads testfixtures/resource-classicbom.json from the
//     repo root and unwraps its ResourceManifest into the classicbom.ClassicBOMSpec.
//
//   - mustRepoRoot: walks up from CWD to find go.mod so the test
//     locates the fixture no matter where it's invoked from.
//
// The big-postgres container itself now lives in
// replica_postgres_helper.go (a primary + streaming-replica pair).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/internal/model"
	"github.com/stretchr/testify/require"
)

// loadRealBOM reads the classic demo's resource-classicbom.json from the repo
// root and returns the ClassicBOMSpec carried in its manifest.spec. The file is a
// model.ResourceManifest ({kind, name, labels, spec}); the 1M test only
// needs the spec body (real data shape: 58 FDs, real names) to extend
// and re-apply, so we unwrap it here.
func loadRealBOM(t *testing.T) classicbom.ClassicBOMSpec {
	t.Helper()
	root := mustRepoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "examples", "demos", "classic", "testfixtures", "resource-classicbom.json"))
	require.NoError(t, err)
	var manifest model.ResourceManifest
	require.NoError(t, json.Unmarshal(data, &manifest))
	var spec classicbom.ClassicBOMSpec
	require.NoError(t, json.Unmarshal(manifest.Spec, &spec))
	return spec
}

// mustRepoRoot walks up from CWD until it finds go.mod. Lets the test
// run from any directory and still locate testfixtures/resource-classicbom.json.
func mustRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	root := wd
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			return root
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatalf("could not find go.mod walking up from %s", wd)
		}
		root = parent
	}
}
