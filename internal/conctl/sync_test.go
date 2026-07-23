package conctl

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// writeFile is a tiny helper: create dir + write a manifest under the temp tree.
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestIsManifestFile(t *testing.T) {
	yes := []string{"a.json", "b.yaml", "c.yml", "RES.JSON", "x.kind.json"}
	no := []string{"converge.yaml.bak", "readme.md", "script.sh", "noext", "a.txt"}
	for _, n := range yes {
		if !isManifestFile(n) {
			t.Errorf("isManifestFile(%q) = false, want true", n)
		}
	}
	for _, n := range no {
		if isManifestFile(n) {
			t.Errorf("isManifestFile(%q) = true, want false", n)
		}
	}
}

func TestApplyRankOrder(t *testing.T) {
	// CRD < providerconfig < reactorbinding < resource — the dependency order sync
	// applies in. Assert the strict ordering, not the exact numbers.
	if !(applyRank(typeManifest) < applyRank(typeProviderConfig) &&
		applyRank(typeProviderConfig) < applyRank(typeReactorBinding) &&
		applyRank(typeReactorBinding) < applyRank(typeResource)) {
		t.Errorf("applyRank order wrong: manifest=%d providerconfig=%d reactorbinding=%d resource=%d",
			applyRank(typeManifest), applyRank(typeProviderConfig), applyRank(typeReactorBinding), applyRank(typeResource))
	}
}

func TestStampAppLabel(t *testing.T) {
	// Stamps the reserved key, overwriting any user value, preserving other labels,
	// and returns the (kind, name).
	in := []byte(`{"type":"resource","kind":"vpc","name":"prod","labels":{"team":"x","converge.sh/app":"WRONG"},"spec":{}}`)
	out, ref, err := stampAppLabel(in, "myapp")
	if err != nil {
		t.Fatal(err)
	}
	if ref != (resourceRef{kind: "vpc", name: "prod"}) {
		t.Errorf("ref = %+v, want vpc/prod", ref)
	}
	var doc struct {
		Labels map[string]string `json:"labels"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Labels[syncAppLabel] != "myapp" {
		t.Errorf("app label = %q, want myapp (must overwrite the user's WRONG value)", doc.Labels[syncAppLabel])
	}
	if doc.Labels["team"] != "x" {
		t.Error("stamp must preserve other labels")
	}
}

func TestStampAppLabelRejectsMissingKindName(t *testing.T) {
	if _, _, err := stampAppLabel([]byte(`{"type":"resource","kind":"vpc","spec":{}}`), "a"); err == nil {
		t.Error("a resource missing name must be rejected")
	}
	if _, _, err := stampAppLabel([]byte(`{"type":"resource","name":"p","spec":{}}`), "a"); err == nil {
		t.Error("a resource missing kind must be rejected")
	}
}

func TestCollectManifestsClassifiesByType(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "crd.json", `{"type":"manifest","kind":"vpc","kind_version":1}`)
	writeFile(t, root, "cfg.json", `{"type":"providerconfig","name":"c","kind":"vpc","kind_version":1,"spec":{}}`)
	writeFile(t, filepath.Join(root, "app"), "converge.yaml", "app: web\n")
	writeFile(t, filepath.Join(root, "app"), "r.json", `{"type":"resource","kind":"vpc","name":"prod","spec":{}}`)

	manifests, appDirs, err := collectManifests(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 3 {
		t.Fatalf("got %d manifests, want 3", len(manifests))
	}
	if !appDirs["web"] {
		t.Error("app scope 'web' not discovered")
	}
	byType := map[objectType]syncManifest{}
	for _, m := range manifests {
		byType[m.typ] = m
	}
	// Only the resource is app-scoped + carries the ref.
	if byType[typeResource].app != "web" {
		t.Errorf("resource app = %q, want web", byType[typeResource].app)
	}
	if byType[typeManifest].app != "" || byType[typeProviderConfig].app != "" {
		t.Error("CRD/providerconfig must not be app-scoped")
	}
}

func TestCollectManifestsRequiresType(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "r.json", `{"kind":"vpc","name":"prod","spec":{}}`) // no "type"
	if _, _, err := collectManifests(root); err == nil {
		t.Error("a manifest with no \"type\" field must be an error")
	}
}

func TestCollectManifestsSkipsNonManifests(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "converge.yaml", "app: x\n") // scope config, not a manifest
	writeFile(t, root, "README.md", "hi")           // not a manifest
	writeFile(t, root, "r.json", `{"type":"resource","kind":"vpc","name":"p","spec":{}}`)
	manifests, _, err := collectManifests(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 {
		t.Fatalf("got %d manifests, want 1 (converge.yaml + README skipped)", len(manifests))
	}
}

// TestCollectManifestsScopeIsolation is the BUG-1 guard: a relative-ish root with a
// DECOY converge.yaml in the PARENT must NOT leak into the tree — a file under root
// with its own scope uses that scope, and the ancestor walk stops at root, never
// climbing to the parent decoy.
func TestCollectManifestsScopeIsolation(t *testing.T) {
	base := t.TempDir()
	// parent/converge.yaml is the DECOY, ABOVE the sync root.
	writeFile(t, base, "converge.yaml", "app: DECOY\n")
	root := filepath.Join(base, "root")
	writeFile(t, filepath.Join(root, "app"), "converge.yaml", "app: real\n")
	writeFile(t, filepath.Join(root, "app"), "r.json", `{"type":"resource","kind":"vpc","name":"scoped","spec":{}}`)
	// a resource directly under root with NO scope → app "" (unscoped), NOT the decoy.
	writeFile(t, root, "u.json", `{"type":"resource","kind":"vpc","name":"unscoped","spec":{}}`)

	manifests, appDirs, err := collectManifests(root)
	if err != nil {
		t.Fatal(err)
	}
	if appDirs["DECOY"] {
		t.Error("the parent DECOY converge.yaml leaked into the tree (scope must stop at root)")
	}
	if !appDirs["real"] {
		t.Error("the in-tree 'real' scope was not discovered")
	}
	for _, m := range manifests {
		if m.ref.name == "unscoped" && m.app != "" {
			t.Errorf("resource above any in-tree converge.yaml must be unscoped, got app=%q", m.app)
		}
		if m.ref.name == "scoped" && m.app != "real" {
			t.Errorf("scoped resource app = %q, want real", m.app)
		}
	}
}

func TestCollectManifestsErrors(t *testing.T) {
	if _, _, err := collectManifests(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Error("missing directory must error")
	}
	// empty app in converge.yaml
	root := t.TempDir()
	writeFile(t, root, "converge.yaml", `app: ""`+"\n")
	writeFile(t, root, "r.json", `{"type":"resource","kind":"vpc","name":"p","spec":{}}`)
	if _, _, err := collectManifests(root); err == nil {
		t.Error("empty app in converge.yaml must error")
	}
}
