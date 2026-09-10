package stdstarlark

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/salesforce/converge/sdk-go/converge"
)

// demoBundleDir is the datadriven demo's stdstarlark .star program dir.
const demoBundleDir = "../../../examples/demos/datadriven/testfixtures/stdstarlark"

// TestDemoBundleComposesBOM guards the datadriven demo's REUSE of this provider: it
// zips the shipped multi-file .star program (compose.star + policies/*.star +
// emit.star), runs it against the demo's BOM spec, and asserts the per-team fan-out —
// 5 FDs × 5 teams × 2 policies × (3 or 2 children) = 125 children + 75 edges — via the
// GENERIC compose(spec, config) contract. If the .star program, the policies, or a
// leaf's fields drift, this fails at `go test` instead of only in a live `just demo`.
func TestDemoBundleComposesBOM(t *testing.T) {
	dir := filepath.FromSlash(demoBundleDir)
	bundle := zipDir(t, dir)
	// Policies live in the providerconfig SPEC (config), the .star program in the bundle.
	config := readSpec(t, filepath.FromSlash("../../../examples/demos/datadriven/testfixtures/providerconfig-stdstarlark.json"))
	bomSpec := readSpec(t, filepath.FromSlash("../../../examples/demos/datadriven/testfixtures/resource-stdstarlark.json"))

	c := composer{
		defaultData: bundle,
		defaultSpec: config,
	}
	out, err := c.React(t.Context(), converge.ReactionRequest{
		Reaction: "compose",
		Trigger:  converge.TriggerSpecChange,
		Resource: converge.Resource{Kind: Kind, Name: "demo-bom", Spec: bomSpec},
		Env:      &converge.Env{},
	})
	if err != nil {
		t.Fatalf("compose demo BOM: %v", err)
	}

	// 25 teams × 2 policies: the "app" policy emits 3 children (app, vpc, db) + 2
	// edges; the "pipeline" policy emits 2 children (job, tf) + 1 edge. So per team:
	// 5 children + 3 edges → 125 children + 75 edges across 25 teams.
	if len(out.Children) != 125 {
		t.Fatalf("want 125 children, got %d", len(out.Children))
	}
	if len(out.Edges) != 75 {
		t.Fatalf("want 75 edges, got %d", len(out.Edges))
	}

	// Spot-check one team's app DAG + the cross-field pipeline flow, and that every
	// child carries the composed_by=stdstarlark label + the sstar- prefix.
	byName := map[string]converge.ChildSpec{}
	for _, ch := range out.Children {
		byName[ch.Name] = ch
		if ch.Labels["composed_by"] != "stdstarlark" {
			t.Fatalf("child %s missing composed_by=stdstarlark label: %v", ch.Name, ch.Labels)
		}
		if len(ch.Name) < 6 || ch.Name[:6] != "sstar-" {
			t.Fatalf("child %s not sstar- prefixed", ch.Name)
		}
	}
	if _, ok := byName["sstar-fd-00-admiring-turing-stack-app"]; !ok {
		t.Fatalf("missing expected app child; got e.g. %v", sampleNames(out))
	}

	// The pipeline edge flows /image_tag → /built_image (cross-field).
	var foundPipelineFlow bool
	for _, e := range out.Edges {
		if e.From.Kind == "fakek8sjob" && e.To.Kind == "faketerraform" {
			if len(e.Values) != 1 || e.Values[0].SourceField != "/image_tag" || e.Values[0].DependentField != "/built_image" {
				t.Fatalf("pipeline flow wrong: %+v", e.Values)
			}
			foundPipelineFlow = true
			break
		}
	}
	if !foundPipelineFlow {
		t.Fatal("no fakek8sjob->faketerraform edge found")
	}
}

// zipDir builds an in-memory zip of every *.star under dir (RECURSIVELY, preserving
// subdir paths like policies/stack.star) — mirroring the demo's apply-bundle recipe
// (`find . -name '*.star' | zip`), so the test exercises the real multi-file bundle.
func zipDir(t *testing.T, dir string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(p) != ".star" {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		// Zip entry names use forward slashes (matching the shell `zip`).
		w, err := zw.Create(filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		_, err = w.Write(src)
		return err
	})
	if err != nil {
		t.Fatalf("zip dir %s: %v", dir, err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

// readSpec reads a demo fixture and returns its `spec` object as raw JSON.
func readSpec(t *testing.T, path string) json.RawMessage {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var wrapper struct {
		Spec json.RawMessage `json:"spec"`
	}
	if err := json.Unmarshal(b, &wrapper); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return wrapper.Spec
}

func sampleNames(out converge.Outcome) []string {
	n := min(len(out.Children), 5)
	names := make([]string, n)
	for i := range n {
		names[i] = out.Children[i].Name
	}
	return names
}
