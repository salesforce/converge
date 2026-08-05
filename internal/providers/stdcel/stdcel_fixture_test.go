package stdcel

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/salesforce/converge/sdk-go/converge"
)

// demoDir is the datadriven demo's self-contained fixture dir (graphs + instances).
const demoDir = "../../../examples/demos/datadriven/testfixtures"

// TestDemoCustomGraphFixtureComposes guards the datadriven demo's CUSTOM-config path:
// the kro-eager-lovelace instance attaches the "stdcel-graph" custom providerconfig
// via provider_config_ref, which reaches the composer through Env.ProviderConfig (not
// the kind default). That graph is the RICHER one — the full app + pipeline DAG — so
// it must compose five children (vpc, db, app, tf, job) wired by three value-flow
// edges. If a leaf kind's field names or the graph drift, this fails at `go test`
// instead of only in a live `just demo`.
func TestDemoCustomGraphFixtureComposes(t *testing.T) {
	dir := filepath.FromSlash(demoDir)
	graphDoc := readSpec(t, filepath.Join(dir, "providerconfig-stdcel.json"))
	instanceSpec := readSpec(t, filepath.Join(dir, "resource-stdcel.json"))

	// CUSTOM path: nil default, graph via the override (an effective config with no
	// default returns the override whole, so the custom config alone drives the compose).
	c := composer{defaultSpec: nil}
	out, err := c.React(t.Context(), converge.ReactionRequest{
		Reaction: "compose",
		Trigger:  converge.TriggerSpecChange,
		Resource: converge.Resource{Kind: Kind, Name: "kro-eager-lovelace", Spec: instanceSpec},
		Env:      &converge.Env{ProviderConfig: graphDoc},
	})
	if err != nil {
		t.Fatalf("compose custom graph: %v", err)
	}

	assertChildNames(t, out, []string{
		"kro-fd-00-eager-lovelace-app",
		"kro-fd-00-eager-lovelace-db",
		"kro-fd-00-eager-lovelace-job",
		"kro-fd-00-eager-lovelace-tf",
		"kro-fd-00-eager-lovelace-vpc",
	})

	// Three value-flow edges: app←vpc (vpc_id), app←db (db_endpoint), job←tf (built_image).
	assertFlows(t, out, map[string]string{
		"fakeapp->fakevpc":          "/vpc_id",
		"fakeapp->fakedb":           "/db_endpoint",
		"fakek8sjob->faketerraform": "/built_image",
	})
}

// TestDemoDefaultGraphFixtureComposes guards the datadriven demo's DEFAULT-config
// path: the kro-happy-newton instance carries NO provider_config_ref, so it composes
// from the kind DEFAULT providerconfig ("stdcel", is_default:true) — the shared,
// kro-RGD-style template, which is the leaner app-DAG-only graph (vpc, db, app). It
// must compose three children wired by two value-flow edges (no pipeline DAG). This
// asserts the default graph is the minimal template and that the default path (graph
// via ConfigSource, not Env.ProviderConfig) works.
func TestDemoDefaultGraphFixtureComposes(t *testing.T) {
	dir := filepath.FromSlash(demoDir)
	graphDoc := readSpec(t, filepath.Join(dir, "providerconfig-stdcel-default.json"))
	instanceSpec := readSpec(t, filepath.Join(dir, "resource-stdcel-default.json"))

	// DEFAULT path: the graph is the kind default (delivered via OnConfig); the instance
	// carries no override.
	c := composer{defaultSpec: graphDoc}
	out, err := c.React(t.Context(), converge.ReactionRequest{
		Reaction: "compose",
		Trigger:  converge.TriggerSpecChange,
		Resource: converge.Resource{Kind: Kind, Name: "kro-happy-newton", Spec: instanceSpec},
		Env:      &converge.Env{},
	})
	if err != nil {
		t.Fatalf("compose default graph: %v", err)
	}

	assertChildNames(t, out, []string{
		"kro-fd-00-happy-newton-app",
		"kro-fd-00-happy-newton-db",
		"kro-fd-00-happy-newton-vpc",
	})

	// Two value-flow edges (app DAG only; no pipeline DAG in the default template).
	assertFlows(t, out, map[string]string{
		"fakeapp->fakevpc": "/vpc_id",
		"fakeapp->fakedb":  "/db_endpoint",
	})
}

// assertChildNames checks the composed children's sorted names match want exactly.
func assertChildNames(t *testing.T, out converge.Outcome, want []string) {
	t.Helper()
	got := make([]string, len(out.Children))
	for i, ch := range out.Children {
		got[i] = ch.Name
	}
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("want %d children, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("child names: want %v, got %v", want, got)
		}
	}
}

// assertFlows checks there is exactly one edge per want entry, each with a single
// value flow into the expected dependent field. Keyed by "fromKind->toKind".
func assertFlows(t *testing.T, out converge.Outcome, want map[string]string) {
	t.Helper()
	if len(out.Edges) != len(want) {
		t.Fatalf("want %d edges, got %d: %+v", len(want), len(out.Edges), out.Edges)
	}
	flows := map[string]string{}
	for _, e := range out.Edges {
		if len(e.Values) != 1 {
			t.Fatalf("edge %s->%s: want 1 flow, got %d", e.From.Name, e.To.Name, len(e.Values))
		}
		flows[string(e.From.Kind)+"->"+string(e.To.Kind)] = e.Values[0].DependentField
	}
	for edge, wantField := range want {
		if got := flows[edge]; got != wantField {
			t.Fatalf("edge %s: want flow into %s, got %q (all: %v)", edge, wantField, got, flows)
		}
	}
}

// readSpec reads a demo fixture and returns its `spec` object as raw JSON (the
// providerconfig's graph, or the resource's instance inputs).
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
