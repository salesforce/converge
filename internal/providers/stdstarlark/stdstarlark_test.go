package stdstarlark

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"testing"

	"github.com/salesforce/converge/sdk-go/converge"
)

// zipBundle builds an in-memory .star zip from name→source entries — the bundle a
// stdstarlark providerconfig's `data` carries.
func zipBundle(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	// Deterministic entry order (sorted) so tests don't depend on map iteration.
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		w, err := zw.Create(n)
		if err != nil {
			t.Fatalf("zip create %s: %v", n, err)
		}
		if _, err := w.Write([]byte(files[n])); err != nil {
			t.Fatalf("zip write %s: %v", n, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	return buf.Bytes()
}

// runCompose composes with the program bundle as the kind DEFAULT bundle and the
// given instance spec + default config — the real Setup wiring.
func runCompose(t *testing.T, files map[string]string, specJSON, configJSON string) (converge.Outcome, error) {
	t.Helper()
	bundle := zipBundle(t, files)
	c := composer{
		defaultData: bundle,
		defaultSpec: json.RawMessage(configJSON),
	}
	var spec json.RawMessage
	if specJSON != "" {
		spec = json.RawMessage(specJSON)
	}
	return c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Trigger:  converge.TriggerSpecChange,
		Resource: converge.Resource{Kind: Kind, Name: "root", Spec: spec},
		Env:      &converge.Env{},
	})
}

func childByName(out converge.Outcome) map[string]converge.ChildSpec {
	m := make(map[string]converge.ChildSpec, len(out.Children))
	for _, c := range out.Children {
		m[c.Name] = c
	}
	return m
}

// TestComposeChildrenFromSpecAndConfig: compose() reads the instance spec + the
// config, and emits children whose kind/name/spec/labels are marshalled correctly. A
// numeric config value round-trips as a number (not a string).
func TestComposeChildrenFromSpecAndConfig(t *testing.T) {
	prog := `
def compose(spec, config):
    children = []
    for i in range(config.count):
        children.append(struct(
            kind = "fakevpc",
            kind_version = 1,
            name = "%s-vpc-%d" % (spec.name, i),
            spec = {"account_id": spec.account, "index": i},
            labels = {"env": config.env},
        ))
    return struct(children = children)
`
	out, err := runCompose(t,
		map[string]string{"compose.star": prog},
		`{"name":"prod","account":"acct-1"}`,
		`{"count":2,"env":"dev"}`)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(out.Children) != 2 {
		t.Fatalf("want 2 children, got %d", len(out.Children))
	}
	c := childByName(out)["prod-vpc-0"]
	if c.Kind != converge.Kind("fakevpc") {
		t.Fatalf("kind: got %q", c.Kind)
	}
	if c.Labels["env"] != "dev" {
		t.Fatalf("label env: got %q", c.Labels["env"])
	}
	var spec struct {
		AccountID string `json:"account_id"`
		Index     int    `json:"index"`
	}
	if err := json.Unmarshal(c.Spec.(json.RawMessage), &spec); err != nil {
		t.Fatalf("child spec not JSON: %s (%v)", c.Spec, err)
	}
	if spec.AccountID != "acct-1" || spec.Index != 0 {
		t.Fatalf("child spec: got %+v", spec)
	}
}

// TestComposeEdgesAndValueFlows: a program emitting edges with value flows produces
// the right DepEdge endpoints + ValueFlow fields (incl. a cross-field flow where the
// dependent and source pointers differ).
func TestComposeEdgesAndValueFlows(t *testing.T) {
	prog := `
def compose(spec, config):
    return struct(
        children = [
            struct(kind = "fakevpc", kind_version = 1, name = "vpc", spec = {"account_id": "a"}),
            struct(kind = "fakeapp", kind_version = 1, name = "app", spec = {}),
        ],
        edges = [
            struct(
                dependent = struct(kind = "fakeapp", name = "app"),
                dependency = struct(kind = "fakevpc", name = "vpc"),
                values = [struct(dependent = "/vpc_id", source = "/id")],
            ),
        ],
    )
`
	out, err := runCompose(t, map[string]string{"compose.star": prog}, `{}`, `{}`)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(out.Edges) != 1 {
		t.Fatalf("want 1 edge, got %d", len(out.Edges))
	}
	e := out.Edges[0]
	if e.From.Name != "app" || e.To.Name != "vpc" {
		t.Fatalf("edge endpoints: got %s -> %s", e.From.Name, e.To.Name)
	}
	if len(e.Values) != 1 || e.Values[0].DependentField != "/vpc_id" || e.Values[0].SourceField != "/id" {
		t.Fatalf("value flow: got %+v", e.Values)
	}
}

// TestComposeConfigsEmitted: a program may emit provider configs alongside children.
func TestComposeConfigsEmitted(t *testing.T) {
	prog := `
def compose(spec, config):
    return struct(
        children = [struct(kind = "fakevpc", kind_version = 1, name = "vpc", spec = {})],
        configs = [struct(kind = "fakevpc", kind_version = 1, name = "vpc-cfg", spec = {"region": config.region})],
    )
`
	out, err := runCompose(t, map[string]string{"compose.star": prog}, `{}`, `{"region":"us-west-2"}`)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(out.Configs) != 1 || out.Configs[0].Name != "vpc-cfg" || out.Configs[0].Kind != converge.Kind("fakevpc") {
		t.Fatalf("configs: got %+v", out.Configs)
	}
}

// TestLoadModule: compose.star can load() a helper from another .star in the bundle.
func TestLoadModule(t *testing.T) {
	files := map[string]string{
		"helpers.star": `
def vpc_name(prefix):
    return prefix + "-vpc"
`,
		"compose.star": `
load("helpers.star", "vpc_name")
def compose(spec, config):
    return struct(children = [struct(kind = "fakevpc", kind_version = 1, name = vpc_name(spec.prefix), spec = {})])
`,
	}
	out, err := runCompose(t, files, `{"prefix":"kro"}`, `{}`)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(out.Children) != 1 || out.Children[0].Name != "kro-vpc" {
		t.Fatalf("load module: got %+v", out.Children)
	}
}

// TestLoadModuleFromSubdir: a module in a subdirectory (policies/p.star) is loadable
// by its slash path — the layout the demo bundle uses (policies/*.star). Proves the
// bundle can span subdirs.
func TestLoadModuleFromSubdir(t *testing.T) {
	files := map[string]string{
		"policies/p.star": `def policy(): return struct(name = "p")`,
		"compose.star": `
load("policies/p.star", "policy")
def compose(spec, config):
    return struct(children = [struct(kind = "k", kind_version = 1, name = policy().name, spec = {})])
`,
	}
	out, err := runCompose(t, files, `{}`, `{}`)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(out.Children) != 1 || out.Children[0].Name != "p" {
		t.Fatalf("subdir load: got %+v", out.Children)
	}
}

// TestLoadCycleRejected: a load() cycle between modules is a (terminal) compose error.
func TestLoadCycleRejected(t *testing.T) {
	files := map[string]string{
		"a.star":       `load("b.star", "b"); def a(): return b()`,
		"b.star":       `load("a.star", "a"); def b(): return a()`,
		"compose.star": `load("a.star", "a"); def compose(spec, config): return struct(children = [])`,
	}
	_, err := runCompose(t, files, `{}`, `{}`)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("load cycle must be terminal, got %v", err)
	}
}

// TestNoBundleIsTransient: an absent program bundle (not yet propagated) is a
// TRANSIENT retry, not terminal — a fresh-cluster startup race must recover once the
// bundle arrives, not stick the root in Failed.
func TestNoBundleIsTransient(t *testing.T) {
	c := composer{defaultData: nil, defaultSpec: nil}
	_, err := c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Resource: converge.Resource{Kind: Kind, Name: "root", Spec: json.RawMessage(`{}`)},
		Env:      &converge.Env{},
	})
	if err == nil {
		t.Fatal("no bundle must error")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("no bundle must be TRANSIENT (retry), got terminal: %v", err)
	}
}

// TestBadBundleIsTerminal: a present-but-malformed bundle (not a zip) is terminal.
func TestBadBundleIsTerminal(t *testing.T) {
	c := composer{defaultData: []byte("not a zip"), defaultSpec: nil}
	_, err := c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Resource: converge.Resource{Kind: Kind, Name: "root", Spec: json.RawMessage(`{}`)},
		Env:      &converge.Env{},
	})
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("bad bundle must be terminal, got %v", err)
	}
}

// TestNoComposeEntryIsTerminal: a bundle with no compose.star at the root is terminal.
func TestNoComposeEntryIsTerminal(t *testing.T) {
	_, err := runCompose(t, map[string]string{"helpers.star": `def x(): return 1`}, `{}`, `{}`)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("missing compose.star must be terminal, got %v", err)
	}
}

// TestNoComposeFunctionIsTerminal: compose.star present but without a compose()
// function is terminal.
func TestNoComposeFunctionIsTerminal(t *testing.T) {
	_, err := runCompose(t, map[string]string{"compose.star": `x = 1`}, `{}`, `{}`)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("missing compose() must be terminal, got %v", err)
	}
}

// TestRuntimeErrorIsTerminal: a Starlark runtime error in compose() is terminal (a
// same-bundle re-run won't fix a program bug).
func TestRuntimeErrorIsTerminal(t *testing.T) {
	prog := `def compose(spec, config): return spec.does_not_exist`
	_, err := runCompose(t, map[string]string{"compose.star": prog}, `{}`, `{}`)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("runtime error must be terminal, got %v", err)
	}
}

// TestBadReturnShapeIsTerminal: compose() returning a non-struct is terminal.
func TestBadReturnShapeIsTerminal(t *testing.T) {
	_, err := runCompose(t, map[string]string{"compose.star": `def compose(spec, config): return 42`}, `{}`, `{}`)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("bad return must be terminal, got %v", err)
	}
}

// TestCustomBundleOverridesDefault: a per-resource CUSTOM bundle (Env.ProviderBundle)
// replaces the kind default program whole.
func TestCustomBundleOverridesDefault(t *testing.T) {
	def := zipBundle(t, map[string]string{"compose.star": `def compose(spec, config): return struct(children = [struct(kind = "k", kind_version = 1, name = "from-default", spec = {})])`})
	custom := zipBundle(t, map[string]string{"compose.star": `def compose(spec, config): return struct(children = [struct(kind = "k", kind_version = 1, name = "from-custom", spec = {})])`})
	c := composer{defaultData: def, defaultSpec: nil}
	out, err := c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Resource: converge.Resource{Kind: Kind, Name: "root", Spec: json.RawMessage(`{}`)},
		Env:      &converge.Env{ProviderBundle: custom},
	})
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(out.Children) != 1 || out.Children[0].Name != "from-custom" {
		t.Fatalf("custom bundle must win: got %+v", out.Children)
	}
}

// TestConfigOverrideMergesOntoDefault: a per-resource config override field-merges
// onto the default config the program reads.
func TestConfigOverrideMergesOntoDefault(t *testing.T) {
	prog := `def compose(spec, config): return struct(children = [struct(kind = "k", kind_version = 1, name = config.env, spec = {})])`
	bundle := zipBundle(t, map[string]string{"compose.star": prog})
	c := composer{
		defaultData: bundle,
		defaultSpec: json.RawMessage(`{"env":"default"}`),
	}
	out, err := c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Resource: converge.Resource{Kind: Kind, Name: "root", Spec: json.RawMessage(`{}`)},
		Env:      &converge.Env{ProviderConfig: json.RawMessage(`{"env":"override"}`)},
	})
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if out.Children[0].Name != "override" {
		t.Fatalf("config override must win: got %q", out.Children[0].Name)
	}
}

// TestDuplicateEntryRejected: two identical entry names in the zip are terminal
// (ambiguous — which content wins?).
func TestDuplicateEntryRejected(t *testing.T) {
	// Build a zip with a duplicate compose.star manually (zipBundle dedups via a map).
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, src := range []string{`def compose(spec, config): return struct()`, `x = 1`} {
		w, _ := zw.Create("compose.star")
		_, _ = w.Write([]byte(src))
	}
	_ = zw.Close()
	c := composer{defaultData: buf.Bytes(), defaultSpec: nil}
	_, err := c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Resource: converge.Resource{Kind: Kind, Name: "root", Spec: json.RawMessage(`{}`)},
		Env:      &converge.Env{},
	})
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("duplicate entry must be terminal, got %v", err)
	}
}

// TestOversizeEntryRejected: a .star entry over the per-entry cap is terminal
// (zip-bomb defence).
func TestOversizeEntryRejected(t *testing.T) {
	big := make([]byte, maxStarBytes+1)
	for i := range big {
		big[i] = ' '
	}
	files := map[string]string{"compose.star": `def compose(spec, config): return struct()`, "big.star": string(big)}
	_, err := runCompose(t, files, `{}`, `{}`)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("oversize entry must be terminal, got %v", err)
	}
}
