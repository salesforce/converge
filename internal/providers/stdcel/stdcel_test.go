package stdcel

import (
	"context"
	"encoding/json"
	"sort"
	"testing"

	"github.com/salesforce/converge/sdk-go/converge"
)

// runCompose builds the composer with graphJSON as its DEFAULT config (how a kind
// default reaches a real composer via OnConfig) and composes an instance whose spec
// is instanceJSON.
func runCompose(t *testing.T, graphJSON, instanceJSON string) (converge.Outcome, error) {
	t.Helper()
	c := composer{defaultSpec: json.RawMessage(graphJSON)}
	return c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Trigger:  converge.TriggerSpecChange,
		Resource: converge.Resource{Kind: Kind, Name: "root", Spec: json.RawMessage(instanceJSON)},
		Env:      &converge.Env{},
	})
}

// childByName indexes an outcome's children by name for assertions.
func childByName(out converge.Outcome) map[string]converge.ChildSpec {
	m := make(map[string]converge.ChildSpec, len(out.Children))
	for _, c := range out.Children {
		m[c.Name] = c
	}
	return m
}

// TestSchemaSpecResolution: a child's Name and Spec leaves read the INSTANCE spec
// via ${schema.spec.*}, both interpolated (into a name) and whole-value (an int stays
// an int, not a string).
func TestSchemaSpecResolution(t *testing.T) {
	graph := `{"resources":[
		{"id":"vpc","template":{"kind":"fakevpc","kind_version":1,"name":"${schema.spec.name}-vpc",
			"spec":{"account_id":"${schema.spec.account}","cidr_size":"${schema.spec.size}"}}}
	]}`
	instance := `{"name":"prod","account":"acct-1","size":24}`

	out, err := runCompose(t, graph, instance)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(out.Children) != 1 {
		t.Fatalf("want 1 child, got %d", len(out.Children))
	}
	c := out.Children[0]
	if c.Name != "prod-vpc" {
		t.Fatalf("name interpolation: want prod-vpc, got %q", c.Name)
	}
	if c.Kind != converge.Kind("fakevpc") {
		t.Fatalf("kind: want fakevpc, got %q", c.Kind)
	}
	var spec struct {
		AccountID string `json:"account_id"`
		CIDRSize  int    `json:"cidr_size"`
	}
	if err := json.Unmarshal(c.Spec.(json.RawMessage), &spec); err != nil {
		t.Fatalf("child spec not JSON: %s (%v)", c.Spec, err)
	}
	if spec.AccountID != "acct-1" {
		t.Fatalf("account_id: want acct-1, got %q", spec.AccountID)
	}
	if spec.CIDRSize != 24 { // whole-value ${...} kept the int type, not "24"
		t.Fatalf("cidr_size: want int 24, got %d", spec.CIDRSize)
	}
	// The provider stamps composed_by + graph_id labels.
	if c.Labels["composed_by"] != string(Kind) || c.Labels["graph_id"] != "vpc" {
		t.Fatalf("labels: %v", c.Labels)
	}
}

// TestSpecCrossReferenceInlines: a resource reading ${other.spec.x} gets the value
// INLINED at compose time (a plain dependency for ordering, but NOT a runtime edge —
// spec is known statically).
func TestSpecCrossReferenceInlines(t *testing.T) {
	graph := `{"resources":[
		{"id":"vpc","template":{"kind":"fakevpc","kind_version":1,"name":"vpc","spec":{"account_id":"${schema.spec.account}"}}},
		{"id":"app","template":{"kind":"fakeapp","kind_version":1,"name":"app","spec":{"account_copy":"${vpc.spec.account_id}"}}}
	]}`
	out, err := runCompose(t, graph, `{"account":"acct-9"}`)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	// vpc must be ordered before app (app depends on vpc.spec).
	if out.Children[0].Name != "vpc" || out.Children[1].Name != "app" {
		t.Fatalf("topo order: want [vpc app], got [%s %s]", out.Children[0].Name, out.Children[1].Name)
	}
	app := childByName(out)["app"]
	var spec struct {
		AccountCopy string `json:"account_copy"`
	}
	_ = json.Unmarshal(app.Spec.(json.RawMessage), &spec)
	if spec.AccountCopy != "acct-9" {
		t.Fatalf("spec cross-ref inline: want acct-9, got %q", spec.AccountCopy)
	}
	// A spec-only reference must NOT create a runtime edge.
	if len(out.Edges) != 0 {
		t.Fatalf("spec-only ref must not emit an edge, got %d", len(out.Edges))
	}
}

// TestStatusReferenceEmitsEdgeAndFlow: a resource reading ${other.status.x} gets a
// DepEdge (dependent → dependency) carrying a ValueFlow that fills spec[x] from the
// upstream's status[x]; the field is null in the composed spec (the engine flows it
// in at runtime).
func TestStatusReferenceEmitsEdgeAndFlow(t *testing.T) {
	graph := `{"resources":[
		{"id":"vpc","template":{"kind":"fakevpc","kind_version":1,"name":"the-vpc","spec":{"account_id":"a"}}},
		{"id":"app","template":{"kind":"fakeapp","kind_version":1,"name":"the-app","spec":{"vpc_id":"${vpc.status.vpc_id}"}}}
	]}`
	out, err := runCompose(t, graph, `{}`)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(out.Edges) != 1 {
		t.Fatalf("want 1 edge, got %d: %+v", len(out.Edges), out.Edges)
	}
	e := out.Edges[0]
	if e.From.Name != "the-app" || e.To.Name != "the-vpc" {
		t.Fatalf("edge endpoints: want the-app -> the-vpc, got %s -> %s", e.From.Name, e.To.Name)
	}
	if len(e.Values) != 1 || e.Values[0].SourceField != "/vpc_id" || e.Values[0].DependentField != "/vpc_id" {
		t.Fatalf("value flow: want /vpc_id -> /vpc_id, got %+v", e.Values)
	}
	// The flowed field is present-but-null in the composed spec (runtime fills it).
	app := childByName(out)["the-app"]
	var spec map[string]any
	_ = json.Unmarshal(app.Spec.(json.RawMessage), &spec)
	if v, ok := spec["vpc_id"]; !ok || v != nil {
		t.Fatalf("flowed field must be present+null pre-flow, got %v (present=%v)", v, ok)
	}
}

// TestNestedStatusPathFlow: a nested status path (${db.status.endpoint.host}) yields
// a JSON-Pointer value flow (/endpoint/host) and the nested placeholder resolves.
func TestNestedStatusPathFlow(t *testing.T) {
	graph := `{"resources":[
		{"id":"db","template":{"kind":"fakedb","kind_version":1,"name":"db","spec":{"account_id":"a"}}},
		{"id":"app","template":{"kind":"fakeapp","kind_version":1,"name":"app","spec":{"endpoint":{"host":"${db.status.endpoint.host}"}}}}
	]}`
	out, err := runCompose(t, graph, `{}`)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if len(out.Edges) != 1 || len(out.Edges[0].Values) != 1 {
		t.Fatalf("want 1 edge with 1 flow, got %+v", out.Edges)
	}
	if got := out.Edges[0].Values[0].SourceField; got != "/endpoint/host" {
		t.Fatalf("nested flow pointer: want /endpoint/host, got %q", got)
	}
}

// TestTopologicalOrderMultiDep: a diamond (app depends on both vpc and db) orders
// both upstreams before app, deterministically, and emits an edge per upstream.
func TestTopologicalOrderMultiDep(t *testing.T) {
	graph := `{"resources":[
		{"id":"app","template":{"kind":"fakeapp","kind_version":1,"name":"app","spec":{
			"vpc_id":"${vpc.status.vpc_id}","db":"${db.status.endpoint}"}}},
		{"id":"vpc","template":{"kind":"fakevpc","kind_version":1,"name":"vpc","spec":{"account_id":"a"}}},
		{"id":"db","template":{"kind":"fakedb","kind_version":1,"name":"db","spec":{"account_id":"a"}}}
	]}`
	out, err := runCompose(t, graph, `{}`)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	// app must come last; the two independent upstreams sort alphabetically (db, vpc).
	got := []string{out.Children[0].Name, out.Children[1].Name, out.Children[2].Name}
	want := []string{"db", "vpc", "app"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("topo order: want %v, got %v", want, got)
		}
	}
	if len(out.Edges) != 2 {
		t.Fatalf("diamond must emit 2 edges, got %d", len(out.Edges))
	}
}

// TestCycleRejected: a dependency cycle is a TERMINAL compose error naming the members.
func TestCycleRejected(t *testing.T) {
	graph := `{"resources":[
		{"id":"a","template":{"kind":"k","kind_version":1,"name":"a","spec":{"x":"${b.status.y}"}}},
		{"id":"b","template":{"kind":"k","kind_version":1,"name":"b","spec":{"y":"${a.status.x}"}}}
	]}`
	_, err := runCompose(t, graph, `{}`)
	if err == nil {
		t.Fatal("cycle must fail")
	}
	if !converge.IsTerminal(err) {
		t.Fatalf("cycle must be terminal, got %v", err)
	}
}

// TestUnknownReferenceRejected: referencing an id that isn't in the graph is terminal.
func TestUnknownReferenceRejected(t *testing.T) {
	graph := `{"resources":[
		{"id":"app","template":{"kind":"k","kind_version":1,"name":"app","spec":{"x":"${ghost.status.y}"}}}
	]}`
	_, err := runCompose(t, graph, `{}`)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("unknown ref must be terminal, got %v", err)
	}
}

// TestSelfReferenceRejected: a resource referencing itself is terminal (would be a
// trivial cycle / undefined at compose time).
func TestSelfReferenceRejected(t *testing.T) {
	graph := `{"resources":[
		{"id":"a","template":{"kind":"k","kind_version":1,"name":"a","spec":{"x":"${a.spec.x}"}}}
	]}`
	_, err := runCompose(t, graph, `{}`)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("self ref must be terminal, got %v", err)
	}
}

// TestDuplicateIdRejected: two resources with the same id is terminal.
func TestDuplicateIdRejected(t *testing.T) {
	graph := `{"resources":[
		{"id":"a","template":{"kind":"k","kind_version":1,"name":"a1"}},
		{"id":"a","template":{"kind":"k","kind_version":1,"name":"a2"}}
	]}`
	_, err := runCompose(t, graph, `{}`)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("duplicate id must be terminal, got %v", err)
	}
}

// TestMissingKindOrName: a template missing kind, kind_version, or name is terminal.
func TestMissingKindOrName(t *testing.T) {
	for _, g := range []string{
		`{"resources":[{"id":"a","template":{"name":"a","kind_version":1}}]}`,   // no kind
		`{"resources":[{"id":"a","template":{"kind":"k","name":"a"}}]}`,         // no kind_version (required, no implicit v1)
		`{"resources":[{"id":"a","template":{"kind":"k","kind_version":1}}]}`,   // no name
		`{"resources":[{"template":{"kind":"k","kind_version":1,"name":"a"}}]}`, // no id
	} {
		_, err := runCompose(t, g, `{}`)
		if err == nil || !converge.IsTerminal(err) {
			t.Fatalf("invalid template %q must be terminal, got %v", g, err)
		}
	}
}

// TestNoGraphIsTransient: an empty/absent graph (not yet propagated) is a TRANSIENT
// retry, not terminal — a fresh-cluster startup race must recover once the graph
// arrives, not stick the root in Failed.
func TestNoGraphIsTransient(t *testing.T) {
	c := composer{defaultSpec: nil} // no default config yet
	_, err := c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Resource: converge.Resource{Kind: Kind, Name: "root", Spec: json.RawMessage(`{}`)},
		Env:      &converge.Env{},
	})
	if err == nil {
		t.Fatal("no graph must error")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("no graph must be TRANSIENT (retry), got terminal: %v", err)
	}
}

// TestPerResourceConfigOverride: a per-resource CUSTOM providerconfig override merges
// onto the kind default graph (the app's graph is replaced for this one instance).
func TestPerResourceConfigOverride(t *testing.T) {
	base := `{"resources":[{"id":"a","template":{"kind":"k","kind_version":1,"name":"base-${schema.spec.n}"}}]}`
	override := `{"resources":[{"id":"a","template":{"kind":"k","kind_version":1,"name":"override-${schema.spec.n}"}}]}`
	c := composer{defaultSpec: json.RawMessage(base)}
	out, err := c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Resource: converge.Resource{Kind: Kind, Name: "root", Spec: json.RawMessage(`{"n":"x"}`)},
		Env:      &converge.Env{ProviderConfig: json.RawMessage(override)},
	})
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if out.Children[0].Name != "override-x" {
		t.Fatalf("override must win: got %q", out.Children[0].Name)
	}
}

// TestDeterministicOutput: the same graph + instance always yields the same child +
// edge order regardless of declaration order (topo-then-alpha).
func TestDeterministicOutput(t *testing.T) {
	graph := `{"resources":[
		{"id":"z","template":{"kind":"k","kind_version":1,"name":"z","spec":{"a":"${m.status.v}"}}},
		{"id":"m","template":{"kind":"k","kind_version":1,"name":"m","spec":{}}},
		{"id":"a","template":{"kind":"k","kind_version":1,"name":"a","spec":{}}}
	]}`
	var first []string
	for range 5 {
		out, err := runCompose(t, graph, `{}`)
		if err != nil {
			t.Fatalf("compose: %v", err)
		}
		names := make([]string, len(out.Children))
		for j, c := range out.Children {
			names[j] = c.Name
		}
		if first == nil {
			first = names
			continue
		}
		if !equalStrs(first, names) {
			t.Fatalf("non-deterministic order: %v vs %v", first, names)
		}
	}
	// Independent 'a' and 'm' sort alphabetically; z (depends on m) comes after m.
	sorted := append([]string(nil), first...)
	if !isTopoValid(first) {
		t.Fatalf("order invalid: %v (sorted %v)", first, func() []string { sort.Strings(sorted); return sorted }())
	}
}

func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// isTopoValid checks m precedes z (the one real edge) in the given order.
func isTopoValid(order []string) bool {
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	return pos["m"] < pos["z"]
}
