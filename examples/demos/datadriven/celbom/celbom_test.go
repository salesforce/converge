package celbom

import (
	"context"
	"encoding/json"
	"sort"
	"testing"

	"github.com/salesforce/converge/sdk-go/converge"
)

// ── The declarative rule set the operator ships (DATA, not a program). ───────
//
// Compare with stdstarlark's compose.star: there the fan-out is a Starlark program;
// here it's a fixed Go loop and the only "logic" is each rule's one-line CEL `when`
// predicate over the `team` variable {fd, name}. The rules are plain JSON config.

// twoRules: a vpc rule + a cloudtrail rule, each selecting every team in any FD.
// child.name uses ${fd}-${team}-${name}; child.kind is "noop" so the demo reconciles.
var twoRules = ruleSet{Rules: []rule{
	{Name: "vpc", When: `team.fd.startsWith("fd-")`, Child: childTemplate{Kind: "noop", Name: "${fd}-${team}-${name}"}},
	{Name: "cloudtrail", When: `team.fd.startsWith("fd-")`, Child: childTemplate{Kind: "noop", Name: "${fd}-${team}-${name}"}},
}}

// The BOM: the shared deployment_instance shape — 2 FDs × 2 teams. IDENTICAL shape
// to stdstarlark's BOM + classicbom, so all three composers speak one vocabulary.
const bomSpec = `{
  "deployment_instance": {
    "name": "demo",
    "functional_domains": [
      { "name": "fd-00", "service_teams": [ {"name": "admiring-turing"}, {"name": "bold-curie"} ] },
      { "name": "fd-01", "service_teams": [ {"name": "clever-bohr"}, {"name": "eager-lovelace"} ] }
    ]
  }
}`

// TestComposeFansBOMxRules is the proof: the declarative-rule composer expands the
// BOM (4 teams across 2 FDs) × rules (vpc, cloudtrail) into 8 children — entirely
// from rule DATA + CEL predicates, no embedded program.
func TestComposeFansBOMxRules(t *testing.T) {
	out, err := runCompose(t, twoRules, []byte(bomSpec))
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	// 8 children: 4 teams × {vpc, cloudtrail}.
	got := childNames(out.Children)
	want := []string{
		"fd-00-admiring-turing-cloudtrail", "fd-00-admiring-turing-vpc",
		"fd-00-bold-curie-cloudtrail", "fd-00-bold-curie-vpc",
		"fd-01-clever-bohr-cloudtrail", "fd-01-clever-bohr-vpc",
		"fd-01-eager-lovelace-cloudtrail", "fd-01-eager-lovelace-vpc",
	}
	if !equal(got, want) {
		t.Fatalf("children = %v, want %v", got, want)
	}
	// Each child carries its functional_domain + team + rule name in its SPEC (same shape stdstarlark emits).
	for _, c := range out.Children {
		var spec map[string]any
		_ = json.Unmarshal(c.Spec.(json.RawMessage), &spec)
		if spec["functional_domain"] == "" || spec["team"] == "" || spec["policy"] == "" {
			t.Fatalf("child %s spec missing functional_domain/team/policy: %v", c.Name, spec)
		}
	}
}

// TestWhenPredicateFilters proves the CEL `when` actually filters: a rule scoped to
// one FD composes only for that FD's teams. This is the CEL equivalent of
// stdstarlark's fd_selector glob — but a real expression, not a glob.
func TestWhenPredicateFilters(t *testing.T) {
	fd1Only := ruleSet{Rules: []rule{
		{Name: "r", When: `team.fd == "fd-01"`, Child: childTemplate{Kind: "noop", Name: "${fd}-${team}-${name}"}},
	}}
	out, err := runCompose(t, fd1Only, []byte(bomSpec))
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if got := childNames(out.Children); !equal(got, []string{"fd-01-clever-bohr-r", "fd-01-eager-lovelace-r"}) {
		t.Fatalf("when did not filter to fd-01: children = %v", got)
	}
}

// TestEmptyWhenSelectsAll proves an empty `when` selects every team.
func TestEmptyWhenSelectsAll(t *testing.T) {
	all := ruleSet{Rules: []rule{
		{Name: "everywhere", When: "", Child: childTemplate{Kind: "noop", Name: "${fd}-${team}"}},
	}}
	out, err := runCompose(t, all, []byte(bomSpec))
	if err != nil {
		t.Fatalf("compose: %v", err)
	}
	if got := childNames(out.Children); !equal(got, []string{
		"fd-00-admiring-turing", "fd-00-bold-curie", "fd-01-clever-bohr", "fd-01-eager-lovelace",
	}) {
		t.Fatalf("empty when did not select all: children = %v", got)
	}
}

// TestBadCELIsTransient proves a malformed CEL predicate fails TRANSIENT (not
// terminal) and emits NO partial children — so the last good DAG survives and it
// retries, rather than ApplyComposeResult pruning everything.
func TestBadCELIsTransient(t *testing.T) {
	bad := ruleSet{Rules: []rule{
		{Name: "broken", When: `team.fd ==== "x"`, Child: childTemplate{Kind: "noop", Name: "${team}"}},
	}}
	out, err := runCompose(t, bad, []byte(bomSpec))
	if err == nil {
		t.Fatal("expected an error from broken CEL")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("broken CEL should be TRANSIENT (retryable), got terminal: %v", err)
	}
	if len(out.Children) != 0 {
		t.Fatalf("a failed compose must emit NO children (would prune the DAG), got %d", len(out.Children))
	}
}

// TestNonBoolWhenIsTransient proves a `when` that compiles but isn't boolean (e.g.
// a string field) is rejected up front — a rule can't silently select-none/all on a
// non-bool predicate.
func TestNonBoolWhenIsTransient(t *testing.T) {
	nonBool := ruleSet{Rules: []rule{
		{Name: "stringy", When: `team.fd`, Child: childTemplate{Kind: "noop", Name: "${team}"}},
	}}
	_, err := runCompose(t, nonBool, []byte(bomSpec))
	if err == nil {
		t.Fatal("expected an error from a non-boolean when")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("non-bool when should be TRANSIENT, got terminal: %v", err)
	}
}

// TestDependsOnEmitsEdgesAndValueFlows proves a rule with a LIST of depends_on
// composes the dependent (fakeapp) AND BOTH upstreams (fakevpc + fakedb) per team,
// and emits a DepEdge from dependent→each upstream carrying its value flow
// (status /vpc_id → spec /vpc_id, status /db_endpoint → spec /db_endpoint). This is
// the declarative-rule expression of a multi-dependency DAG.
func TestDependsOnEmitsEdgesAndValueFlows(t *testing.T) {
	depRule := ruleSet{Rules: []rule{{
		Name:  "app",
		When:  `team.fd == "fd-00"`,
		Child: childTemplate{Kind: "fakeapp", Name: "${fd}-${team}-app"},
		DependsOn: []dependency{
			{Kind: "fakevpc", Name: "${fd}-${team}-vpc", Flow: []flow{{From: "/vpc_id", To: "/vpc_id"}}},
			{Kind: "fakedb", Name: "${fd}-${team}-db", Flow: []flow{{From: "/db_endpoint", To: "/db_endpoint"}}},
		},
	}}}
	out, err := runCompose(t, depRule, []byte(bomSpec))
	if err != nil {
		t.Fatalf("compose: %v", err)
	}

	// 6 children: app + vpc + db per fd-00 team (2 teams in fd-00).
	if got := childNames(out.Children); !equal(got, []string{
		"fd-00-admiring-turing-app", "fd-00-admiring-turing-db", "fd-00-admiring-turing-vpc",
		"fd-00-bold-curie-app", "fd-00-bold-curie-db", "fd-00-bold-curie-vpc",
	}) {
		t.Fatalf("children = %v, want app+vpc+db per fd-00 team", got)
	}

	// Two edges per team (app→vpc, app→db) = 4 total, each with its one value flow.
	if len(out.Edges) != 4 {
		t.Fatalf("edges = %d, want 4 (app→vpc + app→db per fd-00 team)", len(out.Edges))
	}
	gotFlows := map[string]string{} // upstream-kind → flowed field
	for _, e := range out.Edges {
		if e.From.Kind != "fakeapp" {
			t.Fatalf("edge from wrong kind: %s (want fakeapp)", e.From.Kind)
		}
		if len(e.Values) != 1 {
			t.Fatalf("edge %s→%s: want 1 value flow, got %d", e.From, e.To, len(e.Values))
		}
		gotFlows[string(e.To.Kind)] = e.Values[0].SourceField
	}
	if gotFlows["fakevpc"] != "/vpc_id" || gotFlows["fakedb"] != "/db_endpoint" {
		t.Fatalf("value flows wrong: %v (want fakevpc:/vpc_id, fakedb:/db_endpoint)", gotFlows)
	}
}

// TestLiteralSpecIsStamped proves a rule's literal `spec` map (the team-specific
// inputs a kind needs — e.g. a fakek8sjob's image, a faketerraform's tar_url) is
// merged onto the stamped child/dependency spec, with ${...} placeholders expanded
// in string values. This is how teams declaratively apply the tf→k8sjob pipeline.
func TestLiteralSpecIsStamped(t *testing.T) {
	rs := ruleSet{Rules: []rule{{
		Name:  "pipeline",
		When:  `team.fd == "fd-00"`,
		Child: childTemplate{Kind: "fakek8sjob", Name: "${fd}-${team}-job", Spec: map[string]any{"image": "nginx:1.27"}},
		DependsOn: []dependency{{
			Kind: "faketerraform", Name: "${fd}-${team}-tf",
			Spec: map[string]any{"tar_url": "s3://team-bundles/${team}.tar"},
			Flow: []flow{{From: "/image_tag", To: "/built_image"}},
		}},
	}}}
	out, err := runCompose(t, rs, []byte(bomSpec))
	if err != nil {
		t.Fatalf("compose: %v", err)
	}

	specByName := map[string]map[string]any{}
	for _, c := range out.Children {
		var s map[string]any
		_ = json.Unmarshal(c.Spec.(json.RawMessage), &s)
		specByName[c.Name] = s
	}

	// The job carries the LITERAL image (merged onto the identity fields).
	job := specByName["fd-00-admiring-turing-job"]
	if job["image"] != "nginx:1.27" {
		t.Fatalf("job spec image = %v, want nginx:1.27 (full: %v)", job["image"], job)
	}
	// The terraform upstream carries the tar_url with ${team} expanded.
	tf := specByName["fd-00-admiring-turing-tf"]
	if tf["tar_url"] != "s3://team-bundles/admiring-turing.tar" {
		t.Fatalf("tf spec tar_url = %v, want the ${team}-expanded URL (full: %v)", tf["tar_url"], tf)
	}
	// The job's flowed field (built_image) is deliberately ABSENT — the upstream
	// produces it; the engine flows it in before the job runs.
	if _, present := job["built_image"]; present {
		t.Fatalf("job spec must NOT pre-set built_image (it's flowed): %v", job)
	}
}

// TestDeterministic proves the same (BOM, rules) yields the identical child set
// across runs (and regardless of FD/team ordering) — the composer-contract invariant.
func TestDeterministic(t *testing.T) {
	a, err := runCompose(t, twoRules, []byte(bomSpec))
	if err != nil {
		t.Fatal(err)
	}
	// Reversed FD/team order must yield the same sorted child set (compose sorts).
	reversed := `{
	  "deployment_instance": {
	    "name": "demo",
	    "functional_domains": [
	      { "name": "fd-01", "service_teams": [ {"name": "eager-lovelace"}, {"name": "clever-bohr"} ] },
	      { "name": "fd-00", "service_teams": [ {"name": "bold-curie"}, {"name": "admiring-turing"} ] }
	    ]
	  }
	}`
	b, err := runCompose(t, twoRules, []byte(reversed))
	if err != nil {
		t.Fatal(err)
	}
	if !equal(childNames(a.Children), childNames(b.Children)) {
		t.Fatal("compose is non-deterministic across BOM orderings")
	}
}

func childNames(cs []converge.ChildSpec) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Name
	}
	sort.Strings(out)
	return out
}

// runCompose builds the composer with the rule set as its DEFAULT config (how the kind
// default reaches a real composer via OnConfig) and composes the given BOM.
func runCompose(t *testing.T, rs ruleSet, specJSON []byte) (converge.Outcome, error) {
	t.Helper()
	cfg, err := json.Marshal(rs)
	if err != nil {
		t.Fatalf("marshal rules: %v", err)
	}
	c := composer{defaultSpec: cfg}
	return c.React(context.Background(), converge.ReactionRequest{
		Reaction: "compose",
		Trigger:  converge.TriggerSpecChange,
		Resource: converge.Resource{Kind: Kind, Spec: specJSON},
		Env:      &converge.Env{},
	})
}

func equal(a, b []string) bool {
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
