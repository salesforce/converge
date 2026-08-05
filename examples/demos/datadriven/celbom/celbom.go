// Package celbom is the DECLARATIVE-RULES counterpart to the shipped Starlark
// composer providers/stdstarlark: the same "BOM of teams × policies → children
// DAG" composer, but the composition is expressed as RULE DATA + CEL predicates
// instead of an embedded Starlark program.
//
// The contrast (this is the point of the example):
//
//	stdstarlark : the WHOLE composition is a program (a Starlark compose() with
//	              loops/branches), shipped as an opaque .star zip in the BUNDLE
//	              (providerconfigs.data). Maximally flexible; logic is opaque code.
//
//	celbom      : the composition is a fixed Go fan-out — "for each account, for
//	              each rule, if when(account) then emit(template)" — and the only
//	              "code" is a one-line CEL `when` predicate per rule. The rules are
//	              plain typed JSON in the CONFIG (providerconfigs.spec), so they are
//	              structured, queryable, validatable config — not an opaque blob.
//	              CEL is non-Turing-complete (it provably terminates, can't loop or
//	              recurse), so it's safer + more readable for the selector+template
//	              shape, at the cost of expressiveness.
//
// Which to reach for: if every real policy is "select accounts by a predicate,
// stamp out a child from a template", celbom is simpler and safer. If a policy
// needs genuine computation/branching, stdstarlark's full program is the escape
// hatch. Same engine, same Outcome, same fence/recompose/value-flows either way.
//
// Inputs the core hands this composer (it reads NOTHING out-of-band):
//   - req.Resource.Spec      → the BOM (the shared deployment_instance shape:
//     functional_domains[] → service_teams[]), flattened
//     to (fd, team) pairs
//   - the effective CONFIG (default ⊕ override) → {"rules": [ {name, when, child} ... ]}
//
// CEL fits the worker's "pure, no I/O" contract for the same reason Starlark does:
// the environment exposes only the `account` variable + standard CEL operators —
// no filesystem/network/clock — so composition is a deterministic pure function of
// (BOM, rules).
package celbom

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/cel-go/cel"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Kind is the composer kind this provider serves; its CRD lives in
// testfixtures/celbom.kind.json.
const Kind converge.Kind = "celbom"

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). Like stdstarlark it imports only
// sdk/* + a 3rd-party lib (cel-go) — never internal/* — so an external team could
// ship it in its own worker repo.
//
// The rule set is DECLARATIVE CONFIG, so it travels the CONFIG axis (spec). Rather
// than snapshot it, the Provider holds the default providerconfig `spec` bytes,
// swapped by OnConfig on a push so the next compose reads the operator's edit — no
// restart.
type Provider struct {
	// defaultSpec is the kind's default providerconfig (the rule set), delivered by
	// OnConfig. Work overlays the per-resource override on it per task. nil-safe (a
	// config-less provider / test holds nil and falls through to the override).
	defaultSpec json.RawMessage
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = (*Provider)(nil)

// Kind is the single (kind, version) this composer serves — explicit web-API
// version (no implicit v1 default).
func (*Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// Work runs the single composer reaction (specChange → children/configs): flatten
// the BOM, resolve the effective rule set (default ⊕ per-resource override), and fan
// it out. It delegates to composer.React so the compose body has ONE home shared with
// the unit tests.
func (p *Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	return composer{defaultSpec: p.defaultSpec}.React(ctx, req)
}

// OnConfig stores the pushed default providerconfig `spec`, so the next compose reads
// the operator's edit. An empty cfg (deleted default) parks a nil spec and compose
// retries transiently until a rule set is applied.
func (p *Provider) OnConfig(cfg converge.ProviderConfig) {
	p.defaultSpec = cfg.Spec
}

// Ready is always true: the composer is a pure, deterministic function of (BOM,
// rules) with no downstream to dial. A missing rule set is handled per-task as a
// transient retry inside Work, not as un-readiness.
func (*Provider) Ready() bool { return true }

// New builds a configured composer Provider. defaultSpec is the kind's DEFAULT
// providerconfig (the rule set); a test constructor passes the rules JSON (the dumb
// worker instead receives it via OnConfig). The in-process test harness builds the kind
// with it (via the test-only demoruntime bridge); the dumb worker uses the same Provider
// via converge.Serve.
func New(defaultSpec json.RawMessage) *Provider {
	return &Provider{defaultSpec: defaultSpec}
}

// ruleSet is the declarative composition config: a list of rules, each selecting
// accounts by a CEL predicate and stamping out a child from a template. This is
// plain structured JSON in the providerconfig spec — the whole point of the CEL
// approach vs an opaque program.
type ruleSet struct {
	Rules []rule `json:"rules"`
}

// rule is one selector+template: for every BOM team where When evaluates true,
// emit a child of Child.Kind named by the Child.Name template, with the team's
// fields stamped in. An optional DependsOn declares a dependency on another child
// (composed by the same rule, per team) plus the value flows that fill the
// dependent's spec from that upstream's status — so the rule set can express a DAG,
// not just a flat fan-out.
type rule struct {
	Name string `json:"name"`
	// When is a CEL boolean expression over the `team` variable {fd, name}, e.g.
	// `team.fd == "fd-00"` or `team.name.startsWith("admiring")`. Empty = always
	// true (select every team).
	When string `json:"when"`
	// Child is the template stamped out for each matching team. Name supports the
	// ${fd} / ${team} / ${name} placeholders (simple string substitution — NOT CEL,
	// kept trivial); Spec carries the team identity + rule name.
	Child childTemplate `json:"child"`
	// DependsOn, if set, composes one or more UPSTREAM children (per team) that Child
	// depends on, emitting a DepEdge from Child to each carrying its Flow. The engine
	// schedules the upstreams first, and once they are Ready copies their status
	// fields into Child's spec per Flow before Child is scheduled — the real
	// dependency + value-flow path. A LIST so a child can depend on several upstreams
	// (e.g. fakeapp on fakevpc's vpc_id AND fakedb's db_endpoint).
	DependsOn []dependency `json:"depends_on,omitempty"`
}

type childTemplate struct {
	Kind string `json:"kind"`
	// KindVersion is the web-API version the emitted child is applied at. REQUIRED
	// and explicit (>= 1) in the rule — the composer stamps it on each child (no
	// implicit v1 default; ApplyComposeResult rejects a 0).
	KindVersion int    `json:"kind_version"`
	Name        string `json:"name"` // template, e.g. "cel-${fd}-${team}"
	// Spec is an OPTIONAL map of LITERAL spec fields the rule bakes into the child —
	// the team-specific inputs a kind needs that aren't part of the (fd, team)
	// identity, e.g. a faketerraform's {"tar_url": "s3://.../base.tar"} or a
	// fakek8sjob's {"image": "nginx:1.27"}. Merged onto the stamped identity fields
	// (a literal here wins over the identity default). String values support the same
	// ${fd}/${team}/${name} placeholders as Name.
	Spec map[string]any `json:"spec,omitempty"`
}

// dependency is the upstream a rule's child depends on, plus the value flows from
// the upstream's status into the dependent's spec.
type dependency struct {
	Kind string `json:"kind"`
	// KindVersion is the web-API version the upstream child is applied at. REQUIRED
	// and explicit (>= 1), same contract as childTemplate.KindVersion.
	KindVersion int    `json:"kind_version"`
	Name        string `json:"name"` // template, same placeholders as childTemplate.Name
	// Spec is an OPTIONAL map of literal spec fields for the UPSTREAM (same semantics
	// as childTemplate.Spec) — e.g. the faketerraform upstream's tar_url. Merged onto
	// the upstream's stamped identity. The flowed field (what this upstream PRODUCES)
	// is left absent.
	Spec map[string]any `json:"spec,omitempty"`
	// Flow is the per-field value flows: each fills the dependent's spec[To] from
	// the upstream's status[From]. Fields are JSON Pointers ("/vpc_id"); a bare key
	// is a single-segment pointer.
	Flow []flow `json:"flow"`
}

type flow struct {
	From string `json:"from"` // upstream status field (JSON Pointer)
	To   string `json:"to"`   // dependent spec field (JSON Pointer)
}

// composer holds the live default-config source. The BOM (the shared
// deployment_instance shape) is flattened to (fd, team) pairs; each pair is exposed
// to a rule's CEL predicate as the `team` variable {fd, name} — a dynamic map, so a
// rule can select on the FD (team.fd) or the team (team.name).
type composer struct {
	// defaultSpec is the kind default providerconfig (the rule set); the per-resource
	// override merges over it per compose. nil-safe.
	defaultSpec json.RawMessage
}

func (c composer) React(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	// 1. The BOM is the resource's own spec — the shared deployment_instance shape
	//    (same as classicbom): functional_domains[] → service_teams[]. We FLATTEN it
	//    to (fd, team) pairs so a rule's CEL `team` variable can select on either.
	teams, err := flattenBOM(req.Resource.Spec)
	if err != nil {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("celbom: decode BOM spec: %w", err))
	}

	// 2. The rule set is the effective CONFIG (default ⊕ per-resource override),
	//    decoded into the ruleSet type.
	rs := converge.EffectiveConfig[ruleSet](c.defaultSpec, req.Env.ProviderConfig)
	if len(rs.Rules) == 0 {
		// TRANSIENT, not terminal: the rule set is applied out-of-band (PUT the
		// default providerconfig) and reaches the worker via OnConfig on its next
		// prime/push. A compose that fires BEFORE the config has propagated (a
		// fresh-cluster startup race) must RETRY, not fail permanently — otherwise the
		// root sticks in Failed even after the rules arrive.
		return converge.Outcome{}, fmt.Errorf("celbom: no rules yet (apply the celbom default providerconfig); will retry")
	}

	out, err := compose(rs, teams)
	if err != nil {
		// A rule error (bad CEL / template) is TRANSIENT, not terminal: we return an
		// EMPTY Outcome only via the error path — never a partial Children set, which
		// ApplyComposeResult would treat as "prune everything". The last good DAG
		// survives and it retries. (A bad BOM / missing rules above is terminal — a
		// re-run won't fix those without an edit.)
		return converge.Outcome{}, fmt.Errorf("celbom: compose: %w", err)
	}
	return out, nil
}

// compose is the FIXED fan-out: compile each rule's CEL predicate once, then for
// every (account, rule) where the predicate holds, stamp out the child template.
// No loops/branches live in the config — only the per-rule predicate does — so the
// shape is fully determined by Go and the output is deterministic.
func compose(rs ruleSet, teams []map[string]any) (converge.Outcome, error) {
	// The CEL environment exposes exactly one variable, `team` (a dynamic map with
	// fd + name), and nothing else — no I/O functions — so predicates are pure +
	// terminating.
	env, err := cel.NewEnv(cel.Variable("team", cel.DynType))
	if err != nil {
		return converge.Outcome{}, fmt.Errorf("cel env: %w", err)
	}

	// Compile every rule's predicate ONCE (not per team) — CEL compilation is the
	// expensive step; evaluation is cheap.
	type compiled struct {
		rule rule
		prg  cel.Program // nil ⇒ no `when` ⇒ always selects
	}
	progs := make([]compiled, 0, len(rs.Rules))
	for _, r := range rs.Rules {
		if r.When == "" {
			progs = append(progs, compiled{rule: r})
			continue
		}
		ast, iss := env.Compile(r.When)
		if iss != nil && iss.Err() != nil {
			return converge.Outcome{}, fmt.Errorf("rule %q: compile when: %w", r.Name, iss.Err())
		}
		// The predicate must be boolean — reject `when: "team.fd"` (a string) up
		// front so a rule can't silently select-none / select-all on a non-bool.
		if ast.OutputType() != cel.BoolType {
			return converge.Outcome{}, fmt.Errorf("rule %q: when must be boolean, got %s", r.Name, ast.OutputType())
		}
		prg, err := env.Program(ast)
		if err != nil {
			return converge.Outcome{}, fmt.Errorf("rule %q: program: %w", r.Name, err)
		}
		progs = append(progs, compiled{rule: r, prg: prg})
	}

	// Iterate teams in a STABLE order (sorted by fd, then name) × rules in their
	// declared order, so the child set is deterministic regardless of BOM/map order.
	sort.SliceStable(teams, func(i, j int) bool {
		if teams[i]["fd"] != teams[j]["fd"] {
			return teamStr(teams[i], "fd") < teamStr(teams[j], "fd")
		}
		return teamStr(teams[i], "name") < teamStr(teams[j], "name")
	})

	var out converge.Outcome
	for _, team := range teams {
		for _, c := range progs {
			match, err := evalWhen(c.prg, team)
			if err != nil {
				return converge.Outcome{}, fmt.Errorf("rule %q on team %q/%q: %w", c.rule.Name, teamStr(team, "fd"), teamStr(team, "name"), err)
			}
			if !match {
				continue
			}
			name := stamp(c.rule.Child.Name, c.rule, team)
			out.Children = append(out.Children, converge.ChildSpec{
				Kind:        converge.Kind(c.rule.Child.Kind),
				KindVersion: c.rule.Child.KindVersion, // explicit web-API version from the rule (no implicit default)
				Name:        name,
				Spec:        json.RawMessage(childSpecJSON(c.rule, team)),
				Labels:      childLabels(c.rule.Name, team), // composed_by/fd/team/policy — filterable in the UI
			})

			// Optional dependencies: compose each upstream child too and emit a DepEdge
			// from this child to it carrying its value flows. The engine schedules the
			// upstreams first and flows their statuses into this child's spec before
			// this child runs. The dependent's flowed spec fields are left ABSENT (the
			// upstreams fill them) — store.ApplyComposeResult → ValidateSpecPartial
			// skips the flowed paths, so a deferred-value field doesn't fail validation.
			// A child may depend on SEVERAL upstreams (e.g. fakeapp on fakevpc + fakedb).
			for _, dep := range c.rule.DependsOn {
				depName := stamp(dep.Name, c.rule, team)
				out.Children = append(out.Children, converge.ChildSpec{
					Kind:        converge.Kind(dep.Kind),
					KindVersion: dep.KindVersion, // explicit web-API version from the rule
					Name:        depName,
					// The UPSTREAM gets the (fd, team) identity + its own literal spec
					// (e.g. a faketerraform's tar_url) — NOT the dependent rule's `policy`
					// (that belongs to the dependent). The flowed field (e.g. vpc_id /
					// db_endpoint / image_tag) is deliberately ABSENT: this upstream
					// PRODUCES it.
					Spec:   json.RawMessage(depSpecJSON(dep, c.rule, team)),
					Labels: childLabels(c.rule.Name, team),
				})
				values := make([]converge.ValueFlow, 0, len(dep.Flow))
				for _, f := range dep.Flow {
					values = append(values, converge.ValueFlow{DependentField: f.To, SourceField: f.From})
				}
				out.Edges = append(out.Edges, converge.DepEdge{
					From:   converge.ResourceRef{Kind: converge.Kind(c.rule.Child.Kind), Name: name}, // dependent (the app)
					To:     converge.ResourceRef{Kind: converge.Kind(dep.Kind), Name: depName},       // this dependency
					Values: values,
				})
			}
		}
	}
	return out, nil
}

// flattenBOM decodes the shared deployment_instance BOM shape and flattens it to
// one map per (fd, team): {"fd": <fd name>, "name": <team name>}. That map is the
// `team` variable a rule's CEL `when` selects on (so a rule can target an FD via
// team.fd or a team via team.name) and the source for the ${fd}/${name} name
// templates. A team carries only its name in this shape (same as classicbom).
func flattenBOM(spec json.RawMessage) ([]map[string]any, error) {
	if len(spec) == 0 {
		return nil, nil
	}
	var bom struct {
		DeploymentInstance struct {
			FunctionalDomains []struct {
				Name         string `json:"name"`
				ServiceTeams []struct {
					Name string `json:"name"`
				} `json:"service_teams"`
			} `json:"functional_domains"`
		} `json:"deployment_instance"`
	}
	if err := json.Unmarshal(spec, &bom); err != nil {
		return nil, err
	}
	var out []map[string]any
	for _, fd := range bom.DeploymentInstance.FunctionalDomains {
		for _, t := range fd.ServiceTeams {
			out = append(out, map[string]any{"fd": fd.Name, "name": t.Name})
		}
	}
	return out, nil
}

// evalWhen runs a compiled predicate against one team. A nil program (no `when`)
// means "always select".
func evalWhen(prg cel.Program, team map[string]any) (bool, error) {
	if prg == nil {
		return true, nil
	}
	val, _, err := prg.Eval(map[string]any{"team": team})
	if err != nil {
		return false, err
	}
	b, ok := val.Value().(bool)
	if !ok {
		// Compile-time OutputType check should prevent this, but be defensive.
		return false, fmt.Errorf("when did not evaluate to bool (got %v)", val.Type())
	}
	return b, nil
}

// stamp does trivial ${...} placeholder substitution in a child-name template
// (NOT CEL — names are identifiers, not expressions). Supported: ${name} (the rule
// name) and any team field, e.g. ${fd} / ${team} (the team's own name is exposed
// as both "name" and "team" so a template can read either).
func stamp(tmpl string, r rule, team map[string]any) string {
	out := strings.ReplaceAll(tmpl, "${name}", r.Name)
	out = strings.ReplaceAll(out, "${fd}", teamStr(team, "fd"))
	out = strings.ReplaceAll(out, "${team}", teamStr(team, "name"))
	return out
}

// childLabels are the labels stamped on every composed child (and its dependency
// upstream): the composer identity + the (fd, team, policy) that produced it, so a
// child is filterable in the UI by who composed it / which FD / team / rule.
func childLabels(policy string, team map[string]any) map[string]string {
	return map[string]string{
		"composed_by":       string(Kind),
		"functional_domain": teamStr(team, "fd"),
		"team":              teamStr(team, "name"),
		"policy":            policy,
	}
}

// childSpecJSON builds the dependent child's spec: the (fd, team) identity + the
// rule name (the same shape stdstarlark emits, so the two composers are drop-in
// comparable), then merges the rule's optional LITERAL spec map on top — the
// team-specific inputs (e.g. a faketerraform's tar_url) the policy baked in. A
// literal value wins over the identity default of the same key; string literals get
// ${fd}/${team}/${name} substitution.
func childSpecJSON(r rule, team map[string]any) []byte {
	spec := map[string]any{
		"functional_domain": teamStr(team, "fd"),
		"team":              teamStr(team, "name"),
		"policy":            r.Name,
	}
	mergeLiteralSpec(spec, r.Child.Spec, r, team)
	b, _ := json.Marshal(spec)
	return b
}

// depSpecJSON builds an UPSTREAM dependency's spec: the (fd, team) identity, plus
// an account_id derived from the team name (the demo's fakevpc/fakedb upstreams
// require it — they derive their produced id/endpoint from it), then merges the
// dependency's optional LITERAL spec map on top (e.g. a faketerraform upstream's
// tar_url). It deliberately omits the dependent rule's `policy` (the dependent's,
// not the upstream's) and the flowed field (which the upstream PRODUCES into its
// status). Kept separate from childSpecJSON so the two never drift.
func depSpecJSON(dep dependency, r rule, team map[string]any) []byte {
	spec := map[string]any{
		"functional_domain": teamStr(team, "fd"),
		"team":              teamStr(team, "name"),
		"account_id":        teamStr(team, "name"),
	}
	mergeLiteralSpec(spec, dep.Spec, r, team)
	b, _ := json.Marshal(spec)
	return b
}

// mergeLiteralSpec overlays the rule's literal spec map onto the identity-derived
// spec, in place. A string literal is run through stamp() so ${fd}/${team}/${name}
// work in spec values (e.g. tar_url "s3://.../${team}.tar"); non-string values are
// copied as-is. A literal key overrides the identity default of the same name.
func mergeLiteralSpec(dst map[string]any, literal map[string]any, r rule, team map[string]any) {
	for k, v := range literal {
		if s, ok := v.(string); ok {
			dst[k] = stamp(s, r, team)
		} else {
			dst[k] = v
		}
	}
}

func teamStr(team map[string]any, key string) string {
	if v, ok := team[key]; ok {
		return fmt.Sprint(v)
	}
	return ""
}
