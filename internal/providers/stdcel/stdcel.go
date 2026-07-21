// Package stdcel is the GENERIC, kro-style composer: it turns a RESOURCE GRAPH
// DEFINITION expressed entirely as DATA — a set of child resources whose fields are
// CEL expressions over the instance's spec and over each other — into a child DAG,
// with no per-kind Go. One provider serves ANY composition.
//
// It is the generalisation of the celbom demo. celbom uses CEL only for a one-line
// `when` selector and hardcodes a fixed BOM fan-out; stdcel makes the WHOLE graph
// data: you declare resources (any kinds) and wire ANY field between them with CEL —
// exactly kro's ResourceGraphDefinition, on the converge engine.
//
// The two axes (same as every std* composer):
//   - spec   → the INSTANCE inputs (the stdcel resource's own spec). In CEL these are
//     `schema.spec.*` — kro's top-level schema. User-supplied, per resource.
//   - config → the GRAPH DEFINITION (the kind's default providerconfig ⊕ a
//     per-resource override): `{"resources":[{id,template{kind,name,spec}}...]}`.
//     Edit + re-apply to change the composition with NO worker redeploy — the
//     "composition is data" property, generalised from a fixed fan-out to a graph.
//
// How the graph wires (the kro primitive):
//   - `${schema.spec.x}`   reads the instance's own spec.
//   - `${<id>.spec.x}`     reads another graph resource's RESOLVED spec — a
//     compile-time wiring resolved during compose (topological order), no edge.
//   - `${<id>.status.x}`   reads another resource's RUNTIME status — not knowable at
//     compose time, so it becomes a DepEdge + ValueFlow: the engine schedules `<id>`
//     first and flows its status[x] into this child's spec once `<id>` is Ready.
//
// The provider INFERS the dependency order from these references (a resource reading
// `${other.*}` depends on `other`), rejects cycles, and stamps each child's spec.
//
// CEL fits the worker's pure, no-I/O contract: the environment exposes only the
// `schema` variable + the already-resolved sibling maps — no filesystem/network/
// clock — so composition is a deterministic pure function of (instance spec, graph).
// CEL is non-Turing-complete (it provably terminates, can't loop or recurse), so a
// graph is safe to run on arbitrary operator-supplied config.
//
// It imports only sdk/* + cel-go — never internal/* — exactly what an external team
// ships. It serves the single fixed kind "stdcel".
package stdcel

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). It is the GENERIC kro-style CEL
// composer, parameterised by the kind's LIVE default CONFIG (the resource graph
// definition). The graph arrives via OnConfig (the SDK delivers the default at startup +
// on every edit), so a compose reads the latest an operator has applied with no restart.
//
// Imports only sdk/* + cel-go — never internal/* — exactly what an external team
// ships. It serves the single fixed kind "stdcel".
type Provider struct {
	// mu guards defaultSpec (OnConfig writes on the broker goroutine, Work reads per task).
	mu sync.RWMutex
	// defaultSpec is the graph OnConfig last delivered (the default providerconfig spec);
	// nil until the first delivery, and nil again when the default is deleted.
	defaultSpec json.RawMessage
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = (*Provider)(nil)

// Kind is the single (kind, version) this provider serves — the fixed composer kind
// "stdcel" at its one web-API version. PURE per the contract: no env, no dial.
func (*Provider) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: Kind, Version: KindVersion}
}

// Work runs the single "compose" reaction: it reads the effective graph (the
// OnConfig-pushed spec when present, else the live default source) and fans the
// resource's spec out into a child DAG. The reaction dispatch is explicit so a future
// second reaction on this kind is a switch arm, not a silent fall-through.
func (p *Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	switch req.Reaction {
	case reactionCompose:
		return p.composerForTask().React(ctx, req)
	default:
		// Fail closed: an unknown reaction must not silently no-op (which the core would
		// read as "composed to zero children" → prune everything).
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdcel: unknown reaction %q", req.Reaction))
	}
}

// OnConfig records the graph definition (Spec) the broker delivers for this pair. An
// empty cfg (nil Spec) = the default was deleted; recorded as such so Work observes the
// removal (a compose then transient-retries "no graph yet" until a graph is re-applied,
// rather than reusing a stale one). stdcel ignores cfg.Data — the graph is structured
// JSON on the config axis, never an opaque bundle.
func (p *Provider) OnConfig(cfg converge.ProviderConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.defaultSpec = cfg.Spec
}

// Ready is always true: the graph is a compose-time input validated per task (a
// missing graph transient-retries), so the composer has no downstream to dial and no
// boot prerequisite to gate on.
func (*Provider) Ready() bool { return true }

// composerForTask builds the per-task composer from the provider's current default graph
// (from OnConfig). Read under the lock so a concurrent OnConfig can't tear the read.
func (p *Provider) composerForTask() composer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return composer{defaultSpec: p.defaultSpec}
}

// composer holds the graph-definition `spec` (default) for one task. Stateless per task.
type composer struct {
	// defaultSpec is the kind's default providerconfig graph; the per-resource override
	// merges over it per compose. nil-safe.
	defaultSpec json.RawMessage
}

func (c composer) React(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	// 1. The instance inputs are the resource's own spec — exposed to CEL as
	//    `schema.spec`. A resource with no spec is fine (a graph may reference none).
	instance := map[string]any{}
	if len(req.Resource.Spec) > 0 {
		if err := json.Unmarshal(req.Resource.Spec, &instance); err != nil {
			return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdcel: decode instance spec: %w", err))
		}
	}

	// 2. The graph definition is the effective CONFIG (default ⊕ per-resource
	//    override), decoded into the graph type.
	g := converge.EffectiveConfig[graph](c.defaultSpec, req.Env.ProviderConfig)
	if len(g.Resources) == 0 {
		// TRANSIENT, not terminal: the graph is applied out-of-band (PUT the default
		// providerconfig) and reaches the worker via OnConfig on its next prime/push. A
		// compose that fires BEFORE the graph has propagated (a fresh-cluster startup
		// race) must RETRY, not fail permanently — otherwise the root sticks in Failed
		// even after the graph arrives.
		return converge.Outcome{}, fmt.Errorf("stdcel: no graph yet (apply the stdcel default providerconfig); will retry")
	}

	out, err := compose(g, instance)
	if err != nil {
		// A malformed graph (bad CEL, unknown ref, cycle, duplicate id) is TERMINAL:
		// a same-generation re-run won't fix it — it needs a graph edit (which is a
		// config change, re-delivered live). Returning an EMPTY Outcome via the error
		// path (never a partial Children set, which ApplyComposeResult would treat as
		// "prune everything") keeps the last good DAG until the graph is corrected.
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdcel: compose: %w", err))
	}
	return out, nil
}

// exprRe matches one ${ ... } CEL expression. The body is non-greedy and forbids a
// literal '}' so adjacent expressions in one string don't merge; a real CEL body
// never needs a bare '}' (map/list literals are rare in a field reference and can
// use the whole-value form).
var exprRe = regexp.MustCompile(`\$\{([^}]+)\}`)

// refRe extracts a graph reference — <id>.spec.* / <id>.status.* / schema.spec.* —
// from the START of a CEL expression, so dependency inference doesn't need a full
// CEL parser: an expression that reads `vpc.status.id` depends on `vpc`. The leading
// identifier is the referenced id (or the reserved `schema`); `.status` vs `.spec`
// decides edge-vs-inline.
var refRe = regexp.MustCompile(`\b([A-Za-z_]\w*)\.(status|spec)\b`)

// schemaVar is the reserved CEL identifier for the instance's own inputs
// (kro's `schema.spec`). It is never a graph resource id.
const schemaVar = "schema"

// compose is the whole pipeline: validate the graph, infer the dependency order
// from the CEL references, then in that order resolve each child's spec (inlining
// resolved sibling specs + the instance) and emit the child + a DepEdge/ValueFlow
// for every `${<id>.status.*}` reference. Deterministic: children and edges come out
// in topological-then-id order regardless of the graph's declaration order.
func compose(g graph, instance map[string]any) (converge.Outcome, error) {
	byID := make(map[string]resourceDef, len(g.Resources))
	for _, r := range g.Resources {
		if r.Id == "" {
			return converge.Outcome{}, fmt.Errorf("resource with empty id")
		}
		if r.Template.Kind == "" {
			return converge.Outcome{}, fmt.Errorf("resource %q: template.kind is required", r.Id)
		}
		if r.Template.KindVersion < 1 {
			return converge.Outcome{}, fmt.Errorf("resource %q: template.kind_version is required and must be >= 1 (no implicit v1 default)", r.Id)
		}
		if r.Template.Name == "" {
			return converge.Outcome{}, fmt.Errorf("resource %q: template.name is required", r.Id)
		}
		if _, dup := byID[r.Id]; dup {
			return converge.Outcome{}, fmt.Errorf("duplicate resource id %q", r.Id)
		}
		byID[r.Id] = r
	}

	// Infer the dependency graph: id → the set of sibling ids it references (via
	// ${other.spec|status.*}). schema references are not deps (the instance is always
	// available). statusReads[id][refID] is the set of status field paths that id
	// reads from refID, used to build the compose-time status placeholder so the CEL
	// expressions compile against a null of the right shape.
	deps := make(map[string]map[string]bool, len(byID))            // id → referenced sibling ids
	statusReads := make(map[string]map[string][]string, len(byID)) // id → refID → status field paths read
	for id, r := range byID {
		deps[id] = map[string]bool{}
		statusReads[id] = map[string][]string{}
		for refID, ri := range templateRefIDs(r.Template) {
			if refID == id {
				return converge.Outcome{}, fmt.Errorf("resource %q references itself", id)
			}
			if _, ok := byID[refID]; !ok {
				return converge.Outcome{}, fmt.Errorf("resource %q references unknown resource %q", id, refID)
			}
			deps[id][refID] = true
			if len(ri.statusPaths) > 0 {
				statusReads[id][refID] = ri.statusPaths
			}
		}
	}

	order, err := topoSort(byID, deps)
	if err != nil {
		return converge.Outcome{}, err
	}

	// Resolve each child in dependency order. resolvedSpec[id] is the child's spec as
	// a decoded map, so a later sibling reading ${id.spec.x} sees the resolved value.
	// resolvedName[id] is the stamped child name, needed to build DepEdges.
	resolvedSpec := make(map[string]map[string]any, len(byID))
	resolvedName := make(map[string]string, len(byID))

	var out converge.Outcome
	for _, id := range order {
		r := byID[id]

		// The CEL activation for THIS resource: the instance (`schema`) plus every
		// already-resolved sibling as {spec, status}. status is a typed-nil placeholder
		// at compose time (the value flows in at runtime), exposed so a whole-value
		// `${dep.status.x}` compiles/evaluates to null and is filled by the ValueFlow —
		// the child's spec carries the flowed field as null until the engine overwrites it.
		activation := map[string]any{schemaVar: map[string]any{"spec": instance}}
		for dep := range deps[id] {
			activation[dep] = map[string]any{
				"spec":   resolvedSpec[dep],
				"status": statusPlaceholder(statusReads[id][dep]),
			}
		}

		// One CEL env per resource, declaring `schema` + its sibling ids, reused for
		// the name and every spec/label leaf (env construction is the costly step).
		env, err := envFor(sortedKeys(deps[id]))
		if err != nil {
			return converge.Outcome{}, fmt.Errorf("resource %q: cel env: %w", id, err)
		}

		name, err := evalString(env, r.Template.Name, activation)
		if err != nil {
			return converge.Outcome{}, fmt.Errorf("resource %q: name: %w", id, err)
		}
		spec, err := resolveValue(env, decodeAny(r.Template.Spec), activation)
		if err != nil {
			return converge.Outcome{}, fmt.Errorf("resource %q: spec: %w", id, err)
		}
		specMap, _ := spec.(map[string]any)
		if specMap == nil {
			specMap = map[string]any{}
		}
		resolvedSpec[id] = specMap
		resolvedName[id] = name

		labels, err := resolveLabels(env, r, activation)
		if err != nil {
			return converge.Outcome{}, fmt.Errorf("resource %q: labels: %w", id, err)
		}
		out.Children = append(out.Children, converge.ChildSpec{
			Kind:        converge.Kind(r.Template.Kind),
			KindVersion: r.Template.KindVersion, // explicit web-API version from the graph (no implicit default)
			Name:        name,
			Spec:        mustJSON(specMap),
			Labels:      labels,
		})

		// Emit a DepEdge for every dependency THIS child reads the RUNTIME STATUS of —
		// those are the only refs that must wait at runtime (a ${dep.spec.*}-only ref is
		// resolved at compose time and baked in, so it needs compose ORDERING but no
		// runtime edge). A ${dep.status.X} whole-value spec leaf becomes a value flow on
		// the edge: the engine schedules `dep` first and, once Ready, copies its
		// status.X into THIS child's spec at the leaf's location. The dependent field
		// (where the expression sits) and the source field (X) can differ — e.g.
		// spec.built_image = ${tf.status.image_tag}. Deterministic order: deps sorted,
		// flows sorted by dependent pointer.
		// specFlows walks the TEMPLATE spec (the un-resolved ${...} leaves) — after
		// resolution a status leaf is already null, so the reference is gone. The
		// dependent JSON Pointer is structural, identical in template and resolved spec.
		flowsByDep := map[string][]converge.ValueFlow{}
		for _, fl := range specFlows(decodeAny(r.Template.Spec)) {
			flowsByDep[fl.depID] = append(flowsByDep[fl.depID], converge.ValueFlow{
				DependentField: fl.dependentPtr,
				SourceField:    jsonPtr(fl.sourcePath),
			})
		}
		for _, depID := range sortedKeys(statusReads[id]) {
			values := flowsByDep[depID]
			sort.Slice(values, func(i, j int) bool { return values[i].DependentField < values[j].DependentField })
			out.Edges = append(out.Edges, converge.DepEdge{
				From:   converge.ResourceRef{Kind: converge.Kind(r.Template.Kind), Name: name},
				To:     converge.ResourceRef{Kind: converge.Kind(byID[depID].Template.Kind), Name: resolvedName[depID]},
				Values: values,
			})
		}
	}
	return out, nil
}

// ─────────────────────────────────────────────────────────────────────────
// CEL environment + evaluation
// ─────────────────────────────────────────────────────────────────────────

// envFor returns a CEL env declaring `schema` plus one dyn variable per sibling id —
// exactly the identifiers a resource's expressions may reference. cel-go needs every
// variable declared, but a graph's ids are DATA, so the set is built per resource
// from its known dependency set (all dyn, since child specs/statuses are arbitrary
// JSON). Built ONCE per resource and reused for its name + every spec leaf, so a
// resource with N expressions pays one env construction, not N.
func envFor(ids []string) (*cel.Env, error) {
	opts := make([]cel.EnvOption, 0, len(ids)+1)
	opts = append(opts, cel.Variable(schemaVar, cel.DynType))
	for _, id := range ids {
		opts = append(opts, cel.Variable(id, cel.DynType))
	}
	return cel.NewEnv(opts...)
}

// evalString evaluates a template string to its string form — a bare literal (no
// ${...}) verbatim, a whole-string/interpolated expression stringified. Used for the
// child name + label values (always strings).
func evalString(env *cel.Env, tmpl string, activation map[string]any) (string, error) {
	v, err := evalTemplate(env, tmpl, activation)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(v), nil
}

// evalTemplate is the shared string-template evaluator used for names, labels, and
// string spec leaves. If the WHOLE string is one expression it returns the CEL
// result's native value (so a whole-value "${schema.spec.count}" stays an int);
// otherwise it interpolates each ${...} as a string into the surrounding text.
func evalTemplate(env *cel.Env, tmpl string, activation map[string]any) (any, error) {
	loc := exprRe.FindAllStringSubmatchIndex(tmpl, -1)
	if len(loc) == 0 {
		return tmpl, nil // literal
	}
	// Whole-value form: the string is exactly one ${...} with nothing around it.
	if len(loc) == 1 && loc[0][0] == 0 && loc[0][1] == len(tmpl) {
		return evalExpr(env, tmpl[loc[0][2]:loc[0][3]], activation)
	}
	// Interpolation: replace each ${...} with the string form of its result.
	var b strings.Builder
	last := 0
	for _, m := range loc {
		b.WriteString(tmpl[last:m[0]])
		val, err := evalExpr(env, tmpl[m[2]:m[3]], activation)
		if err != nil {
			return nil, err
		}
		fmt.Fprint(&b, val)
		last = m[1]
	}
	b.WriteString(tmpl[last:])
	return b.String(), nil
}

// evalExpr compiles + evaluates a single CEL expression against the activation using
// the resource's pre-built env, returning the result as a plain Go value (via
// ConvertToNative to `any`).
func evalExpr(env *cel.Env, expr string, activation map[string]any) (any, error) {
	ast, iss := env.Compile(strings.TrimSpace(expr))
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("compile %q: %w", expr, iss.Err())
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("program %q: %w", expr, err)
	}
	val, _, err := prg.Eval(activation)
	if err != nil {
		return nil, fmt.Errorf("eval %q: %w", expr, err)
	}
	return refToNative(val)
}

// refToNative converts a CEL result to a plain Go value the child spec can carry
// (marshals cleanly to JSON). A CEL null — which is what a compose-time status
// placeholder field evaluates to (${dep.status.x} before the value flows in) —
// becomes Go nil, so the child spec holds the flowed field as JSON null until the
// engine's ValueFlow overwrites it at runtime. Without this special-case cel-go
// converts null to the zero value of its inferred type (e.g. 0 / ""), which would
// bake a wrong literal into the spec instead of a deferred field.
func refToNative(val ref.Val) (any, error) {
	if val == nil || val.Type() == types.NullType {
		return nil, nil
	}
	native, err := val.ConvertToNative(anyType)
	if err != nil {
		return nil, fmt.Errorf("convert %v result: %w", val.Type(), err)
	}
	return native, nil
}

// resolveValue walks a decoded template value and evaluates every string leaf as a
// template (recursing into maps + slices). Non-string leaves pass through. This is
// how a child's whole spec — nested objects/arrays included — gets its ${...}
// expressions resolved.
func resolveValue(env *cel.Env, v any, activation map[string]any) (any, error) {
	switch t := v.(type) {
	case string:
		return evalTemplate(env, t, activation)
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, sub := range t {
			rv, err := resolveValue(env, sub, activation)
			if err != nil {
				return nil, err
			}
			out[k] = rv
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, sub := range t {
			rv, err := resolveValue(env, sub, activation)
			if err != nil {
				return nil, err
			}
			out[i] = rv
		}
		return out, nil
	default:
		return v, nil
	}
}

// resolveLabels stamps the child's labels: the provider's own identity labels
// (composed_by, graph resource id) plus the template's own labels (values
// interpolated). The provider labels win on a key clash.
func resolveLabels(env *cel.Env, r resourceDef, activation map[string]any) (map[string]string, error) {
	labels := make(map[string]string, len(r.Template.Labels)+2)
	for k, v := range r.Template.Labels {
		s, err := evalTemplate(env, v, activation)
		if err != nil {
			return nil, err
		}
		labels[k] = fmt.Sprint(s)
	}
	labels["composed_by"] = string(Kind)
	labels["graph_id"] = r.Id
	return labels, nil
}

// ─────────────────────────────────────────────────────────────────────────
// Reference extraction + topological sort
// ─────────────────────────────────────────────────────────────────────────

// templateRefIDs finds which sibling ids a template references (via any
// ${id.spec|status.*} expression in its name, spec, or labels) — the DEPENDENCY set
// for ordering. Per id it also records the set of STATUS field paths read anywhere,
// used to build the compose-time status placeholder so the expressions compile. It
// does NOT need a full CEL parse — a field reference always starts with the
// identifier — so inference is cheap and independent of cel-go's AST internals.
//
// Returns id → {statusPaths: the status field paths read for it}. The `schema`
// pseudo-id is excluded (the instance is always available, never a dependency).
func templateRefIDs(t template) map[string]*refInfo {
	refs := map[string]*refInfo{}
	get := func(id string) *refInfo {
		ri := refs[id]
		if ri == nil {
			ri = &refInfo{}
			refs[id] = ri
		}
		return ri
	}
	collect := func(s string) {
		for _, m := range exprRe.FindAllStringSubmatch(s, -1) {
			for _, rm := range refRe.FindAllStringSubmatch(m[1], -1) {
				id, kind := rm[1], rm[2]
				if id == schemaVar {
					continue
				}
				ri := get(id)
				if kind == "status" {
					ri.statusPaths = appendUnique(ri.statusPaths, trailingPath(m[1], rm[0]))
				}
			}
		}
	}
	collect(t.Name)
	for _, v := range t.Labels {
		collect(v)
	}
	_ = walkStrings(decodeAny(t.Spec), func(s string) { collect(s) })
	return refs
}

// refInfo is what a template reads from one sibling id: the status field paths (for
// the compose-time placeholder). Presence in the map alone marks a dependency.
type refInfo struct {
	statusPaths []string
}

// flow is one structural value flow discovered in a spec: a whole-value
// ${<depID>.status.<sourcePath>} leaf at JSON-pointer dependentPtr in the child's
// spec. The dependent field (WHERE the expression sits) and the source field (WHAT
// status field it reads) can differ — e.g. spec.built_image = ${tf.status.image_tag}
// flows /image_tag → /built_image — which a source-path-only inference would miss.
type flow struct {
	depID        string
	dependentPtr string // JSON Pointer into the child's spec (the leaf's location)
	sourcePath   string // dot path into the dependency's status
}

// specFlows walks a decoded spec tree and returns a value flow for every leaf that is
// EXACTLY one ${<depID>.status.<path>} whole-value expression (an interpolated or
// spec-ref leaf is resolved inline, not flowed). The dependentPtr is the leaf's
// structural location, so the flow targets the right spec field regardless of the
// source field's name. Only whole-value status leaves can flow (a runtime status
// value can't be spliced into the middle of a string at schedule time).
func specFlows(spec any) []flow {
	var flows []flow
	var walk func(v any, ptr string)
	walk = func(v any, ptr string) {
		switch t := v.(type) {
		case string:
			if depID, src, ok := wholeStatusRef(t); ok {
				flows = append(flows, flow{depID: depID, dependentPtr: ptr, sourcePath: src})
			}
		case map[string]any:
			for k, sub := range t {
				walk(sub, ptr+"/"+escapePtr(k))
			}
		case []any:
			for i, sub := range t {
				walk(sub, ptr+"/"+itoa(i))
			}
		}
	}
	walk(spec, "")
	return flows
}

// wholeStatusRef reports whether s is EXACTLY one ${<id>.status.<path>} expression
// (nothing around it, no operators/functions inside), returning the id and the dot
// source path. A bare ${id.status} (no field) or any compound expression returns
// false — those are resolved inline against the null placeholder, not flowed.
func wholeStatusRef(s string) (id, sourcePath string, ok bool) {
	m := exprRe.FindStringSubmatch(s) // [full, body]
	if len(m) < 2 || m[0] != s {      // not exactly one whole-string expression
		return "", "", false
	}
	body := strings.TrimSpace(m[1])
	rm := refRe.FindStringSubmatch(body) // [match, id, "status"|"spec"]
	if len(rm) < 3 || rm[2] != "status" {
		return "", "", false
	}
	src := trailingPath(body, rm[0])
	if src == "" {
		return "", "", false // ${id.status} whole map — no single-field flow
	}
	// The body must be JUST the reference (no arithmetic/calls that would make the
	// runtime status value un-substitutable). Compare against the canonical form.
	if body != rm[1]+".status."+src {
		return "", "", false
	}
	return rm[1], src, true
}

// trailingPath returns the dot path after the "<id>.status."/"<id>.spec." prefix in
// an expression — the status/spec field being read (a value-flow field). It reads up
// to the first CEL token boundary (whitespace, operator, paren, bracket, comma), so
// `vpc.status.endpoint.host + "x"` yields "endpoint.host". Empty if the reference is
// bare (`vpc.status`), which reads the whole map (no single-field flow).
func trailingPath(expr, prefix string) string {
	_, rest, found := strings.Cut(expr, prefix)
	if !found {
		return ""
	}
	rest = strings.TrimPrefix(rest, ".")
	// Cut at the first non-path character.
	if end := strings.IndexFunc(rest, func(r rune) bool { return !isPathRune(r) }); end >= 0 {
		rest = rest[:end]
	}
	return rest
}

// isPathRune reports whether r can appear in a dotted field path (identifier chars +
// the '.' segment separator) — the character class trailingPath scans.
func isPathRune(r rune) bool {
	return r == '.' || r == '_' ||
		(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

// topoSort returns the resource ids in dependency order (a dependency before its
// dependents), then alphabetically within an independent set for determinism.
// Rejects a cycle (which stdcel can't schedule) with the members named.
func topoSort(byID map[string]resourceDef, deps map[string]map[string]bool) ([]string, error) {
	// Kahn's algorithm on the "depends-on" edges. indeg[x] = number of unresolved
	// dependencies of x.
	indeg := make(map[string]int, len(byID))
	dependents := make(map[string][]string, len(byID)) // dep → ids that depend on it
	for id := range byID {
		indeg[id] = len(deps[id])
		for d := range deps[id] {
			dependents[d] = append(dependents[d], id)
		}
	}
	// Ready set = zero unresolved deps, kept sorted so output is deterministic.
	var ready []string
	for id, n := range indeg {
		if n == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)

	order := make([]string, 0, len(byID))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		next := dependents[id]
		sort.Strings(next)
		for _, dep := range next {
			indeg[dep]--
			if indeg[dep] == 0 {
				ready = insertSorted(ready, dep)
			}
		}
	}
	if len(order) != len(byID) {
		var cyc []string
		for id, n := range indeg {
			if n > 0 {
				cyc = append(cyc, id)
			}
		}
		sort.Strings(cyc)
		return nil, fmt.Errorf("dependency cycle among resources %v", cyc)
	}
	return order, nil
}
