package stdstarlark

import (
	"encoding/json"
	"fmt"

	"github.com/salesforce/converge/sdk-go/converge"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
)

// convert.go bridges Go ⇄ Starlark: it feeds the resource spec + config in as
// Starlark values and marshals compose()'s returned struct out into the typed
// Outcome (Children / Edges / Configs). Specs/labels round-trip as JSON-native maps,
// so a Starlark dict becomes the same json.RawMessage a Go composer emits.

// goToStarlark converts a JSON-decoded Go value (map/slice/string/float/bool/nil)
// into the equivalent Starlark value, so the spec/config are navigable in Starlark
// both as attributes (v.field, from a struct) and as dict entries.
func goToStarlark(v any) starlark.Value {
	switch t := v.(type) {
	case nil:
		return starlark.None
	case bool:
		return starlark.Bool(t)
	case float64:
		// JSON has one number type, but Starlark distinguishes int from float and
		// programs expect INTEGERS for counts/indices (range(config.count),
		// list[i], %d). Map an integral value to starlark.Int so
		// `for i in range(config.count)` works; keep a genuinely fractional value a
		// Float. (int64 range covers any count a config realistically carries; a huge
		// non-integral float stays a Float.)
		if t == float64(int64(t)) {
			return starlark.MakeInt64(int64(t))
		}
		return starlark.Float(t)
	case string:
		return starlark.String(t)
	case []any:
		elems := make([]starlark.Value, len(t))
		for i, e := range t {
			elems[i] = goToStarlark(e)
		}
		return starlark.NewList(elems)
	case map[string]any:
		// Expose a map as a starlarkstruct so a program can read it ergonomically as
		// attributes (spec.name, config.rules) — the same form a Go struct would give.
		d := starlark.StringDict{}
		for k, e := range t {
			d[k] = goToStarlark(e)
		}
		return starlarkstruct.FromStringDict(starlarkstruct.Default, d)
	default:
		return starlark.None
	}
}

// starlarkToGo is goToStarlark's inverse for the leaf spec/labels values a program
// builds (dicts, lists, scalars, structs) → JSON-native Go, ready to json.Marshal
// into ChildSpec.Spec / ProviderConfigSpec.Spec (which are `any`, marshalled at the
// DB boundary by the core).
func starlarkToGo(v starlark.Value) (any, error) {
	switch t := v.(type) {
	case starlark.NoneType:
		return nil, nil
	case starlark.Bool:
		return bool(t), nil
	case starlark.Int:
		i, _ := t.Int64()
		return i, nil
	case starlark.Float:
		return float64(t), nil
	case starlark.String:
		return string(t), nil
	case *starlark.List:
		out := make([]any, 0, t.Len())
		for i := 0; i < t.Len(); i++ {
			e, err := starlarkToGo(t.Index(i))
			if err != nil {
				return nil, err
			}
			out = append(out, e)
		}
		return out, nil
	case *starlark.Dict:
		out := map[string]any{}
		for _, item := range t.Items() {
			k, ok := starlark.AsString(item[0])
			if !ok {
				return nil, fmt.Errorf("dict key not a string: %v", item[0])
			}
			e, err := starlarkToGo(item[1])
			if err != nil {
				return nil, err
			}
			out[k] = e
		}
		return out, nil
	case *starlarkstruct.Struct:
		out := map[string]any{}
		for _, name := range t.AttrNames() {
			a, err := t.Attr(name)
			if err != nil {
				return nil, err
			}
			e, err := starlarkToGo(a)
			if err != nil {
				return nil, err
			}
			out[name] = e
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported Starlark value %s", v.Type())
	}
}

// outcomeFromStarlark marshals compose()'s returned struct {children, edges, configs}
// into the typed converge.Outcome. Each is an optional list; a missing attribute is
// treated as empty (so a flat fan-out with no edges is fine).
func outcomeFromStarlark(v starlark.Value) (converge.Outcome, error) {
	s, ok := v.(*starlarkstruct.Struct)
	if !ok {
		return converge.Outcome{}, fmt.Errorf("compose() must return a struct, got %s", v.Type())
	}
	var out converge.Outcome
	var err error
	if out.Children, err = childrenFrom(s); err != nil {
		return converge.Outcome{}, err
	}
	if out.Edges, err = edgesFrom(s); err != nil {
		return converge.Outcome{}, err
	}
	if out.Configs, err = configsFrom(s); err != nil {
		return converge.Outcome{}, err
	}
	return out, nil
}

// attrList returns struct field `name` as a []starlark.Value, or nil if absent.
func attrList(s *starlarkstruct.Struct, name string) ([]starlark.Value, error) {
	v, err := s.Attr(name)
	if err != nil {
		return nil, nil // absent attribute → empty
	}
	l, ok := v.(*starlark.List)
	if !ok {
		return nil, fmt.Errorf("compose() result .%s must be a list, got %s", name, v.Type())
	}
	out := make([]starlark.Value, l.Len())
	for i := range out {
		out[i] = l.Index(i)
	}
	return out, nil
}

// attrStr returns a required string field of a struct.
func attrStr(s *starlarkstruct.Struct, name string) (string, error) {
	v, err := s.Attr(name)
	if err != nil {
		return "", fmt.Errorf("missing .%s", name)
	}
	str, ok := starlark.AsString(v)
	if !ok {
		return "", fmt.Errorf(".%s must be a string, got %s", name, v.Type())
	}
	return str, nil
}

// attrInt reads an integer struct field. kind_version is REQUIRED and explicit
// (>= 1): a Starlark composer must set it on each emitted child/config — there is
// no implicit v1 default.
func attrInt(s *starlarkstruct.Struct, name string) (int, error) {
	v, err := s.Attr(name)
	if err != nil {
		return 0, fmt.Errorf("missing .%s", name)
	}
	i, ok := v.(starlark.Int)
	if !ok {
		return 0, fmt.Errorf(".%s must be an int, got %s", name, v.Type())
	}
	n, ok := i.Int64()
	if !ok {
		return 0, fmt.Errorf(".%s is out of range", name)
	}
	return int(n), nil
}

// specJSON marshals a struct field's value (dict/scalars/struct) to json.RawMessage;
// nil when the field is absent.
func specJSON(s *starlarkstruct.Struct, name string) (json.RawMessage, error) {
	v, err := s.Attr(name)
	if err != nil {
		return nil, nil
	}
	g, err := starlarkToGo(v)
	if err != nil {
		return nil, fmt.Errorf(".%s: %w", name, err)
	}
	if g == nil {
		return nil, nil
	}
	return json.Marshal(g)
}

func childrenFrom(s *starlarkstruct.Struct) ([]converge.ChildSpec, error) {
	items, err := attrList(s, "children")
	if err != nil {
		return nil, err
	}
	out := make([]converge.ChildSpec, 0, len(items))
	for i, it := range items {
		cs, ok := it.(*starlarkstruct.Struct)
		if !ok {
			return nil, fmt.Errorf("children[%d] not a struct", i)
		}
		kind, err := attrStr(cs, "kind")
		if err != nil {
			return nil, fmt.Errorf("children[%d]: %w", i, err)
		}
		kindVersion, err := attrInt(cs, "kind_version")
		if err != nil {
			return nil, fmt.Errorf("children[%d]: %w", i, err)
		}
		name, err := attrStr(cs, "name")
		if err != nil {
			return nil, fmt.Errorf("children[%d]: %w", i, err)
		}
		spec, err := specJSON(cs, "spec")
		if err != nil {
			return nil, fmt.Errorf("children[%d]: %w", i, err)
		}
		labels, err := labelsFrom(cs)
		if err != nil {
			return nil, fmt.Errorf("children[%d]: %w", i, err)
		}
		out = append(out, converge.ChildSpec{Kind: converge.Kind(kind), KindVersion: kindVersion, Name: name, Spec: spec, Labels: labels})
	}
	return out, nil
}

// labelsFrom reads the optional `labels` field on a child struct (a Starlark dict of
// string→string) into a Go map. Absent → nil. A non-string value is coerced via fmt
// so a program can stamp e.g. an int label without error.
func labelsFrom(s *starlarkstruct.Struct) (map[string]string, error) {
	v, err := s.Attr("labels")
	if err != nil {
		return nil, nil // absent → no labels
	}
	g, err := starlarkToGo(v)
	if err != nil {
		return nil, fmt.Errorf(".labels: %w", err)
	}
	m, ok := g.(map[string]any)
	if !ok {
		return nil, fmt.Errorf(".labels must be a dict, got %T", g)
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		out[k] = fmt.Sprint(val)
	}
	return out, nil
}

func configsFrom(s *starlarkstruct.Struct) ([]converge.ProviderConfigSpec, error) {
	items, err := attrList(s, "configs")
	if err != nil {
		return nil, err
	}
	out := make([]converge.ProviderConfigSpec, 0, len(items))
	for i, it := range items {
		cs, ok := it.(*starlarkstruct.Struct)
		if !ok {
			return nil, fmt.Errorf("configs[%d] not a struct", i)
		}
		kind, err := attrStr(cs, "kind")
		if err != nil {
			return nil, fmt.Errorf("configs[%d]: %w", i, err)
		}
		kindVersion, err := attrInt(cs, "kind_version")
		if err != nil {
			return nil, fmt.Errorf("configs[%d]: %w", i, err)
		}
		name, err := attrStr(cs, "name")
		if err != nil {
			return nil, fmt.Errorf("configs[%d]: %w", i, err)
		}
		spec, err := specJSON(cs, "spec")
		if err != nil {
			return nil, fmt.Errorf("configs[%d]: %w", i, err)
		}
		out = append(out, converge.ProviderConfigSpec{Name: name, Kind: converge.Kind(kind), KindVersion: kindVersion, Spec: spec})
	}
	return out, nil
}

func edgesFrom(s *starlarkstruct.Struct) ([]converge.DepEdge, error) {
	items, err := attrList(s, "edges")
	if err != nil {
		return nil, err
	}
	out := make([]converge.DepEdge, 0, len(items))
	for i, it := range items {
		es, ok := it.(*starlarkstruct.Struct)
		if !ok {
			return nil, fmt.Errorf("edges[%d] not a struct", i)
		}
		// dependent/dependency are structs {kind, name}. ("from" is a Starlark
		// reserved word, so the field is named `dependent`; the edge points
		// dependent → dependency.)
		from, err := refFrom(es, "dependent")
		if err != nil {
			return nil, fmt.Errorf("edges[%d]: %w", i, err)
		}
		to, err := refFrom(es, "dependency")
		if err != nil {
			return nil, fmt.Errorf("edges[%d]: %w", i, err)
		}
		values, err := valueFlowsFrom(es)
		if err != nil {
			return nil, fmt.Errorf("edges[%d]: %w", i, err)
		}
		out = append(out, converge.DepEdge{From: from, To: to, Values: values})
	}
	return out, nil
}

// valueFlowsFrom reads the optional `values` field on an edge struct: a list of
// struct(dependent="/field", source="/field") declaring which spec fields of the
// dependent are filled from which status fields of the dependency when it goes Ready.
// Absent → nil (a plain ordering edge, no flow).
func valueFlowsFrom(es *starlarkstruct.Struct) ([]converge.ValueFlow, error) {
	v, err := es.Attr("values")
	if err != nil {
		return nil, nil // absent → no flows
	}
	iter, ok := v.(starlark.Iterable)
	if !ok {
		return nil, fmt.Errorf(".values must be a list, got %s", v.Type())
	}
	var out []converge.ValueFlow
	it := iter.Iterate()
	defer it.Done()
	var item starlark.Value
	for i := 0; it.Next(&item); i++ {
		vs, ok := item.(*starlarkstruct.Struct)
		if !ok {
			return nil, fmt.Errorf("values[%d] not a struct{dependent,source}", i)
		}
		dep, err := attrStr(vs, "dependent")
		if err != nil {
			return nil, fmt.Errorf("values[%d]: %w", i, err)
		}
		src, err := attrStr(vs, "source")
		if err != nil {
			return nil, fmt.Errorf("values[%d]: %w", i, err)
		}
		out = append(out, converge.ValueFlow{DependentField: dep, SourceField: src})
	}
	return out, nil
}

func refFrom(s *starlarkstruct.Struct, name string) (converge.ResourceRef, error) {
	v, err := s.Attr(name)
	if err != nil {
		return converge.ResourceRef{}, fmt.Errorf("missing .%s", name)
	}
	rs, ok := v.(*starlarkstruct.Struct)
	if !ok {
		return converge.ResourceRef{}, fmt.Errorf(".%s must be a struct{kind,name}", name)
	}
	kind, err := attrStr(rs, "kind")
	if err != nil {
		return converge.ResourceRef{}, err
	}
	rname, err := attrStr(rs, "name")
	if err != nil {
		return converge.ResourceRef{}, err
	}
	return converge.ResourceRef{Kind: converge.Kind(kind), Name: rname}, nil
}
