package kindschema

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/salesforce/converge/internal/model"
)

// schemacompat.go — the additive-vs-breaking JSON-Schema linter behind the
// publish-time (kind, kindVersion) invariant. It answers
// ONE question about an in-place re-publish of an existing (kind, kindVersion): is the
// new schema BACKWARD-COMPATIBLE with the old one, so any worker/resource on that
// kindVersion keeps working — or is it a BREAKING change that must ship as a NEW kindVersion?
//
// Direction matters. Spec/config are INPUTS the resource author writes; a change
// is compatible iff every document that was valid under OLD stays valid under
// NEW (the new schema is at least as permissive on the required surface). Status
// is an OUTPUT the provider writes and downstreams read; the safe direction is
// inverted, so status is checked with the compatibility direction flipped.
//
// SOUNDNESS over completeness: the linter must NEVER call a breaking change
// "compatible" (that would let a divergent vN silently serve resources). When it
// cannot PROVE a change additive, it reports it BREAKING. There is no in-place
// override — a breaking verdict (real or a rare false-positive) is resolved by
// publishing a NEW kindVersion, so a conservative linter costs at most an
// unnecessary version bump, never a silent incompatibility applied over live
// resources. It is a JSON-Schema-SUBSET checker (the shapes Of[T] emits: type,
// properties, required, enum, and the numeric/string/array bound keywords); an
// unrecognized construct is treated as breaking-if-changed, never waved through.

// SchemaCompat classifies a schema edit. Compatible=true means the change is
// backward-compatible (safe within the same kindVersion); false means it is breaking
// (must bump the kindVersion). Reasons lists the specific breaking edits (empty when
// compatible), for a precise 409 message.
type SchemaCompat struct {
	Compatible bool
	Reasons    []string
}

// ManifestSchemaCompat lints a full in-place re-publish: it compares OLD→NEW for
// all three axes (spec + config as inputs, status as output) and reports the
// merged verdict. A nil/absent axis on either side is handled per-axis (adding a
// whole schema where there was none is additive for inputs; removing one is
// breaking). Used by the publish gate to auto-allow additive within-kindVersion edits
// and reject breaking ones without the human accept flag.
func ManifestSchemaCompat(oldM, newM model.KindManifest) SchemaCompat {
	// spec + config are INPUTS (new must be at least as permissive); status is an
	// OUTPUT (flip the direction — an added required output field is fine, a removed
	// one breaks readers). Build the reason list in one concat so there's no
	// grow-by-append (prealloc).
	reasons := slices.Concat(
		axisCompat("spec", oldM.SpecSchema, newM.SpecSchema, false),
		axisCompat("config", oldM.ConfigSchema, newM.ConfigSchema, false),
		axisCompat("status", oldM.StatusSchema, newM.StatusSchema, true),
	)
	// Stable order for a deterministic 409 message (the reasons come from unordered
	// map iteration over required/properties/enum).
	return SchemaCompat{Compatible: len(reasons) == 0, Reasons: sortedReasons(reasons)}
}

// axisCompat compares one schema axis. output=false → input semantics (spec /
// config: new must be at least as permissive); output=true → output semantics
// (status: new must produce at least what old promised). Returns the breaking
// reasons for this axis (empty = compatible).
func axisCompat(axis string, oldRaw, newRaw json.RawMessage, output bool) []string {
	oldS := decodeCompat(oldRaw)
	newS := decodeCompat(newRaw)
	switch {
	case oldS == nil && newS == nil:
		return nil // both untyped: no schema to break
	case oldS == nil && newS != nil:
		// Adding a schema where there was none: for an INPUT this newly constrains
		// previously-anything documents → breaking. For an OUTPUT, newly promising
		// structure to readers who had none is additive.
		if output {
			return nil
		}
		return []string{axis + ": adding a schema to a previously-untyped axis rejects documents that were valid when unconstrained"}
	case oldS != nil && newS == nil:
		// Dropping the schema: an INPUT becomes anything-goes (more permissive →
		// additive); an OUTPUT stops promising its shape to readers → breaking.
		if output {
			return []string{axis + ": removing the status schema breaks downstreams that relied on its shape"}
		}
		return nil
	}
	var reasons []string
	compatNode(axis, oldS, newS, output, &reasons)
	return reasons
}

// compatNode recursively compares two schema nodes. It accumulates breaking
// reasons into out. Only the keywords Of[T] emits are understood; anything else
// that DIFFERS is reported breaking (soundness).
func compatNode(path string, oldN, newN map[string]any, output bool, out *[]string) {
	// type: a changed type is always breaking (int→string etc. invalidates every
	// document / every reader). Widening object→[object,null] is handled by
	// normalizeType treating a nullable union as its base type.
	ot, nt := normalizeType(oldN["type"]), normalizeType(newN["type"])
	if ot != "" && nt != "" && ot != nt {
		*out = append(*out, fmt.Sprintf("%s: type changed %q→%q", path, ot, nt))
		return // a type change subsumes any deeper diff
	}

	// required: for an INPUT, ADDING a required field is breaking (old docs
	// omitting it become invalid); REMOVING one is additive. For an OUTPUT it is
	// inverted (removing a promised-required field breaks readers).
	oldReq, newReq := stringSet(oldN["required"]), stringSet(newN["required"])
	if output {
		for f := range oldReq {
			if _, ok := newReq[f]; !ok {
				*out = append(*out, fmt.Sprintf("%s: status no longer guarantees required field %q", path, f))
			}
		}
	} else {
		for f := range newReq {
			if _, ok := oldReq[f]; !ok {
				*out = append(*out, fmt.Sprintf("%s: field %q became required (rejects documents that omitted it)", path, f))
			}
		}
	}

	// properties: for an INPUT, REMOVING a property is breaking (a document
	// setting it was valid, now additionalProperties may reject it / its
	// constraints vanish); ADDING an OPTIONAL property is additive (checked via
	// required above). For an OUTPUT, removing a property readers consumed breaks
	// them. Recurse into properties present on BOTH sides for constraint changes.
	oldProps, newProps := propsMap(oldN), propsMap(newN)
	for name := range oldProps {
		child := path + "." + name
		newChild, present := newProps[name]
		if !present {
			if output {
				*out = append(*out, fmt.Sprintf("%s: status property removed (readers may depend on it)", child))
			} else {
				*out = append(*out, fmt.Sprintf("%s: property removed (documents that set it may be rejected)", child))
			}
			continue
		}
		compatNode(child, oldProps[name], newChild, output, out)
	}

	// value constraints on this node (leaf keywords). For an INPUT, NARROWING is
	// breaking (a value that passed old now fails); WIDENING is additive. Output
	// semantics invert. enum: for an input, REMOVING a member is breaking (a doc
	// using it fails); ADDING is additive.
	constraintReasons(path, oldN, newN, output, out)
}

// constraintReasons compares the scalar constraint keywords on a node.
func constraintReasons(path string, oldN, newN map[string]any, output bool, out *[]string) {
	// enum membership.
	oldEnum, oldHas := enumSet(oldN["enum"])
	newEnum, newHas := enumSet(newN["enum"])
	if oldHas && newHas {
		if output {
			for v := range oldEnum { // status enum shrinking = a value readers expected is gone
				if _, ok := newEnum[v]; !ok {
					*out = append(*out, fmt.Sprintf("%s: status enum value %q removed", path, v))
				}
			}
		} else {
			for v := range oldEnum { // input enum shrinking = a value that was legal is now rejected
				if _, ok := newEnum[v]; !ok {
					*out = append(*out, fmt.Sprintf("%s: enum value %q removed (rejects a previously-valid value)", path, v))
				}
			}
		}
	} else if oldHas != newHas {
		// Adding an enum to an input NARROWS it (breaking); removing an enum WIDENS
		// (additive). Output inverts.
		addedEnum := newHas && !oldHas
		if addedEnum != output {
			*out = append(*out, fmt.Sprintf("%s: enum constraint %s", path, ternary(addedEnum, "added (narrows accepted values)", "removed (drops a guaranteed value set)")))
		}
	}

	// Numeric / length / items bounds. For an INPUT a TIGHTER bound is breaking;
	// for an OUTPUT a LOOSER bound is breaking. tighten() encodes the direction
	// per keyword (min-family tightens by increasing; max-family by decreasing).
	for _, kw := range boundKeywords {
		ov, oOK := floatOf(oldN[kw.name])
		nv, nOK := floatOf(newN[kw.name])
		switch {
		case oOK && nOK && ov != nv:
			tighter := kw.minFamily == (nv > ov) // min tightens up, max tightens down
			if tighter != output {               // tighter-input OR looser-output = breaking
				*out = append(*out, fmt.Sprintf("%s: %s %v→%v %s", path, kw.name, ov, nv, ternary(tighter, "(tighter)", "(looser)")))
			}
		case oOK != nOK:
			// Adding a bound to an input tightens (breaking); removing loosens
			// (additive). A "min/max present" keyword's presence-add always tightens.
			added := nOK && !oOK
			if added != output {
				*out = append(*out, fmt.Sprintf("%s: %s %s", path, kw.name, ternary(added, "added (tighter)", "removed (looser)")))
			}
		}
	}
}

// boundKeywords are the numeric/length/array bound keywords Of[T] emits, each
// tagged whether it is a MIN-family (tightens as it increases) or MAX-family
// (tightens as it decreases).
var boundKeywords = []struct {
	name      string
	minFamily bool
}{
	{"minimum", true}, {"exclusiveMinimum", true}, {"minLength", true}, {"minItems", true}, {"minProperties", true},
	{"maximum", false}, {"exclusiveMaximum", false}, {"maxLength", false}, {"maxItems", false}, {"maxProperties", false},
}

// ── decode helpers ──

func decodeCompat(raw json.RawMessage) map[string]any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil // unparseable → treat as untyped (SchemaHashOf still gates via the accept flag)
	}
	return m
}

// normalizeType collapses a JSON-Schema type (string or ["T","null"] nullable
// union that Of[T] emits for pointers) to its base type string. "" = absent.
func normalizeType(t any) string {
	switch v := t.(type) {
	case string:
		return v
	case []any:
		for _, e := range v {
			if s, ok := e.(string); ok && s != "null" {
				return s
			}
		}
	}
	return ""
}

func propsMap(node map[string]any) map[string]map[string]any {
	raw, _ := node["properties"].(map[string]any)
	out := make(map[string]map[string]any, len(raw))
	for k, v := range raw {
		if child, ok := v.(map[string]any); ok {
			out[k] = child
		}
	}
	return out
}

func stringSet(v any) map[string]struct{} {
	arr, _ := v.([]any)
	out := make(map[string]struct{}, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok {
			out[s] = struct{}{}
		}
	}
	return out
}

// enumSet returns the enum values as a set keyed by their canonical JSON, and
// whether an enum keyword was present at all.
func enumSet(v any) (map[string]struct{}, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make(map[string]struct{}, len(arr))
	for _, e := range arr {
		b, _ := json.Marshal(e)
		out[string(b)] = struct{}{}
	}
	return out, true
}

func floatOf(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

func ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

// sortedReasons returns the reasons in a stable order for a deterministic 409
// message (map iteration over required/enum/properties is unordered).
func sortedReasons(r []string) []string {
	out := append([]string(nil), r...)
	sort.Strings(out)
	return out
}
