package stdcel

import (
	"encoding/json"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// anyType is the reflect.Type CEL converts a result into — a plain Go value that
// marshals cleanly to JSON (map[string]any / []any / scalar).
var anyType = reflect.TypeFor[any]()

// decodeAny decodes a raw JSON template value into a plain any (map/slice/scalar) so
// resolveValue / walkStrings can walk it. nil/empty → nil.
func decodeAny(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	return v
}

// mustJSON marshals a resolved value to json.RawMessage for a ChildSpec. The value
// is always plain Go (map/slice/scalar), so marshal cannot fail; on the impossible
// error it yields an empty object rather than panicking a worker.
func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return b
}

// statusPlaceholder builds the compose-time value of a dependency's `status`: a map
// with each read field present as nil, so a `${dep.status.x}` expression compiles
// and evaluates to null (the real value flows in at runtime via the ValueFlow). A
// nested path ("endpoint.host") is expanded to nested nil maps so the read resolves.
func statusPlaceholder(paths []string) map[string]any {
	out := map[string]any{}
	for _, p := range paths {
		segs := strings.Split(p, ".")
		cur := out
		for i, s := range segs {
			if i == len(segs)-1 {
				if _, ok := cur[s]; !ok {
					cur[s] = nil
				}
				break
			}
			next, ok := cur[s].(map[string]any)
			if !ok {
				next = map[string]any{}
				cur[s] = next
			}
			cur = next
		}
	}
	return out
}

// walkStrings visits every string leaf of a decoded JSON value (recursing into maps
// and slices) and calls fn with it — used to collect ${...} references from a
// template's spec.
func walkStrings(v any, fn func(string)) error {
	switch t := v.(type) {
	case string:
		fn(t)
	case map[string]any:
		for _, sub := range t {
			if err := walkStrings(sub, fn); err != nil {
				return err
			}
		}
	case []any:
		for _, sub := range t {
			if err := walkStrings(sub, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// jsonPtr turns a dot path ("endpoint.host") into the RFC 6901 JSON Pointer a
// ValueFlow expects ("/endpoint/host"). A bare key ("vpc_id") becomes "/vpc_id".
// Each segment is escaped per RFC 6901 (~→~0, /→~1).
func jsonPtr(path string) string {
	if path == "" {
		return ""
	}
	segs := strings.Split(path, ".")
	for i, s := range segs {
		segs[i] = escapePtr(s)
	}
	return "/" + strings.Join(segs, "/")
}

// escapePtr escapes one JSON Pointer reference token per RFC 6901: '~' → "~0" and
// '/' → "~1" (order matters — escape '~' first).
func escapePtr(tok string) string {
	tok = strings.ReplaceAll(tok, "~", "~0")
	return strings.ReplaceAll(tok, "/", "~1")
}

// itoa is strconv.Itoa, kept local so the JSON-Pointer builder needs no extra import
// at each call site.
func itoa(i int) string { return strconv.Itoa(i) }

// appendUnique appends s to xs only if absent, keeping the value-flow field set
// de-duplicated (the same status field may be read by several expressions).
func appendUnique(xs []string, s string) []string {
	if slices.Contains(xs, s) {
		return xs
	}
	return append(xs, s)
}

// sortedKeys returns the map keys sorted, for deterministic edge order.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// insertSorted inserts s into the already-sorted slice xs, keeping it sorted — the
// ready-queue insert for the deterministic topological sort.
func insertSorted(xs []string, s string) []string {
	i := sort.SearchStrings(xs, s)
	xs = append(xs, "")
	copy(xs[i+1:], xs[i:])
	xs[i] = s
	return xs
}
