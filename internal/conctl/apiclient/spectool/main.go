// Command spectool down-converts the converge OpenAPI 3.1 golden spec to an
// equivalent 3.0.3 document that oapi-codegen v2 can consume.
//
// WHY THIS EXISTS: the API server (Huma) emits OpenAPI 3.1, whose type-union
// syntax `type: [array, "null"]` (used for the repeatable optional list filters
// like owner_id[]/kind[]/phase[]) is not yet handled by oapi-codegen v2
// (oapi-codegen/oapi-codegen#373). This tool reads the untouched golden spec,
// rewrites each `[<t>, "null"]` union into the 3.0 `type: <t>` + `nullable: true`
// form, drops `openapi: 3.1.0` to `3.0.3`, and writes the normalized copy for
// codegen. The golden file itself is never modified — this is a build-time
// shim, run by `just codegen-cli` before oapi-codegen.
//
// Usage: spectool <in-3.1.yaml> <out-3.0.yaml>
package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: spectool <in.yaml> <out.yaml>")
		os.Exit(2)
	}
	in, err := os.ReadFile(os.Args[1])
	if err != nil {
		fatal("read %s: %v", os.Args[1], err)
	}
	var root yaml.Node
	if err := yaml.Unmarshal(in, &root); err != nil {
		fatal("parse %s: %v", os.Args[1], err)
	}
	normalize(&root)
	setOpenAPIVersion(&root)

	out, err := yaml.Marshal(&root)
	if err != nil {
		fatal("marshal: %v", err)
	}
	if err := os.WriteFile(os.Args[2], out, 0o644); err != nil {
		fatal("write %s: %v", os.Args[2], err)
	}
}

// normalize walks the tree and rewrites every mapping node that has a
// `type:` whose value is a sequence containing "null" (e.g. [array, "null"])
// into a scalar `type: <non-null>` plus `nullable: true` — the 3.0 spelling.
func normalize(n *yaml.Node) {
	switch n.Kind {
	case yaml.DocumentNode:
		for _, c := range n.Content {
			normalize(c)
		}
	case yaml.SequenceNode:
		for _, c := range n.Content {
			normalize(c)
		}
	case yaml.MappingNode:
		// Mapping content is [key0, val0, key1, val1, ...].
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			if key.Value == "type" && val.Kind == yaml.SequenceNode {
				if base, hasNull := splitNullUnion(val); hasNull {
					// Replace the sequence value with the base scalar type.
					n.Content[i+1] = scalar(base)
					ensureNullable(n)
				}
			}
			normalize(val)
		}
	case yaml.ScalarNode, yaml.AliasNode:
		// Leaf nodes: nothing to recurse into.
	}
}

// splitNullUnion returns the single non-"null" member of a type sequence and
// whether "null" was present. A union with more than one non-null member is
// left untouched (returns hasNull=false) — the golden spec only ever pairs one
// concrete type with "null".
func splitNullUnion(seq *yaml.Node) (base string, hasNull bool) {
	var bases []string
	for _, m := range seq.Content {
		if m.Value == "null" {
			hasNull = true
			continue
		}
		bases = append(bases, m.Value)
	}
	if !hasNull || len(bases) != 1 {
		return "", false
	}
	return bases[0], true
}

// ensureNullable adds `nullable: true` to a mapping if not already present.
func ensureNullable(m *yaml.Node) {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == "nullable" {
			return
		}
	}
	m.Content = append(m.Content,
		scalar("nullable"),
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"},
	)
}

// setOpenAPIVersion rewrites the top-level `openapi: 3.1.0` to `3.0.3`.
func setOpenAPIVersion(root *yaml.Node) {
	if len(root.Content) == 0 {
		return
	}
	doc := root.Content[0]
	if doc.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(doc.Content); i += 2 {
		if doc.Content[i].Value == "openapi" {
			doc.Content[i+1] = scalar("3.0.3")
			return
		}
	}
}

func scalar(v string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v}
}

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "spectool: "+format+"\n", a...)
	os.Exit(1)
}
