package main

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestNormalizeNullUnion: the core down-convert — a 3.1 `type: [array, "null"]`
// union (Huma's spelling for optional repeatable filters) becomes the 3.0
// `type: array` + `nullable: true` that oapi-codegen v2 can parse.
func TestNormalizeNullUnion(t *testing.T) {
	src := `
openapi: 3.1.0
components:
  schemas:
    Foo:
      properties:
        items:
          type:
            - array
            - "null"
`
	out := run(t, src)
	if strings.Contains(out, `"null"`) {
		t.Errorf("null union survived normalization:\n%s", out)
	}
	if !strings.Contains(out, "nullable: true") {
		t.Errorf("expected nullable: true after normalization:\n%s", out)
	}
	if !strings.Contains(out, "openapi: 3.0.3") {
		t.Errorf("expected openapi downgraded to 3.0.3:\n%s", out)
	}
}

// TestNormalizeStringUnion: the same transform for a scalar type union
// (`type: [string, "null"]`), which the nullable-timestamp fields emit.
func TestNormalizeStringUnion(t *testing.T) {
	src := `
openapi: 3.1.0
components:
  schemas:
    Bar:
      properties:
        ts:
          format: date-time
          type:
            - string
            - "null"
`
	out := run(t, src)
	if strings.Contains(out, `"null"`) {
		t.Errorf("null union survived:\n%s", out)
	}
	// The base type must be preserved as a scalar.
	if !strings.Contains(out, "type: string") {
		t.Errorf("base scalar type lost:\n%s", out)
	}
	if !strings.Contains(out, "format: date-time") {
		t.Errorf("sibling keys (format) must be preserved:\n%s", out)
	}
}

// TestNormalizePlainTypeUntouched: a non-union scalar `type: string` is left
// exactly as-is (no spurious nullable added).
func TestNormalizePlainTypeUntouched(t *testing.T) {
	src := `
openapi: 3.1.0
components:
  schemas:
    Baz:
      properties:
        name:
          type: string
`
	out := run(t, src)
	if strings.Contains(out, "nullable") {
		t.Errorf("plain scalar type should not gain nullable:\n%s", out)
	}
}

// run parses src, applies the normalization pass + version downgrade, and
// re-marshals — the same transform pipeline main() runs.
func run(t *testing.T, src string) string {
	t.Helper()
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(src), &root); err != nil {
		t.Fatalf("parse: %v", err)
	}
	normalize(&root)
	setOpenAPIVersion(&root)
	out, err := yaml.Marshal(&root)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(out)
}
