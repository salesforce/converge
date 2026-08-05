// Command gen-types generates provider-side TYPES from a kind's CRD manifest — the
// schema-first source of truth. A kind's spec/status/config shapes are declared ONCE as
// JSON Schema in its <kind>.kind.json manifest (the language-neutral contract the core
// validates against and the wire ferries); this tool derives the Go structs AND TypeScript
// interfaces FROM that schema so a provider author never hand-maintains a type in parallel
// with the schema, in either language. One schema, N languages, no drift.
//
// It reads every *.kind.json under the given manifest globs, and for each of the three
// schema blocks present (spec_schema → <Kind>Spec, status_schema → <Kind>Status,
// config_schema → <Kind>Config) it synthesizes a standalone JSON Schema document — the
// manifest embeds only the bare schema, so this injects the draft-2020-12 $schema keyword
// and a title (which the generators use as the type name) — then shells out to the two
// PINNED generators (go-jsonschema for Go, json-schema-to-typescript for TS), exactly as
// the sqlc/proto recipes shell out to their pinned tools. It is a build-time dev tool
// invoked via `just gen-types`; it is NOT part of any shipped binary and imports no
// internal/* core code.
//
// Usage (flags):
//
//	-manifests   glob(s) of *.kind.json, comma-separated (required)
//	-go-out      dir to write Go structs into (one <base>.go per manifest); "" = skip Go
//	-go-package  package name for the generated Go file (default "gen")
//	-ts-out      dir to write TS interfaces into (one <base>.ts per manifest); "" = skip TS
//	-gojsonschema-version / -ts-generator-version  pinned tool versions (passed by the recipe)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// schemaField pairs a manifest key with the type-name suffix its generated type carries.
// The three axes a kind declares: the resource spec, the status a handler writes, and the
// provider config (the "provider spec").
type schemaField struct {
	manifestKey string // e.g. "spec_schema"
	suffix      string // e.g. "Spec" → <Kind>Spec
}

var schemaFields = []schemaField{
	{"spec_schema", "Spec"},
	{"status_schema", "Status"},
	{"config_schema", "Config"},
}

// manifest is the sliver of a *.kind.json this tool reads: the kind name plus the three
// optional schema blocks (each a raw JSON Schema object, or absent).
type manifest struct {
	Kind         string          `json:"kind"`
	SpecSchema   json.RawMessage `json:"spec_schema"`
	StatusSchema json.RawMessage `json:"status_schema"`
	ConfigSchema json.RawMessage `json:"config_schema"`
}

func (m manifest) schema(key string) json.RawMessage {
	switch key {
	case "spec_schema":
		return m.SpecSchema
	case "status_schema":
		return m.StatusSchema
	case "config_schema":
		return m.ConfigSchema
	}
	return nil
}

func main() {
	var (
		manifests = flag.String("manifests", "", "comma-separated glob(s) of *.kind.json manifests (required)")
		goOut     = flag.String("go-out", "", "dir for generated Go structs (empty = skip Go)")
		goPackage = flag.String("go-package", "gen", "package name for generated Go files")
		tsOut     = flag.String("ts-out", "", "dir for generated TS interfaces (empty = skip TS)")
		// The default MUST match justfile's gojsonschema_version (the `just gen-types` recipe
		// passes it explicitly; a direct `go run` caller relies on this default).
		goVersion     = flag.String("gojsonschema-version", "v0.19.0", "pinned github.com/atombender/go-jsonschema version (keep in sync with justfile gojsonschema_version)")
		tsBin         = flag.String("ts-generator-bin", "json2ts", "json-schema-to-typescript binary (on PATH via node_modules/.bin)")
		draftSchemaID = "https://json-schema.org/draft/2020-12/schema"
	)
	flag.Parse()
	ctx := context.Background()

	if *manifests == "" {
		fail("required: -manifests <glob[,glob...]>")
	}
	if *goOut == "" && *tsOut == "" {
		fail("nothing to do: set -go-out and/or -ts-out")
	}

	files := expandGlobs(*manifests)
	if len(files) == 0 {
		fail(fmt.Sprintf("no manifests matched %q", *manifests))
	}

	// A scratch dir for the per-schema standalone documents fed to the generators. Removed
	// on exit — the generators read files, not stdin, so we materialize each schema once.
	tmp, err := os.MkdirTemp("", "gen-types-*")
	if err != nil {
		fail(err.Error())
	}
	defer func() { _ = os.RemoveAll(tmp) }()

	if *goOut != "" {
		mustMkdir(*goOut)
	}
	if *tsOut != "" {
		mustMkdir(*tsOut)
	}

	for _, f := range files {
		var m manifest
		raw, err := os.ReadFile(f)
		if err != nil {
			fail(fmt.Sprintf("read %s: %v", f, err))
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			fail(fmt.Sprintf("decode %s: %v", f, err))
		}
		if m.Kind == "" {
			fail(fmt.Sprintf("%s: empty kind", f))
		}
		base := strings.TrimSuffix(filepath.Base(f), ".kind.json") // e.g. account.kind.json → account

		// Materialize each PRESENT schema as a standalone draft-2020-12 document with a title
		// (the generators name the type from the title), in a stable order.
		var schemaFiles []string // per-schema temp .json paths for this manifest
		for _, sf := range schemaFields {
			s := m.schema(sf.manifestKey)
			if len(s) == 0 {
				continue // axis not typed — skip
			}
			typeName := pascal(m.Kind) + sf.suffix
			doc, err := standalone(s, draftSchemaID, typeName)
			if err != nil {
				fail(fmt.Sprintf("%s %s: %v", f, sf.manifestKey, err))
			}
			p := filepath.Join(tmp, base+"."+strings.ToLower(sf.suffix)+".schema.json")
			if err := os.WriteFile(p, doc, 0o644); err != nil {
				fail(err.Error())
			}
			schemaFiles = append(schemaFiles, p)
		}
		if len(schemaFiles) == 0 {
			fmt.Fprintf(os.Stderr, "gen-types: %s declares no schemas — nothing generated\n", f)
			continue
		}

		if *goOut != "" {
			out := filepath.Join(*goOut, base+".go")
			genGo(ctx, *goVersion, *goPackage, schemaFiles, out)
			fmt.Fprintf(os.Stderr, "gen-types: %s → %s\n", f, out)
		}
		if *tsOut != "" {
			out := filepath.Join(*tsOut, base+".ts")
			genTS(ctx, *tsBin, schemaFiles, out)
			fmt.Fprintf(os.Stderr, "gen-types: %s → %s\n", f, out)
		}
	}
}

// standalone wraps a bare manifest schema into a self-contained draft-2020-12 document with
// a title (the type name). The manifest embeds only the bare schema (no $schema, no title),
// so both are injected here; an existing title/$schema in the schema is overwritten so the
// type name is deterministic from (kind, axis).
func standalone(bare json.RawMessage, schemaID, title string) (json.RawMessage, error) {
	var obj map[string]any
	if err := json.Unmarshal(bare, &obj); err != nil {
		return nil, fmt.Errorf("schema is not a JSON object: %w", err)
	}
	obj["$schema"] = schemaID
	obj["title"] = title
	return json.MarshalIndent(obj, "", "  ")
}

// genGo runs the pinned go-jsonschema over the manifest's schema files into one Go file.
// -t names each struct from its title; --only-models drops unmarshal/validation boilerplate
// (the core validates against the schema); --tags json keeps only json struct tags.
func genGo(ctx context.Context, version, pkg string, schemaFiles []string, out string) {
	args := make([]string, 0, 10+len(schemaFiles))
	args = append(args, "run", "github.com/atombender/go-jsonschema@"+version,
		"-t", "--only-models", "--tags", "json", "--package", pkg, "-o", out)
	args = append(args, schemaFiles...)
	run(ctx, "go", args...)
}

// genTS runs json-schema-to-typescript over the manifest's schema files, concatenating the
// emitted interfaces into one .ts file (the CLI takes one input; we emit per-schema and
// join). The binary is resolved from PATH (the recipe prepends node_modules/.bin).
func genTS(ctx context.Context, bin string, schemaFiles []string, out string) {
	var b strings.Builder
	b.WriteString("// Code generated by json-schema-to-typescript from the kind manifest. DO NOT EDIT.\n\n")
	for _, sf := range schemaFiles {
		// --bannerComment "" suppresses the per-file banner (we write one banner above).
		outBytes := output(ctx, bin, sf, "--bannerComment", "")
		b.Write(outBytes)
		if !strings.HasSuffix(string(outBytes), "\n") {
			b.WriteByte('\n')
		}
	}
	if err := os.WriteFile(out, []byte(b.String()), 0o644); err != nil {
		fail(err.Error())
	}
}

// expandGlobs resolves comma-separated glob patterns into a sorted, de-duplicated file list.
func expandGlobs(spec string) []string {
	seen := map[string]bool{}
	var out []string
	for g := range strings.SplitSeq(spec, ",") {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		matches, err := filepath.Glob(g)
		if err != nil {
			fail(fmt.Sprintf("bad glob %q: %v", g, err))
		}
		for _, m := range matches {
			if !seen[m] {
				seen[m] = true
				out = append(out, m)
			}
		}
	}
	sort.Strings(out)
	return out
}

// pascal upper-cases a kind name into a Go/TS type prefix: "tsproject" → "Tsproject",
// "stdio-composer" → "StdioComposer". Splits on non-alphanumerics.
func pascal(s string) string {
	var b strings.Builder
	upNext := true
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			if upNext {
				b.WriteRune(r - 32)
			} else {
				b.WriteRune(r)
			}
			upNext = false
		case (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			upNext = false
		default: // separator (-, _, space, .)
			upNext = true
		}
	}
	return b.String()
}

// run executes a command, streaming stderr, and fails the tool on a non-zero exit.
func run(ctx context.Context, name string, args ...string) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fail(fmt.Sprintf("%s %s: %v", name, strings.Join(args, " "), err))
	}
}

// output executes a command and returns its stdout, failing the tool on a non-zero exit.
func output(ctx context.Context, name string, args ...string) []byte {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		fail(fmt.Sprintf("%s %s: %v", name, strings.Join(args, " "), err))
	}
	return out
}

func mustMkdir(dir string) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail(err.Error())
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "gen-types: "+msg)
	os.Exit(1)
}
