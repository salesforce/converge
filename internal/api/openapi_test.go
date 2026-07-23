package api

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/internal/host"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/sdk-go/converge"
)

var updateOpenAPI = flag.Bool("update-openapi", false, "overwrite openapi.golden.yaml with the current spec")

const openAPIGoldenFile = "openapi.golden.yaml"

// TestOpenAPISpec generates the OpenAPI 3.1 document by registering
// the API's routes against a throwaway server and compares the YAML
// to openapi.golden.yaml. Mirrors db/migrations_test.go's pattern.
//
// Run with -update-openapi to regenerate the golden file:
//
//	go test ./internal/api/ -run TestOpenAPISpec -update-openapi
func TestOpenAPISpec(t *testing.T) {
	// Pool isn't touched during route registration — Huma walks the
	// registered handlers' input/output types to build the spec,
	// never invoking them. We seed the Server's DECLARED schemas so
	// registerOpenAPIComponents has the per-kind Spec/Status types to
	// register as named schemas.
	// The DECLARED kind manifests are the operator-applied CRD fixtures
	// (*.kind.json) — the same []model.KindManifest production seeds into
	// kind_manifest. These reference providers live in the classic demo, so their
	// CRDs are under examples/demos/classic/testfixtures (test working dir is the
	// package dir → ../../examples/...). We seed the WORK kinds (classicbom,
	// account, networking) as declared resources and the statussink reactor's
	// CONFIG shape separately, mirroring the production wiring.
	all, err := host.LoadKindFixtures("../../examples/demos/classic/testfixtures")
	require.NoError(t, err)
	// Group by kind but keep EVERY kindVersion: vpc ships v1 (vpc.kind.json) and v2
	// (vpc-v2.kind.json), and /docs must document both (VpcSpec + VpcSpecV2, etc.).
	// Keying by kind alone would drop a kindVersion and silently shrink the spec.
	byKind := make(map[model.Kind][]model.KindManifest, len(all))
	for _, m := range all {
		byKind[m.Kind] = append(byKind[m.Kind], m)
	}
	pick := func(kinds ...model.Kind) []model.KindManifest {
		out := make([]model.KindManifest, 0, len(kinds))
		for _, k := range kinds {
			mm, ok := byKind[k]
			require.Truef(t, ok, "kind fixture %q not found in testfixtures", k)
			out = append(out, mm...) // all published kind versions of this kind
		}
		return out
	}

	// The work providers each advertise ONE (kind, version) pair; pick keys on kind NAME
	// (it pulls EVERY published version's manifest per kind), so collapse to distinct
	// names. networking serves several pairs, so New returns one provider per pair.
	net, err := networking.New()
	require.NoError(t, err)
	providers := append([]converge.Provider{classicbom.Provider{}, account.Provider{}}, net...)
	var workKinds []model.Kind
	seenKind := map[model.Kind]bool{}
	for _, p := range providers {
		{
			k := model.Kind(p.Kind().Kind)
			if !seenKind[k] {
				seenKind[k] = true
				workKinds = append(workKinds, k)
			}
		}
	}
	srv := &Server{}
	srv.SetDeclaredSchemas(pick(workKinds...))
	// Register the statussink reactor too, so the golden proves a reactor's CONFIG
	// shape (StatussinkConfig) lands in /docs components — the anchor the Kinds
	// page's "View in API docs" link targets (/docs#/schemas/StatussinkConfig).
	srv.SetReactorSchemas(pick(model.Kind(statussink.Kind)))

	yamlBytes, err := srv.OpenAPISpec().YAML()
	require.NoError(t, err)
	got := string(yamlBytes)

	if *updateOpenAPI {
		require.NoError(t, os.WriteFile(openAPIGoldenFile, []byte(got), 0o644))
		t.Logf("wrote %s", openAPIGoldenFile)
		return
	}

	raw, err := os.ReadFile(openAPIGoldenFile)
	if os.IsNotExist(err) {
		t.Fatalf("%s does not exist; run with -update-openapi to create it", openAPIGoldenFile)
	}
	require.NoError(t, err)

	want := string(raw)
	if got != want {
		wantLines := strings.Split(want, "\n")
		gotLines := strings.Split(got, "\n")
		var sb strings.Builder
		max := len(wantLines)
		if len(gotLines) > max {
			max = len(gotLines)
		}
		for i := 0; i < max; i++ {
			wl, gl := "", ""
			if i < len(wantLines) {
				wl = wantLines[i]
			}
			if i < len(gotLines) {
				gl = gotLines[i]
			}
			if wl != gl {
				fmt.Fprintf(&sb, "line %d:\n  want: %s\n  got:  %s\n", i+1, wl, gl)
			}
		}
		t.Fatalf("openapi spec drift detected (re-run with -update-openapi to accept):\n%s", sb.String())
	}
}
