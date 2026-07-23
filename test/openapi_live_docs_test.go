package test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/api"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// TestOpenAPIDocsReflectRuntimeCRD proves that a CRD applied at RUNTIME (PUT
// /api/kinds/{kind}/manifest) becomes visible in the OpenAPI document — the
// /docs surface — WITHOUT a server restart.
//
// This is the regression this feature exists to prevent: huma serializes the
// OpenAPI spec lazily and then CACHES the bytes for an API instance's life, so
// merely mutating the components map post-boot is masked by that cache. The fix
// is Server.RebuildHandler — it swaps in a fresh huma instance (fresh spec
// cache) built from the current schema surface. The test wires the exact
// SetDeclaredSchemas → RebuildHandler callback cmd/converge registers on apply.
//
// Sequence (the order that masks the cache, so it's the order we must test):
//  1. boot with NO kinds; GET /openapi.json (this PRIMES huma's spec cache),
//  2. assert the to-be-applied kind's component is ABSENT,
//  3. PUT the CRD at runtime → onManifestApply fires → RebuildHandler swaps,
//  4. GET /openapi.json again; assert the kind's *Spec component is now PRESENT.
func TestOpenAPIDocsReflectRuntimeCRD(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	srv := api.NewServer(api.DepsFromPools(pool, nil))

	// Wire the schema surface from the DB exactly as cmd/converge does: on every
	// manifest apply, re-read the full set, re-declare it, and rebuild the handler
	// so /docs re-serializes. Boot with whatever is in the DB (nothing yet).
	rebuild := func() {
		all, err := st.ListKindManifests(ctx)
		require.NoError(t, err)
		var work, reactors []model.KindManifest
		for _, m := range all {
			if m.IsReactor() {
				reactors = append(reactors, m)
			} else {
				work = append(work, m)
			}
		}
		srv.SetDeclaredSchemas(work)
		srv.SetReactorSchemas(reactors)
		srv.RebuildHandler()
	}
	rebuild()
	srv.SetOnManifestApply(rebuild)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Pick a real fixture to apply at runtime; "account" is the simplest CRD.
	const kind = "account"
	component := strings.ToUpper(kind[:1]) + kind[1:] + "Spec" // componentName(kind,"Spec")

	// (1)+(2): prime huma's spec cache with a GET, and confirm the kind is absent.
	require.NotContains(t, fetchOpenAPI(t, ctx, ts.URL), `"`+component+`"`,
		"before applying the CRD, %s must NOT be in the OpenAPI components", component)

	// (3): apply the CRD at runtime, through the real HTTP handler.
	raw := readKindFixture(t, kind)
	req, err := newPut(ctx, ts.URL+"/api/v1/kinds/"+kind+"/manifest", raw)
	require.NoError(t, err)
	resp, err := httpClient().Do(req)
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equalf(t, 200, resp.StatusCode, "PUT CRD failed: %d\n%s", resp.StatusCode, string(body))

	// (4): the SAME server, no restart — the OpenAPI document now includes the kind.
	require.Contains(t, fetchOpenAPI(t, ctx, ts.URL), `"`+component+`"`,
		"after applying the CRD at runtime, %s MUST appear in the OpenAPI components without a restart", component)

	t.Logf("SUCCESS: %s appeared in /openapi.json live after a runtime CRD apply", component)
}

// fetchOpenAPI GETs the served OpenAPI JSON and returns it as a string. Each
// call goes through the live (possibly just-rebuilt) handler.
func fetchOpenAPI(t *testing.T, ctx context.Context, base string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/openapi.json", nil)
	require.NoError(t, err)
	resp, err := httpClient().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, "GET /openapi.json")
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	// Sanity: it's a real OpenAPI doc.
	var probe map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &probe))
	require.Contains(t, probe, "openapi")
	return string(b)
}
