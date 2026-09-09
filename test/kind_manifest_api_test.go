package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/api"
	"github.com/salesforce/converge/internal/store"
)

// TestPutKindManifestFromFixtureOverHTTP applies every canonical CRD fixture
// (testfixtures/*.kind.json) through the REAL HTTP handler — exactly what the UI
// does when an operator clicks Apply CRD with a fixture loaded. It guards the
// request-body contract end to end: the fixture carries the FULL KindManifest
// (schemas + reactions + the policy knobs max_inflight/resync_*/orphan_grace),
// and the PUT body schema must accept all of them. A regression here (e.g. a
// policy field missing from the body, or huma not flattening the body struct)
// surfaces as a 422 "unexpected property" — the bug this test exists to catch.
func TestPutKindManifestFromFixtureOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)

	srv := api.NewServer(api.DepsFromPools(pool, nil))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	matches := kindFixtureFiles()
	require.NotEmpty(t, matches, "no *.kind.json fixtures found")

	// Several demos ship a fixture for the SAME kind (e.g. classic and typescript
	// both define `account`) with DIFFERENT schemas. This test proves the API accepts
	// a full CRD verbatim, one PUT per kind — a second PUT of the same kind with a
	// different schema is a legitimate 409 (breaking-schema guard), not a failure of
	// what we're checking. Apply each kind's FIRST fixture (deterministic: matches is
	// sorted) and skip repeats.
	seen := map[string]bool{}
	for _, f := range matches {
		raw, err := os.ReadFile(f)
		require.NoError(t, err)

		// Pull the kind out of the fixture for the path; the body is sent VERBATIM
		// (the whole CRD document, like the UI's load-from-file flow).
		var probe struct {
			Kind string `json:"kind"`
		}
		require.NoError(t, json.Unmarshal(raw, &probe))
		require.NotEmpty(t, probe.Kind, "%s has no kind", f)
		if seen[probe.Kind] {
			continue // another demo's fixture for a kind already applied
		}
		seen[probe.Kind] = true

		url := ts.URL + "/api/v1/kinds/" + probe.Kind + "/manifest"
		req, err := newPut(ctx, url, raw)
		require.NoError(t, err)
		resp, err := httpClient().Do(req)
		require.NoError(t, err)
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		require.Equalf(t, 200, resp.StatusCode,
			"PUT %s should accept the full CRD fixture verbatim; got %d:\n%s",
			filepath.Base(f), resp.StatusCode, string(body))

		// The response must echo a manifest_version (proves the apply landed).
		var out struct {
			ManifestVersion int64 `json:"manifest_version"`
		}
		require.NoError(t, json.Unmarshal(body, &out))
		require.NotZerof(t, out.ManifestVersion, "%s: expected a manifest_version", probe.Kind)
	}

	t.Logf("SUCCESS: all %d kind CRD fixtures apply through the HTTP handler verbatim", len(matches))
}

func newPut(ctx context.Context, url string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func httpClient() *http.Client { return &http.Client{Timeout: 15 * time.Second} }

// TestDeleteKindManifestOverHTTP guards the DELETE /api/kinds/{kind}/manifest
// status-code contract end to end: 200 on a clean delete (with a deleted_configs
// count), 409 when a resource still pins the version, 404 when the version is
// already gone (idempotent), and 422 when kind_version is missing/0. Applies the
// CRDs through the real handler so the validator-rebuild path runs too.
func TestDeleteKindManifestOverHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)

	srv := api.NewServer(api.DepsFromPools(pool, nil))
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	putManifest := func(kind string, kv int) {
		body, _ := json.Marshal(map[string]any{
			"kind": kind, "kind_version": kv, "spec_schema": json.RawMessage(`{"type":"object"}`),
			"reactions": []map[string]any{{"name": "work", "trigger": "specChange", "emits": []string{"status"}}},
		})
		req, err := newPut(ctx, ts.URL+"/api/v1/kinds/"+kind+"/manifest", body)
		require.NoError(t, err)
		resp, err := httpClient().Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, 200, resp.StatusCode, "PUT %s/v%d", kind, kv)
	}
	del := func(kind string, kv int) (int, []byte) {
		// kind_version is a PATH segment now (identity), not a query param.
		url := ts.URL + "/api/v1/kinds/" + kind + "/versions/" + itoaTest(kv) + "/manifest"
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		require.NoError(t, err)
		resp, err := httpClient().Do(req)
		require.NoError(t, err)
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, b
	}

	// Clean delete → 200 + deleted_configs echoed (0 here, no configs applied).
	putManifest("httpdel", 1)
	code, body := del("httpdel", 1)
	require.Equalf(t, 200, code, "clean delete should be 200; got %d: %s", code, body)
	var out struct {
		Kind           string `json:"kind"`
		KindVersion    int    `json:"kind_version"`
		DeletedConfigs int64  `json:"deleted_configs"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	require.Equal(t, "httpdel", out.Kind)
	require.Equal(t, 1, out.KindVersion)

	// Re-delete the now-gone version → 404 (idempotent).
	code, _ = del("httpdel", 1)
	require.Equal(t, 404, code, "re-deleting a gone version is 404")

	// Blocked by a resource → 409. Apply the referencing resource directly via the
	// store (the API's declared-schema apply gate is exercised elsewhere; here we
	// just need a row pinned to httpdelr/v1).
	putManifest("httpdelr", 1)
	_, err := store.New(pool).ApplySpecKindVersionWithConfig(ctx, "httpdelr", 1, "r1", json.RawMessage(`{}`), nil, "")
	require.NoError(t, err)
	code, body = del("httpdelr", 1)
	require.Equalf(t, 409, code, "a version with a resource should be 409; got %d: %s", code, body)

	// A version=0 in the path → 422 (minimum:"1" rejects it at the edge). This
	// replaces the old "missing ?kind_version → 422": the version is now a required
	// PATH segment, so omitting it entirely wouldn't match the route at all.
	code, _ = del("httpdelr", 0)
	require.Equal(t, 422, code, "DELETE at version 0 is 422 (minimum:1)")
}

func itoaTest(n int) string { return strconv.Itoa(n) }
