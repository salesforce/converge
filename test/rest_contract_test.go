package test

// REST-contract tests for the endpoints whose METHOD / PATH / STATUS / arg
// discipline changed in the REST-correctness pass. Each hits the REAL huma
// router over httptest against a real Postgres, asserting the NEW shape:
//   - kind_version is a PATH segment on GET/DELETE manifest + PUT config
//     (/kinds/{kind}/versions/{v}/...), not a query param.
//   - config is PUT (idempotent full replace), not POST.
//   - the /page action segment is gone (GET /resources, /resources/roots).
//   - /clusterinfo → /cluster-members.
//   - verb invocation is POST /ops with the verb in the BODY (202), and the old
//     /ops/{verb} path no longer exists.
//   - apply returns 201 on create / 200 on update.
//   - delete-by-name 404s a missing name (reactor-bindings), matching providerconfigs.
// A missing/wrong route surfaces as 404/405 here — this is the guard that the
// registration + DTO tags actually took effect on the wire.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/api"
)

// restEnv stands up the API server over httptest against a fresh migrated
// Postgres, plus a small request helper. No workers — these assert the HTTP
// contract of the control API, not reconcile behaviour.
type restEnv struct {
	t   *testing.T
	ctx context.Context
	ts  *httptest.Server
}

func newRESTEnv(t *testing.T) *restEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	srv := api.NewServer(api.DepsFromPools(pool, nil))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &restEnv{t: t, ctx: ctx, ts: ts}
}

// do issues a request and returns (status, body). body may be nil.
func (e *restEnv) do(method, path string, body []byte) (int, []byte) {
	e.t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(e.ctx, method, e.ts.URL+path, r)
	require.NoError(e.t, err)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient().Do(req)
	require.NoError(e.t, err)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, b
}

// assertNotAPIRoute checks that a path is NOT a registered API route: an
// unmatched /api/... path falls through to the SPA catch-all, which serves
// index.html (text/html) — so the absence of a JSON API content-type is the
// signal the route is gone. (huma returns application/json for a real API
// handler; the SPA fallback returns text/html.) This is how we prove an OLD
// route was removed, since the SPA makes every unmatched path a 200.
func assertNotAPIRoute(e *restEnv, method, path string) {
	e.t.Helper()
	var r io.Reader
	req, err := http.NewRequestWithContext(e.ctx, method, e.ts.URL+path, r)
	require.NoError(e.t, err)
	resp, err := httpClient().Do(req)
	require.NoError(e.t, err)
	ct := resp.Header.Get("Content-Type")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.NotContainsf(e.t, ct, "application/json",
		"%s %s should NOT be a live API route (got content-type %q — a real handler would serve JSON)", method, path, ct)
}

// putManifest applies a minimal CRD for (kind, kv) via the real PUT handler.
func (e *restEnv) putManifest(kind string, kv int) {
	e.t.Helper()
	body, _ := json.Marshal(map[string]any{
		"kind": kind, "kind_version": kv, "spec_schema": json.RawMessage(`{"type":"object"}`),
		"reactions": []map[string]any{{"name": "work", "trigger": "specChange", "emits": []string{"status"}}},
	})
	code, b := e.do(http.MethodPut, "/api/v1/kinds/"+kind+"/manifest", body)
	require.Equalf(e.t, 200, code, "PUT manifest %s/v%d: %s", kind, kv, b)
}

// TestManifestVersionInPath: GET/DELETE manifest live at
// /kinds/{kind}/versions/{v}/manifest (identity in the path); the old
// ?kind_version= query route is gone (404), and a version 0 is 422.
func TestManifestVersionInPath(t *testing.T) {
	e := newRESTEnv(t)
	e.putManifest("mpath", 1)

	// New versioned GET path resolves the published manifest (200); an unpublished
	// version 404s with the version echoed — proving the {kind_version} path segment
	// binds (a mis-bound version would 404 v1 or 200 v2).
	code, body := e.do(http.MethodGet, "/api/v1/kinds/mpath/versions/1/manifest", nil)
	require.Equalf(t, 200, code, "GET versioned manifest v1 (published): %s", body)
	code, body = e.do(http.MethodGet, "/api/v1/kinds/mpath/versions/2/manifest", nil)
	require.Equalf(t, 404, code, "GET unpublished v2 → 404: %s", body)
	require.Contains(t, string(body), "v2", "the 404 echoes the requested version (proves path binding)")

	// The OLD query-param path is no longer an API route — it falls through to the
	// SPA catch-all (index.html, text/html), NOT the manifest JSON handler.
	assertNotAPIRoute(e, http.MethodGet, "/api/v1/kinds/mpath/manifest?kind_version=1")

	// version 0 in the path → 422 (minimum:1 on the path param).
	code, _ = e.do(http.MethodGet, "/api/v1/kinds/mpath/versions/0/manifest", nil)
	require.Equal(t, 422, code, "GET at version 0 is 422")

	// DELETE at the versioned path → 200 (clean delete).
	code, body = e.do(http.MethodDelete, "/api/v1/kinds/mpath/versions/1/manifest", nil)
	require.Equalf(t, 200, code, "DELETE versioned manifest: %s", body)
	// Idempotent re-delete → 404.
	code, _ = e.do(http.MethodDelete, "/api/v1/kinds/mpath/versions/1/manifest", nil)
	require.Equal(t, 404, code, "re-deleting a gone version is 404")
}

// TestConfigIsPutAtVersionedPath: the operational-config endpoint is PUT at
// /kinds/{kind}/versions/{v}/config (idempotent full replace); the old POST
// /kinds/{kind}/config?kind_version= route is gone.
func TestConfigIsPutAtVersionedPath(t *testing.T) {
	e := newRESTEnv(t)
	e.putManifest("cpath", 1)

	// The config is a full replace — all effective values are required (no omitempty).
	cfg, _ := json.Marshal(map[string]any{
		"max_inflight": 7, "task_deadline_seconds": 0, "resync_interval_seconds": 0,
		"orphan_grace_seconds": 0, "max_transient_attempts": 0,
	})
	// PUT the config twice — idempotent full-replace, same 200 + echoed version both
	// times (PUT semantics: re-sending is a no-op).
	for i := 0; i < 2; i++ {
		code, body := e.do(http.MethodPut, "/api/v1/kinds/cpath/versions/1/config", cfg)
		require.Equalf(t, 200, code, "PUT config (attempt %d): %s", i, body)
		var out struct {
			Kind        string `json:"kind"`
			KindVersion int    `json:"kind_version"`
		}
		require.NoError(t, json.Unmarshal(body, &out))
		require.Equal(t, "cpath", out.Kind)
		require.Equal(t, 1, out.KindVersion)
	}
	// The OLD POST /kinds/{kind}/config route is gone (falls through to the SPA).
	assertNotAPIRoute(e, http.MethodPost, "/api/v1/kinds/cpath/config?kind_version=1")
}

// TestPageSegmentGone: the list collections answer at their plain paths (no
// /page action segment), and the old /page paths 404.
func TestPageSegmentGone(t *testing.T) {
	e := newRESTEnv(t)

	code, _ := e.do(http.MethodGet, "/api/v1/resources", nil)
	require.Equal(t, 200, code, "GET /resources (cross-cluster list) is the collection default")
	code, _ = e.do(http.MethodGet, "/api/v1/resources/roots", nil)
	require.Equal(t, 200, code, "GET /resources/roots is the roots collection")

	// The old /page action paths are gone (fall through to the SPA).
	assertNotAPIRoute(e, http.MethodGet, "/api/v1/resources/page")
	assertNotAPIRoute(e, http.MethodGet, "/api/v1/resources/roots/page")
}

// TestClusterMembersRenamed: the fleet registry is at /cluster-members; the old
// /clusterinfo is gone.
func TestClusterMembersRenamed(t *testing.T) {
	e := newRESTEnv(t)
	code, body := e.do(http.MethodGet, "/api/v1/cluster-members", nil)
	require.Equalf(t, 200, code, "GET /cluster-members: %s", body)
	var out struct {
		Members []any `json:"members"`
	}
	require.NoError(t, json.Unmarshal(body, &out))

	// The old /clusterinfo is gone (falls through to the SPA).
	assertNotAPIRoute(e, http.MethodGet, "/api/v1/clusterinfo")
}

// TestReactorBindingDelete404: deleting a missing reactor-binding by name is a
// 404 (mirrors provider-config delete), not a silent 200.
func TestReactorBindingDelete404(t *testing.T) {
	e := newRESTEnv(t)
	code, _ := e.do(http.MethodDelete, "/api/v1/reactor-bindings/does-not-exist", nil)
	require.Equal(t, 404, code, "deleting a missing reactor binding is 404")
}

// TestApply201OnCreate200OnUpdate: POST /resources returns 201 Created on a new
// (kind,name) and 200 OK on a re-apply that changes it — the create-vs-update
// distinction the contract now advertises.
func TestApply201OnCreate200OnUpdate(t *testing.T) {
	e := newRESTEnv(t)
	// A kind whose spec_schema accepts anything, so the apply validates.
	body, _ := json.Marshal(map[string]any{
		"kind": "applyc", "kind_version": 1, "spec_schema": json.RawMessage(`{"type":"object"}`),
		"reactions": []map[string]any{{"name": "work", "trigger": "specChange", "emits": []string{"status"}}},
	})
	code, b := e.do(http.MethodPut, "/api/v1/kinds/applyc/manifest", body)
	require.Equalf(t, 200, code, "PUT applyc manifest: %s", b)

	res := func(spec string) []byte {
		bb, _ := json.Marshal(map[string]any{"kind": "applyc", "kind_version": 1, "name": "r1", "spec": json.RawMessage(spec)})
		return bb
	}
	// First apply → 201 Created.
	code, b = e.do(http.MethodPost, "/api/v1/resources", res(`{"a":1}`))
	require.Equalf(t, 201, code, "first apply should be 201 Created: %s", b)
	// Re-apply with a changed spec → 200 OK (updated, not created).
	code, b = e.do(http.MethodPost, "/api/v1/resources", res(`{"a":2}`))
	require.Equalf(t, 200, code, "re-apply (changed) should be 200 OK: %s", b)
}

// TestInvokeOpVerbInBody: verb invocation is POST /ops with the verb in the
// BODY; the old /ops/{verb} path is gone. (A verb the kind doesn't declare 404s
// from the handler, proving the body verb is read + validated.)
func TestInvokeOpVerbInBody(t *testing.T) {
	e := newRESTEnv(t)
	e.putManifest("opk", 1)
	// Apply a resource so the (kind,name) resolves.
	rb, _ := json.Marshal(map[string]any{"kind": "opk", "kind_version": 1, "name": "r1", "spec": json.RawMessage(`{}`)})
	code, b := e.do(http.MethodPost, "/api/v1/resources", rb)
	require.Truef(t, code == 200 || code == 201, "apply opk/r1: %d %s", code, b)

	// The old /ops/{verb} path is gone (falls through to the SPA).
	assertNotAPIRoute(e, http.MethodPost, "/api/v1/resources/opk/r1/ops/somverb")

	// POST /ops with the verb in the body: opk declares no operation reaction, so
	// the handler 404s the unknown verb — which proves the body verb was parsed +
	// validated (an empty/missing verb would 422 at the edge instead).
	code, b = e.do(http.MethodPost, "/api/v1/resources/opk/r1/ops", []byte(`{"verb":"nope","input":{}}`))
	require.Equalf(t, 404, code, "unknown verb via body → 404 from handler: %s", b)
	// An empty verb is rejected at the edge (minLength:1 → 422).
	code, _ = e.do(http.MethodPost, "/api/v1/resources/opk/r1/ops", []byte(`{"input":{}}`))
	require.Equal(t, 422, code, "missing verb in body → 422 (minLength:1)")
}
