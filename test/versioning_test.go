package test

// Integration tests for WEB-API-STYLE KIND VERSION VERSIONING (vpc/v1, vpc/v2 — no
// minor/micro), exercised end to end against a real Postgres (and the real HTTP
// handler for the publish gate + apply/flip paths). Each subtest pins one
// scenario of the versioning design:
//
//   Publish       — per-(kind, kindVersion) manifests + kind_config coexist; a new
//                   kindVersion touches no existing-kindVersion row.
//   PublishGate   — the additive-vs-breaking linter: identical=no-op,
//                   additive=auto-allowed, breaking=409 ALWAYS (a breaking edit must
//                   ship as a new kindVersion, with no override).
//   PinOnCreate   — an apply at vN stamps resources.kindVersion=N + vN's manifest_version.
//   Flip          — the uuid-stable flip: apply/flip an existing resource to a
//                   new kindVersion rewrites spec + bumps kindVersion/generation IN PLACE (same
//                   uuid; deps survive), with the reject-on-schema-fail gate.
//   PerKindVersionCap   — v1 and v2 carry independent inflight budgets (kind_inflight
//                   keyed (kind, kindVersion)).
//   PerKindVersionSchema— a v2-shaped spec is validated against v2's schema, not v1's.
//   ClaimRouting  — work_queue.kindVersion rides the row; a claim for kindVersion M takes only
//                   kindVersion-M rows (strict, no wildcard).

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/api"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/specschema"
	"github.com/salesforce/converge/internal/store"
)

// vManifest builds a KindManifest at a given kindVersion with a spec schema — the
// building block for the versioning scenarios. A single specChange→status
// reaction keeps it a valid leaf kind.
func vManifest(kind model.Kind, kindVersion int, specSchema string) model.KindManifest {
	return model.KindManifest{
		Kind:        kind,
		KindVersion: kindVersion,
		SpecSchema:  json.RawMessage(specSchema),
		Reactions: []model.ReactionDecl{
			{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
		},
	}
}

func resourceKindVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id uuid.UUID) int {
	t.Helper()
	var m int
	require.NoError(t, pool.QueryRow(ctx, `SELECT kind_version FROM resources WHERE id = $1`, id).Scan(&m))
	return m
}

// seedPendingKindVersion inserts n pending (worker_id IS NULL) reconcile rows for
// (kind, kindVersion) into a shard — the versioning analogue of seedPending, stamping
// an explicit kindVersion so the per-kindVersion claim/cap paths can be exercised directly.
func seedPendingKindVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind model.Kind, kindVersion int, shard int16, n int) {
	t.Helper()
	for range n {
		_, err := pool.Exec(ctx,
			`INSERT INTO work_queue (resource_id, task_type, kind, kind_version, generation, spec, shard_id)
			 VALUES ($1, 'reconcile', $2, $3, 1, '{}'::jsonb, $4)`,
			uuid.New(), string(kind), kindVersion, shard)
		require.NoError(t, err)
	}
}

// TestVersioning is the umbrella: one Postgres, subtests share the migrated pool
// but use distinct kinds so they don't collide.
func TestVersioning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	pool := setupPostgres(t, ctx)
	migrateForSeed(t, ctx, pool)
	st := store.New(pool)

	// ── Publish: per-(kind, kindVersion) manifests coexist; a new kindVersion is isolated ──
	t.Run("Publish_PerKindVersion_Isolated", func(t *testing.T) {
		v1, err := st.UpsertKindManifest(ctx, vManifest("vpc", 1, `{"type":"object","properties":{"cidr":{"type":"string"}}}`))
		require.NoError(t, err)
		v2, err := st.UpsertKindManifest(ctx, vManifest("vpc", 2, `{"type":"object","properties":{"cidr":{"type":"string"},"region":{"type":"string"}}}`))
		require.NoError(t, err)
		require.NotEqual(t, v1, v2, "distinct kind versions have distinct content hashes")

		// Both (kind, kindVersion) kind_config rows exist independently.
		kc1, ok1, err := st.GetKindConfig(ctx, "vpc", 1)
		require.NoError(t, err)
		require.True(t, ok1, "vpc/v1 kind_config derived")
		require.Equal(t, 1, kc1.KindVersion)
		kc2, ok2, err := st.GetKindConfig(ctx, "vpc", 2)
		require.NoError(t, err)
		require.True(t, ok2, "vpc/v2 kind_config derived")
		require.Equal(t, 2, kc2.KindVersion)

		// The kind string stays BARE — the address is (kind, name), kindVersion is a column.
		m1, ok, err := st.GetKindManifest(ctx, "vpc", 1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, model.Kind("vpc"), m1.Kind, "kind string is bare vpc, not vpc/v1")
		require.Equal(t, 1, m1.KindVersion)
	})

	// ── Resource pins its kindVersion on create; the two kind versions don't cross ──
	t.Run("PinOnCreate", func(t *testing.T) {
		_, err := st.UpsertKindManifest(ctx, vManifest("bucket", 1, `{"type":"object"}`))
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, vManifest("bucket", 2, `{"type":"object"}`))
		require.NoError(t, err)

		r1, err := st.ApplySpecKindVersionWithConfig(ctx, "bucket", 1, "b-v1", json.RawMessage(`{}`), nil, "")
		require.NoError(t, err)
		r2, err := st.ApplySpecKindVersionWithConfig(ctx, "bucket", 2, "b-v2", json.RawMessage(`{}`), nil, "")
		require.NoError(t, err)

		require.Equal(t, 1, resourceKindVersion(t, ctx, pool, r1.ID), "apply at v1 pins kindVersion=1")
		require.Equal(t, 2, resourceKindVersion(t, ctx, pool, r2.ID), "apply at v2 pins kindVersion=2")

		// Each resource is stamped with ITS (kind, kindVersion)'s manifest_version (read
		// from that kindVersion's kind_config). NOTE: manifest_version is CONTENT-derived,
		// so two kind versions with identical schema+reactions share a hash — the invariant
		// is "stamped from my kindVersion's config", not "differs across kind versions".
		cfgVer := func(kindVersion int) int64 {
			var v int64
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT manifest_version FROM kind_config WHERE kind='bucket' AND kind_version=$1`, kindVersion).Scan(&v))
			return v
		}
		var mv1, mv2 int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT manifest_version FROM resources WHERE id=$1`, r1.ID).Scan(&mv1))
		require.NoError(t, pool.QueryRow(ctx, `SELECT manifest_version FROM resources WHERE id=$1`, r2.ID).Scan(&mv2))
		require.Equal(t, cfgVer(1), mv1, "v1 resource stamped from bucket/v1 config")
		require.Equal(t, cfgVer(2), mv2, "v2 resource stamped from bucket/v2 config")

		// (kind,name) resolves to the pinned kindVersion.
		id, curKindVersion, found, err := st.ResolveResourceIDKindVersionByKindName(ctx, "bucket", "b-v2")
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, r2.ID, id)
		require.Equal(t, 2, curKindVersion)
	})

	// ── uuid-stable flip + reject-on-schema-fail ──
	t.Run("Flip_UUIDStable", func(t *testing.T) {
		// v1 accepts {size:int}; v2 requires {size:int, tier:string}.
		dv1 := vManifest("disk", 1, `{"type":"object","properties":{"size":{"type":"integer"}}}`)
		dv2 := vManifest("disk", 2, `{"type":"object","properties":{"size":{"type":"integer"},"tier":{"type":"string"}},"required":["tier"]}`)
		_, err := st.UpsertKindManifest(ctx, dv1)
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, dv2)
		require.NoError(t, err)

		// The reject-on-schema-fail gate fires through the store's validator, so
		// wire one from the per-kindVersion manifests (production does this from the applied
		// CRDs; a bare store.New carries none, making the gate a silent no-op).
		vst := st.WithValidator(specschema.New([]model.KindManifest{dv1, dv2}))

		r, err := vst.ApplySpecKindVersionWithConfig(ctx, "disk", 1, "d1", json.RawMessage(`{"size":10}`), nil, "")
		require.NoError(t, err)
		require.Equal(t, 1, resourceKindVersion(t, ctx, pool, r.ID))
		genBefore := r.Generation

		// A flip to v2 with a spec MISSING the v2-required field is REJECTED;
		// the resource stays on v1.
		_, ferr := vst.FlipResourceKindVersion(ctx, r.ID, 1, 2, json.RawMessage(`{"size":10}`), "disk")
		require.Error(t, ferr, "flip to v2 with an incomplete v2 spec must be rejected")
		require.ErrorIs(t, ferr, store.ErrInvalidSpec)
		require.Equal(t, 1, resourceKindVersion(t, ctx, pool, r.ID), "rejected flip leaves the resource on v1")

		// A valid v2 spec flips the SAME uuid in place, bumping kindVersion+generation.
		fr, err := vst.FlipResourceKindVersion(ctx, r.ID, 1, 2, json.RawMessage(`{"size":10,"tier":"gold"}`), "disk")
		require.NoError(t, err)
		require.True(t, fr.Flipped)
		require.Equal(t, 2, resourceKindVersion(t, ctx, pool, r.ID), "flip moves the resource to v2 in place")

		var genAfter int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT generation FROM resources WHERE id=$1`, r.ID).Scan(&genAfter))
		require.Greater(t, genAfter, genBefore, "flip bumps generation (forces re-reconcile under v2)")

		// Idempotent: re-flipping an already-v2 row from old-kindVersion=1 is a no-op.
		fr2, err := vst.FlipResourceKindVersion(ctx, r.ID, 1, 2, json.RawMessage(`{"size":10,"tier":"gold"}`), "disk")
		require.NoError(t, err)
		require.False(t, fr2.Flipped, "a redelivered flip whose row already moved is a no-op")
	})

	// ── Per-(kind, kindVersion) concurrency cap: v1 and v2 have independent budgets ──
	t.Run("PerKindVersionCap_Independent", func(t *testing.T) {
		_, err := st.UpsertKindManifest(ctx, vManifest("job", 1, `{"type":"object"}`))
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, vManifest("job", 2, `{"type":"object"}`))
		require.NoError(t, err)
		// Cap v1 at 2, v2 at 3 — independent per-kindVersion caps.
		require.NoError(t, st.UpsertKindConfig(ctx, store.KindConfig{Kind: "job", KindVersion: 1, MaxInflight: 2}))
		require.NoError(t, st.UpsertKindConfig(ctx, store.KindConfig{Kind: "job", KindVersion: 2, MaxInflight: 3}))

		// Seed 10 pending v1 rows and 10 pending v2 rows on shard 0.
		seedPendingKindVersion(t, ctx, pool, "job", 1, 0, 10)
		seedPendingKindVersion(t, ctx, pool, "job", 2, 0, 10)
		shards := []int16{0, 1, 2, 3}

		v1Claimed, err := st.WorkQueueTakeBatch(ctx, "job", 1, store.TaskReconcile, "pod-a", 50, shards)
		require.NoError(t, err)
		require.Len(t, v1Claimed, 2, "v1 claim is bounded by v1's cap of 2")

		v2Claimed, err := st.WorkQueueTakeBatch(ctx, "job", 2, store.TaskReconcile, "pod-a", 50, shards)
		require.NoError(t, err)
		require.Len(t, v2Claimed, 3, "v2 claim is bounded by v2's INDEPENDENT cap of 3")

		// Every claimed row carries its own kindVersion.
		for _, tk := range v1Claimed {
			require.Equal(t, 1, tk.KindVersion, "a v1 claim yields only v1 tasks")
		}
		for _, tk := range v2Claimed {
			require.Equal(t, 2, tk.KindVersion, "a v2 claim yields only v2 tasks")
		}
	})

	// ── Strict claim routing: a claim for kindVersion M never takes another kindVersion's rows ──
	t.Run("ClaimRouting_Strict", func(t *testing.T) {
		_, err := st.UpsertKindManifest(ctx, vManifest("route", 1, `{"type":"object"}`))
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, vManifest("route", 2, `{"type":"object"}`))
		require.NoError(t, err)
		// No cap — pure routing. 5 v1 + 5 v2 pending rows on shard 0.
		seedPendingKindVersion(t, ctx, pool, "route", 1, 0, 5)
		seedPendingKindVersion(t, ctx, pool, "route", 2, 0, 5)
		shards := []int16{0, 1, 2, 3}

		// A v1 claim takes ONLY the 5 v1 rows, never a v2 row (strict, no wildcard).
		got, err := st.WorkQueueTakeBatch(ctx, "route", 1, store.TaskReconcile, "pod-r", 50, shards)
		require.NoError(t, err)
		require.Len(t, got, 5, "a kindVersion-1 claim takes exactly the 5 v1 rows")
		for _, tk := range got {
			require.Equal(t, 1, tk.KindVersion)
		}
		// The 5 v2 rows remain unclaimed for a v1 worker.
		var v2Pending int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM work_queue WHERE kind='route' AND kind_version=2 AND worker_id IS NULL`).Scan(&v2Pending))
		require.Equal(t, 5, v2Pending, "v2 rows are NOT claimable by a v1 claim")
	})

	// ── Per-kindVersion schema validation: a spec is checked against ITS kindVersion's schema ──
	t.Run("PerKindVersionSchemaValidation", func(t *testing.T) {
		// v1: cidr only. v2: requires region.
		nv1 := vManifest("net", 1, `{"type":"object","properties":{"cidr":{"type":"string"}},"required":["cidr"]}`)
		nv2 := vManifest("net", 2, `{"type":"object","properties":{"cidr":{"type":"string"},"region":{"type":"string"}},"required":["cidr","region"]}`)
		_, err := st.UpsertKindManifest(ctx, nv1)
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, nv2)
		require.NoError(t, err)

		// The data-gateway validator must be BUILT from the per-kindVersion manifests (in
		// production cmd/converge does this from the applied CRDs). A bare store.New
		// carries no validator, so wire one keyed by (net,1) + (net,2) here — this is
		// what makes ApplySpecKindVersionWithConfig check against the right kindVersion's schema.
		vst := st.WithValidator(specschema.New([]model.KindManifest{nv1, nv2}))

		// A spec valid for v1 (no region) is fine at v1...
		_, err = vst.ApplySpecKindVersionWithConfig(ctx, "net", 1, "n1", json.RawMessage(`{"cidr":"10.0.0.0/8"}`), nil, "")
		require.NoError(t, err, "v1 spec (no region) valid against v1 schema")

		// ...but the SAME spec applied at v2 is REJECTED (v2 requires region).
		_, err = vst.ApplySpecKindVersionWithConfig(ctx, "net", 2, "n2", json.RawMessage(`{"cidr":"10.0.0.0/8"}`), nil, "")
		require.Error(t, err, "a v1-shaped spec applied at v2 must fail v2's schema (region required)")
		require.ErrorIs(t, err, store.ErrInvalidSpec)

		// A v2-complete spec applies at v2.
		_, err = vst.ApplySpecKindVersionWithConfig(ctx, "net", 2, "n2", json.RawMessage(`{"cidr":"10.0.0.0/8","region":"us"}`), nil, "")
		require.NoError(t, err, "a complete v2 spec is valid at v2")
	})

	// ── The publish-time additive-vs-breaking linter, over the real HTTP gate ──
	t.Run("PublishGate_Linter", func(t *testing.T) {
		srv := api.NewServer(api.DepsFromPools(pool, nil))
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()

		put := func(kind string, kindVersion int, specSchema string) int {
			body := map[string]any{
				"kind":         kind,
				"kind_version": kindVersion,
				"spec_schema":  json.RawMessage(specSchema),
				"reactions": []map[string]any{
					{"name": "work", "trigger": "specChange", "emits": []string{"status"}},
				},
			}
			raw, _ := json.Marshal(body)
			req, err := newPut(ctx, ts.URL+"/api/v1/kinds/"+kind+"/manifest", raw)
			require.NoError(t, err)
			resp, err := httpClient().Do(req)
			require.NoError(t, err)
			resp.Body.Close()
			return resp.StatusCode
		}

		// First publish of app/v1 — allowed (no prior).
		require.Equal(t, 200, put("app", 1, `{"type":"object","properties":{"a":{"type":"string"}}}`))
		// Idempotent re-publish (identical) — allowed.
		require.Equal(t, 200, put("app", 1, `{"type":"object","properties":{"a":{"type":"string"}}}`))
		// ADDITIVE change (add an optional field) — AUTO-ALLOWED, no flag.
		require.Equal(t, 200, put("app", 1, `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"}}}`),
			"an additive schema change is auto-allowed within a kindVersion")
		// BREAKING change (add a REQUIRED field) — 409, ALWAYS. There is no in-place
		// override: a backward-incompatible edit must ship as a NEW kindVersion so live
		// resources of this kindVersion keep validating against the schema they were
		// applied under. (Additive edits above need no flag; breaking edits are never
		// applied in place.)
		require.Equal(t, 409, put("app", 1, `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"string"},"c":{"type":"string"}},"required":["c"]}`),
			"a breaking schema change is rejected 409 (ship a new kindVersion) — no override")
		// Publishing app/v2 (a brand-new kindVersion) — always allowed, no gate.
		require.Equal(t, 200, put("app", 2, `{"type":"object","properties":{"totally":{"type":"integer"}},"required":["totally"]}`),
			"a breaking change shipped as a NEW kindVersion is allowed")
	})

	// ── Provider configs are per-(kind, kindVersion): coexist, independent defaults, and
	//    the ATTACHMENT guard — a resource may only attach a config of its OWN
	//    (kind, kindVersion). This is the correctness hole the feature closes. ──
	t.Run("ProviderConfig_PerKindVersion", func(t *testing.T) {
		// db/v1 and db/v2 both published (db/v1 has no required fields; v2 requires
		// tier so a v2 resource is complete only with it).
		_, err := st.UpsertKindManifest(ctx, vManifest("db", 1, `{"type":"object","properties":{"size":{"type":"integer"}}}`))
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, vManifest("db", 2, `{"type":"object","properties":{"size":{"type":"integer"},"tier":{"type":"string"}},"required":["tier"]}`))
		require.NoError(t, err)

		// A CUSTOM config for db/v1 and a DISTINCT-name custom config for db/v2
		// coexist — name is globally unique, so each name pins one kindVersion.
		cfg1, _, err := st.UpsertProviderConfig(ctx, "db-cfg-v1", "db", 1, false, json.RawMessage(`{}`), nil)
		require.NoError(t, err)
		require.Equal(t, 1, cfg1.KindVersion)
		cfg2, _, err := st.UpsertProviderConfig(ctx, "db-cfg-v2", "db", 2, false, json.RawMessage(`{}`), nil)
		require.NoError(t, err)
		require.Equal(t, 2, cfg2.KindVersion)

		// DEFAULT is per-(kind, kindVersion): a v1 default AND a v2 default coexist (the
		// uq_providerconfigs_default_per_kind index is on (kind, kindVersion)).
		_, _, err = st.UpsertProviderConfig(ctx, "db-default-v1", "db", 1, true, json.RawMessage(`{}`), nil)
		require.NoError(t, err, "db/v1 may have its own default")
		_, _, err = st.UpsertProviderConfig(ctx, "db-default-v2", "db", 2, true, json.RawMessage(`{}`), nil)
		require.NoError(t, err, "db/v2 may have its OWN default independently of v1's")
		// A SECOND v1 default is rejected (one default per (kind, kindVersion)).
		_, _, err = st.UpsertProviderConfig(ctx, "db-default-v1-again", "db", 1, true, json.RawMessage(`{}`), nil)
		require.ErrorIs(t, err, store.ErrConfigDefaultExists, "a second default for db/v1 must be rejected")

		def1, ok1, err := st.GetDefaultProviderConfig(ctx, "db", 1)
		require.NoError(t, err)
		require.True(t, ok1, "db/v1 has a default")
		def2, ok2, err := st.GetDefaultProviderConfig(ctx, "db", 2)
		require.NoError(t, err)
		require.True(t, ok2, "db/v2 has its own default")
		_ = def1
		_ = def2

		// THE GUARD — both directions. A db/v1 resource may attach the v1 config...
		_, err = st.ApplySpecKindVersionWithConfig(ctx, "db", 1, "db-a", json.RawMessage(`{"size":1}`), nil, "db-cfg-v1")
		require.NoError(t, err, "a v1 resource may attach a v1 config")

		// ...but NOT the v2 config (kindVersion mismatch → rejected).
		_, err = st.ApplySpecKindVersionWithConfig(ctx, "db", 1, "db-b", json.RawMessage(`{"size":1}`), nil, "db-cfg-v2")
		require.Error(t, err, "a v1 resource must NOT attach a v2 config")
		require.ErrorIs(t, err, store.ErrConfigKindMismatch, "attaching a v2 config to a v1 resource is a (kind,kindVersion) mismatch")

		// And the reverse: a db/v2 resource may attach the v2 config...
		_, err = st.ApplySpecKindVersionWithConfig(ctx, "db", 2, "db-c", json.RawMessage(`{"size":1,"tier":"gold"}`), nil, "db-cfg-v2")
		require.NoError(t, err, "a v2 resource may attach a v2 config")

		// ...but NOT the v1 config.
		_, err = st.ApplySpecKindVersionWithConfig(ctx, "db", 2, "db-d", json.RawMessage(`{"size":1,"tier":"gold"}`), nil, "db-cfg-v1")
		require.Error(t, err, "a v2 resource must NOT attach a v1 config")
		require.ErrorIs(t, err, store.ErrConfigKindMismatch, "attaching a v1 config to a v2 resource is a (kind,kindVersion) mismatch")

		// THE SAME GUARD OVER HTTP: the store returns ErrConfigKindMismatch; the API
		// must map it to a client 422 (well-formed body, invalid config reference), not
		// leak a 500. A conctl apply of a mismatched provider_config_ref (documented in
		// the classic versioning walkthrough) depends on this mapping.
		srv := api.NewServer(api.DepsFromPools(pool, nil))
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()
		srv.SetDeclaredSchemas([]model.KindManifest{
			vManifest("db", 1, `{"type":"object","properties":{"size":{"type":"integer"}}}`),
			vManifest("db", 2, `{"type":"object","properties":{"size":{"type":"integer"},"tier":{"type":"string"}},"required":["tier"]}`),
		})
		body, _ := json.Marshal(map[string]any{
			"kind": "db", "kind_version": 1, "name": "db-http-mismatch",
			"spec": json.RawMessage(`{"size":1}`), "provider_config_ref": "db-cfg-v2",
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/v1/resources", bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, 422, resp.StatusCode,
			"attaching a v2 config to a v1 resource over HTTP must be a 422, not a leaked 500")
	})

	// ── Retire a version: FREEZE new creates/flips onto it, DRAIN existing ──
	t.Run("Retire_FreezeNewDrainExisting", func(t *testing.T) {
		srv := api.NewServer(api.DepsFromPools(pool, nil))
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()

		// v1 + v2 both published (identical trivial schema).
		rv1 := vManifest("widget", 1, `{"type":"object"}`)
		rv2 := vManifest("widget", 2, `{"type":"object"}`)
		_, err := st.UpsertKindManifest(ctx, rv1)
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, rv2)
		require.NoError(t, err)
		// The API server gates apply on the DECLARED set; wire both versions so
		// widget is a known kind and the per-version schema validates.
		srv.SetDeclaredSchemas([]model.KindManifest{rv1, rv2})

		applyResource := func(name string, kindVersion int) int {
			body, _ := json.Marshal(map[string]any{
				"kind":         "widget",
				"kind_version": kindVersion,
				"name":         name,
				"spec":         json.RawMessage(`{}`),
			})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/v1/resources", bytes.NewReader(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp, err := httpClient().Do(req)
			require.NoError(t, err)
			resp.Body.Close()
			return resp.StatusCode
		}

		// Before retirement: a v1 create succeeds (201).
		require.Equal(t, 201, applyResource("w-live", 1), "a create on a live version succeeds")

		// Retire widget/v1 at runtime via the operational-config endpoint.
		require.NoError(t, st.UpsertKindConfig(ctx, store.KindConfig{Kind: "widget", KindVersion: 1, Retired: true}))
		kc, ok, err := st.GetKindConfig(ctx, "widget", 1)
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, kc.Retired, "widget/v1 is now retired")

		// FREEZE-NEW: a NEW create onto retired v1 is rejected 422.
		require.Equal(t, 422, applyResource("w-new-on-retired", 1),
			"a new create onto a retired version is frozen (422)")

		// DRAIN-EXISTING: an in-place UPDATE of the pre-existing v1 resource is NOT
		// blocked (it keeps reconciling so it can drain / be migrated).
		require.Equal(t, 200, applyResource("w-live", 1),
			"an existing resource on a retired version keeps reconciling (update allowed)")

		// MIGRATE OFF: a FLIP of the existing v1 resource ONTO the live v2 is allowed
		// (that is the migration path off a retired version).
		require.Equal(t, 200, applyResource("w-live", 2),
			"flipping an existing resource OFF the retired version onto a live one is allowed")

		// A create onto the LIVE v2 still works while v1 is retired.
		require.Equal(t, 201, applyResource("w-on-v2", 2),
			"a live version is unaffected by another version's retirement")
	})

	// ── Per-binding reactor-version pin: NULL follows the reactor's highest
	//    published version; a pin locks the exact version. Exercised at the
	//    claim_reactor_deliveries resolution layer. ──
	t.Run("ReactorVersion_Pin", func(t *testing.T) {
		// A reactor kind published at TWO versions with DISTINCT reaction names, so
		// the resolved name reveals which version the claim picked.
		rxV1 := model.KindManifest{
			Kind: "notif", KindVersion: 1,
			Reactions: []model.ReactionDecl{
				{Name: "react-v1", Trigger: model.TriggerReactor, Emits: model.OutcomeMask{model.OutcomeSideEffect}},
			},
		}
		rxV2 := model.KindManifest{
			Kind: "notif", KindVersion: 2,
			Reactions: []model.ReactionDecl{
				{Name: "react-v2", Trigger: model.TriggerReactor, Emits: model.OutcomeMask{model.OutcomeSideEffect}},
			},
		}
		_, err := st.UpsertKindManifest(ctx, rxV1)
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, rxV2)
		require.NoError(t, err)

		// A watched resource that will cross a transition. Its shard determines the
		// lifecycle_outbox partition; shard_of() is computed by the emit query, so we
		// read the row's shard back to claim it.
		_, err = st.UpsertKindManifest(ctx, vManifest("watched", 1, `{"type":"object"}`))
		require.NoError(t, err)
		wr, err := st.ApplySpecKindVersionWithConfig(ctx, "watched", 1, "w-notif", json.RawMessage(`{}`), nil, "")
		require.NoError(t, err)

		// resolveReaction seeds a binding (with the given pin), emits ONE 'synced'
		// lifecycle_outbox row for the watched resource against it, claims it across
		// all shards, and returns BOTH the resolved reaction name AND the resolved
		// reactor kind_version — i.e. which reactor version the claim picked AND that
		// the version is carried through the delivery (the seam the worker uses to run
		// the right handler + pull the right config).
		resolveReaction := func(bindingName string, pin *int) (string, int) {
			require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
				Name:           bindingName,
				WatchKind:      "watched",
				Transition:     string(model.TransitionSynced),
				Reactor:        "notif",
				ReactorVersion: pin,
				Enabled:        true,
			}))
			// Emit the delivery row directly for this binding (the cascade would do
			// this on a real 'synced' crossing; here we insert it deterministically).
			_, err := pool.Exec(ctx,
				`INSERT INTO lifecycle_outbox (resource_id, kind, transition, generation, binding_name, shard_id)
				 VALUES ($1, 'watched', 'synced', $2, $3, shard_of($1))
				 ON CONFLICT DO NOTHING`,
				wr.ID, wr.Generation, bindingName)
			require.NoError(t, err)
			allShards := make([]int16, 256)
			for i := range allShards {
				allShards[i] = int16(i)
			}
			deliveries, err := st.ClaimReactorDeliveries(ctx, "pod-notif", 50, allShards)
			require.NoError(t, err)
			for _, d := range deliveries {
				if d.BindingName == bindingName {
					// Ack so a subsequent resolveReaction call re-emits cleanly.
					// The ack is FENCED on the delivery's claim_epoch, so thread the
					// epoch this delivery was claimed under (ClaimReactorDeliveries bumped it).
					require.NoError(t, st.AckReactorDelivery(ctx, d.ResourceID, d.Transition, d.Generation, d.BindingName, d.ClaimEpoch))
					return d.Reaction, d.ReactorKindVersion
				}
			}
			t.Fatalf("no delivery claimed for binding %q", bindingName)
			return "", 0
		}

		// UNPINNED: resolves the reactor's HIGHEST published version (v2) — and the
		// delivery carries reactor_kind_version=2 (the seam that runs v2's handler).
		rx, rv := resolveReaction("notif-unpinned", nil)
		require.Equal(t, "react-v2", rx, "an unpinned binding resolves the reactor's highest published version")
		require.Equal(t, 2, rv, "the delivery carries the resolved reactor kind_version (2)")

		// PINNED to v1: resolves exactly v1's reaction + version, immune to the v2 publish.
		one := 1
		rx, rv = resolveReaction("notif-pin-v1", &one)
		require.Equal(t, "react-v1", rx, "a binding pinned to reactor_version=1 resolves v1's reaction")
		require.Equal(t, 1, rv, "the delivery carries reactor_kind_version=1 for a v1 pin")

		// PINNED to v2: resolves v2's reaction + version.
		two := 2
		rx, rv = resolveReaction("notif-pin-v2", &two)
		require.Equal(t, "react-v2", rx, "a binding pinned to reactor_version=2 resolves v2's reaction")
		require.Equal(t, 2, rv, "the delivery carries reactor_kind_version=2 for a v2 pin")
	})

	// ── watch_kind_version scope: a version-scoped binding fires ONLY for that
	//    version's resources; an unscoped binding fires for all versions. Exercised
	//    over the cascade 'synced' emit (the real fan-out path). ──
	t.Run("WatchKindVersion_Scope", func(t *testing.T) {
		// A reactor to run, and a watched kind published at v1 + v2.
		_, err := st.UpsertKindManifest(ctx, model.KindManifest{
			Kind: "sink", KindVersion: 1,
			Reactions: []model.ReactionDecl{
				{Name: "run", Trigger: model.TriggerReactor, Emits: model.OutcomeMask{model.OutcomeSideEffect}},
			},
		})
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, vManifest("svc", 1, `{"type":"object"}`))
		require.NoError(t, err)
		_, err = st.UpsertKindManifest(ctx, vManifest("svc", 2, `{"type":"object"}`))
		require.NoError(t, err)

		// One svc/v1 resource and one svc/v2 resource.
		v1r, err := st.ApplySpecKindVersionWithConfig(ctx, "svc", 1, "svc-a", json.RawMessage(`{}`), nil, "")
		require.NoError(t, err)
		v2r, err := st.ApplySpecKindVersionWithConfig(ctx, "svc", 2, "svc-b", json.RawMessage(`{}`), nil, "")
		require.NoError(t, err)

		// A binding SCOPED to svc/v2 'synced'.
		two := 2
		require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
			Name:             "svc-v2-only",
			WatchKind:        "svc",
			WatchKindVersion: &two,
			Transition:       string(model.TransitionSynced),
			Reactor:          "sink",
			Enabled:          true,
		}))

		// Emit a 'synced' via the same fan-out the cascade uses: INSERT joining the
		// binding AND the version filter. A helper mirrors the cascade's WHERE so the
		// scope is what's under test (the cascade trigger runs the identical predicate).
		emitSynced := func(id uuid.UUID, kindVersion int, gen int64) {
			_, err := pool.Exec(ctx,
				`INSERT INTO lifecycle_outbox (resource_id, kind, transition, generation, binding_name, shard_id)
				 SELECT $1, 'svc', 'synced', $3, b.name, shard_of($1)
				   FROM reactor_bindings b
				  WHERE b.enabled AND b.watch_kind = 'svc' AND b.transition = 'synced'
				    AND (b.watch_kind_version IS NULL OR b.watch_kind_version = $2::smallint)
				 ON CONFLICT DO NOTHING`,
				id, kindVersion, gen)
			require.NoError(t, err)
		}
		emitSynced(v1r.ID, 1, v1r.Generation) // svc/v1 syncs — the v2-scoped binding must NOT fire
		emitSynced(v2r.ID, 2, v2r.Generation) // svc/v2 syncs — it MUST fire

		// Exactly ONE delivery for the scoped binding: the v2 resource, never v1.
		var count, forV2, forV1 int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*),
			        count(*) FILTER (WHERE resource_id = $1),
			        count(*) FILTER (WHERE resource_id = $2)
			   FROM lifecycle_outbox WHERE binding_name = 'svc-v2-only'`,
			v2r.ID, v1r.ID).Scan(&count, &forV2, &forV1))
		require.Equal(t, 1, count, "a version-scoped binding emits exactly one delivery")
		require.Equal(t, 1, forV2, "the v2-scoped binding fires for the svc/v2 resource")
		require.Equal(t, 0, forV1, "the v2-scoped binding does NOT fire for the svc/v1 resource")
	})

	// ── DeleteKindManifest: drop a (kind, kindVersion) only when nothing references
	//    it — block on any resource + on an EXACT-version-pinned binding (an UNPINNED
	//    binding does NOT block), cascade the version's provider configs, idempotent. ──
	t.Run("DeleteKindVersion", func(t *testing.T) {
		manifestExists := func(kind model.Kind, kv int) bool {
			_, ok, err := st.GetKindManifest(ctx, kind, kv)
			require.NoError(t, err)
			return ok
		}
		configRowExists := func(kind model.Kind, kv int) bool {
			_, ok, err := st.GetKindConfig(ctx, kind, kv)
			require.NoError(t, err)
			return ok
		}

		// CLEAN DELETE + CASCADE CONFIGS + SIBLING SURVIVES: del/v1 and del/v2 both
		// published; del/v1 carries a default + a custom provider config and NO
		// resources. Deleting del/v1 removes its manifest, kind_config, and BOTH
		// configs, while del/v2 is untouched.
		t.Run("clean_delete_cascades_configs_sibling_survives", func(t *testing.T) {
			_, err := st.UpsertKindManifest(ctx, vManifest("del", 1, `{"type":"object"}`))
			require.NoError(t, err)
			_, err = st.UpsertKindManifest(ctx, vManifest("del", 2, `{"type":"object"}`))
			require.NoError(t, err)
			_, _, err = st.UpsertProviderConfig(ctx, "del-v1-default", "del", 1, true, json.RawMessage(`{}`), nil)
			require.NoError(t, err)
			_, _, err = st.UpsertProviderConfig(ctx, "del-v1-custom", "del", 1, false, json.RawMessage(`{}`), nil)
			require.NoError(t, err)

			found, deletedConfigs, err := st.DeleteKindManifest(ctx, "del", 1)
			require.NoError(t, err)
			require.True(t, found, "del/v1 existed → deleted")
			require.Equal(t, int64(2), deletedConfigs, "both del/v1 provider configs cascade-deleted")

			require.False(t, manifestExists("del", 1), "del/v1 manifest gone")
			require.False(t, configRowExists("del", 1), "del/v1 kind_config gone")
			var cfgLeft int
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT count(*) FROM providerconfigs WHERE kind='del' AND kind_version=1`).Scan(&cfgLeft))
			require.Zero(t, cfgLeft, "no del/v1 provider config rows remain")

			require.True(t, manifestExists("del", 2), "sibling del/v2 manifest untouched")
			require.True(t, configRowExists("del", 2), "sibling del/v2 kind_config untouched")
		})

		// IDEMPOTENT: re-deleting an already-gone version is found=false (the API maps
		// it to 404), never an error.
		t.Run("idempotent_already_gone", func(t *testing.T) {
			found, _, err := st.DeleteKindManifest(ctx, "del", 1)
			require.NoError(t, err)
			require.False(t, found, "re-deleting a gone version is a no-op (found=false)")
		})

		// BLOCKED BY A RESOURCE: a version with any resource (even one draining) is
		// not deletable; the manifest stays.
		t.Run("blocked_by_resource", func(t *testing.T) {
			_, err := st.UpsertKindManifest(ctx, vManifest("delr", 1, `{"type":"object"}`))
			require.NoError(t, err)
			_, err = st.ApplySpecKindVersionWithConfig(ctx, "delr", 1, "r1", json.RawMessage(`{}`), nil, "")
			require.NoError(t, err)

			found, _, err := st.DeleteKindManifest(ctx, "delr", 1)
			require.ErrorIs(t, err, store.ErrKindVersionInUse, "a version with a resource cannot be deleted")
			require.False(t, found)
			require.True(t, manifestExists("delr", 1), "the blocked manifest is untouched")
		})

		// BLOCKED BY AN EXACT-VERSION-PINNED WATCH binding: a binding pinning
		// watch_kind_version=V blocks deleting V.
		t.Run("blocked_by_exact_watch_pin", func(t *testing.T) {
			_, err := st.UpsertKindManifest(ctx, vManifest("delw", 1, `{"type":"object"}`))
			require.NoError(t, err)
			v := 1
			require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
				Name: "delw-v1-watch", WatchKind: "delw", WatchKindVersion: &v,
				Transition: "synced", Reactor: "somesink", Enabled: true,
			}))
			_, _, err = st.DeleteKindManifest(ctx, "delw", 1)
			require.ErrorIs(t, err, store.ErrKindVersionInUse, "an exact watch-version pin blocks the delete")
			require.True(t, manifestExists("delw", 1))
		})

		// BLOCKED BY AN EXACT-VERSION-PINNED REACTOR binding: a binding pinning
		// reactor_version=V blocks deleting the reactor kind's version V.
		t.Run("blocked_by_exact_reactor_pin", func(t *testing.T) {
			_, err := st.UpsertKindManifest(ctx, vManifest("delrk", 2, `{"type":"object"}`))
			require.NoError(t, err)
			rv := 2
			require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
				Name: "delrk-v2-reactor", WatchKind: "somekind", Transition: "synced",
				Reactor: "delrk", ReactorVersion: &rv, Enabled: true,
			}))
			_, _, err = st.DeleteKindManifest(ctx, "delrk", 2)
			require.ErrorIs(t, err, store.ErrKindVersionInUse, "an exact reactor-version pin blocks the delete")
			require.True(t, manifestExists("delrk", 2))
		})

		// UNPINNED binding does NOT block: a binding watching the kind with NO
		// version pin resolves to the remaining versions, so a version can still be
		// deleted out from under it.
		t.Run("unpinned_binding_does_not_block", func(t *testing.T) {
			_, err := st.UpsertKindManifest(ctx, vManifest("delu", 1, `{"type":"object"}`))
			require.NoError(t, err)
			require.NoError(t, st.UpsertReactorBinding(ctx, store.ReactorBinding{
				Name: "delu-unpinned", WatchKind: "delu", WatchKindVersion: nil, // ALL versions
				Transition: "synced", Reactor: "somesink", Enabled: true,
			}))
			found, _, err := st.DeleteKindManifest(ctx, "delu", 1)
			require.NoError(t, err, "an UNPINNED binding must not block the delete")
			require.True(t, found)
			require.False(t, manifestExists("delu", 1), "delu/v1 deleted despite the unpinned binding")
		})
	})
}
