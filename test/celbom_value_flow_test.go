package test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/datadriven/celbom"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

// TestCelbomValueFlowEndToEnd proves the dependency + value-flow simulation against
// a real Postgres + a running engine, end to end:
//
//	celbom root (deployment_instance BOM)
//	  └─ compose (the depends_on LIST rule) → per team: fakeapp + fakevpc + fakedb + 2 DepEdges
//	       fakevpc → status.vpc_id;  fakedb → status.db_endpoint
//	       the engine FLOWS both into fakeapp.spec (the edges' ValueFlows)
//	       fakeapp reconciles → status.wired_to + status.db  (terminal-fails if EITHER empty)
//
// A Ready fakeapp with BOTH set is the proof: the two edges GATED it until BOTH
// upstreams were Ready AND both values FLOWED — the real multi-edge value-flow path.
func TestCelbomValueFlowEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// Seed the celbom DEFAULT providerconfig BEFORE building the registry: the rule
	// with a depends_on LIST (fakevpc + fakedb). This is what the demo POSTs; here we
	// write it directly so the composer reads it as its live default.
	// kind_version is REQUIRED and explicit (>= 1) on every rule child/dep — no implicit
	// v1 default; the composer stamps it and ApplyComposeResult rejects a 0.
	rules := json.RawMessage(`{
	  "rules": [
	    { "name": "app",
	      "when": "team.fd.startsWith(\"fd-\")",
	      "child": { "kind": "fakeapp", "kind_version": 1, "name": "${fd}-${team}-app" },
	      "depends_on": [
	        { "kind": "fakevpc", "kind_version": 1, "name": "${fd}-${team}-vpc", "flow": [ { "from": "/vpc_id", "to": "/vpc_id" } ] },
	        { "kind": "fakedb",  "kind_version": 1, "name": "${fd}-${team}-db",  "flow": [ { "from": "/db_endpoint", "to": "/db_endpoint" } ] }
	      ] }
	  ]
	}`)
	// Migrate before seeding the providerconfig (startEngineWithRegistry migrates too —
	// MigrateAll is idempotent — but the upsert below needs the schema NOW).
	migrateForSeed(t, ctx, pool)
	repo := store.New(pool)

	// Seed celbom's DEFAULT providerconfig (the rule set) FIRST, then read its bytes so
	// demoruntime.CELBOM holds them — the in-process test worker has no ProviderConfigCache/OnConfig
	// push, so it takes the default statically here, exactly what the SDK would deliver a
	// dumb worker via OnConfig at startup.
	_, _, err := repo.UpsertProviderConfig(ctx, "celbom", model.Kind(celbom.Kind), 1, true, rules, nil)
	require.NoError(t, err)
	celbomDef, found, err := repo.GetDefaultProviderConfig(ctx, model.Kind(celbom.Kind), 1)
	require.NoError(t, err)
	require.True(t, found, "celbom default providerconfig must exist after upsert")

	// Registry: the celbom composer + the three sim leaves. seed() loads each kind's
	// CRD from testfixtures/kind-<kind>.json (celbom/fakevpc/fakedb/fakeapp), so the
	// rule schemas + reactions are applied exactly as the operator would.
	reg := newTReg()
	reg.Add(demoruntime.CELBOM(celbomDef.Spec))
	reg.Add(demoruntime.FakeVPC())
	reg.Add(demoruntime.FakeDB())
	reg.Add(demoruntime.FakeApp())

	eng := startEngineWithRegistry(t, ctx, pool, reg)
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// Apply the BOM (the shared deployment_instance shape): 1 FD × 2 teams → the
	// composer fans out 2 apps + 2 vpcs + 2 dbs + 4 edges (app→vpc + app→db per team).
	bom := json.RawMessage(`{
	  "deployment_instance": {
	    "name": "vf",
	    "functional_domains": [
	      { "name": "fd-00", "service_teams": [ {"name": "alpha"}, {"name": "beta"} ] }
	    ]
	  }
	}`)
	_, err = (&testEngine{Pool: pool}).CreateRoot(ctx, model.Kind(celbom.Kind), "vf-root", bom, nil)
	require.NoError(t, err)

	// Each producer derives a deterministic output from spec.account_id, which celbom
	// sets to the (fd, team) identity: fakevpc → "vpc-"+account_id, fakedb →
	// "db-"+account_id+".internal:5432". The app for fd-00/alpha depends on BOTH
	// fd-00-alpha-vpc and fd-00-alpha-db; the two edges gate it until both are Ready
	// and flow vpc_id + db_endpoint into its spec. fakeapp terminal-fails if EITHER
	// is empty, so a Ready app with BOTH status fields set is the multi-edge proof.
	wantApps := []string{"fd-00-alpha-app", "fd-00-beta-app"}
	require.Eventually(t, func() bool {
		for _, name := range wantApps {
			st, ready := readChildStatus(t, ctx, pool, "fakeapp", name)
			if !ready {
				return false
			}
			var s struct {
				WiredTo string `json:"wired_to"`
				DB      string `json:"db"`
			}
			_ = json.Unmarshal(st, &s)
			if s.WiredTo == "" || s.DB == "" {
				return false // not yet flowed (need BOTH upstreams)
			}
		}
		return true
	}, 90*time.Second, 1*time.Second, "both fakeapps should reach Ready wired to their fakevpc id AND fakedb endpoint")

	// Sanity: the upstream vpcs are Ready with a produced id too.
	for _, name := range []string{"fd-00-alpha-vpc", "fd-00-beta-vpc"} {
		st, ready := readChildStatus(t, ctx, pool, "fakevpc", name)
		require.True(t, ready, "fakevpc %s should be Ready", name)
		var s struct {
			VPCID string `json:"vpc_id"`
		}
		_ = json.Unmarshal(st, &s)
		require.NotEmpty(t, s.VPCID, "fakevpc %s should produce a vpc_id", name)
	}

	// Sanity: the upstream dbs are Ready with a produced endpoint too.
	for _, name := range []string{"fd-00-alpha-db", "fd-00-beta-db"} {
		st, ready := readChildStatus(t, ctx, pool, "fakedb", name)
		require.True(t, ready, "fakedb %s should be Ready", name)
		var s struct {
			DBEndpoint string `json:"db_endpoint"`
		}
		_ = json.Unmarshal(st, &s)
		require.NotEmpty(t, s.DBEndpoint, "fakedb %s should produce a db_endpoint", name)
	}

	t.Logf("multi-edge value flow proven: fakevpc+fakedb produced outputs, two edges flowed them, fakeapp reconciled wired to both")
}

// readChildStatus returns a composed child's status + whether it is Ready, by
// (kind, name). is_ready is the GENERATED column (synced_gen >= generation), the
// same readiness signal the other integration tests assert on.
func readChildStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, kind, name string) (json.RawMessage, bool) {
	t.Helper()
	var (
		status json.RawMessage
		ready  bool
	)
	err := pool.QueryRow(ctx, `
		SELECT r.status, r.is_ready
		FROM resources r
		JOIN resource_meta m ON m.id = r.id
		WHERE m.kind = $1 AND m.name = $2`, kind, name).Scan(&status, &ready)
	if err != nil {
		return nil, false // not composed yet
	}
	return status, ready
}
