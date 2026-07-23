package test

// Real-BOM run with random per-call latency and random transient
// failures, over a single-pod engine, with the UI exposed on :18080
// so you can watch progress in a browser.
//
// Submits testfixtures/resource-classicbom.json (the natural
// ~5,400-resource shape so wall time fits a reasonable demo).
// Each Worker (account / vpc / tgw / route) is wrapped to:
//
//   - sleep a random duration in [3s, 30s] on every React call,
//   - fail React with probability flakyFailRate (default 0.20).
//
// A failed Work emits WorkSucceeded=False with reason+message; the
// reaper's RetryAfter is set to 30s, so the row waits 30s before
// being re-pended. End condition: every composed child becomes ready
// (is_ready=true).
//
// Run via:
//   go test -count=1 -timeout 90m ./test/ -run TestRealBOMFlaky -v -args -flaky
//
// Run with -flaky-headless to skip the HTTP UI (purely for CI).

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/fault"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/internal/api"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/test/internal/demoruntime"
)

var (
	flakyEnabled  = flag.Bool("flaky", false, "Run the flaky real-BOM test (default: skip)")
	flakyHeadless = flag.Bool("flaky-headless", false, "Skip the :18080 UI server")
	flakyFailRate = flag.Float64("flaky-rate", 0.20, "Probability that a single Worker call fails")
	flakyMinDelay = flag.Duration("flaky-min-delay", 3*time.Second, "Min per-call worker delay")
	flakyMaxDelay = flag.Duration("flaky-max-delay", 30*time.Second, "Max per-call worker delay")
	flakyRetry    = flag.Duration("flaky-retry", 30*time.Second, "Retry delay applied to failed rows")
	flakyPort     = flag.Int("flaky-port", 18080, "HTTP port for the UI")
	flakyFast     = flag.Bool("flaky-fast", false, "Fast mode: tiny per-call delays + 1s retry so the run finishes in minutes, for reproducing the rollup-latch gap")
)

func TestRealBOMFlaky(t *testing.T) {
	if !*flakyEnabled {
		t.Skip("flaky BOM test disabled; run with -args -flaky")
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	// Fast mode collapses the [3s..30s] provider latency to [200ms..800ms]
	// and the retry window to 1s, so the whole flaky run finishes in a few
	// minutes instead of ~an hour. It doesn't change WHAT the test exercises
	// (random transient failures + retries + rollup re-runs), only how long
	// each pass takes — which is exactly what's needed to reproduce the
	// root rollup-latch gap quickly.
	minDelay, maxDelay, retry := *flakyMinDelay, *flakyMaxDelay, *flakyRetry
	if *flakyFast {
		minDelay, maxDelay, retry = 200*time.Millisecond, 800*time.Millisecond, 1*time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// Wrap each production Worker with the flaky decorator. ClassicBOM is
	// a Composer, not a Worker, so it stays wrapped-free — Composer
	// errors are terminal anyway, the reaper's transient retry path
	// only applies to Worker results.
	reg := newTReg()
	reg.Add(demoruntime.ClassicBOM(0))

	accountRT, accountWraps := flakeRuntime(demoruntime.Account(0, 0, fault.Injector{}), minDelay, maxDelay, *flakyFailRate, 11)
	reg.Add(accountRT)

	var netWraps []*flakyWrap
	for i, c := range networking.AllRuntimes(0, fault.Injector{}) {
		rt, wraps := flakeRuntime(c, minDelay, maxDelay, *flakyFailRate, 22+int64(i)*100)
		reg.Add(rt)
		netWraps = append(netWraps, wraps...)
	}

	// Heartbeat must be < the longest Work call so a slow-but-alive
	// worker isn't reaped as stale; bump SweeperStaleAfter accordingly.
	//
	// WorkerMaxParallel=10: with random Work latencies up to 30s, the
	// continuous-fill dispatcher keeps up to 10 tasks in flight and
	// refills each slot the moment its task finishes — a slow task holds
	// only its own slot, so fast siblings are never blocked behind it.
	eng := startEngineWithConfig(t, ctx, pool, reg, engineOpts{
		HeartbeatEvery:    2 * time.Second,
		SweeperStaleAfter: maxDelay + 2*time.Minute,
		RetryAfter:        retry,
		WorkerMaxParallel: 10,
	})
	require.NoError(t, eng.Start(ctx))
	defer func() { _ = eng.Stop(ctx) }()

	// Optional: HTTP server on :18080 so the dashboard renders the run.
	if !*flakyHeadless {
		srv := api.NewServer(api.DepsFromPools(pool, nil))
		srv.SetDeclaredSchemas(declaredManifests(eng.Registry))
		hs := &http.Server{Addr: fmt.Sprintf(":%d", *flakyPort), Handler: srv.Handler()}
		go func() {
			if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("flaky-bom http server", "error", err)
			}
		}()
		t.Cleanup(func() { _ = hs.Shutdown(context.Background()) })
		t.Logf("UI: http://localhost:%d/", *flakyPort)
	}

	// Real BOM, unmodified shape.
	bom := loadRealBOM(t)
	require.NotNil(t, bom.DeploymentInstance)

	totalTeams := 0
	tgwFDs := 0
	numFDs := len(bom.DeploymentInstance.FunctionalDomains)
	for _, fd := range bom.DeploymentInstance.FunctionalDomains {
		if fd == nil {
			continue
		}
		totalTeams += len(fd.ServiceTeams)
		if fd.AWSTransitGateway != nil && fd.AWSTransitGateway.Enabled && !fd.AccountsOnly {
			tgwFDs++
		}
	}
	expected := tgwFDs*2 + totalTeams*3

	specJSON, err := json.Marshal(bom)
	require.NoError(t, err)

	t.Logf("=== flaky-bom: %d FDs, %d teams, %d expected resources ===", numFDs, totalTeams, expected)
	t.Logf("    delay=[%v..%v]  failRate=%.2f  retryAfter=%v  fast=%v",
		minDelay, maxDelay, *flakyFailRate, retry, *flakyFast)

	start := time.Now()
	rootID, err := eng.CreateRoot(
		ctx,
		model.Kind(classicbom.Kind),
		bom.DeploymentInstance.Name+"-flaky-demo",
		specJSON,
		nil,
	)
	require.NoError(t, err)
	t.Logf("root id: %s — submitted at t=%s", rootID, start.Format("15:04:05.000"))

	// Lots of margin: avg call ~16.5s, plus a 30s retry per failed
	// call, plus heartbeat overhead. 60 min is generous; CI can tune
	// down with the flag.
	waitForTotalReady(t, ctx, pool, expected, 60*time.Minute)
	elapsed := time.Since(start)

	accountCalls, accountFails := sumFlaky(accountWraps)
	netCalls, netFails := sumFlaky(netWraps)
	t.Logf("=== DONE: %d resources ready in %v ===", expected, elapsed.Round(time.Millisecond))
	t.Logf("=== work calls: account=%d (failed=%d)  networking=%d (failed=%d) ===",
		accountCalls, accountFails, netCalls, netFails)

	// Sanity: at least one transient failure must have occurred at the
	// configured rate, otherwise the test isn't actually exercising
	// the retry path.
	if *flakyFailRate > 0 {
		require.Greater(t, accountFails+netFails, int64(0),
			"expected at least one simulated failure at rate %.2f", *flakyFailRate)
	}

	// ROLLUP RE-PROMOTION GATE: every child is now Ready+healthy, so the
	// root's rolled-up health must ALSO recover to Ready. This exercises
	// the reactive re-promotion path in cascade_on_ready_change: when the
	// LAST unhealthy child heals, its already-settled composite root
	// (generation == synced_gen) must be re-pended to re-run its rollup and
	// fold the recovered subtree health back into its own health_ok.
	//
	// Without that path the root latches Degraded on a stale rollup snapshot
	// (a team stuck ready:false) until classicbom's 3600s drift resync — so a
	// deadline WELL under an hour proves the reactive path, not the resync
	// backstop. 90s is generous for the reactive re-pend + one rollup pass.
	waitForRootReady(t, ctx, pool, rootID, 90*time.Second)
	t.Logf("=== root %s rolled up to Ready ===", rootID)

	// NO WASTEFUL FILL-PHASE ROLLUPS: the reactive re-promotion re-pend (the
	// cascade_on_ready_change block that re-runs a settled root's rollup when a
	// descendant heals) must NOT amplify per-child-flake. The root emits a
	// rollup-succeeded event ONLY when descendantsSettled is true (every child
	// synced/failed/frozen), so while ANY child is still purely lagging the root
	// physically cannot roll up — the "other children not ready at all" case
	// costs zero root rollups by construction. The only real root rollups are
	// the initial converge-to-Degraded plus one per NET health transition as
	// failed children heal — a small handful, uncorrelated with the hundreds of
	// child flake-failures. If the re-pend amplified per-heal, this count would
	// track childFails. Empirically ~13 vs ~880 fails; assert it stays a tiny
	// fraction (and well under 200) to lock the invariant without being brittle.
	var rootRollups, childFails int64
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM resource_events WHERE resource_id = $1 AND type = 'rollup-succeeded'`,
		rootID).Scan(&rootRollups))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM resource_events e JOIN resources r ON r.id = e.resource_id
		   WHERE r.root_id = $1 AND e.type = 'work-failed'`,
		rootID).Scan(&childFails))
	t.Logf("=== root rollups: %d  (child work-failed events: %d) ===", rootRollups, childFails)
	require.Less(t, rootRollups, int64(200),
		"root rolled up %d times — the reactive re-promotion re-pend is amplifying per child flake "+
			"(should be a small handful of net health transitions, not tracking the %d child failures)",
		rootRollups, childFails)
}

// waitForRootReady blocks until the given root resource is is_ready (its
// rollup has folded all descendants' health into health_ok AND it is
// synced), or fails the test at the deadline — dumping the root's live
// health/synced axes and, when latched Degraded, the child count so the
// failure is self-diagnosing.
func waitForRootReady(t *testing.T, ctx context.Context, pool *pgxpool.Pool, rootID uuid.UUID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled while waiting for root %s ready", rootID)
		default:
		}
		var ready, healthOK bool
		var gen, syncedGen int64
		err := pool.QueryRow(ctx,
			`SELECT is_ready, health_ok, generation, synced_gen FROM resources WHERE id = $1`,
			rootID,
		).Scan(&ready, &healthOK, &gen, &syncedGen)
		if err == nil && ready {
			return
		}
		if time.Now().After(deadline) {
			var unreadyChildren int64
			_ = pool.QueryRow(ctx,
				`SELECT count(*) FROM resources WHERE root_id = $1 AND owner_id IS NOT NULL AND NOT is_ready`,
				rootID,
			).Scan(&unreadyChildren)
			t.Fatalf("root %s never rolled up to Ready within %v "+
				"(is_ready=%v health_ok=%v generation=%d synced_gen=%d, unready children=%d) — "+
				"the root latched a stale Degraded rollup; the descendant re-promotion cascade "+
				"failed to re-pend the settled root's rollup",
				rootID, timeout, ready, healthOK, gen, syncedGen, unreadyChildren)
		}
		time.Sleep(1 * time.Second)
	}
}

// _ keeps the pgxpool import alive when this is the only file using
// it via the helpers. (setupPostgres handles the actual usage.)
var _ = (*pgxpool.Pool)(nil)
