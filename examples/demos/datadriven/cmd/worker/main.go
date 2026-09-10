// Command worker is the DATA-DRIVEN demo's worker: like every converge worker it declares
// its providers, installs a signal ctx, and calls converge.Serve (which reads config from
// the env and owns dialing, reconnect, config, readiness, and drain).
//
// It hosts the THREE "logic as data" composers (celbom, stdcel, stdstarlark) + the
// dependency/value-flow simulation leaves — fakeapp depends on BOTH fakevpc (flows vpc_id)
// and fakedb (flows db_endpoint). Each is an ordinary converge.Provider.
//
// The READINESS demo: launch with FAKEDB_UNHEALTHY=1 to make this worker report the
// `fakedb` kind UNREADY (its "database" is unreachable) while every other kind stays
// healthy. The broker then stops claiming + pushing `fakedb` work here — its resources
// park (and, with no other healthy fakedb worker, stay queued) — while the rest of the
// DAG reconciles. Launch without it and fakedb flows. A real provider would poll live
// connectivity; the env seed just makes it reproducible (fakedb.Provider.Unhealthy). See
// ../../justfile `demo` / `demo-unhealthy`.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/salesforce/converge/examples/demos/datadriven/celbom"
	"github.com/salesforce/converge/examples/demos/datadriven/fakeapp"
	"github.com/salesforce/converge/examples/demos/datadriven/fakedb"
	"github.com/salesforce/converge/examples/demos/datadriven/fakek8sjob"
	"github.com/salesforce/converge/examples/demos/datadriven/faketerraform"
	"github.com/salesforce/converge/examples/demos/datadriven/fakevpc"
	"github.com/salesforce/converge/internal/providers/stdcel"
	"github.com/salesforce/converge/internal/providers/stdstarlark"
	"github.com/salesforce/converge/sdk-go/converge"
)

func main() {
	// os.Exit lives here, after run's deferred signal-stop unwinds (gocritic exitAfterDefer).
	if err := run(); err != nil {
		slog.Error("datadriven worker exited", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// FAKEDB_UNHEALTHY=1 → fakedb advertises UNREADY (broker parks fakedb work here); unset
	// → fakedb flows. A real provider would poll live connectivity; the env seed makes the
	// readiness demo reproducible.
	dbUnhealthy := os.Getenv("FAKEDB_UNHEALTHY") != ""
	if dbUnhealthy {
		slog.Warn("FAKEDB_UNHEALTHY set — advertising fakedb as UNREADY (broker will not push fakedb work here)")
	}

	return converge.Serve(ctx, []converge.Provider{
		&celbom.Provider{},                      // composer: DECLARATIVE RULES (CEL `when` predicates + templates in the providerconfig)
		&stdcel.Provider{},                      // composer: the GENERIC kro-style CEL resource graph (deps inferred from ${x.status.y})
		&stdstarlark.Provider{},                 // composer: the GENERIC Starlark program (multi-file .star bundle)
		fakevpc.Provider{},                      // sim leaf: PRODUCES vpc_id
		fakeapp.Provider{},                      // sim leaf: CONSUMES vpc_id + db_endpoint
		faketerraform.Provider{},                // sim leaf: PRODUCES image_tag
		fakek8sjob.Provider{},                   // sim leaf: CONSUMES image_tag
		fakedb.Provider{Unhealthy: dbUnhealthy}, // sim leaf: PRODUCES db_endpoint (readiness knob)
	})
}
