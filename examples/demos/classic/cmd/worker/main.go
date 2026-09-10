// Command worker is the CLASSIC demo's dumb worker: the classicbom composer
// (a hand-written Go composer that fans a BOM-of-teams into per-FD/per-team
// resources) + its child kinds (account, networking: vpc/route/tgw) + the
// statussink reactor.
//
// Like every converge worker it's ~15 lines: declare the providers, install a signal
// ctx, and call converge.Serve (all the dial/drain/reconnect/probe machinery lives in
// sdk-go/converge). It imports ONLY sdk/* + provider packages — never internal/*.
// See ../../justfile `demo` to run a 3-control + 3-broker cluster + this worker.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/salesforce/converge/examples/demos/classic/account"
	"github.com/salesforce/converge/examples/demos/classic/classicbom"
	"github.com/salesforce/converge/examples/demos/classic/networking"
	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/sdk-go/converge"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited", "err", err) // after run's deferred signal-stop has unwound
		os.Exit(1)
	}
}

func run() error {
	// Each provider reads its own demo env knobs (FAKE_WORK_DELAY paces every reconcile,
	// FAKE_FAULT_RATE injects transient failures, FAKE_DELETE_DELAY paces teardown) in its
	// constructor — the author's bring-up, done here, not by the SDK. networking serves
	// FOUR pairs (vpc/v1, vpc/v2, tgw, route), so New returns one single-pair provider per pair.
	net, err := networking.New()
	if err != nil {
		return fmt.Errorf("networking provider config: %w", err)
	}
	// The account leaf reads the SAME knobs (NewFromEnv), so a non-zero FAKE_WORK_DELAY
	// paces every leaf reconcile — the lever that gives a demo/test sustained in-flight
	// work (a 0ms leaf finishes before any concurrency is observable).
	acct, err := account.NewFromEnv()
	if err != nil {
		return fmt.Errorf("account provider config: %w", err)
	}
	// networking serves FOUR pairs (vpc/v1, vpc/v2, tgw, route); the composer + its leaf +
	// the reactor append onto it. The composer's only knob is its teardown pacing.
	providers := append(net,
		classicbom.NewFromEnv(), // composer: a BOM of teams → per-FD/per-team children (account + networking), rolled up
		acct,                    // leaf: the AWS account a team's resources live in (a child classicbom emits)
		&statussink.Provider{},  // reactor: (classicbom, synced → statussink) uploads the root's rolled-up status to an object store
	)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return converge.Serve(ctx, providers)
}
