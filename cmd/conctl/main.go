// Command conctl is the kubectl-style CLI for the converge control plane. It
// applies/gets/lists/deletes control-plane objects (resources, kind manifests,
// provider configs, reactor bindings) and shows the running cluster registry,
// all over the REST API. The command tree and rendering live in
// internal/conctl; this is a thin entrypoint (mirrors cmd/converge).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/salesforce/converge/internal/conctl"
)

// BuildVersion is stamped at build time via -ldflags "-X main.BuildVersion=…"
// (same convention as cmd/converge) and forwarded to the CLI's --version.
var BuildVersion = "dev"

func main() {
	// run() owns all deferred cleanup; main only maps its error to an exit code,
	// so os.Exit never skips a defer (the signal-context stop runs on the way out).
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func run() error {
	conctl.BuildVersion = BuildVersion

	// Cancel in-flight requests on Ctrl-C / SIGTERM so the CLI exits promptly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return conctl.NewRootCommand().ExecuteContext(ctx)
}
