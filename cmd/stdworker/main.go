// Command stdworker is the DEFAULT converge worker: a first-class, shipped binary
// that hosts converge's built-in general-purpose ("std") providers. Like every
// converge worker it's a thin main — declare the providers, install a signal ctx, and
// call converge.Serve (all the dial/drain/reconnect/probe machinery lives in
// sdk-go/converge). It imports ONLY sdk/* + the provider packages — never internal/*
// beyond those providers.
//
// The providers it hosts:
//
//   - stdio — the GENERIC seam: lets an external program (ANY language, no converge
//     Go SDK) act as a converge kind. Per task the worker runs the configured
//     program, pipes the task in as one JSON Request on stdin, and reads the outcome
//     as one JSON Response on stdout (the converge stdio protocol; see
//     internal/providers/stdio). Serves `stdio` (leaf) + `stdio-composer` (composer)
//     by default; STDIO_KINDS / STDIO_COMPOSER_KINDS override the names, so one fleet
//     can back many distinct named external-program kinds.
//   - stdshell — runs a user-supplied script carried INLINE in the resource spec,
//     under a configurable interpreter (bash by default), and records the exit
//     result + output as status. Serves the fixed kind `stdshell`.
//   - stdterraform — runs a real `terraform`/`tofu` apply on a user's Terraform module
//     fetched from S3, GCS, Azure Blob, HTTPS, or a git repo (state in S3/azurerm/gcs
//     with native locking), with `terraform destroy` on delete (teardown). Serves the
//     fixed kind `stdterraform`.
//   - stdcel — the GENERIC, kro-style composer: a resource graph definition expressed
//     entirely as DATA (the providerconfig spec) — a set of child resources whose
//     fields are CEL expressions over the instance's spec (${schema.spec.x}) and over
//     each other (${other.spec.x} inlines; ${other.status.x} becomes a dependency
//     edge + value flow). It INFERS the dependency order from those references. One
//     provider serves ANY composition with no code change. Serves the fixed kind
//     `stdcel`.
//   - stdstarlark — the GENERIC Starlark composer: an operator-supplied Starlark
//     PROGRAM (a zip of .star files in the providerconfig bundle, compose.star
//     defining compose(spec, config)) fans a resource's spec out into a child DAG,
//     with no per-kind Go. The program gets the instance spec + the effective config
//     as parameterization data; other .star files are loadable via load(). Sandboxed
//     (no I/O), so composition is deterministic. Serves the fixed kind `stdstarlark`.
//
// Each provider only serves a kind once the operator applies that kind's CRD +
// providerconfig; a worker that boots first simply waits for work. An operator
// scopes a fleet to specific kinds with WORKER_KINDS (converge.Serve), so one binary
// can back a narrow set of kinds. The shipped image (Dockerfile.stdworker at the repo root) bundles
// all the tooling these providers shell out to (tofu + aws + gcloud + az + git + curl
// + bash + jq).
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/salesforce/converge/internal/providers/stdcel"
	"github.com/salesforce/converge/internal/providers/stdio"
	"github.com/salesforce/converge/internal/providers/stdshell"
	"github.com/salesforce/converge/internal/providers/stdstarlark"
	"github.com/salesforce/converge/internal/providers/stdterraform"
	"github.com/salesforce/converge/sdk-go/converge"
)

// BuildVersion is stamped at build time via -ldflags "-X main.BuildVersion=…" (the
// same git-describe value the build/docker/release recipes stamp into converge and
// conctl). Without this package-level symbol the linker's -X flag silently no-ops,
// so the shipped binary would report no version — declare it and log it at boot.
var BuildVersion = "dev"

func main() {
	if err := run(); err != nil {
		log.Fatal(err) // after run's deferred signal-stop has unwound
	}
}

func run() error {
	log.Printf("stdworker %s starting", BuildVersion)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// stdio serves N named kinds (STDIO_KINDS / STDIO_COMPOSER_KINDS, else the built-in
	// defaults), so it returns one single-pair provider per name — run any program per
	// task: task JSON on stdin → outcome JSON on stdout. The fixed-kind providers append
	// onto it.
	providers := append(stdio.Providers(),
		stdshell.New(),          // run an inline script under an interpreter, record the result
		stdterraform.New(),      // apply/destroy a user's Terraform module (S3/GCS/Azure/HTTPS/git)
		&stdcel.Provider{},      // kro-style composer: a CEL resource-graph definition as data → child DAG
		&stdstarlark.Provider{}, // generic Starlark composer: a .star program (bundle) → child DAG, config-parameterized
	)
	return converge.Serve(ctx, providers)
}
