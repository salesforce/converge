// Package archtest holds architecture-invariant guard tests. It has no
// production code; it exists so the build fails the moment a layering rule
// is violated, instead of the violation being discovered only when the
// public SDK is published or the providers module is split out.
package archtest

import (
	"os/exec"
	"strings"
	"testing"
)

// internalPrefix is the import-path prefix for this module's private code.
// Nothing under sdk/ (the public SDK) or examples/demos/ (the open-source example
// providers) is allowed to depend on it.
const internalPrefix = "github.com/salesforce/converge/internal/"

// providersPrefix is the import-path prefix for the example provider plugins (the
// open-source demos under examples/demos/*). The converge ENGINE core must never
// depend on it: the runtime is kind-blind and learns everything from applied
// KindManifests, never from compiled-in provider code. (The private company
// providers were removed from this open-source tree; the demos are the reference
// plugins now.)
const providersPrefix = "github.com/salesforce/converge/examples/demos/"

// corePackages are the engine packages that must stay provider-free. cmd/converge
// is DELIBERATELY excluded: it is the reference binary, and the example workers
// under examples/demos/*/cmd/worker bundle the demo providers for the in-process /
// boot-seed paths, exactly as a Terraform build's main imports the providers it
// ships — that is wiring, not the engine.
var corePackages = []string{
	"github.com/salesforce/converge/internal/runtime/...",
	"github.com/salesforce/converge/internal/broker/...",
	"github.com/salesforce/converge/internal/engine/...",
	"github.com/salesforce/converge/internal/store/...",
	"github.com/salesforce/converge/internal/api/...",
}

// deps runs `go list` with the given args and returns the whitespace-split
// output lines. Callers pass either `-deps <pkg>` (the transitive dep set) or
// `-f {{.ImportPath}} <pattern>` (the matched package list). Shelling out to the
// Go toolchain keeps this test dependency-free — no golang.org/x/tools in go.mod.
func deps(t *testing.T, args ...string) []string {
	t.Helper()
	out, err := exec.Command("go", append([]string{"list"}, args...)...).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list %v failed: %v\n%s", args, err, ee.Stderr)
		}
		t.Fatalf("go list %v failed: %v", args, err)
	}
	return strings.Fields(string(out))
}

// TestSDKHasNoInternalDeps is the load-bearing invariant: the public SDK must
// be importable by an outside party without dragging in any private package.
// If this fails, someone imported internal/* into sdk/* — move the shared code
// into the SDK or keep it host-side instead.
func TestSDKHasNoInternalDeps(t *testing.T) {
	for _, d := range deps(t, "-deps", "github.com/salesforce/converge/sdk-go/...") {
		if strings.HasPrefix(d, internalPrefix) {
			t.Errorf("sdk/ must not import internal/: found dependency on %s", d)
		}
	}
}

// meshProtoPrefix is the import path of the INTERNAL broker↔broker mesh proto
// (converge.mesh.v1, generated into internal/meshpb). The ONLY tissue between a
// worker SDK — in ANY language — and a broker is the PUBLIC worker proto
// (converge.worker.v1 → sdk-go/workerpb); the mesh contract is a broker-private
// implementation detail that must be free to evolve without breaking N language
// SDKs. It lives under internal/ so TestSDKHasNoInternalDeps already forbids the
// SDK from reaching it, but this names the invariant explicitly (and still guards
// it if the mesh proto were ever moved out of internal/).
const meshProtoPrefix = "github.com/salesforce/converge/internal/meshpb"

// TestSDKSpeaksOnlyWorkerProto locks the proto contract boundary: the worker SDK
// depends on the PUBLIC worker proto (sdk-go/workerpb) and NEVER on the internal mesh
// proto. A violation means the two proto surfaces were re-entangled, so a mesh
// change would churn the public worker contract — the exact coupling the
// worker/mesh proto split exists to prevent.
func TestSDKSpeaksOnlyWorkerProto(t *testing.T) {
	for _, d := range deps(t, "-deps", "github.com/salesforce/converge/sdk-go/...") {
		if strings.HasPrefix(d, meshProtoPrefix) {
			t.Errorf("sdk/ must not import the internal mesh proto (%s): the only worker↔broker tissue is sdk-go/workerpb", d)
		}
	}
}

// coreSharedPkgs are the internal/* packages the CORE authors as ITS contract — the
// kind-identity + reaction model types (internal/model) and the proto⇄model wire
// converter (internal/wire). The worker SDK must NOT import ANY of them: the ONLY thing an
// SDK (in any language) shares with the broker is the generated worker proto, and every SDK
// owns its OWN ergonomic types + converter (sdk-go has converge.Resource/Outcome/… +
// convert.go, exactly as sdk-ts has types.ts + convert.ts). The TypeScript-SDK test settles
// it: a TS SDK would never import these Go packages, so neither may the Go SDK — otherwise
// the two bindings are not peers and the core can't evolve its types without churning the SDK.
var coreSharedPkgs = []string{
	"github.com/salesforce/converge/internal/model",
	"github.com/salesforce/converge/internal/wire",
}

// TestSDKSharesNoCoreTypes is the load-bearing tissue invariant: the worker SDK shares NO
// co-authored Go package with the control-plane core — only the generated proto + the
// generic leaf utils (tlsreload/wirelimits/drainctx). If this fails, someone reached back
// into the core's model/wire from sdk-go/, re-coupling the two type worlds the proto
// boundary exists to keep apart. Fix by giving the SDK its own copy (types.go /
// convert.go), never by importing the core's.
func TestSDKSharesNoCoreTypes(t *testing.T) {
	for _, d := range deps(t, "-deps", "github.com/salesforce/converge/sdk-go/...") {
		for _, forbidden := range coreSharedPkgs {
			if d == forbidden {
				t.Errorf("sdk-go/ must not import the core-shared package %s: the SDK owns its own types/converter; the only worker↔broker tissue is the proto (see docs/spec/sdk-spec.md)", d)
			}
		}
	}
}

// TestControllersHaveNoInternalDeps pre-validates the eventual module split:
// the example providers must depend only on the public SDK (plus stdlib and
// third-party deps), never on internal/*. A violation here would block giving
// the providers their own go.mod, and would mean a third party could not author
// a replacement kind against the SDK alone. Scoped to the provider PACKAGES
// (examples/demos/*/{account,celbom,...}); the demo cmd/worker mains and tests are
// wiring and may reach wherever they like.
//
// It checks the deps of each provider package INDIVIDUALLY (excluding the demo
// cmd/worker mains) rather than the deps of the whole examples/demos/... tree: a
// demo worker main legitimately bundles the internal std* providers (as a Terraform
// build's main imports the providers it ships), and a tree-wide `go list -deps`
// would flatten those in and false-fail. The invariant is per-PACKAGE — a provider
// package's own transitive deps — so we enumerate the packages and skip the mains.
func TestControllersHaveNoInternalDeps(t *testing.T) {
	for _, pkg := range deps(t, "-f", "{{.ImportPath}}", "github.com/salesforce/converge/examples/demos/...") {
		// The example workers (examples/demos/*/cmd/worker) are wiring binaries, not
		// provider packages — skip the demo module's own cmd/ mains.
		if strings.Contains(pkg, "/cmd/") {
			continue
		}
		for _, d := range deps(t, "-deps", pkg) {
			if strings.HasPrefix(d, internalPrefix) {
				t.Errorf("examples/demos provider %s must not import internal/: found dependency on %s", pkg, d)
			}
		}
	}
}

// TestCoreHasNoProviderDeps is a load-bearing invariant: the engine core is
// KIND-BLIND. It runs work by reading applied KindManifests + dispatching over the
// StageDispatcher seam (in-process registry OR remote Connect) — never by importing a
// provider. If this fails, someone reached back into providers/* from the
// runtime/broker/engine/store/api layer, which would mean a kind's behavior is
// compiled into core instead of being pure manifest data.
func TestCoreHasNoProviderDeps(t *testing.T) {
	for _, pkg := range corePackages {
		for _, d := range deps(t, "-deps", pkg) {
			if strings.HasPrefix(d, providersPrefix) {
				t.Errorf("%s must not import providers/* (core is kind-blind): found dependency on %s", pkg, d)
			}
		}
	}
}
