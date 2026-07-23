package host

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/salesforce/converge/internal/model"
)

// LoadKindFixtures reads every *.kind.json CRD manifest fixture under the given
// dir(s) and returns them as KindManifests, sorted by kind for a stable apply
// order. These are the canonical "CRDs" that TESTS and DEMOS apply to
// kind_manifest (a demo posts them via conctl / PUT /api/kinds/{kind}/manifest;
// a test seeds them through the store). The SERVER never reads fixtures — a
// fresh cluster comes up empty and an operator applies CRDs over the API. A
// malformed fixture is a hard error (a broken CRD must not be silently skipped).
//
// Variadic over dirs so a caller can compose the core testfixtures/ with one or
// more example-demo fixture dirs (examples/demos/*/testfixtures/) — each demo
// ships its own CRDs. A single dir (the common case) is unchanged.
//
// Each dir is an INDEPENDENT CRD source: a demo is its own cluster (`just demo`
// stands up a fresh, empty control plane), so two different demos legitimately
// defining the same (kind, kindVersion) — e.g. the classic Go demo and the
// TypeScript demo each shipping account/v1 — is NOT a conflict; they never share
// a namespace at runtime. When a caller merges several dirs (to build a validator
// or a seed lookup), a (kind, kindVersion) seen in more than one dir is kept from
// the FIRST dir in the argument order and the later copies are skipped — the
// dirs are passed in a stable order (sorted), so the winner is deterministic.
// A duplicate WITHIN a single dir is still a hard error: two files in one CRD
// source claiming the same (kind, kindVersion) is a genuine ambiguity.
func LoadKindFixtures(dirs ...string) ([]model.KindManifest, error) {
	out := make([]model.KindManifest, 0)
	// A CRD is per-(kind, kindVersion): vpc/v1 and vpc/v2 are two DISTINCT fixtures for
	// the bare kind "vpc". Dedup by (kind, kindVersion), NOT kind.
	type km struct {
		kind        model.Kind
		kindVersion int
	}
	// seenAcrossDirs: (kind, kindVersion) → the dir that first defined it, so a later
	// dir's copy is skipped (first-dir-wins, deterministic in the sorted dir order).
	seenAcrossDirs := make(map[km]string)
	for _, dir := range dirs {
		matches, err := filepath.Glob(filepath.Join(dir, "*.kind.json"))
		if err != nil {
			return nil, fmt.Errorf("glob kind fixtures in %s: %w", dir, err)
		}
		// seenInDir catches the real error: two files in the SAME dir for one pair.
		seenInDir := make(map[km]string)
		for _, f := range matches {
			b, err := os.ReadFile(f)
			if err != nil {
				return nil, fmt.Errorf("read kind fixture %s: %w", f, err)
			}
			var m model.KindManifest
			if err := json.Unmarshal(b, &m); err != nil {
				return nil, fmt.Errorf("decode kind fixture %s: %w", f, err)
			}
			if m.Kind == "" {
				return nil, fmt.Errorf("kind fixture %s has empty kind", f)
			}
			// kind_version is REQUIRED and explicit (>= 1) in every CRD fixture — there
			// is no implicit v1 default. A fixture that omits it is a broken CRD (a hard
			// error, never silently v1).
			if m.KindVersion < 1 {
				return nil, fmt.Errorf("kind fixture %s (%q) has no kind_version (must be >= 1): every CRD must set an explicit kind_version", f, m.Kind)
			}
			key := km{kind: m.Kind, kindVersion: m.KindVersion}
			if prev, dup := seenInDir[key]; dup {
				return nil, fmt.Errorf("duplicate CRD for %q/v%d within %s: %s and %s", m.Kind, m.KindVersion, dir, prev, f)
			}
			seenInDir[key] = f
			// A different dir already published this pair — a separate demo's own copy.
			// Skip it (first dir wins) rather than error: independent clusters may share
			// a (kind, kindVersion).
			if _, dupAcross := seenAcrossDirs[key]; dupAcross {
				continue
			}
			seenAcrossDirs[key] = dir
			out = append(out, m)
		}
	}
	// Stable order: by (kind, kindVersion) ascending.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].KindVersion < out[j].KindVersion
	})
	return out, nil
}
