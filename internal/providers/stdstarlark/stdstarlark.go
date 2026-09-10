// Package stdstarlark is the GENERIC Starlark composer: it runs an operator-supplied
// Starlark program — shipped as a zip of .star files in the providerconfig BUNDLE —
// that fans a resource's spec out into a child DAG (children + edges + provider
// configs), with no per-kind Go. One provider serves ANY composition.
//
// The composition contract is a single, generic entrypoint — the program owns its own
// conventions (how it splits across files, whether it reads policies from config or
// from other .star modules), so the provider stays kind-agnostic:
//
//		compose(spec, config) → struct(children=[…], edges=[…], configs=[…])
//
//	  - spec   = the resource's OWN spec (the instance inputs), a Starlark value.
//	  - config = the EFFECTIVE providerconfig spec (kind default ⊕ per-resource
//	    override) — parameterization DATA the program reads, so behaviour can change
//	    with a config edit and no bundle re-upload.
//	  - the BUNDLE (default ⊕ override, a whole-artifact replace) is the PROGRAM: a zip
//	    with compose.star at the root (the entrypoint) plus any number of other .star
//	    files usable as LOADABLE MODULES via Starlark's load("name.star", "sym").
//
// Return: a struct whose optional children/edges/configs lists the host marshals into
// the typed converge.Outcome — the same shape a Go composer emits, diffed by the
// core (ApplyComposeResult) with the usual recompose / prune / fence / value-flows.
//
// Why Starlark fits the worker's pure, no-I/O contract: a Starlark thread has NO
// filesystem/network/clock unless the host grants a builtin (this package grants only
// pure ones: struct + a glob match()), and the zip is unpacked in memory, so
// composition is a deterministic pure function of (spec, config, bundle bytes).
//
// It imports only sdk/* + go.starlark.net — never internal/* — exactly what an
// external team ships. It serves the single fixed kind "stdstarlark".
package stdstarlark

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	"go.starlark.net/syntax"

	"github.com/salesforce/converge/sdk-go/converge"
)

// KindVersion is the single web-API version this provider serves (no implicit default,
// no minor). It mirrors the version in stdstarlark.kind.json — keep the two in sync.
const KindVersion = 1

// reactionCompose is the single reaction "stdstarlark" declares: a spec/config/bundle
// change fans the resource out into a child DAG. Named so a missed dispatch can't
// silently compile.
const reactionCompose = "compose"

// fileOpts pins the Starlark dialect explicitly (the non-deprecated, no-global form).
// All features default-off keeps the language minimal + deterministic — no recursion,
// no builtins beyond what hostBuiltins grants.
var fileOpts = &syntax.FileOptions{}

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). It is the GENERIC Starlark composer,
// parameterised by the kind's LIVE default BUNDLE (the .star program) + default CONFIG
// (parameterization data), both delivered via OnConfig (the SDK delivers the default at
// startup + on every edit), so a compose reads the latest an operator has applied with no
// restart.
//
// Imports only sdk/* + go.starlark.net — never internal/* — exactly what an external
// team ships.
type Provider struct {
	// mu guards the fields below (OnConfig writes on the broker goroutine, Work reads per
	// task).
	mu sync.RWMutex
	// defaultSpec / defaultData are the config OnConfig last delivered (the parameterization
	// document + the opaque .star bundle bytes); nil until the first delivery, and nil again
	// when the default is deleted.
	defaultSpec json.RawMessage
	defaultData []byte
}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = (*Provider)(nil)

// Kind is the single (kind, version) this provider serves — the fixed composer kind
// "stdstarlark" at its one web-API version. PURE per the contract: no env, no dial.
func (*Provider) Kind() converge.KindVersion {
	return converge.KindVersion{Kind: Kind, Version: KindVersion}
}

// Work runs the single "compose" reaction: it reads the effective .star bundle + config
// (the OnConfig-pushed values when present, else the live default sources) and fans the
// resource's spec out into a child DAG. The reaction dispatch is explicit so a future
// second reaction on this kind is a switch arm, not a silent fall-through.
func (p *Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	switch req.Reaction {
	case reactionCompose:
		return p.composerForTask().React(ctx, req)
	default:
		// Fail closed: an unknown reaction must not silently no-op (which the core would
		// read as "composed to zero children" → prune everything).
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdstarlark: unknown reaction %q", req.Reaction))
	}
}

// OnConfig records the default providerconfig the broker delivers for this pair — both
// the parameterization Spec and the opaque .star bundle Data. An empty cfg (nil Spec + nil
// Data) = the default was deleted; recorded as such so Work observes the removal (a
// compose then transient-retries "no program yet" until a bundle is re-applied, rather
// than reusing a stale one).
func (p *Provider) OnConfig(cfg converge.ProviderConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.defaultSpec = cfg.Spec
	p.defaultData = cfg.Data
}

// Ready is always true: the .star program + config are compose-time inputs validated per
// task (a missing bundle transient-retries), so the composer has no downstream to dial
// and no boot prerequisite to gate on.
func (*Provider) Ready() bool { return true }

// composerForTask builds the per-task composer from the provider's current default bundle
// + config (from OnConfig). Read under the lock so a concurrent OnConfig can't tear a
// spec/data pair.
func (p *Provider) composerForTask() composer {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return composer{defaultData: p.defaultData, defaultSpec: p.defaultSpec}
}

// composer holds the effective bundle + config bytes for one compose. Stateless per task —
// Provider.composerForTask builds one from its current default/pushed values, so the pure
// compose body stays a plain (spec, config, bundle) → Outcome function testable in
// isolation.
type composer struct {
	// defaultData is the kind default .star bundle; the per-resource bundle override
	// whole-replaces it. nil-safe.
	defaultData []byte
	// defaultSpec is the kind default config (parameterization data); the per-resource
	// override merges over it. nil-safe.
	defaultSpec json.RawMessage
}

func (c composer) React(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	// 1. The instance inputs are the resource's own spec — passed to compose() as its
	//    first argument (a Starlark value). A resource with no spec is fine (None).
	var spec any
	if len(req.Resource.Spec) > 0 {
		if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
			return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdstarlark: decode instance spec: %w", err))
		}
	}

	// 2. The parameterization config is the effective providerconfig spec
	//    (default ⊕ per-resource override) — compose()'s second argument.
	cfg := converge.EffectiveConfig[any](c.defaultSpec, req.Env.ProviderConfig)

	// 3. The effective BUNDLE is the per-resource CUSTOM override (Env.ProviderBundle)
	//    if set, else the kind DEFAULT — a whole-artifact REPLACE (opaque bytes can't
	//    merge).
	zipBytes := converge.EffectiveBundle(c.defaultData, req.Env.ProviderBundle)
	if len(zipBytes) == 0 {
		// TRANSIENT, not terminal: the bundle is applied out-of-band (PUT the default
		// providerconfig) and reaches the worker via BundleSource on its next pull/push.
		// A compose that fires BEFORE the bundle propagates (a fresh-cluster startup
		// race) must RETRY, not fail permanently — otherwise the root sticks in Failed
		// even after the bundle arrives. (Contrast a PRESENT-but-malformed bundle below,
		// which IS terminal — a re-run won't fix bad bytes.)
		return converge.Outcome{}, fmt.Errorf("stdstarlark: no program yet (apply the stdstarlark providerconfig bundle); will retry")
	}

	modules, err := unpackBundle(zipBytes)
	if err != nil {
		// A malformed bundle (bad zip / no compose.star / oversize) is terminal: a
		// re-run won't fix it without a new upload.
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdstarlark: bundle: %w", err))
	}

	out, err := compose(modules, spec, cfg)
	if err != nil {
		// A Starlark error (bad program) OR a bad return shape is terminal: same-
		// generation re-run of the same bundle won't fix it — it needs a program/config
		// edit (which is re-delivered live). Return an EMPTY Outcome via the error path
		// only (never a partial Children set, which ApplyComposeResult treats as "prune
		// everything"), so the last good DAG survives until the program is corrected.
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdstarlark: compose: %w", err))
	}
	return out, nil
}

// unpackBundle reads the .star zip in memory (no disk) into a name→source map, bounded
// against a zip-bomb (total + per-entry caps) and restricted to *.star entries, so the
// bundle stays a deterministic, I/O-free input. compose.star at the ROOT must exist
// (the entrypoint); every other .star is a loadable module.
func unpackBundle(zipBytes []byte) (map[string]string, error) {
	zr, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	modules := make(map[string]string, len(zr.File))
	var total int64
	for _, f := range zr.File {
		name := cleanEntry(f.Name)
		if f.FileInfo().IsDir() || !strings.HasSuffix(name, ".star") {
			continue // ignore dirs + non-.star (READMEs, etc.)
		}
		if _, dup := modules[name]; dup {
			// A duplicate entry name is ambiguous (which content wins?) — reject rather
			// than silently last-write-win, so the bundle stays a deterministic input.
			return nil, fmt.Errorf("duplicate entry %q", name)
		}
		src, err := readStar(f, &total)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		modules[name] = src
	}
	if _, ok := modules[entrypoint]; !ok {
		return nil, fmt.Errorf("no %s entry at the bundle root", entrypoint)
	}
	return modules, nil
}

// cleanEntry normalises a zip entry name to a slash-clean relative path so a leading
// "./" or redundant separators don't change matching, and a "../" can't escape
// (path.Clean collapses it; the bundle is unpacked in memory and never written to
// disk, so traversal can't touch the filesystem regardless).
func cleanEntry(name string) string {
	return strings.TrimPrefix(path.Clean(name), "/")
}

// readStar reads one .star entry with a per-entry cap and a running total cap
// (zip-bomb defence), returning its source.
func readStar(f *zip.File, total *int64) (string, error) {
	rc, err := f.Open()
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	buf, err := io.ReadAll(io.LimitReader(rc, maxStarBytes+1))
	if err != nil {
		return "", err
	}
	if int64(len(buf)) > maxStarBytes {
		return "", fmt.Errorf("entry exceeds %d bytes", maxStarBytes)
	}
	*total += int64(len(buf))
	if *total > maxBundleBytes {
		return "", fmt.Errorf("bundle exceeds %d bytes", maxBundleBytes)
	}
	return string(buf), nil
}

// compose execs the entrypoint module and calls compose(spec, config), marshalling its
// returned struct{children,edges,configs} into the typed Outcome. Other .star files in
// the bundle are available via load() (resolved from the in-memory module map — no disk
// or network access).
func compose(modules map[string]string, spec, cfg any) (converge.Outcome, error) {
	loader := newModuleLoader(modules)
	thread := &starlark.Thread{Name: "stdstarlark", Load: loader.load}

	g, err := starlark.ExecFileOptions(fileOpts, thread, entrypoint, modules[entrypoint], hostBuiltins())
	if err != nil {
		return converge.Outcome{}, fmt.Errorf("%s exec: %w", entrypoint, err)
	}
	composeFn, ok := g["compose"].(starlark.Callable)
	if !ok {
		return converge.Outcome{}, fmt.Errorf("%s: no compose(spec, config) function", entrypoint)
	}
	result, err := starlark.Call(thread, composeFn, starlark.Tuple{goToStarlark(spec), goToStarlark(cfg)}, nil)
	if err != nil {
		return converge.Outcome{}, fmt.Errorf("compose() call: %w", err)
	}
	return outcomeFromStarlark(result)
}

// moduleLoader resolves load("name.star", …) against the in-memory bundle modules,
// exec'ing each on first use and caching its globals — the standard Starlark loader
// pattern, with cycle detection and no filesystem access. Its per-compose so no state
// leaks between tasks.
type moduleLoader struct {
	sources map[string]string
	mu      sync.Mutex
	cache   map[string]*moduleEntry
}

type moduleEntry struct {
	globals starlark.StringDict
	err     error
}

func newModuleLoader(sources map[string]string) *moduleLoader {
	return &moduleLoader{sources: sources, cache: make(map[string]*moduleEntry, len(sources))}
}

// load resolves one module by its (cleaned) bundle entry name. The entrypoint may not
// be loaded (it's the program root, not a module); loading a missing name errors.
// Re-entrant loads of a module already in progress are a cycle and error.
func (l *moduleLoader) load(thread *starlark.Thread, module string) (starlark.StringDict, error) {
	name := cleanEntry(module)
	l.mu.Lock()
	if e, ok := l.cache[name]; ok {
		l.mu.Unlock()
		if e == nil {
			return nil, fmt.Errorf("import cycle through %q", name)
		}
		return e.globals, e.err
	}
	src, ok := l.sources[name]
	if !ok {
		l.mu.Unlock()
		return nil, fmt.Errorf("no module %q in bundle", name)
	}
	l.cache[name] = nil // in-progress sentinel for cycle detection
	l.mu.Unlock()

	globals, err := starlark.ExecFileOptions(fileOpts, thread, name, src, hostBuiltins())
	e := &moduleEntry{globals: globals, err: err}
	l.mu.Lock()
	l.cache[name] = e
	l.mu.Unlock()
	return globals, err
}

// hostBuiltins is the (pure, I/O-free) host surface a program may call: the struct
// constructor the engine's typed values are built from, plus a glob matcher. NO
// filesystem/network/clock builtin is granted, so composition stays deterministic.
func hostBuiltins() starlark.StringDict {
	return starlark.StringDict{
		"struct": starlark.NewBuiltin("struct", starlarkstruct.Make),
		// match(pattern, s) — filepath.Match glob, e.g. match("aws-*", "aws-dev").
		"match": starlark.NewBuiltin("match", func(_ *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, _ []starlark.Tuple) (starlark.Value, error) {
			var pattern, s string
			if err := starlark.UnpackArgs("match", args, nil, "pattern", &pattern, "s", &s); err != nil {
				return nil, err
			}
			ok, err := filepath.Match(pattern, s)
			if err != nil {
				return nil, err
			}
			return starlark.Bool(ok), nil
		}),
	}
}
