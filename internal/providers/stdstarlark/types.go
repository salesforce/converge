package stdstarlark

import "github.com/salesforce/converge/sdk-go/converge"

// Kind is the single composer kind this provider serves; its CRD lives beside this
// package in stdstarlark.kind.json.
const Kind converge.Kind = "stdstarlark"

// entrypoint is the .star file at the bundle ROOT the composer execs; it must define
// a compose(spec, config) function returning struct(children=…, edges=…, configs=…).
// Other .star files in the bundle are LOADABLE MODULES (via Starlark's load()), not
// auto-executed — so the program decides how it splits across files (helpers, a set
// of policy modules, etc.) rather than the provider imposing a convention.
const entrypoint = "compose.star"

// Size caps bound the in-memory unzip (zip-bomb defence): a malformed or hostile
// bundle can't exhaust the worker. A real composition program is a few KB of text.
const (
	maxBundleBytes = 4 << 20   // 4 MiB total decompressed
	maxStarBytes   = 512 << 10 // 512 KiB per .star entry
)
