package stdshell

import "github.com/salesforce/converge/sdk-go/converge"

// Kind is the FIXED kind this provider serves — "stdshell". Its reference CRD lives
// beside this package in stdshell.kind.json. Unlike the stdio provider (whose kind names are
// operator-chosen via STDIO_KINDS), the stdshell provider serves exactly one built-in
// kind; there is no env var to rename it.
const Kind converge.Kind = "stdshell"

// Spec is a stdshell resource's desired state: the script to run plus how to run it.
// Unlike the generic `stdio` provider (whose spec is fully opaque and interpreted
// by an external program), the stdshell provider interprets the spec itself — the
// script text is inline, so a resource is entirely self-contained (no external
// program, no fetch, no bundle) and the DB row IS the source of truth.
type Spec struct {
	// Script is the script text to run, inline. It is written to a temp file and
	// executed by Interpreter. Required; an empty script fails TERMINALLY (a
	// misconfiguration a same-generation retry can't fix).
	Script string `json:"script" doc:"The script text to run, inline (executed by the interpreter)."`
	// Interpreter is the program the script is run under (an absolute path or a
	// name on PATH). Empty defaults to the kind's configured DefaultInterpreter,
	// then to "bash". The script path is passed as the interpreter's final argument
	// after Args (e.g. bash [Args…] /tmp/script).
	Interpreter string `json:"interpreter,omitempty" doc:"Interpreter to run the script under (path or PATH name). Default: the kind config's default_interpreter, else bash."`
	// Args are extra arguments passed to the interpreter BEFORE the script path
	// (e.g. ["-x"] for bash trace, or a python module flag).
	Args []string `json:"args,omitempty" doc:"Extra arguments passed to the interpreter before the script path."`
	// Env is extra environment KEY=VALUE pairs for the script, layered over the
	// worker's own environment and the kind config's Env (spec wins on a clash).
	Env map[string]string `json:"env,omitempty" doc:"Extra environment variables (KEY→VALUE) for the script."`
	// TimeoutSeconds bounds this run. 0 = fall back to the kind config's
	// DefaultTimeoutSeconds (also 0 = no provider timeout; the task deadline still
	// applies). A run that overruns is killed and the task fails transiently.
	TimeoutSeconds int `json:"timeout_seconds,omitempty" doc:"Per-run timeout in seconds (0 = the kind config default, then no provider timeout)."`
}

// Status is the run result recorded on the resource: the exit code and captured
// output (bounded). A downstream consumer can value-flow a field out of here.
type Status struct {
	// ExitCode is the script's process exit code (0 on success — a non-zero exit
	// makes the whole task fail, so a persisted status always carries ExitCode 0;
	// the field is kept for completeness + forward compatibility).
	ExitCode int `json:"exit_code" doc:"The script's process exit code (0 on success)."`
	// Stdout / Stderr are the captured, tail-bounded output streams (the last
	// MaxOutputBytes of each) so a chatty script can't bloat the status row.
	Stdout string `json:"stdout,omitempty" doc:"Captured stdout (tail-bounded)."`
	Stderr string `json:"stderr,omitempty" doc:"Captured stderr (tail-bounded)."`
}

// Config is the shell kind's providerconfig — the DEFAULTS applied to every shell
// resource of the kind (each overridable per-resource via the spec). It lets an
// operator standardise the interpreter, a baseline environment, and a timeout for
// a named kind without repeating them in every resource.
type Config struct {
	// DefaultInterpreter is the interpreter used when a spec sets none. Empty →
	// "bash".
	DefaultInterpreter string `json:"default_interpreter,omitempty" doc:"Interpreter used when a resource spec sets none (default bash)."`
	// Env is baseline environment KEY=VALUE pairs for every script of the kind, layered
	// over the worker's environment (a spec's Env overrides on a key clash).
	Env map[string]string `json:"env,omitempty" doc:"Baseline environment variables (KEY→VALUE) for every script of this kind."`
	// DefaultTimeoutSeconds bounds a run when the spec sets no TimeoutSeconds. 0 =
	// no provider-imposed timeout (the task deadline still applies).
	DefaultTimeoutSeconds int `json:"default_timeout_seconds,omitempty" doc:"Per-run timeout used when a resource sets none (0 = none; the task deadline still applies)."`
}
