package stdio

import (
	"encoding/json"

	"github.com/salesforce/converge/sdk-go/converge"
)

// The stdio provider serves LEAF kinds and COMPOSER kinds — the same handler + JSON
// protocol drives both; they differ only in which reaction the kind's manifest
// declares, so the core dispatches the right one:
//
//   - a LEAF kind declares one `work` reaction (specChange → status): "run some
//     external program and record the result".
//   - a COMPOSER kind declares one `compose` reaction (specChange → children +
//     status): the program fans out a child graph, in any language.
//
// They are separate kinds (not one kind with both reactions) so each resource has
// ONE role: a reconcile runs either the leaf program or the composer program.
//
// WHICH kind names this worker serves is an OPERATOR choice, read from env so one
// stdworker fleet can back many distinct, named kinds (e.g. "backups", "dns")
// rather than everyone sharing one generic name — each named kind is then distinct
// in listings, gets its OWN CRD + providerconfig (its own command/settings/schema),
// and its own concurrency cap:
//
//   - STDIO_KINDS          — comma-separated LEAF kind names (each gets `work`).
//   - STDIO_COMPOSER_KINDS — comma-separated COMPOSER kind names (each gets `compose`).
//
// When BOTH are unset the worker falls back to the built-in defaults below, so the
// demo and quick-start work with zero config. Their reference CRDs live beside this
// package in stdio.kind.json and stdio-composer.kind.json.
const (
	Kind         converge.Kind = "stdio"          // default LEAF kind when STDIO_KINDS is unset
	ComposerKind converge.Kind = "stdio-composer" // default COMPOSER kind when STDIO_COMPOSER_KINDS is unset
)

// Env var names an operator sets on the stdworker to serve named kinds instead
// of the defaults (see the const block above).
const (
	envKinds         = "STDIO_KINDS"
	envComposerKinds = "STDIO_COMPOSER_KINDS"
)

// Spec is a stdio resource's desired state — deliberately OPAQUE. The provider
// never interprets it: it is handed to the external program verbatim as
// Request.Spec, so a spec can be "anything" the program understands (a shell
// command, a manifest, a set of inputs). The only convention is optional: a spec
// MAY carry a top-level "command"/"args" to run instead of the config's default,
// letting a single stdio kind drive many one-off commands.
//
// Kept as raw bytes (no Go struct) so no per-resource schema is imposed and no
// marshal/unmarshal round-trip is paid on the hot path — the bytes flow straight
// through to the program's stdin.
type Spec = json.RawMessage

// Config is the stdio kind's providerconfig — WHERE/HOW to run the external
// program, the same for every stdio resource (with per-resource overrides merged
// at work time). Like the spec, its `settings` blob is opaque and forwarded to
// the program as Request.Config; only the launch fields are interpreted here.
type Config struct {
	// Command is the program to run per task (an absolute path or a name on PATH).
	// Empty falls back to the STDIO_COMMAND env var the worker was started with; if
	// BOTH are empty a task fails TERMINALLY (misconfiguration).
	Command string `json:"command,omitempty" doc:"Program to run per task (path or PATH name). Falls back to the STDIO_COMMAND env var."`
	// Args are fixed leading arguments passed before the per-task ones. Use for a
	// subcommand or a script path (e.g. ["-c", "my-handler.sh"]).
	Args []string `json:"args,omitempty" doc:"Fixed leading arguments passed to the command on every task."`
	// Env is extra environment KEY=VALUE pairs layered on the worker's own
	// environment for the child (the child also inherits os.Environ()).
	Env map[string]string `json:"env,omitempty" doc:"Extra environment variables (KEY→VALUE) for the child process."`
	// Settings is an opaque blob forwarded to the program as Request.Config — the
	// program's own configuration, whatever shape it wants. Never interpreted here.
	Settings json.RawMessage `json:"settings,omitempty" doc:"Opaque configuration forwarded to the program as request.config."`
	// TimeoutSeconds bounds one task's subprocess. 0 = no provider-imposed timeout
	// (the task's own deadline/ctx still applies). A run that overruns is killed and
	// the task fails transiently (retried).
	TimeoutSeconds int `json:"timeout_seconds,omitempty" doc:"Per-task subprocess timeout in seconds (0 = none; the task deadline still applies)."`
}

// Request is the JSON envelope written to the program's STDIN per task — the whole
// ReactionRequest an agent needs, flattened to a stable, language-agnostic shape.
// A program reads one Request from stdin, does its work, and writes one Response to
// stdout. Bytes fields (Spec/Config) are passed through verbatim so the program
// owns their schema. This envelope IS the converge agent protocol.
type Request struct {
	// Reaction is which reaction fired: "work" or "compose". A program that only
	// does leaf work can ignore everything but "work".
	Reaction string `json:"reaction"`
	// Trigger is why it fired (e.g. "specChange") — informational.
	Trigger string `json:"trigger"`

	// Kind / Name / Generation identify the subject resource. Name is stable across
	// generations; Generation lets a program key idempotent side effects.
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Generation int64  `json:"generation"`

	// Spec is the resource's spec verbatim (opaque). Config is the EFFECTIVE
	// providerconfig `settings` (kind default ⊕ per-resource override), verbatim.
	Spec   json.RawMessage `json:"spec,omitempty"`
	Config json.RawMessage `json:"config,omitempty"`

	// Bundle is the effective providerconfig bundle bytes (opaque artifact, e.g. a
	// zip the program interprets), base64-encoded for JSON transport. Omitted when
	// there is no bundle. A program that ignores bundles can drop this field.
	Bundle []byte `json:"bundle,omitempty"`

	// Observed are the composer's currently-owned children (compose reaction only),
	// so a program can compute the desired set relative to what exists.
	Observed []ObservedChild `json:"observed,omitempty"`
}

// ObservedChild is one currently-owned child handed to a compose Request, so a
// program can diff desired-vs-observed. Status is the child's last status verbatim.
type ObservedChild struct {
	Kind   string          `json:"kind"`
	Name   string          `json:"name"`
	Ready  bool            `json:"ready"`
	Status json.RawMessage `json:"status,omitempty"`
}

// Response is the JSON envelope the program writes to STDOUT — the Outcome it
// produces, in the same language-agnostic shape. A program fills only the parts its
// reaction is allowed to emit (work → status/conditions; compose → children/edges/
// status); the core enforces the manifest Emits mask, so extra parts are ignored.
// An empty stdout (a program that only side-effects and reports nothing) is a valid
// success — it maps to an empty Outcome.
type Response struct {
	// Status is the resource's new status document (opaque), stored verbatim.
	Status json.RawMessage `json:"status,omitempty"`
	// Conditions are health/custom axes to upsert (see ResponseCondition).
	Conditions []ResponseCondition `json:"conditions,omitempty"`
	// Children / Edges are the composer's desired graph (compose reaction only).
	Children []ResponseChild `json:"children,omitempty"`
	Edges    []ResponseEdge  `json:"edges,omitempty"`
}

// ResponseCondition mirrors converge.Condition in the wire shape. Status is one of
// "True" | "False" | "Unknown".
type ResponseCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ResponseChild is one child a compose program wants to exist. Spec is opaque
// (whatever the child kind expects) and passed through unmodified.
type ResponseChild struct {
	Kind string `json:"kind"`
	// KindVersion is the web-API version the child is applied at. REQUIRED and
	// explicit (>= 1): a compose program must set it on each child — no implicit v1
	// default (ApplyComposeResult rejects a 0).
	KindVersion int               `json:"kind_version"`
	Name        string            `json:"name"`
	Spec        json.RawMessage   `json:"spec"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// ResponseEdge is one dependency edge (dependent `from` depends on dependency
// `to`), optionally carrying value flows that fill the dependent's spec from the
// upstream's status when it becomes ready. Kind+Name identify each endpoint.
type ResponseEdge struct {
	From   ResponseRef         `json:"from"`
	To     ResponseRef         `json:"to"`
	Values []ResponseValueFlow `json:"values,omitempty"`
}

// ResponseRef is a (kind, name) resource reference used by ResponseEdge endpoints.
type ResponseRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// ResponseValueFlow declares one field-level flow on an edge: the dependent's
// spec[dependent_field] is filled from the upstream's status[source_field] once the
// upstream is ready. Both are RFC 6901 JSON Pointers ("/account_id"); a bare key is
// treated as a single-segment pointer.
type ResponseValueFlow struct {
	DependentField string `json:"dependent_field"`
	SourceField    string `json:"source_field"`
}
