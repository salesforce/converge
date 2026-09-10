// Package stdio is the GENERIC provider: it lets an external program — written in
// ANY language, with no converge Go SDK — act as a converge resource kind via a
// stdin/stdout JSON protocol. Per task the worker runs the program (from the
// providerconfig's `command`/`args`, or the STDIO_COMMAND env var), writes the task as one JSON
// Request to the program's stdin, reads one JSON Response from its stdout, and maps
// it to the reaction's Outcome — doing all the DB/fencing work on the program's
// behalf. The program's spec, providerconfig, and bundle are OPAQUE: the worker
// interprets none of them, forwarding each verbatim, so the program owns its schema.
//
// This is the converge equivalent of a plugin protocol (MCP/LSP-style): a stable
// stdio-RPC contract external teams implement to plug in, instead of importing the
// Go SDK. The classic case is "run some arbitrary bash": a `work` reaction whose
// program is a small script that reads the spec and records a status. It can also
// COMPOSE — a program that returns children + edges fans out a graph, the celbom/
// stdstarlark capability from any language.
//
// The converge stdio protocol (one-shot subprocess, stateless):
//   - stdin  ← one Request JSON (reaction, trigger, kind/name/generation, spec,
//     config, bundle, observed children).
//   - stdout → one Response JSON (status, conditions, children, edges). Empty
//     stdout is a valid empty Outcome (a pure side-effect program).
//   - exit 0  → success; parse stdout.
//     exit 2  → TERMINAL failure (a config/spec error a retry can't fix); no retry.
//     other   → TRANSIENT failure; the engine retries with backoff.
//     stderr is folded into the failure message.
//
// The child is run under the task's context: a cancelled task (worker drain, lease
// loss) or a configured timeout kills the process, so no run outlives its claim.
//
// It imports only sdk/* — no internal/* — exactly what an external team ships.
package stdio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/salesforce/converge/sdk-go/converge"
)

// exitTerminal is the child exit code that signals a NON-retryable failure (a bad
// spec/config the same generation can't fix). Any other non-zero exit is transient.
// Chosen as 2 (the common "usage/config error" convention; 1 stays a generic
// transient failure) so a program opts into terminal only deliberately.
const exitTerminal = 2

// envCommandVar is the environment variable an operator sets on the stdworker to
// name the program to run when a providerconfig sets no `command` — the demo/host
// path, where the program lives at a known filesystem location.
const envCommandVar = "STDIO_COMMAND"

// backend is the stdio providers' shared, process-wide environment: the STDIO_COMMAND
// fallback + the captured os.Environ, snapshotted ONCE. Every single-pair provider below
// closes over one backend, so the env capture is done once for the whole worker rather
// than per kind.
type backend struct {
	envCommand string    // STDIO_COMMAND fallback, captured once
	environ    []string  // os.Environ() captured once
	initEnv    sync.Once // guards the one-time capture
}

// ensure captures the process environment on first use (lazily, so tests that set
// STDIO_COMMAND after construction still see it, and construction stays pure).
func (b *backend) ensure() {
	b.initEnv.Do(func() {
		b.environ = os.Environ()
		b.envCommand = os.Getenv(envCommandVar)
	})
}

// provider is ONE stdio (kind, version) as a converge.Provider — the single-pair
// contract. It holds that kind's live default providerconfig (delivered via OnConfig)
// so the command/args/settings a task reads reflect an operator's edit with no restart,
// and shares the process-wide backend (env fallback + captured environ). Leaf vs
// composer is NOT distinguished here: the kind's manifest declares `work` (leaf) or
// `compose` (composer), the core dispatches the right reaction, and Work carries it to
// the program in the Request — so ONE Work body serves both roles.
//
// A worker serving several stdio kinds (STDIO_KINDS / STDIO_COMPOSER_KINDS) registers
// several of these — one per name, all over the same backend — via Providers(). Imports
// only sdk/* — no internal/*.
type provider struct {
	kv  converge.KindVersion
	be  *backend
	mu  sync.RWMutex            // guards cfg
	cfg converge.ProviderConfig // this kind's live default (from OnConfig)
}

var _ converge.Provider = (*provider)(nil)

// Providers resolves the (kind, version) pairs this worker serves — the operator's
// STDIO_KINDS (leaf) + STDIO_COMPOSER_KINDS (composer), each at v1, or the built-in
// defaults ({stdio}, {stdio-composer}) — and returns one single-pair provider per name
// over a shared backend. PURE: an env snapshot, no I/O. A name appearing in BOTH roles is
// deduped (registered once); the genuine leaf-vs-composer conflict fails a task terminally
// in Work rather than silently registering an ambiguous kind. The worker registers each
// in the slice passed to converge.Serve (or via Providers()); the SDK fans out.
func Providers() []converge.Provider {
	leaf, composer := servedKinds()
	be := &backend{}
	seen := make(map[converge.Kind]bool, len(leaf)+len(composer))
	out := make([]converge.Provider, 0, len(leaf)+len(composer))
	for _, k := range append(append([]converge.Kind{}, leaf...), composer...) {
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, &provider{kv: converge.KindVersion{Kind: k, Version: 1}, be: be})
	}
	return out
}

// Kind is the single (kind, version) this provider serves.
func (p *provider) Kind() converge.KindVersion { return p.kv }

// OnConfig stores this kind's live default providerconfig (spec + bundle) so per-task
// reads via Work see an operator's edit immediately.
func (p *provider) OnConfig(cfg converge.ProviderConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cfg = cfg
}

// Ready is always true: stdio dials no downstream at bring-up (a missing command fails
// the individual task terminally in Work, per-resource, not the whole kind).
func (*provider) Ready() bool { return true }

// Work runs one task for this kind: it resolves the live default config into a worker and
// runs the external program. Leaf ("work") vs composer ("compose") is carried in
// req.Reaction, which the program interprets — one body, both roles.
func (p *provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	p.be.ensure()
	p.mu.RLock()
	def := p.cfg
	p.mu.RUnlock()
	w := worker{
		defaultSpec: def.Spec,
		defaultData: def.Data,
		envCommand:  p.be.envCommand,
		environ:     p.be.environ,
	}
	return w.React(ctx, req)
}

// servedKinds resolves the LEAF and COMPOSER kind names this worker serves from
// STDIO_KINDS / STDIO_COMPOSER_KINDS. When BOTH are unset it returns the single
// built-in default of each ({stdio}, {stdio-composer}) so a zero-config worker (the
// demo/quick-start) still serves the reference kinds. Setting EITHER var switches to
// exactly the listed names for that role (an empty other role is then valid — e.g.
// leaf-only). Pure: env snapshot, no I/O.
func servedKinds() (leaf, composer []converge.Kind) {
	leaf = parseKindList(os.Getenv(envKinds))
	composer = parseKindList(os.Getenv(envComposerKinds))
	if len(leaf) == 0 && len(composer) == 0 {
		return []converge.Kind{Kind}, []converge.Kind{ComposerKind}
	}
	return leaf, composer
}

// parseKindList splits a comma-separated env value into trimmed, non-empty kind
// names, preserving order and dropping duplicates. Blank/whitespace → nil.
func parseKindList(v string) []converge.Kind {
	var out []converge.Kind
	seen := map[string]bool{}
	for part := range strings.SplitSeq(v, ",") {
		name := strings.TrimSpace(part)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, converge.Kind(name))
	}
	return out
}

// worker holds the per-kind environment: the kind default config `spec` + `data` bytes,
// the STDIO_COMMAND fallback, and the captured process environment. Stateless per task.
type worker struct {
	defaultSpec json.RawMessage // kind default config `spec` (from OnConfig)
	defaultData []byte          // kind default bundle `data` (from OnConfig)
	envCommand  string          // STDIO_COMMAND fallback when the config sets no command
	environ     []string        // os.Environ() captured once
}

// React runs one task: resolve the effective config, build the launch command, run
// the program with the Request on stdin, and map its stdout Response to an Outcome.
// Both the "work" and "compose" reactions share this body — the same program
// handles both, told which via Request.Reaction and shaping its Response accordingly
// (the core drops any part the reaction's Emits mask forbids).
func (w worker) React(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	cfg := converge.EffectiveConfig[Config](w.defaultSpec, req.Env.ProviderConfig)

	command := cfg.Command
	if command == "" {
		command = w.envCommand
	}
	if command == "" {
		// A resource of this kind with no command anywhere is a misconfiguration a
		// retry can't fix — make it loud (Terminal) instead of spinning.
		return converge.Outcome{}, converge.Terminal(errors.New("stdio: no command configured (set providerconfig.command or the STDIO_COMMAND env var)"))
	}

	payload, err := json.Marshal(w.buildRequest(req, cfg.Settings))
	if err != nil {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdio: encode request: %w", err))
	}

	// Bound the run: the task's ctx already carries the claim/deadline; layer the
	// provider's own timeout on top when configured so a hung program can't pin the
	// slot until the lease ceiling.
	runCtx := ctx
	if cfg.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(cfg.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(runCtx, command, cfg.Args...)
	cmd.Env = w.childEnv(cfg.Env)
	cmd.Stdin = bytes.NewReader(payload)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return converge.Outcome{}, runError(runCtx, req.Resource.Name, err, &stderr)
	}

	return decodeResponse(req.Reaction, stdout.Bytes())
}

// buildRequest flattens the ReactionRequest into the wire Request: the resolved
// program `settings` blob (the effective providerconfig's Settings, already
// default ⊕ override), the effective bundle (per-resource override else kind
// default), and — for compose — the currently-observed children so the program can
// diff desired-vs-observed.
func (w worker) buildRequest(req converge.ReactionRequest, settings json.RawMessage) Request {
	out := Request{
		Reaction:   req.Reaction,
		Trigger:    string(req.Trigger),
		Kind:       string(req.Resource.Kind),
		Name:       req.Resource.Name,
		Generation: req.Generation,
		Spec:       req.Resource.Spec,
		Config:     settings,
		Bundle:     converge.EffectiveBundle(w.defaultData, req.Env.ProviderBundle),
	}
	if req.Reaction == "compose" && len(req.Observed) > 0 {
		out.Observed = make([]ObservedChild, len(req.Observed))
		for i, c := range req.Observed {
			out.Observed[i] = ObservedChild{
				Kind:   string(c.Kind),
				Name:   c.Name,
				Ready:  c.IsReady,
				Status: c.Status,
			}
		}
	}
	return out
}

// childEnv layers the config's extra KEY=VALUE pairs on the worker's captured
// environment. The worker environ comes first so a config can override an inherited
// var (os/exec resolves a duplicate key to the LAST occurrence).
func (w worker) childEnv(extra map[string]string) []string {
	if len(extra) == 0 {
		return w.environ
	}
	env := make([]string, 0, len(w.environ)+len(extra))
	env = append(env, w.environ...)
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

// runError maps a failed cmd.Run to a converge error. A ctx cancellation/timeout
// is transient (the task will re-dispatch or the deadline reaper reclaims it); a
// clean process exit with code exitTerminal is TERMINAL; any other non-zero exit is
// transient. stderr is folded in so the failure is diagnosable.
func runError(ctx context.Context, name string, err error, stderr *bytes.Buffer) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		// The run was cut short by cancellation/timeout, not the program's own
		// verdict — transient so the engine retries (or the reaper reclaims the lease).
		return fmt.Errorf("stdio: %s cancelled/timed out: %w", name, ctxErr)
	}
	msg := trimStderr(stderr)
	var ee *exec.ExitError
	if errors.As(err, &ee) && ee.ExitCode() == exitTerminal {
		return converge.Terminal(fmt.Errorf("stdio: %s failed terminally (exit %d): %s", name, exitTerminal, msg))
	}
	return fmt.Errorf("stdio: %s failed: %w: %s", name, err, msg)
}

// decodeResponse parses the child's stdout into an Outcome. Empty stdout is a valid
// empty Outcome (a program that only side-effects). A non-empty body that is not
// valid JSON is TERMINAL — the program is misbehaving in a way a retry won't fix.
func decodeResponse(reaction string, stdout []byte) (converge.Outcome, error) {
	body := bytes.TrimSpace(stdout)
	if len(body) == 0 {
		return converge.Outcome{}, nil
	}
	var resp Response
	if err := json.Unmarshal(body, &resp); err != nil {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdio: decode response: %w", err))
	}

	out := converge.Outcome{
		Status:     resp.Status,
		Conditions: toConditions(resp.Conditions),
	}
	// Children/edges only matter for a compose reaction; the core's Emits mask drops
	// them for a work reaction anyway, but skip the mapping so a work program that
	// stuffs children can't accidentally shape a graph.
	if reaction == "compose" {
		out.Children = toChildren(resp.Children)
		out.Edges = toEdges(resp.Edges)
	}
	return out, nil
}

func toConditions(in []ResponseCondition) []converge.Condition {
	if len(in) == 0 {
		return nil
	}
	out := make([]converge.Condition, len(in))
	for i, c := range in {
		out[i] = converge.Condition{
			Type:    c.Type,
			Status:  converge.ConditionStatus(c.Status),
			Reason:  c.Reason,
			Message: c.Message,
		}
	}
	return out
}

func toChildren(in []ResponseChild) []converge.ChildSpec {
	if len(in) == 0 {
		return nil
	}
	out := make([]converge.ChildSpec, len(in))
	for i, c := range in {
		out[i] = converge.ChildSpec{
			Kind:        converge.Kind(c.Kind),
			KindVersion: c.KindVersion, // explicit web-API version from the compose program (no implicit default)
			Name:        c.Name,
			Spec:        c.Spec, // json.RawMessage — the core marshals at the DB boundary
			Labels:      c.Labels,
		}
	}
	return out
}

func toEdges(in []ResponseEdge) []converge.DepEdge {
	if len(in) == 0 {
		return nil
	}
	out := make([]converge.DepEdge, len(in))
	for i, e := range in {
		var flows []converge.ValueFlow
		if len(e.Values) > 0 {
			flows = make([]converge.ValueFlow, len(e.Values))
			for j, v := range e.Values {
				flows[j] = converge.ValueFlow{DependentField: v.DependentField, SourceField: v.SourceField}
			}
		}
		out[i] = converge.DepEdge{
			From:   converge.ResourceRef{Kind: converge.Kind(e.From.Kind), Name: e.From.Name},
			To:     converge.ResourceRef{Kind: converge.Kind(e.To.Kind), Name: e.To.Name},
			Values: flows,
		}
	}
	return out
}

// trimStderr bounds captured stderr in an error message so a chatty program can't
// bloat the failure record; the tail (where the fatal message usually is) is kept.
func trimStderr(b *bytes.Buffer) string {
	const max = 2048
	s := bytes.TrimSpace(b.Bytes())
	if len(s) <= max {
		return string(s)
	}
	return "…" + string(s[len(s)-max:])
}
