// Package stdshell is a REAL leaf provider that runs a user-supplied script. A
// stdshell resource carries its script INLINE in the spec; per task the worker writes
// the script to a temp file, runs it under the configured interpreter (bash by
// default), captures stdout/stderr, and records the result as status. It is the
// simplest "run this command as a converge resource" kind — self-contained, no
// external program to install (cf. the generic `stdio` provider, whose spec is
// opaque and driven by a separate stdio program).
//
// Error semantics (so the engine retries correctly):
//   - a bad spec (no script) is TERMINAL — a same-generation retry can't fix it.
//   - the script exiting non-zero is a TRANSIENT failure by default (the engine
//     retries with backoff up to the kind's max_transient_attempts), UNLESS the
//     script exits with code 2, which is TERMINAL (the same "config/usage error"
//     convention the stdio provider uses) — a script opts into terminal
//     deliberately so a genuinely-unfixable run stops retrying and dead-letters.
//   - a ctx cancellation/timeout (worker drain, lease loss, per-run timeout) is
//     TRANSIENT — the run was cut short, not the script's own verdict.
//
// The script runs under the task's context, so a cancelled task (drain, lease
// loss) or a configured timeout kills the process — no run outlives its claim.
//
// It serves the single fixed kind "stdshell" (no env var to rename it). It imports
// only sdk/* — no internal/* — exactly what an external team ships.
package stdshell

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/salesforce/converge/sdk-go/converge"
)

// exitTerminal is the script exit code that signals a NON-retryable failure (a bad
// script the same generation can't fix). Any other non-zero exit is transient.
// Chosen as 2 (the common "usage/config error" convention; 1 stays a generic
// transient failure) and matches the stdio provider so the two behave alike.
const exitTerminal = 2

// maxOutputBytes bounds each captured stream stored in status (tail-kept) so a
// chatty script can't bloat the status row / DB write.
const maxOutputBytes = 8192

// waitDelay bounds how long cmd.Wait blocks for I/O AFTER the run's context is
// cancelled and the process (group) is signalled — the backstop for a grandchild
// that outlives the kill while still holding the output pipe. Kept small so a
// timed-out/cancelled task frees its worker slot promptly instead of hanging on a
// leaked descendant.
const waitDelay = 3 * time.Second

// defaultInterpreter is used when neither the spec nor the kind config names one.
const defaultInterpreter = "bash"

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). It runs an inline script per task,
// so it is a pure leaf — always Ready, no downstream to dial. Imports only sdk/* —
// no internal/*, exactly what an external team ships.
//
// It holds two pieces of per-worker state gathered when the Provider is built:
//   - environ: os.Environ() captured once, the base environment layered under a
//     resource's config/spec env for every child process.
//   - defaultSpec (guarded by mu): the kind's LIVE default providerconfig `spec`, the
//     latest OnConfig push. Work overlays the per-resource override on it per task via
//     converge.EffectiveConfig, so an operator's providerconfig edit lands with no restart.
type Provider struct {
	environ []string // os.Environ() captured when the Provider is built

	mu          sync.RWMutex    // guards defaultSpec (OnConfig writes, Work reads)
	defaultSpec json.RawMessage // latest pushed default config `spec` (nil until first push)
}

// New builds a Provider capturing os.Environ() as the base environment for child
// processes. The zero Provider value (stdshell.Provider{}) is also usable — its
// environ is nil, meaning the child inherits nothing beyond the config/spec env —
// but New is the canonical constructor and the one Runtime uses.
func New() *Provider { return &Provider{environ: os.Environ()} }

// compile-time proof it satisfies the one provider contract. A *Provider is the
// receiver because OnConfig mutates guarded state.
var _ converge.Provider = (*Provider)(nil)

// Kind is the single (kind, version) this provider serves — the fixed kind
// "stdshell" at web-API version 1 (explicit; there is no implicit default).
func (p *Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// OnConfig stores the kind's latest default providerconfig `spec` under the guard so
// Work can overlay the per-resource override on it per task. The stdshell config is
// spec-only (no bundle), so cfg.Data is ignored; an empty cfg.Spec (a deleted default)
// clears it.
func (p *Provider) OnConfig(cfg converge.ProviderConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.defaultSpec = cfg.Spec
}

// Ready is always true: a pure leaf that runs an inline script has no downstream to
// dial, so it can always do work.
func (p *Provider) Ready() bool { return true }

// defaultConfig returns the current default-config `spec` under the read guard, so a
// concurrent OnConfig push and a Work read never race on the field.
func (p *Provider) defaultConfig() json.RawMessage {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.defaultSpec
}

// Work runs one task: decode the spec, resolve the effective config (the kind's live
// default from OnConfig overlaid with the per-resource override), write the inline
// script to a temp file, run it under the interpreter with the task's context, and
// map the result (exit code + output) to an Outcome carrying status and a Ready
// condition. stdshell declares a single reaction, so Work does not switch on
// req.Reaction — every reaction runs this one body.
func (p *Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	var spec Spec
	if len(req.Resource.Spec) > 0 {
		if err := decodeStrict(req.Resource.Spec, &spec); err != nil {
			return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdshell: decode spec: %w", err))
		}
	}
	if strings.TrimSpace(spec.Script) == "" {
		return converge.Outcome{}, converge.Terminal(errors.New("stdshell: spec.script is required and must be non-empty"))
	}

	var override json.RawMessage
	if req.Env != nil {
		override = req.Env.ProviderConfig
	}
	cfg := converge.EffectiveConfig[Config](p.defaultConfig(), override)

	interpreter := firstNonEmpty(spec.Interpreter, cfg.DefaultInterpreter, defaultInterpreter)
	timeoutSecs := spec.TimeoutSeconds
	if timeoutSecs == 0 {
		timeoutSecs = cfg.DefaultTimeoutSeconds
	}

	// Write the inline script to a per-task temp file, removed on return so a worker
	// accrues no state. 0700: readable/executable only by the worker user.
	dir, err := os.MkdirTemp("", "shell-"+sanitize(req.Resource.Name)+"-")
	if err != nil {
		return converge.Outcome{}, fmt.Errorf("stdshell: temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	scriptPath := filepath.Join(dir, "script")
	if err := os.WriteFile(scriptPath, []byte(spec.Script), 0o700); err != nil {
		return converge.Outcome{}, fmt.Errorf("stdshell: write script: %w", err)
	}

	// Bound the run: the task ctx already carries the claim deadline; layer the
	// provider's own timeout on top when configured so a hung script can't pin the
	// slot until the lease ceiling.
	runCtx := ctx
	if timeoutSecs > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
		defer cancel()
	}

	// interpreter [spec.Args…] <scriptPath>: the script path is the final argument so
	// the interpreter's own flags (Args) precede it, matching `bash -x /path`.
	argv := append(append([]string{}, spec.Args...), scriptPath)
	cmd := exec.CommandContext(runCtx, interpreter, argv...)
	cmd.Dir = dir
	cmd.Env = childEnv(p.environ, cfg.Env, spec.Env)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	// Run the script in its OWN process group and, on ctx cancel/timeout, kill the
	// WHOLE group — not just the interpreter — so a script's own children (the
	// `sleep` in `bash -c 'sleep 30'`, a spawned tool) die with it. Without this a
	// grandchild both survives the kill AND keeps the output pipe open, so cmd.Wait
	// blocks on the pipe copy until the grandchild exits, pinning the worker slot far
	// past the deadline. WaitDelay is the backstop: after Cancel fires, Wait waits at
	// most that long for I/O before force-closing the pipes and returning.
	setProcessGroup(cmd)
	cmd.WaitDelay = waitDelay

	runErr := cmd.Run()
	status := Status{
		Stdout: tail(stdout.Bytes(), maxOutputBytes),
		Stderr: tail(stderr.Bytes(), maxOutputBytes),
	}
	if runErr != nil {
		return converge.Outcome{}, runError(runCtx, req.Resource.Name, interpreter, runErr, &stderr)
	}

	statusJSON, err := marshal(status)
	if err != nil {
		return converge.Outcome{}, fmt.Errorf("stdshell: encode status: %w", err)
	}
	if req.Env != nil && req.Env.Logger != nil {
		req.Env.Logger.Info("shell script succeeded",
			"resource", req.Resource.Name, "interpreter", interpreter,
			"stdout_bytes", stdout.Len(), "stderr_bytes", stderr.Len())
	}
	return converge.Outcome{
		Status: statusJSON,
		Conditions: []converge.Condition{{
			Type:   converge.TypeReady,
			Status: converge.ConditionTrue,
			Reason: "ScriptSucceeded",
		}},
	}, nil
}

// runError maps a failed cmd.Run to a converge error. A ctx cancellation/timeout is
// TRANSIENT (the task will re-dispatch or the deadline reaper reclaims the lease);
// a clean exit with code exitTerminal is TERMINAL; a "interpreter not found" is
// TERMINAL (a retry can't summon a missing binary); any other non-zero exit is
// TRANSIENT. stderr is folded in so the failure is diagnosable.
func runError(ctx context.Context, name, interpreter string, err error, stderr *bytes.Buffer) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("stdshell: %s cancelled/timed out: %w", name, ctxErr)
	}
	msg := tail(stderr.Bytes(), maxOutputBytes)
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ee.ExitCode() == exitTerminal {
			return converge.Terminal(fmt.Errorf("stdshell: %s failed terminally (exit %d): %s", name, exitTerminal, msg))
		}
		return fmt.Errorf("stdshell: %s failed (exit %d): %s", name, ee.ExitCode(), msg)
	}
	// Not an ExitError: the process could not be started (e.g. interpreter missing).
	// A retry can't create the binary, so this is TERMINAL and diagnosable.
	if errors.Is(err, exec.ErrNotFound) {
		return converge.Terminal(fmt.Errorf("stdshell: interpreter %q not found on PATH: %w", interpreter, err))
	}
	return converge.Terminal(fmt.Errorf("stdshell: %s could not start interpreter %q: %w", name, interpreter, err))
}

// childEnv layers the kind config's baseline env, then the spec's env, on the
// worker's captured environment. Later entries win (os/exec resolves a duplicate
// key to the LAST occurrence), so precedence is worker < config < spec.
func childEnv(base []string, cfgEnv, specEnv map[string]string) []string {
	if len(cfgEnv) == 0 && len(specEnv) == 0 {
		return base
	}
	env := make([]string, 0, len(base)+len(cfgEnv)+len(specEnv))
	env = append(env, base...)
	for k, v := range cfgEnv {
		env = append(env, k+"="+v)
	}
	for k, v := range specEnv {
		env = append(env, k+"="+v)
	}
	return env
}

// firstNonEmpty returns the first non-empty string, or "" if all are empty.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// sanitize keeps a resource name safe for a temp-dir prefix (alnum/-/_ only), so a
// name with slashes or spaces can't escape or break the path.
func sanitize(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// tail returns the last max bytes of b (trimmed), prefixing "…" when truncated, so
// the fatal/tail portion (where the useful message usually is) is kept.
func tail(b []byte, max int) string {
	s := bytes.TrimSpace(b)
	if len(s) <= max {
		return string(s)
	}
	return "…" + string(s[len(s)-max:])
}
