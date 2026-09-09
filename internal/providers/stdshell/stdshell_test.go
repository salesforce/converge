package stdshell

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/salesforce/converge/sdk-go/converge"
)

// newWorker builds a Provider seeded with the given default config (delivered via the
// OnConfig push seam, exactly as the broker would) and the captured process environ.
func newWorker(t *testing.T, cfg Config) *Provider {
	t.Helper()
	doc, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := New()
	p.OnConfig(converge.ProviderConfig{Spec: doc})
	return p
}

// req builds a work ReactionRequest for a shell resource from a Spec.
func req(t *testing.T, spec Spec) converge.ReactionRequest {
	t.Helper()
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return converge.ReactionRequest{
		Reaction: "work",
		Trigger:  converge.TriggerSpecChange,
		Resource: converge.Resource{Kind: Kind, Name: "x", Spec: raw},
		Env:      &converge.Env{},
	}
}

// TestScriptSucceeds: a zero-exit script yields a Ready=True condition and captures
// stdout into status.
func TestScriptSucceeds(t *testing.T) {
	w := newWorker(t, Config{})
	out, err := w.Work(t.Context(), req(t, Spec{Script: `echo "hello world"`}))
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	var st Status
	if err := json.Unmarshal(out.Status, &st); err != nil {
		t.Fatalf("status not JSON: %s (%v)", out.Status, err)
	}
	if st.Stdout != "hello world" {
		t.Fatalf("stdout = %q, want %q", st.Stdout, "hello world")
	}
	if len(out.Conditions) != 1 || out.Conditions[0].Type != converge.TypeReady ||
		out.Conditions[0].Status != converge.ConditionTrue {
		t.Fatalf("want one Ready=True condition, got %+v", out.Conditions)
	}
}

// TestMissingScriptIsTerminal: an empty script is a misconfiguration a retry can't fix.
func TestMissingScriptIsTerminal(t *testing.T) {
	w := newWorker(t, Config{})
	_, err := w.Work(t.Context(), req(t, Spec{Script: "   \n\t "}))
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("empty script must be a terminal error, got %v", err)
	}
}

// TestExit1IsTransient: a plain non-zero exit is retryable, and stderr is folded in.
func TestExit1IsTransient(t *testing.T) {
	w := newWorker(t, Config{})
	_, err := w.Work(t.Context(), req(t, Spec{Script: `echo "boom" >&2; exit 1`}))
	if err == nil {
		t.Fatal("exit 1 must fail")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("exit 1 must be TRANSIENT, got terminal: %v", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("stderr not folded into error: %v", err)
	}
}

// TestExit2IsTerminal: exit code 2 is the opt-in terminal signal.
func TestExit2IsTerminal(t *testing.T) {
	w := newWorker(t, Config{})
	_, err := w.Work(t.Context(), req(t, Spec{Script: `echo "bad" >&2; exit 2`}))
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("exit 2 must be terminal, got %v", err)
	}
}

// TestTimeoutIsTransient: a run that overruns the per-resource timeout is killed and
// reported transient (the run was cut short, not the script's verdict). It must also
// return PROMPTLY — the whole point of the process-group kill is that a script's
// child (`sleep`) can't keep the output pipe open and pin the worker slot for the
// child's full lifetime. Assert React returns well before the 30s sleep would end.
func TestTimeoutIsTransient(t *testing.T) {
	w := newWorker(t, Config{})
	start := time.Now()
	_, err := w.Work(t.Context(), req(t, Spec{Script: `sleep 30`, TimeoutSeconds: 1}))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a timed-out run must fail")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("timeout must be TRANSIENT, got terminal: %v", err)
	}
	// 1s timeout + waitDelay backstop, with headroom — and far under the 30s sleep.
	if elapsed > 10*time.Second {
		t.Fatalf("timed-out run took %s; the process-group kill should free the slot promptly", elapsed)
	}
}

// TestCtxCancelIsTransient: a cancelled task ctx (drain/lease loss) is transient.
func TestCtxCancelIsTransient(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // already cancelled
	w := newWorker(t, Config{})
	_, err := w.Work(ctx, req(t, Spec{Script: `sleep 30`}))
	if err == nil || converge.IsTerminal(err) {
		t.Fatalf("cancelled ctx must be a transient error, got %v", err)
	}
}

// TestSpecEnvOverridesConfigEnv: precedence is worker < config < spec on a key clash.
func TestSpecEnvOverridesConfigEnv(t *testing.T) {
	w := newWorker(t, Config{Env: map[string]string{"V": "from-config", "ONLY_CFG": "c"}})
	out, err := w.Work(t.Context(), req(t, Spec{
		Script: `printf '%s|%s' "$V" "$ONLY_CFG"`,
		Env:    map[string]string{"V": "from-spec"},
	}))
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	var st Status
	if err := json.Unmarshal(out.Status, &st); err != nil {
		t.Fatal(err)
	}
	if st.Stdout != "from-spec|c" {
		t.Fatalf("env precedence wrong: stdout = %q, want %q", st.Stdout, "from-spec|c")
	}
}

// TestInterpreterFromConfig: the kind config's default_interpreter is used when the
// spec sets none.
func TestInterpreterFromConfig(t *testing.T) {
	w := newWorker(t, Config{DefaultInterpreter: "sh"})
	out, err := w.Work(t.Context(), req(t, Spec{Script: `echo ok`}))
	if err != nil {
		t.Fatalf("React with sh interpreter: %v", err)
	}
	var st Status
	_ = json.Unmarshal(out.Status, &st)
	if st.Stdout != "ok" {
		t.Fatalf("stdout = %q, want ok", st.Stdout)
	}
}

// TestMissingInterpreterIsTerminal: a nonexistent interpreter can't be summoned by a
// retry — terminal.
func TestMissingInterpreterIsTerminal(t *testing.T) {
	w := newWorker(t, Config{})
	_, err := w.Work(t.Context(), req(t, Spec{Script: `echo hi`, Interpreter: "this-interpreter-does-not-exist-xyz"}))
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("missing interpreter must be terminal, got %v", err)
	}
}

// TestUnknownSpecFieldIsTerminal: a typo'd/stale spec key is rejected loudly rather
// than silently ignored.
func TestUnknownSpecFieldIsTerminal(t *testing.T) {
	w := newWorker(t, Config{})
	raw := json.RawMessage(`{"script":"echo hi","scriptt":"typo"}`)
	r := converge.ReactionRequest{
		Reaction: "work",
		Resource: converge.Resource{Kind: Kind, Name: "x", Spec: raw},
		Env:      &converge.Env{},
	}
	_, err := w.Work(t.Context(), r)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("unknown spec field must be terminal, got %v", err)
	}
}

// TestServesFixedKind: the provider serves exactly the single fixed kind "stdshell"
// (no env var to rename it) at web-API version 1.
func TestServesFixedKind(t *testing.T) {
	got := New().Kind()
	if got.Kind != Kind || got.Version != 1 || Kind != "stdshell" {
		t.Fatalf("kind = %v, want {stdshell 1}", got)
	}
}

// TestOutputIsTailBounded: a script that floods stdout has its captured status
// truncated to maxOutputBytes (plus the ellipsis marker).
func TestOutputIsTailBounded(t *testing.T) {
	w := newWorker(t, Config{})
	// Emit far more than maxOutputBytes of 'a'.
	out, err := w.Work(t.Context(), req(t, Spec{Script: `yes a | head -c 100000 | tr -d '\n'`}))
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	var st Status
	_ = json.Unmarshal(out.Status, &st)
	if len(st.Stdout) > maxOutputBytes+len("…") {
		t.Fatalf("stdout not tail-bounded: got %d bytes, cap ~%d", len(st.Stdout), maxOutputBytes)
	}
	if !strings.HasPrefix(st.Stdout, "…") {
		t.Fatalf("truncated stdout should be marked with an ellipsis prefix")
	}
}
