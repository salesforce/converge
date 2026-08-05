package stdio

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/salesforce/converge/sdk-go/converge"
)

// newWorker builds a worker whose command is `bash -c <body>` via a default config
// — the inline-script stand-in for a real handler program.
func newWorker(t *testing.T, body string) worker {
	t.Helper()
	return workerCmd(t, Config{Command: "bash", Args: []string{"-c", body}})
}

// workerCmd builds a worker from an explicit launch Config.
func workerCmd(t *testing.T, cfg Config) worker {
	t.Helper()
	doc, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return worker{
		defaultSpec: doc,
		environ:     os.Environ(),
	}
}

func req(reaction, kind, spec string) converge.ReactionRequest {
	return converge.ReactionRequest{
		Reaction: reaction,
		Trigger:  converge.TriggerSpecChange,
		Resource: converge.Resource{Kind: converge.Kind(kind), Name: "x", Spec: json.RawMessage(spec)},
		Env:      &converge.Env{},
	}
}

// TestWorkStatus: a work program that echoes a Response on stdout has its status
// mapped into the Outcome; exit 0 is success.
func TestWorkStatus(t *testing.T) {
	w := newWorker(t, `echo '{"status":{"ok":true,"n":42}}'`)
	out, err := w.React(t.Context(), req("work", string(Kind), `{"run":"true"}`))
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	var got struct {
		OK bool `json:"ok"`
		N  int  `json:"n"`
	}
	if err := json.Unmarshal(out.Status, &got); err != nil {
		t.Fatalf("status not JSON: %s (%v)", out.Status, err)
	}
	if !got.OK || got.N != 42 {
		t.Fatalf("status mismatch: %+v", got)
	}
}

// TestEmptyStdoutIsEmptyOutcome: a pure side-effect program that prints nothing is
// a valid success with an empty Outcome.
func TestEmptyStdoutIsEmptyOutcome(t *testing.T) {
	w := newWorker(t, `exit 0`)
	out, err := w.React(t.Context(), req("work", string(Kind), `{}`))
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	if out.Status != nil || len(out.Conditions) != 0 {
		t.Fatalf("empty stdout must map to empty Outcome, got %+v", out)
	}
}

// TestExit2IsTerminal: a program exiting with code 2 fails TERMINALLY (no retry),
// and its stderr is folded into the error.
func TestExit2IsTerminal(t *testing.T) {
	w := newWorker(t, `echo "bad spec" >&2; exit 2`)
	_, err := w.React(t.Context(), req("work", string(Kind), `{}`))
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("exit 2 must be TERMINAL, got %v", err)
	}
	if !strings.Contains(err.Error(), "bad spec") {
		t.Fatalf("stderr not folded into error: %v", err)
	}
}

// TestOtherExitIsTransient: any other non-zero exit is TRANSIENT (retried).
func TestOtherExitIsTransient(t *testing.T) {
	w := newWorker(t, `echo "flaky" >&2; exit 1`)
	_, err := w.React(t.Context(), req("work", string(Kind), `{}`))
	if err == nil {
		t.Fatal("exit 1 must fail")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("exit 1 must be TRANSIENT, got terminal: %v", err)
	}
}

// TestBadJSONResponseIsTerminal: non-JSON stdout from a program is a terminal
// protocol error a retry can't fix.
func TestBadJSONResponseIsTerminal(t *testing.T) {
	w := newWorker(t, `echo 'not json'`)
	_, err := w.React(t.Context(), req("work", string(Kind), `{}`))
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("non-JSON stdout must be TERMINAL, got %v", err)
	}
}

// TestNoCommandIsTerminal: a worker with neither a config command nor STDIO_COMMAND
// fails terminally.
func TestNoCommandIsTerminal(t *testing.T) {
	w := worker{defaultSpec: json.RawMessage(`{}`), environ: os.Environ()}
	_, err := w.React(t.Context(), req("work", string(Kind), `{}`))
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("no command must be TERMINAL, got %v", err)
	}
}

// TestComposeChildrenAndEdges: a compose program's children + edges are mapped into
// the Outcome, and value flows survive the round-trip.
func TestComposeChildrenAndEdges(t *testing.T) {
	w := newWorker(t, `cat <<'EOF'
{"children":[
  {"kind":"stdio","kind_version":1,"name":"c1","spec":{"run":"true"}},
  {"kind":"stdio","kind_version":1,"name":"c2","spec":{"run":"true"}}
],
"edges":[
  {"from":{"kind":"stdio","name":"c2"},"to":{"kind":"stdio","name":"c1"},
   "values":[{"dependent_field":"/in","source_field":"/out"}]}
],
"status":{"composed":2}}
EOF`)
	out, err := w.React(t.Context(), req("compose", string(ComposerKind), `{}`))
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	if len(out.Children) != 2 {
		t.Fatalf("want 2 children, got %d", len(out.Children))
	}
	if out.Children[0].Kind != "stdio" || out.Children[0].KindVersion != 1 || out.Children[0].Name != "c1" {
		t.Fatalf("child[0] mismatch (kind/kind_version/name): %+v", out.Children[0])
	}
	if len(out.Edges) != 1 {
		t.Fatalf("want 1 edge, got %d", len(out.Edges))
	}
	e := out.Edges[0]
	if e.From.Name != "c2" || e.To.Name != "c1" {
		t.Fatalf("edge endpoints mismatch: %+v", e)
	}
	if len(e.Values) != 1 || e.Values[0].DependentField != "/in" || e.Values[0].SourceField != "/out" {
		t.Fatalf("value flow mismatch: %+v", e.Values)
	}
}

// TestWorkReactionDropsChildren: a WORK reaction that (misbehaving) returns children
// must not shape a graph — decodeResponse only maps children for compose.
func TestWorkReactionDropsChildren(t *testing.T) {
	w := newWorker(t, `echo '{"status":{},"children":[{"kind":"stdio","kind_version":1,"name":"sneaky","spec":{}}]}'`)
	out, err := w.React(t.Context(), req("work", string(Kind), `{}`))
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	if len(out.Children) != 0 {
		t.Fatalf("a work reaction must not emit children, got %d", len(out.Children))
	}
}

// TestStdinCarriesRequest: the program receives the task as JSON on stdin with the
// spec passed through verbatim — proving the request envelope round-trips.
func TestStdinCarriesRequest(t *testing.T) {
	// The program reads the Request from stdin and re-emits it as its status, so the
	// test can assert the spec was forwarded. jq keeps stdout valid JSON.
	w := newWorker(t, `jq -c '{status: {echoed: .spec}}'`)
	out, err := w.React(t.Context(), req("work", string(Kind), `{"run":"marker-123"}`))
	if err != nil {
		t.Fatalf("React: %v", err)
	}
	if !strings.Contains(string(out.Status), "marker-123") {
		t.Fatalf("spec not forwarded to stdin: %s", out.Status)
	}
}

// TestTimeoutKillsRun: a program that overruns the config timeout is killed and the
// task fails transiently (ctx-driven), not terminally.
func TestTimeoutKillsRun(t *testing.T) {
	w := workerCmd(t, Config{Command: "sleep", Args: []string{"30"}, TimeoutSeconds: 1})
	start := time.Now()
	_, err := w.React(context.Background(), req("work", string(Kind), `{}`))
	if err == nil {
		t.Fatal("overrunning program must fail")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("a timeout is TRANSIENT (retryable), got terminal: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout did not kill the run promptly: %s", elapsed)
	}
}

// TestServedKindsDefaults: with neither env var set, the worker serves the built-in
// default leaf + composer kinds (zero-config demo/quick-start path).
func TestServedKindsDefaults(t *testing.T) {
	t.Setenv(envKinds, "")
	t.Setenv(envComposerKinds, "")
	leaf, composer := servedKinds()
	if len(leaf) != 1 || leaf[0] != Kind {
		t.Fatalf("default leaf = %v, want [%s]", leaf, Kind)
	}
	if len(composer) != 1 || composer[0] != ComposerKind {
		t.Fatalf("default composer = %v, want [%s]", composer, ComposerKind)
	}
}

// TestServedKindsFromEnv: named kinds override the defaults, trimmed + de-duped in
// order, and Providers() covers the union.
func TestServedKindsFromEnv(t *testing.T) {
	t.Setenv(envKinds, " backups , dns ,backups")
	t.Setenv(envComposerKinds, "app-bom")
	leaf, composer := servedKinds()
	if want := []converge.Kind{"backups", "dns"}; !equalKinds(leaf, want) {
		t.Fatalf("leaf = %v, want %v (trimmed + de-duped, order preserved)", leaf, want)
	}
	if want := []converge.Kind{"app-bom"}; !equalKinds(composer, want) {
		t.Fatalf("composer = %v, want %v", composer, want)
	}
	if got := providedKinds(); !equalKinds(got, []converge.Kind{"backups", "dns", "app-bom"}) {
		t.Fatalf("Providers() kinds = %v, want the union", got)
	}
}

// providedKinds is the leaf+composer kind NAMES the current env resolves to, read off
// Providers() (one single-pair provider per served kind) in registration order.
func providedKinds() []converge.Kind {
	ps := Providers()
	out := make([]converge.Kind, len(ps))
	for i, p := range ps {
		out[i] = p.Kind().Kind
	}
	return out
}

// TestServedKindsLeafOnly: setting only STDIO_KINDS yields a leaf-only worker (no
// composer) — the defaults do NOT leak back in once either var is set.
func TestServedKindsLeafOnly(t *testing.T) {
	t.Setenv(envKinds, "backups")
	t.Setenv(envComposerKinds, "")
	leaf, composer := servedKinds()
	if !equalKinds(leaf, []converge.Kind{"backups"}) || len(composer) != 0 {
		t.Fatalf("leaf-only: leaf=%v composer=%v", leaf, composer)
	}
}

// TestProvidersAdvertisesEveryServedKindAtV1: Providers() yields one single-pair provider
// per leaf + composer kind at version 1 (leaf vs composer is a program concern told via
// req.Reaction, not a distinct handler registration — Work routes both).
func TestProvidersAdvertisesEveryServedKindAtV1(t *testing.T) {
	t.Setenv(envKinds, "backups")
	t.Setenv(envComposerKinds, "app-bom")
	seen := map[converge.Kind]int{}
	for _, p := range Providers() {
		kv := p.Kind()
		seen[kv.Kind] = kv.Version
	}
	if seen["backups"] != 1 || seen["app-bom"] != 1 {
		t.Fatalf("Providers() kinds = %v, want backups/v1 + app-bom/v1", seen)
	}
}

func equalKinds(a, b []converge.Kind) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
