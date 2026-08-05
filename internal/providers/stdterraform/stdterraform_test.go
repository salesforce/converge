package stdterraform

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/salesforce/converge/sdk-go/converge"
)

// s3cfg is a minimal valid S3-backend config for the handler-level tests.
func s3cfg() Config { return Config{Backend: BackendS3, S3: S3Backend{Bucket: "st"}} }

// newWorker builds a Provider with the given default config (no bundle) + script path.
func newWorker(t *testing.T, scriptPath string, cfg Config) *Provider {
	t.Helper()
	return newWorkerBundle(t, scriptPath, cfg, nil)
}

// newWorkerBundle builds a Provider with a default config AND a default bundle (the
// raw-HCL backend escape hatch), delivered via the OnConfig push seam exactly as the
// broker would — so the test exercises the same live-config read path as production.
func newWorkerBundle(t *testing.T, scriptPath string, cfg Config, bundle []byte) *Provider {
	t.Helper()
	doc, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{scriptPath: scriptPath}
	p.OnConfig(converge.ProviderConfig{Spec: doc, Data: bundle})
	return p
}

// req builds a work ReactionRequest for a terraform resource from a Spec.
func req(t *testing.T, spec Spec) converge.ReactionRequest {
	t.Helper()
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return converge.ReactionRequest{
		Reaction: "work",
		Resource: converge.Resource{ID: uuid.New(), Kind: Kind, Spec: raw},
		Env:      &converge.Env{},
	}
}

// stubScript writes a fake run.sh that records ALL its TF_* env to a file (KEY=VALUE
// per line) and, on apply, writes a canned outputs.json — so the handler exec path is
// exercised without a real terraform/aws/git on the box.
func stubScript(t *testing.T, body string) (scriptPath, capturePath string) {
	t.Helper()
	dir := t.TempDir()
	scriptPath = filepath.Join(dir, "run.sh")
	capturePath = filepath.Join(dir, "captured.env")
	full := "#!/usr/bin/env bash\nset -euo pipefail\n" +
		"env | grep '^TF_\\|^OUT_FILE=' | sort > " + capturePath + "\n" +
		body + "\n"
	if err := os.WriteFile(scriptPath, []byte(full), 0o755); err != nil {
		t.Fatal(err)
	}
	return scriptPath, capturePath
}

// ── handler-level tests (stub script) ────────────────────────────────────────

// TestApplyHappyPath: a stub that writes outputs.json yields Applied=true, mapped
// outputs, and a Ready=True condition.
func TestApplyHappyPath(t *testing.T) {
	script, _ := stubScript(t, `[ "$TF_ACTION" = apply ] && echo '{"bucket":{"value":"b-123"},"count":{"value":3}}' > "$OUT_FILE"`)
	w := newWorker(t, script, s3cfg())
	out, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz", Vars: map[string]string{"x": "1"}}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	var st Status
	if err := json.Unmarshal(out.Status, &st); err != nil {
		t.Fatalf("status not JSON: %s (%v)", out.Status, err)
	}
	if !st.Applied || st.Outputs["bucket"] != "b-123" || st.Outputs["count"] != "3" {
		t.Fatalf("status mismatch: %+v", st)
	}
	if len(out.Conditions) != 1 || out.Conditions[0].Status != converge.ConditionTrue {
		t.Fatalf("want one Ready=True condition, got %+v", out.Conditions)
	}
}

// TestDestroyStripsFinalizer: a successful destroy returns an empty Outcome (nil error)
// so the core strips the finalizer, and TF_ACTION=destroy reaches the script.
func TestDestroyStripsFinalizer(t *testing.T) {
	script, capture := stubScript(t, `true`)
	w := newWorker(t, script, s3cfg())
	r := req(t, Spec{Source: "git::https://h/r//mod?ref=v1"})
	r.Reaction = "teardown"
	out, err := w.destroy(t.Context(), r)
	if err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if out.Status != nil || len(out.Conditions) != 0 {
		t.Fatalf("destroy must return an empty Outcome, got %+v", out)
	}
	if got := captured(t, capture, "TF_ACTION"); got != "destroy" {
		t.Fatalf("destroy passed TF_ACTION=%q, want destroy", got)
	}
}

// TestStateKeyIsResourceUUID: the state key is keyed by the resource UUID (present on
// both work and delete tasks — the name is not), and includes the kind + prefix.
func TestStateKeyIsResourceUUID(t *testing.T) {
	script, capture := stubScript(t, `[ "$TF_ACTION" = apply ] && echo '{}' > "$OUT_FILE"`)
	w := newWorker(t, script, s3cfg())
	r := req(t, Spec{Source: "s3://b/m.tar.gz"})
	id := r.Resource.ID.String()
	if _, err := w.apply(t.Context(), r); err != nil {
		t.Fatalf("apply: %v", err)
	}
	wantKey := "terraform/stdterraform/" + id + ".tfstate"
	if got := captured(t, capture, "TF_STATE_KEY"); got != wantKey {
		t.Fatalf("state key = %q, want %q", got, wantKey)
	}
}

// TestS3BackendEnvWiring: backend=s3 passes the s3 fields + binary + sorted vars.
func TestS3BackendEnvWiring(t *testing.T) {
	script, capture := stubScript(t, `[ "$TF_ACTION" = apply ] && echo '{}' > "$OUT_FILE"`)
	cfg := Config{Backend: BackendS3, Binary: "tofu", S3: S3Backend{Bucket: "b", Region: "eu-west-1", Endpoint: "http://ls:4566"}}
	w := newWorker(t, script, cfg)
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz", Vars: map[string]string{"b": "2", "a": "1"}})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	checks := map[string]string{
		"TF_BACKEND": "s3", "TF_BIN": "tofu", "TF_S3_BUCKET": "b",
		"TF_S3_REGION": "eu-west-1", "TF_S3_ENDPOINT": "http://ls:4566", "TF_VARS": "a=1 b=2",
	}
	for k, want := range checks {
		if got := captured(t, capture, k); got != want {
			t.Fatalf("%s = %q, want %q", k, got, want)
		}
	}
}

// TestBackendDefaultsToS3: an unset backend defaults to s3, binary to terraform.
func TestBackendDefaultsToS3(t *testing.T) {
	script, capture := stubScript(t, `[ "$TF_ACTION" = apply ] && echo '{}' > "$OUT_FILE"`)
	w := newWorker(t, script, Config{S3: S3Backend{Bucket: "b"}}) // no backend / binary
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := captured(t, capture, "TF_BACKEND"); got != "s3" {
		t.Fatalf("default TF_BACKEND = %q, want s3", got)
	}
	if got := captured(t, capture, "TF_BIN"); got != "terraform" {
		t.Fatalf("default TF_BIN = %q, want terraform", got)
	}
}

// TestAzureBackendEnvWiring: backend=azurerm passes the azure fields.
func TestAzureBackendEnvWiring(t *testing.T) {
	script, capture := stubScript(t, `[ "$TF_ACTION" = apply ] && echo '{}' > "$OUT_FILE"`)
	cfg := Config{Backend: BackendAzureRM, AzureRM: AzureRMBackend{
		StorageAccount: "acct", Container: "state", ResourceGroup: "rg", UseAzureADAuth: true}}
	w := newWorker(t, script, cfg)
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	for k, want := range map[string]string{
		"TF_BACKEND": "azurerm", "TF_AZ_STORAGE_ACCOUNT": "acct",
		"TF_AZ_CONTAINER": "state", "TF_AZ_RESOURCE_GROUP": "rg", "TF_AZ_USE_AZUREAD": "true",
	} {
		if got := captured(t, capture, k); got != want {
			t.Fatalf("%s = %q, want %q", k, got, want)
		}
	}
}

// TestGCSBackendEnvWiring: backend=gcs passes the gcs bucket.
func TestGCSBackendEnvWiring(t *testing.T) {
	script, capture := stubScript(t, `[ "$TF_ACTION" = apply ] && echo '{}' > "$OUT_FILE"`)
	w := newWorker(t, script, Config{Backend: BackendGCS, GCS: GCSBackend{Bucket: "tf-state"}})
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := captured(t, capture, "TF_BACKEND"); got != "gcs" {
		t.Fatalf("TF_BACKEND = %q, want gcs", got)
	}
	if got := captured(t, capture, "TF_GCS_BUCKET"); got != "tf-state" {
		t.Fatalf("TF_GCS_BUCKET = %q, want tf-state", got)
	}
}

// TestScriptFailureIsTransient: a non-zero script exit (other than 2) is retryable
// (not terminal), with the output folded into the error.
func TestScriptFailureIsTransient(t *testing.T) {
	script, _ := stubScript(t, `echo "state lock held" >&2; exit 1`)
	w := newWorker(t, script, s3cfg())
	_, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"}))
	if err == nil {
		t.Fatal("a failing terraform run must error")
	}
	if converge.IsTerminal(err) {
		t.Fatalf("a terraform failure must be TRANSIENT, got terminal: %v", err)
	}
	if !strings.Contains(err.Error(), "state lock held") {
		t.Fatalf("script output not folded into error: %v", err)
	}
}

// TestScriptExit2IsTerminal: run.sh's `exit 2` (an unfixable config error — unknown
// backend, bad TF_BIN, unsupported scheme) is TERMINAL, so the engine stops retrying.
func TestScriptExit2IsTerminal(t *testing.T) {
	script, _ := stubScript(t, `echo "unknown backend" >&2; exit 2`)
	w := newWorker(t, script, s3cfg())
	_, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"}))
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("run.sh exit 2 must be TERMINAL, got %v", err)
	}
}

// TestBadSourceIsTerminal: an unset or unsupported source scheme is terminal.
func TestBadSourceIsTerminal(t *testing.T) {
	script, _ := stubScript(t, `true`)
	w := newWorker(t, script, s3cfg())
	for _, src := range []string{"", "ftp://x/y", "/local/path", "git@host:repo"} {
		_, err := w.apply(t.Context(), req(t, Spec{Source: src}))
		if err == nil || !converge.IsTerminal(err) {
			t.Fatalf("source %q must be terminal, got %v", src, err)
		}
	}
}

// TestAcceptedSourceSchemes: every supported source scheme validates.
func TestAcceptedSourceSchemes(t *testing.T) {
	for _, src := range []string{
		"s3://b/m.tar.gz",
		"gs://b/m.tar.gz",
		"azblob://acct/container/m.tar.gz",
		"https://h/m.tar.gz",
		"git::https://h/r//mod?ref=v1",
	} {
		if err := validateSource(src); err != nil {
			t.Fatalf("source %q should be accepted: %v", src, err)
		}
	}
}

// TestMissingBackendFieldIsTransient: a required backend field missing (config not
// applied yet) is transient per backend — it self-heals when the providerconfig lands.
func TestMissingBackendFieldIsTransient(t *testing.T) {
	script, _ := stubScript(t, `true`)
	cases := []Config{
		{Backend: BackendS3}, // no s3.bucket
		{Backend: BackendAzureRM, AzureRM: AzureRMBackend{Container: "c"}}, // no storage_account
		{Backend: BackendGCS}, // no gcs.bucket
	}
	for _, cfg := range cases {
		w := newWorker(t, script, cfg)
		_, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"}))
		if err == nil || converge.IsTerminal(err) {
			t.Fatalf("missing required field for %s must be TRANSIENT, got %v", cfg.Backend, err)
		}
	}
}

// TestUnknownBackendIsTerminal: a typo'd backend name can't be fixed by retry.
func TestUnknownBackendIsTerminal(t *testing.T) {
	script, _ := stubScript(t, `true`)
	w := newWorker(t, script, Config{Backend: "s33", S3: S3Backend{Bucket: "b"}})
	_, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"}))
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("unknown backend must be terminal, got %v", err)
	}
}

// TestUnknownSpecFieldIsTerminal: a typo'd spec key is rejected loudly.
func TestUnknownSpecFieldIsTerminal(t *testing.T) {
	script, _ := stubScript(t, `true`)
	w := newWorker(t, script, s3cfg())
	r := req(t, Spec{})
	r.Resource.Spec = json.RawMessage(`{"source":"s3://b/m.tar.gz","sourcee":"typo"}`)
	_, err := w.apply(t.Context(), r)
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("unknown spec field must be terminal, got %v", err)
	}
}

// ── raw-HCL backend bundle (escape hatch) ────────────────────────────────────

// TestBundleWinsAndSubstitutesKey: a raw backend.tf in the bundle is written verbatim
// with the per-resource state key substituted for the placeholder, and TF_BACKEND=custom.
func TestBundleWinsAndSubstitutesKey(t *testing.T) {
	// The stub records the backend override the Go side wrote into the workdir.
	dir := t.TempDir()
	script := filepath.Join(dir, "run.sh")
	captured := filepath.Join(dir, "backend.out")
	body := "#!/usr/bin/env bash\nset -euo pipefail\n" +
		"printf 'BACKEND=%s\\n' \"$TF_BACKEND\" > " + captured + "\n" +
		"cat zz_backend_override.tf >> " + captured + "\n" +
		"[ \"$TF_ACTION\" = apply ] && echo '{}' > \"$OUT_FILE\"\ntrue\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	bundle := []byte("terraform {\n  backend \"oss\" {\n    bucket = \"tf\"\n    key    = \"" + StateKeyPlaceholder + "\"\n  }\n}\n")
	w := newWorkerBundle(t, script, s3cfg(), bundle) // typed cfg present but bundle WINS
	r := req(t, Spec{Source: "s3://b/m.tar.gz"})
	id := r.Resource.ID.String()
	if _, err := w.apply(t.Context(), r); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got, err := os.ReadFile(captured)
	if err != nil {
		t.Fatal(err)
	}
	out := string(got)
	if !strings.Contains(out, "BACKEND=custom") {
		t.Fatalf("bundle path must set TF_BACKEND=custom; got:\n%s", out)
	}
	if !strings.Contains(out, `backend "oss"`) {
		t.Fatalf("custom backend HCL not used; got:\n%s", out)
	}
	wantKey := "terraform/stdterraform/" + id + ".tfstate"
	if !strings.Contains(out, wantKey) {
		t.Fatalf("placeholder not substituted with the per-resource key %q; got:\n%s", wantKey, out)
	}
	if strings.Contains(out, StateKeyPlaceholder) {
		t.Fatalf("placeholder still present after substitution; got:\n%s", out)
	}
}

// TestBundleMissingPlaceholderIsTerminal: a raw backend.tf without the per-resource
// placeholder would make all resources share one state file — a terminal misconfig.
func TestBundleMissingPlaceholderIsTerminal(t *testing.T) {
	script, _ := stubScript(t, `true`)
	bundle := []byte(`terraform { backend "s3" { bucket = "tf" key = "hardcoded.tfstate" } }`)
	w := newWorkerBundle(t, script, s3cfg(), bundle)
	_, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"}))
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("a bundle without the state-key placeholder must be terminal, got %v", err)
	}
}

// ── real run.sh backend generation (fake terraform on PATH) ──────────────────

// runHarness holds the capture-file paths a real-run.sh test asserts against.
type runHarness struct {
	backendOut string // the generated zz_backend_override.tf (copied out by fake terraform init)
	fetchLog   string // argv of the fetch CLI that ran (aws/gcloud/az)
	verLog     string // argv of the version manager (tfenv/tofuenv) — the resolved token
}

// realRunWorker builds a worker pointed at the ACTUAL embedded run.sh (materialised to
// a temp file), with fake CLIs on PATH: the version managers (tfenv/tofuenv) + their
// shims (terraform/tofu) + the fetch CLIs (aws/gcloud/az). The fakes record their argv
// to capture files so a test can assert the resolved version, the fetch dispatch, and
// the generated backend HCL — all without any real tooling or network.
func realRunWorker(t *testing.T, cfg Config, bundle []byte) (w *Provider, h runHarness) {
	t.Helper()
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "run.sh")
	if err := os.WriteFile(scriptPath, runScript, 0o755); err != nil {
		t.Fatal(err)
	}
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Version managers: record `<tfenv|tofuenv> <install|use> <token>` so a test can
	// assert the resolved version token. No-op otherwise (success).
	h.verLog = filepath.Join(dir, "version.log")
	verLine := "echo \"$(basename \"$0\") $*\" >> " + h.verLog + "\n"
	writeFake(t, binDir, "tfenv", "#!/usr/bin/env bash\n"+verLine+"exit 0\n")
	writeFake(t, binDir, "tofuenv", "#!/usr/bin/env bash\n"+verLine+"exit 0\n")

	// The `terraform`/`tofu` shims: on `init` copy the generated backend override out
	// for inspection; on `output` print empty JSON; else no-op. (Both stubbed so the
	// binary=tofu path works too.)
	h.backendOut = filepath.Join(dir, "backend.captured.tf")
	shim := "#!/usr/bin/env bash\ncase \"${1:-}\" in\n" +
		"  init) cp zz_backend_override.tf " + h.backendOut + " ;;\n" +
		"  output) echo '{}' ;;\nesac\nexit 0\n"
	writeFake(t, binDir, "terraform", shim)
	writeFake(t, binDir, "tofu", shim)

	// The fetch fakes: log the invocation, then write a valid empty tarball to the LAST
	// arg (aws/gcloud: the dest positional) or the --file value (az). Untarring an empty
	// tgz yields an empty module dir, which is all run.sh needs to proceed.
	h.fetchLog = filepath.Join(dir, "fetch.log")
	logLine := "echo \"$0 $*\" >> " + h.fetchLog + "\n"
	// aws s3 cp <uri> <dest>  (dest = last arg, robust to a leading --endpoint-url).
	writeFake(t, binDir, "aws", "#!/usr/bin/env bash\n"+logLine+"for a in \"$@\"; do dest=\"$a\"; done\ntar -czf \"$dest\" -T /dev/null\nexit 0\n")
	// gcloud storage cp <uri> <dest>  (dest = last arg).
	writeFake(t, binDir, "gcloud", "#!/usr/bin/env bash\n"+logLine+"for a in \"$@\"; do dest=\"$a\"; done\ntar -czf \"$dest\" -T /dev/null\nexit 0\n")
	// az storage blob download … --file <dest>  (find the value after --file).
	writeFake(t, binDir, "az", "#!/usr/bin/env bash\n"+logLine+
		"dest=bundle.tar.gz\nprev=\"\"\nfor a in \"$@\"; do [ \"$prev\" = --file ] && dest=\"$a\"; prev=\"$a\"; done\ntar -czf \"$dest\" -T /dev/null\nexit 0\n")

	doc, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	w = &Provider{scriptPath: scriptPath}
	w.OnConfig(converge.ProviderConfig{Spec: doc, Data: bundle})
	// Prepend our fake bin to PATH for the child process, and give tfenv/tofuenv a
	// writable LOCK_DIR (run.sh flocks the install there).
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("LOCK_DIR", dir)
	return w, h
}

func writeFake(t *testing.T, binDir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(binDir, name), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
}

// TestRealRunGeneratesS3WithLockfile: the actual run.sh emits an s3 backend block with
// native locking (use_lockfile = true) and the per-resource key + region.
func TestRealRunGeneratesS3WithLockfile(t *testing.T) {
	w, h := realRunWorker(t, Config{Backend: BackendS3, S3: S3Backend{Bucket: "tf-state", Region: "us-west-2"}}, nil)
	r := req(t, Spec{Source: "s3://b/m.tar.gz"})
	if _, err := w.apply(t.Context(), r); err != nil {
		t.Fatalf("apply: %v", err)
	}
	hcl := readFile(t, h.backendOut)
	assertContainsAll(t, "s3", hcl,
		`backend "s3"`, `bucket       = "tf-state"`, `use_lockfile = true`,
		`region       = "us-west-2"`, r.Resource.ID.String()+".tfstate")
	if strings.Contains(hcl, "dynamodb") {
		t.Fatalf("s3 backend must NOT reference dynamodb (native lockfile only):\n%s", hcl)
	}
}

// TestRealRunGeneratesS3LocalStackEndpoint: with an endpoint the s3 block adds the
// endpoints{} + skip_*/use_path_style flags for an S3-compatible store.
func TestRealRunGeneratesS3LocalStackEndpoint(t *testing.T) {
	w, h := realRunWorker(t, Config{Backend: BackendS3, S3: S3Backend{Bucket: "tf", Endpoint: "http://localstack:4566"}}, nil)
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	hcl := readFile(t, h.backendOut)
	assertContainsAll(t, "s3-localstack", hcl,
		`endpoints                   = { s3 = "http://localstack:4566" }`, `use_path_style              = true`)
}

// TestRealRunGeneratesAzureRM: the actual run.sh emits an azurerm block (locking is
// automatic — no lock argument), with the conditional resource_group + use_azuread_auth.
func TestRealRunGeneratesAzureRM(t *testing.T) {
	w, h := realRunWorker(t, Config{Backend: BackendAzureRM, AzureRM: AzureRMBackend{
		StorageAccount: "acct", Container: "state", ResourceGroup: "rg", UseAzureADAuth: true}}, nil)
	r := req(t, Spec{Source: "s3://b/m.tar.gz"})
	if _, err := w.apply(t.Context(), r); err != nil {
		t.Fatalf("apply: %v", err)
	}
	hcl := readFile(t, h.backendOut)
	assertContainsAll(t, "azurerm", hcl,
		`backend "azurerm"`, `storage_account_name = "acct"`, `container_name       = "state"`,
		`resource_group_name  = "rg"`, `use_azuread_auth     = true`, r.Resource.ID.String()+".tfstate")
}

// TestRealRunGeneratesGCS: the actual run.sh emits a gcs block using `prefix` (not key)
// — locking is automatic.
func TestRealRunGeneratesGCS(t *testing.T) {
	w, h := realRunWorker(t, Config{Backend: BackendGCS, GCS: GCSBackend{Bucket: "tf-state"}}, nil)
	r := req(t, Spec{Source: "s3://b/m.tar.gz"})
	if _, err := w.apply(t.Context(), r); err != nil {
		t.Fatalf("apply: %v", err)
	}
	hcl := readFile(t, h.backendOut)
	assertContainsAll(t, "gcs", hcl, `backend "gcs"`, `bucket = "tf-state"`, `prefix = "terraform/stdterraform/`)
	if strings.Contains(hcl, "key ") || strings.Contains(hcl, "key=") {
		t.Fatalf("gcs backend must use prefix, not key:\n%s", hcl)
	}
}

// ── source fetch dispatch (real run.sh, fake fetch CLIs) ─────────────────────

// TestRealRunFetchesGCS: a gs:// source routes to `gcloud storage cp gs://… bundle.tar.gz`.
func TestRealRunFetchesGCS(t *testing.T) {
	w, h := realRunWorker(t, s3cfg(), nil) // any valid backend; we assert the FETCH
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "gs://mods/hello.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	log := readFile(t, h.fetchLog)
	if !strings.Contains(log, "gcloud") || !strings.Contains(log, "storage cp gs://mods/hello.tar.gz") {
		t.Fatalf("gs:// source should invoke `gcloud storage cp`; fetch log:\n%s", log)
	}
	if strings.Contains(log, "\naws ") {
		t.Fatalf("gs:// source must NOT invoke aws; fetch log:\n%s", log)
	}
}

// TestRealRunFetchesAzblob: an azblob://acct/container/blob source routes to
// `az storage blob download` with the account/container/blob parsed out of the URI.
func TestRealRunFetchesAzblob(t *testing.T) {
	w, h := realRunWorker(t, s3cfg(), nil)
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "azblob://acct/state/path/to/hello.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	log := readFile(t, h.fetchLog)
	for _, want := range []string{
		"az storage blob download", "--account-name acct",
		"--container-name state", "--name path/to/hello.tar.gz", "--auth-mode login",
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("azblob:// fetch missing %q; fetch log:\n%s", want, log)
		}
	}
}

// TestRealRunAzblobMalformedIsError: an azblob URI missing the container/blob is a
// misconfiguration a retry can't fix — TERMINAL (rejected in validateSource, before exec).
func TestRealRunAzblobMalformedIsError(t *testing.T) {
	w, _ := realRunWorker(t, s3cfg(), nil)
	_, err := w.apply(t.Context(), req(t, Spec{Source: "azblob://acct"})) // no /container/blob
	if err == nil || !converge.IsTerminal(err) {
		t.Fatalf("a malformed azblob:// source must be TERMINAL, got %v", err)
	}
}

// ── version resolution via tfenv/tofuenv (real run.sh, fake managers) ────────

// TestRealRunAutoLatestAllowedWhenConstrained: with no spec pin / bundle version file,
// and a module that DECLARES required_version, run.sh resolves "latest-allowed" (the
// newest the constraint permits) via tfenv (binary=terraform).
func TestRealRunAutoLatestAllowedWhenConstrained(t *testing.T) {
	w, h := realRunWorker(t, s3cfg(), nil)
	// Fetch fake drops a module .tf that declares required_version on its OWN indented
	// line (real HCL formatting — which run.sh's anchored, comment-excluding grep matches).
	binDir := filepath.Dir(mustLook(t, "aws"))
	writeFake(t, binDir, "aws", "#!/usr/bin/env bash\nfor a in \"$@\"; do dest=\"$a\"; done\ntar -czf \"$dest\" -T /dev/null\nprintf 'terraform {\\n  required_version = \"~> 1.7.0\"\\n}\\n' > main.tf\nexit 0\n")
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	log := readFile(t, h.verLog)
	for _, want := range []string{"tfenv install latest-allowed", "tfenv use latest-allowed"} {
		if !strings.Contains(log, want) {
			t.Fatalf("a constrained module should resolve latest-allowed; version log:\n%s", log)
		}
	}
	if strings.Contains(log, "tofuenv") {
		t.Fatalf("binary=terraform must use tfenv, not tofuenv; version log:\n%s", log)
	}
}

// TestRealRunAutoLatestWhenUnconstrained: a module with NO required_version resolves
// "latest" (NOT latest-allowed — tfenv/tofuenv error on an empty constraint, so run.sh
// picks latest itself). This is the demo's case (its sample module declares none).
func TestRealRunAutoLatestWhenUnconstrained(t *testing.T) {
	w, h := realRunWorker(t, s3cfg(), nil) // realRunWorker's aws writes an empty module (no .tf)
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	log := readFile(t, h.verLog)
	for _, want := range []string{"tfenv install latest", "tfenv use latest"} {
		if !strings.Contains(log, want) {
			t.Fatalf("an unconstrained module should resolve latest; version log:\n%s", log)
		}
	}
	if strings.Contains(log, "latest-allowed") {
		t.Fatalf("no required_version → must use latest, NOT latest-allowed; version log:\n%s", log)
	}
}

// TestRealRunAutoIgnoresCommentedRequiredVersion: a COMMENTED-OUT required_version is
// not a real declaration → run.sh resolves "latest" (not latest-allowed). Guards the
// anchored, comment-excluding grep.
func TestRealRunAutoIgnoresCommentedRequiredVersion(t *testing.T) {
	w, h := realRunWorker(t, s3cfg(), nil)
	binDir := filepath.Dir(mustLook(t, "aws"))
	// The module only MENTIONS required_version in a comment — must NOT count.
	writeFake(t, binDir, "aws", "#!/usr/bin/env bash\nfor a in \"$@\"; do dest=\"$a\"; done\ntar -czf \"$dest\" -T /dev/null\nprintf '# required_version = \">= 1.0\" (example — not active)\\nresource \"null_resource\" \"x\" {}\\n' > main.tf\nexit 0\n")
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	log := readFile(t, h.verLog)
	if strings.Contains(log, "latest-allowed") {
		t.Fatalf("a commented-out required_version must be ignored → latest, not latest-allowed; version log:\n%s", log)
	}
	if !strings.Contains(log, "tfenv install latest") {
		t.Fatalf("expected latest; version log:\n%s", log)
	}
}

// TestRealRunSpecPinWins: spec.tf_version is installed+used verbatim.
func TestRealRunSpecPinWins(t *testing.T) {
	w, h := realRunWorker(t, s3cfg(), nil)
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz", TFVersion: "1.7.5"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	log := readFile(t, h.verLog)
	for _, want := range []string{"tfenv install 1.7.5", "tfenv use 1.7.5"} {
		if !strings.Contains(log, want) {
			t.Fatalf("spec.tf_version=1.7.5 should install+use 1.7.5; version log:\n%s", log)
		}
	}
	if strings.Contains(log, "latest-allowed") {
		t.Fatalf("a spec pin must NOT fall back to latest-allowed; version log:\n%s", log)
	}
}

// TestRealRunBundleVersionFileHonored: when the fetched bundle already ships a
// .terraform-version and the spec sets no pin, run.sh honors it (installs its
// contents) instead of overwriting with latest-allowed. We simulate the bundle by
// re-stubbing the fetch fake to drop the file into the workdir.
func TestRealRunBundleVersionFileHonored(t *testing.T) {
	w, h := realRunWorker(t, s3cfg(), nil)
	binDir := filepath.Dir(mustLook(t, "aws"))
	writeFake(t, binDir, "aws", "#!/usr/bin/env bash\nfor a in \"$@\"; do dest=\"$a\"; done\ntar -czf \"$dest\" -T /dev/null\nprintf '1.6.6\\n' > .terraform-version\nexit 0\n")
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	log := readFile(t, h.verLog)
	if !strings.Contains(log, "tfenv install 1.6.6") || !strings.Contains(log, "tfenv use 1.6.6") {
		t.Fatalf("a bundle-shipped .terraform-version (1.6.6) should be honored; version log:\n%s", log)
	}
	if strings.Contains(log, "latest-allowed") {
		t.Fatalf("must NOT overwrite the bundle version file with latest-allowed; version log:\n%s", log)
	}
}

// TestRealRunTofuUsesTofuenv: binary=tofu resolves through tofuenv (not tfenv).
func TestRealRunTofuUsesTofuenv(t *testing.T) {
	w, h := realRunWorker(t, Config{Backend: BackendS3, Binary: "tofu", S3: S3Backend{Bucket: "st"}}, nil)
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	log := readFile(t, h.verLog)
	// Empty module (no required_version) → auto resolves "latest" via tofuenv.
	if !strings.Contains(log, "tofuenv install latest") || !strings.Contains(log, "tofuenv use latest") {
		t.Fatalf("binary=tofu should resolve via tofuenv; version log:\n%s", log)
	}
	if strings.Contains(log, "tfenv ") {
		t.Fatalf("binary=tofu must NOT use tfenv; version log:\n%s", log)
	}
}

// TestVersionEnvWiring (handler-level, stub script): spec.tf_version reaches run.sh as
// TF_VERSION; empty spec → empty TF_VERSION (run.sh then auto-resolves).
func TestVersionEnvWiring(t *testing.T) {
	script, capture := stubScript(t, `[ "$TF_ACTION" = apply ] && echo '{}' > "$OUT_FILE"`)
	w := newWorker(t, script, s3cfg())
	if _, err := w.apply(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz", TFVersion: "1.8.2"})); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := captured(t, capture, "TF_VERSION"); got != "1.8.2" {
		t.Fatalf("TF_VERSION = %q, want 1.8.2", got)
	}
}

// mustLook returns the path of a fake bin realRunWorker placed on the child PATH.
func mustLook(t *testing.T, name string) string {
	t.Helper()
	for _, d := range filepath.SplitList(os.Getenv("PATH")) {
		p := filepath.Join(d, name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Fatalf("fake %q not found on PATH", name)
	return ""
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v (the fake terraform's init should have copied the override)", p, err)
	}
	return string(b)
}

func assertContainsAll(t *testing.T, label, hcl string, subs ...string) {
	t.Helper()
	for _, s := range subs {
		if !strings.Contains(hcl, s) {
			t.Fatalf("[%s] generated backend HCL missing %q:\n%s", label, s, hcl)
		}
	}
}

// ── misc ──────────────────────────────────────────────────────────────────────

// TestServesFixedKind: the provider serves exactly the single fixed kind "stdterraform"
// at web-API version 1.
func TestServesFixedKind(t *testing.T) {
	got := (&Provider{}).Kind()
	if got.Kind != Kind || got.Version != 1 || Kind != "stdterraform" {
		t.Fatalf("kind = %v, want {stdterraform 1}", got)
	}
}

// TestWorkDispatchesOnReaction: Work routes the `work` reaction to apply (spec change →
// Applied status) and the `teardown` reaction to destroy (empty Outcome so the finalizer
// is stripped). An unknown reaction is a wiring error a retry can't fix → TERMINAL.
func TestWorkDispatchesOnReaction(t *testing.T) {
	script, capture := stubScript(t, `[ "$TF_ACTION" = apply ] && echo '{}' > "$OUT_FILE"; true`)
	p := newWorker(t, script, s3cfg())

	// work → apply.
	if _, err := p.Work(t.Context(), req(t, Spec{Source: "s3://b/m.tar.gz"})); err != nil {
		t.Fatalf("work reaction: %v", err)
	}
	if got := captured(t, capture, "TF_ACTION"); got != "apply" {
		t.Fatalf("work reaction ran TF_ACTION=%q, want apply", got)
	}

	// teardown → destroy (empty Outcome).
	r := req(t, Spec{Source: "s3://b/m.tar.gz"})
	r.Reaction = "teardown"
	out, err := p.Work(t.Context(), r)
	if err != nil {
		t.Fatalf("teardown reaction: %v", err)
	}
	if out.Status != nil || len(out.Conditions) != 0 {
		t.Fatalf("teardown must return an empty Outcome, got %+v", out)
	}
	if got := captured(t, capture, "TF_ACTION"); got != "destroy" {
		t.Fatalf("teardown reaction ran TF_ACTION=%q, want destroy", got)
	}

	// an unknown reaction is terminal.
	r.Reaction = "bogus"
	if _, err := p.Work(t.Context(), r); err == nil || !converge.IsTerminal(err) {
		t.Fatalf("unknown reaction must be terminal, got %v", err)
	}
}

// TestReadOutputsFlattening: string outputs are unquoted; complex ones keep JSON.
func TestReadOutputsFlattening(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "o.json")
	if err := os.WriteFile(p, []byte(`{"s":{"value":"hi"},"n":{"value":7},"l":{"value":["a","b"]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readOutputs(p)
	if err != nil {
		t.Fatalf("readOutputs: %v", err)
	}
	if got["s"] != "hi" || got["n"] != "7" || got["l"] != `["a","b"]` {
		t.Fatalf("flattened outputs mismatch: %+v", got)
	}
}

// captured reads the value for KEY from the stub's `env`-dump capture file.
func captured(t *testing.T, path, key string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read capture: %v", err)
	}
	for line := range strings.SplitSeq(string(raw), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok && k == key {
			return v
		}
	}
	return ""
}
