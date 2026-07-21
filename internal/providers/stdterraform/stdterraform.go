// Package stdterraform is a REAL leaf provider that runs a user's Terraform module as
// a converge resource: its spec points at a module (a *.tar.gz in S3, GCS, Azure Blob,
// or over HTTPS, or a git repo), and per task the worker fetches it, injects a state
// backend keyed to the
// resource, and runs a real `terraform apply` (or `terraform destroy` on delete). The
// module's terraform outputs become the resource's status, so a downstream resource
// can value-flow a created ARN/ID out of it.
//
// It is the generic "make any Terraform module a converge resource" kind — author
// no Go per module, just point a resource at the HCL. This mirrors how Terraform
// Enterprise/Cloud works: the user's module ships NO backend block; the platform owns
// state placement + locking centrally (the kind's providerconfig, configured once by
// the operator) and gives each unit of work its OWN isolated, auto-locked state. A
// converge terraform RESOURCE is a TFE workspace (one state, one lock, serialized
// reconciles); the module source + vars are its workspace inputs.
//
// State backend (multi-cloud): the providerconfig picks a backend and the worker
// generates the override with that cloud's NATIVE locking:
//   - s3      → `use_lockfile = true` (a .tflock object; no DynamoDB; needs ≥1.10).
//   - azurerm → automatic (blob lease).
//   - gcs     → automatic (atomic lock object).
//
// An escape hatch: the operator may instead drop a raw backend.tf in the providerconfig
// `data` bundle (any backend, any args) with a StateKeyPlaceholder where the per-
// resource key goes; the worker substitutes it. State is ALWAYS keyed by the resource
// UUID, so many resources share one backend location without clobbering.
//
// Lifecycle:
//   - work     (specChange)     → fetch + `terraform apply`; outputs → status.
//   - teardown (deleteRequested)→ fetch + `terraform destroy`; the finalizer is
//     stripped on success, so the real cloud infra is torn down BEFORE the row is
//     removed (a genuine IaC delete, not just forgetting the resource).
//
// The fetch + backend-inject + init/apply/destroy/output steps are a small embedded
// bash script (run.sh) the worker shells out to, so the worker needs no cloud/IaC/git
// Go SDK — only the terraform (or tofu) CLI plus the tools for the chosen backend/
// source on PATH (aws, az, gcloud, git, curl — its image ships what it uses). The Go
// here validates the spec+config, wires the env, execs the script, and parses outputs
// into status. It imports only sdk/* — no internal/*, no cloud SDK.
package stdterraform

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/salesforce/converge/sdk-go/converge"
)

// exitTerminal is the run.sh exit code that signals a NON-retryable failure — a bad
// spec/config the same generation can't fix (unsupported source scheme, malformed
// azblob URI, a `..` subdir, an unknown backend, a bad binary). run.sh uses `exit 2`
// for exactly these; any other non-zero exit (terraform/cloud/git failures) is
// transient. Matches the stdio + stdshell providers' convention so all three behave
// alike. (terraform CLI errors exit 1 — no -detailed-exitcode is used — so mapping 2
// → Terminal never wrongly terminalizes a genuine transient apply/destroy failure.)
const exitTerminal = 2

// runScript is the bundled work/teardown step (run.sh), materialised once when the
// Provider is built and exec'd per task. Embedding keeps the worker self-contained:
// the image supplies the CLIs, the binary supplies the orchestration script.
//
//go:embed run.sh
var runScript []byte

// reactionWork applies the module (the specChange reaction); reactionTeardown
// destroys it (the deleteRequested reaction). These are the two reaction names the
// stdterraform kind's manifest declares; Work switches on req.Reaction between them.
const (
	reactionWork     = "work"
	reactionTeardown = "teardown"
)

// defaultBinary is the Terraform CLI used when the config names none.
const defaultBinary = "terraform"

// defaultStatePrefix namespaces state objects when the config sets no state_prefix.
const defaultStatePrefix = "terraform/"

// StateKeyPlaceholder is the token a raw backend.tf (supplied via the providerconfig
// bundle) must put where the per-resource state key/path goes; the worker substitutes
// the resource-derived key at apply time. The bundle is kind-wide (identical for every
// resource), so without this every resource of the kind would share one state file.
// Exported so a demo/doc can reference the exact string. Kept underscore-wrapped so it
// can't collide with a real HCL identifier.
const StateKeyPlaceholder = "__CONVERGE_STATE_KEY__"

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). Imports only sdk/* — no internal/*.
//
// It holds the per-worker state gathered when the Provider is built and the kind's
// live default providerconfig:
//   - scriptPath: the embedded run.sh materialised once to a temp file, exec'd per
//     task (the image supplies the CLIs; the binary supplies the orchestration script).
//   - defaultCfg / bundle (guarded by mu): the kind's LIVE default providerconfig
//     `spec` and `data`, the latest OnConfig push. Work reads through them per task via
//     converge.EffectiveConfig / converge.EffectiveBundle so an operator's
//     state-backend (or raw-HCL) edit lands with no restart.
//
// It is a leaf that shells out to run.sh per task, so it has no downstream to dial and
// is always Ready — which binary to run (terraform vs tofu) is work-time config, and a
// missing CLI fails the individual task transiently rather than shedding the kind.
type Provider struct {
	scriptPath string // the embedded run.sh materialised to a temp file

	mu          sync.RWMutex    // guards defaultSpec + defaultData (OnConfig writes, Work reads)
	defaultSpec json.RawMessage // latest pushed default config `spec` (nil until first push)
	// defaultData is the kind's live default providerconfig `data` (a raw backend.tf, when
	// the operator uses the raw-HCL escape hatch); nil when unused. Read per task.
	defaultData []byte
}

// New builds a Provider, materialising the embedded run.sh once to a temp file. It
// panics if the script cannot be written (a process that can't write its own embedded
// asset to a tempdir cannot serve the kind at all — fail loudly at construction rather
// than transiently per task). The dumb worker registers the returned *Provider; the
// in-process host runs it through Runtime.
func New() *Provider {
	scriptPath := filepath.Join(os.TempDir(), "converge-stdterraform-run.sh")
	if err := os.WriteFile(scriptPath, runScript, 0o755); err != nil {
		panic(fmt.Errorf("stdterraform: write run script: %w", err))
	}
	return &Provider{scriptPath: scriptPath}
}

// compile-time proof it satisfies the one provider contract. A *Provider is the
// receiver because OnConfig mutates guarded state.
var _ converge.Provider = (*Provider)(nil)

// Kind is the single (kind, version) this provider serves — the fixed kind
// "stdterraform" at web-API version 1 (explicit; there is no implicit default).
func (p *Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// OnConfig stores the kind's latest default providerconfig under the guard so Work can
// read through to it per task. stdterraform uses BOTH config axes: cfg.Spec is the
// typed state-backend config, cfg.Data is the optional raw-HCL backend.tf escape hatch.
// An empty cfg (a deleted default) clears both sources.
func (p *Provider) OnConfig(cfg converge.ProviderConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.defaultSpec = cfg.Spec
	p.defaultData = cfg.Data
}

// Ready is always true: a leaf that shells out to run.sh has no downstream to dial, so
// it can always do work (a missing CLI fails the individual task transiently instead).
func (p *Provider) Ready() bool { return true }

// defaults returns the current default-config `spec` + bundle `data` bytes under the read
// guard, so a concurrent OnConfig push and a Work read never race on the fields.
func (p *Provider) defaults() (spec json.RawMessage, data []byte) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.defaultSpec, p.defaultData
}

// Work runs one task, routing on the reaction the core selected: the `work` reaction
// applies the module (spec change → status), the `teardown` reaction destroys it
// (delete requested → strip the finalizer). Any other reaction is a manifest/wiring
// error a retry can't fix, so it is TERMINAL.
func (p *Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	switch req.Reaction {
	case reactionWork:
		return p.apply(ctx, req)
	case reactionTeardown:
		return p.destroy(ctx, req)
	default:
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("stdterraform: unknown reaction %q (want %q or %q)", req.Reaction, reactionWork, reactionTeardown))
	}
}

// apply runs `terraform apply` for the work reaction: validate the spec, resolve the
// effective config, fetch+init+apply in a per-task temp dir, and parse outputs into
// status. A bad spec/config is TERMINAL (a same-generation retry can't fix it); a
// transient fetch/terraform failure returns a plain error so the engine retries.
func (p *Provider) apply(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	spec, cfg, backendHCL, err := p.resolve(req)
	if err != nil {
		return converge.Outcome{}, err
	}

	dir, outFile, cleanup, err := workdir(req)
	if err != nil {
		return converge.Outcome{}, err
	}
	defer cleanup()

	if err := p.exec(ctx, "apply", spec, cfg, backendHCL, req, dir, outFile); err != nil {
		return converge.Outcome{}, err
	}

	outputs, err := readOutputs(outFile)
	if err != nil {
		return converge.Outcome{}, fmt.Errorf("stdterraform: read outputs: %w", err)
	}
	status, err := json.Marshal(Status{Outputs: outputs, Applied: true})
	if err != nil {
		return converge.Outcome{}, fmt.Errorf("stdterraform: encode status: %w", err)
	}
	logInfo(req, "terraform apply succeeded", "source", spec.Source, "outputs", len(outputs))
	return converge.Outcome{
		Status: status,
		Conditions: []converge.Condition{{
			Type:   converge.TypeReady,
			Status: converge.ConditionTrue,
			Reason: "Applied",
		}},
	}, nil
}

// destroy runs `terraform destroy` for the teardown reaction: fetch the same module,
// init against the resource's state, and destroy. On success it returns an empty
// Outcome so the core strips the finalizer and removes the row (the real infra is
// gone first). A destroy failure returns a plain error → the reaper retries the
// teardown, holding the resource in Deleting; the finalizer is NOT stripped, so we
// never drop a row whose cloud resources still exist.
func (p *Provider) destroy(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	spec, cfg, backendHCL, err := p.resolve(req)
	if err != nil {
		return converge.Outcome{}, err
	}

	dir, _, cleanup, err := workdir(req)
	if err != nil {
		return converge.Outcome{}, err
	}
	defer cleanup()

	if err := p.exec(ctx, "destroy", spec, cfg, backendHCL, req, dir, ""); err != nil {
		return converge.Outcome{}, err
	}
	logInfo(req, "terraform destroy succeeded (finalizer will be stripped)", "source", spec.Source)
	return converge.Outcome{}, nil
}

// resolve decodes+validates the spec and computes the effective config + backend, and
// resolves the effective raw-HCL bundle (per-resource override else kind default).
// Returns a TERMINAL error for a bad spec/backend config (unfixable by retry) and a
// TRANSIENT error when the providerconfig is not applied yet (self-heals as the
// ConfigSource picks it up live). The returned []byte is the raw backend.tf when the
// escape hatch is used (nil for the typed-backend path).
func (p *Provider) resolve(req converge.ReactionRequest) (Spec, Config, []byte, error) {
	var spec Spec
	if len(req.Resource.Spec) > 0 {
		if err := decodeStrict(req.Resource.Spec, &spec); err != nil {
			return Spec{}, Config{}, nil, converge.Terminal(fmt.Errorf("stdterraform: decode spec: %w", err))
		}
	}
	if err := validateSource(spec.Source); err != nil {
		return Spec{}, Config{}, nil, converge.Terminal(err)
	}

	defaultSpec, defaultData := p.defaults()
	cfg := converge.EffectiveConfig[Config](defaultSpec, req.Env.ProviderConfig).withDefaults()
	backendHCL := converge.EffectiveBundle(defaultData, req.Env.ProviderBundle)

	if len(backendHCL) > 0 {
		// Escape hatch: a raw backend.tf. It MUST carry the per-resource placeholder,
		// else every resource of the kind would clobber one state file — a terminal
		// misconfiguration (the same bytes will never grow the placeholder on retry).
		if !bytes.Contains(backendHCL, []byte(StateKeyPlaceholder)) {
			return Spec{}, Config{}, nil, converge.Terminal(fmt.Errorf(
				"stdterraform: the providerconfig backend bundle must contain the per-resource state-key placeholder %q (else all resources share one state file)", StateKeyPlaceholder))
		}
		return spec, cfg, backendHCL, nil
	}

	// Typed-backend path: validate the selected backend's required fields. An empty/
	// unset required field is TRANSIENT (the providerconfig may not be applied yet — a
	// task claimed in the boot-vs-config window; the engine retries as the config lands).
	if err := validateBackend(cfg); err != nil {
		return Spec{}, Config{}, nil, err
	}
	return spec, cfg, nil, nil
}

// exec shells out to run.sh for one action (apply|destroy), wiring the module source,
// the per-resource state key, the backend selection + fields (or a raw backend.tf), and
// vars via env. A non-zero exit is TRANSIENT (terraform / cloud / git failures are
// often recoverable — a retry with backoff is correct), with the combined output folded
// into the error for diagnosis.
//
// Credentials are NEVER passed here: run.sh + terraform + the cloud CLIs read them from
// each cloud's standard chain (AWS_* / ARM_* / GOOGLE_*) that os.Environ() carries into
// the child (env / instance profile / workload identity).
func (p *Provider) exec(ctx context.Context, action string, spec Spec, cfg Config, backendHCL []byte, req converge.ReactionRequest, dir, outFile string) error {
	// State key is per resource — keyed by the STABLE resource UUID (the name is not
	// carried on a reconcile/delete task) so apply and destroy address the same state,
	// and resources of the kind never collide (each is its own state + its own lock).
	stateKey := cfg.StatePrefix + string(req.Resource.Kind) + "/" + req.Resource.ID.String() + ".tfstate"

	env := append(os.Environ(),
		"TF_BIN="+cfg.Binary,
		"TF_ACTION="+action,
		"TF_SOURCE="+spec.Source,
		"TF_STATE_KEY="+stateKey,
		"TF_VARS="+varFlags(spec.Vars),
		"TF_WORKDIR="+dir,
		"OUT_FILE="+outFile,
		// TF_VERSION: the tfenv/tofuenv token to pin (spec.tf_version). Empty = auto
		// (bundle .terraform-version/.opentofu-version, else latest-allowed) — run.sh
		// resolves + installs the version through the matching version manager.
		"TF_VERSION="+spec.TFVersion,
	)
	if len(backendHCL) > 0 {
		// Escape hatch: write the operator's raw backend.tf with the per-resource key
		// substituted for the placeholder, and tell run.sh to use it as-is.
		rendered := bytes.ReplaceAll(backendHCL, []byte(StateKeyPlaceholder), []byte(stateKey))
		backendPath := filepath.Join(dir, "zz_backend_override.tf")
		if err := os.WriteFile(backendPath, rendered, 0o600); err != nil {
			return fmt.Errorf("stdterraform: write backend override: %w", err)
		}
		env = append(env, "TF_BACKEND=custom")
	} else {
		// Typed-backend path: run.sh generates the block. Pass the selected backend +
		// its fields; empty vars are simply absent from the generated block.
		env = append(env,
			"TF_BACKEND="+string(cfg.Backend),
			"TF_STATE_PREFIX="+cfg.StatePrefix,
			"TF_S3_BUCKET="+cfg.S3.Bucket,
			"TF_S3_REGION="+cfg.S3.Region,
			"TF_S3_ENDPOINT="+cfg.S3.Endpoint,
			"TF_AZ_STORAGE_ACCOUNT="+cfg.AzureRM.StorageAccount,
			"TF_AZ_CONTAINER="+cfg.AzureRM.Container,
			"TF_AZ_RESOURCE_GROUP="+cfg.AzureRM.ResourceGroup,
			"TF_AZ_USE_AZUREAD="+boolEnv(cfg.AzureRM.UseAzureADAuth),
			"TF_GCS_BUCKET="+cfg.GCS.Bucket,
		)
	}

	cmd := exec.CommandContext(ctx, "bash", p.scriptPath)
	cmd.Dir = dir
	cmd.Env = env
	if out, err := cmd.CombinedOutput(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("stdterraform: %s %s cancelled/timed out: %w", action, req.Resource.ID, ctxErr)
		}
		// run.sh exits 2 for an unfixable config error (bad source/backend/binary) — a
		// same-generation retry can't fix it, so surface it TERMINAL. Any other non-zero
		// exit (terraform/cloud/git failure) stays TRANSIENT and the engine retries.
		var ee *exec.ExitError
		if errors.As(err, &ee) && ee.ExitCode() == exitTerminal {
			return converge.Terminal(fmt.Errorf("stdterraform: %s %s failed terminally (exit %d): %w\n%s", action, req.Resource.ID, exitTerminal, err, out))
		}
		return fmt.Errorf("stdterraform: %s %s failed: %w\n%s", action, req.Resource.ID, err, out)
	}
	return nil
}

// withDefaults fills the backend/prefix/binary defaults + the s3 region default so the
// script always gets a concrete backend, state namespace, and CLI name.
func (c Config) withDefaults() Config {
	if c.Backend == "" {
		c.Backend = BackendS3
	}
	if c.StatePrefix == "" {
		c.StatePrefix = defaultStatePrefix
	}
	if c.Binary == "" {
		c.Binary = defaultBinary
	}
	if c.S3.Region == "" {
		c.S3.Region = "us-east-1"
	}
	return c
}

// validateBackend checks the selected typed backend's required fields are present.
// A missing required field is TRANSIENT (the providerconfig may not be applied yet);
// an UNKNOWN backend name is TERMINAL (a same-generation retry can't fix a typo).
func validateBackend(cfg Config) error {
	switch cfg.Backend {
	case BackendS3:
		if cfg.S3.Bucket == "" {
			return fmt.Errorf("stdterraform: providerconfig not applied yet (s3.bucket empty); will retry")
		}
	case BackendAzureRM:
		if cfg.AzureRM.StorageAccount == "" || cfg.AzureRM.Container == "" {
			return fmt.Errorf("stdterraform: providerconfig not applied yet (azurerm.storage_account/container empty); will retry")
		}
	case BackendGCS:
		if cfg.GCS.Bucket == "" {
			return fmt.Errorf("stdterraform: providerconfig not applied yet (gcs.bucket empty); will retry")
		}
	default:
		return converge.Terminal(fmt.Errorf("stdterraform: unknown backend %q (want s3, azurerm, or gcs)", cfg.Backend))
	}
	return nil
}

// boolEnv renders a bool as the "true"/"" run.sh treats as set/unset.
func boolEnv(b bool) string {
	if b {
		return "true"
	}
	return ""
}

// validateSource checks the spec.Source is present and a supported scheme. Returns a
// plain error (the caller wraps it Terminal) since a bad source can't be fixed by a
// same-generation retry.
func validateSource(src string) error {
	if src == "" {
		return fmt.Errorf("stdterraform: spec.source is required")
	}
	switch {
	case strings.HasPrefix(src, "azblob://"):
		// azblob://<account>/<container>/<blob-path> — require all three segments so a
		// malformed URI fails TERMINAL here (in resolve) rather than exit-2'ing in run.sh.
		rest := strings.TrimPrefix(src, "azblob://")
		parts := strings.SplitN(rest, "/", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			return fmt.Errorf("stdterraform: azblob source must be azblob://<account>/<container>/<blob-path> — got %q", src)
		}
		return nil
	case strings.HasPrefix(src, "s3://"),
		strings.HasPrefix(src, "gs://"),
		strings.HasPrefix(src, "https://"),
		strings.HasPrefix(src, "git::"):
		return nil
	default:
		return fmt.Errorf("stdterraform: spec.source must be s3://…tar.gz, gs://…tar.gz, azblob://acct/container/…tar.gz, https://…tar.gz, or git::https://… — got %q", src)
	}
}

// workdir creates a per-task working dir + outputs path, returning a cleanup that
// removes the dir on return so a worker accrues no state.
func workdir(req converge.ReactionRequest) (dir, outFile string, cleanup func(), err error) {
	dir, err = os.MkdirTemp("", "terraform-"+sanitize(req.Resource.ID.String())+"-")
	if err != nil {
		return "", "", func() {}, fmt.Errorf("stdterraform: temp dir: %w", err)
	}
	return dir, filepath.Join(dir, "outputs.json"), func() { _ = os.RemoveAll(dir) }, nil
}

// varFlags renders spec.Vars as the space-separated "k=v" list run.sh splits into
// -var flags, in deterministic key order.
func varFlags(vars map[string]string) string {
	if len(vars) == 0 {
		return ""
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + "=" + vars[k]
	}
	return strings.Join(parts, " ")
}

// readOutputs parses `terraform output -json` (name → {value,…}) into a flat
// name → string map, JSON-encoding non-string values so any output shape round-trips
// into status.
func readOutputs(path string) (map[string]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc map[string]struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decode terraform outputs: %w", err)
	}
	out := make(map[string]string, len(doc))
	for name, o := range doc {
		var s string
		if json.Unmarshal(o.Value, &s) == nil {
			out[name] = s // plain string output: unquoted
		} else {
			out[name] = strings.TrimSpace(string(o.Value)) // complex: keep JSON
		}
	}
	return out, nil
}

// sanitize keeps an identifier safe for a temp-dir prefix (alnum/-/_ only).
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// logInfo logs via the task env's logger when present (nil-safe for tests).
func logInfo(req converge.ReactionRequest, msg string, kv ...any) {
	if req.Env != nil && req.Env.Logger != nil {
		req.Env.Logger.Info(msg, kv...)
	}
}
