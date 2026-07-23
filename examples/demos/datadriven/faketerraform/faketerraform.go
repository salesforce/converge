// Package faketerraform is a simulation leaf that fakes "run a team's Terraform
// bundle from S3". Its spec input is an S3 URL to a *.tar (the team's packaged
// Terraform); on reconcile it pretends to download + `terraform apply` the bundle
// and writes deterministic fake outputs to its status — an apply_id and an
// image_tag the apply "produced".
//
// It's the FIRST half of the data-driven demo's second DAG: a fakek8sjob downstream
// depends on this terraform's status.image_tag (the engine flows it into the job's
// spec), so the job "runs the image the Terraform built" — exercising a value-flow
// edge between two team-applied kinds. Teams apply it DECLARATIVELY: a celbom rule
// or a stdstarlark policy emits a faketerraform child with the team's tar_url baked
// in (a literal in the policy), so adding "run this team's Terraform" to the org is
// a config edit, not a code change.
//
// It imports only sdk/* — no internal/* — exactly what an external team ships.
package faketerraform

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Kind is the kind this provider serves; its CRD lives in
// testfixtures/faketerraform.kind.json.
const Kind converge.Kind = "faketerraform"

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). A pure simulation leaf, so it needs
// no config and is always ready. Imports only sdk/* — no internal/*.
type Provider struct{}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = Provider{}

// Kind is the single (kind, version) this provider serves.
func (Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// Work fakes a deterministic `terraform apply` of the spec's tar bundle and writes
// the apply outputs to status — the one reaction (spec change → status) this kind
// declares. Pure: no env, no I/O (the S3 download + terraform run are simulated).
func (Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	return worker{}.React(ctx, req)
}

// OnConfig is a no-op: this leaf has no default providerconfig.
func (Provider) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure simulation leaf has no downstream to dial.
func (Provider) Ready() bool { return true }

// Spec is faketerraform's desired state: the S3 URL of the team's *.tar Terraform
// bundle to "apply". A composer stamps it as a literal from the team's policy.
type Spec struct {
	TarURL string   `json:"tar_url" doc:"S3 URL to the team's Terraform bundle (*.tar), e.g. s3://team-bundles/base.tar."`
	_      struct{} `additionalProperties:"true"`
}

// Status is the fake `terraform apply` result. ImageTag is the field a downstream
// fakek8sjob consumes via a ValueFlow (the image the apply "built/published").
type Status struct {
	ApplyID  string `json:"apply_id"`  // deterministic fake apply id derived from the bundle
	ImageTag string `json:"image_tag"` // the image the apply "produced" — flowed to a fakek8sjob
}

// worker carries the reaction body; Provider.Work delegates to it so the pure apply
// logic stays one cohesive unit (and the package's unit tests drive it directly).
type worker struct{}

func (worker) React(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	var spec Spec
	if len(req.Resource.Spec) > 0 {
		if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
			return converge.Outcome{}, converge.Terminal(fmt.Errorf("faketerraform: decode spec: %w", err))
		}
	}
	// Validate the input the way a real runner would reject a bad bundle ref. Terminal
	// (not transient): a missing/mis-shaped tar_url is a config error a same-generation
	// retry can't fix — make it loud instead of spinning.
	if spec.TarURL == "" {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("faketerraform: spec.tar_url is required (S3 URL to the team's *.tar bundle)"))
	}
	if !strings.HasPrefix(spec.TarURL, "s3://") {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("faketerraform: tar_url %q must be an s3:// URL", spec.TarURL))
	}
	if !strings.HasSuffix(spec.TarURL, ".tar") {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("faketerraform: tar_url %q must point at a *.tar bundle", spec.TarURL))
	}
	// Fake a deterministic apply: same bundle → same apply_id + image_tag across runs.
	// bundleName is the tar's basename without extension (e.g. "base" from
	// s3://team-bundles/base.tar) — the demo's stand-in for a real apply output.
	bundle := bundleName(spec.TarURL)
	out, err := json.Marshal(Status{
		ApplyID:  "apply-" + bundle,
		ImageTag: "registry.internal/" + bundle + ":built",
	})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

// bundleName extracts the tar's basename without the .tar extension from an s3 URL,
// e.g. "s3://team-bundles/base.tar" → "base". Falls back to the trimmed path.
func bundleName(tarURL string) string {
	p := strings.TrimSuffix(tarURL, ".tar")
	if i := strings.LastIndex(p, "/"); i >= 0 {
		p = p[i+1:]
	}
	if p == "" {
		return "bundle"
	}
	return p
}
