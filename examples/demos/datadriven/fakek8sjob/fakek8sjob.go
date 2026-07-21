// Package fakek8sjob is a simulation leaf that fakes "run a team's Kubernetes Job".
// Its spec input is a Docker image (repository + tag), e.g. "nginx:1.27"; on
// reconcile it pretends to submit a Job running that image and writes a deterministic
// fake Job status (a job name + the image it ran).
//
// It's the SECOND half of the data-driven demo's second DAG: it depends on a
// faketerraform upstream and the engine flows that apply's status.image_tag into
// this job's spec.built_image — so the Job "runs the image the team's Terraform
// built". A fakek8sjob therefore has BOTH a literal input (image, baked into the
// policy) AND a flowed input (built_image, from the upstream apply); Work records
// which it ran, preferring the freshly-built image. Teams apply it DECLARATIVELY via
// a celbom rule / stdstarlark policy.
//
// It imports only sdk/* — no internal/* — exactly what an external team ships.
package fakek8sjob

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/salesforce/converge/sdk-go/converge"
)

// Kind is the kind this provider serves; its CRD lives in
// testfixtures/fakek8sjob.kind.json.
const Kind converge.Kind = "fakek8sjob"

// Provider is the worker-side logic: it implements converge.Provider (the ONE SDK
// provider contract — Kind/Work/OnConfig/Ready). A pure simulation leaf, so it needs
// no config and is always ready. Imports only sdk/* — no internal/*.
type Provider struct{}

// compile-time proof it satisfies the one provider contract.
var _ converge.Provider = Provider{}

// Kind is the single (kind, version) this provider serves.
func (Provider) Kind() converge.KindVersion { return converge.KindVersion{Kind: Kind, Version: 1} }

// Work fakes submitting the Job: it decodes the spec, picks the image to run
// (preferring the freshly built image flowed from an upstream apply over the literal),
// and writes a deterministic fake Job status — the one reaction (spec change → status)
// this kind declares.
func (Provider) Work(ctx context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	return worker{}.React(ctx, req)
}

// OnConfig is a no-op: this leaf has no default providerconfig.
func (Provider) OnConfig(converge.ProviderConfig) {}

// Ready is always true: a pure simulation leaf has no downstream to dial.
func (Provider) Ready() bool { return true }

// Spec is fakek8sjob's desired state. Image is the literal Docker image+tag the team
// sets in its policy. BuiltImage is flowed in from an upstream faketerraform's
// status.image_tag (a composer ValueFlow into /built_image), so the composer emits
// it ABSENT — child validation skips the flowed path. If both are present the freshly
// BUILT image wins (you run what the apply just produced).
type Spec struct {
	Image      string   `json:"image" doc:"Docker image to run (repository:tag), e.g. nginx:1.27. The literal input set in the team's policy."`
	BuiltImage string   `json:"built_image" doc:"Image flowed in from an upstream faketerraform apply (status.image_tag); preferred over Image when present."`
	_          struct{} `additionalProperties:"true"`
}

// Status records the fake Job result — a green status means the Job "ran".
type Status struct {
	Job      string `json:"job"`       // deterministic fake Job name
	RanImage string `json:"ran_image"` // the image the Job actually ran (built wins over literal)
}

// worker is the reaction body, kept as its own type so the pure spec→status logic is
// unit-testable in isolation; Provider.Work delegates to it.
type worker struct{}

func (worker) React(_ context.Context, req converge.ReactionRequest) (converge.Outcome, error) {
	var spec Spec
	if len(req.Resource.Spec) > 0 {
		if err := json.Unmarshal(req.Resource.Spec, &spec); err != nil {
			return converge.Outcome{}, converge.Terminal(fmt.Errorf("fakek8sjob: decode spec: %w", err))
		}
	}
	// Prefer the freshly built image flowed from the upstream apply; fall back to the
	// literal image the policy set.
	image := spec.BuiltImage
	if image == "" {
		image = spec.Image
	}
	// A Job with no image to run is a config error — terminal, not a spin. (If a
	// built_image flow was declared but didn't deliver, the dep-gate guarantees this
	// handler only runs after it did — so an empty image here is genuinely missing
	// input, not a propagation race.)
	if image == "" {
		return converge.Outcome{}, converge.Terminal(fmt.Errorf("fakek8sjob: no image to run (set spec.image, or wire a faketerraform built_image flow)"))
	}
	out, err := json.Marshal(Status{
		Job:      "job-" + jobName(image),
		RanImage: image,
	})
	if err != nil {
		return converge.Outcome{}, err
	}
	return converge.Outcome{Status: out}, nil
}

// jobName derives a stable, k8s-name-ish token from an image ref by taking the
// repository's last path segment + replacing ':' so "registry.internal/base:built"
// → "base-built". Deterministic: same image → same job name across runs.
func jobName(image string) string {
	n := image
	if i := strings.LastIndex(n, "/"); i >= 0 {
		n = n[i+1:]
	}
	n = strings.ReplaceAll(n, ":", "-")
	if n == "" {
		return "job"
	}
	return n
}
