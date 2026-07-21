package noop

import "github.com/salesforce/converge/sdk-go/converge"

// Kind is the kind name registered by this provider.
const Kind converge.Kind = "noop"

// NoopSpec is the no-op kind's desired state. It carries nothing the worker
// consumes; the blank `_` field with additionalProperties:"true" lets a
// caller POST arbitrary extra keys without failing huma's server-side
// validation (which defaults to additionalProperties:false).
type NoopSpec struct {
	_ struct{} `additionalProperties:"true"`
}

// NoopStatus is what the no-op worker writes after its delay. Empty by
// design — there is no external state to report.
type NoopStatus struct{}
