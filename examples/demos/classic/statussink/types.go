package statussink

import "github.com/salesforce/converge/sdk-go/converge"

// Kind is the REACTOR kind handle a reactor_bindings row names in its `reactor`
// column. It is NOT a resource kind — statussink has no resource in the graph;
// it is a pure side-effect sink the ReactorDispatcher invokes when a binding
// matches a lifecycle transition.
const Kind converge.Kind = "statussink"

// ReactionName is the single `reactor`-trigger reaction this kind declares. The
// handler is registered under it (runtime()) and the CRD names it; the claim
// resolves it from the CRD at delivery. One const so the map key, the CRD, and
// any test can't drift.
const ReactionName = "react"

// StatusSinkConfig is the statussink CONFIG document — the type the kind's
// manifest carries as its config_schema, carried by its DEFAULT providerconfig
// (named "statussink"). The worker pulls this default via GetProviderConfig like
// any kind. The type name doubles as the /docs schema component name
// (#/schemas/StatusSinkConfig), so it's spelled out rather than a bare "Config".
// It mirrors fakeevent's two-tier split:
//
//   - Endpoint is BOOTSTRAP: the object-store location the sink DIALS once in
//     Setup (think the S3 bucket endpoint / a base URL). A missing/invalid value
//     fails Setup, so the kind stays PENDING until a config supplies it — then
//     the ProviderRetrier registers it live, no restart.
//   - Prefix is WORK-TIME: the key prefix each uploaded object lands under,
//     read per delivery and live-editable (a default-config edit re-points it).
//
// A real reactor would carry {bucket, region, prefix} here and dial an
// aws-sdk-go-v2 *s3.Client in Setup; the framework imports no AWS SDK — the
// client lives in this provider package's closure, exactly like fakeevent's
// broker.
type StatusSinkConfig struct {
	Endpoint string `json:"endpoint,omitempty" doc:"Object-store endpoint dialed once at startup (BOOTSTRAP). A missing/invalid value fails Setup; the kind stays pending until a default config supplies it."`
	Prefix   string `json:"prefix,omitempty" doc:"Key prefix uploaded objects land under (WORK-TIME, live-editable)."`
}
