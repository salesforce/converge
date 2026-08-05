package store

import "errors"

// errors.go is the store's exported ERROR CONTRACT: the sentinel errors callers
// (chiefly the API's HTTP error-mapping layer) match with errors.Is to turn a
// data-layer outcome into the right status. Kept in ONE place so the whole
// vocabulary is discoverable — a caller asking "what can the store return?" reads
// this file, not the bottom of each query file. Each notes how the API maps it.

// ErrConflict is returned when a concurrent apply lost a unique/PK race even
// after one retry. The API maps it to 409 rather than a raw 500.
var ErrConflict = errors.New("apply conflict: concurrent write to the same resource")

// ErrInvalidSpec wraps a schema-validation failure from the store's validator
// (the data-gateway check in ApplySpec / ApplyComposeResult). The API maps it to
// 422; the composer path treats it as terminal. The wrapped error is the typed
// *specschema.Error carrying the per-field details.
var ErrInvalidSpec = errors.New("spec failed schema validation")

// ErrComposeClaimLost is returned by ApplyComposeResult when the fenced
// composed_gen stamp matches 0 rows — this pod no longer owns the composer root
// claim at the composed generation (reaped / reassigned mid-compose, e.g. a
// network partition that lapsed the heartbeat). The caller rolls the whole
// compose tx back; the reassigned pod's compose is authoritative. NOT a real
// failure (no recordFailed / failure_gen stamp) — it's a benign lost race, so
// the dispatch path just returns without writing a result.
var ErrComposeClaimLost = errors.New("compose claim lost mid-flight")

// ErrConfigNotFound is returned when an Apply's provider_config_ref names a
// providerconfigs row that doesn't exist. The API maps it to 422.
var ErrConfigNotFound = errors.New("provider_config not found")

// ErrResourceNotFound is returned by a (kind, name) lookup when no such resource
// exists — DISTINCT from a transient read error, so the API maps this to 404 and a
// genuine DB fault to 500 (never conflating "absent" with "broken").
var ErrResourceNotFound = errors.New("resource not found")

// ErrConfigKindMismatch is returned when a provider_config_ref names a config
// whose (kind, kindVersion) differs from the resource's — a config parameterises
// exactly one (kind, kindVersion), so a v1 resource can only carry a v1 config and a
// v2 resource a v2 config. The API maps it to 422.
var ErrConfigKindMismatch = errors.New("provider_config is for a different (kind, kindVersion)")

// ErrConfigDefaultExists is returned when upserting a default config for a kind
// that already has a (differently-named) default (uq_providerconfigs_default_per_kind).
// The API maps it to 409.
var ErrConfigDefaultExists = errors.New("kind already has a default provider_config")

// ErrOperationNotEnqueued is returned by CreateOperationRow when the operate task
// could not be enqueued: the resource is frozen (quarantined/orphaned → 0 selected
// rows) or an operate task is already queued for it (ON CONFLICT no-op). The whole
// tx rolls back so no orphaned resource_operations row is left with no backing task.
// The API maps it to 409 (a verb is already in flight / the resource is set aside).
var ErrOperationNotEnqueued = errors.New("operate task not enqueued (resource frozen or a verb already queued)")

// ErrKindVersionInUse is returned by DeleteKindManifest when a (kind, kindVersion)
// still has references that make it unsafe to delete: live resources pinned to it,
// or a reactor binding that EXACTLY pins it (as watched kind or as the reactor that
// runs). An UNPINNED binding on the kind does NOT block — it resolves to the
// remaining versions. The API maps this to 409/422 with the count-bearing message.
var ErrKindVersionInUse = errors.New("kind version is in use")
