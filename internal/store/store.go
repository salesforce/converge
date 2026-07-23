// Package store wraps the sqlc-generated dbq package with the
// higher-level operations the rest of the codebase uses: sorted+typed
// bulk upserts, provider-friendly task abstractions, polymorphic
// outbox append, soft-delete primitives.
package store

import (
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/specschema"
)

// Store is the persistence boundary the rest of the system speaks to.
type Store struct {
	db dbq.DBTX
	// validator checks a spec against its kind's declared schema at the data
	// gateway (ApplySpec, ApplyComposeResult), so EVERY author — the HTTP API, a
	// custom ingestion duty, a reactor chaining a resource, the composer emitting
	// children — is validated, not just the HTTP boundary. nil ⇒ no validation
	// (tests, or a pod wired without schemas); set via WithValidator from
	// cmd/converge with the providers' declared schemas. huma-free interface
	// (internal/specschema), so the store gains no web-framework dependency.
	validator specschema.Validator
}

// shardBounds collapses a worker's owned shard set to its [lo, hi]
// inclusive range. Callers pass these as a BETWEEN predicate to the
// partitioned work_queue / work_outbox tables so Postgres can do runtime
// partition pruning (a `shard_id = ANY($array)` predicate is NOT prunable
// and scans every partition — see the WorkQueueTakeBatch comment in
// db/queries/work_queue.sql).
//
// CONTRACT: shards MUST be a contiguous range — then [min, max] is
// exactly the owned set with no foreign shards swept in. This holds for
// every caller: a dynamic assignment from assign_member_shards is a single
// contiguous range, and the drainer's bandShards sub-slices that contiguously.
// A NON-contiguous set would
// make BETWEEN min..max match shards this pod doesn't own → it would
// claim/heartbeat/delete another pod's work. Empty set → (0, -1), an
// empty range that matches nothing.
func shardBounds(shards []int16) (lo, hi int16) {
	if len(shards) == 0 {
		return 0, -1
	}
	lo, hi = shards[0], shards[0]
	for _, s := range shards[1:] {
		if s < lo {
			lo = s
		}
		if s > hi {
			hi = s
		}
	}
	return lo, hi
}

// secsAtLeast1 converts a Duration to whole seconds as int32, clamped to a
// minimum of 1 — the floor every plpgsql reaper/sweep wrapper applies so a
// sub-second or zero interval never disables the sweep.
func secsAtLeast1(d time.Duration) int32 {
	if s := int32(d.Seconds()); s >= 1 {
		return s
	}
	return 1
}

// defaultValidator is the process-wide spec validator every store.New picks up,
// set ONCE at boot (SetDefaultValidator) before any goroutine runs. It exists
// because store.New is the universal constructor used by the dispatcher's
// composer path (Loop.Repo), the API's commands, the reaper, and any future
// ingestion duty — threading a validator into each call site would be invasive
// and easy to miss, silently reopening the validation gap. A set-once boot
// config read concurrently after that is race-free; nil (tests, or a pod wired
// without schemas) ⇒ no validation. WithValidator still overrides per-store.
var defaultValidator specschema.Validator

// SetDefaultValidator installs the process-wide validator. Call ONCE at boot
// (cmd/converge, right after the providers' declared schemas are known)
// before constructing any engine/server/duty. Idempotent-safe but not meant to
// be called repeatedly.
func SetDefaultValidator(v specschema.Validator) { defaultValidator = v }

// New constructs a pool-backed Store. It adopts the process-wide
// defaultValidator (SetDefaultValidator) so every author path validates specs
// at the data gateway with no per-call-site wiring; tests that never set one get
// no validation. Override per-store with WithValidator.
func New(pool *pgxpool.Pool) *Store {
	return &Store{db: pool, validator: defaultValidator}
}

// WithValidator returns a copy of the store that validates resource specs
// against their kind's declared schema in ApplySpec + ApplyComposeResult.
// cmd/converge builds the validator from the providers' declared schemas
// (specschema.New) and calls this once at boot, so the HTTP API, ingestion
// duties, reactors, and the composer all validate through the one engine.
func (s *Store) WithValidator(v specschema.Validator) *Store {
	cp := *s
	cp.validator = v
	return &cp
}

// WithTx returns a Store that runs all queries inside tx, carrying the
// validator so a tx-scoped store (e.g. the composer's txRepo) validates too.
func (s *Store) WithTx(tx pgx.Tx) *Store {
	return &Store{db: tx, validator: s.validator}
}

func (s *Store) queries() *dbq.Queries { return dbq.New(s.db) }
