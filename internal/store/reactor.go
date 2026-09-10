package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/dbq"
	"github.com/salesforce/converge/internal/model"
)

// ─────────────────────────────────────────────────────────────────────────
// Lifecycle reactor spine: store boundary for the ReactorDispatcher duty and
// the reactor_bindings rule table. The claim is hand-written (sqlc can't type a
// plpgsql RETURNS TABLE); the rest are thin dbq wrappers.
// ─────────────────────────────────────────────────────────────────────────

// ReactorDelivery is one claimed lifecycle transition ready to deliver: the
// transitioned resource's identity + live status, the matched binding, the
// reactor kind, and that reactor's declared reaction name. Everything the
// dispatcher needs to build a model.ReactRequest, in one row.
type ReactorDelivery struct {
	ResourceID  uuid.UUID
	Kind        model.Kind
	Name        string
	Transition  string
	Generation  int64
	Status      json.RawMessage // the resource's current status (e.g. a composer's rolled-up status)
	BindingName string
	Reactor     model.Kind // the reactor kind the broker ships STAGE_REACT to
	// Reaction is the reactor kind's single `reactor`-trigger reaction name,
	// resolved by the claim from the reactor's CRD (kind_manifest). The dispatcher
	// ships it verbatim as the reaction handle — the binding name is opaque and
	// never parsed for it. Empty when the reactor CRD isn't applied yet → the
	// delivery is left claimed for the reaper (at-least-once).
	Reaction string
	// ReactorKindVersion is the reactor kind_version the claim resolved the reaction
	// against — the binding's pinned reactor_version, else the reactor's highest
	// published version. The dispatcher passes it to DispatchStage so the worker looks
	// up THAT version's handler and pulls THAT version's default config (a v2 reactor
	// runs its v2 handler, never silently v1). 0 when no reaction resolved.
	ReactorKindVersion int
	// ClaimEpoch is the strict monotonic fencing token this delivery was claimed under
	// (lifecycle_outbox.claim_epoch, bumped on the claim). The dispatcher passes it back
	// to AckReactorDelivery so only the current epoch's holder may ack: a dispatcher
	// whose claim was reaped and re-issued carries a stale epoch, so its ack matches 0
	// rows and cannot delete the live re-delivery.
	ClaimEpoch int64
}

// ClaimReactorDeliveries claims up to limit due transitions in the pod's
// contiguous shard range that match an enabled binding, stamping worker_id so
// other pods skip them. Range pruning via shardBounds (BETWEEN, never ANY). The
// SQL is hand-written in dbq (dbq.ClaimReactorDeliveriesSQL) — sqlc can't type a
// RETURNS TABLE function.
func (s *Store) ClaimReactorDeliveries(ctx context.Context, brokerID string, limit int, shards []int16) ([]ReactorDelivery, error) {
	lo, hi := shardBounds(shards)
	rows, err := s.db.Query(ctx, dbq.ClaimReactorDeliveriesSQL, brokerID, int32(limit), lo, hi)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReactorDelivery
	for rows.Next() {
		var (
			d          ReactorDelivery
			kindStr    string
			name       *string
			reactor    *string
			reaction   *string
			reactorVer *int32 // NULL when no reaction resolved (reactor CRD not applied)
			status     json.RawMessage
		)
		if err := rows.Scan(&d.ResourceID, &kindStr, &name, &d.Transition, &d.Generation,
			&status, &d.BindingName, &reactor, &reaction, &reactorVer, &d.ClaimEpoch); err != nil {
			return nil, err
		}
		d.Kind = model.Kind(kindStr)
		if name != nil {
			d.Name = *name
		}
		if reactor != nil {
			d.Reactor = model.Kind(*reactor)
		}
		if reaction != nil {
			d.Reaction = *reaction
		}
		if reactorVer != nil {
			d.ReactorKindVersion = int(*reactorVer)
		}
		d.Status = status
		out = append(out, d)
	}
	return out, rows.Err()
}

// AckReactorDelivery marks a delivery done by deleting its outbox row so it is
// never re-delivered. Keyed by the full PER-BINDING tuple (resource, transition,
// gen, binding) so acking one binding's delivery never touches a sibling binding's
// row for the same transition, and FENCED on claimEpoch so only the current epoch's
// holder may ack (a reaped/re-issued claim's stale ack matches 0 rows).
func (s *Store) AckReactorDelivery(ctx context.Context, resourceID uuid.UUID, transition string, generation int64, bindingName string, claimEpoch int64) error {
	return s.queries().AckReactorDelivery(ctx, dbq.AckReactorDeliveryParams{
		Column1: resourceID,
		Column2: transition,
		Column3: generation,
		Column4: bindingName,
		Column5: claimEpoch,
	})
}

// ReactorLeaseKey identifies one in-flight lifecycle delivery to heartbeat: its
// per-binding key plus the claim_epoch it was claimed under. The dispatcher passes the
// exact set it currently holds so HeartbeatReactorClaims refreshes EXACTLY those rows,
// epoch-fenced — a row re-issued under a bumped epoch no longer matches and is skipped
// (the deadlock-safety property; see HeartbeatReactorClaims).
type ReactorLeaseKey struct {
	ResourceID  uuid.UUID
	Transition  string
	Generation  int64
	BindingName string
	ClaimEpoch  int64
}

// HeartbeatReactorClaims refreshes heartbeat_at on the SPECIFIC in-flight deliveries the
// dispatcher holds (keyed by identity + claim_epoch), within its shard range so a slow
// reactor isn't reaped mid-delivery. Epoch-fenced and deadlock-safe: mirrors
// WorkQueueHeartbeat's (id, claim_epoch) contract — a row a concurrent claim/reap
// re-issued (bumped epoch) drops out of the join, so the heartbeat never chases its
// updated tuple version and cannot 40P01-cycle with the re-claimer. Empty input is a
// no-op. The four key parts + epoch are positional parallel arrays (keys[i] ↔ one row).
// No broker_id: the (key, claim_epoch) identifies the row on its own — the broker that
// claimed it is irrelevant to keeping ITS epoch's lease alive (owner-independent, like
// AckReactorDelivery).
func (s *Store) HeartbeatReactorClaims(ctx context.Context, keys []ReactorLeaseKey, shards []int16) error {
	if len(keys) == 0 {
		return nil
	}
	lo, hi := shardBounds(shards)
	ids := make([]uuid.UUID, len(keys))
	trans := make([]string, len(keys))
	gens := make([]int64, len(keys))
	bindings := make([]string, len(keys))
	epochs := make([]int64, len(keys))
	for i, k := range keys {
		ids[i], trans[i], gens[i], bindings[i], epochs[i] = k.ResourceID, k.Transition, k.Generation, k.BindingName, k.ClaimEpoch
	}
	return s.queries().HeartbeatReactorClaims(ctx, ids, trans, gens, bindings, epochs, lo, hi)
}

// ReapStaleLifecycle frees lifecycle_outbox rows whose dispatcher stopped
// heartbeating, re-arming them for one more at-least-once attempt. Clone of
// ReapStaleWork; called from the reaper tick over the reaper's shard range.
func (s *Store) ReapStaleLifecycle(ctx context.Context, staleAfter time.Duration, limit int, shards []int16) error {
	lo, hi := shardBounds(shards)
	return s.queries().ReapStaleLifecycle(ctx, dbq.ReapStaleLifecycleParams{
		Column1: secsAtLeast1(staleAfter),
		Column2: int32(limit),
		Column3: lo,
		Column4: hi,
	})
}

// ─────────────────────────────────────────────────────────────────────────
// reactor_bindings CRUD — the runtime-editable "what reacts to what" surface.
// ─────────────────────────────────────────────────────────────────────────

// ReactorBinding is one subscription: when a `WatchKind` resource crosses
// `Transition` (optionally label-matched), run the `Reactor` kind's reactor
// reaction. Reactor is the handler kind the broker ships STAGE_REACT to; it is
// always distinct from WatchKind (a reactor owns no resource of its own). It is
// the SOLE wiring surface — there is no manifest projection and no derived /
// manual split; every binding is an explicit, editable subscription.
type ReactorBinding struct {
	Name      string
	WatchKind model.Kind
	// WatchKindVersion optionally scopes the subscription to ONE web-API version of
	// the WATCHED kind. nil = all versions (fire on the transition of any version's
	// resource); a value fires only for resources on that version. Distinct from
	// ReactorVersion, which versions the reactor being run.
	WatchKindVersion *int
	Transition       string
	LabelMatch       json.RawMessage
	// Reactor is the reactor KIND whose reaction runs when the subscription fires.
	// A kind identity like WatchKind — model.Kind, not a bare string — so the two
	// ends of the binding carry the same domain type (the string transport is the
	// API body's job, converted at the handler boundary).
	Reactor model.Kind
	// ReactorVersion optionally PINS which published kind_version of the reactor this
	// binding invokes. nil = unpinned (claim resolves the reactor's HIGHEST
	// published version, so a reactor upgrade takes effect automatically); a value
	// pins it to that exact reactor version, immune to a later reactor publish.
	ReactorVersion *int
	Enabled        bool
}

// UpsertReactorBinding creates or replaces a subscription. Live: the dispatcher
// reads bindings fresh on every claim, so the change takes effect with no
// restart and no Go-side cache to invalidate.
func (s *Store) UpsertReactorBinding(ctx context.Context, b ReactorBinding) error {
	labels := b.LabelMatch
	if len(labels) == 0 {
		labels = json.RawMessage(`{}`)
	}
	return s.queries().UpsertReactorBinding(ctx, dbq.UpsertReactorBindingParams{
		Name:             b.Name,
		WatchKind:        string(b.WatchKind),
		WatchKindVersion: int16PtrOrNil(b.WatchKindVersion),
		Transition:       b.Transition,
		LabelMatch:       labels,
		Reactor:          string(b.Reactor),
		ReactorVersion:   int16PtrOrNil(b.ReactorVersion),
		Enabled:          b.Enabled,
	})
}

// int16PtrOrNil narrows a nullable pin (*int) to the *int16 the smallint column
// takes. A value < 1 (or nil) yields nil (unpinned), matching the DB CHECK
// (reactor_version >= 1) so a bogus 0 never becomes a pin.
func int16PtrOrNil(v *int) *int16 {
	if v == nil || *v < 1 {
		return nil
	}
	n := int16(*v)
	return &n
}

// intPtrOfInt16 widens the nullable smallint pin read back from the row (*int16)
// to the *int the domain type carries. nil stays nil (unpinned).
func intPtrOfInt16(v *int16) *int {
	if v == nil {
		return nil
	}
	n := int(*v)
	return &n
}

// ListReactorBindings returns all subscriptions, name-sorted.
func (s *Store) ListReactorBindings(ctx context.Context) ([]ReactorBinding, error) {
	rows, err := s.queries().ListReactorBindings(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ReactorBinding, len(rows))
	for i, r := range rows {
		out[i] = ReactorBinding{
			Name:             r.Name,
			WatchKind:        model.Kind(r.WatchKind),
			WatchKindVersion: intPtrOfInt16(r.WatchKindVersion),
			Transition:       r.Transition,
			LabelMatch:       r.LabelMatch,
			Reactor:          model.Kind(r.Reactor),
			ReactorVersion:   intPtrOfInt16(r.ReactorVersion),
			Enabled:          r.Enabled,
		}
	}
	return out, nil
}

// DeleteReactorBinding removes a binding by name, reporting whether a row went
// (false = no such binding) so the API can 404 a missing name — mirroring
// DeleteProviderConfig.
func (s *Store) DeleteReactorBinding(ctx context.Context, name string) (bool, error) {
	if _, err := s.queries().DeleteReactorBinding(ctx, name); err != nil {
		if isNoRows(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// DedupToken is the stable idempotency key handed to a reactor: a redelivery
// (at-least-once) carries the SAME token, so an idempotent sink overwrites
// rather than duplicates. Includes binding_name so two DIFFERENT bindings on the
// same (resource, transition, generation) get DISTINCT tokens — otherwise two
// reactors sharing one (kind,transition) would collide on a single sink key.
// Format "<resource_id>:<transition>:<generation>:<binding_name>".
func DedupToken(resourceID uuid.UUID, transition string, generation int64, bindingName string) string {
	return fmt.Sprintf("%s:%s:%d:%s", resourceID, transition, generation, bindingName)
}
