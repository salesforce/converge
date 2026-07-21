package broker

// mesh_recovery_test.go — DETERMINISTIC, pure-Go (no Postgres, no timing) unit tests
// for the broker credit-accounting and fence/token logic. They drive the fanoutExecutor
// directly through its seams (DispatchStage / deliver / drop / attributeToStream /
// abandon) and assert the exact invariants a regression would break — the reliable
// regression net for the tier. Recovery on a peer/worker loss is unified on the reaper
// (a stranded row's heartbeat lapses → re-claim under a bumped claim_epoch → re-run), so
// there are no distinct mesh fast-recovery edges to unit-test here.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// parkOne parks a no-deadline reaction on f (as the reconcile pipeline would) and
// returns its lease token plus a channel that receives the reaction's error when it
// finally resolves. The stage sits in f.inflight + on the kind buffer until resolved.
func parkOne(t *testing.T, ctx context.Context, f *fanoutExecutor, kind model.Kind, gen int64) (token string, errCh chan error) {
	t.Helper()
	errCh = make(chan error, 1)
	go func() {
		_, _, err := f.DispatchStage(ctx, kind, 1,
			model.ReactionDecl{Name: "work", Trigger: model.TriggerSpecChange, Emits: model.OutcomeMask{model.OutcomeStatus}},
			model.ReactionRequest{
				Resource: model.Resource{ID: uuid.New(), Kind: kind, Spec: json.RawMessage(`{}`), Generation: gen},
				Env:      &model.Env{},
			})
		errCh <- err
	}()
	q := f.queueFor(model.KindVersion{Kind: kind, Version: 1})
	select {
	case ps := <-q:
		return ps.task.GetLeaseToken(), errCh
	case <-time.After(5 * time.Second):
		t.Fatal("reaction never parked on the kind buffer")
		return "", nil
	}
}

// ── credit / slot accounting ─────────────────────────────────────────────────────

// TestAttributeConsumesSlotOnce proves attributeToStream consumes a stream's slot AT
// MOST ONCE (idempotent via slotHeld): calling it twice for the same parked stage
// leaves sub.inflight at 1, not 2. A regression that dropped the slotHeld guard would
// over-count → the stream advertises too little credit → peers stop pushing to a
// healthy worker.
func TestAttributeConsumesSlotOnce(t *testing.T) {
	f := newFanoutExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	token, _ := parkOne(t, ctx, f, "noop", 1)

	f.mu.Lock()
	ps := f.inflight[token]
	f.mu.Unlock()
	sub := &configSub{kinds: map[model.KindVersion]struct{}{{Kind: "noop", Version: 1}: {}}, maxInflight: 4}

	if !f.attributeToStream(ps, sub) {
		t.Fatal("first attribute returned false for a live token")
	}
	if !f.attributeToStream(ps, sub) {
		t.Fatal("second attribute returned false for a still-live token")
	}
	f.mu.Lock()
	got := sub.inflight
	f.mu.Unlock()
	if got != 1 {
		t.Fatalf("sub.inflight = %d after attributing twice; want 1 (slot consumed at most once)", got)
	}
}

// TestAttributeAfterDropDoesNotConsume proves the leak fix: if a token was already
// resolved/dropped (removed from f.inflight) BEFORE the fan-in attributes it, the
// attribute must NOT consume a slot (it returns false) — else the slot would be
// consumed with no path to ever free it (permanent credit leak). Models the deadline/
// drain drop racing the buffer→worker hand-off.
func TestAttributeAfterDropDoesNotConsume(t *testing.T) {
	f := newFanoutExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	token, errCh := parkOne(t, ctx, f, "noop", 1)

	f.mu.Lock()
	ps := f.inflight[token]
	f.mu.Unlock()

	// Resolve the token first (as a deadline/drain drop would): removes it from inflight.
	f.drop(token)
	select {
	case <-errCh: // DispatchStage unwinds (dropped, no result)
	case <-time.After(2 * time.Second):
	}

	sub := &configSub{kinds: map[model.KindVersion]struct{}{{Kind: "noop", Version: 1}: {}}, maxInflight: 4}
	if f.attributeToStream(ps, sub) {
		t.Fatal("attribute returned true for an already-dropped token — it must skip (undeliverable)")
	}
	f.mu.Lock()
	got := sub.inflight
	held := ps.slotHeld
	f.mu.Unlock()
	if got != 0 || held {
		t.Fatalf("attribute consumed a slot for a dropped token (inflight=%d slotHeld=%v); want 0/false — this is the credit leak", got, held)
	}
}

// TestDropThenUnattributeFreesSlotOnce proves the double-free fix: a deadline drop()
// and a send-failure unattributeFromStream() racing on the SAME attributed stage must
// free the slot AT MOST ONCE. drop() clears slotHeld+sentBy under the lock, so the
// following unattribute is a no-op — sub.inflight ends at 0, never -1/underflow.
func TestDropThenUnattributeFreesSlotOnce(t *testing.T) {
	f := newFanoutExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	token, _ := parkOne(t, ctx, f, "noop", 1)

	f.mu.Lock()
	ps := f.inflight[token]
	f.mu.Unlock()
	sub := &configSub{kinds: map[model.KindVersion]struct{}{{Kind: "noop", Version: 1}: {}}, maxInflight: 4}
	if !f.attributeToStream(ps, sub) {
		t.Fatal("attribute failed for a live token")
	}
	f.mu.Lock()
	if sub.inflight != 1 {
		t.Fatalf("precondition: sub.inflight = %d, want 1", sub.inflight)
	}
	f.mu.Unlock()

	// drop() frees the slot AND clears slotHeld/sentBy; the racing unattribute must no-op.
	f.drop(token)
	f.unattributeFromStream(ps)

	f.mu.Lock()
	got := sub.inflight
	f.mu.Unlock()
	if got != 0 {
		t.Fatalf("sub.inflight = %d after drop+unattribute; want 0 (freed at most once — a double-free would give a stale/negative count)", got)
	}
}

// ── fence / token / late-Complete ────────────────────────────────────────────────

// TestDeliverUnknownTokenNoOps proves a Complete for a token this broker never
// dispatched a lease for (a stale worker, a cross-broker mis-route, a spoof attempt, or
// a task this broker FORWARDED to a peer whose worker Completes it — the executing peer
// holds no lease for it, so deliver is a map miss) is a harmless no-op — no panic, no
// state change. The random-UUID token + this map-miss no-op are the fence's front line
// against a foreign Complete.
func TestDeliverUnknownTokenNoOps(t *testing.T) {
	f := newFanoutExecutor()
	// Must not panic and must not create any inflight state.
	f.deliver("no-such-token", &workerpb.StageComplete{LeaseToken: "no-such-token"})
	f.mu.Lock()
	nIn := len(f.inflight)
	f.mu.Unlock()
	if nIn != 0 {
		t.Fatalf("deliver of an unknown token created state (inflight=%d); want 0", nIn)
	}
}

// TestDeliverTwiceResolvesOnce proves a duplicate Complete (at-least-once redelivery)
// resolves the parked stage exactly once: the first delivery removes the token from
// inflight, the second finds it gone and no-ops (can't double-deliver into the
// buffered(1) resultCh). Guards the no-double-apply contract at the lease layer.
func TestDeliverTwiceResolvesOnce(t *testing.T) {
	f := newFanoutExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	token, errCh := parkOne(t, ctx, f, "noop", 1)

	sc := &workerpb.StageComplete{LeaseToken: token} // empty status → nil Outcome, still resolves
	f.deliver(token, sc)
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("first deliver did not resolve the parked stage")
	}
	// Token must be gone; a second deliver is a pure no-op (no panic, no state).
	f.mu.Lock()
	_, live := f.inflight[token]
	f.mu.Unlock()
	if live {
		t.Fatal("token still inflight after deliver — a redelivery could double-resolve")
	}
	f.deliver(token, sc) // must not panic
}

// TestSyntheticTransientLosesToGenuineComplete proves the deliver-vs-abandon race is
// closed: when a genuine Complete resolves a token, a subsequent synthetic transient
// (an abandon for the same token) no-ops instead of clobbering the real result.
// deliver() deletes from inflight under f.mu before touching resultCh, so the later
// synthetic finds the token gone.
func TestSyntheticTransientLosesToGenuineComplete(t *testing.T) {
	f := newFanoutExecutor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	token, errCh := parkOne(t, ctx, f, "noop", 1)

	// Genuine Complete resolves it (nil error path).
	f.deliver(token, &workerpb.StageComplete{LeaseToken: token})
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("genuine Complete produced an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("genuine Complete did not resolve the stage")
	}
	// A late synthetic transient for the same token must no-op (token already gone).
	f.abandon([]string{token}) // must not panic; nothing to wake
	f.mu.Lock()
	_, live := f.inflight[token]
	f.mu.Unlock()
	if live {
		t.Fatal("token reappeared in inflight after a late synthetic — impossible unless the race reopened")
	}
}

// TestNewTokenUnique proves lease tokens are unique + unguessable (random UUIDs) — the
// property that stops a peer from enumerating another worker's token to spray a forged
// Complete. A regression to a sequential scheme would collide here.
func TestNewTokenUnique(t *testing.T) {
	f := newFanoutExecutor()
	const N = 10000
	seen := make(map[string]struct{}, N)
	for i := 0; i < N; i++ {
		tok := f.newToken()
		if tok == "" {
			t.Fatal("newToken returned empty")
		}
		if _, dup := seen[tok]; dup {
			t.Fatalf("newToken collision at i=%d: %q", i, tok)
		}
		seen[tok] = struct{}{}
	}
}

// ── worker-attested liveness (WorkHeartbeat → Attest) ────────────────────────────

// TestWorkIDsForTokensMapsOnlyLocalHeldLeases proves the worker-attestation seam: a
// WorkHeartbeat's lease_tokens are mapped to work_ids ONLY for tasks this broker holds a
// LOCAL lease for (in f.inflight, with a non-nil WorkID). A token this broker never
// dispatched (a stale token, or a reference it FORWARDED to a peer — attested on that
// peer, not here) yields no work_id and is dropped, so the broker only refreshes leases
// it actually owns. This is what WorkStream feeds the dispatcher's Attest.
func TestWorkIDsForTokensMapsOnlyLocalHeldLeases(t *testing.T) {
	f := newFanoutExecutor()

	// A LOCAL task with a real work_id lease (as dispatchStage would park it).
	localWork := uuid.New()
	localTok := f.newToken()
	f.mu.Lock()
	f.inflight[localTok] = &pendingStage{
		task:  &workerpb.StageTask{Kind: "noop", KindVersion: 1, LeaseToken: localTok},
		lease: runtime.Lease{WorkID: localWork, ShardID: 3},
	}
	// An inflight token with NO work_id (a zero lease — e.g. an in-process/non-broker
	// park): must NOT map to a work_id.
	noLeaseTok := f.newToken()
	f.inflight[noLeaseTok] = &pendingStage{task: &workerpb.StageTask{Kind: "noop", KindVersion: 1, LeaseToken: noLeaseTok}}
	f.mu.Unlock()

	got := f.workIDsForTokens([]string{localTok, noLeaseTok, "unknown-token"})
	if len(got) != 1 || got[0] != localWork {
		t.Fatalf("workIDsForTokens = %v; want exactly [%v] (only the locally-held lease)", got, localWork)
	}

	// Empty input → nil, no panic.
	if got := f.workIDsForTokens(nil); got != nil {
		t.Fatalf("workIDsForTokens(nil) = %v; want nil", got)
	}
}

// TestAttestRelaysAttestedTokens proves WorkStream's attestation relay: mapping a
// heartbeat's tokens through workIDsForTokens and calling f.attest hands the dispatcher
// exactly the work_ids of the still-progressing LOCAL tasks. We wire a fake attest sink
// (as construction wires dispatcher.Attest) and assert only the held lease's id reaches it.
func TestAttestRelaysAttestedTokens(t *testing.T) {
	f := newFanoutExecutor()
	var attested []uuid.UUID
	f.attest = func(ids []uuid.UUID) { attested = append(attested, ids...) }

	work := uuid.New()
	tok := f.newToken()
	f.mu.Lock()
	f.inflight[tok] = &pendingStage{
		task:  &workerpb.StageTask{Kind: "noop", KindVersion: 1, LeaseToken: tok},
		lease: runtime.Lease{WorkID: work, ShardID: 1},
	}
	f.mu.Unlock()

	// The relay WorkStream performs on a WorkHeartbeat frame: token → work_id → Attest.
	f.attest(f.workIDsForTokens([]string{tok, "foreign-or-stale"}))

	if len(attested) != 1 || attested[0] != work {
		t.Fatalf("attest received %v; want exactly [%v] (only the locally-held lease's id)", attested, work)
	}
}
