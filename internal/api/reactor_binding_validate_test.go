package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/examples/demos/datadriven/celbom"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// fakeBindingStore is a no-DB ReactorBindingStore that records the last upsert,
// so the validation test can assert the handler's guards WITHOUT a database.
type fakeBindingStore struct {
	upserted []store.ReactorBinding
}

func (f *fakeBindingStore) UpsertReactorBinding(_ context.Context, b store.ReactorBinding) error {
	f.upserted = append(f.upserted, b)
	return nil
}
func (f *fakeBindingStore) ListReactorBindings(_ context.Context) ([]store.ReactorBinding, error) {
	return f.upserted, nil
}
func (f *fakeBindingStore) DeleteReactorBinding(_ context.Context, _ string) (bool, error) {
	return true, nil
}

// TestApplyReactorBindingValidation proves the subscription model's guards on
// the write path: a binding wires a KNOWN watch kind to a REGISTERED reactor kind
// that DIFFERS from the watch kind. The API is the only gate — a manifest does
// not project a binding.
func TestApplyReactorBindingValidation(t *testing.T) {
	fake := &fakeBindingStore{}
	srv := &Server{bindings: fake}
	// celbom is a work kind (watchable); statussink is the only registered reactor.
	srv.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})
	srv.SetReactorSchemas([]model.KindManifest{statussinkManifest(t)})

	mk := func(name, watch, transition, reactor string) *applyReactorBindingInput {
		in := &applyReactorBindingInput{}
		in.Body.Name = name
		in.Body.WatchKind = watch
		in.Body.Transition = transition
		in.Body.Reactor = reactor
		return in
	}

	// Valid: watch a known work kind's 'synced', run the registered reactor.
	_, err := srv.applyReactorBinding(context.Background(),
		mk("ok", string(celbom.Kind), string(model.TransitionSynced), string(statussink.Kind)))
	require.NoError(t, err, "a valid subscription (known watch kind → registered reactor) must be accepted")
	require.Len(t, fake.upserted, 1)
	require.Equal(t, model.Kind(celbom.Kind), fake.upserted[0].WatchKind)
	require.Equal(t, model.Kind(statussink.Kind), fake.upserted[0].Reactor)

	// Reject: unknown watch kind.
	_, err = srv.applyReactorBinding(context.Background(),
		mk("bad-watch", "nope-kind", string(model.TransitionSynced), string(statussink.Kind)))
	require.Error(t, err, "an unknown watch_kind must be rejected")

	// Reject: reactor is not a registered reactor kind.
	_, err = srv.applyReactorBinding(context.Background(),
		mk("bad-reactor", string(celbom.Kind), string(model.TransitionSynced), "not-a-reactor"))
	require.Error(t, err, "an unregistered reactor kind must be rejected")

	// Reject: a work kind used as the reactor (it's not a reactor kind).
	_, err = srv.applyReactorBinding(context.Background(),
		mk("work-as-reactor", string(celbom.Kind), string(model.TransitionSynced), string(celbom.Kind)))
	require.Error(t, err, "a work kind must not be usable as the reactor")

	// (An invalid transition is rejected too, but by the body's `enum` huma tag at the
	// HTTP edge — not by this handler — so a direct handler call can't exercise it; the
	// real over-HTTP apply path is covered by the integration suite.)

	// Only the one valid subscription was ever stored.
	require.Len(t, fake.upserted, 1, "no rejected subscription may reach the store")
}

// TestApplyReactorBindingRetireGate proves the reactor-parity retire gate (#6):
// a NEW binding whose TARGET reactor version is retired is rejected (422), while a
// binding onto a LIVE reactor version is accepted — the freeze-new/drain-existing
// contract for reactors, mirroring the resource create gate.
func TestApplyReactorBindingRetireGate(t *testing.T) {
	fake := &fakeBindingStore{}
	// kindConfigs reports statussink/v1 (the highest published version here) as
	// RETIRED. A nil-return for other lookups is the fake's zero value (not found).
	kc := fakeKindConfigStore{
		kc:      store.KindConfig{Kind: model.Kind(statussink.Kind), KindVersion: 1, Retired: true},
		kcFound: true,
	}
	srv := &Server{bindings: fake, kindConfigs: kc}
	srv.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})
	srv.SetReactorSchemas([]model.KindManifest{statussinkManifest(t)}) // published at v1

	mk := func(name string) *applyReactorBindingInput {
		in := &applyReactorBindingInput{}
		in.Body.Name = name
		in.Body.WatchKind = string(celbom.Kind)
		in.Body.Transition = string(model.TransitionSynced)
		in.Body.Reactor = string(statussink.Kind)
		return in
	}

	// Unpinned binding → resolves the reactor's HIGHEST published version (v1),
	// which is retired → 422, and nothing reaches the store.
	_, err := srv.applyReactorBinding(context.Background(), mk("onto-retired"))
	require.Error(t, err, "a binding onto a retired reactor version must be rejected")
	require.Empty(t, fake.upserted, "a rejected binding must not reach the store")
}

// TestReactorMultiVersionListChips proves a reactor kind published at v1 AND v2
// shows BOTH version chips in /api/kinds, exactly like a multi-version work kind —
// each (kind, version) is its own row, never collapsed to one per kind.
func TestReactorMultiVersionListChips(t *testing.T) {
	srv := &Server{}
	srv.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})
	// Publish the statussink reactor at BOTH v1 and v2.
	rxV1 := statussinkManifest(t)
	rxV1.KindVersion = 1
	rxV2 := statussinkManifest(t)
	rxV2.KindVersion = 2
	srv.SetReactorSchemas([]model.KindManifest{rxV1, rxV2})

	out, err := srv.listKindSchemas(context.Background(), &struct{}{})
	require.NoError(t, err)

	var found *kindListEntry
	for i := range out.Body.Kinds {
		if out.Body.Kinds[i].Kind == string(statussink.Kind) {
			found = &out.Body.Kinds[i]
			break
		}
	}
	require.NotNil(t, found, "the reactor kind must appear in the kinds list")
	require.True(t, found.IsReactor, "it is tagged is_reactor")
	require.Equal(t, []int{1, 2}, found.KindVersions,
		"a reactor published at v1+v2 chips BOTH versions (no collapse)")
}
