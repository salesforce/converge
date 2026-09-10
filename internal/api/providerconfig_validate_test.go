package api

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/examples/demos/datadriven/celbom"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// fakeConfigStore is a no-DB ProviderConfigStore that records the last upsert, so
// the validation test can assert the handler's kind gate WITHOUT a database.
type fakeConfigStore struct {
	upserted []store.ProviderConfig
}

func (f *fakeConfigStore) UpsertProviderConfig(_ context.Context, name string, kind model.Kind, kindVersion int, isDefault bool, spec json.RawMessage, data []byte) (store.ProviderConfig, bool, error) {
	pc := store.ProviderConfig{Name: name, Kind: kind, KindVersion: kindVersion, IsDefault: isDefault, Spec: spec, Data: data}
	f.upserted = append(f.upserted, pc)
	return pc, true, nil
}
func (f *fakeConfigStore) GetProviderConfig(_ context.Context, _ string) (store.ProviderConfig, bool, error) {
	return store.ProviderConfig{}, false, nil
}
func (f *fakeConfigStore) ListProviderConfigs(_ context.Context, _ store.ProviderConfigFilter, _, _ int) ([]store.ProviderConfig, error) {
	return nil, nil
}
func (f *fakeConfigStore) CountProviderConfigs(_ context.Context, _ store.ProviderConfigFilter) (int64, error) {
	return 0, nil
}
func (f *fakeConfigStore) DeleteProviderConfig(_ context.Context, _ string) (bool, error) {
	return false, nil
}

// TestApplyProviderConfigKindGate proves the providerconfig write gate accepts a
// config for a declared WORK kind AND for a declared REACTOR kind (a reactor's
// BOOTSTRAP endpoint config — e.g. statussink), while still rejecting an unknown
// kind. The key case: with reactors declared, an unknown WORK kind must still 422
// — a present reactor set must not open the gate to everything.
func TestApplyProviderConfigKindGate(t *testing.T) {
	fake := &fakeConfigStore{}
	srv := &Server{configs: fake}
	// celbom is a declared work kind; statussink is a declared reactor kind (out of
	// `declared`, in reactorSchemas).
	srv.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})
	srv.SetReactorSchemas([]model.KindManifest{statussinkManifest(t)})

	mk := func(name, kind string, spec string) *applyProviderConfigInput {
		in := &applyProviderConfigInput{}
		in.Body.Name = name
		in.Body.Kind = kind
		in.Body.KindVersion = 1 // REQUIRED (no implicit default); the kind gate is under test, not the version
		in.Body.IsDefault = true
		in.Body.Spec = json.RawMessage(spec)
		return in
	}

	// Accepted: a REACTOR kind's default config (the fix). statussink's config_schema
	// (endpoint/prefix) lives in reactorSchemas, so validateConfig passes.
	_, err := srv.applyProviderConfig(context.Background(),
		mk("statussink", string(statussink.Kind), `{"endpoint":"s3://bucket","prefix":"rolled-up/"}`))
	require.NoError(t, err, "a reactor kind's default providerconfig must be accepted")

	// Accepted: a declared WORK kind's config. A minimal doc valid against celbom's
	// config_schema (which requires `rules`) — the gate, not the schema, is under test.
	_, err = srv.applyProviderConfig(context.Background(),
		mk("celbom-default", string(celbom.Kind), `{"rules":[]}`))
	require.NoError(t, err, "a declared work kind's providerconfig must be accepted")

	// Rejected: an unknown kind — even though a reactor set is wired. This is the
	// guard against OR-ing the two "empty set accepts anything" fallbacks open.
	_, err = srv.applyProviderConfig(context.Background(),
		mk("bogus", "nope-not-a-kind", `{}`))
	require.Error(t, err, "an unknown kind must be rejected even with reactors declared")

	// Only the two valid configs ever reached the store.
	require.Len(t, fake.upserted, 2, "no rejected config may reach the store")
	require.Equal(t, model.Kind(statussink.Kind), fake.upserted[0].Kind)
	require.Equal(t, model.Kind(celbom.Kind), fake.upserted[1].Kind)
}

// TestApplyResourceRejectsReactorKind proves the symmetric guard: a REACTOR kind
// (statussink) must NOT be applyable as a RESOURCE. A reactor owns no resource — it
// is wired by a kind manifest + providerconfig + reactor_binding. Before the guard,
// mis-applying a reactor's providerconfig with `--type resource` sailed through
// validateSpec (a reactor declares no resource spec_schema → validateSpec no-ops)
// and silently created a junk resource row. The handler must 422 BEFORE the store.
func TestApplyResourceRejectsReactorKind(t *testing.T) {
	// commands is nil on purpose: a reactor kind must be rejected BEFORE the store is
	// ever touched (a nil-deref here would prove the guard didn't fire first).
	srv := &Server{}
	srv.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})
	srv.SetReactorSchemas([]model.KindManifest{statussinkManifest(t)})

	mkRes := func(kind, spec string) *applyResourceManifestInput {
		in := &applyResourceManifestInput{}
		in.Body.Kind = kind
		in.Body.KindVersion = 1 // REQUIRED; the reactor-kind guard is under test, not the version
		in.Body.Name = "x"
		in.Body.Spec = json.RawMessage(spec)
		return in
	}

	// Reactor kind as a resource → rejected (the statussink providerconfig body).
	_, err := srv.applyResourceManifest(context.Background(),
		mkRes(string(statussink.Kind), `{"endpoint":"s3://bucket","prefix":"rolled-up/"}`))
	require.Error(t, err, "a reactor kind must NOT be applyable as a resource")
}
