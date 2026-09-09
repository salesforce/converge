package api

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/examples/demos/classic/statussink"
	"github.com/salesforce/converge/examples/demos/datadriven/celbom"
	"github.com/salesforce/converge/internal/host"
	"github.com/salesforce/converge/internal/model"
	"github.com/salesforce/converge/internal/store"
)

// fakeKindConfigStore is a no-DB KindConfigStore for the unified-view test: it
// returns canned cap+resync (kind_config) and a default config doc
// (providerconfigs) so we can assert getKindSchema joins both WITHOUT fusing the
// tables.
type fakeKindConfigStore struct {
	kc       store.KindConfig
	kcFound  bool
	doc      json.RawMessage
	docFound bool
}

func (f fakeKindConfigStore) GetKindConfig(_ context.Context, _ model.Kind, _ int) (store.KindConfig, bool, error) {
	return f.kc, f.kcFound, nil
}
func (f fakeKindConfigStore) ListKindConfig(_ context.Context) ([]store.KindConfig, error) {
	if f.kcFound {
		return []store.KindConfig{f.kc}, nil
	}
	return nil, nil
}
func (f fakeKindConfigStore) UpsertKindConfig(_ context.Context, _ store.KindConfig) error {
	return nil
}
func (f fakeKindConfigStore) GetDefaultProviderConfig(_ context.Context, _ model.Kind, _ int) (store.DefaultProviderConfig, bool, error) {
	return store.DefaultProviderConfig{Spec: f.doc}, f.docFound, nil
}

// celbomManifest loads the celbom composer's CRD fixture — a WORK kind that
// declares all three schemas (spec = the BOM, status, config = the rule set), so
// it exercises the full /api/kinds schema surface (spec + config). The manifest is
// pure data from the fixture, so the API tests need no successful provider Setup.
// celbom lives in the datadriven demo, so its CRD is under
// examples/demos/datadriven/testfixtures (test working dir is the package dir →
// ../../examples/...).
func celbomManifest(t *testing.T) model.KindManifest {
	t.Helper()
	all, err := host.LoadKindFixtures("../../examples/demos/datadriven/testfixtures")
	require.NoError(t, err)
	for _, m := range all {
		if m.Kind == model.Kind(celbom.Kind) {
			return m
		}
	}
	t.Fatalf("celbom kind fixture not found in examples/demos/datadriven/testfixtures")
	return model.KindManifest{}
}

// statussinkManifest loads the statussink reactor's CRD fixture — the manifest
// an operator applies to kind_manifest (config schema + a `reactor` reaction).
// The worker carries only handler code now, so the schema data comes from the
// fixture, not a provider runtime. statussink moved to the classic demo, so its
// CRD now lives under examples/demos/classic/testfixtures (test working dir is the
// package dir → ../../examples/...).
func statussinkManifest(t *testing.T) model.KindManifest {
	t.Helper()
	all, err := host.LoadKindFixtures("../../examples/demos/classic/testfixtures")
	require.NoError(t, err)
	for _, m := range all {
		if m.Kind == model.Kind(statussink.Kind) {
			return m
		}
	}
	t.Fatalf("statussink kind fixture not found in examples/demos/classic/testfixtures")
	return model.KindManifest{}
}

// TestKindSchemaUnifiesConfigSources proves the API joins the two SEPARATE
// per-kind config tables into one operator view: kind_config (cap + resync) and
// providerconfigs (the default config document). The storage stays split; only
// the read surface is unified. With no kind_config row the operational block is
// simply absent (uncapped, no resync).
func TestKindSchemaUnifiesConfigSources(t *testing.T) {
	srv := &Server{
		kindConfigs: fakeKindConfigStore{
			kc:       store.KindConfig{Kind: model.Kind(celbom.Kind), KindVersion: 1, MaxInflight: 50, ResyncInterval: 10 * time.Second, ResyncRecomposes: true},
			kcFound:  true,
			doc:      json.RawMessage(`{"rules":[]}`),
			docFound: true,
		},
	}
	srv.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})

	one, err := srv.getKindSchema(context.Background(), &kindSchemaInput{Kind: string(celbom.Kind)})
	require.NoError(t, err)

	// kind_config surfaces as the operational block.
	require.NotNil(t, one.Body.Operational, "operational (kind_config) must be joined into the kind view")
	require.Equal(t, 50, one.Body.Operational.MaxInflight)
	require.Equal(t, 10, one.Body.Operational.ResyncIntervalSeconds)
	require.True(t, one.Body.Operational.ResyncRecomposes)

	// providerconfigs default doc surfaces alongside it.
	require.JSONEq(t, `{"rules":[]}`, string(one.Body.DefaultConfig))

	// Absent kind_config row → no operational block (uncapped, no resync).
	srv2 := &Server{kindConfigs: fakeKindConfigStore{}}
	srv2.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})
	none, err := srv2.getKindSchema(context.Background(), &kindSchemaInput{Kind: string(celbom.Kind)})
	require.NoError(t, err)
	require.Nil(t, none.Body.Operational, "no kind_config row → no operational block")
	require.Nil(t, none.Body.DefaultConfig, "no default providerconfig → no default_config")
}

// TestKindsListIncludesReactors proves /api/kinds lists REACTOR kinds
// (statussink) alongside work kinds, tagged is_reactor and with NO operational
// block, while a work kind (celbom) is listed with is_reactor=false. Reactors
// come from the SEPARATE SetReactorSchemas surface, not the work set that gates
// spec/config validation.
func TestKindsListIncludesReactors(t *testing.T) {
	srv := &Server{}
	srv.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})
	srv.SetReactorSchemas([]model.KindManifest{statussinkManifest(t)})

	list, err := srv.listKindSchemas(context.Background(), nil)
	require.NoError(t, err)

	byKind := map[string]kindListEntry{}
	for _, e := range list.Body.Kinds {
		byKind[e.Kind] = e
	}

	work, ok := byKind[string(celbom.Kind)]
	require.True(t, ok, "work kind celbom must be listed")
	require.False(t, work.IsReactor, "a work kind must not be tagged is_reactor")

	react, ok := byKind[string(statussink.Kind)]
	require.True(t, ok, "reactor kind statussink must be listed in /api/kinds")
	require.True(t, react.IsReactor, "statussink must be tagged is_reactor")
	require.Nil(t, react.Operational, "a reactor has no kind_config → no operational block")

	// A reactor is NOT a creatable work kind: it stays out of spec/config write
	// validation (kindKnown false).
	require.False(t, srv.kindKnown(model.Kind(statussink.Kind)), "a reactor must not be a known work kind")

	// But its SCHEMA is served: /api/kinds/{kind}/schema returns the reactor's
	// config schema (its sink config) tagged is_reactor, with no operational block.
	one, err := srv.getKindSchema(context.Background(), &kindSchemaInput{Kind: string(statussink.Kind)})
	require.NoError(t, err, "a reactor's schema must be served, not 404")
	require.True(t, one.Body.IsReactor, "the schema response must be tagged is_reactor")
	require.NotNil(t, one.Body.ConfigSchema, "statussink declares a config (endpoint/prefix) → config_schema served")
	// The /docs component name (the UI's deep-link target) is now the synthetic
	// componentName(kind, "Config") — the manifest carries JSON schema, not a Go
	// type name.
	require.Equal(t, componentName(model.Kind(statussink.Kind), 1, "Config"), one.Body.ConfigSchemaRef, "config_schema_ref is the /docs component name")
	require.Nil(t, one.Body.Operational, "a reactor has no kind_config → no operational block")
}

// capturingKindConfigStore records the last UpsertKindConfig so the write-handler
// test can assert what reached the store.
type capturingKindConfigStore struct {
	fakeKindConfigStore
	last *store.KindConfig
}

func (c *capturingKindConfigStore) UpsertKindConfig(_ context.Context, k store.KindConfig) error {
	c.last = &k
	return nil
}

// TestApplyKindConfig covers the per-kind operational-config write endpoint:
// a known kind's cap+resync reaches the store with seconds→Duration conversion,
// and an unknown kind is rejected (422) before any write.
func TestApplyKindConfig(t *testing.T) {
	cap := &capturingKindConfigStore{}
	srv := &Server{kindConfigs: cap}
	srv.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})

	in := &applyKindConfigInput{Kind: string(celbom.Kind), KindVersion: 1} // KindVersion REQUIRED (no implicit default)
	in.Body.MaxInflight = 25
	in.Body.ResyncIntervalSeconds = 30
	in.Body.ResyncRecomposes = true
	out, err := srv.applyKindConfig(context.Background(), in)
	require.NoError(t, err)
	require.Equal(t, 25, out.Body.MaxInflight)
	require.Equal(t, 30, out.Body.ResyncIntervalSeconds)
	require.NotNil(t, cap.last)
	require.Equal(t, model.Kind(celbom.Kind), cap.last.Kind)
	require.Equal(t, 25, cap.last.MaxInflight)
	require.Equal(t, 30*time.Second, cap.last.ResyncInterval, "seconds must convert to Duration")
	require.True(t, cap.last.ResyncRecomposes)

	// Unknown kind → 422, no write.
	cap.last = nil
	bad := &applyKindConfigInput{Kind: "nope-not-a-kind", KindVersion: 1}
	_, err = srv.applyKindConfig(context.Background(), bad)
	require.Error(t, err)
	require.Nil(t, cap.last, "an unknown kind must not reach the store")
}

// TestDeclaredSchemaSurface proves the API publishes a kind's Spec/Status/Config
// schema at /api/kinds, /api/kinds/{kind}/schema, AND in the /docs OpenAPI
// components — drawn from the applied MANIFEST (pure JSON-schema data), with no
// dependency on a successful provider Setup or a live registry.
func TestDeclaredSchemaSurface(t *testing.T) {
	srv := &Server{}
	srv.SetDeclaredSchemas([]model.KindManifest{celbomManifest(t)})

	// /api/kinds lists the kind with has_spec/has_config=true.
	list, err := srv.listKindSchemas(context.Background(), nil)
	require.NoError(t, err)
	var summary *kindListEntry
	for i := range list.Body.Kinds {
		if list.Body.Kinds[i].Kind == string(celbom.Kind) {
			summary = &list.Body.Kinds[i]
		}
	}
	require.NotNil(t, summary, "celbom must be listed in /api/kinds")
	require.True(t, summary.HasSpec, "celbom declares a spec schema (the BOM)")
	require.True(t, summary.HasConfig, "celbom declares a config schema (the rule set)")

	// /api/kinds/{kind}/schema serves its spec + config schema.
	one, err := srv.getKindSchema(context.Background(), &kindSchemaInput{Kind: string(celbom.Kind)})
	require.NoError(t, err)
	require.NotNil(t, one.Body.SpecSchema, "spec_schema must be served")
	require.NotNil(t, one.Body.ConfigSchema, "config_schema must be served")

	// /docs OpenAPI components include the kind's synthetic component names.
	components := srv.OpenAPISpec().Components.Schemas.Map()
	require.Contains(t, components, componentName(model.Kind(celbom.Kind), 1, "Config"))
	require.Contains(t, components, componentName(model.Kind(celbom.Kind), 1, "Spec"))
}

// vpcKindVersionManifests loads the classic demo's vpc CRD at BOTH published kind versions (v1
// vpc.kind.json + v2 vpc-v2.kind.json) — the same bare kind at two web-API versions.
func vpcKindVersionManifests(t *testing.T) (v1, v2 model.KindManifest) {
	t.Helper()
	all, err := host.LoadKindFixtures("../../examples/demos/classic/testfixtures")
	require.NoError(t, err)
	var got1, got2 bool
	for _, m := range all {
		if m.Kind != model.Kind("vpc") {
			continue
		}
		switch {
		case m.KindVersion <= 1:
			v1, got1 = m, true
		case m.KindVersion == 2:
			v2, got2 = m, true
		}
	}
	require.True(t, got1, "vpc v1 manifest (vpc.kind.json) not found")
	require.True(t, got2, "vpc v2 manifest (vpc-v2.kind.json) not found")
	return v1, v2
}

// TestDeclaredSchemaSurface_MultiKindVersion pins the invariant that a kind published at
// MORE THAN ONE kindVersion surfaces EVERY kindVersion, not just the highest. The bug this
// guards: a per-kind (not per-(kind,kindVersion)) dedup in the declared set silently
// dropped v1's schema from /docs and the Kinds list once v2 was published — the
// spec would look complete while missing a whole version. Asserts (1) /docs carries
// both kind versions' components (unsuffixed v1 + V2-suffixed), (2) the Kinds list shows
// exactly ONE entry for the kind with KindVersions=[1,2], and (3) getKindSchema?kindVersion=2
// points its /docs refs at the V2 components while ?kindVersion=1 (default) points at v1.
func TestDeclaredSchemaSurface_MultiKindVersion(t *testing.T) {
	vpcV1, vpcV2 := vpcKindVersionManifests(t)
	kind := model.Kind("vpc")

	srv := &Server{}
	srv.SetDeclaredSchemas([]model.KindManifest{vpcV1, vpcV2})

	// (1) /docs components carry BOTH kind versions — v1 unsuffixed, v2 suffixed.
	components := srv.OpenAPISpec().Components.Schemas.Map()
	for _, axis := range []string{"Spec", "Status", "Config"} {
		require.Contains(t, components, componentName(kind, 1, axis), "v1 %s component missing from /docs", axis)
		require.Contains(t, components, componentName(kind, 2, axis), "v2 %s component missing from /docs", axis)
	}
	require.NotEqual(t, componentName(kind, 1, "Spec"), componentName(kind, 2, "Spec"),
		"each kindVersion must get a DISTINCT /docs component name")

	// (2) exactly ONE Kinds-list entry for vpc, and its KindVersions chip lists both.
	list, err := srv.listKindSchemas(context.Background(), nil)
	require.NoError(t, err)
	var entries []kindListEntry
	for _, e := range list.Body.Kinds {
		if e.Kind == string(kind) {
			entries = append(entries, e)
		}
	}
	require.Len(t, entries, 1, "a multi-kindVersion kind must appear as ONE list entry, not one per kindVersion")
	require.Equal(t, []int{1, 2}, entries[0].KindVersions, "the entry must chip every published kindVersion")

	// (3) getKindSchema resolves the /docs ref to the REQUESTED kindVersion's component.
	v1Schema, err := srv.getKindSchema(context.Background(), &kindSchemaInput{Kind: string(kind), KindVersion: 1})
	require.NoError(t, err)
	require.Equal(t, componentName(kind, 1, "Spec"), v1Schema.Body.SpecSchemaRef)
	require.Equal(t, []int{1, 2}, v1Schema.Body.KindVersions, "the schema response lists every published kindVersion")

	v2Schema, err := srv.getKindSchema(context.Background(), &kindSchemaInput{Kind: string(kind), KindVersion: 2})
	require.NoError(t, err)
	require.Equal(t, componentName(kind, 2, "Spec"), v2Schema.Body.SpecSchemaRef)
	require.Equal(t, componentName(kind, 2, "Config"), v2Schema.Body.ConfigSchemaRef,
		"v2's config ref must point at the V2 config component (default_region shape)")

	// An OVER-LARGE requested kindVersion clamps to the highest published (v2), never a
	// dangling VpcSpecV99 ref.
	v99, err := srv.getKindSchema(context.Background(), &kindSchemaInput{Kind: string(kind), KindVersion: 99})
	require.NoError(t, err)
	require.Equal(t, componentName(kind, 2, "Spec"), v99.Body.SpecSchemaRef,
		"a kindVersion beyond the published set must clamp to the highest published, not dangle")
}

// TestResolveDocKindVersion pins the /docs-ref kindVersion clamp: pick the highest published
// kindVersion ≤ requested, clamp an over-large request to the highest published, map
// 0/omitted to the LOWEST published (which may not be v1), and fall back to v1 when
// nothing is published. Guards against a dangling /docs#/schemas/{ref} link.
func TestResolveDocKindVersion(t *testing.T) {
	cases := []struct {
		name      string
		requested int
		published []int
		want      int
	}{
		{"omitted maps to lowest published", 0, []int{1, 2}, 1},
		{"omitted on a kind with no v1", 0, []int{2, 3}, 2},
		{"exact v1", 1, []int{1, 2}, 1},
		{"exact v2", 2, []int{1, 2}, 2},
		{"between publishes down to lower", 2, []int{1, 3}, 1},
		{"over-large clamps to highest", 99, []int{1, 2}, 2},
		// No published versions: echo the request back (NO phantom v1 default) — the
		// /docs ref is then a harmless miss rather than pointing at a fabricated v1.
		{"no published echoes the request (no v1 default)", 5, nil, 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, resolveDocKindVersion(c.requested, c.published))
		})
	}
}
