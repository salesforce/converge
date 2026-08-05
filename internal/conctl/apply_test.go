package conctl

import (
	"testing"
)

func TestResolveType(t *testing.T) {
	cases := map[string]objectType{
		"resource": typeResource, "resources": typeResource, "res": typeResource,
		"manifest": typeManifest, "kinds": typeManifest, "crd": typeManifest,
		"providerconfig": typeProviderConfig, "configs": typeProviderConfig, "pc": typeProviderConfig,
		"reactorbinding": typeReactorBinding, "bindings": typeReactorBinding, "rb": typeReactorBinding,
		"cluster": typeCluster, "nodes": typeCluster,
		"RESOURCE": typeResource, // case-insensitive
	}
	for in, want := range cases {
		got, err := resolveType(in)
		if err != nil {
			t.Errorf("resolveType(%q) unexpected error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("resolveType(%q) = %q, want %q", in, got, want)
		}
	}
	if _, err := resolveType("widget"); err == nil {
		t.Error("resolveType(\"widget\") should error on an unknown type")
	}
}
