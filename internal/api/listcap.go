package api

import (
	"context"
	"log/slog"
)

// listcap.go bounds the "registry" list endpoints that have no client-supplied
// limit — kind manifests, reactor bindings, distinct kinds, cluster members.
// These are sized by CLUSTER CONFIG (how many kinds/bindings/members exist), not
// by attacker-inflatable user data, so they're low-risk; the cap here is
// defense-in-depth against a pathological/compromised DB returning a runaway set.
//
// CRITICAL: the cap is applied in the API HANDLER, NOT in the shared SQL query.
// ListKindManifests / ListReactorBindings / ListClusterMembers are ALSO read by
// the engine (KindManifestCache full load, the reactor dispatcher, the broker relay
// router) where a truncated result would silently starve routing/registration —
// a correctness bug. So the query stays complete; only the client-facing slice
// is bounded, and the truncation is surfaced (a WARN log), never silent.

// maxListItems caps a registry list response. Far above any real kind/binding/
// member count, so a healthy cluster is never affected; it only fires on a
// runaway set (bug/attack), bounding the response instead of streaming unbounded.
const maxListItems = 10000

// capList returns items truncated to maxListItems. `what` names the list for the
// truncation log (emitted only when truncation actually happens — no silent cap).
func capList[T any](ctx context.Context, what string, items []T) []T {
	if len(items) <= maxListItems {
		return items
	}
	slog.WarnContext(ctx, "list response truncated to the registry cap",
		"list", what, "total", len(items), "cap", maxListItems)
	return items[:maxListItems]
}
