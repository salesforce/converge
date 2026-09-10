// Package spiffeauthz is the shared, domain-free SPIFFE-ID allowlist primitive:
// parse a comma-separated list of spiffe://… IDs into a matcher, and check a
// verified peer's certificate against it. Both the TLS-handshake authorizer
// (cmd/converge) and the broker's per-service Connect interceptors
// (internal/broker) build on it, so the parse rules and the "which SAN, exact
// match" semantics can't drift between the two enforcement points.
//
// Enforcement is always LAYERED ON TOP of mTLS chain verification — this package
// only decides whether an ALREADY-verified peer's identity is on the allowlist;
// it never establishes trust on its own.
package spiffeauthz

import (
	"crypto/x509"
	"fmt"
	"strings"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
)

// Matcher decides whether a SPIFFE ID is allowed. nil means "no allowlist
// configured" — authz disabled for that surface (chain trust only). A non-nil
// Matcher is safe for concurrent use (it closes over an immutable id set).
type Matcher func(spiffeid.ID) bool

// Parse turns a comma-separated list of SPIFFE IDs into a Matcher, or returns
// (nil, nil) when the list is empty (authz disabled). A malformed ID is a HARD
// error (fail fast at boot) rather than a dropped entry, so a typo can't
// silently widen — or empty — the allowlist. A trailing "/" on an entry is
// tolerated (trimmed) so a copy-pasted trust-domain-only ID matches.
func Parse(csv string) (Matcher, error) {
	csv = strings.TrimSpace(csv)
	if csv == "" {
		return nil, nil
	}
	parts := strings.Split(csv, ",")
	ids := make([]spiffeid.ID, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSuffix(strings.TrimSpace(p), "/")
		if p == "" {
			continue
		}
		id, err := spiffeid.FromString(p)
		if err != nil {
			return nil, fmt.Errorf("invalid SPIFFE ID %q in allowlist: %w", p, err)
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("SPIFFE allowlist was non-empty but yielded no valid SPIFFE IDs")
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id.String()] = struct{}{}
	}
	return func(got spiffeid.ID) bool {
		_, ok := set[got.String()]
		return ok
	}, nil
}

// Union returns a Matcher accepting any ID that ANY of ms accepts. A nil
// element is skipped. Union of only-nils (or none) is nil — i.e. no allowlist.
// Used where several audiences share ONE TLS listener: the handshake admits the
// union, and finer per-audience matchers narrow each RPC afterwards.
func Union(ms ...Matcher) Matcher {
	nonNil := make([]Matcher, 0, len(ms))
	for _, m := range ms {
		if m != nil {
			nonNil = append(nonNil, m)
		}
	}
	if len(nonNil) == 0 {
		return nil
	}
	return func(id spiffeid.ID) bool {
		for _, m := range nonNil {
			if m(id) {
				return true
			}
		}
		return false
	}
}

// CheckLeaf enforces m against a verified client-cert leaf: the leaf must carry
// at least one URI SAN, and at least one of those URI SANs must be a SPIFFE ID
// that m accepts. A nil Matcher is a no-op (returns nil). The caller supplies
// the leaf from a chain the TLS stack ALREADY verified (RequireAndVerifyClientCert
// or a Connect peer's verified chains) — this reads identity, it does not verify
// trust. The SPIFFE ID is read from the URI SAN (leaf.URIs), NOT CN/O.
func CheckLeaf(m Matcher, leaf *x509.Certificate) error {
	if m == nil {
		return nil
	}
	if leaf == nil {
		return fmt.Errorf("spiffe authz: no verified client certificate")
	}
	if len(leaf.URIs) == 0 {
		return fmt.Errorf("spiffe authz: client certificate has no URI SAN")
	}
	for _, uri := range leaf.URIs {
		id, err := spiffeid.FromURI(uri)
		if err != nil {
			continue // not a SPIFFE URI; try the next SAN
		}
		if m(id) {
			return nil // matched the allowlist
		}
	}
	return fmt.Errorf("spiffe authz: client identity not in the allowlist")
}
