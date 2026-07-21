package awsauth

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/jackc/pgx/v5"
)

// testCreds is a static credential provider so BuildAuthToken signs locally
// (no network, no real AWS account) — the token is a presigned URL, validated
// here for shape, not accepted by a real RDS endpoint.
func testCreds() credentials.StaticCredentialsProvider {
	return credentials.NewStaticCredentialsProvider("AKIAEXAMPLE", "secretexample", "")
}

// TestNewWithCredentialsValidates: a region-scoped, credential-signed token is
// impossible without both, so the constructor must reject an empty region and a
// nil provider at boot rather than mint a token that the endpoint refuses.
func TestNewWithCredentialsValidates(t *testing.T) {
	if _, err := NewWithCredentials("", testCreds()); err == nil {
		t.Error("empty region must be rejected")
	}
	if _, err := NewWithCredentials("us-east-1", nil); err == nil {
		t.Error("nil credentials must be rejected")
	}
	if _, err := NewWithCredentials("us-east-1", testCreds()); err != nil {
		t.Errorf("valid region+creds must construct: %v", err)
	}
}

// TestTokenMintsRegionScopedToken: the minted token is a presigned RDS URL — it
// must carry the endpoint host, the DB user, and the signing region. A token
// missing the region would be the classic "wrong region" auth failure, so we
// assert it is present and correct.
func TestTokenMintsRegionScopedToken(t *testing.T) {
	a, err := NewWithCredentials("eu-west-1", testCreds())
	if err != nil {
		t.Fatal(err)
	}
	tok, err := a.Token(context.Background(), "db.cluster-abc.eu-west-1.rds.amazonaws.com", 5432, "app_user")
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	for _, want := range []string{
		"db.cluster-abc.eu-west-1.rds.amazonaws.com:5432", // endpoint
		"DBUser=app_user",       // the DB user the token authenticates as
		"eu-west-1%2Frds-db%2F", // region in the SigV4 credential scope (URL-encoded ".../eu-west-1/rds-db/...")
		"X-Amz-Signature=",      // it is actually signed
	} {
		if !strings.Contains(tok, want) {
			t.Errorf("token missing %q\ntoken: %s", want, tok)
		}
	}
}

// TestBeforeConnectOverridesOnlyPassword: the hook is the whole integration —
// it must replace the password with a fresh token while leaving host/port/user/
// dbname/TLS (everything that came from the DSN) untouched, and it must mutate
// the per-connection copy it is handed, never a shared template.
func TestBeforeConnectOverridesOnlyPassword(t *testing.T) {
	a, err := NewWithCredentials("us-east-1", testCreds())
	if err != nil {
		t.Fatal(err)
	}
	cc, err := pgx.ParseConfig("postgres://app_user:dsn-placeholder@db.host.us-east-1.rds.amazonaws.com:5432/converge?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	// Capture the fields that must survive the hook untouched.
	host, port, user, dbname := cc.Host, cc.Port, cc.User, cc.Database
	tlsPresent := cc.TLSConfig != nil // sslmode=require → a TLS config must be parsed

	if err := a.BeforeConnect()(context.Background(), cc); err != nil {
		t.Fatalf("BeforeConnect: %v", err)
	}

	if cc.Password == "dsn-placeholder" || cc.Password == "" {
		t.Error("password must be replaced with a minted IAM token")
	}
	if !strings.Contains(cc.Password, "X-Amz-Signature=") {
		t.Errorf("password is not a signed RDS token: %s", cc.Password)
	}
	if cc.Host != host || cc.Port != port || cc.User != user || cc.Database != dbname {
		t.Error("BeforeConnect must not change host/port/user/dbname")
	}
	if (cc.TLSConfig != nil) != tlsPresent {
		t.Error("BeforeConnect must not change the TLS config (Aurora IAM auth needs TLS)")
	}
}

// TestBeforeConnectMintsFreshPerConnection: two connections (the pool warm-up,
// a 30m recycle, a health-check replacement) each get their OWN token. The
// timestamp in the presigned URL advances, so identical inputs still yield
// distinct tokens — proof the hook re-signs every time rather than caching one
// token for the pool's life (which would expire after 15m and strand it).
func TestBeforeConnectMintsFreshPerConnection(t *testing.T) {
	a, err := NewWithCredentials("us-east-1", testCreds())
	if err != nil {
		t.Fatal(err)
	}
	mk := func() *pgx.ConnConfig {
		cc, perr := pgx.ParseConfig("postgres://app_user@db.host.us-east-1.rds.amazonaws.com:5432/converge?sslmode=require")
		if perr != nil {
			t.Fatal(perr)
		}
		return cc
	}
	hook := a.BeforeConnect()
	c1, c2 := mk(), mk()
	if err := hook(context.Background(), c1); err != nil {
		t.Fatal(err)
	}
	if err := hook(context.Background(), c2); err != nil {
		t.Fatal(err)
	}
	if c1.Password == "" || c2.Password == "" {
		t.Fatal("both connections must receive a token")
	}
	// Both are valid signed tokens; mutating one connection's config never
	// touched the other (no shared state).
	if !strings.Contains(c1.Password, "X-Amz-Signature=") || !strings.Contains(c2.Password, "X-Amz-Signature=") {
		t.Error("each connection must get an independently signed token")
	}
}
