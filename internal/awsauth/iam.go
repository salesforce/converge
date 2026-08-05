// Package awsauth wires AWS IAM database authentication (RDS / Aurora) into the
// Postgres connection paths. Instead of a static password in the DSN, every new
// physical connection is opened with a short-lived IAM auth token minted from
// the pod's AWS credentials.
//
// Why a per-connection hook and not a one-shot password:
//
//   - An RDS IAM auth token is signed from the pod's AWS credentials and is only
//     valid for 15 minutes. The pod's credentials themselves rotate (IRSA web
//     identity, an instance/task role) — typically on a ~15m–1h cadence.
//   - pgx calls BeforeConnect before EVERY new physical connection: pool warm-up,
//     a health-check replacement, and the MaxConnLifetime recycle (30m, jittered).
//     Minting the token there means each connection carries a fresh token signed
//     with whatever credentials are current at that instant — so a credential
//     refresh is picked up automatically with no restart and no token cache to
//     invalidate.
//   - aws.Config.Credentials is a caching CredentialsProvider: Retrieve() returns
//     the cached value until it nears expiry, then transparently refreshes. So
//     minting per connection is cheap (a local HMAC presign, no network call)
//     except when the underlying creds actually roll over.
//
// The authenticator is constructed ONCE at boot (one aws.Config resolution) and
// shared by the primary pool, the read-replica pool, and the migrate path.
package awsauth

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
)

// IAMAuthenticator mints RDS/Aurora IAM auth tokens for Postgres connections. It
// is safe for concurrent use: BuildAuthToken only reads the (thread-safe) AWS
// credential provider and signs locally.
type IAMAuthenticator struct {
	// region the token is signed for. An RDS IAM token is region-scoped, so this
	// MUST match the database's region. Resolved from the AWS default chain (env
	// AWS_REGION/AWS_DEFAULT_REGION, shared config, IMDS).
	region string
	// creds is the pod's credential provider. It is a caching provider, so
	// Retrieve (called inside BuildAuthToken) refreshes transparently as the
	// pod's credentials rotate — that is what keeps long-lived pools authenticating
	// past the lifetime of any single set of credentials.
	creds aws.CredentialsProvider
}

// New resolves the AWS default config ONCE (env vars, shared config, IRSA
// web-identity, EC2/ECS role, IMDS) and returns an authenticator bound to its
// region and credential provider. It fails fast when no region can be resolved,
// since an RDS IAM token signed for the empty region is rejected at connect time
// — better to surface the misconfiguration at boot than on first query.
func New(ctx context.Context) (*IAMAuthenticator, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config for db iam auth: %w", err)
	}
	return NewWithCredentials(cfg.Region, cfg.Credentials)
}

// NewWithCredentials builds an authenticator from an explicit region and
// credential provider (the seam New resolves from the default chain). It
// validates both: an RDS IAM token is region-scoped and signed from credentials,
// so the empty region or a nil provider can never produce a usable token.
func NewWithCredentials(region string, creds aws.CredentialsProvider) (*IAMAuthenticator, error) {
	if region == "" {
		return nil, fmt.Errorf("db iam auth: no AWS region resolved (set AWS_REGION); an RDS IAM token is region-scoped")
	}
	if creds == nil {
		return nil, fmt.Errorf("db iam auth: no AWS credentials resolved from the default chain")
	}
	return &IAMAuthenticator{region: region, creds: creds}, nil
}

// Token mints a fresh IAM auth token for endpoint host:port and dbUser. The
// token is a presigned RDS URL string used as the connection password. It is
// valid for 15 minutes and signed with the credentials current at call time, so
// it must be generated per connection rather than cached across reconnects.
func (a *IAMAuthenticator) Token(ctx context.Context, host string, port uint16, dbUser string) (string, error) {
	endpoint := fmt.Sprintf("%s:%d", host, port)
	tok, err := auth.BuildAuthToken(ctx, endpoint, a.region, dbUser, a.creds)
	if err != nil {
		return "", fmt.Errorf("build rds iam auth token for %s@%s: %w", dbUser, endpoint, err)
	}
	return tok, nil
}

// BeforeConnect returns a pgx BeforeConnect hook that replaces the connection
// password with a freshly minted IAM token. The hook is passed a per-connection
// COPY of the ConnConfig (pgxpool and database/sql/stdlib both copy before
// calling it), so mutating cc.Password is safe and connection-local — it never
// rewrites the shared template. The DSN still supplies host, port, user, dbname
// and TLS settings; only the password is overridden.
//
// Note: Aurora IAM auth requires TLS (the DSN must use sslmode=require or
// stronger). This hook does not force it — that stays an explicit DSN choice so
// it is visible in config and not silently mutated here.
func (a *IAMAuthenticator) BeforeConnect() func(context.Context, *pgx.ConnConfig) error {
	return func(ctx context.Context, cc *pgx.ConnConfig) error {
		tok, err := a.Token(ctx, cc.Host, cc.Port, cc.User)
		if err != nil {
			return err
		}
		cc.Password = tok
		return nil
	}
}
