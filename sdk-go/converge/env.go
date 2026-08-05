package converge

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// env.go reads the worker's environment configuration — the cloud-SDK "zero-config"
// path (aws.LoadDefaultConfig reads AWS_REGION/AWS_* the same way). A deployment sets the
// twelve-factor worker knobs and calls converge.Serve; Serve reads them here. This is the
// sole config path (there is no programmatic option surface).
//
// envSettings names the ENV variables and is parsed by kelseyhightower/envconfig — a
// stdlib-only leaf already used by cmd/converge, so the lean SDK gains no new
// third-party weight. envconfig TYPES + validates each value, so a malformed setting (a
// bad duration, a non-integer parallelism) is a LOUD error from New, not a silently
// dropped default. No `default:` tags: the Default* consts remain the single source of
// truth for defaults (config is seeded with them BEFORE this overlay), and a pointer
// field left nil = "unset, keep the seeded default" — only a variable the operator
// actually SET overrides.
type envSettings struct {
	BrokerAddr        *string        `envconfig:"BROKER_ADDR"`
	TLSCertFile       *string        `envconfig:"TLS_CERT_FILE"`
	TLSKeyFile        *string        `envconfig:"TLS_KEY_FILE"`
	BrokerCAFile      *string        `envconfig:"BROKER_CA_FILE"`
	TLSReloadInterval *time.Duration `envconfig:"TLS_RELOAD_INTERVAL"`
	MaxParallel       *int           `envconfig:"WORKER_MAX_PARALLEL"`
	DrainTimeout      *time.Duration `envconfig:"WORKER_DRAIN_TIMEOUT"`
}

// applyEnv parses the environment and overlays only the SET, well-formed variables onto
// cfg (cfg is pre-seeded with the Default* consts, so an unset var keeps its default).
// It errors if a SET variable is malformed (envconfig reports the offending var +
// expected type) — bad config fails loud at New, never silently falls back.
func applyEnv(cfg *config) error {
	var s envSettings
	if err := envconfig.Process("", &s); err != nil {
		return fmt.Errorf("converge: invalid worker environment config: %w", err)
	}
	if s.BrokerAddr != nil {
		cfg.brokerAddr = *s.BrokerAddr
	}
	if s.TLSCertFile != nil {
		cfg.tlsCertFile = *s.TLSCertFile
	}
	if s.TLSKeyFile != nil {
		cfg.tlsKeyFile = *s.TLSKeyFile
	}
	if s.BrokerCAFile != nil {
		cfg.tlsCAFile = *s.BrokerCAFile
	}
	if s.TLSReloadInterval != nil {
		cfg.tlsReloadInterval = *s.TLSReloadInterval
	}
	if s.MaxParallel != nil {
		cfg.maxInflight = *s.MaxParallel
	}
	if s.DrainTimeout != nil {
		cfg.drainGrace = *s.DrainTimeout
	}
	return nil
}
