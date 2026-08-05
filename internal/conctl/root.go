// Package conctl implements the converge control-plane CLI — a kubectl-style
// client for the REST API (apply/get/list/delete over resources, kind manifests,
// provider configs, reactor bindings, and the cluster registry).
//
// The wire layer is the OpenAPI-generated client in internal/conctl/apiclient
// (regenerated from the server's golden spec via `just codegen-cli`), so the CLI
// can never drift from the API contract. This package holds only the command
// tree, flag plumbing, and human/JSON/YAML rendering on top of that client.
package conctl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/salesforce/converge/internal/conctl/apiclient"
)

// BuildVersion is stamped at build time via -ldflags (mirrors cmd/converge).
var BuildVersion = "dev"

// globalOpts holds the flags shared by every command, bound on the root.
type globalOpts struct {
	server  string        // API base URL
	output  string        // table | json | yaml
	timeout time.Duration // per-request deadline

	// TLS. All optional: with none set the client uses plain HTTP (or system
	// roots for an https:// server). tlsCert+tlsKey present a client keypair for
	// mTLS; tlsCA verifies the server against a custom CA (else system roots);
	// insecure disables server-cert verification entirely (dev/self-signed only).
	tlsCert  string
	tlsKey   string
	tlsCA    string
	insecure bool
}

// defaultServer is used when neither --server nor $CONVERGE_SERVER is set. It
// matches the API server's default bind and the demos' curl target.
const defaultServer = "http://localhost:8080"

// NewRootCommand assembles the full command tree. main() calls Execute on it.
func NewRootCommand() *cobra.Command {
	opts := &globalOpts{}

	root := &cobra.Command{
		Use:   "conctl",
		Short: "Control-plane CLI for the converge orchestrator",
		Long: `conctl is a kubectl-style client for the converge control-plane REST API.

It applies, gets, lists, and deletes the control-plane objects — resources,
kind manifests (CRDs), provider configs, reactor bindings — and shows the
running cluster registry. The API endpoint defaults to ` + defaultServer + `;
override it with --server or the CONVERGE_SERVER environment variable.

Output is a human table by default; -o json or -o yaml emit machine-readable
documents for scripting (jq/yq).`,
		SilenceUsage:  true, // a runtime error is not a usage error; don't dump help
		SilenceErrors: true, // we print errors ourselves in main()
		Version:       BuildVersion,
	}

	// Resolve --server from the flag, else $CONVERGE_SERVER, else the default.
	root.PersistentFlags().StringVar(&opts.server, "server", "",
		"converge API base URL (default $CONVERGE_SERVER or "+defaultServer+")")
	root.PersistentFlags().StringVarP(&opts.output, "output", "o", "table",
		"output format: table | json | yaml")
	root.PersistentFlags().DurationVar(&opts.timeout, "timeout", 30*time.Second,
		"per-request timeout")
	root.PersistentFlags().StringVar(&opts.tlsCert, "tls-cert", "",
		"client TLS certificate (PEM) for mTLS; requires --tls-key (env TLS_CERT_FILE)")
	root.PersistentFlags().StringVar(&opts.tlsKey, "tls-key", "",
		"client TLS private key (PEM) for mTLS; requires --tls-cert (env TLS_KEY_FILE)")
	root.PersistentFlags().StringVar(&opts.tlsCA, "tls-ca", "",
		"CA bundle (PEM) to verify the server cert against; default is the system roots (env TLS_CA_FILE)")
	root.PersistentFlags().BoolVar(&opts.insecure, "insecure", false,
		"skip server TLS certificate verification (dev/self-signed only)")

	root.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		if opts.server == "" {
			if env := os.Getenv("CONVERGE_SERVER"); env != "" {
				opts.server = env
			} else {
				opts.server = defaultServer
			}
		}
		// TLS material falls back to env vars so a shell can export it once for a
		// session. Names mirror the server's TLS_*_FILE scheme (cmd/converge):
		// TLS_CERT_FILE/TLS_KEY_FILE are the client keypair; TLS_CA_FILE is the CA
		// this client verifies the SERVER against (the client-side parallel of the
		// server's TLS_CLIENT_CA_FILE, which verifies clients).
		opts.tlsCert = orEnv(opts.tlsCert, "TLS_CERT_FILE")
		opts.tlsKey = orEnv(opts.tlsKey, "TLS_KEY_FILE")
		opts.tlsCA = orEnv(opts.tlsCA, "TLS_CA_FILE")

		switch opts.output {
		case outputTable, outputJSON, outputYAML:
		default:
			return fmt.Errorf("invalid --output %q: want table, json, or yaml", opts.output)
		}
		// A client keypair needs both halves.
		if (opts.tlsCert == "") != (opts.tlsKey == "") {
			return fmt.Errorf("--tls-cert and --tls-key must be given together (client mTLS needs both the cert and its key)")
		}
		// --insecure disables the verification --tls-ca configures; refuse the
		// contradiction rather than silently letting one win.
		if opts.insecure && opts.tlsCA != "" {
			return fmt.Errorf("--insecure and --tls-ca are mutually exclusive: --insecure skips the verification --tls-ca would perform")
		}
		return nil
	}

	root.AddCommand(
		newApplyCommand(opts),
		newSyncCommand(opts),
		newGetCommand(opts),
		newListCommand(opts),
		newDeleteCommand(opts),
		newClusterCommand(opts),
	)
	return root
}

// client builds a ClientWithResponses against the resolved server, wiring in a
// TLS-configured HTTP client when any TLS flag is set. Kept on globalOpts so
// every command creates its client the same way.
func (o *globalOpts) client() (*apiclient.ClientWithResponses, error) {
	httpClient, err := o.httpClient()
	if err != nil {
		return nil, err
	}
	c, err := apiclient.NewClientWithResponses(o.server, apiclient.WithHTTPClient(httpClient))
	if err != nil {
		return nil, fmt.Errorf("build API client for %s: %w", o.server, err)
	}
	return c, nil
}

// httpClient builds the underlying *http.Client. When no TLS material is
// configured and verification isn't skipped it returns a default client (plain
// HTTP, or system-root verification for an https:// server) — matching the
// project's remoteWorkerHTTPClient pattern. Otherwise it constructs a
// tls.Config: a client keypair for mTLS (tlsCert/tlsKey), a custom server-CA
// pool (tlsCA), and/or InsecureSkipVerify (insecure).
func (o *globalOpts) httpClient() (*http.Client, error) {
	if o.tlsCert == "" && o.tlsKey == "" && o.tlsCA == "" && !o.insecure {
		return http.DefaultClient, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if o.tlsCert != "" { // presence of both is enforced in PersistentPreRunE
		cert, err := tls.LoadX509KeyPair(o.tlsCert, o.tlsKey)
		if err != nil {
			return nil, fmt.Errorf("load client keypair (%q, %q): %w", o.tlsCert, o.tlsKey, err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	if o.tlsCA != "" {
		pem, err := os.ReadFile(o.tlsCA)
		if err != nil {
			return nil, fmt.Errorf("read server CA %q: %w", o.tlsCA, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("server CA %q: no certificates parsed", o.tlsCA)
		}
		tlsCfg.RootCAs = pool
	}
	if o.insecure {
		tlsCfg.InsecureSkipVerify = true
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}, nil
}

// ctx derives a per-request context with the --timeout deadline off the parent.
func (o *globalOpts) ctx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, o.timeout)
}

// orEnv returns flagVal if non-empty, else the value of environment variable
// envKey (which may itself be empty).
func orEnv(flagVal, envKey string) string {
	if flagVal != "" {
		return flagVal
	}
	return os.Getenv(envKey)
}
