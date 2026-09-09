package conctl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/salesforce/converge/internal/conctl/apiclient"
)

// objectType is the CLI's notion of a control-plane object kind (NOT a converge
// resource `kind` — those are runtime kinds like account/vpc). It selects the
// API endpoint an apply/get/list/delete targets.
type objectType string

const (
	typeResource       objectType = "resource"
	typeManifest       objectType = "manifest" // kind CRD
	typeProviderConfig objectType = "providerconfig"
	typeReactorBinding objectType = "reactorbinding"
	typeCluster        objectType = "cluster"
)

// typeAliases maps the accepted CLI nouns (singular/plural/short) to a canonical
// objectType, so `conctl get resources`, `... resource`, `... res` all resolve.
// Mirrors kubectl's resource-name flexibility.
var typeAliases = map[string]objectType{
	"resource": typeResource, "resources": typeResource, "res": typeResource,
	"manifest": typeManifest, "manifests": typeManifest,
	"kind": typeManifest, "kinds": typeManifest, "crd": typeManifest, "crds": typeManifest,
	"providerconfig": typeProviderConfig, "providerconfigs": typeProviderConfig,
	"config": typeProviderConfig, "configs": typeProviderConfig, "pc": typeProviderConfig,
	"reactorbinding": typeReactorBinding, "reactorbindings": typeReactorBinding,
	"binding": typeReactorBinding, "bindings": typeReactorBinding, "rb": typeReactorBinding,
	"cluster": typeCluster, "clusters": typeCluster, "members": typeCluster, "nodes": typeCluster,
}

// resolveType maps a user-supplied noun to a canonical objectType.
func resolveType(noun string) (objectType, error) {
	if t, ok := typeAliases[strings.ToLower(noun)]; ok {
		return t, nil
	}
	return "", fmt.Errorf("unknown object type %q; want one of: resource, manifest, providerconfig, reactorbinding, cluster", noun)
}

// splitKindName parses a resource reference "<kind>/<name>" into its parts. A
// resource is addressed by its public (kind, name); the internal uuid is never
// used. Splits on the FIRST '/': '/' is the kind/name delimiter and neither a kind
// nor a name contains one (names use hyphens as their own separator), so the first
// '/' unambiguously ends the kind.
func splitKindName(ref string) (kind, name string, err error) {
	kind, name, ok := strings.Cut(ref, "/")
	if !ok || kind == "" || name == "" {
		return "", "", fmt.Errorf("resource reference must be <kind>/<name>, got %q", ref)
	}
	return kind, name, nil
}

// applyDoc is the thin envelope conctl apply reads to route a manifest to the
// right endpoint. `type` is the CLI object type (resource|manifest|
// providerconfig|reactorbinding); the rest of the document is the endpoint's
// request body, passed through verbatim. `type` may be omitted when --type is
// given on the command line. This disambiguates resource vs providerconfig,
// which share kind/name/spec.
type applyDoc struct {
	Type string `json:"type,omitempty"`
}

func newApplyCommand(o *globalOpts) *cobra.Command {
	var files []string
	var typeOverride string

	cmd := &cobra.Command{
		Use:   "apply -f FILE [-f FILE ...]",
		Short: "Apply a manifest (create or update) from a file, - for stdin",
		Long: `Apply reads one or more JSON/YAML manifests and upserts each against the
control plane — the create-or-update path, like 'kubectl apply -f'.

Each document routes to an endpoint by its object type. Give the type either
with a top-level "type" field in the document:

    { "type": "providerconfig", "name": "cc", "kind": "stdstarlark",
      "is_default": true, "spec": {}, "data": "<base64>" }

or with --type on the command line (applies to every document in the files):

    conctl apply --type resource -f bom.json

Accepted types: resource, manifest (kind CRD), providerconfig, reactorbinding.
Resource and providerconfig share kind/name/spec, so one of the two forms of
type is REQUIRED to tell them apart. -f - reads from stdin.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if len(files) == 0 {
				return fmt.Errorf("no manifest given; use -f FILE (or -f - for stdin)")
			}
			var override objectType
			if typeOverride != "" {
				t, err := resolveType(typeOverride)
				if err != nil {
					return err
				}
				override = t
			}
			client, err := o.client()
			if err != nil {
				return err
			}
			ctx, cancel := o.ctx(cmd.Context())
			defer cancel()
			for _, f := range files {
				if err := applyFile(ctx, client, f, override); err != nil {
					return fmt.Errorf("apply %s: %w", f, err)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringArrayVarP(&files, "filename", "f", nil,
		"manifest file to apply (repeatable); - for stdin")
	cmd.Flags().StringVar(&typeOverride, "type", "",
		"object type for every document (resource|manifest|providerconfig|reactorbinding); overrides the document's type field")
	return cmd
}

// applyFile reads one file (which may hold a single JSON/YAML document), routes
// it by type, and POSTs/PUTs it. YAML is normalized to JSON first so the same
// bytes feed the JSON API regardless of the on-disk format.
func applyFile(ctx context.Context, client *apiclient.ClientWithResponses, path string, override objectType) error {
	raw, err := readInput(path)
	if err != nil {
		return err
	}
	// Accept YAML or JSON on disk; the API speaks JSON. yaml.YAMLToJSON is a
	// no-op-ish passthrough for already-JSON input.
	jsonBytes, err := yaml.YAMLToJSON(raw)
	if err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}

	typ := override
	if typ == "" {
		var env applyDoc
		if err := json.Unmarshal(jsonBytes, &env); err != nil {
			return fmt.Errorf("read type from manifest: %w", err)
		}
		if env.Type == "" {
			return fmt.Errorf("manifest has no \"type\" field and --type was not given; " +
				"add \"type\": \"resource|manifest|providerconfig|reactorbinding\" or pass --type")
		}
		typ, err = resolveType(env.Type)
		if err != nil {
			return err
		}
	}

	// Forward the body VERBATIM, including any "type" tag: the apply endpoints accept
	// an optional, enum-validated "type" (it matches the endpoint the CLI routes to),
	// so the self-describing field flows all the way through — no strip. A file that
	// carries "type" applies with or without --type; one that omits it needs --type.
	switch typ {
	case typeResource:
		return applyResource(ctx, client, jsonBytes)
	case typeProviderConfig:
		return applyProviderConfig(ctx, client, jsonBytes)
	case typeReactorBinding:
		return applyReactorBinding(ctx, client, jsonBytes)
	case typeManifest:
		return applyManifest(ctx, client, jsonBytes)
	case typeCluster:
		return fmt.Errorf("cluster members are read-only; nothing to apply")
	default:
		return fmt.Errorf("cannot apply object type %q", typ)
	}
}

func applyResource(ctx context.Context, client *apiclient.ClientWithResponses, body []byte) error {
	resp, err := client.PostApiV1ResourcesWithBodyWithResponse(ctx, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	r, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	if err != nil {
		return err
	}
	fmt.Printf("resource %s/%s %s (generation %d)\n", r.Kind, r.Name,
		applyResult(resp.HTTPResponse), r.Generation)
	return nil
}

func applyProviderConfig(ctx context.Context, client *apiclient.ClientWithResponses, body []byte) error {
	resp, err := client.PostApiV1ProviderconfigsWithBodyWithResponse(ctx, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	c, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	if err != nil {
		return err
	}
	fmt.Printf("providerconfig %s (kind %s) %s\n", c.Name, c.Kind, applyResult(resp.HTTPResponse))
	return nil
}

func applyReactorBinding(ctx context.Context, client *apiclient.ClientWithResponses, body []byte) error {
	resp, err := client.PostApiV1ReactorBindingsWithBodyWithResponse(ctx, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	b, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	if err != nil {
		return err
	}
	fmt.Printf("reactorbinding %s (%s %s → %s) %s\n", b.Name, b.WatchKind, b.Transition, b.Reactor,
		applyResult(resp.HTTPResponse))
	return nil
}

// applyManifest applies a kind CRD via PUT /api/v1/kinds/{kind}/manifest. The
// kind is read from the document body (the endpoint keys on the path segment).
func applyManifest(ctx context.Context, client *apiclient.ClientWithResponses, body []byte) error {
	var head struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(body, &head); err != nil {
		return fmt.Errorf("read kind from manifest: %w", err)
	}
	if head.Kind == "" {
		return fmt.Errorf("kind manifest has no \"kind\" field")
	}
	resp, err := client.PutApiV1KindsByKindManifestWithBodyWithResponse(ctx, head.Kind, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	m, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	if err != nil {
		return err
	}
	fmt.Printf("manifest %s %s (manifest_version %d)\n", head.Kind,
		applyResult(resp.HTTPResponse), m.ManifestVersion)
	return nil
}

// applyResult reads the X-Apply-Result header the API sets (created/configured/
// unchanged) so the CLI echoes the same verb kubectl does. Falls back to
// "configured" when the header is absent.
func applyResult(resp *http.Response) string {
	if resp != nil {
		if v := resp.Header.Get("X-Apply-Result"); v != "" {
			return v
		}
	}
	return "configured"
}

// readInput reads a manifest file, or stdin when path is "-".
func readInput(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}
	return b, nil
}
