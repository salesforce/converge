package conctl

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"

	"github.com/salesforce/converge/internal/conctl/apiclient"
)

// syncConfigFile is the per-directory config that names the ownership SCOPE for
// every manifest at or below it — the converge analogue of Flux's per-directory
// `kustomization.yaml` or Helm's `Chart.yaml`. It carries one required field, the
// app name, which becomes the tracking-label VALUE conctl stamps on every resource
// it applies from that scope. A nested directory may carry its own converge.yaml to
// start a new scope; the NEAREST ancestor wins (like the nearest kustomization).
const syncConfigFile = "converge.yaml"

// syncAppLabel is the RESERVED label key conctl owns for GitOps ownership tracking
// (mirroring Argo CD's app.kubernetes.io/instance / tracking-id). conctl SETS it on
// every synced resource to the app name from the governing converge.yaml; prune then
// lists by exactly this key=value and deletes the resources it owns that the current
// manifests no longer declare. Users never write this key by hand — a manifest that
// sets it is overwritten by the sync scope.
const syncAppLabel = "converge.sh/app"

// syncPruneOptOutLabel, when set to "false" on a resource, exempts it from prune —
// the per-resource escape hatch (Argo CD's Prune=false). A resource carrying it is
// applied and tracked but never auto-deleted, even when it drops out of the manifests.
const syncPruneOptOutLabel = "converge.sh/prune"

// syncConfig is the parsed converge.yaml. `app` is the ownership scope name; it MUST
// be globally unique across the tree — two directories declaring the same app merge
// into one scope and can prune each other's resources, so conctl warns on a collision.
type syncConfig struct {
	App string `json:"app"`
}

// resourceRef identifies a resource by its public (kind, name) — the handle prune
// diffs on. The internal uuid and kind_version are irrelevant to "does this manifest
// still exist": a resource is the same resource across versions.
type resourceRef struct {
	kind, name string
}

func (r resourceRef) String() string { return r.kind + "/" + r.name }

// syncManifest is one on-disk manifest resolved to its type, its owning app scope
// (empty for CRDs, which are cluster-wide and never pruned), and its parsed body.
type syncManifest struct {
	path string
	typ  objectType
	app  string          // "" for manifests (CRDs) — not app-scoped
	ref  resourceRef     // (kind, name) for resources; zero for others
	body json.RawMessage // the manifest JSON (label stamped in for app-scoped types)
}

func newSyncCommand(o *globalOpts) *cobra.Command {
	var (
		prune  bool
		dryRun bool
	)
	cmd := &cobra.Command{
		Use:   "sync [DIR]",
		Short: "Apply every manifest under a directory tree, GitOps-style",
		Long: `Sync recursively applies every manifest under DIR (default ".") to the control
plane — the GitOps CD loop, like 'kubectl apply -f DIR -R' with Argo-CD-style
ownership tracking and prune.

DIR is a plain directory tree (check it out of Git first, then 'conctl sync .').
Each subtree is OWNED by the nearest-ancestor ` + syncConfigFile + `, a one-line
config naming the app scope:

    # ` + syncConfigFile + `
    app: payments-platform

Every resource conctl applies from that scope is stamped with the reserved label
` + syncAppLabel + `=<app>. With --prune, conctl then lists the resources it owns
(that label) and DELETES the ones the current manifests no longer declare — a file
removed from the tree, or a directory that lost its manifests, means "deleted".
Prune only ever touches resources conctl itself stamped, never hand-applied ones.

    conctl sync ./manifests                 # apply everything (no deletes)
    conctl sync ./manifests --prune         # apply + delete resources dropped from the tree
    conctl sync ./manifests --prune --dry-run   # show the plan, change nothing

Every .json / .yaml / .yml file under the tree (except ` + syncConfigFile + ` itself)
is a manifest. Since there is NO filename convention, each is REQUIRED to be
SELF-DESCRIBING via a top-level "type" (the same envelope 'conctl apply' reads); a file
without one is an error. Accepted "type" values:

    manifest        a kind CRD
    providerconfig  a provider config
    reactorbinding  a reactor binding
    resource        a resource (the only app-scoped, prunable type)

    { "type": "manifest",       "kind": "vpc", "kind_version": 1, ... }
    { "type": "resource",       "kind": "vpc", "name": "prod", "spec": {} }

Manifests apply in dependency order regardless of file order: manifest, then
providerconfig, then reactorbinding, then resource. Only "resource" documents are
app-scoped and prunable; the other three are applied but never stamped or deleted.
A resource outside an ` + syncConfigFile + ` scope is applied but cannot be pruned
(nothing owns it); conctl warns so the gap is visible.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			root := "."
			if len(args) == 1 {
				root = args[0]
			}
			return runSync(cmd.Context(), o, root, prune, dryRun)
		},
	}
	cmd.Flags().BoolVar(&prune, "prune", false,
		"delete resources this app owns (by the "+syncAppLabel+" label) that the manifests no longer declare")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false,
		"print the apply/prune plan without changing anything")
	return cmd
}

func runSync(parent context.Context, o *globalOpts, root string, prune, dryRun bool) error {
	manifests, appDirs, err := collectManifests(root)
	if err != nil {
		return err
	}
	if len(manifests) == 0 {
		return fmt.Errorf("no manifests found under %s (expected .json/.yaml/.yml files, each with a \"type\" field)", root)
	}
	// A resource with no governing converge.yaml can't be pruned — nothing owns it.
	// Warn so the coverage gap is visible before a prune (only resources are scoped).
	warnUnscoped(manifests)

	client, err := o.client()
	if err != nil {
		return err
	}
	ctx, cancel := o.ctx(parent)
	defer cancel()

	// 1. APPLY every manifest. CRDs first so a resource's kind is registered before
	//    the resource lands (the same ordering the demos' apply loops use).
	sort.SliceStable(manifests, func(i, j int) bool {
		return applyRank(manifests[i].typ) < applyRank(manifests[j].typ)
	})
	// applied tracks, per app scope, the (kind, name) set we declared this run — the
	// desired set prune diffs the live-owned set against.
	applied := map[string]map[resourceRef]bool{}
	for _, m := range manifests {
		if m.app != "" && m.typ == typeResource {
			if applied[m.app] == nil {
				applied[m.app] = map[resourceRef]bool{}
			}
			applied[m.app][m.ref] = true
		}
		if dryRun {
			fmt.Printf("would apply %-14s %s%s\n", m.typ, m.path, appSuffix(m.app))
			continue
		}
		if err := applySyncManifest(ctx, client, m); err != nil {
			return fmt.Errorf("apply %s: %w", m.path, err)
		}
	}

	if !prune {
		return nil
	}

	// 2. PRUNE per app scope: list the resources we own (the app label), delete the
	//    ones the current manifests no longer declare. A scope present in the tree
	//    with zero resources still prunes (all its old resources are now "deleted").
	apps := sortedKeys(appDirs)
	for _, app := range apps {
		desired := applied[app] // may be nil → every owned resource is pruned
		if err := pruneApp(ctx, client, app, desired, dryRun); err != nil {
			return fmt.Errorf("prune app %q: %w", app, err)
		}
	}
	return nil
}

// collectManifests walks root, resolving each recognized manifest to its type and
// owning app scope (nearest-ancestor converge.yaml). It returns the manifests plus
// the set of app scopes discovered (so prune runs for every app in the tree, even
// one whose directory now holds no resources).
func collectManifests(root string) (manifests []syncManifest, appDirs map[string]bool, err error) {
	// Clean root ONCE so the ancestor-walk boundary is reliable: WalkDir yields
	// cleaned paths and filepath.Dir returns cleaned paths, so an uncleaned root
	// ("./manifests", "manifests/") would never string-equal the walked dirs and
	// resolveApp would climb ABOVE the sync root reading a stray converge.yaml — a
	// scoping leak. Cleaning makes `d == root` the exact top-of-tree stop.
	root = filepath.Clean(root)
	info, err := os.Stat(root)
	if err != nil {
		return nil, nil, fmt.Errorf("stat %s: %w", root, err)
	}
	if !info.IsDir() {
		return nil, nil, fmt.Errorf("%s is not a directory", root)
	}
	appDirs = map[string]bool{}
	// appToDir records the first directory that declared each app name, so a second
	// directory declaring the SAME app can be warned about (two scopes sharing a name
	// merge and can prune each other's resources — a globally-unique app is the rule).
	appToDir := map[string]string{}
	// appOf resolves the governing app for a file: the nearest converge.yaml walking
	// UP from the file's directory to root. Cached per directory.
	dirApp := map[string]string{}
	resolveApp := func(dir string) (string, error) {
		if a, ok := dirApp[dir]; ok {
			return a, nil
		}
		// Walk from `dir` up to (and including) root looking for the config.
		app := ""
		for d := dir; ; d = filepath.Dir(d) {
			cfgPath := filepath.Join(d, syncConfigFile)
			if b, rerr := os.ReadFile(cfgPath); rerr == nil {
				var cfg syncConfig
				if uerr := yaml.Unmarshal(b, &cfg); uerr != nil {
					return "", fmt.Errorf("parse %s: %w", cfgPath, uerr)
				}
				if strings.TrimSpace(cfg.App) == "" {
					return "", fmt.Errorf("%s: `app` is required and must be non-empty", cfgPath)
				}
				app = cfg.App
				// A second converge.yaml (in a DIFFERENT directory) declaring the same
				// app name merges the two scopes — they then prune against each other's
				// union. App names are meant to be globally unique across the tree, so
				// surface the collision rather than let it silently merge.
				if prev, seen := appToDir[app]; seen && prev != d {
					fmt.Fprintf(os.Stderr, "warning: app %q declared in both %s and %s — the scopes merge and prune against each other\n",
						app, filepath.Join(prev, syncConfigFile), cfgPath)
				} else if !seen {
					appToDir[app] = d
				}
				break
			}
			if d == root || d == filepath.Dir(d) {
				break // reached the tree root (or filesystem root) with no config
			}
		}
		dirApp[dir] = app
		return app, nil
	}

	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			return nil
		}
		// The ONLY filename convention is converge.yaml (the scope config); it is not
		// a manifest. Every other .json/.yaml/.yml file is a manifest, classified by
		// its "type" field (below), not its name.
		if d.Name() == syncConfigFile || !isManifestFile(d.Name()) {
			return nil
		}
		raw, rerr := os.ReadFile(path)
		if rerr != nil {
			return fmt.Errorf("read %s: %w", path, rerr)
		}
		jsonBytes, jerr := yaml.YAMLToJSON(raw)
		if jerr != nil {
			return fmt.Errorf("parse %s: %w", path, jerr)
		}
		// Route by the self-describing "type" envelope — the same field 'conctl apply'
		// reads. There is no filename convention, so a manifest with no "type" cannot
		// be routed; that is a hard error, not a silent skip.
		var env applyDoc
		if uerr := json.Unmarshal(jsonBytes, &env); uerr != nil {
			return fmt.Errorf("read type from %s: %w", path, uerr)
		}
		if env.Type == "" {
			return fmt.Errorf("%s has no \"type\" field: sync manifests must be self-describing "+
				"(\"type\": \"resource|manifest|providerconfig|reactorbinding\") — there is no filename convention", path)
		}
		typ, terr := resolveType(env.Type)
		if terr != nil {
			return fmt.Errorf("%s: %w", path, terr)
		}
		// Forward the body VERBATIM including its "type" tag — the apply endpoints
		// accept the optional, enum-validated "type", so it flows through unchanged
		// (label-stamping below only rewrites `labels`, on resources).
		app, aerr := resolveApp(filepath.Dir(path))
		if aerr != nil {
			return aerr
		}
		m := syncManifest{path: path, typ: typ, body: jsonBytes}
		// Only RESOURCES carry the ownership label and participate in prune: the
		// resources table has a labels column, the API's list filters by it, and a
		// resource is the unit sync creates/deletes. CRDs are cluster-wide (applied,
		// never scoped/pruned); providerconfigs and reactor bindings have no labels
		// field on their apply body and are managed by whatever owns them — applied
		// as-is, not stamped, not pruned.
		if typ == typeResource {
			m.app = app
			if app != "" {
				appDirs[app] = true
				stamped, ref, lerr := stampAppLabel(m.body, app)
				if lerr != nil {
					return fmt.Errorf("%s: %w", path, lerr)
				}
				m.body, m.ref = stamped, ref
			}
		}
		manifests = append(manifests, m)
		return nil
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
	return manifests, appDirs, nil
}

// isManifestFile reports whether a filename is a manifest by EXTENSION only — the
// only filename rule sync applies (converge.yaml, the scope config, is filtered out
// by the caller). A file's OBJECT type comes from its "type" field, never its name.
func isManifestFile(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".json", ".yaml", ".yml":
		return true
	default:
		return false
	}
}

// stampAppLabel injects the ownership label (syncAppLabel=app) into the manifest's
// `labels` map and returns the rewritten body plus the resource's (kind, name). A
// user-set value for the reserved key is overwritten — conctl owns it. It also reads
// kind/name so the caller can track the desired set for prune.
func stampAppLabel(jsonBytes []byte, app string) (json.RawMessage, resourceRef, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(jsonBytes, &doc); err != nil {
		return nil, resourceRef{}, fmt.Errorf("parse manifest object: %w", err)
	}
	labels := map[string]string{}
	if raw, ok := doc["labels"]; ok {
		if err := json.Unmarshal(raw, &labels); err != nil {
			return nil, resourceRef{}, fmt.Errorf("parse labels: %w", err)
		}
	}
	labels[syncAppLabel] = app // conctl OWNS this key; overwrite any user value
	lb, err := json.Marshal(labels)
	if err != nil {
		return nil, resourceRef{}, err
	}
	doc["labels"] = lb
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, resourceRef{}, err
	}
	var head struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(jsonBytes, &head); err != nil {
		return nil, resourceRef{}, fmt.Errorf("read kind/name: %w", err)
	}
	// A resource must have both to be applied AND to be tracked for prune — a blank
	// kind/name would collide with any other blank one in the desired set and mask a
	// malformed manifest. Fail loudly here rather than send a broken apply.
	if head.Kind == "" || head.Name == "" {
		return nil, resourceRef{}, fmt.Errorf("resource manifest missing kind and/or name")
	}
	return out, resourceRef{kind: head.Kind, name: head.Name}, nil
}

// applySyncManifest routes an already-classified, already-stamped manifest to the
// same per-type apply the `apply` command uses — one code path for both.
func applySyncManifest(ctx context.Context, client *apiclient.ClientWithResponses, m syncManifest) error {
	switch m.typ {
	case typeResource:
		return applyResource(ctx, client, m.body)
	case typeProviderConfig:
		return applyProviderConfig(ctx, client, m.body)
	case typeReactorBinding:
		return applyReactorBinding(ctx, client, m.body)
	case typeManifest:
		return applyManifest(ctx, client, m.body)
	default:
		return fmt.Errorf("cannot sync object type %q", m.typ)
	}
}

// pruneApp deletes the resources owned by app (the syncAppLabel) that the current
// manifests no longer declare. It lists the live-owned set by label, diffs against
// desired, and deletes the difference — honoring the per-resource prune opt-out.
func pruneApp(ctx context.Context, client *apiclient.ClientWithResponses, app string, desired map[resourceRef]bool, dryRun bool) error {
	// List every resource carrying our ownership label for this app. The label
	// filter format on the wire is key:value (see the API's parseLabels). The list
	// endpoint caps limit at maxListLimit; an app owning more than that is beyond
	// what one prune pass can see, so we warn rather than silently under-prune.
	const maxListLimit = 500
	limit := int64(maxListLimit)
	labelSel := []string{syncAppLabel + ":" + app}
	params := &apiclient.GetApiV1ResourcesParams{Limit: &limit, Label: &labelSel}
	resp, err := client.GetApiV1ResourcesWithResponse(ctx, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return apiError(resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	}
	var live []apiclient.ResourceListItem
	if resp.JSON200.Resources != nil {
		live = *resp.JSON200.Resources
	}
	if len(live) == maxListLimit {
		fmt.Fprintf(os.Stderr, "warning: app %q owns >= %d resources; prune saw only the first %d this pass\n",
			app, maxListLimit, maxListLimit)
	}
	for _, r := range live {
		ref := resourceRef{kind: r.Kind, name: r.Name}
		if desired[ref] {
			continue // still declared — keep it
		}
		if r.Labels[syncPruneOptOutLabel] == "false" {
			fmt.Printf("skip prune %s (app %s): %s=false\n", ref, app, syncPruneOptOutLabel)
			continue
		}
		if dryRun {
			fmt.Printf("would prune %s (app %s — no longer in manifests)\n", ref, app)
			continue
		}
		if err := deleteResource(ctx, client, ref.String()); err != nil {
			return fmt.Errorf("prune %s: %w", ref, err)
		}
	}
	return nil
}

// applyRank orders the apply by dependency: a kind CRD first (its kind must be
// registered before anything references it), then provider configs (a resource's
// provider_config_ref must resolve), then reactor bindings (subscribe a reactor to a
// kind's transition — wired before the resources start transitioning), then the
// resources themselves. Same ordering the demos' hand apply loops use.
func applyRank(t objectType) int {
	switch t {
	case typeManifest:
		return 0
	case typeProviderConfig:
		return 1
	case typeReactorBinding:
		return 2
	default: // typeResource
		return 3
	}
}

// warnUnscoped prints a warning for each resource/providerconfig that has no
// governing converge.yaml, so the operator sees it will apply but never prune.
func warnUnscoped(manifests []syncManifest) {
	for _, m := range manifests {
		// Only resources are prunable; a resource with no governing converge.yaml is
		// applied but can never be pruned, so surface that gap. CRDs and
		// providerconfigs are not app-scoped by design — no warning.
		if m.typ == typeResource && m.app == "" {
			fmt.Fprintf(os.Stderr, "warning: %s has no %s scope — it will be applied but cannot be pruned\n", m.path, syncConfigFile)
		}
	}
}

func appSuffix(app string) string {
	if app == "" {
		return ""
	}
	return "  [app=" + app + "]"
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
