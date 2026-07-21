package conctl

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/salesforce/converge/internal/conctl/apiclient"
)

func newGetCommand(o *globalOpts) *cobra.Command {
	// kindVersion is REQUIRED for `get manifest` (the manifest GET has no implicit
	// v1 default — it names the exact (kind, kind_version) whose CRD to fetch).
	var kindVersion int
	cmd := &cobra.Command{
		Use:   "get TYPE NAME",
		Short: "Get a single object by kind/name and print it",
		Long: `Get fetches ONE object and prints its detail.

    conctl get resource <kind>/<name>              # a resource by kind+name (full spec/status)
    conctl get providerconfig <name>               # a provider config (incl. spec + data)
    conctl get manifest <kind> --kind-version <N>  # a kind CRD at an EXACT version
    conctl get reactorbinding <name>               # a binding (from the list; no by-name GET)

Use -o json / -o yaml for the full machine-readable object. To list many
objects, use 'conctl list TYPE'.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			typ, err := resolveType(args[0])
			if err != nil {
				return err
			}
			name := args[1]
			client, err := o.client()
			if err != nil {
				return err
			}
			ctx, cancel := o.ctx(cmd.Context())
			defer cancel()

			switch typ {
			case typeResource:
				return getResource(ctx, o, client, name)
			case typeProviderConfig:
				return getProviderConfig(ctx, o, client, name)
			case typeManifest:
				// kind_version is REQUIRED and explicit (>= 1): no implicit v1 default.
				if kindVersion < 1 {
					return fmt.Errorf("get manifest requires --kind-version >= 1 (no implicit v1 default): name the version to fetch, e.g. --kind-version 1")
				}
				return getManifest(ctx, o, client, name, kindVersion)
			case typeReactorBinding:
				return getReactorBinding(ctx, o, client, name)
			case typeCluster:
				return fmt.Errorf("cluster has no single-object get; use 'conctl cluster' or 'conctl list cluster'")
			default:
				return fmt.Errorf("cannot get object type %q", typ)
			}
		},
	}
	cmd.Flags().IntVar(&kindVersion, "kind-version", 0, "Web-API version (v1, v2, …; 1–32767) — REQUIRED for 'get manifest'.")
	return cmd
}

func getResource(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses, ref string) error {
	kind, name, err := splitKindName(ref)
	if err != nil {
		return err
	}
	resp, err := client.GetApiV1ResourcesByKindByNameWithResponse(ctx, kind, name)
	if err != nil {
		return err
	}
	r, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	if err != nil {
		return err
	}
	// Address + display by kind/name. The internal uuid is available only as
	// `uid` in the machine-readable -o json/yaml output, never in this table.
	fields := []kv{
		{"Kind", r.Kind},
		{"Name", r.Name},
		{"Phase", r.Phase},
		{"Ready", yesNo(r.IsReady)},
		{"Generation", fmt.Sprintf("%d", r.Generation)},
		{"Synced gen", fmt.Sprintf("%d", r.SyncedGen)},
		{"Owner", ownerRef(r.OwnerKind, r.OwnerName)},
		{"Root", ownerRef(r.RootKind, r.RootName)},
		{"Created", fmtTime(r.CreatedAt)},
		{"Updated", fmtTime(r.UpdatedAt)},
	}
	if r.Work != nil {
		fields = append(fields, workFields(r.Work)...)
	}
	return renderOne(o, r, fields)
}

// ownerRef renders an owner/root cross-reference as "kind/name", or "" when the
// resource has none (a root has no owner). Both come from the API as optional
// strings (public identity — never an id).
func ownerRef(kind, name *string) string {
	if kind == nil || name == nil || *kind == "" || *name == "" {
		return ""
	}
	return *kind + "/" + *name
}

func getProviderConfig(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses, name string) error {
	resp, err := client.GetApiV1ProviderconfigsByNameWithResponse(ctx, name)
	if err != nil {
		return err
	}
	c, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	if err != nil {
		return err
	}
	dataLen := 0
	if c.Data != nil {
		dataLen = len(*c.Data)
	}
	fields := []kv{
		{"Name", c.Name},
		{"Kind", c.Kind},
		{"Default", yesNo(c.IsDefault)},
		{"Owner", deref(c.OwnerName)},
		{"Data (base64 bytes)", fmt.Sprintf("%d", dataLen)},
		{"Created", c.CreatedAt.Format(timeLayout)},
		{"Updated", c.UpdatedAt.Format(timeLayout)},
	}
	return renderOne(o, c, fields)
}

func getManifest(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses, kind string, kindVersion int) error {
	resp, err := client.GetApiV1KindsByKindVersionsByKindVersionManifestWithResponse(ctx, kind, int64(kindVersion))
	if err != nil {
		return err
	}
	m, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	if err != nil {
		return err
	}
	// The by-kind manifest GET returns only manifest_version in its typed body;
	// for the full CRD document, list manifests (GET /kinds/manifests) carries
	// the schemas. Here we show what this endpoint provides.
	fields := []kv{
		{"Kind", kind},
		{"Kind version", fmt.Sprintf("%d", kindVersion)},
		{"Manifest version", fmt.Sprintf("%d", m.ManifestVersion)},
	}
	return renderOne(o, m, fields)
}

// getReactorBinding fetches the binding by name by listing (the API has no
// by-name GET for bindings) and selecting the match.
func getReactorBinding(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses, name string) error {
	resp, err := client.GetApiV1ReactorBindingsWithResponse(ctx)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return apiError(resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	}
	if resp.JSON200.Bindings != nil {
		for i := range *resp.JSON200.Bindings {
			b := (*resp.JSON200.Bindings)[i]
			if b.Name == name {
				fields := []kv{
					{"Name", b.Name},
					{"Watch kind", b.WatchKind},
					{"Transition", b.Transition},
					{"Reactor", b.Reactor},
					{"Enabled", yesNo(b.Enabled)},
				}
				return renderOne(o, b, fields)
			}
		}
	}
	return fmt.Errorf("reactorbinding %q not found", name)
}

// workFields renders the live work-queue claim (executing worker / heartbeat /
// attempts) into the single-resource detail view — the same claim data the UI panel
// shows, including the executing-worker-vs-claiming-broker distinction.
func workFields(w *apiclient.WorkDTO) []kv {
	claimed := "queued (not yet claimed)"
	if w.Claimed {
		claimed = "claimed"
	}
	fields := []kv{
		{"Work", claimed},
		{"Task type", w.TaskType},
	}
	// "Worker" is the process that RUNS the stage: worker_id (the dumb worker) when
	// present, else broker_id (the claiming/lease pod — the broker in a fanned-out
	// deploy, or the in-process pod for a local run). When both are present and
	// differ, also show the claiming broker. Mirrors the UI work card. Gated on
	// Claimed so an unclaimed ("queued") row never prints a worker line.
	worker := ""
	if w.WorkerId != nil {
		worker = *w.WorkerId
	}
	broker := ""
	if w.BrokerId != nil {
		broker = *w.BrokerId
	}
	if w.Claimed && (worker != "" || broker != "") {
		shown := worker
		if shown == "" {
			shown = broker
		}
		fields = append(fields, kv{"Worker", shown})
		if worker != "" && broker != "" && worker != broker {
			fields = append(fields, kv{"Claimed by broker", broker})
		}
	}
	fields = append(fields, kv{"Attempts", fmt.Sprintf("%d", w.Attempts)})
	if w.HeartbeatAge != "" {
		fields = append(fields, kv{"Heartbeat age", w.HeartbeatAge})
	}
	return fields
}
