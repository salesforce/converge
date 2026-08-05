package conctl

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/salesforce/converge/internal/conctl/apiclient"
)

func newListCommand(o *globalOpts) *cobra.Command {
	var (
		kind   string
		name   string
		limit  int64
		all    bool
		labels []string
	)
	cmd := &cobra.Command{
		Use:   "list TYPE",
		Short: "List objects of a type",
		Long: `List prints a table of objects of one type.

    conctl list resources                 # ROOTS (top-level objects) + child count
    conctl list resources --kind classicbom
    conctl list resources --all           # EVERY resource incl. children (leaves)
    conctl list providerconfigs           # provider configs (slim rows)
    conctl list manifests                 # applied kind CRDs
    conctl list reactorbindings           # lifecycle bindings
    conctl list cluster                   # running fleet (alias of 'conctl cluster')

By default 'list resources' shows only ROOTS — the top-level objects (owner_id
IS NULL), like 'kubectl get' shows top-level objects — each with the size of the
tree beneath it (CHILDREN). This is the level you usually search: a composition
root (e.g. classicbom) is one row, not buried under a million leaf resources.
Use --all to list every resource including children (ordered by recency); at
scale the child leaves dominate, so prefer --kind/--name to narrow, or drill in
with 'conctl get resource <kind>/<name>'.

--limit caps the page fetched; lists paginate server-side. -o json/yaml emits
the full objects.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			typ, err := resolveType(args[0])
			if err != nil {
				return err
			}
			client, err := o.client()
			if err != nil {
				return err
			}
			ctx, cancel := o.ctx(cmd.Context())
			defer cancel()

			switch typ {
			case typeResource:
				if all {
					return listAllResources(ctx, o, client, kind, name, labels, limit)
				}
				return listRootResources(ctx, o, client, kind, name, limit)
			case typeProviderConfig:
				return listProviderConfigs(ctx, o, client, kind, name, limit)
			case typeManifest:
				return listManifests(ctx, o, client)
			case typeReactorBinding:
				return listReactorBindings(ctx, o, client)
			case typeCluster:
				return listCluster(ctx, o, client, false)
			default:
				return fmt.Errorf("cannot list object type %q", typ)
			}
		},
	}
	cmd.Flags().StringVar(&kind, "kind", "", "filter by runtime kind (resources, providerconfigs)")
	cmd.Flags().StringVar(&name, "name", "", "filter by name substring (resources, providerconfigs)")
	cmd.Flags().Int64Var(&limit, "limit", 100, "maximum rows to fetch")
	cmd.Flags().BoolVar(&all, "all", false, "list-resources: include children (leaves), not just roots")
	cmd.Flags().StringArrayVarP(&labels, "label", "l", nil,
		"list-resources --all: filter by label, key:value (repeatable, AND-combined)")
	return cmd
}

// listRootResources lists ROOTS (top-level objects) — the default for
// 'list resources'. Each row carries the child count of the tree beneath it, so
// a composition root shows as one searchable row rather than being lost among a
// million leaves in the recency-ordered all-resources page.
func listRootResources(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses, kind, name string, limit int64) error {
	params := &apiclient.GetApiV1ResourcesRootsParams{Limit: &limit}
	if kind != "" {
		params.Kind = &[]string{kind}
	}
	if name != "" {
		params.Name = &name
	}
	resp, err := client.GetApiV1ResourcesRootsWithResponse(ctx, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return apiError(resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	}
	var rows []apiclient.RootPageItem
	if resp.JSON200.Roots != nil {
		rows = *resp.JSON200.Roots
	}
	// A resource is addressed by KIND/NAME; the internal uuid is never shown.
	cols := []column[apiclient.RootPageItem]{
		{"KIND", func(r apiclient.RootPageItem) string { return r.Kind }},
		{"NAME", func(r apiclient.RootPageItem) string { return r.Name }},
		{"PHASE", func(r apiclient.RootPageItem) string { return r.Phase }},
		{"READY", func(r apiclient.RootPageItem) string { return yesNo(r.IsReady) }},
		{"CHILDREN", func(r apiclient.RootPageItem) string { return fmt.Sprintf("%d", r.ResourceCount) }},
		{"GEN", func(r apiclient.RootPageItem) string { return fmt.Sprintf("%d/%d", r.SyncedGen, r.Generation) }},
	}
	return render(o, resp.JSON200, rows, cols)
}

// listAllResources lists EVERY resource including children (the --all path). At
// scale the leaf resources dominate the recency-ordered page, so this is for
// narrow filters or a targeted sweep — not a name search for a root.
func listAllResources(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses, kind, name string, labels []string, limit int64) error {
	params := &apiclient.GetApiV1ResourcesParams{Limit: &limit}
	if kind != "" {
		params.Kind = &[]string{kind}
	}
	if name != "" {
		params.Name = &name
	}
	if len(labels) > 0 {
		params.Label = &labels
	}
	resp, err := client.GetApiV1ResourcesWithResponse(ctx, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return apiError(resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	}
	var rows []apiclient.ResourceListItem
	if resp.JSON200.Resources != nil {
		rows = *resp.JSON200.Resources
	}
	// KIND/NAME is the handle; OWNER (kind/name) locates a child under its root.
	// No id column — the internal uuid never surfaces.
	cols := []column[apiclient.ResourceListItem]{
		{"KIND", func(r apiclient.ResourceListItem) string { return r.Kind }},
		{"NAME", func(r apiclient.ResourceListItem) string { return r.Name }},
		{"PHASE", func(r apiclient.ResourceListItem) string { return r.Phase }},
		{"READY", func(r apiclient.ResourceListItem) string { return yesNo(r.IsReady) }},
		{"GEN", func(r apiclient.ResourceListItem) string { return fmt.Sprintf("%d/%d", r.SyncedGen, r.Generation) }},
		{"OWNER", func(r apiclient.ResourceListItem) string { return ownerRef(r.OwnerKind, r.OwnerName) }},
	}
	return render(o, resp.JSON200, rows, cols)
}

func listProviderConfigs(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses, kind, name string, limit int64) error {
	params := &apiclient.GetApiV1ProviderconfigsParams{Limit: &limit}
	if kind != "" {
		params.Kind = &kind
	}
	if name != "" {
		params.Name = &name
	}
	resp, err := client.GetApiV1ProviderconfigsWithResponse(ctx, params)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return apiError(resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	}
	var rows []apiclient.ProviderConfigBody
	if resp.JSON200.ProviderConfigs != nil {
		rows = *resp.JSON200.ProviderConfigs
	}
	cols := []column[apiclient.ProviderConfigBody]{
		{"NAME", func(c apiclient.ProviderConfigBody) string { return c.Name }},
		{"KIND", func(c apiclient.ProviderConfigBody) string { return c.Kind }},
		{"DEFAULT", func(c apiclient.ProviderConfigBody) string { return yesNo(c.IsDefault) }},
		{"OWNER", func(c apiclient.ProviderConfigBody) string { return deref(c.OwnerName) }},
		{"UPDATED", func(c apiclient.ProviderConfigBody) string { return c.UpdatedAt.Format(timeLayout) }},
	}
	if err := render(o, resp.JSON200, rows, cols); err != nil {
		return err
	}
	if o.output == outputTable && resp.JSON200.Total > int64(len(rows)) {
		fmt.Printf("\nShowing %d of %d — raise --limit or page with --limit/offset.\n", len(rows), resp.JSON200.Total)
	}
	return nil
}

func listManifests(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses) error {
	resp, err := client.GetApiV1KindsManifestsWithResponse(ctx)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return apiError(resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	}
	var rows []apiclient.ManifestBody
	if resp.JSON200.Manifests != nil {
		rows = *resp.JSON200.Manifests
	}
	cols := []column[apiclient.ManifestBody]{
		{"KIND", func(m apiclient.ManifestBody) string { return m.Kind }},
		{"REACTIONS", func(m apiclient.ManifestBody) string {
			if m.Reactions == nil {
				return "0"
			}
			return fmt.Sprintf("%d", len(*m.Reactions))
		}},
		{"FINALIZER", func(m apiclient.ManifestBody) string { return deref(m.FinalizerName) }},
		{"MAX-INFLIGHT", func(m apiclient.ManifestBody) string { return i64ptr(m.MaxInflight) }},
		{"DESCRIPTION", func(m apiclient.ManifestBody) string { return deref(m.Description) }},
	}
	return render(o, resp.JSON200, rows, cols)
}

func listReactorBindings(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses) error {
	resp, err := client.GetApiV1ReactorBindingsWithResponse(ctx)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return apiError(resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	}
	var rows []apiclient.ReactorBindingBody
	if resp.JSON200.Bindings != nil {
		rows = *resp.JSON200.Bindings
	}
	cols := []column[apiclient.ReactorBindingBody]{
		{"NAME", func(b apiclient.ReactorBindingBody) string { return b.Name }},
		{"WATCH KIND", func(b apiclient.ReactorBindingBody) string { return b.WatchKind }},
		{"TRANSITION", func(b apiclient.ReactorBindingBody) string { return b.Transition }},
		{"REACTOR", func(b apiclient.ReactorBindingBody) string { return b.Reactor }},
		{"ENABLED", func(b apiclient.ReactorBindingBody) string { return yesNo(b.Enabled) }},
	}
	return render(o, resp.JSON200, rows, cols)
}

// i64ptr renders an *int64 as its decimal string, or "" when nil.
func i64ptr(p *int64) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%d", *p)
}
