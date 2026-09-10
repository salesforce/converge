package conctl

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/salesforce/converge/internal/conctl/apiclient"
)

func newDeleteCommand(o *globalOpts) *cobra.Command {
	// kindVersion is REQUIRED for `delete manifest` — it names the exact
	// (kind, kind_version) to remove (there is no implicit v1 default).
	var kindVersion int
	cmd := &cobra.Command{
		Use:   "delete TYPE NAME",
		Short: "Delete an object by kind/name",
		Long: `Delete removes an object.

    conctl delete resource <kind>/<name>              # soft-delete a resource (teardown)
    conctl delete providerconfig <name>               # delete a provider config
    conctl delete reactorbinding <name>               # delete a lifecycle binding
    conctl delete manifest <kind> --kind-version <N>  # delete a CRD at an EXACT version

Deleting a manifest is refused if any resource is pinned to that
(kind, kind_version) or a reactor binding pins it exactly; the version's provider
configs are cascade-deleted. Cluster members are not deletable via this command.`,
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
				return deleteResource(ctx, client, name)
			case typeProviderConfig:
				return deleteProviderConfig(ctx, client, name)
			case typeReactorBinding:
				return deleteReactorBinding(ctx, client, name)
			case typeManifest:
				// kind_version is REQUIRED and explicit (>= 1).
				if kindVersion < 1 {
					return fmt.Errorf("delete manifest requires --kind-version >= 1 (no implicit v1 default): name the version to delete, e.g. --kind-version 1")
				}
				return deleteKindManifest(ctx, client, name, kindVersion)
			case typeCluster:
				return fmt.Errorf("cluster members are not deletable; they deregister on shutdown")
			default:
				return fmt.Errorf("cannot delete object type %q", typ)
			}
		},
	}
	cmd.Flags().IntVar(&kindVersion, "kind-version", 0, "Web-API version (v1, v2, …; 1–32767) — REQUIRED for 'delete manifest'.")
	return cmd
}

func deleteResource(ctx context.Context, client *apiclient.ClientWithResponses, ref string) error {
	kind, name, err := splitKindName(ref)
	if err != nil {
		return err
	}
	resp, err := client.DeleteApiV1ResourcesByKindByNameWithResponse(ctx, kind, name)
	if err != nil {
		return err
	}
	if _, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault); err != nil {
		return err
	}
	fmt.Printf("resource %s/%s deletion requested\n", kind, name)
	return nil
}

func deleteProviderConfig(ctx context.Context, client *apiclient.ClientWithResponses, name string) error {
	resp, err := client.DeleteApiV1ProviderconfigsByNameWithResponse(ctx, name)
	if err != nil {
		return err
	}
	if _, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault); err != nil {
		return err
	}
	fmt.Printf("providerconfig %s deleted\n", name)
	return nil
}

func deleteKindManifest(ctx context.Context, client *apiclient.ClientWithResponses, kind string, kindVersion int) error {
	resp, err := client.DeleteApiV1KindsByKindVersionsByKindVersionManifestWithResponse(ctx, kind, int64(kindVersion))
	if err != nil {
		return err
	}
	body, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	if err != nil {
		return err
	}
	if body != nil && body.DeletedConfigs > 0 {
		fmt.Printf("kind %s/v%d manifest deleted (%d provider config(s) cascaded)\n", kind, kindVersion, body.DeletedConfigs)
		return nil
	}
	fmt.Printf("kind %s/v%d manifest deleted\n", kind, kindVersion)
	return nil
}

func deleteReactorBinding(ctx context.Context, client *apiclient.ClientWithResponses, name string) error {
	resp, err := client.DeleteApiV1ReactorBindingsByNameWithResponse(ctx, name)
	if err != nil {
		return err
	}
	if _, err := okBody(resp.JSON200, resp.Body, resp.HTTPResponse, resp.ApplicationproblemJSONDefault); err != nil {
		return err
	}
	fmt.Printf("reactorbinding %s deleted\n", name)
	return nil
}
