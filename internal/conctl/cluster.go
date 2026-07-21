package conctl

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/salesforce/converge/internal/conctl/apiclient"
)

// newClusterCommand is the fleet view — "conctl cluster", the analogue of
// 'kubectl get nodes'. It's a top-level convenience over 'conctl list cluster'.
// With --workers it also lists the dumb workers connected to each broker
// (the analogue of 'kubectl get pods'), with the kinds each can execute.
func newClusterCommand(o *globalOpts) *cobra.Command {
	var showWorkers bool
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Show the running fleet (control/broker/reactor members and connected workers)",
		Long: `Cluster prints the running fleet registry — the control pods, brokers, and
reactors that have a heartbeat, with their role, owned shard tile, status, and
(for brokers) in-flight task count plus the number of workers connected.

Remote workers hold no DB row of their own; each broker publishes its live
set of connected workers in its heartbeat, so --workers expands them: one row
per connected worker with the broker it's on, its id (hostname), the kinds it can
execute (a worker may serve several), and its in-flight/max task slots.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := o.client()
			if err != nil {
				return err
			}
			ctx, cancel := o.ctx(cmd.Context())
			defer cancel()
			return listCluster(ctx, o, client, showWorkers)
		},
	}
	cmd.Flags().BoolVar(&showWorkers, "workers", false, "also list the workers connected to each broker (kinds + slots)")
	return cmd
}

func listCluster(ctx context.Context, o *globalOpts, client *apiclient.ClientWithResponses, showWorkers bool) error {
	resp, err := client.GetApiV1ClusterMembersWithResponse(ctx)
	if err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return apiError(resp.HTTPResponse, resp.ApplicationproblemJSONDefault)
	}
	var rows []apiclient.ClusterMemberInfo
	if resp.JSON200.Members != nil {
		rows = *resp.JSON200.Members
	}
	cols := []column[apiclient.ClusterMemberInfo]{
		{"HOSTNAME", func(m apiclient.ClusterMemberInfo) string { return m.Hostname }},
		{"ROLE", func(m apiclient.ClusterMemberInfo) string { return m.Role }},
		{"STATUS", func(m apiclient.ClusterMemberInfo) string { return m.Status }},
		{"SHARDS", func(m apiclient.ClusterMemberInfo) string { return fmtShards(m.Shards) }},
		{"IN-FLIGHT", func(m apiclient.ClusterMemberInfo) string { return fmt.Sprintf("%d", m.InFlight) }},
		{"WORKERS", func(m apiclient.ClusterMemberInfo) string { return fmtWorkerCount(m.Workers) }},
		{"VERSION", func(m apiclient.ClusterMemberInfo) string { return m.Version }},
		{"UPTIME", func(m apiclient.ClusterMemberInfo) string { return m.Uptime }},
	}
	// json/yaml already round-trip the full shape (workers nested per member), so
	// --workers only affects the table view.
	if err := render(o, resp.JSON200, rows, cols); err != nil {
		return err
	}
	if showWorkers && o.output == outputTable {
		printConnectedWorkers(rows)
	}
	return nil
}

// fmtWorkerCount renders a broker row's connected-worker count for the WORKERS
// column: "—" for a member that accepts none (control/react) or a broker with
// zero connected right now, else the count.
func fmtWorkerCount(workers *[]apiclient.ConnectedWorkerInfo) string {
	if workers == nil || len(*workers) == 0 {
		return "—"
	}
	return fmt.Sprintf("%d", len(*workers))
}

// printConnectedWorkers prints the flattened connected-worker table below the
// member table (--workers): one row per worker, tagged with the broker it's on.
// Sorted by (broker, worker) for a stable listing.
func printConnectedWorkers(members []apiclient.ClusterMemberInfo) {
	type row struct {
		broker string
		w      apiclient.ConnectedWorkerInfo
	}
	var rows []row
	for _, m := range members {
		if m.Workers == nil {
			continue
		}
		for _, w := range *m.Workers {
			rows = append(rows, row{broker: m.Hostname, w: w})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].broker != rows[j].broker {
			return rows[i].broker < rows[j].broker
		}
		return rows[i].w.WorkerId < rows[j].w.WorkerId
	})

	_, _ = fmt.Fprintln(os.Stdout) // blank line separating the two tables
	cols := []column[row]{
		{"BROKER", func(r row) string { return r.broker }},
		{"WORKER", func(r row) string { return r.w.WorkerId }},
		{"KINDS", func(r row) string { return fmtKinds(r.w.Kinds) }},
		{"SLOTS", func(r row) string { return fmt.Sprintf("%d/%d", r.w.Inflight, r.w.MaxInflight) }},
	}
	if len(rows) == 0 {
		_, _ = fmt.Fprintln(os.Stdout, "No workers connected.")
		return
	}
	_ = writeTable(os.Stdout, rows, cols)
}

// fmtKinds renders a worker's kind list as a comma-joined cell ("—" if none).
func fmtKinds(kinds *[]string) string {
	if kinds == nil || len(*kinds) == 0 {
		return "—"
	}
	return strings.Join(*kinds, ", ")
}

// fmtShards renders the owned inclusive shard span [lo, hi] as "lo-hi" (or "lo"
// for a single shard); a nil/short slice (no owned tile — e.g. a control pod or
// dev all-in-one) renders as "—".
func fmtShards(shards *[]int32) string {
	if shards == nil || len(*shards) < 2 {
		return "—"
	}
	lo, hi := (*shards)[0], (*shards)[1]
	if lo == hi {
		return fmt.Sprintf("%d", lo)
	}
	return fmt.Sprintf("%d-%d", lo, hi)
}
