package engine

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/runtime"
)

// A Duty is one unit of work a process performs. A process is simply the SET of
// duties it was asked to run (see DutiesFromConfig). Every sweeper and the
// broker are peer duties behind this one interface, so "what does this pod do?"
// is answered by reading its duty list.
//
// The concrete duties (controlDuty / claimDuty) are thin wrappers around the
// ControlPlane sweeper bundle and the Connect broker (which itself also drains
// lifecycle_outbox and fans reactors out to its workers); the Engine runs the
// list uniformly. converge (control/broker) carries NO provider code and runs no
// in-process dispatch — every kind's handlers live in a dumb Connect worker, and the
// broker learns which kinds exist LIVE from kind_manifest, not from the duty objects.
type Duty interface {
	// Name is a short identifier for logs and the cluster view ("control",
	// "claim").
	Name() string

	// Start launches the duty's goroutines against the shared Deps and returns
	// once they are running (it does not block until they finish). The duty owns
	// its own lifecycle off the passed ctx; cancelling ctx stops it.
	Start(ctx context.Context, deps Deps) error

	// Stop joins the duty's goroutines (idempotent; a no-op before Start). The
	// Engine calls it in reverse start order on shutdown.
	Stop(ctx context.Context) error
}

// HandlerProvider is an OPTIONAL capability a Duty may also implement: it
// publishes an HTTP route to mount on a pod's listener (the claim duty exposes
// the broker Connect service route this way). Engine.ClaimHandler ranges the duty
// list for the first implementer, so a new route-publishing duty needs no Engine
// edit — it just implements this. ok=false means the duty has no route to mount
// right now (e.g. its server isn't built until Start). Called AFTER Start.
type HandlerProvider interface {
	Handler(opts ...connect.HandlerOption) (path string, h http.Handler, ok bool)
}

// Deps is the shared bag of dependencies wired ONCE at boot and handed to every
// duty's Start. A duty reads only the fields it needs. The API server's
// read-replica pool is NOT here — serving the API stays in cmd/converge, not a
// duty.
type Deps struct {
	Pool   *pgxpool.Pool
	Shards *runtime.ShardSet

	// Configs is the live default-config source the CLAIM duty serves to dumb
	// workers via GetProviderConfig (the ProviderConfigCache satisfies it). nil-safe: a
	// broker with no source serves no defaults (workers get only the per-resource
	// override). A non-broker pod leaves it nil.
	Configs broker.ConfigReader
}
