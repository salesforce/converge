package converge

import (
	"context"

	"connectrpc.com/connect"

	"github.com/salesforce/converge/sdk-go/workerpb"
)

// transport is the broker RPC surface a WORKER uses: pull work + results over the bidi
// WorkStream, and fetch a (kind, kindVersion)'s default provider config. It mirrors the
// generated WorkerServiceClient exactly — the worker-facing service. The broker↔broker
// mesh (Route) is a SEPARATE service (MeshServiceClient) a worker never
// dials, so a worker structurally cannot reach it. The generated WorkerServiceClient
// satisfies transport, so production passes it unchanged; a test supplies a fake without
// dialing a broker.
type transport interface {
	// WorkStream opens the bidi stream the worker pulls tasks from and reports
	// results/heartbeats on.
	WorkStream(context.Context) *connect.BidiStreamForClient[workerpb.WorkStreamClientMsg, workerpb.WorkStreamServerMsg]
	// GetProviderConfig fetches one (kind, kindVersion)'s default provider config
	// document + bundle for the boot load and the periodic refresh.
	GetProviderConfig(context.Context, *connect.Request[workerpb.GetProviderConfigRequest]) (*connect.Response[workerpb.GetProviderConfigResponse], error)
}
