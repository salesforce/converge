package test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/salesforce/converge/internal/broker"
	"github.com/salesforce/converge/internal/engine"
	"github.com/salesforce/converge/internal/runtime"
	"github.com/salesforce/converge/internal/store"
	"github.com/salesforce/converge/sdk-go/converge"
	"github.com/salesforce/converge/sdk-go/workerpb/workerpbconnect"
	"github.com/salesforce/converge/test/internal/noop"
)

// testBroker is one broker in a multi-broker relay test: its id (also its
// work_queue.worker_id / cluster_members id), the httptest server hosting its
// broker Connect services, the broker.Server itself, and the [lo,hi] shard tile it owns.
// Shared by every TestBrokerRelay* case (they stand up identical broker fleets).
type testBroker struct {
	id   string
	srv  *httptest.Server
	cl   *broker.Server
	lohi [2]int16
}

// TestBrokerRelayMatching proves the broker-to-broker mesh against a real Postgres:
// THREE brokers split the 256-shard keyspace into three disjoint tiles, but only ONE
// worker connects — to broker-0. Work whose resource_id hashes into broker-1's or
// broker-2's tile is claimed by THAT broker (its worker_id, its fenced AppendOutbox),
// and — since that broker has no local worker — PUSHED over the mesh to broker-0,
// whose single worker runs it; broker-0 forwards the Complete home (RelayComplete),
// so the CLAIMING broker resolves its own parked lease and writes under its worker_id.
//
// This is the placement≠consumption decoupling: brokers 1 and 2 keep claiming their
// tiles because broker-0 advertises a worker for the kind (the presence-based gate),
// and push the work to it. We assert EVERY root synced — the only way that happens
// with one worker on one broker is if the mesh drained the other two tiles.
//
// It also proves the fence is intact end-to-end: each synced resource's outbox was
// applied under whichever broker CLAIMED it, never broker-0 (the executor proxy).
func TestBrokerRelayMatching(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	// noop only: a Worker-only kind (no Composer). Tiny delay — we want throughput,
	// not a wide in-flight window.
	noopProvider := noop.New(10 * time.Millisecond)
	reg := newTReg()
	reg.Add(noopProvider)

	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))

	// CONTROL duty only (drainer applies outbox → synced_gen, reaper backstops). No
	// in-process dispatch — every noop stage flows worker→broker→DB via the tier.
	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{RunControl: true})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	// Shared manifest cache (pairs + policy) for all three brokers.
	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	st := store.New(pool)

	// Three brokers, each owning a DISJOINT contiguous third of the 256-shard space.
	// Together they cover the whole keyspace, so every root's shard is claimed by
	// exactly one of them.
	tiles := [][]int16{
		shardRangeSlice(0, 85),    // broker-0 (the ONLY one with a worker)
		shardRangeSlice(86, 170),  // broker-1 (no worker → relay-only)
		shardRangeSlice(171, 255), // broker-2 (no worker → relay-only)
	}
	brokers := make([]*testBroker, len(tiles))

	// stand up each broker's Connect services on an httptest server FIRST so we know
	// its dial-able URL, then register it in cluster_members advertising that URL
	// (connect_addr) so the relay mesh discovers its peers via ListClusterMembers.
	for i, shards := range tiles {
		id := fmt.Sprintf("broker-%d", i)
		ss := runtime.NewShardSet(shards)
		cl := broker.NewDispatch(pool, mc, id, 16, ss)
		mux := http.NewServeMux()
		mux.Handle(cl.Handler())
		srv := h2Server(t, mux)
		lo, hi := ss.Bounds()
		brokers[i] = &testBroker{id: id, srv: srv, cl: cl, lohi: [2]int16{lo, hi}}
	}

	// Enable relay on every broker, pointing its mesh at the live cluster_members
	// registry (real peer discovery), then register each broker's row advertising
	// its Connect address.
	for _, b := range brokers {
		// EnableRelay: refresh fast (300ms) so peer discovery is prompt in-test; the
		// HTTP transport is the httptest server's own client (all three share a
		// loopback client — fine for a test). This also swaps in the CLUSTER-AWARE
		// claim gate (local worker OR any peer advertising a worker for the kind).
		b.cl.EnableRelay(b.id, h2TestClient(), st, runtime.NewPgxListener(pool), 300*time.Millisecond, time.Hour)
	}

	brokerCtx, brokerCancel := context.WithCancel(ctx)
	t.Cleanup(brokerCancel)

	// Per-broker heartbeat: re-publish each broker's dial address into cluster_members
	// every 300ms so peers can DISCOVER + dial the Route mesh. Worker CREDIT is pushed
	// LIVE over the route (Interest frames) once broker-0's worker connects — that is
	// what opens brokers 1 and 2's cluster-aware claim gate (anyPeerHasCredit) so they
	// claim their tiles, whose work is then PUSHED to broker-0's worker.
	for _, b := range brokers {
		beat := func() {
			require.NoError(t, st.UpsertClusterMember(ctx, store.ClusterMemberInfo{
				MemberID:  b.id,
				Role:      "broker",
				Shards:    &b.lohi,
				Config:    broker.AdvertiseAddr(nil, b.srv.URL),
				StartedAt: time.Unix(0, 0),
			}, 0, nil))
		}
		beat() // register immediately so the mesh's first refresh sees the peer
		go func() {
			tick := time.NewTicker(300 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-brokerCtx.Done():
					return
				case <-tick.C:
					beat()
				}
			}
		}()
	}

	// Run every broker's dispatcher + relay refresh loop.
	for _, b := range brokers {
		go func() { _ = b.cl.Dispatcher().Run(brokerCtx) }()
		go b.cl.StartRelay(brokerCtx)
	}

	// ONE dumb worker, connected ONLY to broker-0. It is the sole executor for the
	// entire fleet — brokers 1 and 2 have no local worker and must relay to it.
	client := workerpbconnect.NewWorkerServiceClient(h2TestClient(), brokers[0].srv.URL)
	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)
	go func() {
		_ = converge.RunWorker(workerCtx, client, []converge.Provider{noopProvider}, converge.RunOptions{MaxInflight: 16})
	}()

	// Create enough roots that, by hash, some land in EACH tile. 60 roots over 256
	// shards makes an empty tile astronomically unlikely; we verify coverage below.
	const nRoots = 60
	te := &testEngine{Pool: pool}
	type rootRow struct {
		id    uuid.UUID
		shard int16
	}
	roots := make([]rootRow, 0, nRoots)
	tileHits := make([]int, len(tiles))
	for i := range nRoots {
		id, err := te.CreateRoot(ctx, "noop", fmt.Sprintf("relay-root-%d", i), json.RawMessage(`{}`), nil)
		require.NoError(t, err)
		var shard int16
		require.NoError(t, pool.QueryRow(ctx, `SELECT shard_id FROM resources WHERE id = $1`, id).Scan(&shard))
		roots = append(roots, rootRow{id: id, shard: shard})
		tileHits[tileOf(shard, tiles)]++
	}
	// The test is only meaningful if work landed in the NON-worker tiles (1 and 2) —
	// those are the ones that can ONLY drain via relay. If by fluke none did, fail
	// loud rather than pass a test that exercised nothing.
	require.Greaterf(t, tileHits[1]+tileHits[2], 0,
		"no roots landed in a relay-only tile (broker-1/2); test exercised no relay — hits=%v", tileHits)
	t.Logf("root distribution across tiles [b0 b1 b2] = %v", tileHits)

	// Every root must sync. The only way brokers 1 and 2's roots sync — with zero
	// workers connected to them — is the relay mesh handing their claimed work to
	// broker-0's worker. Poll until all synced or the deadline.
	deadline := time.Now().Add(90 * time.Second)
	for {
		var pending int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM resources WHERE kind = 'noop' AND synced_gen < generation`,
		).Scan(&pending))
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			// Dump the stragglers' tiles + work_queue claim state so a failure shows
			// WHETHER the row was even claimed (worker_id) and by whom.
			shown := 0
			for _, r := range roots {
				var gen, sg int64
				_ = pool.QueryRow(ctx, `SELECT generation, synced_gen FROM resources WHERE id = $1`, r.id).Scan(&gen, &sg)
				if sg < gen && shown < 8 {
					var wid *string
					var att *int32
					_ = pool.QueryRow(ctx, `SELECT worker_id, attempts FROM work_queue WHERE resource_id = $1`, r.id).Scan(&wid, &att)
					widStr, attStr := "(no work_queue row)", ""
					if wid != nil {
						widStr = *wid
					}
					if att != nil {
						attStr = fmt.Sprintf(" attempts=%d", *att)
					}
					t.Logf("STRANDED root %s shard=%d tile=broker-%d claimed_by=%s%s", r.id, r.shard, tileOf(r.shard, tiles), widStr, attStr)
					shown++
				}
			}
			t.Fatalf("%d noop roots never synced (relay did not drain the worker-less tiles)", pending)
		}
		time.Sleep(250 * time.Millisecond)
	}

	// FENCE INTACT: every synced root's work was CLAIMED (and its fenced outbox
	// written) by the broker that owns its tile — NEVER broker-0 for a tile-1/2 root
	// (broker-0 only executed as a proxy; it never held those leases). The work_queue
	// rows are gone after drain, so we assert via the reaped lease invariant: no row
	// remains, and synced_gen advanced — which the drainer only does on a fenced
	// AppendOutbox from the CLAIMING broker. (A relay-broken fence would have left
	// the tile-1/2 rows claimed under broker-0 and the drainer would reject the
	// cross-worker apply, leaving them unsynced — already excluded by the sync gate
	// above.)
	var leftover int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM work_queue wq JOIN resources r ON r.id = wq.resource_id WHERE r.kind = 'noop'`,
	).Scan(&leftover))
	require.Zerof(t, leftover, "work_queue rows remain after full sync — a lease stranded (fence/relay bug)")

	t.Logf("all %d noop roots synced via relay: one worker on broker-0 drained all 3 tiles", nRoots)
}

// TestBrokerRelayManyWorkersOneBroker is the multi-worker sibling of
// TestBrokerRelayMatching: THREE brokers split the keyspace, but now MULTIPLE
// workers connect — all to broker-0. The worker-less brokers push their tiles'
// work to broker-0 (which advertises credit for all of its workers), so a pile of
// workers on one broker still drains every peer's tile.
func TestBrokerRelayManyWorkersOneBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	noopProvider := noop.New(10 * time.Millisecond)
	reg := newTReg()
	reg.Add(noopProvider)

	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))

	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{RunControl: true})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	st := store.New(pool)

	tiles := [][]int16{
		shardRangeSlice(0, 85),    // broker-0 (ALL workers connect here)
		shardRangeSlice(86, 170),  // broker-1 (no worker → relay-only)
		shardRangeSlice(171, 255), // broker-2 (no worker → relay-only)
	}
	brokers := make([]*testBroker, len(tiles))
	for i, shards := range tiles {
		id := fmt.Sprintf("broker-%d", i)
		ss := runtime.NewShardSet(shards)
		cl := broker.NewDispatch(pool, mc, id, 64, ss)
		mux := http.NewServeMux()
		mux.Handle(cl.Handler())
		srv := h2Server(t, mux)
		lo, hi := ss.Bounds()
		brokers[i] = &testBroker{id: id, srv: srv, cl: cl, lohi: [2]int16{lo, hi}}
	}
	for _, b := range brokers {
		b.cl.EnableRelay(b.id, h2TestClient(), st, runtime.NewPgxListener(pool), 300*time.Millisecond, time.Hour)
	}

	brokerCtx, brokerCancel := context.WithCancel(ctx)
	t.Cleanup(brokerCancel)

	for _, b := range brokers {
		beat := func() {
			require.NoError(t, st.UpsertClusterMember(ctx, store.ClusterMemberInfo{
				MemberID:  b.id,
				Role:      "broker",
				Shards:    &b.lohi,
				Config:    broker.AdvertiseAddr(nil, b.srv.URL),
				StartedAt: time.Unix(0, 0),
			}, 0, nil))
		}
		beat()
		go func() {
			tick := time.NewTicker(300 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-brokerCtx.Done():
					return
				case <-tick.C:
					beat()
				}
			}
		}()
	}

	for _, b := range brokers {
		go func() { _ = b.cl.Dispatcher().Run(brokerCtx) }()
		go b.cl.StartRelay(brokerCtx)
	}

	// THREE dumb workers, ALL connected to broker-0. broker-0 advertises ["noop"]
	// once ANY of them connects; brokers 1 and 2 (no local worker) open their
	// cluster-aware claim gate and claim their tiles, which broker-0 must drain by
	// relaying to whichever of its three idle workers is free.
	const nWorkers = 3
	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)
	for range nWorkers {
		client := workerpbconnect.NewWorkerServiceClient(h2TestClient(), brokers[0].srv.URL)
		go func() {
			_ = converge.RunWorker(workerCtx, client, []converge.Provider{noopProvider}, converge.RunOptions{MaxInflight: 16})
		}()
	}

	const nRoots = 90
	te := &testEngine{Pool: pool}
	tileHits := make([]int, len(tiles))
	for i := range nRoots {
		id, err := te.CreateRoot(ctx, "noop", fmt.Sprintf("many-root-%d", i), json.RawMessage(`{}`), nil)
		require.NoError(t, err)
		var shard int16
		require.NoError(t, pool.QueryRow(ctx, `SELECT shard_id FROM resources WHERE id = $1`, id).Scan(&shard))
		tileHits[tileOf(shard, tiles)]++
	}
	require.Greaterf(t, tileHits[1]+tileHits[2], 0,
		"no roots landed in a relay-only tile (broker-1/2); test exercised no relay — hits=%v", tileHits)
	t.Logf("root distribution across tiles [b0 b1 b2] = %v", tileHits)

	deadline := time.Now().Add(90 * time.Second)
	for {
		var pending int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM resources WHERE kind = 'noop' AND synced_gen < generation`,
		).Scan(&pending))
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d noop roots never synced (relay did not drain the worker-less tiles with %d workers on broker-0)", pending, nWorkers)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Logf("all %d noop roots synced: %d workers on broker-0 drained all 3 tiles via relay", nRoots, nWorkers)
}

// TestBrokerRelayFewerWorkersThanBrokers is the workers<brokers extreme: FIVE
// brokers split the keyspace but only TWO workers exist, both on broker-0. So
// broker-1..4 are ENTIRELY worker-less — 4/5 of the keyspace can ONLY drain by
// pushing over the mesh to broker-0's two workers. This is the case the mesh exists
// for; it must converge with no stranding (the presence-based claim gate keeps the
// worker-less brokers claiming, and buffer-push forwards to broker-0 as its two
// workers free credit).
func TestBrokerRelayFewerWorkersThanBrokers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pool := setupPostgres(t, ctx)

	noopProvider := noop.New(5 * time.Millisecond)
	reg := newTReg()
	reg.Add(noopProvider)
	migrateForSeed(t, ctx, pool)
	require.NoError(t, reg.seed(ctx, pool))

	controlDuties, err := engine.DutiesFromConfig(engine.EngineConfig{RunControl: true})
	require.NoError(t, err)
	control := engine.NewEngine(controlDuties, engine.Deps{Pool: pool})
	require.NoError(t, control.Start(ctx))
	t.Cleanup(func() { _ = control.Stop(context.Background()) })

	mc := runtime.NewKindManifestCache(pool)
	require.NoError(t, mc.Load(ctx))
	mc.Start(ctx)
	t.Cleanup(mc.Stop)

	st := store.New(pool)

	// FIVE brokers, disjoint fifths of the 256-shard space.
	tiles := [][]int16{
		shardRangeSlice(0, 51),
		shardRangeSlice(52, 103),
		shardRangeSlice(104, 155),
		shardRangeSlice(156, 207),
		shardRangeSlice(208, 255),
	}
	brokers := make([]*testBroker, len(tiles))
	for i, shards := range tiles {
		id := fmt.Sprintf("broker-%d", i)
		ss := runtime.NewShardSet(shards)
		cl := broker.NewDispatch(pool, mc, id, 64, ss)
		mux := http.NewServeMux()
		mux.Handle(cl.Handler())
		srv := h2Server(t, mux)
		lo, hi := ss.Bounds()
		brokers[i] = &testBroker{id: id, srv: srv, cl: cl, lohi: [2]int16{lo, hi}}
	}
	for _, b := range brokers {
		b.cl.EnableRelay(b.id, h2TestClient(), st, runtime.NewPgxListener(pool), 300*time.Millisecond, time.Hour)
	}

	brokerCtx, brokerCancel := context.WithCancel(ctx)
	t.Cleanup(brokerCancel)
	for _, b := range brokers {
		beat := func() {
			require.NoError(t, st.UpsertClusterMember(ctx, store.ClusterMemberInfo{
				MemberID: b.id, Role: "broker", Shards: &b.lohi,
				Config: broker.AdvertiseAddr(nil, b.srv.URL), StartedAt: time.Unix(0, 0),
			}, 0, nil))
		}
		beat()
		go func() {
			tick := time.NewTicker(300 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-brokerCtx.Done():
					return
				case <-tick.C:
					beat()
				}
			}
		}()
	}
	for _, b := range brokers {
		go func() { _ = b.cl.Dispatcher().Run(brokerCtx) }()
		go b.cl.StartRelay(brokerCtx)
	}

	// TWO workers total — BOTH on broker-0. Fewer workers than brokers.
	const nWorkers = 2
	workerCtx, workerCancel := context.WithCancel(ctx)
	t.Cleanup(workerCancel)
	for range nWorkers {
		client := workerpbconnect.NewWorkerServiceClient(h2TestClient(), brokers[0].srv.URL)
		go func() {
			_ = converge.RunWorker(workerCtx, client, []converge.Provider{noopProvider}, converge.RunOptions{MaxInflight: 16})
		}()
	}

	const nRoots = 100
	te := &testEngine{Pool: pool}
	tileHits := make([]int, len(tiles))
	for i := range nRoots {
		id, err := te.CreateRoot(ctx, "noop", fmt.Sprintf("fw-root-%d", i), json.RawMessage(`{}`), nil)
		require.NoError(t, err)
		var shard int16
		require.NoError(t, pool.QueryRow(ctx, `SELECT shard_id FROM resources WHERE id = $1`, id).Scan(&shard))
		tileHits[tileOf(shard, tiles)]++
	}
	// Most roots must land on worker-less brokers (1..4) — those can ONLY drain via push.
	workerless := tileHits[1] + tileHits[2] + tileHits[3] + tileHits[4]
	require.Greaterf(t, workerless, 0, "no roots on a worker-less broker; test exercised no forwarding — hits=%v", tileHits)
	t.Logf("root distribution across 5 tiles = %v (%d on worker-less brokers)", tileHits, workerless)

	deadline := time.Now().Add(90 * time.Second)
	for {
		var pending int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT count(*) FROM resources WHERE kind = 'noop' AND synced_gen < generation`).Scan(&pending))
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d noop roots never synced — workers(%d) < brokers(%d) did NOT converge via the mesh", pending, nWorkers, len(tiles))
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Logf("all %d roots synced: %d workers drained %d brokers' tiles via mesh push (workers < brokers)", nRoots, nWorkers, len(tiles))
}

// h2Server stands up an httptest server that speaks HTTP/2 over TLS (ALPN h2) so
// the bidi broker↔broker Route stream + the worker's WorkStream both work (the mesh
// REQUIRES HTTP/2; a plaintext HTTP/1.1 httptest server would break bidi). Prod
// uses h2c on the plaintext path; tests use the TLS path to avoid a stdlib race in
// http2ConfigureServer when many cleartext-HTTP/2 servers start concurrently.
func h2Server(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// h2TestClient is an ALPN-h2 client that skips cert verification, so ONE shared
// client can dial any h2Server (their per-server httptest certs differ) — used as
// both the mesh peer transport and the worker→broker client.
func h2TestClient() *http.Client {
	var protos http.Protocols
	protos.SetHTTP2(true)
	return &http.Client{Transport: &http.Transport{
		Protocols:       &protos,
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}}, //nolint:gosec // test-only
	}}
}

// shardRangeSlice returns the inclusive contiguous shard slice [lo, hi].
func shardRangeSlice(lo, hi int16) []int16 {
	out := make([]int16, 0, hi-lo+1)
	for s := lo; s <= hi; s++ {
		out = append(out, s)
	}
	return out
}

// tileOf returns the index of the tile whose contiguous range contains shard.
func tileOf(shard int16, tiles [][]int16) int {
	for i, t := range tiles {
		if len(t) > 0 && shard >= t[0] && shard <= t[len(t)-1] {
			return i
		}
	}
	return -1
}
