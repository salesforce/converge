package test

// chaos_faults.go — the fault ACTION REGISTRY for TestBrokerChaos, plus the richer
// fault implementations grouped by scenario (see -chaos-scenario). Each action is a
// named unit tagged with the scenario classes it belongs to; the driver
// (oneChaosAction) picks seeded-random among the actions active for the selected
// scenario. "all" activates every action.
//
// Scenario classes:
//   base    — worker/broker drain+kill+join + worker RST/half-open partition (original)
//   db      — Postgres blip, broker↔DB partition, control-plane (drainer/reaper) restart
//   rollout — rolling broker restart, simultaneous multi-broker loss, STAGGERED cascading
//             loss (crash during recovery), node loss, zero-broker outage
//   network — broker↔broker relay partition, asymmetric (send/recv-only), latency,
//             flaky link, relay-slow-peer (drives the non-blocking overflow-drop branch)
//   worker  — hung handler past ceiling, crash-replace, drain-restart, permanent scale-in,
//             MASS crash (all workers at once → zero-worker window)
//
// The broker-MESH fast-recovery edges (onPeerGone on peer departure, dropForeign on a
// foreign worker crash) are covered DETERMINISTICALLY in broker_mesh_recovery_test.go,
// NOT as soak actions — see the note in the registry where they'd otherwise live.
//
// The db-* actions cut the POOL (DialFunc trip + reset) — the connection-loss code path
// a Postgres restart also drives. A real Postgres process RESTART WITH PERSISTENT DATA
// (the pod dies + comes back on the same volume) is a MANUAL / k8s check, not a soak
// action: it needs a real DB restart, which isn't deterministic in a testcontainer run.
// The deadlock that a recovering DB's reconnect-storm induces IS codified deterministically
// in TestWorkQueueBatchWritersNoDeadlock (the ordered SKIP-LOCKED pre-lock guard).

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/salesforce/converge/internal/engine"
)

// chaosAction is one named fault the driver can inject. desc says WHAT the fault
// does to the fleet; expect says WHAT the system must do to survive it (the
// invariant/recovery a reader — and the -v narrated log — can check against). The
// driver logs "chaos[N] <name>: <desc> — expect: <expect>" each time it fires, so a
// -v run reads as a narrated fault story.
type chaosAction struct {
	name      string
	scenarios []string // which -chaos-scenario values activate it (besides "all")
	desc      string   // what this fault injects
	expect    string   // the guarantee/recovery the system must uphold under it
	apply     func(*chaosFleet)
}

func (a chaosAction) inScenario(s string) bool {
	if s == "all" {
		return true
	}
	for _, sc := range a.scenarios {
		if sc == s {
			return true
		}
	}
	return false
}

// activeActions returns the registry entries active for the fleet's scenario.
func (f *chaosFleet) activeActions() []chaosAction {
	out := make([]chaosAction, 0, len(chaosActions))
	for _, a := range chaosActions {
		if a.inScenario(f.scenario) {
			out = append(out, a)
		}
	}
	return out
}

// chaosActions is the full registry. Order is stable so a given seed reproduces.
var chaosActions = []chaosAction{
	// ── base: the original churn + worker partitions ─────────────────────────────
	{
		name: "worker-drain-restart", scenarios: []string{"base", "worker"},
		desc:   "SIGTERM-drain a worker (in-flight handlers finish within DrainGrace), then a replacement joins — a rolling worker deploy",
		expect: "no result lost: work in-flight at drain lands its Complete; anything unfinished re-dispatches (at-least-once). No double-apply",
		apply: func(f *chaosFleet) {
			if w := f.pickLiveWorker(); w != nil {
				w.stop()
				f.replaceWorker(w)
			}
		},
	},
	{
		name: "worker-crash-replace", scenarios: []string{"base", "worker"},
		desc:   "hard-CRASH a worker mid-task (cut its link first so NO clean Complete escapes — models an OOM-kill), then replace it",
		expect: "the broker's parked lease is reclaimed (releaseAbandoned / reaper) and the task re-runs on another worker; fence prevents any double-apply",
		apply: func(f *chaosFleet) {
			if w := f.pickLiveWorker(); w != nil {
				w.partition(faultCut) // no clean Complete escapes → models an OOM-kill mid-task
				w.kill()
				f.replaceWorker(w)
			}
		},
	},
	{
		name: "worker-join", scenarios: []string{"base", "worker"},
		desc:   "a NEW worker connects to a random live broker (scale-out / churn)",
		expect: "the new worker picks up work immediately; existing in-flight work is unaffected",
		apply:  func(f *chaosFleet) { f.addWorker() },
	},
	{
		name: "broker-drain-restart", scenarios: []string{"base", "rollout"},
		desc:   "gracefully DRAIN a broker (ReleaseClaims fast-handoff + deregister + reshard) then a replacement joins — a rolling broker deploy",
		expect: "its shard tile is re-covered by survivors + the replacement; released leases re-claim in ms (not the reaper window); its workers reconnect elsewhere",
		apply: func(f *chaosFleet) {
			if f.liveBrokerCount() > 2 {
				if b := f.pickLiveBroker(); b != nil {
					b.stop(f.ctx)
					f.retile()
					f.startNewBrokerForGap()
				}
			}
		},
	},
	{
		name: "broker-crash", scenarios: []string{"base", "rollout"},
		desc:   "hard-CRASH a broker (close its listener, NO release/deregister — node loss / OOM)",
		expect: "its leases strand only until the reaper's StaleAfter, then survivors (re-tiled to cover its shards) re-claim + finish the work",
		apply: func(f *chaosFleet) {
			if f.liveBrokerCount() > 2 {
				if b := f.pickLiveBroker(); b != nil {
					b.kill()
					f.retile()
					f.startNewBrokerForGap()
				}
			}
		},
	},
	{
		name: "broker-join", scenarios: []string{"base", "rollout"},
		desc:   "a NEW broker binds a listener, registers in cluster_members, and the keyspace re-tiles onto it (scale-out)",
		expect: "the new broker takes over its slice of shards and starts claiming; no work is dropped during the re-tile",
		apply:  func(f *chaosFleet) { f.addBroker() },
	},
	{
		name: "worker-rst-partition", scenarios: []string{"base", "network"},
		desc:   "abruptly CUT a worker's connections (RST-like: reads+writes fail now) for 2s, then heal — a cable-pull / LB drop the peer notices fast",
		expect: "the worker's stream errors + it reconnects; the broker frees its sent-but-unacked leases fast (releaseAbandoned) so the work re-runs promptly",
		apply: func(f *chaosFleet) {
			if w := f.pickLiveWorker(); w != nil {
				w.partition(faultCut)
				time.AfterFunc(2*time.Second, w.heal)
			}
		},
	},
	{
		name: "worker-halfopen-partition", scenarios: []string{"base", "network"},
		desc:   "SILENT half-open partition a worker for 4s (reads hang, writes vanish, no TCP FIN) — a NAT/firewall drop the socket never notices",
		expect: "the broker's WorkStream keepalive detects the dead stream and frees the worker's leases in seconds — NOT stranded the full 30-min ceiling",
		apply: func(f *chaosFleet) {
			if w := f.pickLiveWorker(); w != nil {
				w.partition(faultBlackhole) // silent: keepalive must detect + free the leases
				time.AfterFunc(4*time.Second, w.heal)
			}
		},
	},

	// ── db: the shared backend + control plane ───────────────────────────────────
	{
		name: "db-blip", scenarios: []string{"db"},
		desc:   "a 3s Postgres outage: trip the pool's DialFunc + Reset live conns so every broker's claim/heartbeat/drain fails, then heal",
		expect: "the heartbeat-failure guard pauses NEW claims (bounding blast radius); on heal the whole system resumes with no work lost and no falsely-reaped live lease double-applied",
		apply: func(f *chaosFleet) {
			// cutDBFor SELF-GUARDS: no-op while already cut or inside its post-heal
			// recovery gap, so back-to-back db actions can't hold the DB down forever
			// (the bug: without this, every ~tick re-cut before the heal timer → DB
			// never up → zero progress).
			f.cutDBFor(3 * time.Second)
		},
	},
	{
		name: "db-partition-long", scenarios: []string{"db"},
		desc:   "a LONGER 8s Postgres outage (broker↔DB partition) — same mechanism as db-blip, held long enough to fully stall claiming",
		expect: "brokers pause claiming and their in-flight parked stages ride it out; recovery is clean once the DB returns (no lost/duplicated work)",
		apply:  func(f *chaosFleet) { f.cutDBFor(8 * time.Second) },
	},
	{
		name: "control-restart", scenarios: []string{"db"},
		desc:   "BOUNCE the control plane (drainer + reaper) for 2s — while down, outbox rows don't apply and stale leases aren't reaped",
		expect: "work PAUSES then RESUMES when the sweepers return (they're resumable) — no permanent stranding, no lost outbox rows",
		apply: func(f *chaosFleet) {
			if f.control == nil {
				return
			}
			f.control.stop()
			time.AfterFunc(2*time.Second, func() { _ = f.control.start(f.ctx) })
		},
	},

	// ── rollout: correlated / compound faults ────────────────────────────────────
	{
		name: "rolling-broker-restart", scenarios: []string{"rollout"},
		desc:   "drain EVERY live broker one-by-one (ReleaseClaims + reshard + replace before the next), keeping a ≥2 floor — a real deploy rollout",
		expect: "coverage never drops to zero; each drained broker hands its tile off cleanly, so the workload keeps flowing throughout the rollout",
		apply: func(f *chaosFleet) {
			brokers := f.snapshotLiveBrokers()
			for _, b := range brokers {
				if f.liveBrokerCount() <= 2 {
					break // keep a floor so tiles stay covered mid-rollout
				}
				b.stop(f.ctx)
				f.retile()
				f.addBroker() // replacement joins before moving to the next
				time.Sleep(500 * time.Millisecond)
			}
		},
	},
	{
		name: "multi-broker-loss", scenarios: []string{"rollout"},
		desc:   "TWO brokers CRASH at once (a rack/AZ loss) — mass reshard + mass lease reclaim in one hit",
		expect: "survivors re-tile to cover both dead tiles; all the crashed leases reclaim via the reaper and the work re-runs — no permanent gap",
		apply: func(f *chaosFleet) {
			brokers := f.snapshotLiveBrokers()
			if len(brokers) < 3 {
				return
			}
			brokers[0].kill()
			brokers[1].kill()
			f.retile()
			f.addBroker()
			f.addBroker()
		},
	},
	{
		name: "node-loss", scenarios: []string{"rollout"},
		desc:   "a NODE dies: a broker AND 2 co-located workers crash TOGETHER (a correlated failure domain, not independent faults)",
		expect: "the broker's tile re-covers AND its crashed workers' in-flight tasks reclaim + re-run — the system tolerates correlated, not just independent, loss",
		apply: func(f *chaosFleet) {
			if f.liveBrokerCount() > 2 {
				if b := f.pickLiveBroker(); b != nil {
					b.kill()
					f.retile()
					f.startNewBrokerForGap()
				}
			}
			for i := 0; i < 2; i++ {
				if w := f.pickLiveWorker(); w != nil {
					w.partition(faultCut)
					w.kill()
					f.replaceWorker(w)
				}
			}
		},
	},
	{
		name: "zero-broker-blip", scenarios: []string{"rollout"},
		desc:   "BRIEF TOTAL broker outage: crash ALL brokers at once, then immediately bring a fresh set up — nothing is claimed while they're down",
		expect: "on recovery the reaper frees the crashed leases and the new brokers re-claim + drain everything — full outage is survivable, just paused",
		apply: func(f *chaosFleet) {
			brokers := f.snapshotLiveBrokers()
			if len(brokers) == 0 {
				return
			}
			for _, b := range brokers {
				b.kill()
			}
			// bring the fleet back to its baseline size.
			for i := 0; i < 3; i++ {
				f.addBroker()
			}
		},
	},

	// ── network: richer transport faults ─────────────────────────────────────────
	{
		name: "relay-mesh-partition", scenarios: []string{"network"},
		desc:   "sever a broker's PEER-relay reachability for 3s (Route/RelayComplete to it fail) WITHOUT touching its worker-facing listener",
		expect: "the peer-RPC timeouts fire so peers don't hang; the broker's own workers keep running; relayed results fall back to the owner's deadline/reaper",
		apply: func(f *chaosFleet) {
			if b := f.pickLiveBroker(); b != nil {
				b.relayGate.set(faultCut)
				time.AfterFunc(3*time.Second, func() { b.relayGate.set(faultNone) })
			}
		},
	},
	{
		name: "worker-asymmetric-partition", scenarios: []string{"network"},
		desc:   "ONE-WAY partition a worker for 3s: either recv-only (reads hang, StageCompletes still land) or send-only (writes vanish, the WorkStream still reads) — a one-way firewall/route drop",
		expect: "the half that works doesn't mask the half that doesn't; the stream is eventually torn down + reconnects, and any un-acked work re-runs",
		apply: func(f *chaosFleet) {
			if w := f.pickLiveWorker(); w != nil {
				if f.rng.Intn(2) == 0 {
					w.partition(faultRecvOnly) // reads hang, writes ok
				} else {
					w.partition(faultSendOnly) // writes vanish, reads ok
				}
				time.AfterFunc(3*time.Second, w.heal)
			}
		},
	},
	{
		name: "worker-latency", scenarios: []string{"network"},
		desc:   "DEGRADE (not cut) a worker's link for 4s: inject per-I/O latency so RPCs flirt with their timeouts without a clean break",
		expect: "the system tolerates a slow link — tasks still complete (perhaps after a retry) rather than being wrongly failed or stranded",
		apply: func(f *chaosFleet) {
			if w := f.pickLiveWorker(); w != nil {
				w.partition(faultLatency)
				time.AfterFunc(4*time.Second, w.heal)
			}
		},
	},
	{
		name: "worker-flaky-link", scenarios: []string{"network"},
		desc:   "INTERMITTENT link: flap a worker cut↔heal 4× (~400ms each) so its stream repeatedly drops and reconnects",
		expect: "reconnect backoff+jitter keeps the flapping worker from storming, and its work still lands via at-least-once across the flaps",
		apply: func(f *chaosFleet) {
			w := f.pickLiveWorker()
			if w == nil {
				return
			}
			// Scope the flap loop to the fleet ctx so it can't outlive the test and
			// race worker/cleanup teardown (a detached fire-and-forget goroutine would
			// keep flipping w's gate after shutdown began). It exits promptly on cancel.
			go func() {
				for i := 0; i < 4; i++ {
					if f.ctx.Err() != nil {
						return
					}
					w.partition(faultCut)
					select {
					case <-time.After(400 * time.Millisecond):
					case <-f.ctx.Done():
						w.heal()
						return
					}
					w.heal()
					select {
					case <-time.After(400 * time.Millisecond):
					case <-f.ctx.Done():
						return
					}
				}
			}()
		},
	},

	// NOTE: the broker-MESH fast-recovery edges (onPeerGone on peer departure,
	// dropForeign on a foreign worker's crash, relay send/completes overflow) are
	// covered DETERMINISTICALLY in test/broker_mesh_recovery_test.go — a stable
	// two-broker workers<brokers harness that CONFIRMS a parked push then injects the
	// exact fault and asserts fast recovery vs the ceiling. They are intentionally NOT
	// chaos-soak actions: the soak's constant tile/worker churn prevents a worker-less
	// broker from reliably learning (via mesh interest) that a peer serves the kind, so
	// it never claims+pushes → the precondition can't be built deterministically here.

	// ── network: broker relay latency (drives the non-blocking overflow-drop branch) ──
	{
		name: "relay-slow-peer", scenarios: []string{"network"},
		desc:   "DEGRADE (not cut) a broker's PEER-relay link for 4s (per-I/O latency) under a push workload, so its route send / completes queues back up and hit the non-blocking overflow-drop branches",
		expect: "the overflow drop is non-blocking (never stalls the claim loop); dropped pushes retry locally and dropped completes fall to the owner deadline/reaper — no stall, no double-apply",
		apply: func(f *chaosFleet) {
			if b := f.pickLiveBroker(); b != nil {
				b.relayGate.set(faultLatency)
				time.AfterFunc(4*time.Second, func() { b.relayGate.set(faultNone) })
			}
		},
	},

	// ── worker: edge lifecycle faults ────────────────────────────────────────────
	{
		name: "worker-permanent-scale-in", scenarios: []string{"worker"},
		desc:   "PERMANENTLY remove a worker (no replacement) above a 2-worker floor — the fleet SHRINKS, so fewer workers than brokers",
		expect: "the remaining workers absorb the load and relay covers broker tiles that lost their local worker; throughput drops but nothing strands",
		apply: func(f *chaosFleet) {
			if f.liveWorkerCount() > 2 {
				if w := f.pickLiveWorker(); w != nil {
					w.stop()
					f.mu.Lock()
					f.pruneDeadLocked()
					f.mu.Unlock()
				}
			}
		},
	},
	{
		name: "worker-hung-handler", scenarios: []string{"worker"},
		desc:   "WEDGE a connected worker for 50s (half-open so its in-flight handler can neither finish nor Complete, but the stream lingers) — a stuck-but-alive worker",
		expect: "the broker's RemoteFanoutCeiling (45s in-test) reclaims the parked slot BEFORE the heal, so a wedged worker can't pin capacity forever",
		apply: func(f *chaosFleet) {
			if w := f.pickLiveWorker(); w != nil {
				w.partition(faultBlackhole)
				time.AfterFunc(50*time.Second, w.heal) // > the 45s ceiling → ceiling reclaims first
			}
		},
	},
	{
		name: "cascading-broker-loss", scenarios: []string{"rollout"},
		desc:   "STAGGERED broker loss: crash one broker + reshard + replace, then a beat later crash ANOTHER while the first's leases are still being reclaimed — loss DURING recovery, not the one-shot multi-broker-loss",
		expect: "each crash's tile re-covers and its leases reclaim even though the fleet never fully settles between hits — recovery is composable, a second failure mid-recovery doesn't strand the first's work",
		apply: func(f *chaosFleet) {
			// Only when there's headroom to stay above a survivable floor across BOTH hits.
			if f.liveBrokerCount() < 3 {
				return
			}
			if b := f.pickLiveBroker(); b != nil {
				b.kill()
				f.retile()
				f.startNewBrokerForGap()
			}
			// A short beat so the first crash's reaper reclaim + retile is IN FLIGHT (not
			// settled) when the second lands — the "loss during recovery" this exercises.
			time.Sleep(1500 * time.Millisecond)
			if f.liveBrokerCount() >= 3 {
				if b := f.pickLiveBroker(); b != nil {
					b.kill()
					f.retile()
					f.startNewBrokerForGap()
				}
			}
		},
	},
	{
		name: "worker-mass-crash", scenarios: []string{"worker"},
		desc:   "ALL live workers hard-crash AT ONCE (transport cut + cancel), then a fresh set joins — the fleet briefly has ZERO workers while brokers hold claimed/parked work",
		expect: "brokers stop dispatching (no ready subscriber → the claim gate pauses NEW claims, in-flight parks); when workers return, the parked + reclaimed work drains — a total worker outage is survivable, just paused, with no double-apply",
		apply: func(f *chaosFleet) {
			workers := f.snapshotLiveWorkers()
			if len(workers) == 0 {
				return
			}
			for _, w := range workers {
				w.partition(faultCut) // model an OOM/node loss: connection dies with the process
				w.kill()
			}
			// Bring the fleet back to its baseline worker count so it can drain again.
			for _, w := range workers {
				f.replaceWorker(w)
			}
		},
	},
}

// ── restartable control plane ──────────────────────────────────────────────────

// restartableControl wraps the drainer/reaper engine so the "db" scenario can stop
// and restart it on the same pool. Guarded so concurrent stop/start from the
// timer callbacks can't race.
type restartableControl struct {
	pool   *pgxpool.Pool
	duties func() []engine.Duty

	mu  sync.Mutex
	eng *engine.Engine
}

func (c *restartableControl) start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.eng != nil {
		return nil // already running
	}
	eng := engine.NewEngine(c.duties(), engine.Deps{Pool: c.pool})
	if err := eng.Start(ctx); err != nil {
		return err
	}
	c.eng = eng
	return nil
}

func (c *restartableControl) running() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.eng != nil
}

func (c *restartableControl) stop() {
	c.mu.Lock()
	eng := c.eng
	c.eng = nil
	c.mu.Unlock()
	if eng != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = eng.Stop(sctx)
		cancel()
	}
}

// ── fleet count/snapshot helpers used by the actions ─────────────────────────────

func (f *chaosFleet) liveBrokerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.brokers {
		if b.isRunning() {
			n++
		}
	}
	return n
}

func (f *chaosFleet) liveWorkerCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, w := range f.workers {
		if w.isRunning() {
			n++
		}
	}
	return n
}

func (f *chaosFleet) snapshotLiveBrokers() []*managedBroker {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*managedBroker, 0, len(f.brokers))
	for _, b := range f.brokers {
		if b.isRunning() {
			out = append(out, b)
		}
	}
	return out
}

func (f *chaosFleet) snapshotLiveWorkers() []*managedWorker {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*managedWorker, 0, len(f.workers))
	for _, w := range f.workers {
		if w.isRunning() {
			out = append(out, w)
		}
	}
	return out
}

// stallLimit is the max time ready-count may plateau WITH NO ACTIVE FAULT before
// the liveness invariant flags a livelock. Generous: the fleet must absorb a fault
// window + its recovery (reshard, reconnect, retry) between progress ticks.
const stallLimit = 120 * time.Second

// faultActive reports whether a whole-system deliberate outage is in progress — a
// DB cut or a stopped control plane. The liveness + stranding invariants skip while
// one is active (no progress / stale heartbeats are EXPECTED during it, not a bug).
func (f *chaosFleet) faultActive() bool {
	if f.dbGate != nil && f.dbGate.get() == faultCut {
		return true
	}
	if f.control != nil && !f.control.running() {
		return true
	}
	return false
}

// cutDBFor trips the DB gate for `d`, then heals AND holds a recovery gap (2×d)
// before another db cut may fire. SELF-GUARDING via dbBusy (CAS): if a cut is
// already in progress or inside its recovery gap, this no-ops — so back-to-back db
// actions can't hold the DB down indefinitely (the bug that made the db scenario
// make zero progress). Reset() drops live conns so the cut bites immediately, and
// again on heal so brokers reconnect at once. No-op if the db gate isn't wired.
func (f *chaosFleet) cutDBFor(d time.Duration) {
	if f.dbGate == nil {
		return
	}
	if !f.dbBusy.CompareAndSwap(false, true) {
		return // a cut is already active or in its recovery gap
	}
	f.dbGate.set(faultCut)
	f.pool.Reset()
	time.AfterFunc(d, func() {
		f.dbGate.set(faultNone)
		f.pool.Reset()
		// recovery gap: let the system fully catch up before the next cut is allowed.
		time.AfterFunc(2*d, func() { f.dbBusy.Store(false) })
	})
}
