package converge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/salesforce/converge/pkg/drainctx"
	"github.com/salesforce/converge/sdk-go/workerpb"
)

// runner is the transport ENGINE behind the converge worker — NOT an author API. A
// provider author calls converge.Serve; the worker owns a runner and drives it. The runner
// is a DUMB worker client that connects INTO the cluster and pulls work, in the
// Temporal/Restate worker style: it holds a reactionRegistry of the kinds it can execute,
// opens a bidirectional WorkStream to a broker, runs each streamed task's reaction locally
// via the kind's ReactionHandler, and reports the result back UP the same stream (a
// StageComplete). It has NO database handle, NO shard knowledge, and NO worker_id — the
// broker owns the work_queue lease and performs the fenced status write on the worker's
// behalf, so a worker binary can live entirely outside the cluster and reach the broker
// over (m)TLS Connect.
//
// The runner OUTLIVES a single stream: run.go re-enters Run on every reconnect. So the
// cross-session state (the in-flight task set + the buffered results awaiting redelivery)
// lives on the runner, not per-Run — which is what makes a worker that reconnects to a
// DIFFERENT broker deliver its computed results there instead of re-running the work.

// runnerConfig configures a runner.
type runnerConfig struct {
	// Client is the broker transport the worker pulls work + config over (built with the
	// caller's HTTP client + (m)TLS + base URL). Required. A transport (WorkStream +
	// GetProviderConfig) — the generated Connect client satisfies it.
	Client transport
	// Kinds is the set of kindRuntimes this worker can execute: each carries the kind name
	// and its Handlers (the code, keyed by reaction name). The worker holds NO manifest —
	// the broker ships the selected reaction NAME in each StageTask, and the worker looks up
	// the handler by (kind, reaction). Required (at least one).
	Kinds []kindRuntime
	// MaxInflight bounds concurrent task execution; it also sizes the broker's per-kind
	// fanout buffer headroom for this worker (backpressure). 0 → 1.
	MaxInflight int
	// Logger is the base logger; per-task loggers derive from it. nil → default.
	Logger *slog.Logger
	// OnConfigUpdate, if set, is invoked when the broker PUSHES a (kind, kindVersion)'s new
	// default providerconfig down the WorkStream (a ProviderConfigUpdate). A providerconfig
	// is a MONOLITH of {spec, data}, so BOTH arrive together in one push and one call — the
	// host applies both into its configCache so the live config the providers read reflects
	// an operator's edit at once, without waiting for the periodic GetProviderConfig poll.
	// Defaults are per (kind, kindVersion); the kindVersion routes into the right cache slot.
	// Both empty = the default was deleted. nil → pushes are ignored (the worker still picks
	// the change up on its next poll).
	OnConfigUpdate func(kind Kind, kindVersion int, spec json.RawMessage, data []byte)

	// OnConnect, if set, is invoked once per successful (re)connect — right after the
	// Subscribe is sent and before the receive loop processes any task. The host wires it to
	// configCache.load so a RECONNECTING worker re-pulls the CURRENT default config/bundle
	// instead of trusting the boot snapshot: a broker restart or a disconnect window can
	// leave the worker's cache stale, and the broker only PUSHES future changes — without a
	// re-pull the worker relies on the (slow) periodic refresh and a config/bundle-
	// bootstrapped provider can terminal-fail "no bundle yet" first. It runs on the connect
	// ctx; a slow/failed load must not wedge the stream, so keep it bounded (configCache.load
	// has its own timeout) — an error is logged, not fatal (the broker's prime-on-subscribe +
	// the periodic refresh are the backstops). nil → skipped.
	OnConnect func(ctx context.Context)

	// DrainGrace is how long run lets in-flight handlers finish after its ctx is cancelled
	// (SIGTERM) before abandoning them — the graceful-drain window. Keep it below the
	// deployment's termination grace so the process exits on its own, and at/above the
	// slowest handler so a node move drains real work. 0 → no drain (in-flight cancelled at
	// once with ctx).
	DrainGrace time.Duration

	// HeartbeatEvery is the cadence of the worker's per-task liveness attestation frame
	// (WorkHeartbeat). Each tick sends ONE coalesced frame listing the tasks whose handler
	// goroutine is STILL RUNNING (hasn't returned), so the broker keeps their leases fresh; a
	// task whose handler has finished stops being attested (its buffered result carries it
	// forward). 0 → defaultWorkerHeartbeatEvery.
	HeartbeatEvery time.Duration

	// InitialUnready, if set, is evaluated ONCE per (re)connect immediately after the
	// Subscribe and returns the (kind, kindVersion) pairs this worker is currently UNREADY
	// for (a provider whose downstream is degraded). run then sends an Interest RS- for each
	// SYNCHRONOUSLY, before the task loop — so the broker learns the degraded kinds in the
	// SAME ordered burst as the Subscribe and never dispatches one of them into the window
	// between subscribe and the first health-poll tick. nil → the worker subscribes fully
	// ready (every kind), the unchanged default.
	InitialUnready func() []KindVersion
}

// Worker liveness/attestation cadence. Kept well under the broker's require-within window so
// several attestation frames land inside it, and that window sits under the reaper's
// StaleAfter — a silent worker's lease crosses stale within a bounded number of missed
// frames, never indefinitely.
const defaultWorkerHeartbeatEvery = 5 * time.Second

// inflightTask is one task this worker is currently executing (or holding a computed
// result for), tracked on the runner so it survives a stream drop: its epoch (echoed to
// the broker for the fenced write + attestation) and whether it has finished (a finished
// task stops being attested but its buffered result awaits redelivery).
type inflightTask struct {
	epoch int64
	done  bool
}

// runner is a dumb worker. Construct with newRunner, drive with Run.
type runner struct {
	client         transport
	reactions      *reactionRegistry
	kinds          []string
	kindVersions   []int32 // parallel to kinds: the web-API kindVersion this worker serves per kind
	maxInfl        int
	log            *slog.Logger
	onConfig       func(kind Kind, kindVersion int, spec json.RawMessage, data []byte) // broker providerconfig push sink (spec+data together); nil-safe
	onConnect      func(ctx context.Context)                                           // per-(re)connect config re-pull hook; nil-safe
	initUnready    func() []KindVersion                                                // per-(re)connect initial RS- set; nil-safe
	drainGrace     time.Duration                                                       // graceful in-flight drain window on ctx cancel
	heartbeatEvery time.Duration                                                       // per-task liveness attestation cadence

	// sendCh is the CURRENT WorkStream's client-send mux: the frames a worker sends UP are
	// StageCompletes (finished-task results, from task goroutines), readiness Interests
	// (RS+/RS-, from the host's health poller), and WorkHeartbeats (per-task liveness, from
	// the attestation ticker). A SINGLE sender goroutine in Run() drains this onto the bidi
	// stream — connect streams are not safe for concurrent Send. Set at the top of each Run
	// (one active stream at a time); a send after the stream closed is dropped (a Complete is
	// BUFFERED for redelivery on the next stream, see resultBuf; a dropped Interest/heartbeat
	// self-heals on the next edge/tick). Guarded by sendMu because Run may be re-entered on
	// reconnect AND the health poller + attestation ticker send concurrently with the task
	// goroutines.
	sendMu sync.Mutex
	sendCh chan *workerpb.WorkStreamClientMsg

	// mu guards the cross-session task state below (read/written by task goroutines, the
	// attestation ticker, and Run across reconnects).
	mu sync.Mutex
	// inflight tracks every task this worker is currently executing, keyed by lease_token.
	// The attestation ticker reads it to build each WorkHeartbeat; a reconnect reads it to
	// re-advertise the still-live tasks in the Subscribe.resumed burst so the new broker
	// adopts their liveness.
	inflight map[string]*inflightTask
	// resultBuf holds computed StageCompletes that could not be sent (the stream was down
	// when the handler finished). On the next connect, Run REDELIVERS them before pulling new
	// work — so a worker that reconnects to a DIFFERENT broker lands its results there
	// instead of re-executing. Bounded by resultBufCap; past the cap a result is discarded +
	// counted (the task re-runs via the reaper, at-least-once), never silently dropped.
	resultBuf []*workerpb.StageComplete

	// discarded counts task RESULTS the worker computed but could not report even by
	// buffering (the buffer was full). These tasks are NOT lost — the broker's lease reaps
	// and the task re-runs (at-least-once, the claim_epoch fence prevents double-apply) — but
	// the compute was wasted, so a rising count signals sustained broker churn / network
	// trouble eating throughput. Logged (with the running total) on each discard.
	discarded atomic.Int64
}

// resultBufCap bounds the redelivery buffer. Sized above a healthy stream's in-flight
// ceiling so a brief reconnect never overflows; a sustained outage past it discards +
// counts (the reaper re-runs), so the buffer can never grow unbounded (OOM guard).
const resultBufCap = 1024

// newRunner builds a runner from the kindRuntimes the host assembled. It returns an error
// if no kinds are given (a worker that can execute nothing would poll forever for work it
// can't run).
func newRunner(cfg runnerConfig) (*runner, error) {
	if cfg.Client == nil {
		return nil, errors.New("converge: runnerConfig.Client is required")
	}
	if len(cfg.Kinds) == 0 {
		return nil, errors.New("converge: runnerConfig.Kinds is empty (no kinds to execute)")
	}
	reactions := newReactionRegistry()
	kindStrs := make([]string, 0, len(cfg.Kinds))
	kindVersions := make([]int32, 0, len(cfg.Kinds))
	for _, kr := range cfg.Kinds {
		// kind_version is REQUIRED and explicit (>= 1): a worker MUST advertise the web-API
		// version it serves for each kind — there is no implicit v1 default. An unversioned
		// kindRuntime is a wiring bug: fail fast at construction rather than silently
		// advertising v1 (which would then claim/run the wrong version).
		if kr.KindVersion < 1 {
			return nil, fmt.Errorf("converge: kindRuntime for kind %q has no kind_version (must be >= 1); every worker must explicitly advertise its kind_version", kr.Kind)
		}
		kr.registerInto(reactions)
		kindStrs = append(kindStrs, string(kr.Kind))
		kindVersions = append(kindVersions, int32(kr.KindVersion))
	}
	maxInfl := cfg.MaxInflight
	if maxInfl <= 0 {
		maxInfl = 1
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	hbEvery := cfg.HeartbeatEvery
	if hbEvery <= 0 {
		hbEvery = defaultWorkerHeartbeatEvery
	}
	return &runner{
		client:         cfg.Client,
		reactions:      reactions,
		kinds:          kindStrs,
		kindVersions:   kindVersions,
		maxInfl:        maxInfl,
		log:            log,
		onConfig:       cfg.OnConfigUpdate,
		onConnect:      cfg.OnConnect,
		initUnready:    cfg.InitialUnready,
		drainGrace:     cfg.DrainGrace,
		heartbeatEvery: hbEvery,
		inflight:       map[string]*inflightTask{},
	}, nil
}

// Run opens the WorkStream and processes tasks until ctx is cancelled or the stream errors,
// returning the error so the caller can reconnect with backoff (the SDK keeps no reconnect
// policy of its own).
//
// SHAPE — the mirror of the broker handler: ONE sender goroutine (drains sendCh, onto which
// finished handlers enqueue StageCompletes, the health poller enqueues Interests, and the
// attestation ticker enqueues WorkHeartbeats) and this goroutine as the sole Receiver (the
// dispatch loop). connect forbids concurrent Send/Receive with themselves, so exactly one
// of each; the two run concurrently, which connect allows.
//
// RECONNECT WITHOUT REDO — the runner outlives one stream. Handlers run on handlerCtx (the
// caller's ctx, NOT the per-stream drainCtx), so a transport DROP does not cancel a running
// handler: it keeps computing and buffers its result (resultBuf), which the NEXT Run
// redelivers to whatever broker it reconnects to. Only a genuine ctx cancel (SIGTERM) drains
// then stops handlers. On connect, Run first replays any buffered results and advertises the
// still-live tasks in the Subscribe so the new broker adopts their liveness.
//
// GRACEFUL DRAIN on SIGTERM falls out of the context split: the stream runs on drainCtx,
// which follows ctx but lingers DrainGrace after cancel. On cancel the sem-acquire stops
// pulling NEW work; in-flight handlers finish and enqueue Completes, the sender flushes them,
// drainCtx tears the stream down, and the tail waits for handlers + the sender to drain.
func (r *runner) Run(ctx context.Context) error {
	drainCtx, cancelDrain := drainctx.New(ctx, r.drainGrace)
	defer cancelDrain()

	// Open the BIDIRECTIONAL stream on drainCtx (so it outlives ctx by DrainGrace to carry
	// drain completions), and SUBSCRIBE as the first client message. The Subscribe carries
	// the still-live tasks (resumed) so a broker we reconnect to adopts their liveness (keeps
	// their leases fresh via epoch-fenced heartbeat) instead of reaping a task under a worker
	// that is still finishing it.
	stream := r.client.WorkStream(drainCtx)
	if err := stream.Send(&workerpb.WorkStreamClientMsg{
		Body: &workerpb.WorkStreamClientMsg_Subscribe{Subscribe: &workerpb.Subscribe{
			Kinds:        r.kinds,
			KindVersions: r.kindVersions, // parallel to Kinds: the (kind, kindVersion) this worker serves
			MaxInflight:  uint32(r.maxInfl),
			Resumed:      r.resumedTasks(),
		}},
	}); err != nil {
		return fmt.Errorf("converge: subscribe: %w", err)
	}

	// Initial readiness, in the SAME ordered burst as the Subscribe: send an Interest RS-
	// for every (kind, kindVersion) this worker is currently unready for, directly on the
	// stream BEFORE the task loop. This closes the subscribe→first-poll window — the broker
	// processes Subscribe then these RS- in order, so a kind that boots degraded never has
	// its work dispatched here even once. Sent on the raw stream (not sendCh, which isn't set
	// up yet); a send error tears the stream down like Subscribe.
	if r.initUnready != nil {
		for _, km := range r.initUnready() {
			if err := stream.Send(&workerpb.WorkStreamClientMsg{Body: &workerpb.WorkStreamClientMsg_Interest{Interest: &workerpb.Interest{
				Kind: string(km.Kind), KindVersion: int32(km.Version), HasWorker: false,
			}}}); err != nil {
				return fmt.Errorf("converge: initial readiness: %w", err)
			}
		}
	}

	// Re-pull the current default config/bundle on EVERY (re)connect: the boot snapshot may
	// be stale after a broker restart or a disconnect window, and the broker only pushes
	// FUTURE changes. Runs on ctx (not drainCtx) so a shutdown-time reconnect doesn't block;
	// configCache.load is timeout-bounded and non-fatal on error (the broker's
	// prime-on-subscribe + the periodic refresh back it up). nil-safe.
	if r.onConnect != nil {
		r.onConnect(ctx)
	}

	// The client-send mux: finished handlers enqueue StageCompletes on sendCh; the sender
	// goroutine is the SOLE Sender. Buffered so a finishing handler never blocks. r.send
	// reads r.sendCh under sendMu; nil after Run returns → buffered for redelivery.
	sendCh := make(chan *workerpb.WorkStreamClientMsg, r.maxInfl+16)
	r.sendMu.Lock()
	r.sendCh = sendCh
	r.sendMu.Unlock()
	senderDone := make(chan struct{})
	go func() {
		defer close(senderDone)
		for msg := range sendCh { // ranges until the tail closes sendCh (all handlers done)
			if err := stream.Send(msg); err != nil {
				// Stream broken (broker gone mid-run): the sender is dead, but sendCh is still
				// OPEN. Detach the mux (nil r.sendCh) so subsequent send()s buffer their result
				// for redelivery on the next stream. The Receive loop errors too, so Run tears
				// down and the caller reconnects.
				r.sendMu.Lock()
				if r.sendCh == sendCh { // don't clobber a newer stream's mux on reconnect
					r.sendCh = nil
				}
				r.sendMu.Unlock()
				return
			}
		}
	}()

	// REDELIVER buffered results from a prior session FIRST, before pulling new work — so a
	// worker that reconnects (to this or a different broker) lands its already-computed
	// results instead of re-running. A result whose row was reaped+re-issued carries a stale
	// epoch and the broker's fence no-ops it (harmless); a live one lands.
	r.redeliverBuffered()

	sem := make(chan struct{}, r.maxInfl)
	var wg sync.WaitGroup

	// closeSend flushes-then-detaches the client-send mux exactly once, shared by the
	// prompt-drain watcher and the tail (whichever reaches idle first). It nils r.sendCh AND
	// closes sendCh under sendMu held ACROSS both: nil-then-close must be atomic wrt the
	// health poller + attestation ticker (uncounted goroutines that send under the same
	// lock), else one could observe a non-nil ch and then write to the just-closed channel
	// (panic). Closing sendCh lets the sender flush every buffered frame over the STILL-LIVE
	// stream and exit (senderDone); teardown (cancelDrain) happens only AFTER this returns,
	// so no Complete is lost to a prematurely-killed stream.
	var closeOnce sync.Once
	closeSend := func() {
		closeOnce.Do(func() {
			r.sendMu.Lock()
			r.sendCh = nil
			close(sendCh)
			r.sendMu.Unlock()
			<-senderDone
		})
	}

	// ATTESTATION TICKER: send ONE coalesced WorkHeartbeat per tick listing the tasks whose
	// handler goroutine is STILL RUNNING (hasn't returned), so the broker keeps their leases
	// fresh; a task whose handler has finished drops out (its buffered result carries the
	// completion forward). Bound to drainCtx so it stops with the stream; it sends through
	// sendMu (like the poller) so it can't race closeSend.
	go r.attestLoop(drainCtx)

	// PROMPT DRAIN-EXIT: on SIGTERM (ctx cancelled) the broker, on a rolling deploy, stays up
	// and keeps the stream open. This watcher waits for ctx.Done, then for the in-flight
	// handlers to finish, then FLUSHES their Completes (closeSend) and half-closes the send
	// side (CloseRequest) so the broker sees EOF and our Receive() returns — the loop exits
	// at once rather than idling out the full DrainGrace. New tasks stop being accepted after
	// ctx.Done (the task case's ctx branch), so once inflight hits 0 it can't climb again.
	var inflight atomic.Int64
	drainWake := make(chan struct{}, 1)
	go func() {
		select {
		case <-ctx.Done():
		case <-drainCtx.Done(): // Run already returned / grace expired — nothing to do
			return
		}
		for inflight.Load() > 0 {
			select {
			case <-drainWake: // a handler finished — re-check the count
			case <-drainCtx.Done(): // grace expired first; the deferred cancelDrain covers it
				return
			}
		}
		closeSend()
		_ = stream.CloseRequest()
	}()

	var recvErr error
	for {
		msg, err := stream.Receive()
		if err != nil {
			recvErr = err // stream ended: broker gone, drainCtx cancelled (drained/grace) after SIGTERM
			break
		}
		switch {
		case msg.GetConfig() != nil: // live default-providerconfig push (spec + data together) → apply, keep reading
			if r.onConfig != nil {
				cu := msg.GetConfig()
				r.onConfig(Kind(cu.GetKind()), int(cu.GetKindVersion()), rawOrNil(cu.GetConfig()), cu.GetBundle())
			}
		case msg.GetTask() != nil:
			// Acquire a slot; on SIGTERM stop pulling NEW work (in-flight ones drain).
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				recvErr = ctx.Err()
				goto drained
			}
			t := msg.GetTask()
			r.track(t) // register the task so the attestation ticker + resume burst see it
			inflight.Add(1)
			wg.Add(1)
			go func(t *workerpb.StageTask) {
				defer wg.Done()
				defer func() {
					<-sem
					if inflight.Add(-1) == 0 { // last one out — wake the drain watcher
						select {
						case drainWake <- struct{}{}:
						default:
						}
					}
				}()
				// Per-task panic boundary: a panicking handler fails ITS OWN task terminally
				// (a panic isn't retryable) and the process survives, so one kind's bug can't
				// take the worker down. Handlers run on ctx (the WORKER lifecycle), NOT the
				// per-stream drainCtx: a transport drop must not cancel a running handler — it
				// keeps computing and its result is buffered for redelivery. Only a genuine
				// SIGTERM (ctx cancel) drains and then, past DrainGrace, cancels it.
				defer r.recoverTask(ctx, t)
				r.runStage(ctx, t)
			}(t)
		}
	}

drained:
	wg.Wait()   // in-flight handlers finish (bounded by DrainGrace via runStage's deadline) + enqueue Completes
	closeSend() // flush + detach the mux (idempotent). The sender flushes buffered Completes
	// over the still-live stream; cancelDrain (deferred, or already called by the watcher)
	// tears the stream down only after that flush.
	if recvErr != nil && !errors.Is(recvErr, io.EOF) && ctx.Err() == nil {
		return fmt.Errorf("converge: WorkStream: %w", recvErr)
	}
	return ctx.Err()
}

// resumedTasks snapshots the still-live in-flight tasks as ResumedTask{lease_token, epoch}
// so a reconnecting worker advertises them in its Subscribe — the broker it lands on adopts
// their liveness (epoch-fenced heartbeat) and does not reap a task under a worker that is
// still finishing it. Includes tasks with a buffered-but-undelivered result too (still ours
// until the broker acks the write). Empty on a first connect.
func (r *runner) resumedTasks() []*workerpb.ResumedTask {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.inflight) == 0 {
		return nil
	}
	out := make([]*workerpb.ResumedTask, 0, len(r.inflight))
	for tok, it := range r.inflight {
		out = append(out, &workerpb.ResumedTask{LeaseToken: tok, ClaimEpoch: it.epoch})
	}
	return out
}

// track registers a freshly-dispatched task in the in-flight set, stamping its epoch. It is
// attested from the first tick after dispatch (dispatch itself proves the worker took it)
// until its handler goroutine returns.
func (r *runner) track(t *workerpb.StageTask) {
	r.mu.Lock()
	r.inflight[t.GetLeaseToken()] = &inflightTask{epoch: t.GetClaimEpoch()}
	r.mu.Unlock()
}

// untrack removes a task from the in-flight set. Called when the worker no longer holds
// anything for it: its result went straight onto a live stream (send), a buffered result was
// redelivered (redeliverBuffered), or the redelivery buffer overflowed and the result was
// discarded (send → reaper re-runs it). A result buffered across a stream drop stays TRACKED
// (done=true) until one of those happens, so resumedTasks can re-advertise it.
func (r *runner) untrack(token string) {
	r.mu.Lock()
	delete(r.inflight, token)
	r.mu.Unlock()
}

// attestFrame builds the coalesced WorkHeartbeat for the current tick: the (token, epoch) of
// every in-flight task whose handler is STILL RUNNING (not done). A running handler goroutine
// IS a live worker, so its lease is attested for as long as the handler takes — however long
// that is. Env.Heartbeat is an OPTIONAL progress signal, NOT a gate: a handler that never
// calls it (the common case — a straight-line 1-minute compute) stays attested. A genuinely
// wedged handler is bounded by the task's hard deadline (task_deadline_secs / the broker's
// no-deadline ceiling), not by attestation silence. A FINISHED task (done) is excluded: its
// buffered result carries the completion forward. Returns nil when nothing is attestable.
func (r *runner) attestFrame() *workerpb.WorkHeartbeat {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.inflight) == 0 {
		return nil
	}
	hb := &workerpb.WorkHeartbeat{}
	for tok, it := range r.inflight {
		if it.done {
			continue // finished — the buffered result carries it forward; nothing to keep alive
		}
		hb.LeaseToken = append(hb.LeaseToken, tok)
		hb.ClaimEpoch = append(hb.ClaimEpoch, it.epoch)
	}
	if len(hb.LeaseToken) == 0 {
		return nil
	}
	return hb
}

// attestLoop sends one coalesced WorkHeartbeat per tick through the send mux. Bound to the
// stream's ctx (stops with the session; the next Run starts a fresh one). A frame that can't
// enqueue (stream gone / buffer full) is simply skipped — the next tick re-attests the
// current set (absolute, idempotent), and a genuinely dropped stream reaps the leases anyway.
func (r *runner) attestLoop(ctx context.Context) {
	t := time.NewTicker(r.heartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			hb := r.attestFrame()
			if hb == nil {
				continue
			}
			r.sendMu.Lock()
			if r.sendCh != nil {
				select {
				case r.sendCh <- &workerpb.WorkStreamClientMsg{Body: &workerpb.WorkStreamClientMsg_Heartbeat{Heartbeat: hb}}:
				default: // mux full — skip; next tick re-attests
				}
			}
			r.sendMu.Unlock()
		}
	}
}

// redeliverBuffered replays the results buffered while the stream was down onto the fresh
// mux, before the task loop pulls new work. A result whose row was reaped+re-issued carries a
// stale epoch and the broker's fence no-ops it; a live one lands. Drops each from the buffer
// as it is enqueued; a full mux leaves the rest for the next attempt.
//
// A redelivered result's task is untracked here: send() kept it in r.inflight (done=true) so
// resumedTasks would re-advertise it across the drop; once its result is back on the wire the
// worker no longer owns it, so it must leave the in-flight set or resumedTasks would advertise
// a phantom on the NEXT reconnect.
func (r *runner) redeliverBuffered() {
	r.mu.Lock()
	buf := r.resultBuf
	r.resultBuf = nil
	r.mu.Unlock()
	if len(buf) == 0 {
		return
	}
	var unsent []*workerpb.StageComplete
	var sent []string // lease tokens redelivered this pass, untracked after releasing sendMu
	r.sendMu.Lock()
	for i, sc := range buf {
		if r.sendCh == nil {
			unsent = append(unsent, buf[i:]...)
			break
		}
		select {
		case r.sendCh <- &workerpb.WorkStreamClientMsg{Body: &workerpb.WorkStreamClientMsg_Complete{Complete: sc}}:
			sent = append(sent, sc.GetLeaseToken())
		default:
			unsent = append(unsent, buf[i:]...)
		}
		if len(unsent) > 0 {
			break
		}
	}
	r.sendMu.Unlock()
	if len(unsent) > 0 {
		r.mu.Lock()
		r.resultBuf = append(unsent, r.resultBuf...)
		r.mu.Unlock()
	}
	for _, tok := range sent {
		r.untrack(tok)
	}
}

// runStage executes one pure reaction and reports its result up the stream. The wire carries
// a Stage enum; the shared computeComplete body maps it to the fixed Trigger, decodes the
// typed request, looks up the kind's handler in the reactionRegistry (by (kind, kindVersion,
// reaction)), runs it, and encodes the Outcome back. runStage owns the transport-shaped parts
// around that pure core: it narrows ctx to task_deadline_ms, stamps the rich per-task logger,
// and r.sends the resulting StageComplete. Liveness is attested by the running handler
// goroutine itself (the attestation ticker lists every still-running task), so runStage wires
// no progress hook (nil): a task is attested for as long as its handler runs.
func (r *runner) runStage(ctx context.Context, t *workerpb.StageTask) {
	// A per-task ctx honoring task_deadline_ms bounds the reaction (0 → no per-task bound).
	// The deadline is the hard floor on a hung handler: a wedged handler is cancelled here and
	// its slot frees. The broker requires every kind to declare a deadline
	// (kind_config.task_deadline_secs), so 0 here means an old/misconfigured task, not the norm.
	taskCtx := ctx
	if ms := t.GetTaskDeadlineMs(); ms > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(ctx, durMs(ms))
		defer cancel()
	}
	log := r.log.With("kind", t.GetKind(), "resource", bytesToID(t.GetResourceId()),
		"stage", t.GetStage().String(), "reaction", t.GetReaction(), "generation", t.GetGeneration())
	sc := computeComplete(taskCtx, r.reactions, t, log, nil)
	r.send(ctx, t, sc)
}

// recoverTask is the deferred per-task panic boundary. If the handler goroutine panicked, it
// reports a TERMINAL stage failure for THIS task (a panic isn't retryable) and logs the
// stack, then lets the goroutine unwind normally. A panic in one kind's handler never
// propagates to crash the worker, isolating every other kind in the same binary.
func (r *runner) recoverTask(ctx context.Context, t *workerpb.StageTask) {
	p := recover()
	if p == nil {
		return
	}
	stack := debug.Stack()
	r.log.Error("converge: handler panic (task failed, worker survives)",
		"kind", t.GetKind(), "stage", t.GetStage().String(),
		"reaction", t.GetReaction(), "panic", p, "stack", string(stack))
	r.send(ctx, t, failComplete(t, fmt.Sprintf("handler panic: %v", p), true))
}

// send reports a finished stage's result. It marks the task done + enqueues the result onto
// the CURRENT WorkStream's client-send mux; the single sender goroutine writes it up the
// stream. If the stream is gone (broker down / mid-reconnect), the result is BUFFERED
// (resultBuf) for redelivery on the next connect — to whatever broker the worker lands on,
// which fences the write on the echoed epoch. Buffering (not dropping) is what avoids
// re-executing a computed result across a rollout. Only when the buffer is full does the
// result get discarded + counted (the task then re-runs via the reaper — at-least-once, the
// epoch fence prevents a double-apply).
func (r *runner) send(_ context.Context, t *workerpb.StageTask, sc *workerpb.StageComplete) {
	token := t.GetLeaseToken()

	r.sendMu.Lock()
	msg := &workerpb.WorkStreamClientMsg{Body: &workerpb.WorkStreamClientMsg_Complete{Complete: sc}}
	if r.sendCh != nil {
		select {
		case r.sendCh <- msg:
			r.sendMu.Unlock()
			// Result handed to the live stream's sender — the task is done AND its
			// completion is on the wire, so it no longer needs tracking (nothing to
			// attest, nothing to re-advertise).
			r.untrack(token)
			return
		default:
			// mux full (a wedged/backed-up stream) → fall through to buffering.
		}
	}
	r.sendMu.Unlock()

	// Stream down or mux full: buffer for redelivery on the next connect.
	r.mu.Lock()
	if len(r.resultBuf) < resultBufCap {
		r.resultBuf = append(r.resultBuf, sc)
		// Keep the task TRACKED but mark it done: done excludes it from attestation
		// (the handler has returned — its buffered result carries the completion
		// forward, so there's nothing live to keep alive), while staying in r.inflight
		// so resumedTasks re-advertises it on the next connect. That tells the broker
		// this worker still owns the task and is redelivering its result, narrowing
		// the window in which the reaper could reclaim + re-dispatch a task whose
		// result is already computed and merely waiting for the stream to return.
		if it := r.inflight[token]; it != nil {
			it.done = true
		}
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	// Buffer full: the result is discarded and the task re-runs via the reaper
	// (at-least-once; the epoch fence prevents a double-apply). Drop the tracking
	// too — we no longer hold a result for it.
	r.untrack(token)
	r.discardResult(t, sc, "redelivery buffer full")
}

// SendReadiness enqueues a worker-readiness Interest (RS+/RS-) for (kind, kindVersion) onto
// the CURRENT WorkStream's client-send mux — the SAME single-sender path a StageComplete
// rides. ready=false is RS- ("this provider is degraded for the pair, stop sending me its
// work"); ready=true is RS+. credit is left 0 — a worker advertises PRESENCE, not slots.
// Safe for concurrent callers (the health poller) via sendMu.
//
// It returns whether the frame was ENQUEUED. A false return (no active stream, or a full
// send buffer) means the flip did NOT reach the broker — the caller (the health poller) must
// NOT treat the edge as delivered, so it re-attempts on the next tick. Readiness is ABSOLUTE,
// so a re-attempt carries the current state; nothing is lost.
func (r *runner) SendReadiness(kind Kind, kindVersion int, ready bool) bool {
	msg := &workerpb.WorkStreamClientMsg{Body: &workerpb.WorkStreamClientMsg_Interest{Interest: &workerpb.Interest{
		Kind:        string(kind),
		KindVersion: int32(kindVersion),
		HasWorker:   ready, // RS+/RS-: this worker's readiness for the pair
	}}}
	// Hold sendMu ACROSS the send. The health poller runs on its own goroutine (NOT a counted
	// in-flight handler), so it can race closeSend's nil-then-close(sendCh); closeSend also
	// holds sendMu across its close, so the two are mutually excluded. Safe to hold the lock
	// because the send is NON-BLOCKING (the default branch).
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	if r.sendCh == nil {
		return false // no active stream (mid-reconnect / drained); the poller retries next tick
	}
	select {
	case r.sendCh <- msg:
		return true
	default:
		return false // send buffer full (wedged stream): retry on the next tick
	}
}

// discardResult counts + logs a result the worker computed but couldn't hand off even by
// buffering (the redelivery buffer was full). The task is NOT lost — the lease reaps and it
// re-runs (at-least-once; the epoch fence prevents double-apply) — but the compute was
// wasted, so a rising count signals sustained broker churn / network trouble.
func (r *runner) discardResult(t *workerpb.StageTask, sc *workerpb.StageComplete, why string) {
	n := r.discarded.Add(1)
	r.log.Warn("converge: result discarded (task will re-run)",
		"reason", why, "kind", t.GetKind(), "reaction", t.GetReaction(), "stage", t.GetStage().String(),
		"generation", t.GetGeneration(), "lease", sc.GetLeaseToken(), "discarded_total", n)
}

// durMs converts a wire millisecond deadline to a Duration.
func durMs(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }
