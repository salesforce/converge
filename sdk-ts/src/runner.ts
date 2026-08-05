// runner.ts is the WorkStream engine behind the converge worker: the lifecycle (dial,
// prime, refresh, poll, reconnect-with-backoff) and ONE stream session (subscribe, pull
// tasks, dispatch, report, drain). A provider author never touches this; serve() /
// runWorker() drive it. The runner holds the providers it can execute, opens a bidi
// WorkStream to a broker, runs each streamed task's reaction locally, and reports the result
// back UP the same stream. It has NO database, NO shard knowledge, NO worker_id trust — the
// broker owns the lease and the fenced status write, so a worker lives entirely outside the
// cluster.
import { create } from "@bufbuild/protobuf";
import {
  WorkStreamClientMsgSchema,
  SubscribeSchema,
  InterestSchema,
  type WorkStreamClientMsg,
  type StageTask,
  type StageComplete,
} from "../gen/converge/worker/v1/worker_pb.js";
import { jitter, sleep } from "./backoff.js";
import type { Config, Logger } from "./config.js";
import { ConfigCache, primeAndLoad } from "./configcache.js";
import { completeFromOutcome, failComplete, requestFromTask } from "./convert.js";
import { initialUnready, runHealthPoller, type SendReadiness } from "./health.js";
import { type Provider, type ProviderConfig, type KindVersion, isTerminal } from "./types.js";
import type { WorkerClient } from "./transport.js";

// runLoop owns the WHOLE worker lifecycle over an already-built client until signal aborts,
// then drains in-flight work and returns. It is the shared body of serve() (which dials +
// reads env) and runWorker() (which injects the client + options). It requires ≥1 provider
// (a worker that can execute nothing would poll forever).
//
// Lifecycle: prime+load the config cache (blocking, jittered backoff until the broker
// answers, so the first task sees live config); start the background config-refresh + the
// health poller (both span the reconnect loop); then reconnect-with-backoff across stream
// drops until signal aborts. A dropped stream is reconnected internally (never surfaced).
export async function runLoop(
  client: WorkerClient,
  providers: Provider[],
  cfg: Config,
  log: Logger,
  signal: AbortSignal,
): Promise<void> {
  if (providers.length === 0) {
    throw new Error("converge: no providers registered");
  }

  const pairs: KindVersion[] = providers.map((p) => p.kind());

  // The config cache: onChange fires the matching provider's onConfig with the full
  // {spec, data} monolith — the provider's hook to react to its default (dial a client,
  // recompile a bundle). An empty pair (both empty) = the default was deleted.
  const onConfigByKV = new Map<string, (cfg: ProviderConfig) => void>();
  for (const p of providers) {
    const kv = p.kind();
    onConfigByKV.set(`${kv.kind} ${kv.version}`, (c) => p.onConfig(c));
  }
  const cache = new ConfigCache(client, pairs, cfg.configRefreshMs, (kind, version, spec, data) => {
    const fn = onConfigByKV.get(`${kind} ${version}`);
    if (fn) fn({ spec, data });
  });

  // Prime + load the defaults BEFORE the first task (fires each provider's onConfig with its
  // primed default), retrying with jittered backoff until the broker answers.
  await primeAndLoad(cache, log, signal);
  if (signal.aborted) return;

  // Background config-refresh (the failsafe behind the broker's live push) — spans the whole
  // reconnect loop, bound to signal.
  void cache.run(signal, (err) => log.warn("converge: config refresh failed (keeping last-known)", { err: String(err) }));

  // Reconnect-with-backoff until signal aborts — the SDK owns this so the dev doesn't. Each
  // session() is ONE stream: it returns when the stream drops (reconnect) or, on abort,
  // after draining in-flight work (then the loop exits).
  let reconnectMs = 1000;
  while (!signal.aborted) {
    const start = Date.now();
    try {
      await session(client, providers, cache, cfg, log, signal);
    } catch (err) {
      if (signal.aborted) break;
      log.warn("converge: stream ended; reconnecting", { err: String(err), backoffMs: reconnectMs });
    }
    if (signal.aborted) break;
    // Reset the backoff after a session that stayed up longer than the current backoff — a
    // genuinely healthy stream, not a fast-failing flap.
    if (Date.now() - start > reconnectMs) reconnectMs = 1000;
    // ±25% jitter: a broker crash drops every worker's stream at once; jitter decorrelates
    // the herd, the escalation uses the raw value.
    await sleep(jitter(reconnectMs), signal);
    if (reconnectMs < cfg.reconnectCeilingMs) reconnectMs *= 2;
  }
}

// session runs ONE WorkStream connection: subscribe (+ initial RS- for boot-degraded
// pairs), re-pull config on connect, then pump server messages — dispatching each task to
// the matching provider (concurrency-bounded, panic-isolated), applying config pushes live,
// and reporting each result UP the same stream. On abort it stops pulling NEW work, drains
// in-flight handlers within the drain grace, flushes their completions, and returns.
async function session(
  client: WorkerClient,
  providers: Provider[],
  cache: ConfigCache,
  cfg: Config,
  log: Logger,
  signal: AbortSignal,
): Promise<void> {
  const byKV = new Map<string, Provider>();
  for (const p of providers) {
    const kv = p.kind();
    byKV.set(`${kv.kind} ${kv.version}`, p);
  }

  // This session's own abort: aborts on the parent signal OR when the stream ends, so the
  // health poller / outbound generator bound to it stop with the session.
  const sessionCtl = new AbortController();
  const onParentAbort = () => sessionCtl.abort();
  signal.addEventListener("abort", onParentAbort, { once: true });

  // The client-send mux: the ONLY frames a worker sends UP are StageCompletes and readiness
  // Interests. A single outbound generator feeds the bidi call; push() enqueues onto it.
  const outbound = new MessageQueue<WorkStreamClientMsg>(sessionCtl.signal);

  // sendReadiness is the health poller's sink — enqueue an Interest, return whether it was
  // accepted (the queue is unbounded here, so it always is unless the session ended). RS-/RS+
  // carry no credit (a worker advertises PRESENCE; the broker never trusts self-reported credit).
  const sendReadiness: SendReadiness = (kind, version, ready) => {
    if (sessionCtl.signal.aborted) return false;
    outbound.push(
      create(WorkStreamClientMsgSchema, {
        body: { case: "interest", value: create(InterestSchema, { kind, kindVersion: version, hasWorker: ready }) },
      }),
    );
    return true;
  };

  // Subscribe FIRST, then an RS- for every currently-unready pair in the SAME ordered burst
  // (closes the subscribe→first-poll window: a boot-degraded kind gets zero tasks).
  outbound.push(
    create(WorkStreamClientMsgSchema, {
      body: {
        case: "subscribe",
        value: create(SubscribeSchema, {
          kinds: providers.map((p) => p.kind().kind),
          kindVersions: providers.map((p) => p.kind().version),
          maxInflight: cfg.maxInflight,
        }),
      },
    }),
  );
  for (const kv of initialUnready(providers)) {
    sendReadiness(kv.kind, kv.version, false);
  }

  // Re-pull the current defaults on connect so a reconnecting worker converges at once
  // (the broker only PUSHES future changes). Bounded + non-fatal — the refresh backs it up.
  try {
    await cache.load(sessionCtl.signal);
  } catch (err) {
    log.warn("converge: config re-pull on connect failed (keeping last-known)", { err: String(err) });
  }

  // Health poller for this session (edge RS+/RS- on ready() changes). Bound to the session
  // signal; a fresh poller per connect re-establishes readiness via the seed above.
  void runHealthPoller(providers, sendReadiness, cfg.healthPollMs, sessionCtl.signal);

  // In-flight bookkeeping for graceful drain: stop pulling NEW work on abort, but let
  // running handlers finish (bounded by the drain grace) and flush their completions.
  const inflight = new Set<Promise<void>>();
  let draining = false;

  const dispatch = (task: StageTask) => {
    const provider = byKV.get(`${task.kind} ${task.kindVersion}`);
    if (!provider) {
      outbound.push(complete(failComplete(task, `converge: no provider for kind ${task.kind}/v${task.kindVersion}`, true)));
      return;
    }
    const job = runTask(provider, task, log)
      .then((sc) => outbound.push(complete(sc)))
      .catch(() => {
        // runTask never rejects (it maps errors to a failure StageComplete); this is a
        // belt-and-suspenders guard so one task can't wedge the drain.
      });
    inflight.add(job);
    void job.finally(() => inflight.delete(job));
  };

  try {
    for await (const msg of client.workStream(outbound.iterate(), { signal: sessionCtl.signal })) {
      switch (msg.body.case) {
        case "task":
          if (draining) break; // abort began: stop accepting NEW work (in-flight drains)
          dispatch(msg.body.value);
          break;
        case "config": {
          const cu = msg.body.value; // live default-providerconfig push (spec + data together)
          cache.apply(cu.kind, cu.kindVersion, cu.config, cu.bundle);
          break;
        }
        default:
          break;
      }
      if (signal.aborted) break;
    }
  } finally {
    // Drain: wait for in-flight handlers (bounded by the drain grace), flush their queued
    // completions, then close the outbound stream. On a clean stream-end (not abort) there
    // is nothing in flight; on abort this is the graceful window.
    draining = true;
    await drainInflight(inflight, cfg.drainGraceMs);
    outbound.close();
    signal.removeEventListener("abort", onParentAbort);
    sessionCtl.abort();
  }
}

// runTask runs ONE task's reaction and returns its StageComplete. It decodes the task, runs
// the matching provider's work(), and encodes the Outcome back. A thrown error becomes a
// failure StageComplete (terminal if it was terminal()); a work() that throws never crashes
// the worker (one kind's bug can't take the process down). Never rejects.
async function runTask(provider: Provider, task: StageTask, log: Logger): Promise<StageComplete> {
  try {
    const out = await provider.work(requestFromTask(task));
    return completeFromOutcome(task, out);
  } catch (err) {
    const terminal = isTerminal(err);
    if (!terminal) {
      log.warn("converge: task failed (transient, will re-dispatch)", { kind: task.kind, reaction: task.reaction, err: String(err) });
    }
    return failComplete(task, String(err), terminal);
  }
}

// drainInflight waits for every in-flight job to settle, bounded by graceMs — after the
// grace, unfinished jobs are abandoned (their results re-run via the broker's abandon/reaper
// at-least-once, the fence prevents double-apply).
async function drainInflight(inflight: Set<Promise<void>>, graceMs: number): Promise<void> {
  if (inflight.size === 0) return;
  const all = Promise.allSettled([...inflight]);
  if (graceMs <= 0) {
    await all;
    return;
  }
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<void>((resolve) => {
    timer = setTimeout(resolve, graceMs);
  });
  await Promise.race([all.then(() => undefined), timeout]);
  clearTimeout(timer!);
}

function complete(sc: StageComplete): WorkStreamClientMsg {
  return create(WorkStreamClientMsgSchema, { body: { case: "complete", value: sc } });
}

// MessageQueue bridges push-style frames (subscribe, completions, readiness) to the
// pull-style async generator the Connect bidi client consumes. The single sender: connect
// streams are not safe for concurrent send, so every outbound frame funnels through here.
class MessageQueue<T> {
  private queue: T[] = [];
  private waiters: ((v: IteratorResult<T>) => void)[] = [];
  private closed = false;

  constructor(private readonly signal: AbortSignal) {
    signal.addEventListener("abort", () => this.close(), { once: true });
  }

  push(v: T): void {
    if (this.closed) return;
    const w = this.waiters.shift();
    if (w) w({ value: v, done: false });
    else this.queue.push(v);
  }

  // close ends the generator once the buffered frames drain, so a final flush of completions
  // still reaches the broker before the stream tears down.
  close(): void {
    if (this.closed) return;
    this.closed = true;
    const w = this.waiters.shift();
    if (w) w({ value: undefined as never, done: true });
  }

  async *iterate(): AsyncGenerator<T> {
    for (;;) {
      const v = this.queue.shift();
      if (v !== undefined) {
        yield v;
        continue;
      }
      if (this.closed) return;
      const next = await new Promise<IteratorResult<T>>((resolve) => {
        this.waiters.push(resolve);
      });
      if (next.done) return;
      yield next.value;
    }
  }
}
