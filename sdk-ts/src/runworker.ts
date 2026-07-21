// runworker.ts is the LOW-LEVEL worker tier — the advanced entrypoint beside serve().
//
// serve() is the 99% path: it reads config from the environment, DIALS the broker itself
// (h2c/mTLS + cert hot-reload), and owns the whole relationship. runWorker() is for the
// caller who already HOLDS a broker client — a test standing up an in-process broker, an
// embedder with a bespoke transport/auth, a program that dials on its own schedule — and
// wants the SDK to run the WorkStream loop over it. It is the analogue of driving a raw
// client directly instead of the batteries-included serve().
//
// Both tiers share ONE body: serve() builds the env config + dials a Transport, then calls
// the same run loop runWorker() does — there is no second worker implementation.
import {
  DefaultMaxInflight,
  DefaultDrainTimeoutMs,
  DefaultConfigRefreshMs,
  DefaultHealthPollMs,
  DefaultReconnectCeilingMs,
  DefaultTLSReloadIntervalMs,
  newLogger,
  type Config,
  type Logger,
} from "./config.js";
import { runLoop } from "./runner.js";
import type { WorkerClient } from "./transport.js";
import type { Provider } from "./types.js";

// Transport is the broker RPC surface a worker speaks: the bidi WorkStream (pull tasks,
// report results/readiness) + getProviderConfig (prime/refresh the default config). The
// generated WorkerService Connect client satisfies it, so a caller passes that (built over
// any transport + base URL + TLS); a test passes an in-process fake. Deliberately the
// worker-facing service ONLY — a Transport cannot reach the broker↔broker mesh, so a worker
// structurally cannot.
export type Transport = WorkerClient;

// RunOptions are the knobs runWorker needs that serve would otherwise read from the
// environment. All optional — omit any to take the SDK default (the Default* consts).
export interface RunOptions {
  // maxInflight bounds concurrent task execution (also sizes the broker's per-kind fanout
  // buffer headroom for this worker). Omit → DefaultMaxInflight.
  maxInflight?: number;
  // A worker does not report its own identity: the broker derives it from the connection it
  // observes (the mTLS cert's SPIFFE ID, a trusted service-mesh header, or the peer IP).
  // drainGraceMs is how long runWorker lets in-flight handlers finish after the signal
  // aborts before abandoning them. Omit → DefaultDrainTimeoutMs.
  drainGraceMs?: number;
  // logger is the SDK's internal logger (reconnect/config warnings). Omit → env-configured.
  logger?: Logger;
}

// runWorker serves the given providers over an ALREADY-BUILT broker client until signal
// aborts, then drains in-flight work and returns. It owns the WorkStream loop — subscribe,
// pull tasks, dispatch to each provider's work(), report results, prime/refresh the default
// config (firing onConfig), poll ready() (RS-/RS+), and reconnect-with-backoff across stream
// drops — everything serve() does EXCEPT dialing (the caller supplies the client) and
// reading the environment (the caller supplies RunOptions). Requires ≥1 provider and a
// client. Most workers should use serve(); reach for runWorker only to inject the transport
// (tests, embedders, custom auth).
export async function runWorker(
  client: Transport,
  providers: Provider[],
  signal: AbortSignal,
  opts: RunOptions = {},
): Promise<void> {
  const log = opts.logger ?? newLogger();
  const cfg: Config = {
    brokerAddr: "", // unused — the client is already built
    tlsCertFile: "",
    tlsKeyFile: "",
    tlsCAFile: "",
    tlsReloadIntervalMs: DefaultTLSReloadIntervalMs,
    maxInflight: opts.maxInflight ?? DefaultMaxInflight,
    drainGraceMs: opts.drainGraceMs ?? DefaultDrainTimeoutMs,
    configRefreshMs: DefaultConfigRefreshMs,
    healthPollMs: DefaultHealthPollMs,
    reconnectCeilingMs: DefaultReconnectCeilingMs,
    workerKinds: null,
    healthAddr: "",
  };
  await runLoop(client, providers, cfg, log, signal);
}
