// serve.ts is the HIGH-LEVEL worker entrypoint. It reads config from the ENVIRONMENT
// (BROKER_ADDR / TLS_* / WORKER_* — the "zero-config" path), registers the given providers
// (scoped by WORKER_KINDS), optionally starts the k8s probe listener (HEALTH_ADDR), dials
// the broker, and runs the WorkStream loop until the AbortSignal fires — draining in-flight
// work. It is the convenience shell over the same run loop the low-level runWorker drives.
//
// The caller owns the signal + the exit decision. A worker main is: build an AbortController
// wired to SIGINT/SIGTERM, await serve(providers, controller.signal). serve reads nothing but
// the environment — there is no options argument (use runWorker for programmatic control).
import { createServer, type Server } from "node:http";
import { loadConfig, newLogger, type Config, type Logger } from "./config.js";
import { runLoop } from "./runner.js";
import { dial, watchTLS } from "./transport.js";
import type { Provider } from "./types.js";

// serve runs the worker's providers until signal aborts, reading ALL config from the
// environment. It BLOCKS (returns a Promise that resolves on clean shutdown). Throws on a
// malformed env var (loud, never a silent fallback), if WORKER_KINDS matched none of the
// providers (nothing to run), or on a fatal run error.
export async function serve(providers: Provider[], signal: AbortSignal): Promise<void> {
  const log = newLogger();
  const cfg = loadConfig();

  // Register the providers this binary serves, scoped by WORKER_KINDS. A provider whose kind
  // isn't in the (non-null) allowlist is dropped without connecting.
  const active = cfg.workerKinds === null ? providers : providers.filter((p) => cfg.workerKinds!.has(p.kind().kind));
  if (active.length === 0) {
    throw new Error(`converge: no providers to run (WORKER_KINDS matched nothing)`);
  }
  if (cfg.brokerAddr === "") {
    throw new Error("converge: BROKER_ADDR is required");
  }

  // Optional k8s liveness/readiness probes (a worker holds no DB, so liveness is a bare 200
  // while up; /readyz flips NotReady once shutdown begins so k8s pulls the pod during drain).
  const probes = cfg.healthAddr !== "" ? serveProbes(cfg.healthAddr, log, signal) : undefined;

  // Dial the broker; rebuild the client on a cert rotation (watchTLS) so mTLS material
  // hot-reloads with no restart. On the cleartext path watchTLS is a no-op.
  let client = dial(cfg);
  watchTLS(cfg, log, () => {
    client = dial(cfg);
  }, signal);

  log.info("worker starting", { providers: active.length, brokerAddr: cfg.brokerAddr });
  try {
    await runLoop(client, active, cfg, log, signal);
  } finally {
    probes?.close();
  }
}

// serveProbes starts the DB-free k8s liveness/readiness listener on addr, bound to signal.
// /livez + /healthz are a bare 200 while the process is up; /readyz flips 503 once shutdown
// begins (signal aborted) so k8s pulls the pod from Service endpoints during the drain.
function serveProbes(addr: string, log: Logger, signal: AbortSignal): Server {
  const { host, port } = splitAddr(addr);
  const server = createServer((req, res) => {
    if (req.url === "/readyz") {
      res.writeHead(signal.aborted ? 503 : 200).end();
      return;
    }
    // /livez, /healthz, anything else: alive while the process is up.
    res.writeHead(200).end();
  });
  server.listen(port, host, () => log.info("worker health listener started", { addr }));
  server.on("error", (err) => log.error("worker health listener", { err: String(err) }));
  signal.addEventListener("abort", () => server.close(), { once: true });
  return server;
}

// splitAddr parses a ":8080" or "host:8080" HEALTH_ADDR into { host, port }. A bare ":port"
// binds all interfaces (host undefined → Node's default).
function splitAddr(addr: string): { host: string | undefined; port: number } {
  const i = addr.lastIndexOf(":");
  const host = i > 0 ? addr.slice(0, i) : undefined;
  const port = Number(addr.slice(i + 1));
  return { host, port };
}

// Config/Logger are re-exported so an embedder can type against them if they build their
// own transport for runWorker; serve itself needs no options.
export type { Config, Logger };
