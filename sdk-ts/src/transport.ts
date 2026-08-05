// transport.ts builds the broker Connect client from the worker's address + TLS config —
// the plumbing the SDK hides from a provider author: they set BROKER_ADDR (and, for prod,
// TLS_CERT_FILE/TLS_KEY_FILE/BROKER_CA_FILE) and never touch Connect, the h2c handshake, the
// HTTP/2 session, or cert hot-reload.
//
// The bidi WorkStream requires end-to-end HTTP/2, so the transport is always httpVersion
// "2": over a plain http:// baseUrl that is prior-knowledge cleartext h2c (NOT HTTP/1.1,
// which the broker rejects); over https:// it is HTTP/2 with the worker's mTLS material.
import { readFileSync } from "node:fs";
import { createGrpcTransport, compressionGzip } from "@connectrpc/connect-node";
import { createClient, type Client, type Transport as ConnectTransport } from "@connectrpc/connect";
import { WorkerService } from "../gen/converge/worker/v1/worker_pb.js";
import type { Config, Logger } from "./config.js";

// MaxMessageBytes bounds a single Connect message AFTER decompression — the SAME cap the
// broker enforces (pkg/wirelimits.MaxMessageBytes = 512 MiB) so a large-but-legitimate
// spec/subtree isn't rejected while a pathologically large one is bounded. Kept in sync
// with the Go const by hand (both mirror the shared wirelimits value).
export const MaxMessageBytes = 512 * 1024 * 1024;

// WorkerClient is the typed Connect client for the WorkerService — the bidi WorkStream +
// unary GetProviderConfig. It is the concrete shape the run loop + config cache speak; the
// low-level runWorker tier accepts it as its injected `client` (see the Transport alias in
// runworker.ts).
export type WorkerClient = Client<typeof WorkerService>;

// dial builds the WorkerService Connect client from cfg.brokerAddr + the TLS options. It
// reads the mTLS material once here; for cert HOT-RELOAD it re-reads on a timer via watchTLS
// (started by the caller when TLS is set), rebuilding the client so a rotated cert/CA takes
// effect with no restart. gzip compresses outbound messages (a large COMPOSE result gzips to
// a fraction); the MaxBytes caps bound the DECOMPRESSED size on both directions.
export function dial(cfg: Config): WorkerClient {
  const transport = buildTransport(cfg);
  return createClient(WorkerService, transport);
}

// buildTransport builds the underlying Connect gRPC transport. The gRPC transport is ALWAYS
// HTTP/2 (createGrpcTransport needs no httpVersion discriminator — the bidi WorkStream
// requires end-to-end HTTP/2): a cleartext http:// baseUrl is prior-knowledge h2c (no TLS);
// an https:// baseUrl with TLS_CERT_FILE/TLS_KEY_FILE set is mTLS (present the client cert
// via nodeOptions, verify the broker against BROKER_CA_FILE). HTTP/2 multiplexes the one
// long-lived WorkStream and the unary GetProviderConfig calls over a single connection — no
// HTTP/1.1 connection-storm to tune (unlike Go, whose HTTP/1.1 fallback raises
// MaxIdleConnsPerHost).
function buildTransport(cfg: Config): ConnectTransport {
  const nodeOptions = tlsNodeOptions(cfg);
  return createGrpcTransport({
    baseUrl: cfg.brokerAddr,
    // gzip OUTBOUND (compressMinBytes leaves tiny frames uncompressed); the client also
    // advertises Accept-Encoding: gzip by default, so the broker's replies are compressed too.
    sendCompression: compressionGzip,
    acceptCompression: [compressionGzip],
    readMaxBytes: MaxMessageBytes,
    writeMaxBytes: MaxMessageBytes,
    ...(nodeOptions ? { nodeOptions } : {}),
  });
}

// tlsNodeOptions returns the HTTP/2 session TLS material, or undefined for the cleartext
// h2c path. When TLS_CERT_FILE + TLS_KEY_FILE are set it loads the worker's client keypair
// (mTLS) and, if BROKER_CA_FILE is set, the CA to verify the broker; without a CA it trusts
// the system roots (unchanged). The files are read fresh on each call, so a caller that
// rebuilds the transport on a reload tick (watchTLS) picks up a rotated cert/CA.
function tlsNodeOptions(cfg: Config): { ca?: Buffer; cert?: Buffer; key?: Buffer } | undefined {
  if (cfg.tlsCertFile === "" || cfg.tlsKeyFile === "") {
    return undefined; // h2c cleartext (the WorkStream still rides HTTP/2, just unencrypted)
  }
  const opts: { ca?: Buffer; cert?: Buffer; key?: Buffer } = {
    cert: readFileSync(cfg.tlsCertFile),
    key: readFileSync(cfg.tlsKeyFile),
  };
  if (cfg.tlsCAFile !== "") {
    opts.ca = readFileSync(cfg.tlsCAFile);
  }
  return opts;
}

// watchTLS polls the cert/key/CA files every cfg.tlsReloadIntervalMs and, when their
// contents change, invokes rebuild so the caller can swap in a freshly-dialed client. Node's
// http2 session caches its TLS context, so a rotation is applied by rebuilding the transport
// rather than re-reading per handshake; the run loop's next reconnect uses the new client.
// No-op (returns immediately) on the cleartext path. Stops when signal aborts.
export function watchTLS(cfg: Config, log: Logger, rebuild: () => void, signal: AbortSignal): void {
  if (cfg.tlsCertFile === "" || cfg.tlsKeyFile === "") return;
  let last = fingerprint(cfg);
  const timer = setInterval(() => {
    let cur: string;
    try {
      cur = fingerprint(cfg);
    } catch (err) {
      log.warn("converge: worker TLS reload failed; keeping last-good material", { error: String(err) });
      return;
    }
    if (cur !== last) {
      last = cur;
      log.info("converge: worker reloaded TLS material", { cert_file: cfg.tlsCertFile, key_file: cfg.tlsKeyFile, broker_ca: cfg.tlsCAFile });
      rebuild();
    }
  }, cfg.tlsReloadIntervalMs);
  signal.addEventListener("abort", () => clearInterval(timer), { once: true });
}

// fingerprint is the concatenated cert+key+CA bytes' length+head — a cheap change detector
// (a real rotation changes the bytes). Reading the files here surfaces a mid-rotation
// missing file as a throw the caller logs (keeping last-good).
function fingerprint(cfg: Config): string {
  const parts = [readFileSync(cfg.tlsCertFile), readFileSync(cfg.tlsKeyFile)];
  if (cfg.tlsCAFile !== "") parts.push(readFileSync(cfg.tlsCAFile));
  return parts.map((b) => `${b.length}:${b.subarray(0, 32).toString("hex")}`).join("|");
}
