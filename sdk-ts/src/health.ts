// health.ts drives per-kind readiness. A worker process hosts many providers over ONE
// WorkStream; readiness is how it sheds a SINGLE degraded pair without dropping the stream
// (the other kinds keep flowing). A provider's ready() flipping false advertises an Interest
// RS- ("stop sending this pair's work") for just its (kind, version); false→true resumes it (RS+).

import { sleep } from "./backoff.js";
import type { Provider, KindVersion } from "./types.js";

// SendReadiness enqueues a readiness Interest (RS+/RS-) for (kind, version); returns whether
// the frame was DELIVERED (enqueued onto the current stream). A false return (no active
// stream / full buffer) means the flip did NOT reach the broker, so the poller must not
// latch it and re-attempts next tick. Supplied by the run loop (its send mux). Mirrors the
// Go runner.SendReadiness signature the poller depends on.
export type SendReadiness = (kind: string, version: number, ready: boolean) => boolean;

function key(kv: KindVersion): string {
  return `${kv.kind} ${kv.version}`;
}

// initialUnready evaluates every provider's ready() and returns the (kind, version) pairs
// currently NOT ready. The run loop sends an RS- for each in the SAME ordered burst as the
// Subscribe, so a kind whose probe is false at connect is gated BEFORE the broker can
// dispatch it (no subscribe→first-poll window).
export function initialUnready(providers: Provider[]): KindVersion[] {
  const out: KindVersion[] = [];
  for (const p of providers) {
    if (!p.ready()) out.push(p.kind());
  }
  return out;
}

// runHealthPoller polls each provider's ready() every pollMs and drives the broker-facing
// RS+/RS- via sendReadiness. It owns EDGE detection: a probe is remembered as its
// last-DELIVERED bool, and only a CHANGE emits a frame (true→false → RS-, false→true → RS+).
//
// Two subtleties keep it correct across reconnects:
//   - CONNECT-TIME state rides the Subscribe (initialUnready), not this poller. The poller
//     SEEDS its last-seen map from that same state (without re-sending) so it only emits on
//     a subsequent EDGE. Created fresh per connect, so the seed re-establishes each session.
//   - A pair is recorded as delivered ONLY once sendReadiness confirms the enqueue, so a
//     dropped frame stays an edge and re-attempts next tick (the poller never latches a
//     state the broker never learned).
//
// Bound to the connect signal: it stops the moment the session ends. A pair whose provider
// is always ready never emits (ready is the implicit broker default). Returns when signal
// aborts.
export async function runHealthPoller(
  providers: Provider[],
  send: SendReadiness,
  pollMs: number,
  signal: AbortSignal,
): Promise<void> {
  if (providers.length === 0) return; // nothing to poll: every kind is always ready

  // last-observed readiness per pair, recorded ONLY once its state has been DELIVERED to the
  // broker. A pair absent from `last` is treated as "the broker's default, ready", so an
  // undelivered flip stays an edge.
  const last = new Map<string, boolean>();

  const emit = (p: Provider) => {
    const kv = p.kind();
    const k = key(kv);
    const ready = p.ready();
    const prev = last.get(k);
    if (ready && prev === undefined) return; // never unready + untracked → implicit ready; no frame
    if (prev !== undefined && ready === prev) return; // no change from last DELIVERED state
    if (send(kv.kind, kv.version, ready)) last.set(k, ready); // latch only what the broker learned
  };

  // SEED (don't emit) the connect-time state: the run loop already advertised the
  // currently-unready pairs via initialUnready in the Subscribe burst, so record them as
  // delivered here to emit only on a subsequent EDGE (a recovery RS+, or a later degradation).
  for (const p of providers) {
    if (!p.ready()) last.set(key(p.kind()), false);
  }

  while (!signal.aborted) {
    await sleep(pollMs, signal);
    if (signal.aborted) return;
    for (const p of providers) emit(p);
  }
}
