// Package wirelimits holds Connect wire-size limits shared by BOTH ends of the
// WorkerService — the broker handler (internal/broker) and the worker client
// (sdk-go/converge). It lives in its own leaf package (importing nothing) so the
// broker can reference the cap WITHOUT importing the worker client SDK: the
// dependency edge between the two ends stays cut, which keeps the worker +
// providers cleanly separable into their own repo (they import only sdk-go/*).
package wirelimits

// MaxMessageBytes bounds a single Connect message on BOTH ends — AFTER decompression
// (gzip is enabled, so the wire payload is a fraction of this for the highly
// compressible JSON shipped). The bound must hold the largest LEGITIMATE stage
// payload, a COMPOSE RESULT: a 1M-child composer composes to a ~282MB
// StageComplete (desired children + edges). 64 MiB rejected that with
// resource_exhausted and stranded the root, so it is 512 MiB — above the 1M case
// with headroom for a rollup subtree, while still bounding a pathological payload
// (a DoS bound) vs Connect's default 0 = unlimited.
//
// NOTE: a single reaction's result must fit one message. At the 10M-resource
// design target a per-shard compose would exceed this; that needs chunked/
// streamed compose results, tracked separately.
const MaxMessageBytes = 512 << 20 // 512 MiB (decompressed)
