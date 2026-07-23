# TLS, mTLS & SPIFFE authz

Converge has three mTLS listeners, each dialed by a **distinct** client population and
each with its **own** SPIFFE-ID allowlist — so a worker cert can't reach the API, and a
peer-broker cert can't pull work:

| Listener | Port (default) | Who dials it | Allowlist env |
|----------|----------------|--------------|---------------|
| Control API | `:8080` | `conctl`, the UI, CI | `API_AUTHZ_SPIFFE_IDS` |
| Broker WorkerService | `:9090` | the dumb workers that pull work | `WORKER_AUTHZ_SPIFFE_IDS` |
| Broker MeshService | `:9090` (same listener) | peer brokers on the internal mesh | `MESH_AUTHZ_SPIFFE_IDS` |

Each allowlist is a comma-separated list of `spiffe://…` IDs. Enforcement is layered
**on top of** mTLS: the server first requires + chain-verifies the client cert
(`TLS_CLIENT_CA_FILE`), then requires the cert's **URI-SAN SPIFFE ID** to be in the
allowlist for the listener it reached. An allowlist requires mTLS (the app refuses to
boot with an allowlist but no client CA). Empty allowlist = chain trust only for that
listener.

The broker's WorkerService and MeshService share one listener, so its TLS handshake
admits the **union** of `WORKER_AUTHZ_SPIFFE_IDS` + `MESH_AUTHZ_SPIFFE_IDS`, and a
per-service gate then narrows each RPC to its own audience by request path.

### Which env var governs which edge

Two things are configured per connection: **authz** (may this peer connect to this
service?) and **identity** (who is this peer, for the cluster view + attribution). They are
set on the SERVER side of each edge (the pod being dialed):

| Edge (dialer → server) | Server pod | AuthZ allowlist (server-side) | Identity resolution (server-side) |
|------------------------|------------|-------------------------------|-----------------------------------|
| `conctl`/UI/CI → **Control API** | control | `API_AUTHZ_SPIFFE_IDS` | *(no worker identity to observe — clients aren't listed)* |
| worker → **Broker WorkerService** | broker | `WORKER_AUTHZ_SPIFFE_IDS` | `PEER_IDENTITY_SOURCE` |
| peer broker → **Broker MeshService** | broker | `MESH_AUTHZ_SPIFFE_IDS` | `PEER_IDENTITY_SOURCE` |

The cert material (`TLS_CERT_FILE` / `TLS_KEY_FILE` / `TLS_CLIENT_CA_FILE` /
`TLS_RELOAD_INTERVAL`) and `PEER_IDENTITY_SOURCE` are **per-pod, not per-edge**: a broker
pod runs ONE listener carrying both its WorkerService and MeshService, so a single
`PEER_IDENTITY_SOURCE` governs how it observes BOTH the workers and the peer brokers that
dial it. The two allowlists are what differ per service. `RELAY_ADVERTISE_ADDR` is
mesh-only (the address a broker publishes so peers can dial its MeshService).

## What a SPIFFE ID is (and where it must live in the cert)

A SPIFFE ID is a structured URI — `spiffe://<trust-domain>/<path>` — **not** a free-form
string. It must be carried in the certificate's **URI Subject Alternative Name (URI SAN)**.
The `CN`/`O` (subject) fields are **ignored** by the authz check; putting the ID there does
nothing. Converge parses `leaf.URIs`, so:

```
subjectAltName = URI:spiffe://example.org/ns/converge/sa/worker
```

## Generate certs yourself with OpenSSL

For a self-hosted PKI (or local testing) you can hand-roll the certs. The important part is
the **URI SAN** — everything else is a normal CA + leaf setup. Below issues a throwaway CA,
a server cert, and three client certs (one per audience) each with its own SPIFFE ID.

```sh
mkdir -p certs && cd certs

# 1) A self-signed CA (used as both TLS_CLIENT_CA_FILE and the trust root).
openssl req -x509 -newkey rsa:2048 -nodes -days 30 \
  -keyout ca.key -out ca.crt -subj "/CN=converge-dev-ca"

# Helper: issue <name>.crt/<name>.key signed by the CA, with a SPIFFE URI SAN.
#   $1 = file prefix   $2 = SPIFFE ID   $3 = "server" for a serverAuth cert (adds localhost SANs)
issue() {
  name="$1"; spiffe="$2"; kind="${3:-client}"
  ext="subjectAltName=URI:$spiffe"
  if [ "$kind" = "server" ]; then
    ext="subjectAltName=URI:$spiffe,DNS:localhost,IP:127.0.0.1"
    eku="serverAuth,clientAuth"        # the broker also dials peers, so it needs clientAuth too
  else
    eku="clientAuth"
  fi
  openssl req -newkey rsa:2048 -nodes -keyout "$name.key" -out "$name.csr" \
    -subj "/CN=$name"
  openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 30 -out "$name.crt" \
    -extfile <(printf "%s\nextendedKeyUsage=%s\n" "$ext" "$eku")
  rm -f "$name.csr"
}

# 2) Server cert (used by the control API + broker listeners).
issue server spiffe://example.org/ns/converge/sa/server server

# 3) One client cert per audience, each with a DISTINCT SPIFFE ID.
issue conctl spiffe://example.org/ns/converge/sa/conctl
issue worker spiffe://example.org/ns/converge/sa/worker
issue broker spiffe://example.org/ns/converge/sa/broker
```

Confirm the SPIFFE ID landed in the URI SAN (not CN):

```sh
openssl x509 -in worker.crt -noout -text | grep -A1 "Subject Alternative Name"
# → URI:spiffe://example.org/ns/converge/sa/worker
```

## Wire it into the servers

```sh
# Control (API):
ROLE=control LISTEN_ADDR=https://:8080 \
  TLS_CERT_FILE=certs/server.crt TLS_KEY_FILE=certs/server.key \
  TLS_CLIENT_CA_FILE=certs/ca.crt \
  API_AUTHZ_SPIFFE_IDS=spiffe://example.org/ns/converge/sa/conctl \
  ./bin/converge

# Broker (WorkerService + MeshService on one listener):
#   PEER_IDENTITY_SOURCE defaults to mtls (worker + peer-broker identity = their client
#   cert SPIFFE ID); set PEER_IDENTITY_SOURCE=mesh-header only behind an Istio/Linkerd mesh.
ROLE=broker BROKER_ADDR_LISTEN=https://:9090 \
  RELAY_ADVERTISE_ADDR=https://localhost:9090 \
  TLS_CERT_FILE=certs/server.crt TLS_KEY_FILE=certs/server.key \
  TLS_CLIENT_CA_FILE=certs/ca.crt BROKER_CA_FILE=certs/ca.crt \
  WORKER_AUTHZ_SPIFFE_IDS=spiffe://example.org/ns/converge/sa/worker \
  MESH_AUTHZ_SPIFFE_IDS=spiffe://example.org/ns/converge/sa/broker \
  HEALTH_ADDR=:8081 ./bin/converge
```

## Test with curl

`curl` presents a client cert with `--cert`/`--key` and trusts the server with `--cacert`.
Against the API listener, the `conctl` cert (in `API_AUTHZ_SPIFFE_IDS`) is accepted; the
`worker` cert is rejected with **403** even though it chain-verifies — that is the
per-audience split working:

```sh
# Allowed: the conctl identity on the API.
curl --cacert certs/ca.crt --cert certs/conctl.crt --key certs/conctl.key \
  https://localhost:8080/api/v1/version
# → 200, JSON version

# Forbidden: a worker identity may hold a valid cert, but it's not on the API allowlist.
curl -so /dev/null -w '%{http_code}\n' \
  --cacert certs/ca.crt --cert certs/worker.crt --key certs/worker.key \
  https://localhost:8080/api/v1/version
# → 403
```

Rotation is picked up automatically: the servers hot-reload `TLS_CERT_FILE`/`TLS_KEY_FILE`/
`TLS_CLIENT_CA_FILE` on a timer (`TLS_RELOAD_INTERVAL`, default 3m) — no restart. The
allowlists are read once at boot (they're config, not rotated material).

## Worker & peer identity (who the broker thinks you are)

The broker **never trusts a client-reported id.** A worker's `Subscribe` message carries
no identity field — the broker derives the connected client's identity from what it can
**observe** about the connection, and uses it for the connected-worker cluster view and the
"running on `<worker>`" attribution (`work_queue.worker_id`, display-only — never a
fence/lease; the lease is the broker's own `broker_id`).

`PEER_IDENTITY_SOURCE` selects how, and reflects your **topology** — it is a declared mode,
not a fallback chain, because trusting a mesh-injected header is a security decision only the
operator can make:

| `PEER_IDENTITY_SOURCE` | Topology | Verified identity | Fallback |
|------------------------|----------|-------------------|----------|
| `mtls` (default, or empty) | the broker terminates mTLS itself | the client cert's **SPIFFE ID** (URI SAN) | **peer IP** (unverified) |
| `mesh-header` | a service mesh (Istio/Linkerd) terminates mTLS in a sidecar | a **trusted mesh header** — Istio `X-Forwarded-Client-Cert` (the `URI=spiffe://…` field) or Linkerd `l5d-client-id` | **peer IP** (unverified) |

Notes:
- The two verified sources are **mutually exclusive**, not tried in sequence. In `mtls`
  mode a mesh header is **ignored** (a direct client could forge it); in `mesh-header` mode
  the broker's own connection is the sidecar, so the cert isn't the workload's — the header
  is. Set `mesh-header` **only** when a mesh fronts the broker and sets/strips those headers
  (same trust model as `API_RATE_LIMIT_TRUST_PROXY`).
- **A SPIFFE ID is stable across cert rotation** (the SVID's URI SAN outlives its key), so a
  worker reconnecting on a rotated cert keeps the same identity — no flicker, and nothing
  strands (the lease was never keyed to the worker).
- When no verified identity is available (no mTLS, no trusted header), the broker attributes
  by the **peer IP**, marked *unverified* — the cluster view badges it so an operator knows
  the id isn't cryptographically vouched-for. If even the peer IP is unavailable the view
  synthesizes a stable `worker-<n>` label.

This applies to **peer brokers on the mesh** too: a broker dialing a peer is identified by
its observed identity (SPIFFE ID under mTLS, else peer IP), never a self-report.

## Broker↔broker mesh mTLS (multi-broker)

The mesh is a distinct mTLS leg from the worker→broker one, but it runs on the **same
broker listener** and reuses the **same per-pod TLS material** — only the allowlist differs
(`MESH_AUTHZ_SPIFFE_IDS`, not `WORKER_AUTHZ_SPIFFE_IDS`). A broker dials each PEER broker at
its **own advertised address** (`RELAY_ADVERTISE_ADDR` — the pod IP in Kubernetes),
presenting its **server** keypair (which carries the `clientAuth` EKU) and verifying the
peer's server cert against `TLS_CLIENT_CA_FILE`. The peer, in turn:
- **admits** the connection via `MESH_AUTHZ_SPIFFE_IDS` — so the mesh allowlist holds the
  dialing broker's **server**-cert identity, not a client one; and
- **observes** the dialing broker's identity via the SAME `PEER_IDENTITY_SOURCE` the pod
  uses for workers (so under `mtls` it's the peer's server-cert SPIFFE ID; under
  `mesh-header` it's the trusted header). The broker↔broker edge is NOT a separate
  identity knob.

`PEER_IDENTITY_SOURCE=mesh-header` for the mesh has one extra requirement worth calling out:
the service mesh must treat the broker's Connect port as **HTTP/2 / gRPC** (Istio: name the
port `grpc`/`http2` or set `appProtocol: grpc`; Linkerd: don't mark it opaque/skip) so the
sidecar does L7 and injects the identity header. If the mesh treats it as raw TCP (L4) the
bidi streams still flow, but NO identity header is added and the broker falls back to the
peer IP (which, behind a mesh, is the sidecar's — effectively unverified). The Connect
transport itself is mesh-compatible over TLS (ALPN `h2`); do NOT run the mesh over the
plaintext h2c dev path (proxies mis-sniff prior-knowledge h2c as HTTP/1.1 and break bidi).

Because the dial target is the peer's real network address, the server cert's SAN **must
cover the address peers dial** — the pod IP, or a per-pod DNS name. Otherwise TLS hostname
verification fails and the dialer aborts with `remote error: tls: bad certificate`. In
practice cert-manager or SPIRE issues each broker a cert whose SAN includes its pod
IP / per-pod DNS, so this is automatic. A single hand-rolled cert with a fixed
`localhost` SAN works for the **host** dev fleet (every process dials localhost) and a
**single-broker** install, but NOT a multi-broker mesh on Kubernetes — issue per-pod certs
there. The `deploy/gen_mtls_certs.sh` helper is localhost-only for exactly this reason.

## Kubernetes

The Helm chart wires all of this from `tls.*` values — see
[deploy/helm/converge/README.md](../deploy/helm/converge/README.md). Provision the cert
`Secret` out of band (cert-manager / SPIRE), set `tls.enabled=true`, `tls.secretName`,
`tls.clientCAKey`, and the three per-audience lists `tls.apiSpiffeIDs` /
`tls.workerSpiffeIDs` / `tls.meshSpiffeIDs`. SPIRE-issued JWT/X.509-SVIDs already carry the
SPIFFE URI SAN, so they slot straight in.
```
