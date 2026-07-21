# converge Helm chart

Deploys the converge control plane as plain, dynamically-sharded Deployments:

| Tier | What it runs | AWS identity | Default | Scale for |
|------|--------------|--------------|---------|-----------|
| **control** | sweepers + drainer + reaper + HTTP API | none (default SA) | on | control-plane throughput |
| **broker** | owns work_queue shard tiles, claims + fans work out over Connect; broker↔broker mesh | none (default SA) | on | DB-claim throughput |
| **stdworker** | the SHIPPED generic worker (`/usr/local/bin/stdworker`, hosts the std* providers: stdio, stdshell, stdterraform, stdcel, stdstarlark) — a dumb client, the only tier with provider code | **IRSA SA** | **off** | execution capacity |

This chart deploys **control + broker** by default. The **stdworker** tier is
**off by default** (`stdWorker.enabled=false`): most deployments ship their OWN
provider-worker image (compiled-in Go providers) as a separate worker Deployment
pointed at this release's broker (`<release>-broker:9090`). Enable the shipped
generic stdworker instead with `--set stdWorker.enabled=true`.

All tiers shard the 256 logical shards automatically from the `cluster_members`
registry — scale a tier by changing its `replicaCount` (or enabling its HPA) and
the cluster re-tiles itself. No StatefulSets, no leader election.

## Upgrades

A `helm upgrade` to a new app version rolls the pods, so there is a window where
**new-version and old-version pods run against the same database at once** — the
standard rolling-update overlap. The contract this relies on:

- **A new app version must be backward-compatible with the previous one** at the
  database level, so the still-running old pods keep working while the new ones roll
  in. This is a discipline on the app, not something the chart can enforce.
- The chart sets control + broker to `maxSurge: 1, maxUnavailable: 0` (see
  `<tier>.strategy`) so a quorum stays up throughout and pods roll one at a time. At
  the default 3 replicas Kubernetes rounding already yields this; the explicit value
  keeps it correct at higher/HPA replica counts.
- Rotating an **externally-managed** DB Secret (`database.existingSecret`) does not
  restart pods on its own — run `kubectl rollout restart deploy -l app.kubernetes.io/name=converge`.
  An **inline** `database.url` change *does* roll the pods (a `checksum/config`
  annotation tracks it). A TLS cert Secret rotates in place (hot-reload, no restart).

## Autoscaling, PDBs & container churn

HPA is **off by default**; scaling is by `replicaCount`. Before enabling
`<tier>.autoscaling`, understand what drives container churn (pods repeatedly
created/killed/restarted) here:

- **HPA flapping → re-tiling churn (the converge-specific one).** Every change to
  the live pod set re-tiles the 256 shards across the cluster. So an HPA that scales
  up→down→up on bursty load churns not just pods but the *shard assignment*. Damp it
  with `<tier>.autoscaling.behavior` — a longer `scaleDown.stabilizationWindowSeconds`
  (e.g. 600s) so a transient dip can't scale you down and re-tile. The chart leaves
  `behavior: {}` (k8s defaults: 300s scale-down, immediate scale-up); set it when you
  enable HPA. HPA utilization is `usage / request`, so the meaningful CPU target
  depends on `resources.requests.cpu` being a real steady-state (it is, by default).
- **PDB + HPA deadlock.** Control/broker use `pdb.minAvailable: 2` (correct quorum at
  3 replicas). If HPA lets a tier drop **below 3**, `minAvailable: 2` blocks *all*
  voluntary eviction at 2 pods → node drains stall and pods get force-killed after
  grace (churn). Fix: keep `minReplicas >= 3`, or set that tier's
  `pdb.maxUnavailable: 1` (one eviction at any replica count, never deadlocks).
- **OOMKills.** Memory `limit` defaults to `4Gi` (request `1Gi`/`512Mi`). If real
  usage exceeds the limit the container is OOMKilled and restarts — **profile your
  load and tune `resources`**; the shipped values are conservative starting points.
- **Startup vs liveness.** A `startupProbe` fronts liveness so a slow boot can't trip
  liveness and CrashLoop; liveness itself is DB-free, so a DB blip never restarts a pod.
- **Rollouts.** Control/broker use `maxSurge: 1, maxUnavailable: 0` so an upgrade
  keeps quorum and rolls one pod at a time (minimal churn) — see Upgrades above.

## Install

```sh
# Minimum: control + broker (the default tiers), with a writer DSN. No worker
# tier — deploy your provider workers separately, pointed at <release>-broker:9090.
helm install converge deploy/helm/converge \
  --namespace converge --create-namespace \
  --set image.repository=<your-registry>/converge \
  --set image.tag=<tag> \
  --set database.url='postgres://user:pass@writer:5432/orchestrator?sslmode=require'
```

Prod-shaped install (managed DB secret + read replica), opting the generic
**stdworker** in with IRSA + autoscaling:

```sh
helm install converge deploy/helm/converge -n converge --create-namespace \
  --set image.repository=<registry>/converge --set image.tag=<tag> \
  --set database.existingSecret.name=converge-db --set database.existingSecret.key=dsn \
  --set database.readExistingSecret.name=converge-db --set database.readExistingSecret.key=reader-dsn \
  --set database.iamAuth=true \
  --set stdWorker.enabled=true \
  --set serviceAccount.name=converge-stdworker-irsa \
  --set stdWorker.autoscaling.enabled=true --set stdWorker.autoscaling.maxReplicas=50
```

Most deployments leave `stdWorker` off and run their own provider-worker image
as a separate worker Deployment, pointed at this release's broker
(`<release>-broker:9090`).

### Local full-stack (Docker Desktop k8s, all three tiers)

A self-contained **dev** install — 3 control · 3 broker · 10 stdworker + a throwaway
in-cluster Postgres — using **locally-built images** (no registry). Build both images
into the local Docker store first (Docker Desktop k8s shares it), then
`image.pullPolicy=Never` makes k8s use them without pulling:

```sh
# From the repo root — build the two images into the local Docker store:
just docker image=converge:latest                                  # lean control/broker image
just docker-stdworker stdworker_image=converge-stdworker:latest    # worker image (tofu + cloud CLIs)

# Fresh namespace:
helm uninstall cv -n cv-test 2>/dev/null; kubectl delete ns cv-test 2>/dev/null
kubectl create namespace cv-test

# Throwaway in-cluster Postgres (NOT the tuned/HA setup — dev only):
kubectl -n cv-test create deployment pg --image=postgres:16-alpine
kubectl -n cv-test set env deployment/pg \
  POSTGRES_USER=test POSTGRES_PASSWORD=test POSTGRES_DB=converge
kubectl -n cv-test expose deployment/pg --port=5432
kubectl -n cv-test rollout status deployment/pg --timeout=120s

helm install cv deploy/helm/converge \
  --namespace cv-test \
  --set database.url='postgres://test:test@pg:5432/converge?sslmode=disable' \
  --set image.repository=converge \
  --set image.tag=latest \
  --set image.pullPolicy=Never \
  --set control.replicaCount=3 \
  --set broker.replicaCount=3 \
  --set stdWorker.enabled=true \
  --set stdWorker.image.repository=converge-stdworker \
  --set stdWorker.image.tag=latest \
  --set stdWorker.replicaCount=10 \
  --set control.pdb.enabled=false \
  --set broker.pdb.enabled=false \
  --set stdWorker.pdb.enabled=false
```

By default (`stdWorker.kinds=""`) the worker advertises **every** provider the image
ships — `stdio`, `stdshell`, `stdterraform`, `stdcel`, `stdstarlark`. Add
`--set stdWorker.kinds=stdterraform,stdshell` only to scope a pool to a subset.

Watch it converge, then reach the API:

```sh
kubectl -n cv-test get pods -w   # pg 1/1; 3× control 1/1; 3× broker 1/1; 10× stdworker → 1/1
kubectl -n cv-test port-forward svc/cv-converge 8080:8080 &
curl -fsS http://localhost:8080/livez && echo " API OK"
```

Tear down: `helm uninstall cv -n cv-test && kubectl delete namespace cv-test`.

> **`image.pullPolicy=Never`** is what makes k8s use the locally-built images; skip the
> `just docker …` builds and the pods fail `ErrImageNeverPull` (the chart's default
> `image.repository` is a `your-registry/…` placeholder, never present locally). This
> recipe is for local testing only — for real deployments use `database.existingSecret`
> and a managed Postgres (see [Database](#database)), not an inline DSN + in-cluster PG.

## Database

Provide the **writer** DSN one of three ways (control + broker need it; workers don't):

- `database.url` — inline; the chart writes a `Secret`.
- `database.existingSecret.{name,key}` — reference a Secret you manage (recommended).
- `database.iamAuth=true` — Aurora IAM auth. **Still requires** `url`/`existingSecret`
  (host/db/user/sslmode); only the password is replaced each connection by a minted
  IAM token. Needs an AWS identity on the pods and the DB user `GRANT`ed `rds_iam`.

Optional read replica for the API's UI GETs: `database.readUrl` or
`database.readExistingSecret`.

## Cloud portability & workload identity

This chart runs on **any conformant Kubernetes** — EKS, GKE, AKS, or self-managed.
It provisions no cloud-specific objects; the only cloud touchpoint is the
stdworker's *identity*, and the chart stays agnostic by **not** creating,
annotating, or labeling the ServiceAccount — you set that up out of band and point
the worker at it by name (`--set serviceAccount.name=<sa>`). Leave it empty and the
worker runs as the namespace `default` SA (no cloud identity — fine if it calls no
cloud).

Workload identity is configured **out of band**, and the mechanism differs per
cloud (the chart doesn't care which you use):

| Cloud | What you do out of band | Chart side |
|-------|-------------------------|------------|
| **EKS (IRSA)** | Annotate the SA `eks.amazonaws.com/role-arn: arn:…` + an IAM role trusting the cluster OIDC provider for this `ns:sa`. **The annotation is required — EKS injects the token automatically only once it sees it.** | `serviceAccount.name=<sa>` |
| **EKS (Pod Identity)** | Create a `PodIdentityAssociation` (eksctl / EKS API) — **no SA annotation.** | `serviceAccount.name=<sa>` |
| **GKE (Workload Identity)** | Annotate the SA `iam.gke.io/gcp-service-account: <gsa>@…` + an `iam.workloadIdentityUser` binding. | `serviceAccount.name=<sa>` |
| **AKS (Workload Identity)** | Annotate the SA `azure.workload.identity/client-id: <id>` + a federated credential, **and** add the pod label `azure.workload.identity/use: "true"` | `serviceAccount.name=<sa>` + `--set stdWorker.podLabels.azure\.workload\.identity/use=true` |

So on EKS you *do* still annotate the SA (classic IRSA) — EKS only auto-injects the
token/env *after* seeing that annotation; it isn't automatic without it. Pod Identity
is the newer EKS mode that drops the annotation.

(`serviceAccount.name` is **stdworker-only** — control + broker always use the
namespace `default` SA. If those tiers need an identity for Aurora IAM auth,
annotate the `default` SA out of band. A separately-deployed provider worker
carries its own ServiceAccount.)

**Not portable across clouds** (all opt-in / harmless where unsupported): Aurora
`database.iamAuth` is AWS-only (use a password DSN or the cloud's own DB auth
elsewhere); the worker's `karpenter.sh/do-not-disrupt` /
`cluster-autoscaler.kubernetes.io/safe-to-evict` annotations are AWS-ecosystem hints
that are silently ignored by other clouds' autoscalers (disable with
`stdWorker.doNotDisrupt=false` if you prefer clean manifests).

## TLS / mTLS / SPIFFE

One switch (`tls.enabled=true`) turns on server-side TLS for **both** the
control-plane HTTP API (`:8080`) and the broker Connect server (`:9090`). Like the
ServiceAccount, the chart does **not** hold key material — the cert `Secret` is
provisioned **out of band** (cert-manager `Certificate`, a SPIRE-issued keypair,
or a manual `Secret`). The chart mounts it read-only and points the app's `TLS_*`
env at it, and the app **hot-reloads** the files (so a cert rotation needs no
restart). The Secret carries the standard `kubernetes.io/tls` keys:

| Key in the Secret | Used as | Purpose |
|-------------------|---------|---------|
| `tls.crt` | `TLS_CERT_FILE` | server certificate |
| `tls.key` | `TLS_KEY_FILE` | server private key |
| `ca.crt` | `TLS_CLIENT_CA_FILE` | client-CA bundle for **mTLS** (optional; `tls.clientCAKey`) |

```sh
# One-way TLS (server certs only):
helm install converge deploy/helm/converge -n converge \
  --set database.url=... \
  --set tls.enabled=true --set tls.secretName=converge-tls --set tls.clientCAKey=""

# mTLS + per-audience SPIFFE authz (require + verify client certs, then allowlist
# each listener's SPIFFE IDs separately):
helm install converge deploy/helm/converge -n converge \
  --set database.url=... \
  --set tls.enabled=true --set tls.secretName=converge-tls \
  --set 'tls.apiSpiffeIDs[0]=spiffe://example.org/ns/converge/sa/conctl' \
  --set 'tls.workerSpiffeIDs[0]=spiffe://example.org/ns/converge/sa/worker' \
  --set 'tls.meshSpiffeIDs[0]=spiffe://example.org/ns/converge/sa/broker'
```

- **mTLS** — present `ca.crt` (`tls.clientCAKey`, default `ca.crt`) and the servers
  **require + verify** a client cert. Set `tls.clientCAKey=""` for one-way TLS.
- **SPIFFE — per audience.** After mTLS chain verification the server additionally
  requires the client cert's URI-SAN SPIFFE ID to be in the allowlist for the
  **listener it reached**, so a worker cert can't reach the API and a peer-broker
  cert can't pull work: `tls.apiSpiffeIDs` (`API_AUTHZ_SPIFFE_IDS` — conctl/UI/CI on
  the control API), `tls.workerSpiffeIDs` (`WORKER_AUTHZ_SPIFFE_IDS` — the workers
  that pull work), `tls.meshSpiffeIDs` (`MESH_AUTHZ_SPIFFE_IDS` — peer brokers on the
  internal mesh). The broker's worker + mesh services share one listener; its
  handshake admits the union of the two and a per-service gate narrows each RPC.
  Enforcement only — SVIDs are still issued out of band (SPIRE / cert-manager). A
  non-empty list **requires** mTLS (the chart refuses to render otherwise). See
  [docs/tls.md](../../../docs/tls.md) to generate certs with the right SPIFFE URI SAN.
- **Health probes** — with TLS on, control pods run a separate **plaintext** probe
  listener on `:8081` (the kubelet can't complete an mTLS handshake against the API
  port), while the API stays TLS/mTLS on `:8080`. Broker probes are already on
  `:8081`. Handled automatically; nothing to configure.
- **Workers** — the built-in `stdworker` **auto-follows**: it dials the broker
  over `https://` with the same keypair, no extra flags. A separately-deployed
  provider worker must be pointed at the broker over `https://` and given the same
  client keypair + CA; the install NOTES print the exact env it needs.

## Common overrides

| Key | Default | Notes |
|-----|---------|-------|
| `stdWorker.enabled` | **false** | opt the shipped generic stdworker tier in |
| `control.replicaCount` / `broker.replicaCount` / `stdWorker.replicaCount` | 3 / 3 / 1 | or enable `<tier>.autoscaling` |
| `<tier>.autoscaling.enabled` | false | HPA on CPU/mem; ignores replicaCount |
| `<tier>.pdb.*` | quorum-preserving | control/broker minAvailable 2; stdworker maxUnavailable 10% |
| `stdWorker.maxParallel` | 10 | per-pod, per-kind in-flight cap |
| `stdWorker.kinds` | "" | scope the pool to a subset of kinds; empty = every kind |
| `stdWorker.command` | `[/usr/local/bin/stdworker]` | point at your own worker binary |
| `stdWorker.image.{repository,tag}` | "" | separate image for a custom-provider worker |
| `stdWorker.doNotDisrupt` | true | block voluntary node-autoscaler eviction mid-task |
| `tls.enabled` | false | server-side TLS for the control API + broker Connect |
| `tls.secretName` | "" | out-of-band Secret with `tls.crt`/`tls.key`/`ca.crt` (required when enabled) |
| `tls.clientCAKey` | `ca.crt` | Secret key for the client-CA bundle → mTLS; `""` = one-way TLS |
| `tls.apiSpiffeIDs` | `[]` | SPIFFE-ID allowlist for the API listener (conctl/UI/CI); requires mTLS |
| `tls.workerSpiffeIDs` | `[]` | SPIFFE-ID allowlist for the broker WorkerService (workers); requires mTLS |
| `tls.meshSpiffeIDs` | `[]` | SPIFFE-ID allowlist for the broker MeshService (peer brokers); requires mTLS |
| `tls.reloadInterval` | 3m | cert hot-reload cadence |
| `stdWorker.tls.secretName` | "" | worker-scoped client cert Secret (its own SVID); empty → reuse the server Secret (demo only) |
| `metrics.enabled` | false | OTel metrics on (sets `OTEL_METRICS_ENABLED`) + scrape annotations; served plaintext on `:8081` |
| `metrics.serviceMonitor.enabled` | false | render a prometheus-operator `ServiceMonitor` |
| `networkPolicy.enabled` | false | per-tier NetworkPolicies scoping ingress to real ports |
| `networkPolicy.egress.restrict` | false | also cap egress to DNS + the DB (`egress.dbPort`) |
| `<tier>.strategy` | `maxSurge 1 / maxUnavailable 0` (control, broker) | Deployment rollout strategy |
| `<tier>.topologySpreadConstraints` | hostname, ScheduleAnyway | spread replicas across nodes |
| `<tier>.autoscaling.behavior` | `{}` | HPA scale up/down behavior passthrough |
| `<tier>.probes.{startup,liveness,readiness}` | tuned per tier | probe thresholds/timeouts |
| `image.digest` | "" | pin the image immutably by `repo@sha256:…` (tag ignored) |
| `priorityClassName` | "" | reference an existing PriorityClass for all tiers |
| `revisionHistoryLimit` | 3 | old ReplicaSets kept per tier |
| `commonLabels` / `commonAnnotations` | `{}` | stamped on every object |
| `extraObjects` | `[]` | extra manifests rendered with the release (`tpl`-evaluated) |
| `ingress.enabled` | false | expose the HTTP API |
| `database.iamAuth` | false | Aurora IAM auth (still needs a DSN) |

Per-tier `resources`, `nodeSelector`, `tolerations`, `affinity`, `extraEnv`,
`podLabels`/`podAnnotations`, and `probes` are all overridable; see `values.yaml`.
Values are validated at install/lint time against `values.schema.json`.

## Observability (metrics)

`metrics.enabled=true` turns on the app's OpenTelemetry metrics (sets
`OTEL_METRICS_ENABLED` on control + broker) and annotates the pods for a
scrape-annotation Prometheus; `metrics.serviceMonitor.enabled=true` additionally
renders a prometheus-operator `ServiceMonitor`. The control + broker tiers export
(the OTel Prometheus exporter appends `_total` to every counter — names below are
as they appear in Prometheus):

- `converge_up`, `converge_build_info{version,role}` (gauges)
- `converge_db_pool_connections{state}` (gauge), `converge_db_pool_acquires_total`,
  `converge_db_pool_empty_acquires_total` (pgx pool)
- **control:** `converge_control_sweeper_rows_total{sweep}` (reaper/orphan/specgc/membergc/resync)
- **broker gauges:** `converge_broker_inflight_tasks`, `converge_broker_connected_workers`
- **broker counters:** `converge_broker_mesh_events_total{event}`,
  `converge_broker_dispatch_claimed_total{kind}`,
  `converge_broker_dispatch_completed_total{outcome}`

**On `dispatch_claimed` vs `dispatch_completed`:** `claimed` counts stage tasks
*dispatched to a worker* and `completed` counts *worker-reported resolutions*, so
`claimed ≥ completed` is normal under churn — a task whose worker stream drops (a
rolling deploy / HPA scale-down) or whose broker peer departs is re-dispatched
(re-incrementing `claimed`), and the broker's synthetic-transient recovery for those
two paths resolves the parked stage without a worker report, so it is *not* counted
in `completed`. Treat `completed{outcome=…}` as genuine worker outcomes, not as a
`1:1` accounting of every `claimed`. (A counter with no increments yet emits no
series — expect `completed{outcome="terminal"}` to be absent until a task actually
fails terminally.)

### Why /metrics is plaintext (not behind TLS)

Metrics are served on the **plaintext health port** (`:8081`), never on the (m)TLS
API/Connect ports — the same reason the health probes live there: a Prometheus scraper
(like the kubelet) can't present a client cert, so metrics behind mTLS would be
unscrapeable without issuing every scraper a cert. This is the mainstream approach —
etcd (`--listen-metrics-urls`), CoreDNS, NATS, and most controller-runtime operators
all expose `/metrics` on a dedicated plaintext port and rely on the **network layer**
for isolation. To lock it down, restrict `:8081` ingress with the chart's
`networkPolicy` (to your monitoring namespace) or front it with a `kube-rbac-proxy`
sidecar — *not* by putting TLS on the metrics handler. (controller-runtime's newer
"secure metrics" default takes the sidecar/authn route, not app-level mTLS.)

## Validate

```sh
# Lint (enforces values.schema.json) + render.
helm lint deploy/helm/converge --set database.url=postgres://u:p@h:5432/db
helm template converge deploy/helm/converge --set database.url=postgres://u:p@h:5432/db | less

# Rendered-template unit tests (probe wiring, TLS, PSS, guards):
helm unittest deploy/helm/converge          # needs the helm-unittest plugin
```

CI (`.github/workflows/helm-chart.yaml`) runs `ct lint` across `ci/*-values.yaml`
plus `helm unittest` on every change under `deploy/helm/`.

## After install

Apply kind manifests (CRDs) and providerconfigs over the API before submitting
resources — e.g. via
`conctl apply --type manifest -f <crd>.json --server http://<api>:8080`.
