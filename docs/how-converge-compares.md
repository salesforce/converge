# How Converge compares

Converge is **not** an IaC tool like Terraform, Pulumi, or CloudFormation (which
reconcile only on a triggered `plan`/`apply` run against a state file), nor a
durable-execution engine like Temporal or Restate (which orchestrate code, not
declarative resources).

Only **Crossplane** and **kro** share Converge's whole problem — a generic,
provider-agnostic control plane that composes abstract resources into a
dependency graph with cross-resource value flow **and** child→root status
rollup. Everything else is adjacent or a deliberate contrast:

| System | What it is | Relation | One-line difference vs Converge |
|---|---|---|---|
| [**kro**](https://kro.run) | K8s SIG generic resource orchestrator (a blueprint → composed graph) | **Peer** (closest) | Same composition + value flow + rollup, but on Kubernetes/etcd with a spawned microcontroller per blueprint — not one Postgres binary |
| [**Crossplane**](https://www.crossplane.io/) | CNCF provider-agnostic composition control plane | **Peer** | Cross-resource wiring is imperative composition-function code on an etcd-backed controller swarm; Converge uses first-class declarative edges + a Postgres cascade |
| [**Radius**](https://radapp.io) | Multi-service app control plane with Recipe handlers | Adjacent | Generic + declarative + value flow, but no child→root status rollup; state on the k8s API server |
| **K8s CRDs + Operators** | Reconcile framework (controller-runtime) | **Contrast** | Composition, value flow, and rollup are hand-coded per controller; Converge makes all three first-class engine primitives |
| **Terraform / OpenTofu** | Declarative IaC, `plan`/`apply` CLI | **Contrast** | Reconciles only on a triggered run against a state file; drift is detected, not continuously corrected |
| **Pulumi** | IaC engine (program → state file) | **Contrast** | Client-side on-demand `pulumi up`; continuous reconcile only via an add-on operator; no health rollup |
| **AWS CloudFormation / CDK** | Managed template provisioner | **Contrast** | Acts only on explicit stack ops over an opaque backend; drift is report-only |
| [**Nuage**](https://www.linkedin.com/blog/engineering/infrastructure/journey-of-next-generation-control-plane-for-data-systems) | LinkedIn's provider-extensible data-infra gateway | Adjacent | Imperative contract-first CRUD + workflow DAGs; no declarative desired-state reconcile, value flow, or rollup |
| **Temporal / Restate** | Durable-execution / workflow engines | Different tool | Orchestrate imperative code executions, not declarative resources — a layer a handler could sit *on*, not a peer |

In one line: **Crossplane/kro-style declarative composition — dependency graph,
value flow, and status rollup — but on Postgres at 1M+ scale, as a single
binary, with no Kubernetes, etcd, or controller swarm.**

## Where Converge has the edge over Crossplane / kro

Converge, Crossplane, and kro solve the same problem — compose abstract resources into a
dependency graph, flow values along the edges, roll descendant readiness up into a
composite's status. The difference is the *substrate* and how much it constrains you.
Crossplane and kro are Kubernetes-native: they inherit etcd's write ceiling, the
controller-per-graph runtime, and the requirement that a worker be a Kubernetes controller.
Converge keeps the model and drops the substrate — a single Go binary over Postgres — which
is what unlocks the scale, simplicity, and flexibility below. (For the whole feature surface,
see [Converge at a glance](features.md).)

| Axis | Crossplane / kro | Converge | Why it's an edge |
|---|---|---|---|
| **Scale** | Bounded by etcd (~8 GB, thousands of objects) and one controller per graph | Tuned for **millions** of resources on one Postgres primary; **thousands of reconciliations per second** | The whole graph lives in the database and a reconcile is a single commit, not a controller wakeup — so it scales with the database, not etcd |
| **Simplicity** | Kubernetes + etcd + a controller-manager + CRDs + a provider pod per set | **One binary + a database.** No etcd, Redis, Kafka, or message broker; embeds the UI | A dev runs the whole control plane locally with one command; ops runs a handful of plain, interchangeable pods — no StatefulSet, no leader election |
| **Flexibility (kinds)** | A kind is a CRD **compiled into** a controller/provider you build and deploy | A kind is a **declarative manifest applied as data** — schemas, reactions, and policy — and the core stays agnostic to it | Add or re-tune a kind (schema, concurrency cap, resync interval, grace window) live over the API, with no rebuild and no redeploy of the engine |
| **A worker can be anything** | A worker **is** a Kubernetes controller (Crossplane provider / kro microcontroller), in Go, in-cluster | A worker is just a client that dials in and pulls work over one public contract — in **any language**, in or out of the cluster | Go & TypeScript SDKs, or a shipped worker that makes *any external program* (bash, Python, a binary) a kind — no SDK, no compile, no redeploy |
| **Composition is flexible** | Composition is imperative controller code (Crossplane) or a per-blueprint microcontroller (kro) | Composition is **first-class declarative data**; a composer is just a provider — write it in Go, TypeScript, CEL, Starlark, or an external program | The last four ship *as data* and are edited live; one built-in composer implements kro's resource-graph model (deps + value flows inferred from references) as one such provider |
| **Reactive, not polled** | Controllers reconcile off a work-queue with resync intervals | Work is **pushed**: the moment an upstream syncs, the engine reactively wakes exactly the dependents and roots that need it | Reconcile latency is carried by the push, not a poll cadence; a composite's rollup fires the instant its last child settles |
| **Honest composite status** | Rollup is hand-coded per controller, if present at all | A root rolls its whole subtree up into **one status** (Ready / Degraded / Failed / Reconciling / Orphaned / Quarantined / Deleting), and names the child that degraded it | One value every list, count, and badge keys off — so the engine, API, and UI can never disagree |
| **Lifecycle depth** | Finalizers; ordering and side-effects are per-controller code | Built-in reverse-dependency **cascade delete**, **orphan-grace** pruning, operator **quarantine**, per-kind **concurrency caps**, and **reactors** (durable "after X, do Y" sagas via editable subscriptions) | Fleet-grade lifecycle primitives are engine features, not glue you write per kind |

The rows below are capabilities Converge ships as first-class engine features that
**Crossplane and kro lack** — their gaps, not just a different trade-off:

| Capability they lack | Crossplane / kro | Converge |
|---|---|---|
| **Runs without Kubernetes** | Require a K8s cluster (they *are* K8s controllers) | A single binary on a bare VM, a container, or any cloud — a database is the only dependency |
| **Continuous drift correction** | A controller only re-checks when its spec changes or on a coarse periodic re-list; there is no fleet-wide "keep re-verifying real health" engine | Opt a kind into **resync**: the engine periodically re-observes each live resource's actual health — even with no spec change — and a resource found drifted (unhealthy) is automatically re-reconciled back to its spec. Runs across millions of resources; zero cost for kinds that don't opt in |
| **"After X, do Y" side effects** | No side-effect-on-transition primitive; you write another controller | **Reactor bindings** — an editable subscription that wires a durable side effect (upload status, compensate on failure, enrich) to a resource's lifecycle transition; adding one is an API call, no code |
| **Runtime-editable per-kind config** | Crossplane's `ProviderConfig` is a CR; kro has no equivalent | **Provider configs** are a runtime-editable per-kind document: re-point a bucket or endpoint over the API and every worker picks it up **live**, with an optional per-resource override |
| **Spec history + rollback** | Rely on GitOps/external tooling for revision history | The last N spec revisions are kept per resource; **roll back** to any earlier one over the API — built into the control plane |
| **Editable kind at runtime** | A kind's schema/logic is compiled into a controller; a change is a rebuild + redeploy | The manifest is **declarative data** — edit a schema, add a reaction, re-tune a cap/interval/grace window live over the API; the agnostic core never recompiles |

### Built for platform teams to contribute and own their kinds

The sharpest edge is organizational: Converge is designed for **many teams to contribute
and maintain kinds independently**, on a control plane a central platform team runs. A team
onboards a new kind without touching the core, without a shared rebuild, and without
learning Kubernetes controllers:

- **Contribute a kind as data + a worker, not a controller.** A team authors its kind's
  manifest (schema-typed spec/status/config, its reactions, its policy) and applies it over
  the API — the agnostic core never recompiles. Its handler ships in the team's **own
  worker**, in **the language the team already uses** (Go, TypeScript, or *any* external
  program). No PR to the engine, no shared binary to coordinate.
- **Own your kind end to end, live.** The manifest is editable data: the owning team
  tunes its own schema, concurrency cap, resync interval, and grace window over the API with
  no redeploy — and a **schema-stability gate** blocks a breaking schema change unless the
  owner explicitly accepts a new version, so one team can't silently break resources of its
  kind. Each team owns its kind's policy.
- **Fault isolation between teams' kinds.** A worker hosts only the kinds its author
  registers, and per-kind readiness **sheds a single degraded kind** — when one team's
  downstream breaks, just that kind's work parks while every other team's kinds keep flowing
  on the same fleet. One team's outage is not the platform's outage.
- **A team's worker is its own identity.** Workers authenticate to the control plane with
  **mutual TLS** and dial *in* from anywhere — a team runs its worker in its own
  account/namespace/cloud, and the control plane never trusts a worker's self-reported id.

So a central team runs one control plane, and each platform team ships and maintains its
slice — kinds as data, handlers in their own workers, isolated at runtime — exactly the
federated self-service-platform case, without every team having to become a Kubernetes
controller author.
