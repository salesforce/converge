# stdterraform demo

## Quickstart

`cd examples/demos/stdterraform && just demo` (needs Docker — it runs LocalStack + the worker
in containers), then open the UI at http://localhost:8080 and watch it converge. `Ctrl+C` tears
it down. The rest of this README explains what you're looking at.

Make **any Terraform module a converge resource** — the shipped
[`stdterraform`](../../../internal/providers/stdterraform) provider (the same one
[`bin/stdworker`](../../../cmd/stdworker) hosts alongside `stdio` + `stdshell`)
points a resource at a module, and a worker **pulls it, injects an S3 state backend,
and runs a real `tofu apply`** ([OpenTofu](https://opentofu.org) — the open-source,
Terraform-compatible CLI). The state backend is **multi-cloud** (AWS S3, Azure Blob,
or GCS, each with its native locking); this demo uses **S3 against
[LocalStack](https://localstack.cloud)** with the module (a `*.tar.gz`) in S3 too. The
module's outputs become the resource's status. (We use OpenTofu, MPL-2.0, rather than
Terraform's BUSL-licensed CLI; the HCL, backend, subcommands, and outputs are
identical — the providerconfig's `binary: "tofu"` selects it.)

This is the pattern for *"make any Terraform module a converge resource"*: you write
no Go per module — ship the HCL, point a `stdterraform` resource at it, and the engine
reconciles it like any other kind (retry on transient failure, value-flow its outputs
into dependents, roll it up into a composite's status, and — new here — **`tofu
destroy` it on delete**).

## What this demo shows

- **A shipped provider that shells out to real tooling.** The
  [`stdterraform`](../../../internal/providers/stdterraform) provider
  ([stdterraform.go](../../../internal/providers/stdterraform/stdterraform.go)) validates the
  spec, wires env, and execs a small embedded
  [`run.sh`](../../../internal/providers/stdterraform/run.sh) that does *fetch* →
  *resolve+install the TF version* → `tofu init` → `tofu apply` → `tofu output -json`
  (or `tofu destroy` on delete). The worker needs **no AWS/IaC Go SDK** — only the
  `tofu`/`terraform`, `tfenv`/`tofuenv`, `aws`, `gcloud`, `az`, `git`, and `curl` CLIs,
  which its Docker image ships.
- **Auto Terraform/OpenTofu version per module.** `run.sh` picks the version through
  **tfenv** (`binary=terraform`) / **tofuenv** (`binary=tofu`) and auto-installs it, so
  the image pins NO Terraform version: `spec.tf_version` wins if set (`1.7.5`, `latest`,
  `latest:^1.7`, `min-required`, `latest-allowed`); else a `.terraform-version` /
  `.opentofu-version` shipped in the bundle; else **`latest-allowed`** — the newest
  version the module's own `required_version` permits. No version is baked into the
  image — the resolved version is installed on first use (flock-guarded + idempotent),
  and cached in the worker's writable version-manager dir so it installs once per
  (worker, version), not per task. Note: tfenv installs HashiCorp Terraform (BUSL-1.1);
  tofuenv installs OpenTofu (MPL-2.0) — this demo uses `binary: tofu`.
- **Destroy-on-delete teardown.** The CRD carries a `deleteRequested → teardown`
  reaction with a finalizer (`converge.dev/stdterraform`). Deleting a resource runs a
  real `tofu destroy`; the core holds the row in `Deleting` until the destroy succeeds,
  then strips the finalizer and removes the row — so the **real cloud infra is gone
  before the resource disappears** (a genuine IaC delete, not just forgetting it).
- **The Terraform Enterprise model.** A converge `stdterraform` resource is, in effect, a
  TFE *workspace*: the user's module ships **no `backend` block**, the platform owns
  state placement + locking centrally (the providerconfig, configured once), and each
  resource gets its **own isolated, auto-locked state**. The **spec** says *which*
  module (`source`) + its input `vars` (the workspace inputs); the **providerconfig**
  says *where state lives* (the backend). Credentials are in **neither** — each backend
  reads its cloud's standard chain (`AWS_*`/`ARM_*`/`GOOGLE_*`, set on the worker).
  (Unlike TFE we point at *your* cloud's native backend, not a converge-hosted store.)
- **Multi-cloud state backend, native locking.** The providerconfig's `backend` field
  selects `s3` | `azurerm` | `gcs`; the worker generates the backend override with that
  cloud's **native state locking** auto-on — S3 `use_lockfile = true` (a `.tflock`
  object; **no DynamoDB**; needs terraform/tofu ≥ 1.10), azurerm blob leases, gcs atomic
  lock object. For any *other* backend (oss, pg, http, custom endpoints, KMS keys, …),
  drop a raw `backend.tf` in the providerconfig `data` bundle — see *State backends* below.
- **State keyed per resource.** Whatever the backend, each resource's state lands under
  `state_prefix<kind>/<resource-UUID>.tfstate` (this demo: `terraform/stdterraform/<UUID>.tfstate`),
  so many resources share one backend location without colliding — each its own state +
  its own lock. Keying by the stable **resource UUID** (not the name) means apply and
  destroy address the *same* state.
- **Outputs → status → value flow.** The created bucket name/ARN/object-key come back
  as `status.outputs`, so a composer could flow one into a dependent resource's spec.
- **Multi-cloud module sources.** `spec.source` accepts a tarball from any of the
  three cloud object stores — `s3://…tar.gz` (AWS, `aws` CLI), `gs://…tar.gz` (GCS,
  `gcloud storage`), `azblob://<account>/<container>/…tar.gz` (Azure Blob, `az`) — plus
  `https://…tar.gz` (`curl`) and `git::https://repo[//subdir][?ref=REF]` (a git clone
  at a ref, optional module subdir). Each object-store scheme uses that cloud's
  **ambient identity** (no creds in the spec): S3 the AWS chain, GCS the active gcloud
  credential / Workload Identity, Azure `--auth-mode login` (Entra ID). This demo uses
  the `s3://` path (against LocalStack); the others are the same provider — the worker
  image ships the CLI for whichever schemes you use (`aws`, `gcloud`, `az`, `git`, `curl`).

| Provider | What it shows |
|---|---|
| [`stdterraform`](../../../internal/providers/stdterraform) | fetch a Terraform module (here an `s3://…tar.gz`) → `tofu apply` against LocalStack → outputs become status; `tofu destroy` on delete. The sample module ([testfixtures/hcl-bundle/](testfixtures/hcl-bundle/)) creates an S3 bucket + object. |

This is a **shipped** provider — it imports only `sdk/*`, never `internal/*` (bar its
own package), exactly what a real provider ships, and its CRD lives *with its code*
under [`internal/providers/stdterraform/`](../../../internal/providers/stdterraform) to
signal "this is a shipped provider; the CRD defines what it is". (Contrast the fake,
demo-only `classic`/`datadriven` providers, whose code **and** CRD live under
`examples/demos/`.)

## Prerequisites

The repo root's `just setup`, plus **Docker** (Compose v2). You do **not** need a
local `tofu` or `aws` CLI — those live in the worker image.

## Run the demo

First time only, from the repo root: `just setup` then `just gen`. Then, from this
directory:

```bash
just demo                 # 3 control + 3 brokers (host) + 1 stdworker + LocalStack (docker)
```

`just demo`:

1. builds the converge binary + the shipped **stdworker** (host + linux/amd64,
   `bin/stdworker.linux-amd64`) and the **stdworker Docker image**
   ([`Dockerfile.stdworker`](../../../Dockerfile.stdworker) — OpenTofu + aws +
   gcloud + az + git + curl + the worker binary);
2. brings up the cluster on the host via `deploy/dev-local.sh` with **no host workers**
   (`NUM_WORKERS=0` — the stdworker runs in Docker, since it needs `tofu`+`aws`);
3. applies the CRD (from the provider package) + the providerconfig **first**, so the
   worker's first task already sees its config;
4. generates the HCL bundle from source (`hcl-bundle/*.tf` → `bundle.tar.gz`) and
   brings up **LocalStack + the stdworker** via
   [`docker-compose.yml`](docker-compose.yml) (the worker is scoped to just the
   `stdterraform` kind with `WORKER_KINDS`). **LocalStack seeds its own S3** via an
   init hook (creates the `tf-state` + `tf-bundles` buckets and uploads the bundle) —
   its healthcheck passes only once that's done, so nothing races;
5. applies a `stdterraform` resource — it queues, the worker claims it, pulls the
   bundle, and runs `tofu apply` against LocalStack.

One worker serves the whole fleet: the 3 host brokers (9090/9091/9092) each own a
disjoint shard tile, the worker connects to one broker, and broker↔broker **relay**
drains the other two tiles to it.

## Watch it converge

In the UI (http://localhost:8080) or with the [`conctl`](../../../cmd/conctl/README.md) CLI —
`just demo` builds it at `bin/conctl` (run it from the repo root as below, or `just install`
to put `conctl` on your `PATH`):

```bash
bin/conctl list resources                           # stdterraform/hello-bucket → Ready
bin/conctl get resource stdterraform/hello-bucket      # status.outputs = { bucket, bucket_arn, object_key }, status.applied = true
aws --endpoint-url http://localhost:4566 s3 ls      # the created bucket + the state object appear in LocalStack
```

Delete it to watch the **destroy-on-delete teardown** — a real `tofu destroy`, then
the row is removed only once the infra is gone:

```bash
bin/conctl delete resource stdterraform/hello-bucket   # runs 'tofu destroy' (teardown), holds Deleting until done, then removes the row
aws --endpoint-url http://localhost:4566 s3 ls      # the bucket is gone
```

`Ctrl+C` tears down the cluster, LocalStack, and the worker.

## Register the kind / add a providerconfig / create a resource

`just demo` does this for you; here is the same flow by hand (a resource is addressed
by `<kind>/<name>`):

```bash
# 1. register the kind — the CRD ships WITH the provider:
conctl apply --type manifest -f ../../../internal/providers/stdterraform/stdterraform.kind.json
# 2. add the providerconfig (where state lives: backend=s3 + a tf-state bucket +
#    the LocalStack endpoint + binary=tofu):
conctl apply --type providerconfig -f testfixtures/providerconfig-stdterraform.json
# 3. create a resource (points at s3://tf-bundles/hello.tar.gz):
conctl apply --type resource -f testfixtures/resource-stdterraform.json
```

The CRD ([stdterraform.kind.json](../../../internal/providers/stdterraform/stdterraform.kind.json)) is a plain
JSON document an **operator** applies by path — it is *not* embedded in the binary and
the worker never reads it at runtime. It defines the `spec`/`status`/`config` schemas
and the two reactions: `work` (`specChange → status`) and `teardown` (`deleteRequested
→ finalizer converge.dev/stdterraform`).

## State backends

The state backend is set **once** in the providerconfig (all resources of the kind
share it; each still gets its own per-resource state + lock). Locking is always on,
using each cloud's **native** mechanism — no separate lock table to provision.

```jsonc
// AWS S3 — native lockfile (a .tflock object; NO DynamoDB; needs terraform/tofu ≥ 1.10)
{ "backend": "s3",      "s3":      { "bucket": "tf-state", "region": "us-east-1" } }

// Azure Blob — locking automatic via blob leases
{ "backend": "azurerm", "azurerm": { "storage_account": "tfacct", "container": "state",
                                     "resource_group": "rg", "use_azuread_auth": true } }

// Google Cloud Storage — locking automatic via an atomic lock object
{ "backend": "gcs",     "gcs":     { "bucket": "tf-state" } }
```

Credentials never appear here — each backend reads its cloud's standard chain the
worker inherits (`AWS_*` / IRSA, `ARM_*` / workload identity, `GOOGLE_*` / GKE
Workload Identity). This demo uses the S3 block above against LocalStack (with an
`endpoint`); drop the `endpoint` + give the worker real creds to target real AWS.

**Any other backend** (oss, pg, http, consul, custom endpoints, KMS-encrypted state,
…): skip the typed `backend` and instead put a raw `backend.tf` in the providerconfig
`data` bundle. It **must** use the placeholder `__CONVERGE_STATE_KEY__` where the
per-resource state key/path goes (the worker substitutes it — the bundle is kind-wide,
so a hardcoded key would make every resource share one state file):

```hcl
# providerconfig `data` (base64 of this file); wins over the typed backend above
terraform {
  backend "oss" {
    bucket = "my-tf-state"
    key    = "__CONVERGE_STATE_KEY__"
  }
}
```

## Files

| File | Role |
|---|---|
| [`internal/providers/stdterraform/stdterraform.go`](../../../internal/providers/stdterraform/stdterraform.go) | the provider (shipped, not demo): `Work` switches on the reaction — `work` (fetch, `tofu apply`, parse outputs → status) + `teardown` (`tofu destroy`, strip finalizer) |
| [`internal/providers/stdterraform/run.sh`](../../../internal/providers/stdterraform/run.sh) | embedded work/teardown step: fetch (s3/gs/azblob/https/git) → resolve+install the TF/OpenTofu version via tfenv/tofuenv → generate the backend override (s3/azurerm/gcs, native locking; or a raw bundle backend.tf) → `tofu init` → `apply`/`destroy` → `output -json` |
| [`internal/providers/stdterraform/types.go`](../../../internal/providers/stdterraform/types.go) | `Spec` (source + vars), `Status` (outputs + applied), `Config` (`backend` s3/azurerm/gcs + per-cloud fields + state_prefix + binary; no creds) |
| [`internal/providers/stdterraform/stdterraform.kind.json`](../../../internal/providers/stdterraform/stdterraform.kind.json) | the CRD (spec/status/config schemas + `work` and `teardown` reactions); ships with the provider |
| [`cmd/stdworker`](../../../cmd/stdworker) | the SHIPPED worker that hosts this provider (no demo-specific worker) — the demo runs its Docker image |
| [`Dockerfile.stdworker`](../../../Dockerfile.stdworker) | the worker image the demo builds + runs: Debian-slim + tfenv + tofuenv (auto-install any TF/OpenTofu version on demand — none baked in) + aws + gcloud + az + git + curl + jq + the prebuilt `bin/stdworker` (all source schemes + the version manager work out of the box) |
| [`testfixtures/providerconfig-stdterraform.json`](testfixtures/providerconfig-stdterraform.json) | where state lives: `backend: s3` + `s3.bucket: tf-state` + `s3.endpoint: http://localstack:4566` + `binary: tofu` (creds via env, not here) |
| [`testfixtures/resource-stdterraform.json`](testfixtures/resource-stdterraform.json) | a `stdterraform` resource pointing `spec.source` at `s3://tf-bundles/hello.tar.gz` |
| [`testfixtures/hcl-bundle/main.tf`](testfixtures/hcl-bundle/main.tf) | the sample module — declares `var.name`, creates an S3 bucket + object, outputs `bucket`/`bucket_arn`/`object_key` |
| [`testfixtures/gen-bundle.sh`](testfixtures/gen-bundle.sh) | packs `hcl-bundle/` into `bundle.tar.gz` (regenerated each `just demo`; git-ignored) |
| [`testfixtures/localstack-init.sh`](testfixtures/localstack-init.sh) | LocalStack `ready.d` init hook: creates the buckets + uploads the bundle, in-container |
| [`docker-compose.yml`](docker-compose.yml) | LocalStack (self-seeding via the init hook) + the stdworker container (built from the repo-root `Dockerfile.stdworker`) |

## How it fits together

```
stdterraform resource (spec.source = s3://tf-bundles/hello.tar.gz, spec.vars = {name})
        │  specChange → work                       deleteRequested → teardown
        ▼                                                   ▼
stdworker (docker: tofu + aws + gcloud + az + git + curl)  ── providerconfig: backend=s3, s3.bucket/prefix, endpoint, binary
        │  run.sh:                                   cloud creds from the default chain (env)
        │    fetch module by scheme (aws s3 cp | gcloud storage cp | az blob download | curl | git clone)  →  tar xz
        │    generate backend override, native locking (state → s3://tf-state/terraform/stdterraform/<resource-UUID>.tfstate, use_lockfile)
        │    tofu init && tofu apply  (or: tofu destroy)   ── all AWS calls → LocalStack (:4566)
        │    tofu output -json                             (apply only)
        ▼
apply:   status.outputs = { bucket, bucket_arn, object_key },  status.applied = true
destroy: infra removed → finalizer stripped → row deleted
```

Everything hits **one LocalStack**: the bundle fetch, the Terraform state backend, and
the resources the module creates. Point the endpoint at real AWS (or drop it) in the
providerconfig and give the worker real credentials (env / mounted `~/.aws` / instance
profile) and the same `stdterraform` kind applies real modules — including from
`https://` tarballs and `git::` repos, not just `s3://`.
