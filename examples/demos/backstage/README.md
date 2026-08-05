# Backstage golden-path demo

Show a **platform-engineer audience** the Backstage half of an Internal Developer Platform
driving Converge as its provisioning backend: a developer opens the Backstage **Software
Catalog**, clicks a **golden-path Template** ("Provision a deployment"), answers a few
knobs, and clicks **Create** — and Converge **composes a whole DAG of cloud infrastructure**
(accounts, VPCs, transit gateways, routes) and **rolls status up to a single root** the
developer watches go `Ready`. The developer never writes a nested BOM, touches `conctl`, or
sees the REST API.

This is the **direct-API-apply** integration: the Backstage Scaffolder runs a custom action
that expands the form knobs into a [`classicbom`](../classic) BOM spec and `POST`s it
straight to the Converge control-plane API — the exact call
[`conctl apply`](../../../cmd/conctl/README.md) makes (`POST /api/v1/resources`). No Git repo,
no CI loop, no `conctl sync` — the template *is* the deploy. (For the decoupled GitOps
variant — a Template that opens a PR into a manifest tree that CI then `conctl sync`s — see
the [`gitops`](../gitops) demo.)

The backend is the **classic composition demo's** fleet: `bin/classic-worker` hosts the
`classicbom` composer plus the `account` / `vpc` / `tgw` / `route` leaf providers and the
`statussink` reactor. This demo ships **no worker of its own** — only the Backstage
artifacts (the Template + the custom action) that drive it.

## Why classicbom (the platform-team story)

`classicbom` is the demo that reads like a real platform: one request fans out into a
**dependency-ordered graph of infrastructure**, wired by value flows, healed on failure, and
**rolled up** so the developer tracks one status instead of dozens. That composition +
rollup is the product a platform team sells — "ask for a deployment, get a deployment" — and
it's exactly what a golden-path Template should front. The developer supplies shape
(**how many functional domains, teams per domain, transit gateway on/off**); the platform
builds and reconciles the rest.

## What you're looking at

```
Developer in Backstage                Backstage Scaffolder                Converge control plane
──────────────────────                ────────────────────                ──────────────────────
Catalog → "Provision a deployment"
  form: name, #domains,        ──▶     template.yaml runs steps:
        teams/domain, TGW                1. fetch:template (skeleton)
  click Create                          2. converge:apply  ──────────────▶  builds classicbom BOM
                                           (custom action)                   from the knobs, then
                                             expands knobs → BOM             POST /api/v1/resources
                                                                          ◀── 201 Created
                                          3. catalog:register                 X-Apply-Result: created
                                        registers a Catalog entity                 │
                                        linking to the Converge root              ▼
                                                                          composer fans the BOM into
                                                                          account→vpc→tgw→route DAG,
                                                                          rolls status up → root Ready
```

The developer's mental model is **"I asked the platform for a deployment and got one."** The
platform engineer's takeaway is **"the golden path is a form over Converge's declarative
composition API — the same API `conctl` and GitOps use, so nothing about the platform is
Backstage-specific."**

## Quickstart — one command, fully automatic

```bash
cd examples/demos/backstage && just demo
```

`just demo` brings up the **entire stack** in Docker with nothing to wire by hand — Postgres,
a Converge control pod + broker + 3 `classic-worker`s, a one-shot seed of the CRDs + reactor
wiring, and a **real Backstage instance** with the `converge:apply` action and the golden-path
template baked in and pre-configured. When it's up:

- **Backstage UI** — http://localhost:3000 → **Create → "Provision a deployment"**. Fill the
  form (deployment name / domains / teams / TGW), click **Create**.
- **Converge UI** — http://localhost:8080 → watch the composed DAG converge to `Ready`.

That's it — you're the customer requesting a deployment, and the platform provisions it. No
manual action-install, config, or template-registration step: the image
([`Dockerfile.backstage`](Dockerfile.backstage)) scaffolds Backstage and injects all of it.

`Ctrl+C` (or `just down`) tears the stack down. `just logs backstage` tails the Backstage
container (its first boot after the image build takes ~1 min).

Check the result from the host too:

```bash
curl -s http://localhost:8080/api/v1/resources/classicbom/<name> | jq '{phase,is_ready}'
```

## The pieces (this directory)

| File | Role |
|------|------|
| [`justfile`](justfile) | `just demo` (build images + bring the whole stack up), `just down`, `just logs`. |
| [`docker-compose.yml`](docker-compose.yml) | the full stack: Postgres → migrate → control + broker + workers → seed → Backstage. |
| [`Dockerfile.backstage`](Dockerfile.backstage) | builds the Backstage image — scaffolds a real Backstage 1.53 app and bakes in the action + template + wiring (node:22, so the native modules compile). |
| [`template.yaml`](template.yaml) | the Backstage **Software Template** — the knob form + the steps that provision the deployment. THE artifact a platform engineer authors. |
| [`converge-apply-action/`](converge-apply-action) | the **Scaffolder custom action** (`converge:apply`) — expands the knobs into a `classicbom` BOM and POSTs it to the Converge API. |
| [`docker/`](docker) | `convergeModule.ts` (registers the action on the backend) + `inject.mjs` (wires the module + config + template location into the scaffolded app at image-build time). |
| [`skeleton/`](skeleton) | the files the Template scaffolds into the new component's repo — a `catalog-info.yaml` for the *provisioned* deployment, linked to its Converge root resource. |
| [`seed.sh`](seed.sh) | one-shot: applies the `classicbom` + `account`/`vpc`/`tgw`/`route` + `statussink` CRDs and the statussink reactor binding via `conctl` (the out-of-band "operator sets up kinds" step). |

## Using it against your OWN Backstage (not the bundled one)

The bundled Backstage exists so the demo is one command. To drive the same golden path from an
existing Backstage instance instead, the artifacts here drop in directly — see
[`converge-apply-action/README.md`](converge-apply-action/README.md) to install the action and
[`app-config.demo.yaml`](app-config.demo.yaml) for the config fragment (`converge.baseUrl` +
the template location). Point `converge.baseUrl` at your Converge control API and register
[`template.yaml`](template.yaml) in your catalog.

## Why direct-apply (and when to prefer GitOps)

Direct-apply is the **shortest, most legible** demo of the seam: one HTTP POST, no external
moving parts. It is the right shape for **self-service** provisioning. For **production infra
with an audit trail**, prefer the GitOps variant: the Template opens a PR into a manifest
repo, review/merge is the gate, and CI runs `conctl sync --prune` (see the [`gitops`](../gitops)
demo). The resource *envelope* is identical between the two — only the delivery changes.

## Contract this demo depends on

The custom action encodes exactly one API call plus the classicbom spec shape — keep both in
sync if the contracts move (there are **no wire compatibility guarantees before the first
stable release**, per the repo's REST discipline):

- `POST /api/v1/resources`
- envelope `{ "type":"resource", "kind":"classicbom", "kind_version":1, "name":<string>,
  "labels":{…}, "spec":<BOM> }`
- BOM `{ "deployment_instance": { "name":<string>, "functional_domains": [ { "name":<string>,
  "aws_transit_gateway": { "enabled":<bool> }, "service_teams": [ { "name":<string> } ] } ] } }`
- `201 Created` (first apply) / `200 OK` (update); applied verb in the `X-Apply-Result` header
  (`created` | `configured` | `unchanged`).

See [`examples/demos/classic`](../classic) for the composer + providers this drives,
[`examples/demos/classic/testfixtures/classicbom.kind.json`](../classic/testfixtures/classicbom.kind.json)
for the authoritative spec_schema, and [`cmd/conctl/README.md`](../../../cmd/conctl/README.md)
for the same apply from the CLI.
