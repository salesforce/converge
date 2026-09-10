# GitOps demo

## Quickstart

`cd examples/demos/gitops && just demo`, then open the UI at http://localhost:8080 and
watch it converge. `Ctrl+C` tears it down. The rest of this README explains what you're
looking at.

Reconcile a whole **directory of manifests** with one command —
[`conctl sync`](../../../cmd/conctl/README.md), the Argo-CD / Flux-style CD loop, but for
converge resources instead of Kubernetes objects. Point conctl at a manifest tree and it
recursively **applies every manifest**; with `--prune` it also **deletes what you removed
from the tree**. Editing, adding, or deleting a file and re-running `conctl sync` *is* the
deploy — the tree is the desired state, converge reconciles the world to match.

This is the piece kro/Crossplane leave to a separate tool: they watch a store and
reconcile; a GitOps tool (Argo CD, Flux) syncs Git into that store. `conctl sync` is that
sync tool for converge — you check a repo out and run `conctl sync <dir>` (on an interval,
a webhook, or in CI).

## How ownership + prune work

The danger in any "delete what's gone" tool is deleting something it doesn't own. conctl
solves it exactly like Argo CD: **ownership tracking**.

- Each managed directory carries a **`converge.yaml`** naming an **app scope**:

  ```yaml
  # converge.yaml
  app: web
  ```

- On apply, conctl **stamps every resource** in that scope with the reserved label
  `converge.sh/app=<app>` (you never write it by hand — conctl owns the key).
- **Prune** = list the resources conctl owns for that app (by the label) minus the ones the
  tree currently declares → delete the difference. It only ever touches resources it
  stamped, never hand-applied ones. Deletes route through the normal teardown path
  (finalizers run), so a resource backing real infrastructure tears down cleanly.

The **nearest-ancestor `converge.yaml` wins**, so a monorepo with several app dirs
self-scopes: this demo's tree has two scopes, `web/` and `data/`, pruned independently —
removing a file under `web/` never deletes a `data` resource.

**No `converge.yaml`?** A `type: resource` document with no governing scope is still
**applied**, but it **cannot be pruned** (nothing owns it) — conctl warns and moves on.
CRDs, provider configs, and reactor bindings are never app-scoped: they're applied, not
pruned.

## The tree

```
manifests/
  stdshell.kind.json              # "type":"manifest"       — the CRD (cluster-wide, never scoped/pruned)
  providerconfig-stdshell.json    # "type":"providerconfig" — shared config (never scoped/pruned)
  web/
    converge.yaml                 # app: web   (the ONLY filename convention)
    resource-hello.json           # "type":"resource" → stdshell/web-hello
    resource-clock.json           # "type":"resource" → stdshell/web-clock
  data/
    converge.yaml                 # app: data
    resource-backup.json          # "type":"resource" → stdshell/data-backup
```

**There is no filename convention** — the file names above are just for humans. `conctl
sync` reads every `.json`/`.yaml`/`.yml` file under the tree (except `converge.yaml`) and
**requires each to declare a top-level `type`** — the same self-describing envelope
`conctl apply` reads, and an optional (accepted, validated) field on the control-plane API
bodies too. Accepted values:

| `type` | object |
|--------|--------|
| `manifest` | a kind CRD |
| `providerconfig` | a provider config |
| `reactorbinding` | a reactor binding |
| `resource` | a resource — the **only** app-scoped, prunable type |

A file with no `type` is an error (nothing to route it by). Manifests apply in dependency
order regardless of file order: `manifest` → `providerconfig` → `reactorbinding` →
`resource`. Only `resource` documents are app-scoped and prunable.

The resources are [`stdshell`](../../../internal/providers/stdshell) kinds — a small inline
bash script each — hosted by the shipped `bin/stdworker`. This demo ships no worker or
tooling of its own; it exists to demonstrate `conctl sync`, not a new provider.

## What `just demo` does

1. Brings up a 3-control + 3-broker cluster + stdworkers (via the shared launcher).
2. Runs **one command** — `conctl sync manifests/ --prune` — which:
   - applies the CRD + providerconfig (cluster-wide),
   - applies every `type: resource` document, stamping `converge.sh/app=web` / `=data` from
     the nearest `converge.yaml`,
   - (prune has nothing to remove on a first run).

Then it prints the follow-up commands to exercise the GitOps loop.

## Try the loop

With the cluster up (the commands `just demo` prints), from this directory:

```bash
# what each app owns
conctl list resources --all -l converge.sh/app:web    # web-hello, web-clock
conctl list resources --all -l converge.sh/app:data   # data-backup

# PRUNE — remove a manifest, re-sync, watch it get deleted (web only; data untouched):
rm manifests/web/resource-clock.json
conctl sync manifests --prune                          # web-clock → Deleting

# ADD — drop a new "type":"resource" file under web/ or data/, re-sync → created
# DRY-RUN — preview the plan without changing anything:
conctl sync manifests --prune --dry-run
```

`conctl sync` is idempotent: re-running with no changes reports every resource
`unchanged`. A spec edit re-applies (`configured`, generation bumps). A composer root
synced this way recomposes into a new child DAG on a spec change — the children are
managed by the engine, the root by the sync.

## Files

| File | Role |
|------|------|
| [`justfile`](justfile) | `just demo` (cluster + `conctl sync manifests/ --prune`) and `just build` (delegates to the root build) |
| [`manifests/`](manifests/) | the desired-state tree: CRD + providerconfig + two app scopes (`web/`, `data/`) |
| [`manifests/web/converge.yaml`](manifests/web/converge.yaml) / [`data/converge.yaml`](manifests/data/converge.yaml) | the per-scope app names conctl stamps + prunes by |
