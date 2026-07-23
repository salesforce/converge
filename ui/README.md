# Converge UI

The web console for Converge, built with React 19 + Vite and
**embedded into the Go binary** (its `dist/` bundle is served from `/`).
It reads the HTTP API under `/api/v1`; when a read replica is
configured, GETs are routed there server-side.

## Run

```sh
npm install
npm run dev      # Vite dev server with HMR; proxies /api -> http://localhost:8080
npm run build    # tsc -b && vite build -> dist/ (embedded by the Go build)
npm run lint
```

`just build` compiles this UI and embeds `dist/` into the binary, so the
production console is the same bundle served by Converge itself —
there is no separate frontend deploy.

## Routes

| Route | Page | Purpose |
| --- | --- | --- |
| `/resources/:view` | `ResourcesPage` | The main surface. `:view` is `summary`, `list`, `topology`, or `subgraph`; filters live in the query string. |
| `/resources/:view/r/:resourceId` | `ResourcesPage` | Same, with the resource detail panel open (`ResourcePanel`). |
| `/providerconfigs[/:name]` | `ProviderConfigsPage` | Runtime-editable per-kind config store. |
| `/reactor-bindings` | `ReactorBindingsPage` | Lifecycle-reactor bindings (`when <kind> crosses <transition>, run <reactor>`). |
| `/kinds` | `KindsPage` | Registered kinds + per-kind caps/config. |
| `/cluster` | `ClusterPage` | `kubectl get nodes`-style cluster member table. |

`/` and `/resources` redirect to `/resources/summary`.

## The four resource views

The `ResourcesPage` view switcher picks the right idiom for each shape of
the data — they are deliberately different components:

- **Summary** (`SummaryView`) — readiness totals + a by-kind breakdown
  for the scoped owner(s).
- **List** (in `ResourcesPage`) — token-paginated, virtualized resource
  table with the filter bar (phase / kind / name / labels).
- **Topology** (`TopologyView`) — **how resources are organized by
  label**. This is a containment/grouping hierarchy (e.g. `fi -> fd ->
  team -> resource`), not an arbitrary graph, so it is rendered as a
  virtualized, expand-in-place **tree** — *not* a node-link canvas.
  Each open node lazily fetches its level via `/topology` (and
  `/topology/leaves` at the bottom); the expanded tree is flattened DFS
  and windowed with `@tanstack/react-virtual`, so hundreds of siblings
  and very large leaf sets stay readable and fast. Siblings sort
  failures-first; rows show a per-phase readiness strip and the rollup
  count, with depth guide lines connecting deep rows to their parent.
  Requires exactly one owner.
- **Subgraph** (`SubgraphView`) — the per-resource **dependency DAG**
  (upstream dependencies + downstream dependents, bounded depth). This
  *is* a real graph (cross-links, failure-path closure), so it uses
  `@xyflow/react` (React Flow) with `@dagrejs/dagre` for hierarchical
  layout; the path from the root to any failed neighbor is highlighted.

The split is the point: **containment -> tree, dependencies -> graph.**
React Flow + dagre are used *only* by `SubgraphView`.

## Stack

| Library | Role |
| --- | --- |
| `react` 19 + `react-dom` | UI runtime. |
| `react-router-dom` 7 | Routing + deep-linkable views and resource panels. |
| `@tanstack/react-query` 5 | All server state: fetching, caching, polling (live views poll ~30s), retry/backoff. |
| `@tanstack/react-virtual` 3 | Row virtualization for the resource list and the topology tree. |
| `@xyflow/react` + `@dagrejs/dagre` | Node-link graph + hierarchical layout — `SubgraphView` only. |
| `tailwindcss` 4 (`@tailwindcss/vite`) | Styling. There is no component kit; every component is hand-built with utility classes. |
| `yaml` | Manifest parse/serialize in the resource detail panel. |

Linting uses `eslint-plugin-react-hooks` (incl. its compiler-readiness
rules); the React Compiler itself is not enabled in the build. The API
client and shared types (phases, colors, labels, endpoint wrappers) live
in [src/api.ts](src/api.ts).

## Layout

```
src/
  api.ts            API client + shared types/constants (PHASE_*, endpoint wrappers)
  App.tsx           routes + nav shell
  main.tsx          React Query client + router bootstrap
  pages/            one component per route
  components/       views (Summary/Topology/Subgraph), the detail panel, modals, filter bar
  utils.ts          shared helpers
```
