import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import {
  ReactFlow,
  Background,
  Controls,
  MarkerType,
  type Node,
  type Edge,
  type NodeMouseHandler,
  type NodeProps,
  Handle,
  Position,
  ReactFlowProvider,
  useReactFlow,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import dagre from '@dagrejs/dagre'
import {
  api,
  retryReadAfterWrite,
  retryDelayReadAfterWrite,
  type SubgraphNode,
  PHASE_VALUES,
  PHASE_FILL,
  PHASE_LABELS,
} from '../api'

// SubgraphView is the per-resource debug view. From a root resource,
// walk the dep graph upstream (dependencies: things this resource
// depends on) and optionally downstream (dependents: things that
// depend on this resource) with bounded depth. Render root + the
// neighborhood as a hierarchical DAG so the operator can see exactly
// why a resource is in the state it's in.
//
// Defaults: dependency depth 3, dependent depth 1. For a route,
// depth=3 reaches its account and the FD's TGW + tgw-host account —
// the canonical "what's blocking my route?" answer.

// Phase colors (PHASE_FILL) and labels (PHASE_LABELS) come from api.ts;
// they're used inline as svg/border fills where tailwind classes can't
// reach.

interface Props {
  kind: string
  name: string
  onNodeClick: (kind: string, name: string) => void
}

export function SubgraphView({ kind, name, onNodeClick }: Props) {
  return (
    <ReactFlowProvider>
      <Inner kind={kind} name={name} onNodeClick={onNodeClick} />
    </ReactFlowProvider>
  )
}

// Node-count caps the user can pick. The server's hard ceiling is 2000.
const NODE_LIMIT_OPTIONS = [200, 500, 1000, 2000] as const
type NodeLimit = (typeof NODE_LIMIT_OPTIONS)[number]

// refKind splits a "kind/name" node ref on its FIRST '/' (a kind never
// contains '/', so the head is the kind and the tail is the full name).
function splitRef(ref: string): { kind: string; name: string } {
  const i = ref.indexOf('/')
  return { kind: ref.slice(0, i), name: ref.slice(i + 1) }
}

function Inner({ kind, name, onNodeClick }: Props) {
  // The root's "kind/name" ref — matches SubgraphNode.ref, so it's the join
  // key against the node map and the "is this the root?" test.
  const rootRef = `${kind}/${name}`
  // dependencyDepth = "things this resource depends on" (upstream).
  // dependentDepth  = "things that depend on this resource" (downstream).
  // Server still calls these ancestor_depth / descendant_depth; the
  // wire names are graph-theory jargon, the UI labels match the
  // codebase's own dependent/dependency vocabulary (resource_deps).
  const [dependencyDepth, setDependencyDepth] = useState(3)
  const [dependentDepth, setDependentDepth] = useState(1)
  const [nodeLimit, setNodeLimit] = useState<NodeLimit>(200)
  const { fitView } = useReactFlow()
  const didInitialFit = useRef(false)

  const { data, isPending, isError, error } = useQuery({
    queryKey: ['subgraph', rootRef, dependencyDepth, dependentDepth, nodeLimit],
    queryFn: () => api.getSubgraph(kind, name, dependencyDepth, dependentDepth, nodeLimit),
    refetchInterval: 2_000,
    // keepPreviousData (not (prev) => prev): the queryKey includes the root
    // ref, so navigating to another node changes the key — `(prev) => prev`
    // would see the NEW key's empty cache and flash "Loading subgraph…".
    // keepPrevious holds the last successful data across the key change.
    placeholderData: keepPreviousData,
    retry: retryReadAfterWrite,
    retryDelay: retryDelayReadAfterWrite,
  })

  const { positioned, edges } = useMemo(() => {
    if (!data) return { positioned: [], edges: [] }

    // Subgraph nodes are slim ResourceListItems (kind+name); their "kind/name"
    // ref is the join key GraphEdge from/to reference.
    const nodeRef = (n: SubgraphNode) => `${n.kind}/${n.name}`

    // Node map keyed by the "kind/name" ref, for direct edge-join lookups.
    const byRef = new Map<string, SubgraphNode>(data.nodes.map((n) => [nodeRef(n), n]))

    // Compute the set of failed dependencies (upstream). We highlight
    // the path from root → any failed dependency in red so the cause
    // jumps out. "Failed" here is the server-truth phase carried on each
    // subgraph node.
    const dependencyFailedIDs = new Set<string>()
    for (const n of data.nodes) {
      if (n.min_depth < 0 && n.phase === 'Failed') dependencyFailedIDs.add(nodeRef(n))
    }
    const dependentFailedIDs = new Set<string>()
    for (const n of data.nodes) {
      if (n.min_depth > 0 && n.phase === 'Failed') dependentFailedIDs.add(nodeRef(n))
    }

    // Wire React Flow nodes.
    const flowNodes: Node[] = data.nodes.map((n) => ({
      id: nodeRef(n),
      type: 'resource',
      data: {
        node: n,
        isRoot: nodeRef(n) === rootRef,
      },
      position: { x: 0, y: 0 }, // dagre overrides
    }))

    // Direction: in resource_deps, dependent_id depends-on dependency_id.
    // For visual hierarchy we want "things you depend on" ABOVE you, so
    // edges point from dependency (top) → dependent (bottom).
    //
    // failedClosure marks every node on a path between the root and a failed
    // node — BOTH directions — so the UI paints the whole chain red and the
    // operator instantly sees "this VPC's account is broken, that's why my
    // route is stuck" (upstream) AND "this dependent of mine has failed"
    // (downstream). We grow it to a fixed point over the edges:
    //   • upstream  (min_depth < 0): walk dependent → dependency (e.from→e.to)
    //   • downstream (min_depth > 0): walk dependency → dependent (e.to→e.from)
    // Seeding from both sets (not just upstream) is what fixes downstream
    // failure edges rendering gray despite being counted as failed.
    const failedClosure = new Set<string>([...dependencyFailedIDs, ...dependentFailedIDs])
    let changed = true
    while (changed) {
      changed = false
      for (const e of data.edges) {
        const dep = e.to // dependency
        const dent = e.from // dependent
        // Upstream: extend from a failed dependent back to its dependency.
        if (failedClosure.has(dent) && !failedClosure.has(dep)) {
          const node = byRef.get(dep)
          if (node && node.min_depth < 0) {
            failedClosure.add(dep)
            changed = true
          }
        }
        // Downstream: extend from a failed dependency forward to its dependent.
        if (failedClosure.has(dep) && !failedClosure.has(dent)) {
          const node = byRef.get(dent)
          if (node && node.min_depth > 0) {
            failedClosure.add(dent)
            changed = true
          }
        }
      }
    }

    const flowEdges: Edge[] = data.edges.map((e, i) => {
      const dep = e.to
      const dent = e.from
      // On the failure path when both endpoints are in the closure, or one
      // endpoint is the root and the other is in the closure — covers the
      // root→failed-dependency (upstream) and root→failed-dependent
      // (downstream) edges, where the root itself may not be failed.
      const onFailurePath =
        (failedClosure.has(dep) && failedClosure.has(dent)) ||
        (dep === rootRef && failedClosure.has(dent)) ||
        (dent === rootRef && failedClosure.has(dep))
      const dentNode = byRef.get(dent)
      // Animate edges that feed an unready downstream — gives the
      // operator a moving cue that work is still pending here.
      const animated = dentNode ? !dentNode.is_ready : false
      return {
        id: `e-${i}`,
        source: dep,
        target: dent,
        animated,
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: onFailurePath ? '#ef4444' : '#94a3b8',
        },
        style: {
          stroke: onFailurePath ? '#ef4444' : '#94a3b8',
          strokeWidth: onFailurePath ? 2 : 1.25,
        },
      }
    })

    const positioned = layout(flowNodes, flowEdges)
    return { positioned, edges: flowEdges }
  }, [data, rootRef])

  // Re-arm the one-shot fit when the user navigates to a different root.
  // (Mutating the ref in an effect, not during render — only depth changes
  // keep the same root, so the camera/zoom is preserved across those.)
  useEffect(() => {
    didInitialFit.current = false
  }, [rootRef])

  // Camera: fit once when the data lands or when the root changes.
  useEffect(() => {
    if (positioned.length === 0) return
    if (didInitialFit.current) return
    const t = setTimeout(() => {
      fitView({ padding: 0.2, duration: 250, maxZoom: 1 })
      didInitialFit.current = true
    }, 50)
    return () => clearTimeout(t)
  }, [positioned, fitView])

  const handleNodeClick: NodeMouseHandler = useCallback(
    (_, node) => {
      // node.id is the "kind/name" ref — split on the first '/' to navigate.
      const { kind: k, name: n } = splitRef(node.id)
      onNodeClick(k, n)
    },
    [onNodeClick],
  )

  if (isError) {
    return (
      <div className="flex-1 flex items-center justify-center p-6">
        <div className="max-w-lg p-3 bg-red-50 border border-red-200 rounded text-xs text-red-800">
          <div className="font-semibold mb-1">Failed to load subgraph</div>
          <div className="font-mono">{(error as Error)?.message}</div>
        </div>
      </div>
    )
  }
  if (isPending && !data) {
    return (
      <div className="flex-1 flex items-center justify-center text-sm text-slate-500" aria-busy="true">
        Loading subgraph…
      </div>
    )
  }

  const counts = data
    ? {
        total: data.nodes.length,
        dependencies: data.nodes.filter((n) => n.min_depth < 0).length,
        dependents: data.nodes.filter((n) => n.min_depth > 0).length,
        // Count ALL failed nodes by phase — including the root itself
        // (min_depth === 0), which the upstream/downstream sets exclude. The
        // old `dependencyFailedIDs.size + dependentFailedIDs.size` undercounted
        // a failed root.
        failed: data.nodes.filter((n) => n.phase === 'Failed').length,
      }
    : { total: 0, dependencies: 0, dependents: 0, failed: 0 }

  // The server caps the result set at `nodeLimit`. If we hit that
  // exact number, more rows might exist — surface that to the user
  // rather than silently misleading them.
  const limitReached = counts.total >= nodeLimit
  const nextLimit = NODE_LIMIT_OPTIONS.find((L) => L > nodeLimit)

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <DepthBar
        dependencyDepth={dependencyDepth}
        dependentDepth={dependentDepth}
        onDependencyDepth={setDependencyDepth}
        onDependentDepth={setDependentDepth}
        nodeLimit={nodeLimit}
        onNodeLimit={setNodeLimit}
        counts={counts}
        limitReached={limitReached}
      />
      {limitReached && (
        <LimitNotice
          shown={counts.total}
          limit={nodeLimit}
          nextLimit={nextLimit}
          onRaise={() => nextLimit && setNodeLimit(nextLimit)}
          onReduceDepth={() => {
            // Trim the larger depth one notch so the user can step
            // down and see the whole picture without bumping the cap.
            if (dependencyDepth >= dependentDepth) {
              setDependencyDepth(Math.max(1, dependencyDepth - 1))
            } else {
              setDependentDepth(Math.max(0, dependentDepth - 1))
            }
          }}
        />
      )}
      <div className="flex-1 relative">
        <ReactFlow
          nodes={positioned}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodeClick={handleNodeClick}
          proOptions={{ hideAttribution: true }}
          minZoom={0.1}
          maxZoom={2}
        >
          <Background color="#e2e8f0" gap={20} />
          <Controls />
        </ReactFlow>

        <div className="absolute top-3 right-3 flex items-center gap-2 bg-white/90 backdrop-blur rounded-lg p-1.5 border border-slate-200 shadow-sm text-xs">
          <button
            onClick={() => fitView({ padding: 0.2, duration: 250, maxZoom: 1 })}
            className="px-2 py-1 rounded hover:bg-slate-100 text-slate-700"
            title="Zoom out to fit everything visible"
          >
            Fit view
          </button>
        </div>

        <Legend />
      </div>
    </div>
  )
}

function DepthBar({
  dependencyDepth,
  dependentDepth,
  onDependencyDepth,
  onDependentDepth,
  nodeLimit,
  onNodeLimit,
  counts,
  limitReached,
}: {
  dependencyDepth: number
  dependentDepth: number
  onDependencyDepth: (d: number) => void
  onDependentDepth: (d: number) => void
  nodeLimit: NodeLimit
  onNodeLimit: (n: NodeLimit) => void
  counts: { total: number; dependencies: number; dependents: number; failed: number }
  limitReached: boolean
}) {
  return (
    <div className="px-3 py-2 border-b border-slate-200 bg-white flex items-center gap-4 flex-wrap">
      <HistoryButtons />
      <DepthControl
        label="Dependencies"
        value={dependencyDepth}
        onChange={onDependencyDepth}
        max={5}
        title="How many hops upstream — things this resource depends on, transitively. Click + to follow the dep chain further."
      />
      <DepthControl
        label="Dependents"
        value={dependentDepth}
        onChange={onDependentDepth}
        max={3}
        title="How many hops downstream — things that depend on this resource, transitively."
      />
      <div className="flex items-center gap-2 text-sm" title="Cap on returned nodes; increase to load more">
        <span className="text-xs text-slate-500 uppercase tracking-wider">Max</span>
        <select
          value={nodeLimit}
          onChange={(e) => onNodeLimit(Number(e.target.value) as NodeLimit)}
          className="text-xs px-2 py-0.5 border border-slate-200 rounded bg-white tabular-nums"
        >
          {NODE_LIMIT_OPTIONS.map((L) => (
            <option key={L} value={L}>
              {L.toLocaleString()}
            </option>
          ))}
        </select>
      </div>
      <div className="ml-auto flex items-center gap-3 text-xs text-slate-500">
        <span title="Total nodes shown (root + dependencies + dependents)">
          Showing{' '}
          <span className={`tabular-nums ${limitReached ? 'text-amber-700 font-semibold' : 'text-slate-700'}`}>
            {counts.total}
          </span>
          {limitReached && <span className="text-amber-700"> / {nodeLimit} (cap)</span>}
        </span>
        <span title="Things this resource depends on, transitively">
          <span className="tabular-nums text-slate-700">{counts.dependencies}</span> dependencies
        </span>
        <span title="Things that depend on this resource, transitively">
          <span className="tabular-nums text-slate-700">{counts.dependents}</span> dependents
        </span>
        {counts.failed > 0 && (
          <span className="text-red-600">
            <span className="tabular-nums">{counts.failed}</span> failed
          </span>
        )}
      </div>
    </div>
  )
}

// HistoryButtons drive the browser's session history. The Subgraph
// route is part of the URL (we navigate on every node click), so the
// browser already records each visited resource. These buttons just
// expose Back / Forward as obvious on-screen affordances since users
// don't always think to use the browser's chrome inside an SPA.
//
// We always render the buttons enabled. There's no portable API to
// detect whether forward history actually exists; the browser
// silently no-ops when there's nothing in that direction. That's
// acceptable — better than tracking a parallel stack and keeping it
// in sync with native back/forward + bookmark loads.
function HistoryButtons() {
  return (
    <div
      className="flex items-center gap-0.5 -my-0.5"
      role="group"
      aria-label="Subgraph history"
    >
      <button
        onClick={() => history.back()}
        className="px-2 py-1 rounded text-slate-700 hover:bg-slate-100 text-sm font-medium"
        title="Back to the previous resource"
        aria-label="Back"
      >
        ←
      </button>
      <button
        onClick={() => history.forward()}
        className="px-2 py-1 rounded text-slate-700 hover:bg-slate-100 text-sm font-medium"
        title="Forward"
        aria-label="Forward"
      >
        →
      </button>
    </div>
  )
}

// LimitNotice surfaces the "your view is truncated" condition. If the
// returned node count equals the cap we asked for, more nodes might
// exist that we're not showing — better to say so loudly than to
// silently mislead the operator into thinking they see the full graph.
function LimitNotice({
  shown,
  limit,
  nextLimit,
  onRaise,
  onReduceDepth,
}: {
  shown: number
  limit: number
  nextLimit: number | undefined
  onRaise: () => void
  onReduceDepth: () => void
}) {
  return (
    <div className="px-3 py-2 bg-amber-50 border-b border-amber-200 text-xs text-amber-900 flex items-center gap-3 flex-wrap">
      <span className="text-base leading-none">⚠</span>
      <span>
        Showing <span className="tabular-nums font-semibold">{shown.toLocaleString()}</span> nodes —
        the cap of <span className="tabular-nums">{limit.toLocaleString()}</span> was hit, so more
        dependencies/dependents may exist beyond what's drawn here.
      </span>
      <div className="ml-auto flex items-center gap-2">
        {nextLimit !== undefined && (
          <button
            onClick={onRaise}
            className="px-2 py-0.5 rounded bg-white border border-amber-300 hover:bg-amber-100 text-amber-900"
            title="Increase the node cap"
          >
            Raise to {nextLimit.toLocaleString()}
          </button>
        )}
        <button
          onClick={onReduceDepth}
          className="px-2 py-0.5 rounded bg-white border border-amber-300 hover:bg-amber-100 text-amber-900"
          title="Reduce the largest depth by one"
        >
          Reduce depth
        </button>
      </div>
    </div>
  )
}

function DepthControl({
  label,
  value,
  onChange,
  max,
  title,
}: {
  label: string
  value: number
  onChange: (n: number) => void
  max: number
  title: string
}) {
  return (
    <div className="flex items-center gap-2 text-sm" title={title}>
      <span className="text-xs text-slate-500 uppercase tracking-wider">{label}</span>
      <button
        onClick={() => onChange(Math.max(0, value - 1))}
        disabled={value <= 0}
        className="w-6 h-6 rounded border border-slate-200 bg-white hover:bg-slate-50 disabled:opacity-40 disabled:cursor-not-allowed text-slate-700"
      >
        −
      </button>
      <span className="tabular-nums w-4 text-center font-mono text-slate-800">{value}</span>
      <button
        onClick={() => onChange(Math.min(max, value + 1))}
        disabled={value >= max}
        className="w-6 h-6 rounded border border-slate-200 bg-white hover:bg-slate-50 disabled:opacity-40 disabled:cursor-not-allowed text-slate-700"
      >
        +
      </button>
    </div>
  )
}

// ── nodes ────────────────────────────────────────────────────────────

const nodeTypes = {
  resource: ResourceNode,
}

function ResourceNode({ data }: NodeProps) {
  const d = data as { node: SubgraphNode; isRoot: boolean }
  const n = d.node
  const phase = n.phase
  const isFailed = phase === 'Failed'
  // Slim subgraph nodes don't carry the conditions array (only the
  // detail fetch does), so the node just renders in the phase color;
  // the operator clicks through to the panel for the failure message.

  return (
    <div
      className={`rounded-md bg-white shadow-sm overflow-hidden ${
        d.isRoot
          ? 'border-2 border-blue-500 shadow-md'
          : isFailed
          ? 'border border-red-400'
          : 'border border-slate-300'
      }`}
      style={{ width: 240, borderLeft: `4px solid ${PHASE_FILL[phase]}` }}
    >
      <Handle type="target" position={Position.Top} className="!bg-slate-400" />
      <div className="px-2 py-1.5">
        <div className="flex items-center justify-between gap-2">
          <span className="text-[10px] text-slate-400 uppercase tracking-wider">{n.kind}</span>
          {d.isRoot ? (
            <span className="text-[10px] font-semibold text-blue-600 uppercase tracking-wider">
              root
            </span>
          ) : (
            <span className="text-[10px] text-slate-400 tabular-nums">
              {n.min_depth > 0 ? `+${n.min_depth}` : n.min_depth}
            </span>
          )}
        </div>
        <div className="text-sm font-mono text-slate-800 truncate" title={n.name}>
          {n.name}
        </div>
        <div className="flex items-center justify-between mt-0.5">
          <span className="text-[10px] text-slate-500 capitalize">{PHASE_LABELS[phase]}</span>
          {n.synced_gen < n.generation && (
            <span className="text-[10px] text-amber-600 tabular-nums" title="synced generation behind spec generation">
              g{n.synced_gen}/{n.generation}
            </span>
          )}
        </div>
      </div>
      <Handle type="source" position={Position.Bottom} className="!bg-slate-400" />
    </div>
  )
}

function Legend() {
  return (
    <div className="absolute bottom-4 left-4 flex gap-3 bg-white/90 backdrop-blur rounded-lg p-2 border border-slate-200 shadow-sm">
      {PHASE_VALUES.map((b) => (
        <div key={b} className="flex items-center gap-1.5">
          <div className="w-3 h-3 rounded-sm" style={{ background: PHASE_FILL[b] }} />
          <span className="text-xs text-slate-600">{PHASE_LABELS[b].toLowerCase()}</span>
        </div>
      ))}
      <span className="w-px h-4 bg-slate-200 mx-1 self-center" />
      <div className="flex items-center gap-1.5">
        <div className="w-3 h-3 rounded-sm border-2 border-blue-500 bg-white" />
        <span className="text-xs text-slate-600">root</span>
      </div>
      <div className="flex items-center gap-1.5">
        <div className="w-4 h-0.5" style={{ background: '#ef4444' }} />
        <span className="text-xs text-slate-600">failure path</span>
      </div>
    </div>
  )
}

// ── layout ───────────────────────────────────────────────────────────

// dagre top-down: ancestors above root, descendants below. We don't
// pin the root explicitly — dagre's rank assignment plus our edge
// direction (dependency → dependent) puts ancestors at the top
// automatically.
function layout(nodes: Node[], edges: Edge[]): Node[] {
  if (nodes.length === 0) return nodes
  const g = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'TB', ranksep: 70, nodesep: 30, marginx: 20, marginy: 20 })
  const W = 240
  const H = 80
  for (const n of nodes) g.setNode(n.id, { width: W, height: H })
  for (const e of edges) g.setEdge(e.source, e.target)
  dagre.layout(g)
  return nodes.map((n) => {
    const p = g.node(n.id)
    return {
      ...n,
      position: { x: p.x - W / 2, y: p.y - H / 2 },
      targetPosition: Position.Top,
      sourcePosition: Position.Bottom,
    }
  })
}
