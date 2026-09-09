import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { stringify as yamlStringify } from 'yaml'
import {
  api,
  retryReadAfterWrite,
  retryDelayReadAfterWrite,
  type Condition,
  type ResourceEvent,
  type ResourceInfo,
  type UpstreamDep,
  type WorkInfo,
  PHASE_BG,
  PHASE_LABELS,
} from '../api'
import { formatDateTime } from '../utils'
import { CreateResourceModal } from './CreateResourceModal'
import { ConfirmDeleteModal } from './ConfirmDeleteModal'

// rawResourceURL builds the raw-bytes download URL for a resource's
// manifest/status/spec, addressed by its public (kind, name).
function rawResourceURL(kind: string, name: string): string {
  return `/api/v1/raw/resources/${encodeURIComponent(kind)}/${encodeURIComponent(name)}`
}

interface Props {
  kind: string
  name: string
  onClose: () => void
  onShowSubgraph?: () => void
  onFilterByLabel?: (key: string, value: string) => void
  // Navigate to a resource's panel (used by clicks on the Owner row).
  onOpenResource?: (kind: string, name: string) => void
}

export function ResourcePanel({
  kind,
  name,
  onClose,
  onShowSubgraph,
  onFilterByLabel,
  onOpenResource,
}: Props) {
  // The resource is addressed by its public (kind, name); its "kind/name"
  // ref is the React Query cache key + label-comparison string.
  const resourceRef = `${kind}/${name}`
  const { data: resource, isPending, isError, error } = useQuery({
    queryKey: ['resource', resourceRef],
    queryFn: () => api.getResource(kind, name),
    refetchInterval: 10000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
    // Tolerate replica-lag 404s right after a create/update — the
    // server may be hitting a read replica that hasn't replicated
    // the new row yet. Bounded + short fixed delay so a genuinely-missing
    // resource surfaces the "not found" panel in ~1.5 s instead of
    // skeletoning for ~30 s.
    retry: retryReadAfterWrite,
    retryDelay: retryDelayReadAfterWrite,
  })

  // The owner is addressed by owner_kind/owner_name (carried on the resource);
  // fetch that specific row for its human name + generation badge. Works for
  // any owner, root or nested.
  const ownerKind = resource?.owner_kind
  const ownerName = resource?.owner_name
  const ownerRef = ownerKind && ownerName ? `${ownerKind}/${ownerName}` : undefined
  const { data: owner } = useQuery({
    queryKey: ['resource', ownerRef],
    queryFn: () => api.getResource(ownerKind!, ownerName!),
    enabled: !!ownerRef,
    staleTime: 5_000,
    retry: retryReadAfterWrite,
  })

  // Apply-spec modal — opens prefilled with the current resource's spec.
  const [applying, setApplying] = useState(false)
  // Delete confirmation modal open flag. Declared here so the prop-change reset
  // below can close it if the panel switches resources.
  const [deleteOpen, setDeleteOpen] = useState(false)
  // Close the Apply modal if the panel switches to a different resource while
  // it's open — otherwise the modal would silently retarget the new resource
  // and an Apply would write to the wrong one. Render-phase reset keyed on the
  // previous resource ref (React's "adjust state on prop change" pattern).
  const [lastResourceRef, setLastResourceRef] = useState(resourceRef)
  if (resourceRef !== lastResourceRef) {
    setLastResourceRef(resourceRef)
    setApplying(false)
    // Also close the delete-confirm modal so its Delete can never fire against a
    // resource the panel just switched away from.
    setDeleteOpen(false)
  }
  const applyTarget: ResourceInfo | null = resource ?? null

  // Force resync — re-pends the resource so its kind's provider runs
  // again even when the spec is unchanged (bumps generation server-side,
  // see Store.Reconcile). Refresh the resource so the new in-flight work
  // claim and Synced=False (generation drift) render right away.
  const queryClient = useQueryClient()

  // Manual refresh — refetch the resource and every per-resource sub-query
  // (deps, spec history, events) on demand, independent of the poll. Used by
  // the ↻ button in the header; also gives a way to refresh while the
  // background poll is paused (tab unfocused).
  const refreshAll = () => {
    for (const key of [
      ['resource', resourceRef],
      ['resource-deps', resourceRef],
      ['resource-spec-history', resourceRef],
      ['resource-events', resourceRef],
    ]) {
      queryClient.invalidateQueries({ queryKey: key })
    }
    // Also refresh the owner row (its name/generation badge) — but only when
    // there IS an owner: invalidating ['resource', undefined] would partial-
    // match and refetch every cached resource.
    if (ownerRef) {
      queryClient.invalidateQueries({ queryKey: ['resource', ownerRef] })
    }
  }

  const resync = useMutation({
    mutationFn: () => api.reconcileResource(kind, name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['resource', resourceRef] })
    },
  })

  // Quarantine / un-quarantine — set a failed/stuck resource aside (freezing
  // it from all schedulers + excluding it from its root's rollup, without
  // deleting it) or release it. The button toggles on the current phase.
  const quarantine = useMutation({
    mutationFn: () => api.quarantineResource(kind, name),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['resource', resourceRef] }),
  })
  const unquarantine = useMutation({
    mutationFn: () => api.unquarantineResource(kind, name),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['resource', resourceRef] }),
  })

  // Delete — a K8s-style SOFT delete: the server stamps deletion_requested_at +
  // a finalizer and drives teardown; the resource enters phase=Deleting and is
  // removed only once its finalizer is stripped (a finalizer-less kind goes
  // immediately). Confirmed in a focused modal (ConfirmDeleteModal) so the
  // action row doesn't reflow. We keep the panel OPEN and just refetch, so the
  // user watches it flip to Deleting rather than losing context; the row (and
  // this panel's 404 "not found" state) appears once teardown completes.
  const del = useMutation({
    mutationFn: () => api.deleteResource(kind, name),
    onSuccess: () => {
      setDeleteOpen(false)
      queryClient.invalidateQueries({ queryKey: ['resource', resourceRef] })
    },
  })

  if (isError) {
    // A 404 here means the resource genuinely doesn't exist (we already
    // retried through any brief replica-lag window) — show a clear
    // "not found" instead of a raw error, since the usual cause is a stale
    // link or a resource that was deleted while the panel was open.
    const notFound = (error as Error)?.message?.startsWith('404')
    return (
      <div className="p-4">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold text-slate-900">Resource</h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 text-lg"
            aria-label="Close"
          >
            &times;
          </button>
        </div>
        {notFound ? (
          <div className="p-3 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800">
            <div className="font-semibold mb-1">Resource not found</div>
            <div>
              No resource <span className="font-mono break-all">{resourceRef}</span>. It may
              have been deleted, or the link is stale.
            </div>
          </div>
        ) : (
          <div className="p-3 bg-red-50 border border-red-200 rounded text-xs text-red-800">
            <div className="font-semibold mb-1">Failed to load resource</div>
            <div className="font-mono break-all">{(error as Error)?.message}</div>
          </div>
        )}
      </div>
    )
  }
  if (isPending || !resource) {
    return <ResourcePanelSkeleton onClose={onClose} />
  }

  const phase = resource.phase
  // Domain labels only — the synthesized owner_kind/owner_name keys are
  // surfaced in the Owner header, not the Manifest → Labels subsection.
  const domainLabels = resource.labels
    ? Object.entries(resource.labels).filter(([k]) => k !== 'owner_kind' && k !== 'owner_name')
    : []

  return (
    <div className="p-4">
      <div className="flex items-start justify-between mb-3 gap-2">
        <div className="min-w-0">
          <h2 className="text-base font-semibold text-slate-900 break-all leading-tight">
            <span className="font-mono text-slate-500 text-sm">{resource.kind}/</span>
            {resource.name}
          </h2>
        </div>
        <div className="flex items-center gap-1 shrink-0">
          <button
            onClick={refreshAll}
            className="text-slate-400 hover:text-slate-700 text-sm leading-none p-1"
            aria-label="Refresh"
            title="Refetch this resource (and its deps, history, events) now"
          >
            ↻
          </button>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 text-lg leading-none"
            aria-label="Close"
          >
            &times;
          </button>
        </div>
      </div>

      {/* Phase badge + actions — the headline status, kept directly under
          the title. The badge renders the server-computed `phase` scalar
          verbatim; the per-axis verdicts (Synced/Ready/custom, each
          True/False/Unknown with a reason) live in Conditions below, and the
          raw scalars (synced_gen/generation, health_ok) in Lifecycle.
          Whole-resource actions (subgraph) sit on the right. */}
      <div className="mb-4 flex items-center gap-2 flex-wrap">
        <span
          className={`inline-block px-2 py-0.5 rounded text-xs text-white font-medium ${PHASE_BG[phase]}`}
        >
          {PHASE_LABELS[phase].toLowerCase()}
        </span>
        <div className="ml-auto flex items-center gap-2">
          {onShowSubgraph && (
            <button
              onClick={onShowSubgraph}
              className="text-xs px-2 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
            >
              View subgraph →
            </button>
          )}
        </div>
      </div>

      {/* Owner pulled to the top — this is the spine of the panel.
          Click owner name → open that resource's panel (navigate by its
          kind/name). Click the kind chip → add an owner_kind filter on the
          list. Both read the resource's owner_kind / owner_name (the K8s
          identity of its owner). Roots have no owner; section is hidden. */}
      {(resource.owner_kind || resource.owner_name) && (
        <div className="mb-4 p-2 bg-slate-50 border border-slate-200 rounded">
          <div className="text-[10px] font-medium uppercase tracking-wider text-slate-500 mb-1">
            Owner
          </div>
          <div className="flex items-center gap-2 min-w-0">
            {resource.owner_kind &&
              (onFilterByLabel ? (
                <button
                  onClick={() => onFilterByLabel('owner_kind', resource.owner_kind!)}
                  className="text-[11px] font-mono px-1.5 py-0.5 rounded border bg-indigo-50 text-indigo-800 border-indigo-200 hover:bg-indigo-100 shrink-0"
                  title="Filter Resources by owner_kind"
                >
                  {resource.owner_kind}
                </button>
              ) : (
                <span className="text-[11px] font-mono px-1.5 py-0.5 rounded border bg-indigo-50 text-indigo-800 border-indigo-200 shrink-0">
                  {resource.owner_kind}
                </span>
              ))}
            {resource.owner_name &&
              (onOpenResource && resource.owner_kind ? (
                <button
                  onClick={() => onOpenResource(resource.owner_kind!, resource.owner_name!)}
                  className="text-sm font-medium text-slate-800 hover:text-blue-700 truncate text-left"
                  title="Open this owner's resource panel"
                >
                  {owner?.name ?? resource.owner_name}
                </button>
              ) : (
                <span className="text-sm font-medium text-slate-800 truncate">
                  {owner?.name ?? resource.owner_name}
                </span>
              ))}
            {owner && (
              <span className="ml-auto text-[10px] text-slate-400 tabular-nums shrink-0">
                g{owner.generation}
              </span>
            )}
          </div>
        </div>
      )}

      {/* In-progress work — who is reconciling this and for how long.
          Present only while a task is queued/claimed (the server omits it
          when settled), so this card appears for Reconciling/Deleting
          resources and answers "is it stuck, and where do I look?". */}
      {resource.work && <WorkInProgress work={resource.work} resourceRef={resourceRef} />}

      {/* Lifecycle: the RAW scalar facts behind the phase + soft-delete
          state. These are the underlying numbers, not verdicts — the
          Synced/Ready verdicts (with reasons) live in Conditions. We show
          synced_gen/generation (the Synced axis numbers) and the raw
          health_ok boolean (the Ready axis input); health_ok defaults true
          on a never-probed resource, so it is shown as a plain value, not
          a "healthy" claim that could contradict phase=Failed. finalizers +
          deletion_requested_at expose the soft-delete drain when active. */}
      <Section title="Lifecycle">
        <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
          {/* Web-API version this resource is pinned to (v1, v2, …). It routes to
              a worker serving its (kind, kind_version); a change is a breaking flip. */}
          <dt className="text-slate-500">version</dt>
          <dd className="text-slate-800 tabular-nums font-mono">v{resource.kind_version}</dd>
          <dt className="text-slate-500">synced_gen</dt>
          <dd className="text-slate-800 tabular-nums">
            {resource.synced_gen}/{resource.generation}
            {resource.synced_gen < resource.generation && (
              <span className="text-amber-600"> (behind)</span>
            )}
          </dd>
          <dt className="text-slate-500">health_ok</dt>
          <dd className="text-slate-800 tabular-nums font-mono">
            {String(resource.health_ok)}
          </dd>
          {/* Transient-failure retry accounting. failure_attempts is the durable
              count of consecutive transient reconcile failures for this generation;
              it climbs toward the kind's max_transient_attempts cap, at which point
              the failure DEAD-LETTERS (failure_terminal → stops auto-retrying).
              Shown only when there IS a failure so a healthy resource stays clean. */}
          {(resource.failure_terminal || (resource.failure_attempts ?? 0) > 0) && (
            <>
              <dt className="text-slate-500">retries</dt>
              <dd className="text-slate-800 tabular-nums">
                {resource.failure_terminal ? (
                  <span
                    className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-red-50 text-red-800 border border-red-200"
                    title="This failure won't be retried automatically (a persistently-transient failure hit the kind's max_transient_attempts cap, or the provider declared it terminal). Recover by editing the spec or forcing a reconcile."
                  >
                    dead-lettered
                    {(resource.failure_attempts ?? 0) > 0 &&
                      ` after ${resource.failure_attempts} attempt${resource.failure_attempts === 1 ? '' : 's'}`}
                  </span>
                ) : (
                  <span className="text-amber-600">
                    attempt {resource.failure_attempts}
                    {resource.max_transient_attempts !== undefined &&
                      resource.max_transient_attempts > 0 &&
                      ` of ${resource.max_transient_attempts}`}
                    {' '}
                    <span className="text-slate-500">(transient, retrying)</span>
                  </span>
                )}
              </dd>
            </>
          )}
          {/* Manifest drift: this resource is pinned to an OLDER kind manifest
              than the kind's current applied version — a CRD was re-applied
              since it last scheduled. It re-pins on its next reconcile. Shown
              only when drifted so a fresh resource stays clean. */}
          {resource.manifest_drift && (
            <>
              <dt className="text-slate-500">CRD version</dt>
              <dd className="text-slate-800 tabular-nums">
                <span
                  className="inline-block px-1.5 py-0.5 rounded text-[11px] bg-amber-50 text-amber-800 border border-amber-200"
                  title="This resource is pinned to an older kind manifest than the kind's current version; it re-pins on its next reconcile."
                >
                  stale manifest
                </span>
              </dd>
            </>
          )}
          {resource.finalizers && resource.finalizers.length > 0 && (
            <>
              <dt className="text-slate-500">finalizers</dt>
              <dd className="text-slate-800 font-mono text-[11px]">
                {resource.finalizers.join(', ')}
              </dd>
            </>
          )}
          {resource.deletion_requested_at && (
            <>
              <dt className="text-slate-500">deletion requested</dt>
              <dd className="text-slate-800 tabular-nums" title={resource.deletion_requested_at}>
                {formatDateTime(resource.deletion_requested_at)}
              </dd>
            </>
          )}
          <Timestamp label="updated" value={resource.updated_at} />
          <Timestamp label="created" value={resource.created_at} />
        </dl>
      </Section>

      {/* Conditions — K8s/Crossplane multi-axis status. The detail
          fetch carries the synthesized Synced + Ready axes plus any
          custom conditions the provider reported; render only when
          the array is non-empty. */}
      {resource.conditions && resource.conditions.length > 0 && (
        <Section title="Conditions">
          <ul className="space-y-1">
            {resource.conditions.map((c, i) => (
              <ConditionRow key={`${c.type}-${i}`} condition={c} />
            ))}
          </ul>
        </Section>
      )}

      {/* Dependencies — upstream resources this one waits on. Shows
          each upstream's readiness plus any pending value mappings
          (spec field still waiting on a source status field). When
          this resource is itself not ready and at least one upstream
          isn't ready (or has unresolved mappings), the section flips
          to a "Blocked on" view — exactly which spec fields are still
          unfilled and which upstream is supplying them. */}
      <DependenciesSection kind={kind} name={name} dependentReady={resource.is_ready} />

      {/* Manifest — the resource's full document grouped in one place:
          domain Labels, the desired-state Spec, any attached custom
          Provider config, and the provider's observed Status. The
          section header carries the whole-document actions: Apply… opens
          the manifest editor (prefilled); Download streams the full
          ResourceManifest ({kind,name,labels,spec}) from the raw endpoint
          so the saved file re-applies as-is (full bytes even when the spec
          is server-elided inline). */}
      <div className="mb-4">
        <div className="flex items-center gap-2 mb-2">
          <h3 className="text-xs font-medium text-slate-500 uppercase tracking-wider">
            Manifest
          </h3>
          <div className="ml-auto flex items-center gap-2">
            {/* Resync is hidden while Quarantined: quarantine is a hard freeze,
                so a resync would be a silent no-op (the scheduler skips frozen
                rows). Release first to resume. (Apply… stays — you can stage a
                new spec that takes effect once released.) */}
            {phase !== 'Quarantined' && (
              <button
                onClick={() => resync.mutate()}
                disabled={resync.isPending}
                className="text-[11px] px-1.5 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700 disabled:opacity-50"
                title="Force the provider to reconcile this resource again (re-pends even if the spec is unchanged)"
              >
                {resync.isPending ? 'Resyncing…' : 'Resync'}
              </button>
            )}
            {phase === 'Quarantined' ? (
              <button
                onClick={() => unquarantine.mutate()}
                disabled={unquarantine.isPending}
                className="text-[11px] px-1.5 py-0.5 rounded bg-yellow-200 hover:bg-yellow-300 text-yellow-900 disabled:opacity-50"
                title="Release this resource from quarantine — it rejoins normal scheduling and re-pends if still lagging"
              >
                {unquarantine.isPending ? 'Releasing…' : 'Release'}
              </button>
            ) : (
              <button
                onClick={() => quarantine.mutate()}
                disabled={quarantine.isPending}
                className="text-[11px] px-1.5 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700 disabled:opacity-50"
                title="Set this resource aside: stop retrying it and exclude it from its root's rollup, without deleting it"
              >
                {quarantine.isPending ? 'Quarantining…' : 'Quarantine'}
              </button>
            )}
            <button
              onClick={() => setApplying(true)}
              className="text-[11px] px-1.5 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
              title="Apply a new manifest to this resource"
            >
              Apply…
            </button>
            <a
              href={`${rawResourceURL(kind, name)}/manifest`}
              download={`${resource.kind}-${resource.name}-manifest.json`}
              className="text-[11px] px-1.5 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
              title="Download the full manifest (kind, name, labels, spec)"
            >
              Download
            </a>
            {/* Delete — a soft delete (server stamps deletion_requested_at + a
                finalizer, then tears down). Once a delete is already in flight
                (phase=Deleting), show a static badge instead of re-offering it.
                Confirmed in ConfirmDeleteModal so the row doesn't reflow. */}
            {resource.deletion_requested_at ? (
              <span
                className="text-[11px] px-1.5 py-0.5 rounded bg-slate-400 text-white"
                title="Deletion requested — teardown in progress"
              >
                Deleting…
              </span>
            ) : (
              <button
                onClick={() => setDeleteOpen(true)}
                className="text-[11px] px-1.5 py-0.5 rounded bg-red-50 hover:bg-red-100 text-red-700 border border-red-200"
                title="Delete this resource (soft delete: requests teardown)"
              >
                Delete…
              </button>
            )}
          </div>
        </div>

        <div className="space-y-4">
          {/* Labels — planner-attached domain labels only (owner_kind /
              owner_name are surfaced in the Owner header above). */}
          <SubSection title="Labels">
            {domainLabels.length > 0 ? (
              <div className="flex flex-wrap gap-1">
                {domainLabels.map(([k, v]) =>
                  onFilterByLabel ? (
                    <button
                      key={k}
                      onClick={() => onFilterByLabel(k, v)}
                      title={`Filter Resources by ${k}=${v}`}
                      className="inline-flex items-center text-[11px] font-mono bg-slate-100 border border-slate-200 rounded px-1.5 py-0.5 text-slate-700 hover:bg-blue-50 hover:border-blue-300 hover:text-blue-800 transition-colors"
                    >
                      <span className="text-slate-500">{k}</span>
                      <span className="text-slate-400 mx-0.5">=</span>
                      <span>{v}</span>
                    </button>
                  ) : (
                    <span
                      key={k}
                      className="inline-flex items-center text-[11px] font-mono bg-slate-100 border border-slate-200 rounded px-1.5 py-0.5 text-slate-700"
                    >
                      <span className="text-slate-500">{k}</span>
                      <span className="text-slate-400 mx-0.5">=</span>
                      <span>{v}</span>
                    </span>
                  ),
                )}
              </div>
            ) : (
              <p className="text-xs text-slate-500 italic">No labels</p>
            )}
          </SubSection>

          {/* Spec — desired state. Actions live on the Manifest header;
              this viewer is body-only (its own Download is suppressed — the
              Manifest Download streams the spec bytes, server-elided or
              not). */}
          <SubSection title="Spec">
            <JsonViewer
              value={resource.spec}
              downloadName={`${resource.kind}-${resource.name}-spec.json`}
              hideDownload
            />
          </SubSection>

          {/* Provider config — the resource's attached CUSTOM config (if
              any), cloned into the work queue at schedule and overriding the
              kind default per field. Rendered as a chip (matching Labels)
              linking to the provider-configs page. No attachment → the kind
              default. */}
          <SubSection title="Provider config">
            {resource.provider_config ? (
              <Link
                to={`/providerconfigs/${encodeURIComponent(resource.provider_config.name)}`}
                className="inline-flex items-center text-[11px] font-mono bg-slate-100 border border-slate-200 rounded px-1.5 py-0.5 text-slate-700 hover:bg-violet-50 hover:border-violet-300 hover:text-violet-800 transition-colors break-all"
                title="Open this provider config"
              >
                <span className="text-slate-500">{resource.provider_config.kind}/</span>
                {resource.provider_config.name}
              </Link>
            ) : (
              <p className="text-xs text-slate-500 italic">Kind default (no custom config attached)</p>
            )}
          </SubSection>

          {/* Status — provider's observed output. Keeps its own Download
              (status bytes aren't part of the manifest); elided server-side
              on the same threshold as spec. */}
          <SubSection title="Status">
            {resource.status ? (
              <JsonViewer
                value={resource.status}
                downloadName={`${resource.kind}-${resource.name}-status.json`}
                downloadURL={`${rawResourceURL(kind, name)}/status`}
              />
            ) : (
              <p className="text-xs text-slate-500 italic">Not yet reported</p>
            )}
          </SubSection>
        </div>
      </div>

      {/* Spec history — roots only. Composed children keep only their
          live body (no meaningful history), so the section is shown only
          for roots (no owner). Lists immutable revisions; each can be
          viewed (raw) or restored (re-applied as a new revision). */}
      {!resource.owner_kind && <HistorySection kind={kind} name={name} />}

      {/* Events — append-only audit log per resource. Polls at the
          same cadence as the resource itself so the timeline stays
          fresh while the panel is open. */}
      <EventsSection kind={kind} name={name} />

      {/* Apply-manifest modal. Opens prefilled with the resource's
          current manifest; closes itself on a successful apply
          (create-or-update keyed by kind+name). The same modal backs the
          "Apply manifest" button on the page header (with applyTo={null}).
          When the spec is server-elided (too big to inline), tell the
          modal to skip the prefill fetch — it'd only pull megabytes to
          refuse to render them. */}
      <CreateResourceModal
        open={applying}
        applyTo={applyTarget}
        prefillTooLarge={isElidedMarker(resource.spec)}
        onClose={() => setApplying(false)}
        onApplied={() => setApplying(false)}
      />

      <ConfirmDeleteModal
        open={deleteOpen}
        target={`${resource.kind}/${resource.name}`}
        detail="Soft delete: the server requests teardown (finalizers run first) and the resource moves to Deleting. This cascades — its entire dependency tree (any owned children and anything depending on it) is torn down too, deepest-first. An in-flight reconcile finishes harmlessly."
        busy={del.isPending}
        errorMessage={del.isError ? (del.error as Error)?.message : undefined}
        onConfirm={() => del.mutate()}
        onClose={() => {
          // Don't close mid-request (the mutation's onSuccess closes it), and
          // clear any prior error so a reopened dialog starts clean.
          if (del.isPending) return
          del.reset()
          setDeleteOpen(false)
        }}
      />
    </div>
  )
}

// Keys must match internal/store/events.go EventType constants.
const EVENT_COLORS: Record<string, string> = {
  'compose-succeeded':   'bg-indigo-100   text-indigo-800   border-indigo-200',
  'compose-failed':      'bg-red-100      text-red-800      border-red-200',
  'work-started':        'bg-amber-100    text-amber-800    border-amber-200',
  'work-succeeded':      'bg-emerald-100  text-emerald-800  border-emerald-200',
  'work-failed':         'bg-red-100      text-red-800      border-red-200',
  'rollup-succeeded':    'bg-cyan-100     text-cyan-800     border-cyan-200',
  'rollup-failed':       'bg-red-100      text-red-800      border-red-200',
  'delete-succeeded':    'bg-purple-100   text-purple-800   border-purple-200',
  'delete-failed':       'bg-red-100      text-red-800      border-red-200',
  'operate-started':     'bg-amber-100    text-amber-800    border-amber-200',
  'operate-succeeded':   'bg-emerald-100  text-emerald-800  border-emerald-200',
  'operate-failed':      'bg-red-100      text-red-800      border-red-200',
  'condition-flipped':   'bg-slate-100    text-slate-800    border-slate-200',
  'deletion-requested':  'bg-rose-100     text-rose-800     border-rose-200',
  'deletion-finalized':  'bg-purple-100   text-purple-800   border-purple-200',
  requeued:              'bg-yellow-100   text-yellow-800   border-yellow-200',
  'spec-changed':        'bg-blue-100     text-blue-800     border-blue-200',
}

// WorkInProgress surfaces the live work_queue claim for a resource that's
// currently being reconciled (or deleted): which worker holds it, how long
// it's been running, whether its heartbeat is fresh (genuinely working) or
// stale (stuck / about to be reaped), and the attempt count. It also shows
// the log-filter recipe — the executing worker + resource id are the slog keys.
// Turns a flat "Reconciling" into actionable detail for long-running work.
function WorkInProgress({ work, resourceRef }: { work: WorkInfo; resourceRef: string }) {
  // A heartbeat age in seconds/minutes is fine; once it reads in
  // minutes-plural or hours it's likely stalling (the reaper's stale
  // window is ~30s, so a multi-minute heartbeat age means trouble).
  const hbStale = /\d+m|\d+h/.test(work.heartbeat_age)
  return (
    <Section title={work.claimed ? 'Working' : 'Queued'}>
      <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
        {work.claimed ? (
          <>
            {/* The dumb worker that actually runs the stage — what the operator
                usually wants ("which worker's log?"). Falls back to broker_id
                (the claiming/lease pod) for an in-process run where no separate
                worker executed it. */}
            <dt className="text-slate-500">worker</dt>
            <dd className="text-slate-800 font-mono text-[11px] break-all" title="the worker process running this stage — use it to find logs">
              {work.worker_id || work.broker_id}
            </dd>
            {work.worker_id && work.broker_id && work.worker_id !== work.broker_id && (
              <>
                <dt className="text-slate-500">claimed by</dt>
                <dd className="text-slate-500 font-mono text-[11px] break-all" title="the broker that claimed this task and dispatched it to the worker above">
                  {work.broker_id}
                </dd>
              </>
            )}
            <dt className="text-slate-500">running for</dt>
            <dd className="text-slate-800 tabular-nums">{work.elapsed}</dd>
            <dt className="text-slate-500">heartbeat</dt>
            <dd className={`tabular-nums ${hbStale ? 'text-red-600' : 'text-emerald-600'}`}
                title={hbStale ? 'heartbeat is old — worker may be stuck; the reaper will reclaim it' : 'fresh — worker is alive'}>
              {work.heartbeat_age} ago{hbStale ? ' ⚠' : ''}
            </dd>
          </>
        ) : (
          <>
            <dt className="text-slate-500">status</dt>
            <dd className="text-slate-800">queued, not yet claimed</dd>
            <dt className="text-slate-500">queued for</dt>
            <dd className="text-slate-800 tabular-nums">{work.elapsed}</dd>
          </>
        )}
        <dt className="text-slate-500">task</dt>
        <dd className="text-slate-800">{work.task_type}</dd>
        {work.attempts > 1 && (
          <>
            <dt className="text-slate-500">attempt</dt>
            <dd className="text-amber-700 tabular-nums" title="this task has failed and been retried">
              #{work.attempts}
            </dd>
          </>
        )}
      </dl>
      {work.claimed && (work.worker_id || work.broker_id) && (
        <p className="mt-2 text-[10px] text-slate-400 font-mono break-all">
          logs: worker={work.worker_id || work.broker_id} resource={resourceRef}
        </p>
      )}
    </Section>
  )
}

// DependenciesSection renders the resource's upstream dependencies.
// Each upstream gets a row with its readiness state, name, and (when
// present) the spec.X ← status.Y pending mappings still waiting to be
// filled in. When the dependent itself isn't ready and at least one
// upstream is "blocking" (upstream_ready=false or has pending mappings),
// the section title becomes "Blocked on (N)" and blocking rows are
// highlighted.
function DependenciesSection({
  kind,
  name,
  dependentReady,
}: {
  kind: string
  name: string
  dependentReady: boolean
}) {
  const { data, isPending, isError } = useQuery({
    queryKey: ['resource-deps', `${kind}/${name}`],
    queryFn: () => api.listResourceDependencies(kind, name),
    refetchInterval: 10000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
  })

  if (isError) {
    return (
      <Section title="Dependencies">
        <p className="text-xs text-red-600 italic">Failed to load dependencies.</p>
      </Section>
    )
  }
  if (isPending) return null
  if (data.length === 0) return null

  // arch-v2 has no per-mapping bootstrap latch: an upstream is blocking
  // purely when it isn't ready (is_ready=false — not synced and/or not
  // healthy). Value mappings always render as the wired data path.
  const blocking = data.filter((d) => !d.upstream_ready)
  const isBlocked = !dependentReady && blocking.length > 0
  const title = isBlocked ? `Blocked on (${blocking.length})` : 'Dependencies'

  return (
    <Section title={title}>
      <ul className="space-y-1">
        {data.map((d) => (
          <DependencyRow key={`${d.kind}/${d.name}`} dep={d} highlight={isBlocked && !d.upstream_ready} />
        ))}
      </ul>
    </Section>
  )
}

const DEP_READY_BADGE = 'bg-emerald-100 text-emerald-800 border-emerald-300'
const DEP_UNHEALTHY_BADGE = 'bg-rose-100 text-rose-800 border-rose-300'
const DEP_NOT_READY_BADGE = 'bg-amber-100 text-amber-800 border-amber-300'

function DependencyRow({ dep, highlight }: { dep: UpstreamDep; highlight: boolean }) {
  // Two-axis upstream status: ready (synced AND healthy) vs
  // synced-but-unhealthy (Ready=False) vs not-yet-synced.
  let badge = DEP_NOT_READY_BADGE
  let label = 'not ready'
  let title = 'upstream not synced yet'
  if (dep.upstream_ready) {
    badge = DEP_READY_BADGE
    label = 'ready'
    title = 'upstream synced and healthy'
  } else if (!dep.upstream_health_ok) {
    badge = DEP_UNHEALTHY_BADGE
    label = 'unhealthy'
    title = 'upstream synced but reporting Ready=False'
  }
  return (
    <li
      className={`text-xs flex items-start gap-2 px-2 py-1 rounded ${
        highlight ? 'bg-amber-50 border border-amber-200' : ''
      }`}
    >
      <span
        className={`inline-block px-1.5 py-0.5 rounded border font-mono text-[10px] shrink-0 ${badge}`}
        title={title}
      >
        {label}
      </span>
      <div className="min-w-0 flex-1">
        <div className="text-slate-800 break-words">
          <span className="font-mono text-slate-500">{dep.kind}/</span>
          <span className="font-medium">{dep.name}</span>
        </div>
        {dep.value_mappings && dep.value_mappings.length > 0 && (
          <ul className="mt-0.5 space-y-0.5">
            {dep.value_mappings.map((m, i) => (
              <li
                key={i}
                className="text-[11px] font-mono text-slate-600"
                title="wired value flow"
              >
                spec{m.dependent_field} ← status{m.source_field}
              </li>
            ))}
          </ul>
        )}
      </div>
      {dep.upstream_updated_at && (
        <span
          className="text-[10px] text-slate-400 tabular-nums shrink-0"
          title={dep.upstream_updated_at}
        >
          {formatDateTime(dep.upstream_updated_at)}
        </span>
      )}
    </li>
  )
}

// HistorySection lists a root's spec revisions (newest-first). Each row
// shows the authored generation/source/size/time + a "current" badge for
// whichever revision is live now. View streams the raw body; Apply makes
// that revision live (copies its body into the live spec + re-reconciles —
// no new version; navigable in any order, any number of times). Roots-only.
function HistorySection({ kind, name }: { kind: string; name: string }) {
  const queryClient = useQueryClient()
  const resourceRef = `${kind}/${name}`
  const { data, isPending, isError } = useQuery({
    queryKey: ['resource-spec-history', resourceRef],
    queryFn: () => api.listSpecHistory(kind, name, 50),
    refetchInterval: 10000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
  })

  // Apply = make this revision live: copies its body into the live spec (no
  // new version). Refresh the resource + its history so the current badge
  // and the live spec re-render immediately.
  const checkout = useMutation({
    mutationFn: (generation: number) => api.rollbackResource(kind, name, generation),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['resource', resourceRef] })
      queryClient.invalidateQueries({ queryKey: ['resource-spec-history', resourceRef] })
    },
  })

  if (isError) {
    return (
      <Section title="Spec history">
        <p className="text-xs text-red-600 italic">Failed to load history.</p>
      </Section>
    )
  }
  if (isPending) {
    return (
      <Section title="Spec history">
        <p className="text-xs text-slate-500 italic">Loading…</p>
      </Section>
    )
  }
  if (data.length === 0) {
    return (
      <Section title="Spec history">
        <p className="text-xs text-slate-500 italic">No revisions yet.</p>
      </Section>
    )
  }
  return (
    <Section title="Spec history">
      <ul className="space-y-1">
        {data.map((rev) => (
          <li
            key={rev.generation}
            className="flex items-center gap-2 text-xs py-0.5 border-b border-slate-100 last:border-0"
          >
            <span className="tabular-nums font-mono text-slate-700">gen {rev.generation}</span>
            <span className="px-1 rounded bg-slate-100 text-slate-600 text-[10px]">{rev.source}</span>
            {rev.is_current && (
              <span className="px-1 rounded bg-emerald-100 text-emerald-800 text-[10px] border border-emerald-200">
                current
              </span>
            )}
            <span className="text-slate-400 tabular-nums">{formatBytes(rev.size_bytes)}</span>
            <span className="ml-auto text-[10px] text-slate-400 tabular-nums" title={rev.created_at}>
              {formatDateTime(rev.created_at)}
            </span>
            <a
              href={`${rawResourceURL(kind, name)}/spec/${rev.generation}`}
              target="_blank"
              rel="noreferrer"
              className="text-[11px] px-1.5 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
              title="View this revision's raw spec"
            >
              View
            </a>
            {!rev.is_current && (
              <button
                onClick={() => checkout.mutate(rev.generation)}
                disabled={checkout.isPending}
                className="text-[11px] px-1.5 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700 disabled:opacity-50"
                title="Make this revision the live spec"
              >
                Apply
              </button>
            )}
          </li>
        ))}
      </ul>
    </Section>
  )
}

function EventsSection({ kind, name }: { kind: string; name: string }) {
  const { data, isPending, isError } = useQuery({
    queryKey: ['resource-events', `${kind}/${name}`],
    queryFn: () => api.listEvents(kind, name, '', 25),
    refetchInterval: 10000,
    refetchIntervalInBackground: false,
    placeholderData: (prev) => prev,
  })

  if (isError) {
    return (
      <Section title="Events">
        <p className="text-xs text-red-600 italic">Failed to load events.</p>
      </Section>
    )
  }
  if (isPending) {
    return (
      <Section title="Events">
        <p className="text-xs text-slate-500 italic">Loading…</p>
      </Section>
    )
  }
  if (data.events.length === 0) {
    return (
      <Section title="Events">
        <p className="text-xs text-slate-500 italic">No events yet.</p>
      </Section>
    )
  }
  return (
    <Section title="Events">
      <ul className="space-y-1">
        {data.events.map((e, i) => (
          <EventRow key={`${e.created_at}-${e.type}-${i}`} event={e} />
        ))}
      </ul>
    </Section>
  )
}

// Condition palette: each canonical condition type carries an "intent"
// — Synced/Ready=True is the good outcome; Deleting=True means the row
// is in finalizer drain. Anything else (Status=False/Unknown for the
// success types, or an unknown/custom type) renders neutrally and the
// operator inspects message/reason for the detail.
const CONDITION_HEALTHY_TYPES = new Set([
  'Synced',
  'Ready',
])

const CONDITION_PALETTE: Record<string, string> = {
  healthy: 'bg-emerald-100 text-emerald-800 border-emerald-200',
  deleting: 'bg-rose-100   text-rose-800   border-rose-200',
  alarm:    'bg-amber-100  text-amber-800  border-amber-200',
  neutral:  'bg-slate-100  text-slate-700  border-slate-200',
}

function conditionPalette(c: Condition): string {
  if (c.type === 'Deleting' && c.status === 'True') return CONDITION_PALETTE.deleting
  if (CONDITION_HEALTHY_TYPES.has(c.type) && c.status === 'True') return CONDITION_PALETTE.healthy
  if (CONDITION_HEALTHY_TYPES.has(c.type) && c.status === 'False') return CONDITION_PALETTE.alarm
  return CONDITION_PALETTE.neutral
}

function ConditionRow({ condition }: { condition: Condition }) {
  const palette = conditionPalette(condition)
  return (
    <li className="text-xs flex items-start gap-2">
      <span
        className={`inline-block px-1.5 py-0.5 rounded border font-mono text-[10px] shrink-0 ${palette}`}
        title={`${condition.type}=${condition.status}`}
      >
        {condition.type}={condition.status}
      </span>
      <div className="min-w-0 flex-1">
        {condition.reason && (
          <span className="text-slate-700 font-medium">{condition.reason}</span>
        )}
        {condition.message && (
          <span className="text-slate-600">
            {condition.reason ? ' — ' : ''}
            {condition.message}
          </span>
        )}
      </div>
      <span
        className="text-[10px] text-slate-400 tabular-nums shrink-0"
        title={condition.last_transition_at}
      >
        {formatDateTime(condition.last_transition_at)}
      </span>
    </li>
  )
}

function EventRow({ event }: { event: ResourceEvent }) {
  const palette = EVENT_COLORS[event.type] ?? 'bg-slate-100 text-slate-800 border-slate-200'
  const detail = event.detail && Object.keys(event.detail).length > 0
    ? Object.entries(event.detail).map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`).join(' · ')
    : ''
  return (
    <li className="text-xs flex items-start gap-2">
      <span
        className={`inline-block px-1.5 py-0.5 rounded border font-mono text-[10px] shrink-0 ${palette}`}
      >
        {event.type}
      </span>
      <div className="min-w-0 flex-1">
        {event.message && <div className="text-slate-800 break-words">{event.message}</div>}
        {detail && <div className="text-slate-500 font-mono text-[10px] break-all">{detail}</div>}
      </div>
      <span className="text-[10px] text-slate-400 tabular-nums shrink-0" title={event.created_at}>
        {formatDateTime(event.created_at)}
      </span>
    </li>
  )
}

function ResourcePanelSkeleton({ onClose }: { onClose: () => void }) {
  return (
    <div className="p-4 animate-pulse" aria-busy="true" aria-live="polite">
      <div className="flex items-center justify-between mb-4">
        <div className="h-4 w-40 bg-slate-200 rounded" />
        <button
          onClick={onClose}
          className="text-slate-400 hover:text-slate-700 text-lg"
          aria-label="Close"
        >
          &times;
        </button>
      </div>
      <div className="h-5 w-20 bg-slate-200 rounded mb-4" />
      <div className="h-3 w-24 bg-slate-200 rounded mb-2" />
      <div className="space-y-1">
        <div className="h-3 bg-slate-200 rounded" />
        <div className="h-3 bg-slate-200 rounded" />
        <div className="h-3 w-2/3 bg-slate-200 rounded" />
      </div>
      <div className="h-3 w-24 bg-slate-200 rounded mt-6 mb-2" />
      <div className="h-24 bg-slate-200 rounded" />
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="mb-4">
      <h3 className="text-xs font-medium text-slate-500 mb-1 uppercase tracking-wider">
        {title}
      </h3>
      {children}
    </div>
  )
}

// SubSection is a lighter heading used INSIDE the Manifest group (Labels,
// Spec, Provider config, Status). Smaller/dimmer than Section so the nesting
// under the Manifest header reads at a glance; the Manifest body spaces the
// subsections with its own space-y, so this carries no bottom margin.
function SubSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h4 className="text-[10px] font-medium text-slate-400 mb-1 uppercase tracking-wider">
        {title}
      </h4>
      {children}
    </div>
  )
}


// JsonViewer renders JSON inline up to JSON_INLINE_LIMIT_BYTES; above
// that threshold the body is elided client-side. Two separate elision
// stages can fire:
//
//   1. Server-side: the API replaces oversize spec/status payloads
//      with {"_elided": true, "size_bytes": N, "reason": "..."}. The
//      original bytes are NOT in the polled response — `value` IS the
//      marker. We detect it via shape, render the original size, and
//      (if downloadURL was supplied) route the Download button to the
//      server's raw-bytes endpoint.
//
//   2. Client-side fallback: payload is small enough that the server
//      shipped it inline, but still over JSON_INLINE_LIMIT_BYTES so we
//      don't want to render it as text. Build a local blob and
//      download from memory.
const JSON_INLINE_LIMIT_BYTES = 50 * 1024

interface ElidedMarker {
  _elided: true
  size_bytes: number
  reason?: string
}

function isElidedMarker(v: unknown): v is ElidedMarker {
  return (
    typeof v === 'object' &&
    v !== null &&
    (v as { _elided?: unknown })._elided === true &&
    typeof (v as { size_bytes?: unknown }).size_bytes === 'number'
  )
}

function JsonViewer({
  value,
  downloadName,
  downloadURL,
  extraActions,
  hideDownload,
}: {
  value: unknown
  downloadName: string
  // When set, the Download button hits this URL instead of building a
  // blob from `value`. Required for server-elided payloads — `value`
  // doesn't carry the real bytes.
  downloadURL?: string
  // Optional extra buttons rendered to the LEFT of Download in the
  // viewer header (e.g. an Update button on the Spec section).
  extraActions?: React.ReactNode
  // Suppress this viewer's own Download button — used by the Spec
  // subsection under Manifest, whose bytes are fetched via the Manifest
  // header's Download (the full-manifest stream). The too-large/elided
  // hint then points there instead of to a button that isn't rendered.
  hideDownload?: boolean
}) {
  const elided = isElidedMarker(value)
  // Inline preview is YAML — easier to read at a glance for the
  // nested specs/statuses these resources produce. The download path
  // still emits the canonical JSON bytes (the server stores JSON, and
  // round-tripping YAML→JSON is a different shape contract).
  const jsonSerialized = elided ? '' : JSON.stringify(value, null, 2) ?? 'null'
  const yamlSerialized = elided
    ? ''
    : value === null || value === undefined
      ? 'null'
      : yamlStringify(value, { indent: 2 })
  const sizeBytes = elided ? value.size_bytes : jsonSerialized.length
  const tooLarge = elided || sizeBytes > JSON_INLINE_LIMIT_BYTES

  // Client-side download disabled when we don't actually have the bytes.
  const canClientDownload = !elided

  const handleDownload = () => {
    if (downloadURL) {
      // Server-elided OR caller wants the canonical bytes regardless.
      // Trigger a same-origin navigation; the server sets
      // Content-Disposition: attachment so the browser saves it.
      const a = document.createElement('a')
      a.href = downloadURL
      a.download = downloadName
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      return
    }
    if (!canClientDownload) return // shouldn't happen — button is disabled
    const blob = new Blob([jsonSerialized], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = downloadName
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  const downloadDisabled = !downloadURL && !canClientDownload
  // Hint shown inside the too-large/elided box. When this viewer hides its
  // own Download, point at the Manifest header's Download (which streams the
  // full manifest, spec bytes included) instead of a button that isn't here.
  const largeHint = hideDownload
    ? ' Use Manifest → Download above to fetch the full bytes.'
    : ' Use Download to fetch the full bytes.'

  return (
    <div>
      <div className="flex items-center gap-2 mb-1">
        <span className="text-[10px] text-slate-400 tabular-nums">{formatBytes(sizeBytes)}</span>
        {(extraActions || !hideDownload) && (
          <div className="ml-auto flex items-center gap-2">
            {extraActions}
            {!hideDownload && (
              <button
                onClick={handleDownload}
                disabled={downloadDisabled}
                className="text-[11px] px-1.5 py-0.5 rounded bg-slate-200 hover:bg-slate-300 text-slate-700 disabled:opacity-50 disabled:cursor-not-allowed"
                title={
                  downloadDisabled
                    ? 'Payload was elided by the server and no download endpoint is wired up'
                    : `Download ${downloadName}`
                }
              >
                Download
              </button>
            )}
          </div>
        )}
      </div>
      {tooLarge ? (
        <div className="p-2 bg-slate-100 border border-slate-200 rounded text-xs text-slate-500 italic">
          {elided ? (
            <>
              Server-elided: payload is {formatBytes(sizeBytes)}.
              {value.reason ? ` ${value.reason}.` : ''}
              {downloadURL || hideDownload ? largeHint : ''}
            </>
          ) : (
            <>Payload is {formatBytes(sizeBytes)} — too large to render inline.{largeHint}</>
          )}
        </div>
      ) : (
        <pre className="p-2 bg-slate-100 border border-slate-200 rounded text-xs text-slate-800 overflow-x-auto whitespace-pre-wrap">
          {yamlSerialized}
        </pre>
      )}
    </div>
  )
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / (1024 * 1024)).toFixed(1)} MB`
}

function Timestamp({ label, value }: { label: string; value?: string | null }) {
  return (
    <>
      <dt className="text-slate-500">{label}</dt>
      <dd className="text-slate-800 truncate">
        {value ? formatDateTime(value) : <span className="text-slate-400">—</span>}
      </dd>
    </>
  )
}
