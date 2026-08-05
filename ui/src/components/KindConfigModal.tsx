import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type KindOperational } from '../api'

interface Props {
  open: boolean
  kind: string
  /** Current operational config (undefined ⇒ uncapped, no deadline, no resync — the form starts empty). */
  current?: KindOperational
  /** The kind's published web-API versions (ascending); enables the per-version selector. Defaults to [1]. */
  kindVersions?: number[]
  onClose: () => void
  onApplied?: () => void
}

// secondsToText / textToSeconds render an interval (resync or task deadline) as
// a friendly duration string ("10s", "1h") while the wire format stays whole
// seconds. Accepts a bare number (seconds) or a Go-style duration suffix s/m/h.
function secondsToText(secs: number): string {
  if (secs <= 0) return ''
  if (secs % 3600 === 0) return `${secs / 3600}h`
  if (secs % 60 === 0) return `${secs / 60}m`
  return `${secs}s`
}
function textToSeconds(t: string): number | null {
  const s = t.trim()
  if (s === '') return 0
  const m = s.match(/^(\d+)\s*([smh]?)$/i)
  if (!m) return null
  const n = parseInt(m[1], 10)
  switch (m[2].toLowerCase()) {
    case 'h':
      return n * 3600
    case 'm':
      return n * 60
    default:
      return n // bare number or 's' = seconds
  }
}

// KindConfigModal edits a (kind, kind_version)'s OPERATIONAL config — the
// concurrency cap, per-task deadline, drift-resync policy, orphan-grace, the
// transient dead-letter cap, and the RETIRED flag (the kind_config row), distinct
// from its config DOCUMENT. The cap/deadline change is LIVE (next claim); resync
// applies on the control plane's next refresh; retired freezes new creates onto
// the version at once. When the kind serves more than one web-API version the
// header carries a selector so each version's config is edited independently.
//
// This shell is the open-gate + version selector: it fetches the SELECTED
// version's config (the `current` prop only carries the representative/v1 block)
// and remounts KindConfigForm keyed on version so the form reseeds per version
// (no reset-on-open effect inside the form).
export function KindConfigModal({ open, kind, current, kindVersions, onClose, onApplied }: Props) {
  const versions = kindVersions && kindVersions.length ? kindVersions : [1]
  const [version, setVersion] = useState(() => versions[0])
  // Fetch the selected version's operational config. v1 (or the representative)
  // is already in `current`, so skip the fetch for it; v2+ pulls its own block so
  // a per-version edit starts from that version's real values, not v1's.
  const { data } = useQuery({
    queryKey: ['kind-schema-config', kind, version],
    queryFn: () => api.getKindSchema(kind, version),
    enabled: open && version > 1,
    staleTime: 30_000,
  })
  if (!open) return null
  const seed = version > 1 ? data?.operational : current
  return (
    <KindConfigForm
      // Re-key on (kind, version) so switching either reseeds the form from that
      // version's config.
      key={`${kind}:${version}`}
      kind={kind}
      version={version}
      versions={versions}
      onVersionChange={setVersion}
      current={seed}
      onClose={onClose}
      onApplied={onApplied}
    />
  )
}

// KindConfigForm is the mounted editor. A structured form (not a JSON editor)
// since these are typed knobs. State is seeded once from `current` via useState
// initializers — the form only lives while open (the shell gates that), so there
// is no reset effect.
function KindConfigForm({
  kind,
  version,
  versions,
  onVersionChange,
  current,
  onClose,
  onApplied,
}: Omit<Props, 'open' | 'kindVersions'> & {
  version: number
  versions: number[]
  onVersionChange: (v: number) => void
}) {
  const queryClient = useQueryClient()
  const [maxInflight, setMaxInflight] = useState(() =>
    current && current.max_inflight > 0 ? String(current.max_inflight) : '',
  )
  const [deadline, setDeadline] = useState(() =>
    current ? secondsToText(current.task_deadline_seconds) : '',
  )
  const [resync, setResync] = useState(() =>
    current ? secondsToText(current.resync_interval_seconds) : '',
  )
  const [recompose, setRecompose] = useState(() => current?.resync_recomposes ?? false)
  const [orphanGrace, setOrphanGrace] = useState(() =>
    current ? secondsToText(current.orphan_grace_seconds) : '',
  )
  const [maxTransient, setMaxTransient] = useState(() =>
    current && current.max_transient_attempts > 0 ? String(current.max_transient_attempts) : '',
  )
  const [retired, setRetired] = useState(() => current?.retired ?? false)
  const [error, setError] = useState<string>()

  const mutate = useMutation({
    mutationFn: () => {
      const cap = maxInflight.trim() === '' ? 0 : Number(maxInflight)
      if (!Number.isInteger(cap) || cap < 0) {
        throw new Error('Max in-flight must be a non-negative whole number (blank or 0 = uncapped).')
      }
      const deadlineSecs = textToSeconds(deadline)
      if (deadlineSecs === null) {
        throw new Error('Task deadline must be like 30, 30s, 10m, or 1h (blank or 0 = no deadline).')
      }
      const secs = textToSeconds(resync)
      if (secs === null) {
        throw new Error('Resync interval must be like 30, 30s, 10m, or 1h (blank or 0 = disabled).')
      }
      const graceSecs = textToSeconds(orphanGrace)
      if (graceSecs === null) {
        throw new Error('Orphan grace must be like 30, 30s, 10m, or 1h (blank or 0 = prune immediately).')
      }
      const maxTransientN = maxTransient.trim() === '' ? 0 : Number(maxTransient)
      if (!Number.isInteger(maxTransientN) || maxTransientN < 0) {
        throw new Error('Max transient attempts must be a non-negative whole number (blank or 0 = unbounded / retry forever).')
      }
      return api.applyKindConfig(
        kind,
        {
          max_inflight: cap,
          task_deadline_seconds: deadlineSecs,
          resync_interval_seconds: secs,
          resync_recomposes: secs > 0 && recompose,
          orphan_grace_seconds: graceSecs,
          max_transient_attempts: maxTransientN,
          retired,
        },
        version,
      )
    },
    onSuccess: () => {
      // Refresh the Kinds page list + this kind's detail so the new
      // cap/resync/retired render. ['kind-schemas'] is the KindsPage list key
      // (NOT ['kinds'], the string[] kind list used elsewhere).
      queryClient.invalidateQueries({ queryKey: ['kind-schemas'] })
      queryClient.invalidateQueries({ queryKey: ['kind', kind] })
      queryClient.invalidateQueries({ queryKey: ['kind-schema-config', kind] })
      onApplied?.()
      onClose()
    },
    onError: (err: Error) => setError(err.message),
  })

  // Esc to close (not mid-save).
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !mutate.isPending) onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose, mutate.isPending])

  const resyncOff = textToSeconds(resync) === 0

  return (
    <div
      className="fixed inset-0 bg-slate-900/40 backdrop-blur-sm flex items-center justify-center z-50"
      onClick={() => {
        if (!mutate.isPending) onClose()
      }}
      role="presentation"
    >
      <div
        className="bg-white border border-slate-200 rounded-lg shadow-xl w-full max-w-md flex flex-col"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="kind-config-title"
      >
        <div className="flex items-center justify-between p-4 border-b border-slate-200">
          <h2 id="kind-config-title" className="text-base font-semibold text-slate-900 flex items-center gap-2">
            Operational config: <span className="font-mono">{kind}</span>
            {/* Per-version selector — only when the kind serves >1 web-API version.
                Switching it fetches + reseeds the form from THAT version's config. */}
            {versions.length > 1 && (
              <select
                value={version}
                onChange={(e) => onVersionChange(Number(e.target.value))}
                disabled={mutate.isPending}
                className="px-1.5 py-0.5 text-xs border border-slate-300 rounded bg-white text-slate-700 font-mono focus:outline-none focus:ring-1 focus:ring-blue-400 disabled:opacity-50"
                title="Edit the operational config for this web-API version"
              >
                {versions.map((v) => (
                  <option key={v} value={v}>
                    v{v}
                  </option>
                ))}
              </select>
            )}
          </h2>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-700 text-2xl leading-none"
            aria-label="Close"
          >
            &times;
          </button>
        </div>

        <div className="p-4 space-y-4">
          <div>
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block mb-1">
              Max in-flight (global cap)
            </label>
            <input
              type="number"
              min={0}
              value={maxInflight}
              onChange={(e) => setMaxInflight(e.target.value)}
              placeholder="blank = uncapped"
              className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
            />
            <p className="text-xs text-slate-500 mt-1">
              Tasks of this kind in flight at once, summed across all worker pods. Blank or{' '}
              <span className="font-mono">0</span> = uncapped. Takes effect on the next claim — no
              restart.
            </p>
          </div>

          <div>
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block mb-1">
              Task deadline
            </label>
            <input
              type="text"
              value={deadline}
              onChange={(e) => setDeadline(e.target.value)}
              placeholder="blank = no deadline (e.g. 30s, 10m, 1h)"
              className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
            />
            <p className="text-xs text-slate-500 mt-1">
              Per-task timeout — a task of this kind that runs longer is cancelled and recorded as a
              transient failure (retried). Blank or <span className="font-mono">0</span> = no
              deadline. Takes effect on the next claim — no restart.
            </p>
          </div>

          <div>
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block mb-1">
              Resync interval
            </label>
            <input
              type="text"
              value={resync}
              onChange={(e) => setResync(e.target.value)}
              placeholder="blank = disabled (e.g. 30s, 10m, 1h)"
              className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
            />
            <p className="text-xs text-slate-500 mt-1">
              How often settled resources are re-checked for drift. Blank or{' '}
              <span className="font-mono">0</span> = disabled. Re-tuning or disabling applies on the
              control plane's next refresh (no restart). <span className="text-amber-700">Enabling
              resync for the first time on a cluster where no kind currently resyncs needs a control
              restart.</span>
            </p>
          </div>

          <label
            className={`flex items-center gap-2 text-sm ${resyncOff ? 'text-slate-400' : 'text-slate-700'}`}
          >
            <input
              type="checkbox"
              checked={recompose}
              disabled={resyncOff}
              onChange={(e) => setRecompose(e.target.checked)}
            />
            Resync also re-runs the composer (drift-correct composed children)
          </label>

          <div>
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block mb-1">
              Orphan grace
            </label>
            <input
              type="text"
              value={orphanGrace}
              onChange={(e) => setOrphanGrace(e.target.value)}
              placeholder="blank = prune immediately (e.g. 30s, 10m, 1h)"
              className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
            />
            <p className="text-xs text-slate-500 mt-1">
              When a composer stops producing a child of this kind, how long it lingers in the{' '}
              <span className="font-mono">Orphaned</span> phase before teardown — a re-emit within
              the window re-adopts it untouched (guards against a buggy composer that drops a child
              by mistake). Blank or <span className="font-mono">0</span> = prune immediately. Applies
              on the composer's next run.
            </p>
          </div>

          <div>
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block mb-1">
              Max transient attempts
            </label>
            <input
              type="text"
              value={maxTransient}
              onChange={(e) => setMaxTransient(e.target.value)}
              placeholder="blank = unbounded (retry forever)"
              className="w-full px-2 py-1 text-sm border border-slate-300 rounded outline-none focus:ring-1 focus:ring-blue-400"
            />
            <p className="text-xs text-slate-500 mt-1">
              After this many consecutive <em>transient</em> reconcile failures, a resource is
              dead-lettered (marked <span className="font-mono">Failed</span>/terminal) so a
              persistently-broken handler stops retrying every few seconds forever. Recover by editing
              its spec or forcing a reconcile. Blank or <span className="font-mono">0</span> = unbounded
              (retry forever). Takes effect on the next drain — no restart.
            </p>
          </div>

          <div className="border-t border-slate-200 pt-3">
            <label className="flex items-center gap-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={retired}
                onChange={(e) => setRetired(e.target.checked)}
              />
              Retire this version (<span className="font-mono">v{version}</span>)
            </label>
            <p className="text-xs text-slate-500 mt-1">
              Freezes <em>new</em> creates and version-flips onto{' '}
              <span className="font-mono">
                {kind}/v{version}
              </span>{' '}
              (they’re rejected 422), while <em>existing</em> resources keep reconciling so they can
              drain or be migrated onto a live version. Not a teardown — no resource is deleted. Once
              the version’s population reaches zero it shows as <em>drained</em> in the Versions panel
              and its config/manifest rows can be dropped. Takes effect immediately.
            </p>
          </div>

          {error && (
            <div className="p-2 bg-red-50 border border-red-200 rounded text-sm text-red-800">
              {error}
            </div>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 p-4 border-t border-slate-200 bg-slate-50">
          <button
            onClick={onClose}
            disabled={mutate.isPending}
            className="text-sm px-3 py-1.5 rounded bg-white border border-slate-300 hover:bg-slate-100 text-slate-700 disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            onClick={() => {
              setError(undefined)
              mutate.mutate()
            }}
            disabled={mutate.isPending}
            className="text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {mutate.isPending ? 'Saving…' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  )
}
