import { useEffect, useRef, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { api, type FullResource, type ResourceInfo, type ResourceManifest } from '../api'

// Above this size a loaded manifest is held OUT of the editor (see
// loadManifest / oversizedText) rather than rendered — putting hundreds of
// KB of JSON into a <textarea> makes the browser unresponsive, and even one
// such frame is a visible flash before the size check hides it. The blob is
// still sent to the server intact on apply.
const LARGE_MANIFEST_BYTES = 300 * 1024

interface Props {
  open: boolean
  onClose: () => void
  onApplied: (r: FullResource) => void
  // When set, the modal prefills from an existing resource: the editor is
  // pre-populated with its current manifest (kind+name+labels+spec,
  // fetched on open since the listing omits spec). kind+name are the
  // resource's identity — a changed pair is rejected on apply; re-applying
  // overwrites spec+labels. An identical spec is a no-op (no gen bump).
  applyTo?: ResourceInfo | null
  // When the caller already knows the resource's spec is too large to
  // edit inline (the panel shows it server-elided), set this so the modal
  // SKIPS the prefill fetch entirely — pulling a multi-MB manifest just to
  // flip the editor to a "too big" notice is wasted work. The user
  // downloads, edits, and re-uploads via "Load from file" instead. Ignored
  // unless applyTo is set.
  prefillTooLarge?: boolean
}

// CreateResourceModal: load or paste a ResourceManifest JSON
// ({kind, name, labels, spec} + optional provider_config_ref), then Apply
// Manifest. Everything travels inside the manifest — there are no separate
// pickers — so a file downloaded from a resource re-applies as-is,
// including any attached custom provider config. Apply is a single
// create-or-update keyed by the manifest's (kind, name): a new pair
// creates the root and the cascade triggers schedule its composer
// immediately; an existing one overwrites its spec + labels. There's no
// separate create vs. update step, and the modal makes no assumptions
// about spec shape — it works for ANY registered kind.
export function CreateResourceModal({ open, onClose, onApplied, applyTo, prefillTooLarge }: Props) {
  const queryClient = useQueryClient()
  // Prefill mode: opened from an existing resource (kind+name locked).
  const prefill = !!applyTo
  // Prefill was skipped because the existing spec is too large to edit
  // inline (the panel already showed it server-elided). The editor stays
  // empty and we show a download-edit-reupload hint instead of pulling
  // megabytes only to refuse to render them.
  const skipPrefill = prefill && !!prefillTooLarge
  // The manifest JSON shown in the editor. This is the textarea-bound
  // value, so it only ever holds content small enough to render: a loaded
  // file that exceeds LARGE_MANIFEST_BYTES is NOT mirrored here (it lives
  // in oversizedText), which is what keeps a multi-hundred-KB blob from
  // ever flashing into the <textarea> before the size check hides it.
  const [manifestText, setManifestText] = useState('')
  // An oversized loaded manifest, held verbatim out of the editor. When
  // set, the editor is replaced by a "too large to edit inline" notice and
  // THIS is the blob sent to the server on apply. Mutually exclusive with a
  // non-empty manifestText: loading a file routes to exactly one of them.
  const [oversizedText, setOversizedText] = useState<string | undefined>()
  const [error, setError] = useState<string | undefined>()
  // Set when an apply was a no-op (identical spec → no generation bump).
  // Shown inline, kubectl-style ("unchanged"), keeping the modal open
  // rather than navigating away as if something happened.
  const [notice, setNotice] = useState<string | undefined>()
  const fileInputRef = useRef<HTMLInputElement>(null)
  // Guards against a rapid second file selection: only the latest
  // onFile() read is allowed to write state (see onFile).
  const fileReadToken = useRef(0)

  // loadManifest ingests a manifest blob (from a file or the prefill fetch)
  // and routes it to exactly ONE of the two states based on size — deciding
  // BEFORE anything reaches the textarea-bound state. This is what fixes the
  // flash: an oversized blob never enters manifestText, so the <textarea> is
  // never asked to render hundreds of KB for a frame before the size check
  // swaps it for the "too large" notice.
  //
  // Order matters for performance: check the RAW length first and, if it's
  // already over the limit, stash it verbatim WITHOUT parsing. Parsing +
  // pretty-printing a multi-MB blob just to discover it's too big to show
  // would jank the main thread — the exact stall this feature avoids. Only
  // when the raw text is small enough to edit do we pretty-print it (the raw
  // endpoint returns compact JSON; indenting makes the editor readable), and
  // re-check: indentation can push a near-limit blob over, in which case it
  // falls back to oversizedText. Only touches stable setState setters, so it
  // needs no memoization for the effect below.
  const loadManifest = (raw: string) => {
    if (raw.length > LARGE_MANIFEST_BYTES) {
      setOversizedText(raw)
      setManifestText('')
      return
    }
    let display = raw
    try {
      display = JSON.stringify(JSON.parse(raw), null, 2)
    } catch {
      // Not valid JSON (or not an object) — keep as-is; apply surfaces the
      // parse error.
    }
    if (display.length > LARGE_MANIFEST_BYTES) {
      setOversizedText(display)
      setManifestText('')
    } else {
      setManifestText(display)
      setOversizedText(undefined)
    }
  }

  // Reset state every time the modal opens. When prefilling, fetch the
  // resource's current manifest from the raw-bytes endpoint so the
  // editor shows the actual {kind,name,labels,spec} — UNLESS the spec is
  // already known to be too large to edit inline (skipPrefill), in which
  // case we leave the editor empty and don't pull the bytes at all.
  // Intentional sync from props on the open→true edge (plus an async
  // prefill load); the follow-up render is correct, so the
  // cascading-render rule is disabled for this deliberate case.
  /* eslint-disable react-hooks/set-state-in-effect */
  useEffect(() => {
    if (!open) return
    if (prefill && applyTo) {
      setManifestText('')
      setOversizedText(undefined)
      setError(undefined)
      setNotice(undefined)
      // Spec too large to edit inline: skip the fetch entirely. The render
      // shows a download-edit-reupload hint; nothing to load.
      if (skipPrefill) return
      let cancelled = false
      const manifestURL = `/api/v1/raw/resources/${encodeURIComponent(applyTo.kind)}/${encodeURIComponent(applyTo.name)}/manifest`
      ;(async () => {
        try {
          const res = await fetch(manifestURL)
          if (!res.ok) {
            throw new Error(`${res.status}: ${await res.text()}`)
          }
          const text = await res.text()
          if (!cancelled) loadManifest(text)
        } catch (err) {
          if (!cancelled) setError(`could not load resource: ${(err as Error).message}`)
        }
      })()
      return () => {
        cancelled = true
      }
    }
    setManifestText('')
    setOversizedText(undefined)
    setError(undefined)
    setNotice(undefined)
  }, [open, prefill, applyTo, skipPrefill])
  /* eslint-enable react-hooks/set-state-in-effect */

  const mutate = useMutation({
    mutationFn: async () => {
      // The blob to apply is whichever is active: the editable textarea
      // content, or — when too large to edit inline — the oversized blob
      // held verbatim out of the editor. Exactly one is ever non-empty.
      const source = oversizedText ?? manifestText
      // Parse the manifest document. Show a clean error if it parses badly.
      let parsed: unknown
      try {
        parsed = JSON.parse(source)
      } catch (err) {
        throw new Error(`invalid manifest JSON: ${(err as Error).message}`, { cause: err })
      }
      if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
        throw new Error('manifest must be a JSON object with kind, name, labels and spec')
      }
      const m = parsed as Record<string, unknown>

      // kind + name are the resource's identity and always required. In
      // prefill mode the loaded manifest already carries the (fixed) pair;
      // the upsert is keyed on it regardless of what's typed, so a changed
      // kind/name on an existing resource just wouldn't match — surface a
      // clear error instead of silently creating a different resource.
      const kind = typeof m.kind === 'string' ? m.kind.trim() : ''
      const name = typeof m.name === 'string' ? m.name.trim() : ''
      if (!kind) throw new Error('manifest.kind is required (a string)')
      if (!name) throw new Error('manifest.name is required (a string)')
      if (prefill && applyTo) {
        if (kind !== applyTo.kind) {
          throw new Error(`manifest.kind "${kind}" can't change; this resource is "${applyTo.kind}"`)
        }
        if (name !== applyTo.name) {
          throw new Error(`manifest.name "${name}" can't change; this resource is "${applyTo.name}"`)
        }
      }

      if (!('spec' in m)) throw new Error('manifest.spec is required')

      // kind_version: REQUIRED web-API version (v1, v2, … — any positive integer).
      // No implicit v1 default; the server rejects a missing/0 kind_version (422). On
      // an existing resource a DIFFERENT version is a breaking flip (handled server-side).
      if (
        m.kind_version === undefined ||
        m.kind_version === null ||
        typeof m.kind_version !== 'number' ||
        !Number.isInteger(m.kind_version) ||
        m.kind_version < 1
      ) {
        throw new Error('manifest.kind_version is required and must be a positive integer (v1, v2, …)')
      }
      const kindVersion = m.kind_version

      // Labels: optional, but if present must be a flat string→string map.
      // Apply overwrites the FULL set, so send exactly what's in the
      // manifest (absent ⇒ {}).
      let labels: Record<string, string> | undefined
      if (m.labels !== undefined && m.labels !== null) {
        const l = m.labels
        if (typeof l !== 'object' || Array.isArray(l)) {
          throw new Error('manifest.labels must be a JSON object of string values')
        }
        labels = {}
        for (const [k, v] of Object.entries(l as Record<string, unknown>)) {
          if (typeof v !== 'string') {
            throw new Error(`label "${k}" must be a string value`)
          }
          labels[k] = v
        }
      }

      // provider_config_ref: optional CUSTOM provider-config attachment.
      // It travels INSIDE the manifest (like kind/name/spec) so a
      // download→reapply round-trips it — the download endpoint emits it
      // only when one is attached. Validate the type if present and forward
      // it verbatim; the server resolves the name and returns 422 if it
      // doesn't exist. A non-empty string attaches; an explicit empty
      // string clears an existing attachment; absent ⇒ omit (no change to
      // the no-config common case).
      let providerConfigRef: string | undefined
      if (m.provider_config_ref !== undefined && m.provider_config_ref !== null) {
        if (typeof m.provider_config_ref !== 'string') {
          throw new Error('manifest.provider_config_ref must be a string (a providerconfig name)')
        }
        providerConfigRef = m.provider_config_ref.trim()
      }

      const manifest: ResourceManifest = { kind, kind_version: kindVersion, name, spec: m.spec, labels }
      if (providerConfigRef !== undefined) {
        manifest.provider_config_ref = providerConfigRef
      }
      return api.applyManifest(manifest)
    },
    onSuccess: ({ resource: r, result }) => {
      // No-op apply (identical spec → no generation bump): report it
      // kubectl-style and keep the modal open. Nothing changed, so don't
      // refresh anything or navigate away.
      if (result === 'unchanged') {
        setNotice(`${r.kind}/${r.name} unchanged — spec is identical, no new generation.`)
        return
      }
      // Mark the rendered lists stale so a freshly-applied resource shows
      // up on their next poll — the resource table (paginated, polls every
      // 10s) and the owner-picker search. Both fetch from paginated
      // endpoints; there's no full-roots cache to patch. refetchType:'none'
      // avoids an immediate refetch that could race a read replica's lag.
      queryClient.invalidateQueries({ queryKey: ['resources-page'], refetchType: 'none' })
      queryClient.invalidateQueries({ queryKey: ['roots-search'], refetchType: 'none' })
      onApplied(r)
    },
    onError: (err: Error) => setError(err.message),
  })

  // Esc to close (but not while a save is in flight — would lose data).
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !mutate.isPending) onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onClose, mutate.isPending])

  const onFile = async (file: File) => {
    setError(undefined)
    // Clear any stale "unchanged" notice from a prior apply — the freshly
    // loaded file is new content, so the old result no longer applies.
    setNotice(undefined)
    // Monotonic token: if the user picks another file while this read is
    // in flight, a newer onFile bumps the token; this (now-stale) call
    // then drops its result instead of clobbering the newer file's.
    const myToken = ++fileReadToken.current
    try {
      const text = await file.text()
      if (myToken !== fileReadToken.current) return
      loadManifest(text)
    } catch (err) {
      if (myToken !== fileReadToken.current) return
      setError(`could not read file: ${(err as Error).message}`)
    }
  }

  if (!open) return null

  return (
    <div
      className="fixed inset-0 bg-slate-900/40 backdrop-blur-sm flex items-center justify-center z-50"
      onClick={() => {
        // Don't close on backdrop click while saving — would lose work.
        if (!mutate.isPending) onClose()
      }}
      role="presentation"
    >
      <div
        className="bg-white border border-slate-200 rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="resource-modal-title"
      >
        <div className="flex items-center justify-between p-4 border-b border-slate-200">
          <h2 id="resource-modal-title" className="text-base font-semibold text-slate-900">
            {prefill ? `Apply manifest: ${applyTo?.name}` : 'Apply manifest'}
          </h2>
          <button
            onClick={onClose}
            disabled={mutate.isPending}
            className="text-slate-400 hover:text-slate-700 text-2xl leading-none disabled:opacity-40 disabled:cursor-not-allowed"
            aria-label="Close"
          >
            &times;
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-4 space-y-4">
          <div>
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wider block mb-1">
              Manifest (JSON)
            </label>
            <div className="flex items-center gap-2 mb-2">
              <button
                onClick={() => fileInputRef.current?.click()}
                className="text-xs px-2 py-1 rounded bg-slate-200 hover:bg-slate-300 text-slate-700"
              >
                Load from file…
              </button>
              <input
                ref={fileInputRef}
                type="file"
                accept=".json,application/json"
                className="hidden"
                onChange={(e) => {
                  const f = e.target.files?.[0]
                  if (f) void onFile(f)
                  e.target.value = '' // allow re-loading the same file
                }}
              />
              {(manifestText || oversizedText) && (
                <>
                  <span className="text-xs text-slate-500">
                    {((oversizedText ?? manifestText).length / 1024).toFixed(1)} KB
                  </span>
                  <button
                    onClick={() => {
                      setManifestText('')
                      setOversizedText(undefined)
                    }}
                    className="ml-auto text-xs text-slate-500 hover:text-slate-800"
                    title="Clear and load a different file"
                  >
                    clear
                  </button>
                </>
              )}
            </div>
            {skipPrefill && !manifestText && !oversizedText ? (
              <div className="w-full bg-slate-50 border border-slate-300 rounded p-3 text-sm text-slate-600">
                <p className="font-medium text-slate-700 mb-1">
                  Manifest too large to edit inline
                </p>
                <p className="text-xs">
                  This resource's spec is too big to load into the editor. Use{' '}
                  <strong>Download</strong> on the Spec panel to save the current manifest,
                  edit it locally, then <strong>Load from file…</strong> above to apply it.
                </p>
              </div>
            ) : oversizedText ? (
              <div className="w-full bg-slate-50 border border-slate-300 rounded p-3 text-sm text-slate-600">
                <p className="font-medium text-slate-700 mb-1">
                  Manifest loaded ({(oversizedText.length / 1024).toFixed(1)} KB)
                </p>
                <p className="text-xs">
                  Preview hidden — rendering JSON above {LARGE_MANIFEST_BYTES / 1024} KB in a textarea
                  makes the browser unresponsive. The content will be sent as-is when you click{' '}
                  <strong>Apply manifest</strong>. Use <strong>clear</strong> to load a different file.
                </p>
              </div>
            ) : (
              <textarea
                value={manifestText}
                onChange={(e) => {
                  setManifestText(e.target.value)
                  // Editing toward a new apply — drop a stale "unchanged".
                  if (notice) setNotice(undefined)
                }}
                placeholder={'{\n  "kind": "a-kind",\n  "kind_version": 1,\n  "name": "my-resource",\n  "labels": {},\n  "spec": {}\n}'}
                rows={16}
                className="w-full bg-slate-50 border border-slate-300 px-2 py-1.5 rounded font-mono text-xs text-slate-800 placeholder-slate-400 outline-none focus:ring-1 focus:ring-blue-500"
              />
            )}
            <p className="text-xs text-slate-500 mt-1">
              A full manifest: <span className="font-mono">{`{ "kind", "name", "labels", "spec" }`}</span>.
              Labels apply as the complete set (clearing a key removes it). Add an optional{' '}
              <span className="font-mono">"provider_config_ref"</span> (a providerconfig name) to
              attach a custom config override.
            </p>
          </div>

          {prefill && (
            <p className="text-xs text-slate-500 italic">
              Owned resources missing from the new spec will be flagged with{' '}
              <span className="font-mono text-rose-700">deletion_requested_at</span> and run their
              finalizer drain. Cloud-side cleanup is the deleter's responsibility.
            </p>
          )}

          {notice && (
            <div className="p-2 bg-slate-50 border border-slate-200 rounded text-sm text-slate-600">
              {notice}
            </div>
          )}

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
              setNotice(undefined)
              mutate.mutate()
            }}
            disabled={mutate.isPending || !(oversizedText ?? manifestText).trim()}
            className="text-sm px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {mutate.isPending ? 'Applying…' : 'Apply manifest'}
          </button>
        </div>
      </div>
    </div>
  )
}
