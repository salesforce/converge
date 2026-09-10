import { useEffect } from 'react'

interface Props {
  open: boolean
  // Short subject line, e.g. "vm/payments-db". Rendered mono in the prompt.
  target: string
  // Optional extra explanation shown under the prompt (e.g. soft-delete note).
  detail?: string
  busy: boolean
  errorMessage?: string
  onConfirm: () => void
  onClose: () => void
}

// ConfirmDeleteModal: a small, centered confirmation dialog for a destructive
// action — a focused overlay instead of an inline confirm that would reflow the
// surrounding controls. Matches the app's modal chrome (backdrop, rounded card,
// Esc/backdrop close) but sized for a one-line decision. Backdrop/Esc close is
// disabled while the delete is in flight so a mid-request click can't strand the
// dialog. Confirm is a red primary; Cancel is the neutral secondary.
export function ConfirmDeleteModal({
  open,
  target,
  detail,
  busy,
  errorMessage,
  onConfirm,
  onClose,
}: Props) {
  // Esc closes (but not mid-request). Enter confirms — a delete dialog is a
  // single decision, so Enter defaulting to the primary action is expected.
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (busy) return
      if (e.key === 'Escape') onClose()
      if (e.key === 'Enter') onConfirm()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, busy, onClose, onConfirm])

  if (!open) return null

  return (
    <div
      className="fixed inset-0 bg-slate-900/40 backdrop-blur-sm flex items-center justify-center z-50"
      onClick={() => {
        if (!busy) onClose()
      }}
      role="presentation"
    >
      <div
        className="bg-white border border-slate-200 rounded-lg shadow-xl w-full max-w-md flex flex-col"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-labelledby="confirm-delete-title"
      >
        <div className="flex items-center justify-between p-4 border-b border-slate-200">
          <h2 id="confirm-delete-title" className="text-base font-semibold text-slate-900">
            Delete resource
          </h2>
          <button
            onClick={onClose}
            disabled={busy}
            className="text-slate-400 hover:text-slate-700 text-2xl leading-none disabled:opacity-40 disabled:cursor-not-allowed"
            aria-label="Close"
          >
            &times;
          </button>
        </div>

        <div className="p-4 space-y-2">
          <p className="text-sm text-slate-700">
            Delete <span className="font-mono text-slate-900">{target}</span>?
          </p>
          {detail && <p className="text-xs text-slate-500">{detail}</p>}
          {errorMessage && (
            <p className="text-xs text-red-600 font-mono break-all">{errorMessage}</p>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 p-4 border-t border-slate-200">
          <button
            onClick={onClose}
            disabled={busy}
            className="text-sm px-3 py-1.5 rounded bg-white border border-slate-300 hover:bg-slate-100 text-slate-700 disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={busy}
            className="text-sm px-3 py-1.5 rounded bg-red-600 hover:bg-red-500 text-white disabled:opacity-50"
          >
            {busy ? 'Deleting…' : 'Delete'}
          </button>
        </div>
      </div>
    </div>
  )
}
