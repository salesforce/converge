export function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return ''
  const d = new Date(iso)
  const mon = d.toLocaleString(undefined, { month: 'short' })
  const day = d.getDate()
  const h = d.getHours().toString().padStart(2, '0')
  const m = d.getMinutes().toString().padStart(2, '0')
  const s = d.getSeconds().toString().padStart(2, '0')
  return `${mon} ${day}, ${h}:${m}:${s}`
}
