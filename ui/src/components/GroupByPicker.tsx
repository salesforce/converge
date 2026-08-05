import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api'

// GroupByPicker is a reusable chip-style editor for an ordered list of
// label keys. Click "edit" to enter edit mode; reorder chips by drag,
// remove via × button, add via the dropdown of label keys actually
// present on the owner. Used in both Topology (graph) and any future
// hierarchy view.
//
// Drag uses native HTML5 drag-and-drop — minimal, no extra deps.

interface Props {
  ownerKind: string
  ownerName: string
  value: string[]
  onChange: (next: string[]) => void
}

export function GroupByPicker({ ownerKind, ownerName, value, onChange }: Props) {
  const [editing, setEditing] = useState(false)
  const { data: keysResp } = useQuery({
    queryKey: ['topology-keys', `${ownerKind}/${ownerName}`],
    queryFn: () => api.getTopologyKeys(ownerKind, ownerName),
    staleTime: 60_000,
  })
  const allKeys = useMemo(() => keysResp?.keys ?? [], [keysResp])

  if (!editing) {
    return (
      <div className="flex items-center gap-2 text-sm">
        <span className="text-xs text-slate-500 uppercase tracking-wider">Group by</span>
        {value.map((k) => (
          <span key={k} className="px-2 py-0.5 rounded bg-slate-100 text-slate-700 font-mono text-xs">
            {k}
          </span>
        ))}
        <button onClick={() => setEditing(true)} className="ml-2 text-xs text-blue-600 hover:underline">
          edit
        </button>
      </div>
    )
  }

  return (
    <EditMode
      value={value}
      allKeys={allKeys}
      onChange={onChange}
      onDone={() => setEditing(false)}
    />
  )
}

function EditMode({
  value,
  allKeys,
  onChange,
  onDone,
}: {
  value: string[]
  allKeys: string[]
  onChange: (next: string[]) => void
  onDone: () => void
}) {
  // Track which chip is being dragged so we can swap it on drop.
  const [dragFrom, setDragFrom] = useState<number | null>(null)
  const [dragOver, setDragOver] = useState<number | null>(null)

  const remove = (i: number) => onChange(value.filter((_, j) => j !== i))
  const add = (k: string) => {
    if (!value.includes(k)) onChange([...value, k])
  }
  const move = (from: number, to: number) => {
    if (from === to) return
    const next = value.slice()
    const [item] = next.splice(from, 1)
    next.splice(to, 0, item)
    onChange(next)
  }

  return (
    <div className="flex flex-wrap items-center gap-2 text-sm p-2 rounded bg-slate-50 border border-slate-200">
      <span className="text-xs text-slate-500 uppercase tracking-wider">Group by</span>
      {value.map((k, i) => {
        const isDragging = dragFrom === i
        const isOver = dragOver === i && dragFrom !== null && dragFrom !== i
        return (
          <span
            key={k}
            draggable
            onDragStart={(e) => {
              setDragFrom(i)
              e.dataTransfer.effectAllowed = 'move'
              // Required by Firefox to actually start the drag
              e.dataTransfer.setData('text/plain', k)
            }}
            onDragOver={(e) => {
              e.preventDefault()
              e.dataTransfer.dropEffect = 'move'
              if (dragOver !== i) setDragOver(i)
            }}
            onDragLeave={() => {
              if (dragOver === i) setDragOver(null)
            }}
            onDrop={(e) => {
              e.preventDefault()
              if (dragFrom !== null) move(dragFrom, i)
              setDragFrom(null)
              setDragOver(null)
            }}
            onDragEnd={() => {
              setDragFrom(null)
              setDragOver(null)
            }}
            className={`flex items-center gap-1 px-2 py-0.5 rounded border font-mono text-xs cursor-grab active:cursor-grabbing select-none transition-colors ${
              isDragging
                ? 'opacity-40 border-blue-400 bg-white'
                : isOver
                ? 'border-blue-500 bg-blue-50 text-blue-900'
                : 'bg-white border-slate-200 text-slate-700'
            }`}
            title="drag to reorder"
          >
            <span className="text-slate-400 leading-none">⋮⋮</span>
            {k}
            <button
              onClick={() => remove(i)}
              className="text-slate-400 hover:text-slate-700"
              aria-label={`remove ${k}`}
            >
              ×
            </button>
          </span>
        )
      })}
      <select
        className="text-xs px-2 py-0.5 border border-slate-200 rounded bg-white"
        defaultValue=""
        onChange={(e) => {
          if (e.target.value) {
            add(e.target.value)
            e.currentTarget.value = ''
          }
        }}
      >
        <option value="" disabled>
          + add key
        </option>
        {allKeys
          .filter((k) => !value.includes(k))
          .map((k) => (
            <option key={k} value={k}>
              {k}
            </option>
          ))}
      </select>
      <button onClick={onDone} className="ml-auto text-xs text-blue-600 hover:underline">
        done
      </button>
    </div>
  )
}
