import { useState, type ReactNode } from 'react'
import { Routes, Route, Navigate, NavLink, useLocation } from 'react-router-dom'
import { ResourcesPage } from './pages/ResourcesPage'
import { ClusterPage } from './pages/ClusterPage'
import { ProviderConfigsPage } from './pages/ProviderConfigsPage'
import { ReactorBindingsPage } from './pages/ReactorBindingsPage'
import { KindsPage } from './pages/KindsPage'
import { ErrorBoundary } from './components/ErrorBoundary'

// NAV_COLLAPSED_KEY persists the sidebar's collapsed/expanded choice across
// reloads. Stored as the string "1" (collapsed) / "0" (expanded).
const NAV_COLLAPSED_KEY = 'converge:nav-collapsed'

// App is the shell: a collapsible nav sidebar on the left, then routes.
//
// Single-page model: everything lives on /resources. Roots and their
// owned children render together in one list, grouped by owner.
// Filtering by owner (a root's kind/name) is a chip in the FilterBar.
export default function App() {
  // Sidebar collapsed state, seeded from localStorage so the choice sticks
  // across reloads. Collapsed = icons-only rail; expanded = icons + labels.
  const [collapsed, setCollapsed] = useState<boolean>(
    () => localStorage.getItem(NAV_COLLAPSED_KEY) === '1',
  )
  const toggleNav = () => {
    setCollapsed((c) => {
      const next = !c
      localStorage.setItem(NAV_COLLAPSED_KEY, next ? '1' : '0')
      return next
    })
  }

  // Re-key the ErrorBoundary on the route's top segment so navigating to a
  // different page clears a prior page's caught error and re-mounts a fresh
  // tree (a blanked page recovers by switching tabs, not just a reload).
  const { pathname } = useLocation()
  const section = '/' + (pathname.split('/')[1] ?? '')
  return (
    <div className="flex h-screen bg-slate-50 text-slate-900">
      <SideNav collapsed={collapsed} onToggle={toggleNav} />
      <ErrorBoundary key={section}>
        <Routes>
          <Route path="/" element={<Navigate to="/resources/summary" replace />} />
          <Route path="/resources" element={<Navigate to="/resources/summary" replace />} />
          <Route path="/resources/:view" element={<ResourcesPage />} />
          <Route path="/resources/:view/r/:kind/:name" element={<ResourcesPage />} />
          <Route path="/providerconfigs" element={<ProviderConfigsPage />} />
          <Route path="/providerconfigs/:name" element={<ProviderConfigsPage />} />
          <Route path="/reactor-bindings" element={<ReactorBindingsPage />} />
          <Route path="/kinds" element={<KindsPage />} />
          <Route path="/cluster" element={<ClusterPage />} />
          <Route path="*" element={<NotFound />} />
        </Routes>
      </ErrorBoundary>
    </div>
  )
}

// itemClass styles a nav item from its NavLink active state: the matched route
// gets the blue "active" treatment (and NavLink auto-sets aria-current="page");
// the rest stay muted. Exactly one item is lit at a time. The layout adapts to
// the collapsed state — a centered icon square when collapsed, an icon + label
// row when expanded.
function itemClass(collapsed: boolean) {
  return ({ isActive }: { isActive: boolean }): string => {
    const shape = collapsed
      ? 'w-10 h-10 justify-center'
      : 'w-full h-9 px-2.5 gap-3 justify-start'
    return `${shape} rounded flex items-center text-sm ${
      isActive
        ? 'bg-blue-50 text-blue-700 hover:bg-blue-100'
        : 'text-slate-500 hover:bg-slate-100 hover:text-slate-800'
    }`
  }
}

// NavItem is one sidebar entry — icon always, label only when expanded. The
// label is hidden (not just visually) when collapsed so the tooltip (title)
// carries the name instead. `end` scopes the active match to an exact path.
function NavItem({
  to,
  icon,
  label,
  collapsed,
  end,
}: {
  to: string
  icon: ReactNode
  label: string
  collapsed: boolean
  end?: boolean
}) {
  return (
    <NavLink
      to={to}
      end={end}
      className={itemClass(collapsed)}
      title={collapsed ? label : undefined}
      aria-label={label}
    >
      <span className="shrink-0">{icon}</span>
      {!collapsed && <span className="truncate">{label}</span>}
    </NavLink>
  )
}

// SideNav is the primary navigation: a collapsible left sidebar. Collapsed, it
// is a thin icons-only rail; expanded, it shows a label beside each icon. The
// collapsed/expanded choice is owned by App and persisted to localStorage, so
// it sticks across reloads. A toggle at the bottom flips it.
function SideNav({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
  return (
    <nav
      className={`${
        collapsed ? 'w-12 items-center' : 'w-52'
      } shrink-0 border-r border-slate-200 bg-white flex flex-col py-3 px-2 gap-1 transition-[width] duration-150`}
      aria-label="Primary"
    >
      {/* Brand — a rounded-square "C" (blue gradient), the same identity as the
          docs feature slide. Links home; the wordmark shows only when expanded. */}
      <NavLink
        to="/resources"
        className={`flex items-center ${collapsed ? 'justify-center' : 'gap-2.5 px-1'} mb-1`}
        title="Converge — home"
        aria-label="Converge — home"
      >
        <span className="w-8 h-8 shrink-0 rounded-lg flex items-center justify-center text-white font-extrabold text-sm bg-gradient-to-br from-blue-600 to-blue-500 shadow-sm">
          C
        </span>
        {!collapsed && <span className="font-semibold text-slate-800">Converge</span>}
      </NavLink>
      <div className={`${collapsed ? 'w-6 mx-auto' : 'w-full'} border-t border-slate-200 mb-1`} aria-hidden="true" />

      {/* Resources — home. Matches the whole /resources/* subtree (summary,
          topology, a selected resource…) so it stays lit while drilling in. */}
      <NavItem to="/resources" icon={<ListIcon />} label="Resources" collapsed={collapsed} />
      {/* Cluster view — the running-fleet registry of members ("like kubectl
          get nodes"). Cluster-wide, not resource-scoped. Matches /cluster only. */}
      <NavItem to="/cluster" icon={<ServerIcon />} label="Cluster" collapsed={collapsed} />
      {/* Provider configs — the runtime-editable per-kind config store. */}
      <NavItem to="/providerconfigs" icon={<SlidersIcon />} label="Provider configs" collapsed={collapsed} />
      {/* Reactor bindings — the runtime-editable reactor subscriptions ("when
          a kind crosses a transition, run a reactor"). */}
      <NavItem to="/reactor-bindings" icon={<ZapIcon />} label="Reactor bindings" collapsed={collapsed} />
      {/* Kinds — every declared kind + its operational config (concurrency cap
          + drift-resync, the kind_config table), runtime-editable. */}
      <NavItem to="/kinds" icon={<TagIcon />} label="Kinds" collapsed={collapsed} />

      {/* API docs link — opens huma's built-in Stoplight Elements page at /docs
          in a new tab. Served by the Go binary directly, so it's available
          wherever the Converge HTTP server runs. A plain link, not a NavLink
          (external target, never "active"). */}
      <a
        href="/docs"
        target="_blank"
        rel="noopener noreferrer"
        className={`${
          collapsed ? 'w-10 h-10 justify-center' : 'w-full h-9 px-2.5 gap-3 justify-start'
        } rounded flex items-center text-sm text-slate-500 hover:bg-slate-100 hover:text-slate-800`}
        title={collapsed ? 'API docs' : undefined}
        aria-label="API docs"
      >
        <span className="shrink-0"><BookIcon /></span>
        {!collapsed && <span className="truncate">API docs</span>}
      </a>

      {/* Collapse/expand toggle, pinned to the bottom. The chevron points the
          way it will move the edge (‹ to collapse, › to expand). */}
      <button
        onClick={onToggle}
        className={`mt-auto ${
          collapsed ? 'w-10 h-10 justify-center' : 'w-full h-9 px-2.5 gap-3 justify-start'
        } rounded flex items-center text-sm text-slate-400 hover:bg-slate-100 hover:text-slate-700`}
        title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        aria-expanded={!collapsed}
      >
        <span className="shrink-0"><ChevronIcon dir={collapsed ? 'right' : 'left'} /></span>
        {!collapsed && <span className="truncate">Collapse</span>}
      </button>
    </nav>
  )
}

function ListIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <line x1="8" y1="6" x2="21" y2="6" />
      <line x1="8" y1="12" x2="21" y2="12" />
      <line x1="8" y1="18" x2="21" y2="18" />
      <circle cx="3.5" cy="6" r="1.5" />
      <circle cx="3.5" cy="12" r="1.5" />
      <circle cx="3.5" cy="18" r="1.5" />
    </svg>
  )
}

function ServerIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <rect x="3" y="4" width="18" height="6" rx="1" />
      <rect x="3" y="14" width="18" height="6" rx="1" />
      <line x1="7" y1="7" x2="7.01" y2="7" />
      <line x1="7" y1="17" x2="7.01" y2="17" />
    </svg>
  )
}

function SlidersIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <line x1="4" y1="21" x2="4" y2="14" />
      <line x1="4" y1="10" x2="4" y2="3" />
      <line x1="12" y1="21" x2="12" y2="12" />
      <line x1="12" y1="8" x2="12" y2="3" />
      <line x1="20" y1="21" x2="20" y2="16" />
      <line x1="20" y1="12" x2="20" y2="3" />
      <line x1="1" y1="14" x2="7" y2="14" />
      <line x1="9" y1="8" x2="15" y2="8" />
      <line x1="17" y1="16" x2="23" y2="16" />
    </svg>
  )
}

function ZapIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
    </svg>
  )
}

function TagIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z" />
      <line x1="7" y1="7" x2="7.01" y2="7" />
    </svg>
  )
}

function BookIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      <path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20" />
      <path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z" />
    </svg>
  )
}

// ChevronIcon points left (collapse) or right (expand), matching the sidebar
// toggle's direction.
function ChevronIcon({ dir }: { dir: 'left' | 'right' }) {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    >
      {dir === 'left' ? <polyline points="15 18 9 12 15 6" /> : <polyline points="9 18 15 12 9 6" />}
    </svg>
  )
}

function NotFound() {
  return <main className="flex-1 flex items-center justify-center text-slate-400">Not found</main>
}
