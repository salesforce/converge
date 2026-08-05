#!/usr/bin/env bash
# Regenerates docs/assets/reconcile-flow.gif — the animated end-to-end
# state machine used by docs/state-machine-2026-06-01.md.
#
# The GIF walks the FULL graph one flow at a time. Each scene traces a
# complete path through the same fixed layout, advancing the lit node
# (blue) stage by stage, dimming passed nodes (green) and coloring the
# edges of the active flow. The scenes:
#
#   1. happy path     spec → gates → claim → pipeline → drain → Ready
#   2. cascade        drain → cascade trigger → re-schedule (reactive)
#   3. failure        pipeline error → drain(failed) → Failed → reaper retry
#   4. demotion       drain(health flip) → Degraded
#   5. deletion       Ready → delete requested → Deleting → gone
#
# Node ids (stable): U BG SE WQ W WO DR RD FL CT RP DEL DG GONE
# Edge indices (declaration order, 0-based):
#   0 U→BG  1 BG→SE  2 SE→WQ  3 WQ→W  4 W→WO  5 WO→DR  6 DR→RD  7 DR→FL
#   8 DR→CT  9 CT→SE  10 DR→RP  11 RP→SE  12 FL→RP  13 RD→DEL  14 DEL→DG  15 DG→GONE
#
# Requires: npx (node), ImageMagick `magick`. Both run headless.
#   ./docs/assets/gen-reconcile-flow.sh
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

ALL_NODES="U,BG,SE,WQ,W,WO,DR,RD,FL,CT,RP,DEL,DG,GONE"

# emit_frame FILE TITLE HOT DONE EDGES
#   HOT/DONE: comma node lists. EDGES: comma list of 0-based edge indices to light.
emit_frame() {
  local file="$1" title="$2" hot="$3" done_="$4" edges="$5"
  {
    echo "---"
    echo "title: \"$title\""   # quoted: titles may contain ':' which is bare-YAML-illegal
    echo "---"
    cat <<'GRAPH'
flowchart LR
  U([user / API]) --> BG[bump_generation<br/>phase Reconciling]
  BG --> SE{schedule_eligible<br/>gates}
  SE --> WQ[(work_queue)]
  WQ --> W[worker pipeline<br/>compose? · work? · rollup?]
  W --> WO[(work_outbox)]
  WO --> DR[drain_outbox_batch<br/>one tx · status · synced_gen<br/>health_ok · failure_gen]
  DR --> RD([phase Ready / Degraded])
  DR --> FL([phase Failed])
  DR --> CT{{cascade_on_ready_change}}
  CT --> SE
  DR --> RP[reaper backstop]
  RP --> SE
  FL --> RP
  RD --> DEL[delete requested]
  DEL --> DG([phase Deleting])
  DG --> GONE([finalizers cleared · gone])
  classDef idle fill:#e5e7eb,stroke:#9ca3af,color:#374151;
  classDef hot  fill:#2563eb,stroke:#1e40af,color:#ffffff;
  classDef done fill:#16a34a,stroke:#15803d,color:#ffffff;
GRAPH
    echo "  class $ALL_NODES idle;"
    [ -n "$done_" ] && echo "  class $done_ done;"
    [ -n "$hot" ]   && echo "  class $hot hot;"
    # Light the active flow's edges blue + thick.
    if [ -n "$edges" ]; then
      echo "  linkStyle $edges stroke:#2563eb,stroke-width:3px;"
    fi
  } > "$file"
}

i=0
nf() { printf "%02d" "$i"; }

# ── Scene 1: happy path ──
emit_frame "$work/f$(nf).mmd" "spec write"          U  ""                 ""        ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "schedule"            SE "U,BG"             "0,1"     ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "claim"               W  "U,BG,SE,WQ"       "0,1,2,3" ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "drain"               DR "U,BG,SE,WQ,W,WO"  "0,1,2,3,4,5" ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "Ready"               RD "U,BG,SE,WQ,W,WO,DR" "0,1,2,3,4,5,6" ; i=$((i+1))

# ── Scene 2: reactive cascade (drain re-schedules dependents/roots) ──
emit_frame "$work/f$(nf).mmd" "cascade fires"       CT "DR"     "8"   ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "re-schedule"         SE "DR,CT"  "8,9" ; i=$((i+1))

# ── Scene 3: failure + retry ──
emit_frame "$work/f$(nf).mmd" "pipeline error"      W  "U,BG,SE,WQ"      "0,1,2,3" ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "drain: failed"       DR "U,BG,SE,WQ,W,WO" "0,1,2,3,4,5" ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "phase Failed"        FL "DR"     "7"     ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "reaper retries"      RP "FL"     "12"    ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "re-pend"             SE "FL,RP"  "12,11" ; i=$((i+1))

# ── Scene 4: demotion (health flip / child unready → Degraded) ──
emit_frame "$work/f$(nf).mmd" "health flips"        DR "WO"  "5" ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "phase Degraded"      RD "DR"  "6" ; i=$((i+1))

# ── Scene 5: deletion ──
emit_frame "$work/f$(nf).mmd" "delete requested"    DEL "RD"        "13"       ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "phase Deleting"      DG  "RD,DEL"    "13,14"    ; i=$((i+1))
emit_frame "$work/f$(nf).mmd" "finalizers · gone"   GONE "RD,DEL,DG" "13,14,15" ; i=$((i+1))

# Render every frame.
for f in "$work"/f*.mmd; do
  npx -y @mermaid-js/mermaid-cli@latest -i "$f" -o "${f%.mmd}.png" -b white -s 2 >/dev/null 2>&1
done

# Normalize onto one canvas (mermaid sizes per-frame; pad so the GIF doesn't
# jump). Center so the graph stays put as the title line changes width.
maxw=$(identify -format '%w\n' "$work"/f*.png | sort -n | tail -1)
maxh=$(identify -format '%h\n' "$work"/f*.png | sort -n | tail -1)
for f in "$work"/f*.png; do
  magick "$f" -background white -gravity North -extent "${maxw}x${maxh}" "${f%.png}_n.png"
done

# Assemble: each step held 3s so a human can follow the transition; the
# terminal frame of each scene held longer (4.5s) so the eye lands on the
# resulting phase before the next flow begins. Infinite loop.
args=(-loop 0)
for f in "$work"/f*_n.png; do
  case "$f" in
    *f04_n.png|*f06_n.png|*f12_n.png|*f13_n.png|*f16_n.png) args+=(-delay 450 "$f") ;; # scene-ending frames
    *) args+=(-delay 300 "$f") ;;
  esac
done
magick "${args[@]}" -layers Optimize "$here/reconcile-flow.gif"

echo "wrote $here/reconcile-flow.gif ($(du -h "$here/reconcile-flow.gif" | cut -f1), $(identify -format '%n frames' "$here/reconcile-flow.gif" | head -1))"
