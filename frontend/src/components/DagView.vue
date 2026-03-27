<script setup lang="ts">
import { computed } from 'vue'
import { useNotebook } from '../stores/notebook'
import type { Cell, CellId } from '../types/notebook'

const { orderedCells, dagEdges } = useNotebook()

interface NodeLayout {
  id: CellId
  x: number
  y: number
  label: string
  status: Cell['status']
  depth: number
}

const nodeSize = { w: 110, h: 36 }
const layerGapY = 56
const siblingGapX = 16
const padding = 24

/**
 * Compute topological depth for each cell.
 * Roots (no upstream) are depth 0. Each cell's depth = max(upstream depths) + 1.
 * Cells with no edges get depth based on their order index.
 */
const nodes = computed<NodeLayout[]>(() => {
  const cells = orderedCells.value
  if (cells.length === 0) return []

  // Build adjacency: cell_id -> upstream cell ids
  const upstreamMap = new Map<CellId, Set<CellId>>()
  for (const c of cells) upstreamMap.set(c.id, new Set())
  for (const e of dagEdges.value) {
    upstreamMap.get(e.to_cell_id)?.add(e.from_cell_id)
  }

  // Compute depth via BFS
  const depth = new Map<CellId, number>()
  const hasEdge = new Set<CellId>()
  for (const e of dagEdges.value) {
    hasEdge.add(e.from_cell_id)
    hasEdge.add(e.to_cell_id)
  }

  // Topological depth assignment
  for (const c of cells) {
    computeDepth(c.id, upstreamMap, depth, new Set())
  }

  // For cells not in the DAG at all, assign depth = order index
  // so they still appear in sequence
  let maxDepth = 0
  for (const d of depth.values()) maxDepth = Math.max(maxDepth, d)

  // Group cells by depth layer
  const layers = new Map<number, CellId[]>()
  for (const c of cells) {
    const d = depth.get(c.id) ?? 0
    if (!layers.has(d)) layers.set(d, [])
    layers.get(d)!.push(c.id)
  }

  // Compute positions: Y by depth, X centered within each layer
  const result: NodeLayout[] = []
  const sortedDepths = [...layers.keys()].sort((a, b) => a - b)

  // Find max layer width for centering
  let maxLayerWidth = 0
  for (const ids of layers.values()) {
    const w = ids.length * (nodeSize.w + siblingGapX) - siblingGapX
    maxLayerWidth = Math.max(maxLayerWidth, w)
  }

  for (const d of sortedDepths) {
    const ids = layers.get(d)!
    const layerWidth = ids.length * (nodeSize.w + siblingGapX) - siblingGapX
    const startX = padding + (maxLayerWidth - layerWidth) / 2 + nodeSize.w / 2

    for (let i = 0; i < ids.length; i++) {
      const c = cells.find((cell) => cell.id === ids[i])!
      const cellIdx = cells.indexOf(c)
      result.push({
        id: c.id,
        x: startX + i * (nodeSize.w + siblingGapX),
        y: padding + d * layerGapY + nodeSize.h / 2,
        label: `[${cellIdx + 1}]`,
        status: c.status,
        depth: d,
      })
    }
  }

  return result
})

function computeDepth(
  id: CellId,
  upstreamMap: Map<CellId, Set<CellId>>,
  depth: Map<CellId, number>,
  visiting: Set<CellId>,
): number {
  if (depth.has(id)) return depth.get(id)!
  if (visiting.has(id)) return 0 // cycle guard

  visiting.add(id)
  const ups = upstreamMap.get(id)
  if (!ups || ups.size === 0) {
    depth.set(id, 0)
    return 0
  }

  let maxUp = 0
  for (const uid of ups) {
    maxUp = Math.max(maxUp, computeDepth(uid, upstreamMap, depth, visiting))
  }
  const d = maxUp + 1
  depth.set(id, d)
  return d
}

const edges = computed(() => {
  const nodeMap = new Map(nodes.value.map((n) => [n.id, n]))
  return dagEdges.value
    .map((e) => {
      const from = nodeMap.get(e.from_cell_id)
      const to = nodeMap.get(e.to_cell_id)
      if (!from || !to) return null
      return { from, to, variable: e.variable }
    })
    .filter(Boolean) as { from: NodeLayout; to: NodeLayout; variable: string }[]
})

const svgWidth = computed(() => {
  if (nodes.value.length === 0) return nodeSize.w + padding * 2
  let maxX = 0
  for (const n of nodes.value) maxX = Math.max(maxX, n.x)
  return maxX + nodeSize.w / 2 + padding
})

const svgHeight = computed(() => {
  if (nodes.value.length === 0) return nodeSize.h + padding * 2
  let maxY = 0
  for (const n of nodes.value) maxY = Math.max(maxY, n.y)
  return maxY + nodeSize.h / 2 + padding
})

function statusColor(status: Cell['status']): string {
  switch (status) {
    case 'ready': return '#a6e3a1'
    case 'running': return '#89b4fa'
    case 'stale': return '#f9e2af'
    case 'error': return '#f38ba8'
    default: return '#6c7086'
  }
}

/** Compute a curved path between two nodes */
function edgePath(from: NodeLayout, to: NodeLayout): string {
  const x1 = from.x
  const y1 = from.y + nodeSize.h / 2
  const x2 = to.x
  const y2 = to.y - nodeSize.h / 2
  const dy = (y2 - y1) * 0.4
  return `M ${x1} ${y1} C ${x1} ${y1 + dy}, ${x2} ${y2 - dy}, ${x2} ${y2}`
}
</script>

<template>
  <div class="dag-panel">
    <div class="dag-header">Cell DAG</div>
    <svg
      :width="svgWidth"
      :height="svgHeight"
      class="dag-svg"
    >
      <defs>
        <marker
          id="arrow"
          viewBox="0 0 10 10"
          refX="10"
          refY="5"
          markerWidth="6"
          markerHeight="6"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill="#6c7086" />
        </marker>
      </defs>

      <!-- Edges (curved paths) -->
      <path
        v-for="(e, i) in edges"
        :key="'e' + i"
        :d="edgePath(e.from, e.to)"
        fill="none"
        stroke="#6c7086"
        stroke-width="1.5"
        marker-end="url(#arrow)"
      />

      <!-- Edge variable labels -->
      <text
        v-for="(e, i) in edges"
        :key="'el' + i"
        :x="(e.from.x + e.to.x) / 2 + 6"
        :y="(e.from.y + e.to.y) / 2 + 3"
        fill="#585b70"
        font-size="9"
        font-family="JetBrains Mono, Fira Code, monospace"
      >
        {{ e.variable }}
      </text>

      <!-- Nodes -->
      <g v-for="n in nodes" :key="n.id">
        <rect
          :x="n.x - nodeSize.w / 2"
          :y="n.y - nodeSize.h / 2"
          :width="nodeSize.w"
          :height="nodeSize.h"
          rx="6"
          :fill="statusColor(n.status) + '22'"
          :stroke="statusColor(n.status)"
          stroke-width="1.5"
        />
        <text
          :x="n.x"
          :y="n.y + 4"
          text-anchor="middle"
          fill="#cdd6f4"
          font-size="11"
          font-family="JetBrains Mono, Fira Code, monospace"
        >
          {{ n.label }}
        </text>
      </g>
    </svg>
  </div>
</template>

<style scoped>
.dag-panel {
  background: #1e1e2e;
  border: 1px solid #2a2a3c;
  border-radius: 8px;
  min-width: 180px;
  overflow: auto;
}
.dag-header {
  padding: 8px 12px;
  font-size: 12px;
  font-weight: 600;
  color: #6c7086;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  border-bottom: 1px solid #2a2a3c;
}
.dag-svg { display: block; }
</style>
