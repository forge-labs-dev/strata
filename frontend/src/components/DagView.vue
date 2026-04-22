<script setup lang="ts">
import * as dagre from '@dagrejs/dagre'
import { computed, reactive, ref } from 'vue'
import { useNotebook } from '../stores/notebook'
import type { Cell, CellId } from '../types/notebook'

const { orderedCells, dagEdges } = useNotebook()

interface NodeLayout {
  id: CellId
  x: number
  y: number
  label: string
  fullLabel: string
  status: Cell['status']
}

interface EdgeLayout {
  from: NodeLayout
  to: NodeLayout
  variable: string
  points: { x: number; y: number }[]
}

const nodeSize = { w: 140, h: 36 }
const maxLabelLen = 18

// Layout using dagre
const layout = computed(() => {
  const cells = orderedCells.value
  if (cells.length === 0) return { nodes: [] as NodeLayout[], edges: [] as EdgeLayout[] }

  const g = new dagre.graphlib.Graph()
  g.setGraph({
    rankdir: 'TB',
    nodesep: 24,
    ranksep: 48,
    marginx: 24,
    marginy: 24,
  })
  g.setDefaultEdgeLabel(() => ({}))

  // Add nodes
  for (const c of cells) {
    g.setNode(c.id, { width: nodeSize.w, height: nodeSize.h })
  }

  // Add edges (dedupe by from+to since multiple variables can connect same pair)
  const edgeSet = new Set<string>()
  for (const e of dagEdges.value) {
    const key = `${e.from_cell_id}->${e.to_cell_id}`
    if (!edgeSet.has(key)) {
      edgeSet.add(key)
      g.setEdge(e.from_cell_id, e.to_cell_id)
    }
  }

  dagre.layout(g)

  // Build node layouts
  const nodeMap = new Map<CellId, NodeLayout>()
  const nodes: NodeLayout[] = []
  for (const c of cells) {
    const dagreNode = g.node(c.id)
    if (!dagreNode) continue

    const name = c.annotations?.name
    const defines = c.defines.length ? c.defines.join(', ') : null
    const rawLabel = name || defines || c.id.slice(0, 8)
    const label =
      rawLabel.length > maxLabelLen ? rawLabel.slice(0, maxLabelLen - 1) + '\u2026' : rawLabel

    const node: NodeLayout = {
      id: c.id,
      x: dagreNode.x,
      y: dagreNode.y,
      label,
      fullLabel: rawLabel,
      status: c.status,
    }
    nodes.push(node)
    nodeMap.set(c.id, node)
  }

  // Build edge layouts with dagre's routed points
  const edges: EdgeLayout[] = []
  for (const e of dagEdges.value) {
    const from = nodeMap.get(e.from_cell_id)
    const to = nodeMap.get(e.to_cell_id)
    if (!from || !to) continue

    const dagreEdge = g.edge(e.from_cell_id, e.to_cell_id)
    const points = dagreEdge?.points ?? [
      { x: from.x, y: from.y + nodeSize.h / 2 },
      { x: to.x, y: to.y - nodeSize.h / 2 },
    ]

    edges.push({ from, to, variable: e.variable, points })
  }

  return { nodes, edges }
})

const nodes = computed(() => layout.value.nodes)
const edges = computed(() => layout.value.edges)

// SVG dimensions from dagre layout bounds
const svgWidth = computed(() => {
  if (nodes.value.length === 0) return 240
  let maxX = 0
  for (const n of nodes.value) maxX = Math.max(maxX, n.x + nodeSize.w / 2)
  return maxX + 24
})

const svgHeight = computed(() => {
  if (nodes.value.length === 0) return 100
  let maxY = 0
  for (const n of nodes.value) maxY = Math.max(maxY, n.y + nodeSize.h / 2)
  return maxY + 24
})

// Pan and zoom state
const pan = reactive({ x: 0, y: 0 })
const zoom = ref(1)
const isPanning = ref(false)
const panStart = reactive({ x: 0, y: 0, panX: 0, panY: 0 })

const viewBox = computed(() => {
  const w = svgWidth.value / zoom.value
  const h = svgHeight.value / zoom.value
  return `${-pan.x / zoom.value} ${-pan.y / zoom.value} ${w} ${h}`
})

function onWheel(e: WheelEvent) {
  e.preventDefault()
  const factor = e.deltaY > 0 ? 0.9 : 1.1
  const newZoom = Math.max(0.3, Math.min(3, zoom.value * factor))
  zoom.value = newZoom
}

function onMouseDown(e: MouseEvent) {
  if (e.button !== 0) return
  isPanning.value = true
  panStart.x = e.clientX
  panStart.y = e.clientY
  panStart.panX = pan.x
  panStart.panY = pan.y
}

function onMouseMove(e: MouseEvent) {
  if (!isPanning.value) return
  pan.x = panStart.panX + (e.clientX - panStart.x)
  pan.y = panStart.panY + (e.clientY - panStart.y)
}

function onMouseUp() {
  isPanning.value = false
}

function resetView() {
  pan.x = 0
  pan.y = 0
  zoom.value = 1
}

function scrollToCell(cellId: CellId) {
  const el = document.querySelector(`[data-testid="notebook-cell"][data-cell-id="${cellId}"]`)
  if (el) {
    el.scrollIntoView({ behavior: 'smooth', block: 'center' })
    // Brief highlight flash
    el.classList.add('dag-jump-highlight')
    setTimeout(() => el.classList.remove('dag-jump-highlight'), 1500)
  }
}

function statusColor(status: Cell['status']): string {
  switch (status) {
    case 'ready':
      return '#a6e3a1'
    case 'running':
      return '#89b4fa'
    case 'stale':
      return '#f9e2af'
    case 'error':
      return '#f38ba8'
    default:
      return '#6c7086'
  }
}

/** Build a smooth path through dagre's edge points */
function edgePath(points: { x: number; y: number }[]): string {
  if (points.length < 2) return ''
  const [start, ...rest] = points
  let d = `M ${start.x} ${start.y}`
  if (rest.length === 1) {
    d += ` L ${rest[0].x} ${rest[0].y}`
  } else {
    // Use cubic bezier through the control points
    for (let i = 0; i < rest.length - 1; i += 2) {
      const cp = rest[i]
      const end = rest[i + 1] ?? rest[i]
      d += ` Q ${cp.x} ${cp.y}, ${end.x} ${end.y}`
    }
    // If odd number of remaining points, line to the last
    if (rest.length % 2 === 1) {
      d += ` L ${rest[rest.length - 1].x} ${rest[rest.length - 1].y}`
    }
  }
  return d
}
</script>

<template>
  <div class="dag-panel">
    <div class="dag-controls">
      <button class="dag-reset" title="Reset zoom" @click.stop="resetView">Reset</button>
    </div>
    <div
      class="dag-viewport"
      @wheel="onWheel"
      @mousedown="onMouseDown"
      @mousemove="onMouseMove"
      @mouseup="onMouseUp"
      @mouseleave="onMouseUp"
    >
      <svg
        width="100%"
        height="100%"
        :viewBox="viewBox"
        preserveAspectRatio="xMidYMid meet"
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

        <!-- Edges -->
        <path
          v-for="(e, i) in edges"
          :key="'e' + i"
          :d="edgePath(e.points)"
          fill="none"
          stroke="#6c7086"
          stroke-width="1.5"
          marker-end="url(#arrow)"
        />

        <!-- Edge variable labels (at midpoint of edge) -->
        <text
          v-for="(e, i) in edges"
          :key="'el' + i"
          :x="e.points[Math.floor(e.points.length / 2)]?.x + 6"
          :y="e.points[Math.floor(e.points.length / 2)]?.y + 3"
          fill="#585b70"
          font-size="9"
          font-family="JetBrains Mono, Fira Code, monospace"
        >
          {{ e.variable }}
        </text>

        <!-- Nodes -->
        <g v-for="n in nodes" :key="n.id" class="dag-node" @dblclick.stop="scrollToCell(n.id)">
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
            <title>{{ n.fullLabel }}</title>
            {{ n.label }}
          </text>
        </g>
      </svg>
    </div>
  </div>
</template>

<style scoped>
.dag-panel {
  background: transparent;
  min-width: 180px;
  overflow: hidden;
  height: 100%;
  display: flex;
  flex-direction: column;
}
.dag-controls {
  display: flex;
  justify-content: flex-end;
  padding: 0 0 6px 0;
}
.dag-reset {
  font-size: 10px;
  padding: 2px 6px;
  background: none;
  border: 1px solid #313244;
  border-radius: 4px;
  color: #6c7086;
  cursor: pointer;
  text-transform: none;
  letter-spacing: 0;
}
.dag-reset:hover {
  border-color: #89b4fa;
  color: #89b4fa;
}
.dag-viewport {
  overflow: hidden;
  cursor: grab;
  flex: 1;
  min-height: 0;
}
.dag-viewport:active {
  cursor: grabbing;
}
.dag-svg {
  display: block;
  width: 100%;
  height: 100%;
}
.dag-node {
  cursor: pointer;
}
.dag-node:hover rect {
  filter: brightness(1.3);
}
</style>
