<script setup lang="ts">
import { ref, computed } from 'vue'
import { useCodemirror } from '../composables/useCodemirror'
import { useNotebook } from '../stores/notebook'
import InspectPanel from './InspectPanel.vue'
import type { Cell } from '../types/notebook'

const props = defineProps<{ cell: Cell }>()
const emit = defineEmits<{
  run: [cellId: string]
  delete: [cellId: string]
  addBelow: [cellId: string]
}>()

const { updateSource, openInspect, inspectCellId, addDependencyAction } = useNotebook()

const isInspecting = computed(() => inspectCellId.value === props.cell.id)

function toggleInspect() {
  if (isInspecting.value) {
    // Close handled inside InspectPanel
    return
  }
  openInspect(props.cell.id)
}

const editorEl = ref<HTMLElement | null>(null)
const showCausality = ref(false)

useCodemirror(editorEl, {
  initialDoc: props.cell.source,
  language: props.cell.language,
  onUpdate: (doc) => updateSource(props.cell.id, doc),
  onRun: () => emit('run', props.cell.id),
})

const statusClass = computed(() => `status-${props.cell.status}`)
const statusLabel = computed(() => {
  switch (props.cell.status) {
    case 'idle': return '\u25CB'
    case 'queued': return '\u25F7'
    case 'running': return '\u25CC'
    case 'ready': return '\u25CF'
    case 'stale': return '\u25D0'
    case 'error': return '\u2715'
  }
})

const durationLabel = computed(() => {
  if (!props.cell.durationMs) return ''
  return props.cell.durationMs < 1000
    ? `${props.cell.durationMs}ms`
    : `${(props.cell.durationMs / 1000).toFixed(1)}s`
})

/** v1.1: Execution method label */
const executionMethodLabel = computed(() => {
  if (!props.cell.executorName) return ''
  switch (props.cell.executorName) {
    case 'cached': return 'cached'
    case 'warm': return 'warm'
    case 'cold': return 'cold'
    default: return props.cell.executorName
  }
})

/** v1.1: Causality summary for tooltip */
const causalityTooltip = computed(() => {
  const c = props.cell.causality
  if (!c) return ''
  return c.details.map(d => {
    switch (d.type) {
      case 'source_changed':
        return `source of "${d.cellName || d.cellId}" changed`
      case 'input_changed':
        return `upstream "${d.cellName || d.cellId}" changed`
      case 'env_changed':
        return `environment changed (${d.package || 'uv.lock'})`
      default:
        return d.type
    }
  }).join('; ')
})

function toggleCausality() {
  showCausality.value = !showCausality.value
}
</script>

<template>
  <div class="cell" :class="statusClass">
    <!-- Left gutter -->
    <div class="cell-gutter">
      <span class="status-dot" :title="cell.status">{{ statusLabel }}</span>
      <div class="cell-actions">
        <button title="Run (Shift+Enter)" @click="emit('run', cell.id)">&#x25B6;</button>
        <button title="Add cell below" @click="emit('addBelow', cell.id)">+</button>
        <button title="Delete cell" @click="emit('delete', cell.id)">&times;</button>
        <button
          title="Inspect inputs"
          :class="{ active: isInspecting }"
          @click="toggleInspect"
        >&#x1F50D;</button>
      </div>
    </div>

    <!-- Editor + output -->
    <div class="cell-body">
      <div class="cell-meta">
        <span class="cell-lang">{{ cell.language }}</span>
        <span v-if="cell.isLeaf" class="leaf-badge" title="This cell is a leaf (no downstream consumers)">leaf</span>
        <span v-if="cell.defines.length" class="cell-vars">
          defines: <code>{{ cell.defines.join(', ') }}</code>
        </span>
        <span v-if="cell.upstreamIds.length" class="cell-vars">
          reads: <code>{{ cell.references.join(', ') }}</code>
        </span>
        <!-- v1.1: Cache/execution badges -->
        <span v-if="cell.output?.cacheHit" class="cache-badge" title="Result loaded from cache">
          &#x26A1; cached
        </span>
        <span v-if="executionMethodLabel && !cell.output?.cacheHit" class="exec-method-badge" :title="`Executed via ${executionMethodLabel} process`">
          {{ executionMethodLabel }}
        </span>
        <span v-if="durationLabel" class="duration">{{ durationLabel }}</span>
        <!-- v1.1: Causality indicator -->
        <button
          v-if="cell.causality"
          class="causality-btn"
          :title="causalityTooltip"
          @click="toggleCausality"
        >
          Why stale?
        </button>
      </div>

      <!-- v1.1: Causality chain detail (expanded) -->
      <div v-if="showCausality && cell.causality" class="causality-panel">
        <div class="causality-header">
          Stale because: <span class="causality-reason">{{ cell.causality.reason }}</span>
        </div>
        <ul class="causality-details">
          <li v-for="(detail, i) in cell.causality.details" :key="i" class="causality-detail">
            <span v-if="detail.type === 'source_changed'" class="detail-icon">&#x270E;</span>
            <span v-else-if="detail.type === 'input_changed'" class="detail-icon">&#x2191;</span>
            <span v-else class="detail-icon">&#x2699;</span>
            <span v-if="detail.type === 'source_changed'">
              Source of <code>{{ detail.cellName || detail.cellId }}</code> changed
            </span>
            <span v-else-if="detail.type === 'input_changed'">
              Upstream <code>{{ detail.cellName || detail.cellId }}</code> changed
              <span v-if="detail.fromVersion" class="version-change">
                {{ detail.fromVersion }} &rarr; {{ detail.toVersion }}
              </span>
            </span>
            <span v-else>
              Environment changed
              <span v-if="detail.package">({{ detail.package }})</span>
            </span>
          </li>
        </ul>
      </div>

      <div ref="editorEl" class="editor-container" />

      <!-- Console output (stdout/stderr) -->
      <div v-if="cell.output && cell.output.scalar && typeof cell.output.scalar === 'object' && 'console' in (cell.output.scalar as Record<string, unknown>)" class="cell-console">
        <pre>{{ (cell.output.scalar as Record<string, unknown>).console }}</pre>
      </div>

      <!-- Output -->
      <div v-if="cell.output" class="cell-output">
        <div v-if="cell.output.error" class="output-error">
          {{ cell.output.error }}
          <div v-if="cell.suggestInstall" class="suggest-install">
            <span>Missing package <code>{{ cell.suggestInstall }}</code></span>
            <button class="btn-install" @click="addDependencyAction(cell.suggestInstall!)">
              Install {{ cell.suggestInstall }}
            </button>
          </div>
        </div>
        <div v-else-if="cell.output.rows?.length" class="output-table-wrap">
          <table class="output-table">
            <thead>
              <tr>
                <th v-for="col in cell.output.columns" :key="col">{{ col }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(row, i) in cell.output.rows?.slice(0, 50)" :key="i">
                <td v-for="col in cell.output.columns" :key="col">{{ row[col] }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="(cell.output.rowCount ?? 0) > 50" class="row-count">
            showing 50 of {{ cell.output.rowCount?.toLocaleString() }} rows
          </div>
        </div>
      </div>

      <!-- Inspect REPL panel -->
      <InspectPanel v-if="isInspecting" />
    </div>
  </div>
</template>

<style scoped>
.cell {
  display: flex;
  border: 1px solid #2a2a3c;
  border-radius: 8px;
  margin-bottom: 8px;
  background: #1e1e2e;
  transition: border-color 0.2s;
}
.cell:hover { border-color: #44447a; }
.cell.status-running { border-color: #89b4fa; }
.cell.status-ready { border-color: #a6e3a1; }
.cell.status-stale { border-color: #f9e2af; }
.cell.status-error { border-color: #f38ba8; }

.cell-gutter {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 8px 4px;
  gap: 8px;
  min-width: 36px;
  border-right: 1px solid #2a2a3c;
}
.status-dot { font-size: 14px; color: #6c7086; }
.status-ready .status-dot { color: #a6e3a1; }
.status-running .status-dot { color: #89b4fa; animation: pulse 1s infinite; }
.status-stale .status-dot { color: #f9e2af; }
.status-error .status-dot { color: #f38ba8; }

@keyframes pulse { 50% { opacity: 0.4; } }

.cell-actions {
  display: flex;
  flex-direction: column;
  gap: 2px;
  opacity: 0;
  transition: opacity 0.15s;
}
.cell:hover .cell-actions { opacity: 1; }
.cell-actions button {
  background: none;
  border: none;
  color: #6c7086;
  cursor: pointer;
  font-size: 14px;
  padding: 2px 4px;
  border-radius: 3px;
}
.cell-actions button:hover { background: #313244; color: #cdd6f4; }
.cell-actions button.active { color: #89b4fa; background: #89b4fa22; }

.cell-body { flex: 1; min-width: 0; }

.cell-meta {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 4px 12px;
  font-size: 11px;
  color: #6c7086;
  border-bottom: 1px solid #2a2a3c;
}
.cell-lang { text-transform: uppercase; font-weight: 600; }
.cell-vars code { color: #89b4fa; }
.cache-badge {
  background: #a6e3a133;
  color: #a6e3a1;
  padding: 1px 6px;
  border-radius: 3px;
  font-weight: 600;
}
.exec-method-badge {
  background: #89b4fa22;
  color: #89b4fa;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
}
.leaf-badge {
  background: #89b4fa33;
  color: #89b4fa;
  padding: 1px 6px;
  border-radius: 3px;
  font-weight: 600;
}
.duration { margin-left: auto; }

/* v1.1: Causality inspector */
.causality-btn {
  background: #f9e2af22;
  color: #f9e2af;
  border: 1px solid #f9e2af44;
  padding: 1px 8px;
  border-radius: 3px;
  font-size: 10px;
  cursor: pointer;
  font-weight: 600;
}
.causality-btn:hover {
  background: #f9e2af33;
  border-color: #f9e2af66;
}

.causality-panel {
  background: #181825;
  border-bottom: 1px solid #2a2a3c;
  padding: 8px 12px;
  font-size: 12px;
}
.causality-header {
  color: #f9e2af;
  font-weight: 600;
  margin-bottom: 4px;
}
.causality-reason {
  background: #f9e2af22;
  padding: 1px 6px;
  border-radius: 3px;
}
.causality-details {
  list-style: none;
  padding: 0;
  margin: 0;
}
.causality-detail {
  color: #a6adc8;
  padding: 2px 0;
  display: flex;
  align-items: center;
  gap: 6px;
}
.causality-detail code {
  color: #89b4fa;
}
.detail-icon {
  color: #f9e2af;
  font-size: 11px;
  width: 16px;
  text-align: center;
}
.version-change {
  color: #6c7086;
  font-size: 10px;
}

/* Console output */
.cell-console {
  border-top: 1px solid #2a2a3c;
  padding: 6px 12px;
  font-size: 12px;
  background: #11111b;
}
.cell-console pre {
  margin: 0;
  font-family: "JetBrains Mono", "Fira Code", monospace;
  color: #a6adc8;
  white-space: pre-wrap;
  max-height: 200px;
  overflow-y: auto;
}

.editor-container { min-height: 40px; }

.cell-output {
  border-top: 1px solid #2a2a3c;
  padding: 8px 12px;
  font-size: 13px;
}
.output-error {
  color: #f38ba8;
  font-family: monospace;
  white-space: pre-wrap;
}
.suggest-install {
  margin-top: 8px;
  padding: 8px 10px;
  background: #89b4fa15;
  border: 1px solid #89b4fa33;
  border-radius: 6px;
  display: flex;
  align-items: center;
  gap: 12px;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  font-size: 13px;
  color: #cdd6f4;
}
.suggest-install code { color: #89b4fa; font-weight: 600; }
.btn-install {
  background: #89b4fa;
  color: #1e1e2e;
  border: none;
  padding: 4px 12px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  white-space: nowrap;
  flex-shrink: 0;
}
.btn-install:hover { background: #74c7ec; }
.output-table-wrap { overflow-x: auto; }
.output-table {
  width: 100%;
  border-collapse: collapse;
  font-family: "JetBrains Mono", "Fira Code", monospace;
  font-size: 12px;
}
.output-table th {
  text-align: left;
  padding: 4px 12px;
  color: #89b4fa;
  border-bottom: 1px solid #313244;
  font-weight: 600;
  position: sticky;
  top: 0;
  background: #1e1e2e;
}
.output-table td {
  padding: 3px 12px;
  color: #cdd6f4;
  border-bottom: 1px solid #1e1e2e;
}
.output-table tr:hover td { background: #313244; }
.row-count { color: #6c7086; font-size: 11px; margin-top: 4px; }
</style>
