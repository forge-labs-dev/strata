<script setup lang="ts">
import { ref, computed } from 'vue'
import { useCodemirror } from '../composables/useCodemirror'
import { useNotebook } from '../stores/notebook'
import EnvVarsEditor from './EnvVarsEditor.vue'
import InspectPanel from './InspectPanel.vue'
import MountListEditor from './MountListEditor.vue'
import TimeoutConfigEditor from './TimeoutConfigEditor.vue'
import WorkerConfigEditor from './WorkerConfigEditor.vue'
import type { Cell } from '../types/notebook'
import {
  resolveEffectiveWorkerEntry,
  summarizeRemoteExecutionIssue,
  workerTransportLabel,
  workerWarningForEntry,
} from '../utils/notebookWorkers'

const props = defineProps<{ cell: Cell }>()
const emit = defineEmits<{
  run: [cellId: string]
  delete: [cellId: string]
  addBelow: [cellId: string]
}>()

const {
  updateSource,
  openInspect,
  inspectCellId,
  availableWorkers,
  cellWorkerErrorForCell,
  addDependencyAction,
  updateCellEnvAction,
  updateCellTimeoutAction,
  updateCellWorkerAction,
  updateCellMountsAction,
} = useNotebook()

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
const showInfra = ref(false)

useCodemirror(editorEl, {
  initialDoc: props.cell.source,
  language: props.cell.language,
  onUpdate: (doc) => updateSource(props.cell.id, doc),
  onRun: () => emit('run', props.cell.id),
})

const statusClass = computed(() => `status-${props.cell.status}`)
const statusLabel = computed(() => {
  switch (props.cell.status) {
    case 'idle':
      return '\u25CB'
    case 'queued':
      return '\u25F7'
    case 'running':
      return '\u25CC'
    case 'ready':
      return '\u25CF'
    case 'stale':
      return '\u25D0'
    case 'error':
      return '\u2715'
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
    case 'cached':
      return 'cached'
    case 'warm':
      return 'warm'
    case 'cold':
      return 'cold'
    default:
      return props.cell.executorName
  }
})

/** v1.1: Causality summary for tooltip */
const causalityTooltip = computed(() => {
  const c = props.cell.causality
  if (!c) return ''
  return c.details
    .map((d) => {
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
    })
    .join('; ')
})

function toggleCausality() {
  showCausality.value = !showCausality.value
}

const mountSummary = computed(() =>
  props.cell.mounts.map((mount) => `${mount.name}:${mount.mode}`).join(', '),
)

const effectiveWorkerLabel = computed(
  () => props.cell.annotations?.worker || props.cell.worker || 'local',
)
const effectiveWorkerEntry = computed(() =>
  resolveEffectiveWorkerEntry(availableWorkers.value, effectiveWorkerLabel.value),
)
const effectiveWorkerHealthLabel = computed(() => {
  const entry = effectiveWorkerEntry.value
  if (!entry || entry.backend === 'local') return null
  return entry.health
})
const effectiveWorkerTransportLabel = computed(() => {
  const entry = effectiveWorkerEntry.value
  if (!entry || entry.backend === 'local') return null
  return workerTransportLabel(entry)
})
const workerWarning = computed(() => {
  return workerWarningForEntry(effectiveWorkerEntry.value, effectiveWorkerLabel.value)
})
const remoteExecutionIssueSummary = computed(() => {
  return summarizeRemoteExecutionIssue(
    props.cell.output?.error || '',
    effectiveWorkerEntry.value,
    effectiveWorkerLabel.value,
  )
})
const effectiveTimeoutLabel = computed(() => {
  const timeout = props.cell.annotations?.timeout ?? props.cell.timeout
  return timeout == null ? null : `${timeout}s`
})
const effectiveEnvCount = computed(
  () =>
    Object.keys({
      ...props.cell.env,
      ...(props.cell.annotations?.env || {}),
    }).length,
)

const hasAnnotationOverrides = computed(() => {
  const annotations = props.cell.annotations
  return Boolean(
    annotations &&
    (annotations.mounts.length ||
      annotations.worker ||
      annotations.timeout != null ||
      Object.keys(annotations.env).length),
  )
})

/** Check if scalar is only console output (no display value) */
function isConsoleOnly(scalar: unknown): boolean {
  if (scalar && typeof scalar === 'object' && 'console' in (scalar as Record<string, unknown>)) {
    return Object.keys(scalar as Record<string, unknown>).length === 1
  }
  return false
}

function consoleOutput(scalar: unknown): string | null {
  if (!scalar || typeof scalar !== 'object') return null
  const obj = scalar as Record<string, unknown>
  if (!('console' in obj) || typeof obj.console !== 'string') return null
  return obj.console
}

/** Format scalar output for display */
function formatScalar(scalar: unknown): string {
  if (scalar === null || scalar === undefined) return 'None'
  if (typeof scalar === 'object') {
    // Skip console-only objects
    const obj = scalar as Record<string, unknown>
    if ('console' in obj && Object.keys(obj).length === 1) return ''
    return JSON.stringify(scalar, null, 2)
  }
  return String(scalar)
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
        <button title="Inspect inputs" :class="{ active: isInspecting }" @click="toggleInspect">
          &#x1F50D;
        </button>
      </div>
    </div>

    <!-- Editor + output -->
    <div class="cell-body">
      <div class="cell-meta">
        <span class="cell-lang">{{ cell.language }}</span>
        <span
          v-if="cell.isLeaf"
          class="leaf-badge"
          title="This cell is a leaf (no downstream consumers)"
          >leaf</span
        >
        <span v-if="cell.defines.length" class="cell-vars">
          defines: <code>{{ cell.defines.join(', ') }}</code>
        </span>
        <span v-if="cell.upstreamIds.length" class="cell-vars">
          reads: <code>{{ cell.references.join(', ') }}</code>
        </span>
        <span v-if="cell.mounts.length" class="mount-badge" :title="mountSummary">
          mounts: {{ cell.mounts.length }}
        </span>
        <span class="worker-badge" :title="`Worker: ${effectiveWorkerLabel}`">
          worker: {{ effectiveWorkerLabel }}
        </span>
        <span
          v-if="effectiveWorkerTransportLabel"
          class="worker-transport-badge"
          :title="`Remote worker transport: ${effectiveWorkerTransportLabel}`"
        >
          {{ effectiveWorkerTransportLabel }}
        </span>
        <span
          v-if="effectiveWorkerHealthLabel"
          class="worker-health-badge"
          :class="{ warning: workerWarning }"
          :title="workerWarning || `Worker health: ${effectiveWorkerHealthLabel}`"
        >
          {{ workerWarning ? 'attention' : effectiveWorkerHealthLabel }}
        </span>
        <span
          v-if="effectiveTimeoutLabel"
          class="timeout-badge"
          :title="`Timeout: ${effectiveTimeoutLabel}`"
        >
          timeout: {{ effectiveTimeoutLabel }}
        </span>
        <span v-if="effectiveEnvCount" class="env-badge" :title="`${effectiveEnvCount} env vars`">
          env: {{ effectiveEnvCount }}
        </span>
        <!-- v1.1: Cache/execution badges -->
        <span v-if="cell.output?.cacheHit" class="cache-badge" title="Result loaded from cache">
          &#x26A1; cached
        </span>
        <span
          v-if="executionMethodLabel && !cell.output?.cacheHit"
          class="exec-method-badge"
          :title="`Executed via ${executionMethodLabel} process`"
        >
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
        <button class="infra-btn" @click="showInfra = !showInfra">
          {{ showInfra ? 'Hide infra' : 'Infra' }}
        </button>
      </div>

      <div v-if="showInfra" class="infra-panel">
        <div v-if="workerWarning" class="infra-warning">
          {{ workerWarning }}
        </div>

        <WorkerConfigEditor
          :worker="cell.workerOverride"
          :options="availableWorkers"
          title="Cell Worker Override"
          compact
          :error="cellWorkerErrorForCell(cell.id)"
          @save="(worker) => updateCellWorkerAction(cell.id, worker)"
        />

        <TimeoutConfigEditor
          :timeout="cell.timeoutOverride"
          title="Cell Timeout Override"
          compact
          @save="(timeout) => updateCellTimeoutAction(cell.id, timeout)"
        />

        <EnvVarsEditor
          :env="cell.envOverrides"
          title="Cell Env Overrides"
          compact
          @save="(env) => updateCellEnvAction(cell.id, env)"
        />

        <MountListEditor
          :mounts="cell.mountOverrides"
          title="Cell Mount Overrides"
          compact
          @save="(mounts) => updateCellMountsAction(cell.id, mounts)"
        />

        <div v-if="hasAnnotationOverrides" class="annotation-panel">
          <div class="annotation-title">Source Annotations Override Saved Config</div>
          <MountListEditor
            v-if="cell.annotations?.mounts?.length"
            :mounts="cell.annotations.mounts"
            title="Annotation Mounts"
            compact
            read-only
          />
          <div v-if="cell.annotations?.timeout != null" class="annotation-item">
            <span class="annotation-key">@timeout</span>
            <code>{{ cell.annotations.timeout }}</code>
          </div>
          <div v-if="cell.annotations?.worker" class="annotation-item">
            <span class="annotation-key">@worker</span>
            <code>{{ cell.annotations.worker }}</code>
          </div>
          <EnvVarsEditor
            v-if="Object.keys(cell.annotations?.env || {}).length"
            :env="cell.annotations?.env || {}"
            title="Annotation Env"
            compact
            read-only
          />
        </div>
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
      <div v-if="cell.output && consoleOutput(cell.output.scalar)" class="cell-console">
        <pre>{{ consoleOutput(cell.output.scalar) }}</pre>
      </div>

      <!-- Output -->
      <div v-if="cell.output" class="cell-output">
        <div v-if="cell.output.error" class="output-error">
          <div v-if="remoteExecutionIssueSummary" class="remote-error-summary">
            <span class="remote-error-label">Remote</span>
            <span>{{ remoteExecutionIssueSummary }}</span>
            <span v-if="effectiveWorkerTransportLabel" class="remote-error-pill">
              {{ effectiveWorkerTransportLabel }}
            </span>
            <span v-if="effectiveWorkerHealthLabel" class="remote-error-pill">
              {{ effectiveWorkerHealthLabel }}
            </span>
          </div>
          <pre class="output-error-detail">{{ cell.output.error }}</pre>
          <div v-if="cell.suggestInstall" class="suggest-install">
            <span
              >Missing package <code>{{ cell.suggestInstall }}</code></span
            >
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
        <div
          v-else-if="cell.output.scalar !== undefined && !isConsoleOnly(cell.output.scalar)"
          class="output-scalar"
        >
          <pre>{{ formatScalar(cell.output.scalar) }}</pre>
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
.cell:hover {
  border-color: #44447a;
}
.cell.status-running {
  border-color: #89b4fa;
}
.cell.status-ready {
  border-color: #a6e3a1;
}
.cell.status-stale {
  border-color: #f9e2af;
}
.cell.status-error {
  border-color: #f38ba8;
}

.cell-gutter {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 8px 4px;
  gap: 8px;
  min-width: 36px;
  border-right: 1px solid #2a2a3c;
}
.status-dot {
  font-size: 14px;
  color: #6c7086;
}
.status-ready .status-dot {
  color: #a6e3a1;
}
.status-running .status-dot {
  color: #89b4fa;
  animation: pulse 1s infinite;
}
.status-stale .status-dot {
  color: #f9e2af;
}
.status-error .status-dot {
  color: #f38ba8;
}

@keyframes pulse {
  50% {
    opacity: 0.4;
  }
}

.cell-actions {
  display: flex;
  flex-direction: column;
  gap: 2px;
  opacity: 0;
  transition: opacity 0.15s;
}
.cell:hover .cell-actions {
  opacity: 1;
}
.cell-actions button {
  background: none;
  border: none;
  color: #6c7086;
  cursor: pointer;
  font-size: 14px;
  padding: 2px 4px;
  border-radius: 3px;
}
.cell-actions button:hover {
  background: #313244;
  color: #cdd6f4;
}
.cell-actions button.active {
  color: #89b4fa;
  background: #89b4fa22;
}

.cell-body {
  flex: 1;
  min-width: 0;
}

.cell-meta {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 4px 12px;
  font-size: 11px;
  color: #6c7086;
  border-bottom: 1px solid #2a2a3c;
}
.cell-lang {
  text-transform: uppercase;
  font-weight: 600;
}
.cell-vars code {
  color: #89b4fa;
}
.cache-badge {
  background: #a6e3a133;
  color: #a6e3a1;
  padding: 1px 6px;
  border-radius: 3px;
  font-weight: 600;
}
.mount-badge {
  background: #94e2d522;
  color: #94e2d5;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
}
.worker-badge {
  background: #cba6f722;
  color: #cba6f7;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
}
.worker-transport-badge {
  background: #89dceb22;
  color: #89dceb;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
}
.worker-health-badge {
  background: #89b4fa22;
  color: #89b4fa;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
}
.worker-health-badge.warning {
  background: #f9e2af22;
  color: #f9e2af;
}
.timeout-badge {
  background: #f9e2af22;
  color: #f9e2af;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
}
.env-badge {
  background: #fab38722;
  color: #fab387;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
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
.duration {
  margin-left: auto;
}
.infra-btn {
  background: #313244;
  border: 1px solid #45475a;
  color: #cdd6f4;
  padding: 3px 8px;
  border-radius: 4px;
  font-size: 11px;
  cursor: pointer;
}
.infra-panel {
  border-bottom: 1px solid #2a2a3c;
  padding: 10px 12px 12px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.infra-warning {
  padding: 8px 10px;
  border: 1px solid #f9e2af33;
  background: #f9e2af14;
  color: #f9e2af;
  border-radius: 6px;
  font-size: 12px;
}
.annotation-panel {
  border: 1px solid #45475a;
  border-radius: 8px;
  padding: 10px;
  background: #181825;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.annotation-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: #f9e2af;
}
.annotation-item {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  color: #a6adc8;
}
.annotation-key {
  color: #f9e2af;
  font-weight: 600;
}

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
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  color: #a6adc8;
  white-space: pre-wrap;
  max-height: 200px;
  overflow-y: auto;
}

.editor-container {
  min-height: 40px;
}

.cell-output {
  border-top: 1px solid #2a2a3c;
  padding: 8px 12px;
  font-size: 13px;
}
.output-error {
  color: #f38ba8;
}
.output-error-detail {
  margin: 0;
  font-family: monospace;
  white-space: pre-wrap;
}
.remote-error-summary {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 8px;
  color: #f9e2af;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  font-size: 13px;
}
.remote-error-label,
.remote-error-pill {
  padding: 2px 8px;
  border-radius: 999px;
  background: #313244;
  color: #f9e2af;
  font-size: 11px;
  font-weight: 700;
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
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  font-size: 13px;
  color: #cdd6f4;
}
.suggest-install code {
  color: #89b4fa;
  font-weight: 600;
}
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
.btn-install:hover {
  background: #74c7ec;
}
.output-table-wrap {
  overflow-x: auto;
}
.output-table {
  width: 100%;
  border-collapse: collapse;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
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
.output-table tr:hover td {
  background: #313244;
}
.row-count {
  color: #6c7086;
  font-size: 11px;
  margin-top: 4px;
}

.output-scalar pre {
  margin: 0;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 13px;
  color: #a6e3a1;
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
