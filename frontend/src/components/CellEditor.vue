<script setup lang="ts">
import { computed, defineAsyncComponent, ref, watch } from 'vue'
import { useCodemirror } from '../composables/useCodemirror'
import { useNotebook } from '../stores/notebook'
import EnvVarsEditor from './EnvVarsEditor.vue'
import MountListEditor from './MountListEditor.vue'
import TimeoutConfigEditor from './TimeoutConfigEditor.vue'
import WorkerConfigEditor from './WorkerConfigEditor.vue'
import type { Cell } from '../types/notebook'
import {
  resolveEffectiveWorkerEntry,
  summarizeRemoteExecutionState,
  summarizeRemoteExecutionIssue,
  workerTransportLabel,
  workerWarningForEntry,
} from '../utils/notebookWorkers'
import { renderMarkdownToHtml } from '../utils/markdown'
import { applySourceAnnotations } from '../utils/notebookAnnotations'
import type { CellAnnotations, MountSpec } from '../types/notebook'

const InspectPanel = defineAsyncComponent(() => import('./InspectPanel.vue'))

const props = defineProps<{ cell: Cell }>()
const emit = defineEmits<{
  run: [cellId: string]
  delete: [cellId: string]
  addBelow: [cellId: string]
}>()

const {
  connected,
  environmentMutationActive,
  environmentLastAction,
  environmentOperation,
  updateSource,
  openInspect,
  inspectCellId,
  availableWorkers,
  ensureWorkersLoaded,
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
const showAnnotationEditor = ref(false)
const installRequestedPackage = ref<string | null>(null)

const { view, setDoc } = useCodemirror(editorEl, {
  initialDoc: props.cell.source,
  language: props.cell.language,
  onUpdate: (doc) => updateSource(props.cell.id, doc),
  onRun: () => emit('run', props.cell.id),
})

watch(
  () => props.cell.source,
  (source) => {
    if (view.value && view.value.state.doc.toString() !== source) {
      setDoc(source)
    }
  },
)

watch(
  () => props.cell.status,
  (status) => {
    if (status === 'running') {
      installRequestedPackage.value = null
    }
  },
)

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

const canExplainStaleness = computed(() =>
  Boolean(props.cell.causality && (props.cell.status === 'idle' || props.cell.status === 'stale')),
)

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
  if (!canExplainStaleness.value) {
    showCausality.value = false
    return
  }
  showCausality.value = !showCausality.value
}

watch(canExplainStaleness, (canExplain) => {
  if (!canExplain) {
    showCausality.value = false
  }
})

watch(showInfra, (visible) => {
  if (visible) {
    void ensureWorkersLoaded()
  }
})

watch(
  () => props.cell.annotations,
  (annotations) => {
    const hasOverrides = Boolean(
      annotations &&
      (annotations.mounts.length ||
        annotations.worker ||
        annotations.timeout != null ||
        Object.keys(annotations.env).length),
    )
    if (!hasOverrides) {
      showAnnotationEditor.value = false
    }
  },
  { deep: true },
)

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
const lastRemoteWorkerLabel = computed(() => props.cell.remoteWorkerName || null)
const lastRemoteTransportLabel = computed(() => props.cell.remoteTransport || null)
const lastRemoteBuildId = computed(() => props.cell.remoteBuildId || null)
const lastRemoteBuildState = computed(() => props.cell.remoteBuildState || null)
const lastRemoteErrorCode = computed(() => props.cell.remoteErrorCode || null)
const workerWarning = computed(() => {
  return workerWarningForEntry(effectiveWorkerEntry.value, effectiveWorkerLabel.value)
})
const remoteExecutionIssueSummary = computed(() => {
  return summarizeRemoteExecutionIssue(
    props.cell.output?.error || '',
    effectiveWorkerEntry.value,
    effectiveWorkerLabel.value,
    lastRemoteErrorCode.value,
    lastRemoteBuildState.value,
  )
})
const remoteExecutionSummary = computed(() =>
  summarizeRemoteExecutionState({
    executionMethod: props.cell.executorName,
    remoteWorkerName: lastRemoteWorkerLabel.value,
    remoteTransport: lastRemoteTransportLabel.value,
    remoteBuildState: lastRemoteBuildState.value,
    remoteErrorCode: lastRemoteErrorCode.value,
    hasError: Boolean(props.cell.output?.error),
  }),
)
const showRemoteExecutionSummary = computed(() =>
  Boolean(
    remoteExecutionSummary.value &&
    (lastRemoteWorkerLabel.value ||
      lastRemoteTransportLabel.value ||
      lastRemoteBuildId.value ||
      lastRemoteBuildState.value ||
      lastRemoteErrorCode.value ||
      props.cell.executorName === 'executor' ||
      props.cell.output?.cacheHit),
  ),
)
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

const annotationSummaryChips = computed(() => {
  const annotations = props.cell.annotations
  if (!annotations) return []

  const chips: string[] = []
  if (annotations.worker) chips.push(`@worker ${annotations.worker}`)
  if (annotations.timeout != null) chips.push(`@timeout ${annotations.timeout}s`)
  const envCount = Object.keys(annotations.env).length
  if (envCount) chips.push(`@env ${envCount}`)
  if (annotations.mounts.length) chips.push(`@mount ${annotations.mounts.length}`)
  return chips
})

const visibleAnnotationSummaryChips = computed(() => annotationSummaryChips.value.slice(0, 2))
const hiddenAnnotationSummaryCount = computed(() =>
  Math.max(0, annotationSummaryChips.value.length - visibleAnnotationSummaryChips.value.length),
)
const annotationSummaryTitle = computed(() => {
  if (!annotationSummaryChips.value.length) {
    return 'Source annotations override saved config'
  }
  return `Source annotations: ${annotationSummaryChips.value.join(' · ')}`
})

function currentSourceAnnotations(): CellAnnotations {
  return {
    worker: props.cell.annotations?.worker ?? null,
    timeout: props.cell.annotations?.timeout ?? null,
    env: { ...(props.cell.annotations?.env || {}) },
    mounts: (props.cell.annotations?.mounts || []).map((mount) => ({ ...mount })),
  }
}

async function saveSourceAnnotations(next: Partial<CellAnnotations>) {
  const annotations = currentSourceAnnotations()
  const merged: CellAnnotations = {
    worker: next.worker !== undefined ? next.worker : annotations.worker,
    timeout: next.timeout !== undefined ? next.timeout : annotations.timeout,
    env: next.env !== undefined ? next.env : annotations.env,
    mounts: next.mounts !== undefined ? next.mounts : annotations.mounts,
  }

  await updateSource(props.cell.id, applySourceAnnotations(props.cell.source, merged))
}

function saveAnnotationWorker(worker: string | null) {
  return saveSourceAnnotations({ worker })
}

function saveAnnotationTimeout(timeout: number | null) {
  return saveSourceAnnotations({ timeout })
}

function saveAnnotationEnv(env: Record<string, string>) {
  return saveSourceAnnotations({ env })
}

function saveAnnotationMounts(mounts: MountSpec[]) {
  return saveSourceAnnotations({ mounts })
}

function toggleAnnotationEditor() {
  showAnnotationEditor.value = !showAnnotationEditor.value
}

function normalizePackageName(pkg: string | null | undefined): string {
  return (pkg || '').trim().toLowerCase()
}

const installTargetPackage = computed(
  () => installRequestedPackage.value || props.cell.suggestInstall || null,
)

const installInProgress = computed(() => {
  return (
    environmentMutationActive.value &&
    environmentOperation.value?.action === 'add' &&
    normalizePackageName(environmentOperation.value.packageName) ===
      normalizePackageName(installTargetPackage.value)
  )
})

const installCompleted = computed(() => {
  return (
    !!installRequestedPackage.value &&
    !props.cell.suggestInstall &&
    environmentLastAction.value?.action === 'add' &&
    normalizePackageName(environmentLastAction.value.packageName) ===
      normalizePackageName(installRequestedPackage.value)
  )
})

async function installSuggestedPackage() {
  const pkg = props.cell.suggestInstall
  if (!pkg || installInProgress.value) return
  installRequestedPackage.value = pkg
  await addDependencyAction(pkg)
}

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

const renderedMarkdownOutput = computed(() => {
  if (props.cell.output?.contentType !== 'text/markdown' || !props.cell.output.markdownText) {
    return ''
  }
  return renderMarkdownToHtml(props.cell.output.markdownText)
})
</script>

<template>
  <div class="cell" :class="statusClass" data-testid="notebook-cell">
    <!-- Left gutter -->
    <div class="cell-gutter">
      <span class="status-dot" :title="cell.status">{{ statusLabel }}</span>
      <div class="cell-actions">
        <button
          title="Run (Shift+Enter)"
          :disabled="environmentMutationActive"
          @click="emit('run', cell.id)"
        >
          &#x25B6;
        </button>
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
        <div class="cell-meta-main">
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
          <span
            v-if="hasAnnotationOverrides"
            class="annotation-badge"
            :title="annotationSummaryTitle"
          >
            source overrides
          </span>
          <span
            v-for="chip in visibleAnnotationSummaryChips"
            :key="chip"
            class="annotation-summary-chip"
            :title="annotationSummaryTitle"
          >
            {{ chip }}
          </span>
          <span
            v-if="hiddenAnnotationSummaryCount"
            class="annotation-summary-chip"
            :title="annotationSummaryTitle"
          >
            +{{ hiddenAnnotationSummaryCount }}
          </span>
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
        </div>
        <div class="cell-meta-actions">
          <span v-if="durationLabel" class="duration">{{ durationLabel }}</span>
          <button
            v-if="canExplainStaleness"
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
      </div>

      <div
        v-if="showRemoteExecutionSummary && remoteExecutionSummary"
        class="remote-execution-summary"
        :class="`tone-${remoteExecutionSummary.tone}`"
      >
        <span class="remote-execution-label">{{ remoteExecutionSummary.label }}</span>
        <span class="remote-execution-detail">{{ remoteExecutionSummary.detail }}</span>
        <span v-if="lastRemoteWorkerLabel" class="remote-execution-pill">
          {{ lastRemoteWorkerLabel }}
        </span>
        <span v-if="lastRemoteTransportLabel" class="remote-execution-pill">
          {{ lastRemoteTransportLabel }}
        </span>
        <span v-if="lastRemoteBuildId" class="remote-execution-pill">
          build {{ lastRemoteBuildId }}
        </span>
        <span v-if="lastRemoteBuildState" class="remote-execution-pill">
          {{ lastRemoteBuildState }}
        </span>
        <span v-if="lastRemoteErrorCode" class="remote-execution-pill">
          {{ lastRemoteErrorCode }}
        </span>
        <span v-if="effectiveWorkerHealthLabel" class="remote-execution-pill">
          {{ effectiveWorkerHealthLabel }}
        </span>
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

        <div class="annotation-panel">
          <div class="annotation-header">
            <div>
              <div class="annotation-title">Source Overrides</div>
              <div class="annotation-copy">
                Optional <code># @...</code> directives stored in cell source. These override saved
                config when present.
              </div>
            </div>
            <button
              class="annotation-toggle"
              :disabled="!connected"
              @click="toggleAnnotationEditor"
            >
              {{
                showAnnotationEditor
                  ? 'Hide editor'
                  : hasAnnotationOverrides
                    ? 'Edit overrides'
                    : 'Add override'
              }}
            </button>
          </div>

          <div
            v-if="hasAnnotationOverrides"
            class="annotation-summary-row"
            :title="annotationSummaryTitle"
          >
            <span class="annotation-summary-label">Active</span>
            <span
              v-for="chip in annotationSummaryChips"
              :key="`panel-${chip}`"
              class="annotation-summary-chip"
            >
              {{ chip }}
            </span>
          </div>

          <div v-else class="annotation-copy annotation-copy-muted">
            No source overrides are currently set for this cell.
          </div>

          <div v-if="showAnnotationEditor" class="annotation-editor-grid">
            <WorkerConfigEditor
              :worker="cell.annotations?.worker ?? null"
              :options="availableWorkers"
              title="Annotation Worker"
              compact
              :read-only="!connected"
              @save="saveAnnotationWorker"
            />

            <TimeoutConfigEditor
              :timeout="cell.annotations?.timeout ?? null"
              title="Annotation Timeout"
              compact
              :read-only="!connected"
              @save="saveAnnotationTimeout"
            />

            <EnvVarsEditor
              :env="cell.annotations?.env || {}"
              title="Annotation Env"
              compact
              :read-only="!connected"
              @save="saveAnnotationEnv"
            />

            <MountListEditor
              :mounts="cell.annotations?.mounts || []"
              title="Annotation Mounts"
              compact
              :read-only="!connected"
              :show-pin="false"
              @save="saveAnnotationMounts"
            />
          </div>
        </div>
      </div>

      <!-- v1.1: Causality chain detail (expanded) -->
      <div v-if="showCausality && canExplainStaleness && cell.causality" class="causality-panel">
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
            <span v-if="lastRemoteWorkerLabel" class="remote-error-pill">
              {{ lastRemoteWorkerLabel }}
            </span>
            <span v-if="lastRemoteTransportLabel" class="remote-error-pill">
              {{ lastRemoteTransportLabel }}
            </span>
            <span v-if="lastRemoteBuildId" class="remote-error-pill">
              {{ lastRemoteBuildId }}
            </span>
            <span v-if="lastRemoteBuildState" class="remote-error-pill">
              {{ lastRemoteBuildState }}
            </span>
            <span v-if="lastRemoteErrorCode" class="remote-error-pill">
              {{ lastRemoteErrorCode }}
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
            <button
              class="btn-install"
              :disabled="installInProgress || environmentMutationActive"
              @click="installSuggestedPackage"
            >
              {{
                installInProgress
                  ? `Installing ${installTargetPackage}`
                  : `Install ${cell.suggestInstall}`
              }}
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
          v-else-if="cell.output.contentType === 'image/png' && cell.output.inlineDataUrl"
          class="output-image"
        >
          <img
            :src="cell.output.inlineDataUrl"
            alt="Cell output"
            :width="cell.output.width || undefined"
            :height="cell.output.height || undefined"
          />
        </div>
        <div
          v-else-if="cell.output.contentType === 'text/markdown' && cell.output.markdownText"
          class="output-markdown"
          v-html="renderedMarkdownOutput"
        ></div>
        <div
          v-else-if="cell.output.scalar !== undefined && !isConsoleOnly(cell.output.scalar)"
          class="output-scalar"
        >
          <pre>{{ formatScalar(cell.output.scalar) }}</pre>
        </div>
      </div>

      <div v-if="installCompleted && installTargetPackage" class="install-complete-hint">
        Installed <code>{{ installTargetPackage }}</code
        >. Re-run the cell to continue.
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
  min-width: 0;
}
.cell-meta-main {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 12px;
}
.cell-meta-actions {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-left: auto;
  flex-shrink: 0;
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
  white-space: nowrap;
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
.remote-execution-summary {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-bottom: 1px solid #2a2a3c;
  background: #11111b;
}
.remote-execution-summary.tone-info {
  background: #11111b;
}
.remote-execution-summary.tone-success {
  background: #13221b;
}
.remote-execution-summary.tone-warning {
  background: #201b12;
}
.remote-execution-summary.tone-error {
  background: #24161b;
}
.remote-execution-label {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #a6adc8;
}
.remote-execution-detail {
  font-size: 12px;
  color: #cdd6f4;
}
.remote-execution-pill {
  padding: 2px 8px;
  border-radius: 999px;
  background: #313244;
  color: #89b4fa;
  font-size: 11px;
  font-weight: 600;
}
.remote-execution-summary.tone-success .remote-execution-pill {
  color: #a6e3a1;
}
.remote-execution-summary.tone-warning .remote-execution-pill {
  color: #f9e2af;
}
.remote-execution-summary.tone-error .remote-execution-pill {
  color: #f38ba8;
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
.annotation-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}
.annotation-badge {
  background: #f9e2af22;
  color: #f9e2af;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
  font-weight: 600;
}
.annotation-summary-chip {
  background: #fab38722;
  color: #fab387;
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
}
.annotation-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: #f9e2af;
}
.annotation-toggle {
  background: #313244;
  border: 1px solid #45475a;
  color: #f9e2af;
  padding: 6px 10px;
  border-radius: 6px;
  font-size: 12px;
  cursor: pointer;
  white-space: nowrap;
}
.annotation-toggle:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}
.annotation-copy {
  color: #a6adc8;
  font-size: 12px;
  line-height: 1.4;
}
.annotation-copy code {
  color: #f9e2af;
}
.annotation-copy-muted {
  color: #6c7086;
}
.annotation-summary-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
}
.annotation-summary-label {
  color: #f9e2af;
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}
.annotation-editor-grid {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding-top: 4px;
  border-top: 1px solid #313244;
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

@media (max-width: 900px) {
  .cell-meta {
    flex-direction: column;
    align-items: stretch;
  }

  .cell-meta-actions {
    margin-left: 0;
    justify-content: flex-end;
    flex-wrap: wrap;
  }

  .annotation-header {
    flex-direction: column;
    align-items: stretch;
  }
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
.btn-install:disabled {
  background: #6c7086;
  color: #cdd6f4;
  cursor: wait;
}
.install-complete-hint {
  margin-top: 8px;
  padding: 8px 10px;
  background: #a6e3a115;
  border: 1px solid #a6e3a133;
  border-radius: 6px;
  color: #a6e3a1;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  font-size: 13px;
}
.install-complete-hint code {
  color: #cdd6f4;
}
.output-table-wrap {
  overflow-x: auto;
}
.output-image {
  overflow-x: auto;
}
.output-image img {
  display: block;
  max-width: 100%;
  height: auto;
  border: 1px solid #313244;
  border-radius: 6px;
  background: #11111b;
}
.output-markdown {
  color: #cdd6f4;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
  line-height: 1.6;
}
.output-markdown :deep(h1),
.output-markdown :deep(h2),
.output-markdown :deep(h3),
.output-markdown :deep(h4),
.output-markdown :deep(h5),
.output-markdown :deep(h6) {
  margin: 0 0 8px;
  color: #f5e0dc;
}
.output-markdown :deep(p),
.output-markdown :deep(blockquote),
.output-markdown :deep(pre),
.output-markdown :deep(table),
.output-markdown :deep(ul),
.output-markdown :deep(ol) {
  margin: 0 0 10px;
}
.output-markdown :deep(code) {
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 12px;
  background: #11111b;
  color: #f9e2af;
  padding: 1px 4px;
  border-radius: 4px;
}
.output-markdown :deep(pre) {
  overflow-x: auto;
  padding: 10px 12px;
  border: 1px solid #313244;
  border-radius: 6px;
  background: #11111b;
}
.output-markdown :deep(pre code) {
  padding: 0;
  background: transparent;
}
.output-markdown :deep(blockquote) {
  padding-left: 12px;
  border-left: 3px solid #89b4fa55;
  color: #bac2de;
}
.output-markdown :deep(a) {
  color: #89b4fa;
}
.output-markdown :deep(table) {
  width: 100%;
  border-collapse: collapse;
}
.output-markdown :deep(th),
.output-markdown :deep(td) {
  padding: 6px 10px;
  border: 1px solid #313244;
  text-align: left;
}
.output-markdown :deep(th) {
  color: #89b4fa;
  background: #181825;
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
