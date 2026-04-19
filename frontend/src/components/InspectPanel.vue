<script setup lang="ts">
import { ref, nextTick, watch, computed, onMounted } from 'vue'
import { useNotebook } from '../stores/notebook'
import type { CellId } from '../types/notebook'
import { useStrata, type CellIterationInfo } from '../composables/useStrata'

const props = defineProps<{ cellId: CellId }>()

const { inspectReadyFor, inspectHistoryFor, evalInspect, closeInspect, cellMap, notebook } =
  useNotebook()
const strata = useStrata()

const inputExpr = ref('')
const historyEl = ref<HTMLElement | null>(null)

const ready = computed(() => inspectReadyFor(props.cellId))
const history = computed(() => inspectHistoryFor(props.cellId))

const iterations = ref<CellIterationInfo[]>([])
const selectedIteration = ref<number | null>(null)
const iterationsError = ref<string | null>(null)
const iterationsLoading = ref(false)
const uriCopiedHint = ref(false)

const currentCell = computed(() => cellMap.value.get(props.cellId))
const carryVariable = computed(() => currentCell.value?.annotations?.loop?.carry ?? null)

const cellLabel = computed(() => {
  const cell = currentCell.value
  // Prefer the @name annotation (user-given cell name), falling back to
  // the short cell id. We used to show cell.defines[0] here, but the
  // REPL is scoped to the cell's *inputs*, not its defines — showing a
  // define name was misleading.
  if (cell?.annotations?.name) return cell.annotations.name
  return props.cellId.slice(0, 8)
})

const selectedIterationInfo = computed(
  () => iterations.value.find((entry) => entry.iteration === selectedIteration.value) ?? null,
)

async function refreshIterations() {
  if (!carryVariable.value) {
    iterations.value = []
    selectedIteration.value = null
    return
  }
  const notebookId = notebook.id
  if (!notebookId) return
  iterationsLoading.value = true
  iterationsError.value = null
  try {
    const result = await strata.listCellIterations(notebookId, props.cellId)
    iterations.value = result.iterations
    if (!iterations.value.length) {
      selectedIteration.value = null
      return
    }
    const last = iterations.value[iterations.value.length - 1].iteration
    if (
      selectedIteration.value === null ||
      !iterations.value.some((entry) => entry.iteration === selectedIteration.value)
    ) {
      selectedIteration.value = last
    }
  } catch (err) {
    iterationsError.value = err instanceof Error ? err.message : String(err)
  } finally {
    iterationsLoading.value = false
  }
}

onMounted(() => {
  refreshIterations()
})

// Refresh whenever a new iteration completes (the progress badge updates
// cell.loopProgress.iteration) so the picker tracks live progress.
watch(
  () => currentCell.value?.loopProgress?.iteration ?? -1,
  () => {
    refreshIterations()
  },
)

// Also refresh when the user opens the panel on a different cell.
watch(
  () => props.cellId,
  () => {
    selectedIteration.value = null
    refreshIterations()
  },
)

async function copyArtifactUri() {
  const info = selectedIterationInfo.value
  if (!info) return
  try {
    await navigator.clipboard.writeText(info.artifactUri)
    uriCopiedHint.value = true
    setTimeout(() => {
      uriCopiedHint.value = false
    }, 1500)
  } catch {
    // Clipboard API can fail in insecure contexts; silently no-op.
  }
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / (1024 * 1024)).toFixed(1)} MB`
}

// Auto-scroll history when new entries arrive
watch(
  () => history.value.length,
  async () => {
    await nextTick()
    if (historyEl.value) {
      historyEl.value.scrollTop = historyEl.value.scrollHeight
    }
  },
)

function submitExpr() {
  const expr = inputExpr.value.trim()
  if (!expr) return
  evalInspect(props.cellId, expr)
  inputExpr.value = ''
}

function handleKeydown(e: KeyboardEvent) {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault()
    submitExpr()
  }
}

function close() {
  closeInspect(props.cellId)
}
</script>

<template>
  <div class="inspect-panel">
    <div class="inspect-header">
      <span class="inspect-title">
        Inspect inputs of <code>{{ cellLabel }}</code>
      </span>
      <span v-if="!ready" class="inspect-loading">loading...</span>
      <button class="inspect-close-btn" title="Close inspect REPL" @click="close">&times;</button>
    </div>

    <div v-if="carryVariable" class="iteration-picker">
      <div class="iteration-picker-header">
        <span class="iteration-picker-title">
          Iterations of <code>{{ carryVariable }}</code>
        </span>
        <span v-if="iterationsLoading" class="iteration-picker-loading">refreshing...</span>
      </div>
      <div v-if="iterationsError" class="iteration-picker-error">
        {{ iterationsError }}
      </div>
      <div v-else-if="!iterations.length" class="iteration-picker-empty">
        No iteration artifacts yet. Run the cell to populate iterations.
      </div>
      <div v-else class="iteration-picker-row">
        <select
          v-model.number="selectedIteration"
          class="iteration-picker-select"
          title="Select an iteration to inspect"
        >
          <option v-for="entry in iterations" :key="entry.iteration" :value="entry.iteration">
            iter {{ entry.iteration }} · {{ formatBytes(entry.byteSize) }}
          </option>
        </select>
        <button
          v-if="selectedIterationInfo"
          class="iteration-picker-copy"
          :title="`Copy ${selectedIterationInfo.artifactUri}`"
          @click="copyArtifactUri"
        >
          {{ uriCopiedHint ? 'copied' : 'copy URI' }}
        </button>
      </div>
      <div v-if="selectedIterationInfo" class="iteration-picker-meta">
        <span class="iteration-picker-pill">{{ selectedIterationInfo.contentType }}</span>
        <span v-if="selectedIterationInfo.rowCount !== null" class="iteration-picker-pill">
          {{ selectedIterationInfo.rowCount }} rows
        </span>
        <code class="iteration-picker-uri">{{ selectedIterationInfo.artifactUri }}</code>
      </div>
    </div>

    <div ref="historyEl" class="inspect-history">
      <div v-if="!history.length && ready" class="inspect-hint">
        Type an expression to inspect cell inputs. Variables from upstream cells are pre-loaded.
      </div>
      <div v-for="(entry, i) in history" :key="i" class="inspect-entry">
        <div class="inspect-expr">
          <span class="prompt-marker">&gt;&gt;&gt;</span> {{ entry.expr }}
        </div>
        <div v-if="entry.stdout" class="inspect-stdout">{{ entry.stdout }}</div>
        <div v-if="entry.error" class="inspect-error">{{ entry.error }}</div>
        <div v-else-if="entry.result" class="inspect-result">
          <span v-if="entry.type" class="inspect-type">{{ entry.type }}</span>
          <pre class="inspect-value">{{ entry.result }}</pre>
        </div>
      </div>
    </div>

    <div class="inspect-input-row">
      <span class="prompt-marker">&gt;&gt;&gt;</span>
      <input
        v-model="inputExpr"
        class="inspect-input"
        placeholder="expression..."
        :disabled="!ready"
        @keydown="handleKeydown"
      />
      <button class="inspect-run-btn" :disabled="!ready || !inputExpr.trim()" @click="submitExpr">
        Eval
      </button>
    </div>
  </div>
</template>

<style scoped>
.inspect-panel {
  background: #11111b;
  border: 1px solid #89b4fa44;
  border-radius: 8px;
  margin-top: 4px;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  max-height: 360px;
}
.inspect-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  border-bottom: 1px solid #2a2a3c;
  font-size: 12px;
  background: #181825;
}
.inspect-title {
  color: #89b4fa;
  font-weight: 600;
}
.inspect-title code {
  color: #cdd6f4;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
}
.inspect-loading {
  color: #f9e2af;
  font-size: 10px;
  animation: pulse 1s infinite;
}
@keyframes pulse {
  50% {
    opacity: 0.4;
  }
}
.inspect-close-btn {
  margin-left: auto;
  background: none;
  border: none;
  color: #6c7086;
  cursor: pointer;
  font-size: 16px;
  padding: 0 4px;
  line-height: 1;
}
.inspect-close-btn:hover {
  color: #f38ba8;
}

.inspect-history {
  flex: 1;
  overflow-y: auto;
  padding: 8px 12px;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 12px;
  min-height: 60px;
  max-height: 260px;
}
.inspect-hint {
  color: #6c7086;
  font-size: 11px;
  font-family: inherit;
}
.inspect-entry {
  margin-bottom: 8px;
}
.inspect-expr {
  color: #cdd6f4;
  white-space: pre-wrap;
  word-break: break-all;
}
.prompt-marker {
  color: #89b4fa;
  user-select: none;
  margin-right: 4px;
}
.inspect-stdout {
  color: #a6adc8;
  white-space: pre-wrap;
  padding-left: 24px;
}
.inspect-error {
  color: #f38ba8;
  white-space: pre-wrap;
  padding-left: 24px;
  font-size: 11px;
}
.inspect-result {
  padding-left: 24px;
}
.inspect-type {
  color: #6c7086;
  font-size: 10px;
  display: block;
  margin-bottom: 1px;
}
.inspect-value {
  color: #a6e3a1;
  white-space: pre-wrap;
  margin: 0;
  font-size: 12px;
  max-height: 120px;
  overflow-y: auto;
}

.inspect-input-row {
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 6px 12px;
  border-top: 1px solid #2a2a3c;
  background: #181825;
}
.inspect-input {
  flex: 1;
  background: #1e1e2e;
  border: 1px solid #2a2a3c;
  border-radius: 4px;
  color: #cdd6f4;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 12px;
  padding: 4px 8px;
  outline: none;
}
.inspect-input:focus {
  border-color: #89b4fa;
}
.inspect-input::placeholder {
  color: #45475a;
}
.inspect-input:disabled {
  opacity: 0.5;
}
.inspect-run-btn {
  background: #89b4fa22;
  color: #89b4fa;
  border: 1px solid #89b4fa44;
  border-radius: 4px;
  padding: 4px 10px;
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
}
.inspect-run-btn:hover:not(:disabled) {
  background: #89b4fa33;
}
.inspect-run-btn:disabled {
  opacity: 0.4;
  cursor: default;
}

.iteration-picker {
  padding: 6px 12px;
  border-bottom: 1px solid #2a2a3c;
  background: #181825;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.iteration-picker-header {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 11px;
}
.iteration-picker-title {
  color: #89b4fa;
  font-weight: 600;
}
.iteration-picker-title code {
  color: #cdd6f4;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
}
.iteration-picker-loading {
  color: #f9e2af;
  font-size: 10px;
}
.iteration-picker-error {
  color: #f38ba8;
  font-size: 11px;
}
.iteration-picker-empty {
  color: #6c7086;
  font-size: 11px;
  font-style: italic;
}
.iteration-picker-row {
  display: flex;
  align-items: center;
  gap: 6px;
}
.iteration-picker-select {
  flex: 1;
  background: #1e1e2e;
  border: 1px solid #2a2a3c;
  border-radius: 4px;
  color: #cdd6f4;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 12px;
  padding: 3px 6px;
  outline: none;
}
.iteration-picker-select:focus {
  border-color: #89b4fa;
}
.iteration-picker-copy {
  background: #89b4fa22;
  color: #89b4fa;
  border: 1px solid #89b4fa44;
  border-radius: 4px;
  padding: 3px 10px;
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
}
.iteration-picker-copy:hover {
  background: #89b4fa33;
}
.iteration-picker-meta {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
  font-size: 10px;
}
.iteration-picker-pill {
  background: #11111b;
  color: #a6adc8;
  padding: 1px 6px;
  border-radius: 3px;
  border: 1px solid #2a2a3c;
}
.iteration-picker-uri {
  color: #6c7086;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 10px;
  word-break: break-all;
}
</style>
