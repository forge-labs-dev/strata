<script setup lang="ts">
import { ref, nextTick, watch, computed } from 'vue'
import { useNotebook } from '../stores/notebook'
import type { CellId } from '../types/notebook'

const props = defineProps<{ cellId: CellId }>()

const { inspectReadyFor, inspectHistoryFor, evalInspect, closeInspect, cellMap } = useNotebook()

const inputExpr = ref('')
const historyEl = ref<HTMLElement | null>(null)

const ready = computed(() => inspectReadyFor(props.cellId))
const history = computed(() => inspectHistoryFor(props.cellId))

const cellLabel = computed(() => {
  const cell = cellMap.value.get(props.cellId)
  // Prefer the @name annotation (user-given cell name), falling back to
  // the short cell id. We used to show cell.defines[0] here, but the
  // REPL is scoped to the cell's *inputs*, not its defines — showing a
  // define name was misleading.
  if (cell?.annotations?.name) return cell.annotations.name
  return props.cellId.slice(0, 8)
})

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
</style>
