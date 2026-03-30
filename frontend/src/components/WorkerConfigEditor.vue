<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import type { WorkerCatalogEntry } from '../types/notebook'
import { workerTransportLabel } from '../utils/notebookWorkers'

const props = withDefaults(
  defineProps<{
    worker: string | null
    options?: WorkerCatalogEntry[]
    title?: string
    compact?: boolean
    readOnly?: boolean
    error?: string | null
  }>(),
  {
    title: 'Worker',
    compact: false,
    readOnly: false,
    options: () => [],
    error: null,
  },
)

const emit = defineEmits<{
  save: [worker: string | null]
}>()

const draft = ref('')

watch(
  () => props.worker,
  (worker) => {
    draft.value = worker ?? ''
  },
  { immediate: true },
)

const normalizedOptions = computed(() => {
  const options = props.options.filter((option) => option.name !== 'local')
  if (draft.value && !options.some((option) => option.name === draft.value)) {
    options.push({
      name: draft.value,
      backend: 'executor',
      runtimeId: null,
      config: {},
      health: 'unavailable',
      source: 'referenced',
      allowed: false,
    })
  }
  return options
})

const selectedOption = computed(
  () => normalizedOptions.value.find((option) => option.name === draft.value) ?? null,
)

const canSave = computed(() => {
  if (!draft.value.trim()) return true
  const option = selectedOption.value
  if (!option) return true
  if (option.allowed === false) return false
  if (option.backend === 'executor' && option.health === 'unavailable') return false
  return true
})

function save() {
  if (!canSave.value) return
  const normalized = draft.value.trim()
  emit('save', normalized || null)
}

function healthLabel(health: WorkerCatalogEntry['health']) {
  switch (health) {
    case 'healthy':
      return 'healthy'
    case 'unavailable':
      return 'missing'
    default:
      return 'unknown'
  }
}

function sourceLabel(option: WorkerCatalogEntry) {
  switch (option.source) {
    case 'server':
      return 'server'
    case 'notebook':
      return 'notebook'
    case 'referenced':
      return 'referenced'
    default:
      return option.backend
  }
}
</script>

<template>
  <div class="worker-editor" :class="{ compact: compact }">
    <div class="worker-editor-header">
      <span class="worker-editor-title">{{ title }}</span>
    </div>

    <div class="worker-row">
      <select v-model="draft" class="worker-select" :disabled="readOnly">
        <option value="">local</option>
        <option v-for="option in normalizedOptions" :key="option.name" :value="option.name">
          {{ option.name }} · {{ sourceLabel(option) }} · {{ workerTransportLabel(option) }} ·
          {{ healthLabel(option.health) }}{{ option.allowed === false ? ' · blocked' : '' }}
        </option>
      </select>
      <button v-if="!readOnly" class="worker-save" :disabled="!canSave" @click="save">Save</button>
    </div>

    <div v-if="draft && selectedOption?.allowed === false" class="worker-hint worker-hint-warning">
      This worker is visible for reference but blocked by the current server policy.
    </div>
    <div
      v-else-if="
        draft && selectedOption?.backend === 'executor' && selectedOption?.health === 'unavailable'
      "
      class="worker-hint worker-hint-warning"
    >
      This executor is currently unreachable. Saving it would likely fail execution immediately.
    </div>
    <div v-else-if="draft" class="worker-hint">
      Empty means local execution. Worker selection affects runtime identity, transport, and cache
      lineage.
    </div>
    <div v-else class="worker-hint">Empty means local execution.</div>

    <div v-if="error" class="worker-error">
      {{ error }}
    </div>
  </div>
</template>

<style scoped>
.worker-editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.worker-editor.compact {
  gap: 6px;
}

.worker-editor-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.worker-editor-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #a6adc8;
}

.worker-row {
  display: flex;
  gap: 8px;
}

.worker-select {
  flex: 1;
  min-width: 0;
  padding: 6px 8px;
  background: #11111b;
  border: 1px solid #313244;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 12px;
}

.worker-save {
  padding: 6px 10px;
  font-size: 12px;
  background: #313244;
  border: 1px solid #45475a;
  color: #cdd6f4;
  border-radius: 6px;
  cursor: pointer;
}

.worker-save:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}

.worker-hint {
  color: #6c7086;
  font-size: 12px;
  line-height: 1.4;
}

.worker-hint-warning {
  color: #f9e2af;
}

.worker-error {
  color: #f38ba8;
  font-size: 12px;
  line-height: 1.4;
}
</style>
