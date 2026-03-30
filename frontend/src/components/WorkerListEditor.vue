<script setup lang="ts">
import { ref, watch } from 'vue'
import type { WorkerSpec } from '../types/notebook'

interface WorkerDraft {
  name: string
  backend: WorkerSpec['backend']
  runtimeId: string
  executorUrl: string
}

const props = withDefaults(defineProps<{
  workers: WorkerSpec[]
  title?: string
  compact?: boolean
  readOnly?: boolean
}>(), {
  title: 'Registered Workers',
  compact: false,
  readOnly: false,
})

const emit = defineEmits<{
  save: [workers: WorkerSpec[]]
}>()

const draft = ref<WorkerDraft[]>([])

function toDraft(workers: WorkerSpec[]): WorkerDraft[] {
  return workers.map((worker) => ({
    name: worker.name,
    backend: worker.backend,
    runtimeId: worker.runtimeId ?? '',
    executorUrl:
      typeof worker.config?.url === 'string' ? worker.config.url : '',
  }))
}

watch(
  () => props.workers,
  (workers) => {
    draft.value = toDraft(workers)
  },
  { immediate: true, deep: true },
)

function addWorker() {
  draft.value.push({
    name: '',
    backend: 'executor',
    runtimeId: '',
    executorUrl: '',
  })
}

function removeWorker(index: number) {
  draft.value.splice(index, 1)
}

function save() {
  emit(
    'save',
    draft.value
      .map((worker) => ({
        name: worker.name.trim(),
        backend: worker.backend,
        runtimeId: worker.runtimeId.trim() || null,
        config:
          worker.backend === 'executor' && worker.executorUrl.trim()
            ? { url: worker.executorUrl.trim() }
            : {},
      }))
      .filter((worker) => worker.name),
  )
}
</script>

<template>
  <div class="worker-list-editor" :class="{ compact: compact }">
    <div class="worker-list-header">
      <span class="worker-list-title">{{ title }}</span>
      <button v-if="!readOnly" class="worker-list-add" @click="addWorker">+ Worker</button>
    </div>

    <div v-if="draft.length === 0" class="worker-list-empty">
      {{ readOnly ? 'No notebook workers configured' : 'No workers configured yet' }}
    </div>

    <div
      v-for="(worker, index) in draft"
      :key="`${index}-${worker.name}`"
      class="worker-list-row"
    >
      <input
        v-model="worker.name"
        class="worker-input"
        type="text"
        placeholder="name"
        :disabled="readOnly"
      />
      <select v-model="worker.backend" class="worker-input" :disabled="readOnly">
        <option value="local">local</option>
        <option value="executor">executor</option>
      </select>
      <input
        v-model="worker.runtimeId"
        class="worker-input"
        type="text"
        placeholder="runtime id (optional)"
        :disabled="readOnly"
      />
      <input
        v-model="worker.executorUrl"
        class="worker-input"
        type="text"
        placeholder="executor url (executor only)"
        :disabled="readOnly"
      />
      <button v-if="!readOnly" class="worker-list-remove" @click="removeWorker(index)">×</button>
    </div>

    <div v-if="!readOnly" class="worker-list-actions">
      <button class="worker-list-save" @click="save">Save workers</button>
    </div>
  </div>
</template>

<style scoped>
.worker-list-editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.worker-list-editor.compact {
  gap: 6px;
}

.worker-list-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.worker-list-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #a6adc8;
}

.worker-list-add,
.worker-list-save,
.worker-list-remove {
  background: #313244;
  border: 1px solid #45475a;
  color: #cdd6f4;
  border-radius: 6px;
  cursor: pointer;
}

.worker-list-add,
.worker-list-save {
  padding: 6px 10px;
  font-size: 12px;
}

.worker-list-remove {
  width: 28px;
  min-width: 28px;
  height: 30px;
  font-size: 16px;
}

.worker-list-row {
  display: grid;
  grid-template-columns: minmax(100px, 1fr) 110px minmax(130px, 1fr) minmax(180px, 2fr) 28px;
  gap: 6px;
}

.worker-input {
  width: 100%;
  min-width: 0;
  padding: 6px 8px;
  background: #11111b;
  border: 1px solid #313244;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 12px;
}

.worker-list-empty {
  color: #6c7086;
  font-size: 12px;
}

.worker-list-actions {
  display: flex;
  justify-content: flex-end;
}

@media (max-width: 920px) {
  .worker-list-row {
    grid-template-columns: 1fr 1fr;
  }
}
</style>
