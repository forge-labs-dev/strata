<script setup lang="ts">
import { ref, watch } from 'vue'
import type { EditableWorkerSpec, WorkerSpec } from '../types/notebook'

interface WorkerDraft {
  name: string
  backend: WorkerSpec['backend']
  enabled: boolean
  runtimeId: string
  executorUrl: string
  transport: 'direct' | 'signed'
  strataUrl: string
  extraConfig: Record<string, unknown>
}

const props = withDefaults(
  defineProps<{
    workers: EditableWorkerSpec[]
    title?: string
    compact?: boolean
    showEnabled?: boolean
    readOnly?: boolean
    error?: string | null
  }>(),
  {
    title: 'Registered Workers',
    compact: false,
    showEnabled: false,
    readOnly: false,
    error: null,
  },
)

const emit = defineEmits<{
  save: [workers: EditableWorkerSpec[]]
}>()

const draft = ref<WorkerDraft[]>([])

function toDraft(workers: EditableWorkerSpec[]): WorkerDraft[] {
  return workers.map((worker) => ({
    name: worker.name,
    backend: worker.backend,
    enabled: worker.enabled !== false,
    runtimeId: worker.runtimeId ?? '',
    executorUrl: typeof worker.config?.url === 'string' ? worker.config.url : '',
    transport:
      String(worker.config?.transport || 'direct')
        .trim()
        .toLowerCase() === 'signed'
        ? 'signed'
        : 'direct',
    strataUrl: typeof worker.config?.strata_url === 'string' ? worker.config.strata_url : '',
    extraConfig:
      worker.config && typeof worker.config === 'object'
        ? Object.fromEntries(
            Object.entries(worker.config).filter(
              ([key]) => !['url', 'transport', 'strata_url'].includes(key),
            ),
          )
        : {},
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
    enabled: true,
    runtimeId: '',
    executorUrl: '',
    transport: 'direct',
    strataUrl: '',
    extraConfig: {},
  })
}

function removeWorker(index: number) {
  draft.value.splice(index, 1)
}

function save() {
  emit(
    'save',
    draft.value
      .map((worker) => {
        const config =
          worker.backend === 'executor'
            ? {
                ...worker.extraConfig,
                ...(worker.executorUrl.trim() ? { url: worker.executorUrl.trim() } : {}),
                ...(worker.transport === 'signed' ? { transport: 'signed' } : {}),
                ...(worker.transport === 'signed' && worker.strataUrl.trim()
                  ? { strata_url: worker.strataUrl.trim() }
                  : {}),
              }
            : {}

        return {
          name: worker.name.trim(),
          backend: worker.backend,
          ...(props.showEnabled ? { enabled: worker.enabled } : {}),
          runtimeId: worker.runtimeId.trim() || null,
          config,
        }
      })
      .filter((worker) => worker.name),
  )
}
</script>

<template>
  <div class="worker-list-editor" :class="{ compact: compact, 'show-enabled': showEnabled }">
    <div class="worker-list-header">
      <span class="worker-list-title">{{ title }}</span>
      <button v-if="!readOnly" class="worker-list-add" @click="addWorker">+ Worker</button>
    </div>

    <div v-if="draft.length === 0" class="worker-list-empty">
      {{ readOnly ? 'No notebook workers configured' : 'No workers configured yet' }}
    </div>

    <div v-for="(worker, index) in draft" :key="`${index}-${worker.name}`" class="worker-list-row">
      <label v-if="showEnabled" class="worker-enabled-toggle">
        <input v-model="worker.enabled" type="checkbox" :disabled="readOnly" />
        <span>{{ worker.enabled ? 'enabled' : 'disabled' }}</span>
      </label>
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
      <select
        v-model="worker.transport"
        class="worker-input"
        :disabled="readOnly || worker.backend !== 'executor'"
      >
        <option value="direct">direct</option>
        <option value="signed">signed</option>
      </select>
      <input
        v-model="worker.strataUrl"
        class="worker-input"
        type="text"
        placeholder="strata url (signed only, optional)"
        :disabled="readOnly || worker.backend !== 'executor' || worker.transport !== 'signed'"
      />
      <button v-if="!readOnly" class="worker-list-remove" @click="removeWorker(index)">×</button>
    </div>

    <div
      v-for="(worker, index) in draft"
      :key="`hint-${index}-${worker.name}`"
      class="worker-list-hint"
    >
      <template v-if="worker.backend === 'executor' && worker.transport === 'signed'">
        Signed transport uses the Strata build + signed-URL path. Set <code>strata_url</code>
        when the worker cannot reach the default server URL.
      </template>
      <template v-else-if="worker.backend === 'executor'">
        Direct transport uploads notebook inputs to the executor over HTTP and returns the output
        bundle directly.
      </template>
      <template v-else> Local workers execute in the notebook runtime on this machine. </template>
    </div>

    <div v-if="!readOnly" class="worker-list-actions">
      <button class="worker-list-save" @click="save">Save workers</button>
    </div>

    <div v-if="error" class="worker-list-error">
      {{ error }}
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
  grid-template-columns:
    110px
    minmax(100px, 1fr)
    110px
    minmax(130px, 1fr)
    minmax(180px, 2fr)
    110px
    minmax(180px, 2fr)
    28px;
  gap: 6px;
}

.worker-list-editor:not(.show-enabled) .worker-list-row {
  grid-template-columns:
    minmax(100px, 1fr)
    110px
    minmax(130px, 1fr)
    minmax(180px, 2fr)
    110px
    minmax(180px, 2fr)
    28px;
}

.worker-enabled-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 8px;
  border-radius: 6px;
  border: 1px solid #313244;
  background: #181825;
  color: #bac2de;
  font-size: 12px;
}

.worker-enabled-toggle input {
  margin: 0;
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

.worker-list-hint {
  margin-top: -2px;
  color: #6c7086;
  font-size: 12px;
  line-height: 1.4;
}

.worker-list-hint code {
  color: #89b4fa;
}

.worker-list-actions {
  display: flex;
  justify-content: flex-end;
}

.worker-list-error {
  color: #f38ba8;
  font-size: 12px;
  line-height: 1.4;
}

@media (max-width: 920px) {
  .worker-list-row {
    grid-template-columns: 1fr 1fr;
  }
}
</style>
