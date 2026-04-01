<script setup lang="ts">
import { ref, watch } from 'vue'

interface EnvRow {
  localId: string
  key: string
  value: string
}

const props = withDefaults(
  defineProps<{
    env: Record<string, string>
    title?: string
    compact?: boolean
    readOnly?: boolean
  }>(),
  {
    title: 'Environment Variables',
    compact: false,
    readOnly: false,
  },
)

const emit = defineEmits<{
  save: [env: Record<string, string>]
}>()

const draft = ref<EnvRow[]>([])
let nextEnvRowId = 0

function nextLocalId(): string {
  nextEnvRowId += 1
  return `env-row-${nextEnvRowId}`
}

watch(
  () => props.env,
  (env) => {
    draft.value = Object.entries(env || {}).map(([key, value], index) => ({
      localId: draft.value[index]?.localId ?? nextLocalId(),
      key,
      value,
    }))
  },
  { immediate: true, deep: true },
)

function addRow() {
  draft.value.push({ localId: nextLocalId(), key: '', value: '' })
}

function removeRow(index: number) {
  draft.value.splice(index, 1)
}

function save() {
  const env = Object.fromEntries(
    draft.value.map((row) => [row.key.trim(), row.value] as const).filter(([key]) => key),
  )
  emit('save', env)
}
</script>

<template>
  <div class="env-editor" :class="{ compact: compact }">
    <div class="env-editor-header">
      <span class="env-editor-title">{{ title }}</span>
      <button v-if="!readOnly" class="env-add" @click="addRow">+ Var</button>
    </div>

    <div v-if="draft.length === 0" class="env-empty">
      {{ readOnly ? 'No annotation env vars' : 'No env vars configured' }}
    </div>

    <div v-for="(row, index) in draft" :key="row.localId" class="env-row">
      <input
        v-model="row.key"
        class="env-input env-key"
        type="text"
        placeholder="KEY"
        :disabled="readOnly"
      />
      <input
        v-model="row.value"
        class="env-input env-value"
        type="text"
        placeholder="value"
        :disabled="readOnly"
      />
      <button v-if="!readOnly" class="env-remove" @click="removeRow(index)">×</button>
    </div>

    <div v-if="!readOnly" class="env-actions">
      <button class="env-save" @click="save">Save env</button>
    </div>
  </div>
</template>

<style scoped>
.env-editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.env-editor.compact {
  gap: 6px;
}

.env-editor-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.env-editor-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #a6adc8;
}

.env-add,
.env-save,
.env-remove {
  background: #313244;
  border: 1px solid #45475a;
  color: #cdd6f4;
  border-radius: 6px;
  cursor: pointer;
}

.env-add,
.env-save {
  padding: 6px 10px;
  font-size: 12px;
}

.env-remove {
  width: 28px;
  min-width: 28px;
  height: 30px;
  font-size: 16px;
}

.env-row {
  display: grid;
  grid-template-columns: minmax(120px, 1fr) minmax(180px, 2fr) 28px;
  gap: 6px;
}

.env-input {
  width: 100%;
  min-width: 0;
  padding: 6px 8px;
  background: #11111b;
  border: 1px solid #313244;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 12px;
}

.env-empty {
  color: #6c7086;
  font-size: 12px;
}

.env-actions {
  display: flex;
  justify-content: flex-end;
}

@media (max-width: 920px) {
  .env-row {
    grid-template-columns: 1fr 1fr;
  }
}
</style>
