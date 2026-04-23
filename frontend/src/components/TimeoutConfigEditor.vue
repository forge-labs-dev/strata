<script setup lang="ts">
import { ref, watch } from 'vue'

const props = withDefaults(
  defineProps<{
    timeout: number | null
    title?: string
    compact?: boolean
    readOnly?: boolean
  }>(),
  {
    title: 'Timeout',
    compact: false,
    readOnly: false,
  },
)

const emit = defineEmits<{
  save: [timeout: number | null]
}>()

const draft = ref('')

watch(
  () => props.timeout,
  (timeout) => {
    draft.value = timeout == null ? '' : String(timeout)
  },
  { immediate: true },
)

function save() {
  const normalized = draft.value.trim()
  emit('save', normalized ? Number(normalized) : null)
}
</script>

<template>
  <div class="timeout-editor" :class="{ compact: compact }">
    <div class="timeout-editor-title">{{ title }}</div>
    <div class="timeout-row">
      <input
        v-model="draft"
        class="timeout-input"
        type="number"
        min="0"
        step="0.1"
        placeholder="default"
        :disabled="readOnly"
      />
      <button v-if="!readOnly" class="timeout-save" @click="save">Save</button>
    </div>
    <div class="timeout-hint">Empty means use the inherited/default executor timeout.</div>
  </div>
</template>

<style scoped>
.timeout-editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.timeout-editor.compact {
  gap: 6px;
}

.timeout-editor-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--text-secondary);
}

.timeout-row {
  display: flex;
  gap: 8px;
}

.timeout-input {
  flex: 1;
  min-width: 0;
  padding: 6px 8px;
  background: var(--bg-base);
  border: 1px solid var(--bg-input);
  border-radius: 6px;
  color: var(--text-primary);
  font-size: 12px;
}

.timeout-save {
  padding: 6px 10px;
  font-size: 12px;
  background: var(--bg-input);
  border: 1px solid var(--border-strong);
  color: var(--text-primary);
  border-radius: 6px;
  cursor: pointer;
}

.timeout-hint {
  color: var(--text-muted);
  font-size: 12px;
  line-height: 1.4;
}
</style>
