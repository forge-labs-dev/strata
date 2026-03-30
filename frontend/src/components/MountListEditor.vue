<script setup lang="ts">
import { ref, watch } from 'vue'
import type { MountSpec } from '../types/notebook'

const props = withDefaults(
  defineProps<{
    mounts: MountSpec[]
    title?: string
    compact?: boolean
    readOnly?: boolean
    showPin?: boolean
  }>(),
  {
    title: 'Mounts',
    compact: false,
    readOnly: false,
    showPin: true,
  },
)

const emit = defineEmits<{
  save: [mounts: MountSpec[]]
}>()

const draft = ref<MountSpec[]>([])

function cloneMounts(mounts: MountSpec[]): MountSpec[] {
  return mounts.map((mount) => ({ ...mount, pin: mount.pin ?? null }))
}

watch(
  () => props.mounts,
  (mounts) => {
    draft.value = cloneMounts(mounts)
  },
  { immediate: true, deep: true },
)

function addMount() {
  draft.value.push({
    name: '',
    uri: '',
    mode: 'ro',
    pin: null,
  })
}

function removeMount(index: number) {
  draft.value.splice(index, 1)
}

function save() {
  emit(
    'save',
    draft.value
      .map((mount) => ({
        ...mount,
        name: mount.name.trim(),
        uri: mount.uri.trim(),
        pin: mount.pin?.trim() || null,
      }))
      .filter((mount) => mount.name && mount.uri),
  )
}
</script>

<template>
  <div class="mount-editor" :class="{ compact: compact }">
    <div class="mount-editor-header">
      <span class="mount-editor-title">{{ title }}</span>
      <button v-if="!readOnly" class="mount-add" @click="addMount">+ Mount</button>
    </div>

    <div v-if="draft.length === 0" class="mount-empty">
      {{ readOnly ? 'No source annotations' : 'No mounts configured' }}
    </div>

    <div
      v-for="(mount, index) in draft"
      :key="`${index}-${mount.name}`"
      class="mount-row"
      :class="{ 'mount-row-no-pin': !showPin }"
    >
      <input
        v-model="mount.name"
        class="mount-input mount-name"
        type="text"
        placeholder="name"
        :disabled="readOnly"
      />
      <input
        v-model="mount.uri"
        class="mount-input mount-uri"
        type="text"
        placeholder="file:///... or s3://..."
        :disabled="readOnly"
      />
      <select v-model="mount.mode" class="mount-input mount-mode" :disabled="readOnly">
        <option value="ro">ro</option>
        <option value="rw">rw</option>
      </select>
      <input
        v-if="showPin"
        v-model="mount.pin"
        class="mount-input mount-pin"
        type="text"
        placeholder="pin (optional)"
        :disabled="readOnly"
      />
      <button v-if="!readOnly" class="mount-remove" @click="removeMount(index)">×</button>
    </div>

    <div v-if="!readOnly" class="mount-actions">
      <button class="mount-save" @click="save">Save mounts</button>
    </div>
  </div>
</template>

<style scoped>
.mount-editor {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.mount-editor.compact {
  gap: 6px;
}

.mount-editor-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.mount-editor-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #a6adc8;
}

.mount-add,
.mount-save,
.mount-remove {
  background: #313244;
  border: 1px solid #45475a;
  color: #cdd6f4;
  border-radius: 6px;
  cursor: pointer;
}

.mount-add,
.mount-save {
  padding: 6px 10px;
  font-size: 12px;
}

.mount-remove {
  width: 28px;
  min-width: 28px;
  height: 30px;
  font-size: 16px;
}

.mount-row {
  display: grid;
  grid-template-columns: minmax(90px, 1fr) minmax(220px, 2fr) 72px minmax(120px, 1fr) 28px;
  gap: 6px;
}

.mount-row-no-pin {
  grid-template-columns: minmax(90px, 1fr) minmax(220px, 2fr) 72px 28px;
}

.compact .mount-row {
  grid-template-columns: minmax(80px, 1fr) minmax(160px, 2fr) 64px minmax(100px, 1fr) 28px;
}

.compact .mount-row-no-pin {
  grid-template-columns: minmax(80px, 1fr) minmax(160px, 2fr) 64px 28px;
}

.mount-input {
  width: 100%;
  min-width: 0;
  padding: 6px 8px;
  background: #11111b;
  border: 1px solid #313244;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 12px;
}

.mount-input:disabled {
  opacity: 0.85;
}

.mount-empty {
  color: #6c7086;
  font-size: 12px;
}

.mount-actions {
  display: flex;
  justify-content: flex-end;
}

@media (max-width: 920px) {
  .mount-row,
  .compact .mount-row {
    grid-template-columns: 1fr 1fr;
  }
}
</style>
