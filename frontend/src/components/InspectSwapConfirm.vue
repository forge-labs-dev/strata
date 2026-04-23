<script setup lang="ts">
import { computed } from 'vue'
import { useNotebook } from '../stores/notebook'

const { pendingInspectRequest, confirmInspectSwap, cancelInspectSwap, cellMap } = useNotebook()

function labelFor(cellId: string | undefined): string {
  if (!cellId) return ''
  const cell = cellMap.value.get(cellId)
  if (cell?.annotations?.name) return cell.annotations.name
  return cellId.slice(0, 8)
}

const evictLabel = computed(() => labelFor(pendingInspectRequest.value?.evictCellId))
const newLabel = computed(() => labelFor(pendingInspectRequest.value?.newCellId))
</script>

<template>
  <div v-if="pendingInspectRequest" class="swap-overlay" role="dialog" aria-modal="true">
    <div class="swap-card">
      <div class="swap-title">Close inspect panel?</div>
      <div class="swap-body">
        Open inspect for <code>{{ newLabel }}</code
        >? The inspect panel for <code>{{ evictLabel }}</code> will be closed. Raise the panel limit
        in settings if you need more open at once.
      </div>
      <div class="swap-actions">
        <button class="swap-cancel" @click="cancelInspectSwap">Keep current</button>
        <button class="swap-confirm" @click="confirmInspectSwap">Close &amp; open new</button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.swap-overlay {
  position: fixed;
  inset: 0;
  background: rgba(10, 10, 20, 0.55);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}
.swap-card {
  background: var(--bg-elevated);
  border: 1px solid var(--accent-primary);
  border-radius: 8px;
  padding: 18px 20px;
  min-width: 340px;
  max-width: 480px;
  box-shadow: 0 12px 40px rgba(0, 0, 0, 0.45);
}
.swap-title {
  color: var(--text-primary);
  font-weight: 600;
  font-size: 14px;
  margin-bottom: 8px;
}
.swap-body {
  color: var(--text-secondary);
  font-size: 13px;
  line-height: 1.45;
  margin-bottom: 14px;
}
.swap-body code {
  color: var(--accent-primary);
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
}
.swap-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}
.swap-cancel,
.swap-confirm {
  padding: 6px 14px;
  font-size: 12px;
  font-weight: 600;
  border-radius: 4px;
  cursor: pointer;
  border: 1px solid transparent;
}
.swap-cancel {
  background: transparent;
  border-color: var(--border-strong);
  color: var(--text-primary);
}
.swap-cancel:hover {
  background: var(--bg-input);
}
.swap-confirm {
  background: var(--accent-primary);
  color: var(--bg-base);
}
.swap-confirm:hover {
  background: var(--accent-lavender);
}
</style>
