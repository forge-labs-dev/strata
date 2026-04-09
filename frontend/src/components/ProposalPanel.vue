<script setup lang="ts">
import { computed } from 'vue'
import { useNotebook } from '../stores/notebook'

const { proposedPlan, applyingPlan, applyError, discardPlan, applyProposedChanges } = useNotebook()

const acceptedCount = computed(() => {
  if (!proposedPlan.value) return 0
  return proposedPlan.value.changes.filter((c) => c.accepted).length
})

const totalCount = computed(() => proposedPlan.value?.changes.length ?? 0)

function toggleChange(index: number) {
  if (!proposedPlan.value) return
  proposedPlan.value.changes[index].accepted = !proposedPlan.value.changes[index].accepted
}

function acceptAll() {
  if (!proposedPlan.value) return
  proposedPlan.value.changes.forEach((c) => (c.accepted = true))
}

function declineAll() {
  if (!proposedPlan.value) return
  proposedPlan.value.changes.forEach((c) => (c.accepted = false))
}

function changeIcon(type: string): string {
  switch (type) {
    case 'add_cell':
      return '+'
    case 'modify_cell':
      return '~'
    case 'delete_cell':
      return '-'
    case 'add_package':
      return '📦'
    case 'remove_package':
      return '🗑'
    case 'set_env':
      return '⚙'
    case 'reorder_cells':
      return '↕'
    default:
      return '?'
  }
}

function changeLabel(change: any): string {
  switch (change.type) {
    case 'add_cell':
      return change.name || `New ${change.language || 'python'} cell`
    case 'modify_cell':
      return `Edit cell ${(change.cell_id || '').slice(0, 6)}`
    case 'delete_cell':
      return `Delete cell ${(change.cell_id || '').slice(0, 6)}`
    case 'add_package':
      return `Install ${change.package}`
    case 'remove_package':
      return `Remove ${change.package}`
    case 'set_env':
      return `Set ${change.key}`
    case 'reorder_cells':
      return 'Reorder cells'
    default:
      return change.type
  }
}

function changeBadgeClass(type: string): string {
  if (type === 'add_cell' || type === 'add_package') return 'badge-add'
  if (type === 'modify_cell' || type === 'set_env') return 'badge-modify'
  if (type === 'delete_cell' || type === 'remove_package') return 'badge-delete'
  return ''
}
</script>

<template>
  <div v-if="proposedPlan" class="proposal-overlay" @click.self="discardPlan">
    <div class="proposal-panel">
      <div class="proposal-header">
        <h3>Proposed Changes</h3>
        <button class="close-btn" @click="discardPlan">&times;</button>
      </div>

      <p v-if="proposedPlan.summary" class="proposal-summary">
        {{ proposedPlan.summary }}
      </p>

      <div class="proposal-actions-top">
        <button class="btn-small" @click="acceptAll">Select All</button>
        <button class="btn-small" @click="declineAll">Deselect All</button>
        <span class="count-label">{{ acceptedCount }} / {{ totalCount }} selected</span>
      </div>

      <div class="changes-list">
        <div
          v-for="(change, idx) in proposedPlan.changes"
          :key="idx"
          class="change-item"
          :class="{ declined: !change.accepted }"
          @click="toggleChange(idx)"
        >
          <input type="checkbox" :checked="change.accepted" class="change-checkbox" />
          <span class="change-icon" :class="changeBadgeClass(change.type)">
            {{ changeIcon(change.type) }}
          </span>
          <div class="change-info">
            <span class="change-label">{{ changeLabel(change) }}</span>
            <span v-if="change.reason" class="change-reason">{{ change.reason }}</span>
          </div>
        </div>

        <!-- Source preview for cell changes -->
        <template v-for="(change, idx) in proposedPlan.changes" :key="'preview-' + idx">
          <div
            v-if="
              change.source &&
              change.accepted &&
              (change.type === 'add_cell' || change.type === 'modify_cell')
            "
            class="source-preview"
          >
            <pre>{{ change.source }}</pre>
          </div>
        </template>
      </div>

      <div v-if="applyError" class="apply-error">{{ applyError }}</div>

      <div class="proposal-actions">
        <button
          class="btn-apply"
          :disabled="acceptedCount === 0 || applyingPlan"
          @click="applyProposedChanges"
        >
          {{
            applyingPlan
              ? 'Applying...'
              : `Apply ${acceptedCount} Change${acceptedCount !== 1 ? 's' : ''}`
          }}
        </button>
        <button class="btn-discard" :disabled="applyingPlan" @click="discardPlan">Discard</button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.proposal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.proposal-panel {
  background: #181825;
  border: 1px solid #313244;
  border-radius: 12px;
  padding: 24px;
  width: 90%;
  max-width: 600px;
  max-height: 80vh;
  overflow-y: auto;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
}

.proposal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}

.proposal-header h3 {
  font-size: 16px;
  color: #cdd6f4;
}

.close-btn {
  background: none;
  border: none;
  color: #6c7086;
  font-size: 20px;
  cursor: pointer;
}

.close-btn:hover {
  color: #cdd6f4;
}

.proposal-summary {
  font-size: 13px;
  color: #a6adc8;
  margin-bottom: 16px;
  line-height: 1.4;
}

.proposal-actions-top {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
}

.btn-small {
  padding: 3px 8px;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 4px;
  color: #cdd6f4;
  font-size: 11px;
  cursor: pointer;
}

.btn-small:hover {
  background: #45475a;
}

.count-label {
  font-size: 11px;
  color: #6c7086;
  margin-left: auto;
}

.changes-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin-bottom: 16px;
}

.change-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border-radius: 6px;
  cursor: pointer;
  background: #1e1e2e;
  border: 1px solid transparent;
}

.change-item:hover {
  border-color: #313244;
}

.change-item.declined {
  opacity: 0.4;
}

.change-checkbox {
  flex-shrink: 0;
  cursor: pointer;
}

.change-icon {
  flex-shrink: 0;
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  font-size: 14px;
  font-weight: 700;
  background: #313244;
  color: #cdd6f4;
}

.badge-add {
  background: #a6e3a130;
  color: #a6e3a1;
}

.badge-modify {
  background: #f9e2af30;
  color: #f9e2af;
}

.badge-delete {
  background: #f38ba830;
  color: #f38ba8;
}

.change-info {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.change-label {
  font-size: 13px;
  color: #cdd6f4;
}

.change-reason {
  font-size: 11px;
  color: #6c7086;
}

.source-preview {
  margin-left: 40px;
  margin-bottom: 4px;
}

.source-preview pre {
  font-size: 11px;
  line-height: 1.4;
  color: #a6adc8;
  background: #11111b;
  padding: 8px;
  border-radius: 4px;
  overflow-x: auto;
  margin: 0;
  max-height: 120px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-word;
}

.apply-error {
  padding: 8px;
  color: #f38ba8;
  font-size: 12px;
  background: #45252530;
  border-radius: 6px;
  margin-bottom: 12px;
}

.proposal-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
}

.btn-apply {
  padding: 8px 16px;
  background: #a6e3a1;
  color: #1e1e2e;
  border: none;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
}

.btn-apply:hover:not(:disabled) {
  background: #94e2d5;
}

.btn-apply:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.btn-discard {
  padding: 8px 16px;
  background: #313244;
  color: #cdd6f4;
  border: none;
  border-radius: 6px;
  font-size: 13px;
  cursor: pointer;
}

.btn-discard:hover:not(:disabled) {
  background: #45475a;
}

.btn-discard:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
</style>
