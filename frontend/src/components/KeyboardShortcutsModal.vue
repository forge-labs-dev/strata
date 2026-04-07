<script setup lang="ts">
defineProps<{ visible: boolean }>()
const emit = defineEmits<{ close: [] }>()

const shortcuts = [
  { keys: 'Shift + Enter', description: 'Run cell' },
  { keys: '?', description: 'Show keyboard shortcuts' },
  { keys: 'Esc', description: 'Close modal' },
  { keys: 'Ctrl + Z', description: 'Undo (in editor)' },
  { keys: 'Ctrl + Shift + Z', description: 'Redo (in editor)' },
]
</script>

<template>
  <div v-if="visible" class="shortcuts-overlay" @click="emit('close')">
    <div class="shortcuts-dialog" @click.stop>
      <div class="shortcuts-header">
        <h2>Keyboard Shortcuts</h2>
        <button class="shortcuts-close" @click="emit('close')">&times;</button>
      </div>
      <table class="shortcuts-table">
        <tbody>
          <tr v-for="s in shortcuts" :key="s.keys">
            <td class="shortcut-keys">
              <kbd v-for="(part, i) in s.keys.split(' + ')" :key="i">
                <span v-if="i > 0" class="key-separator">+</span>
                {{ part }}
              </kbd>
            </td>
            <td class="shortcut-desc">{{ s.description }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.shortcuts-overlay {
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

.shortcuts-dialog {
  background: #181825;
  border: 1px solid #313244;
  border-radius: 12px;
  padding: 24px;
  min-width: 360px;
  max-width: 480px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
}

.shortcuts-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
}

.shortcuts-header h2 {
  font-size: 16px;
  color: #cdd6f4;
}

.shortcuts-close {
  background: none;
  border: none;
  color: #6c7086;
  font-size: 20px;
  cursor: pointer;
  padding: 0 4px;
}

.shortcuts-close:hover {
  color: #cdd6f4;
}

.shortcuts-table {
  width: 100%;
  border-collapse: collapse;
}

.shortcuts-table tr {
  border-bottom: 1px solid #2a2a3c;
}

.shortcuts-table tr:last-child {
  border-bottom: none;
}

.shortcuts-table td {
  padding: 8px 4px;
}

.shortcut-keys {
  white-space: nowrap;
  text-align: right;
  padding-right: 16px;
  width: 1%;
}

kbd {
  display: inline-block;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 4px;
  padding: 2px 8px;
  font-family: inherit;
  font-size: 12px;
  font-weight: 600;
  color: #cdd6f4;
}

.key-separator {
  color: #6c7086;
  margin: 0 4px;
  font-size: 11px;
}

.shortcut-desc {
  font-size: 13px;
  color: #a6adc8;
}
</style>
