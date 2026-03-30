<script setup lang="ts">
import { computed, ref } from 'vue'
import { useNotebook } from '../stores/notebook'
import EnvVarsEditor from './EnvVarsEditor.vue'
import TimeoutConfigEditor from './TimeoutConfigEditor.vue'

const {
  notebook,
  connected,
  updateNotebookTimeoutAction,
  updateNotebookEnvAction,
} = useNotebook()

const showPanel = ref(false)
const envCount = computed(() => Object.keys(notebook.env).length)
const timeoutLabel = computed(() =>
  notebook.timeout == null ? 'default' : `${notebook.timeout}s`,
)
</script>

<template>
  <div class="runtime-panel">
    <button class="runtime-toggle" @click="showPanel = !showPanel">
      Runtime
      <span class="runtime-label">{{ timeoutLabel }}</span>
      <span class="runtime-label">{{ envCount }} env</span>
      <span class="toggle-icon">{{ showPanel ? '&#9650;' : '&#9660;' }}</span>
    </button>

    <div v-if="showPanel" class="runtime-content">
      <p class="runtime-copy">
        Notebook defaults. Cells can override these, and source annotations still win.
      </p>
      <TimeoutConfigEditor
        :timeout="notebook.timeout"
        title="Notebook Default Timeout"
        :read-only="!connected"
        @save="updateNotebookTimeoutAction"
      />
      <EnvVarsEditor
        :env="notebook.env"
        title="Notebook Default Env"
        :read-only="!connected"
        @save="updateNotebookEnvAction"
      />
    </div>
  </div>
</template>

<style scoped>
.runtime-panel {
  margin-top: 12px;
  border-top: 1px solid #2a2a3c;
  padding-top: 8px;
}

.runtime-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  width: 100%;
  background: none;
  border: none;
  color: #a6adc8;
  font-size: 12px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  cursor: pointer;
  padding: 4px 0;
}

.runtime-toggle:hover {
  color: #cdd6f4;
}

.runtime-label {
  background: #313244;
  color: #f9e2af;
  padding: 1px 6px;
  border-radius: 8px;
  font-size: 11px;
  font-weight: 600;
}

.toggle-icon {
  margin-left: auto;
  font-size: 10px;
}

.runtime-content {
  margin-top: 8px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.runtime-copy {
  font-size: 12px;
  color: #6c7086;
  line-height: 1.4;
}
</style>
