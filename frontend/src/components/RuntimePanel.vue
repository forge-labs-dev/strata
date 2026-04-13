<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useStrata } from '../composables/useStrata'
import { useNotebook } from '../stores/notebook'
import EnvVarsEditor from './EnvVarsEditor.vue'
import TimeoutConfigEditor from './TimeoutConfigEditor.vue'

const {
  notebook,
  connected,
  updateNotebookTimeoutAction,
  updateNotebookEnvAction,
  llmAvailable,
  llmModel,
  checkLlmStatus,
} = useNotebook()
const strata = useStrata()

const showPanel = ref(false)
const envCount = computed(() => Object.keys(notebook.env).length)
const timeoutLabel = computed(() => (notebook.timeout == null ? 'default' : `${notebook.timeout}s`))

// Model selector state
const availableModels = ref<string[]>([])
const modelsLoading = ref(false)
const selectedModel = ref('')

function sessionId(): string | null {
  return (notebook as any).sessionId ?? null
}

async function fetchModels() {
  const sid = sessionId()
  if (!sid) return
  modelsLoading.value = true
  try {
    const data = await strata.getLlmModels(sid)
    availableModels.value = data.models
    selectedModel.value = data.current || ''
  } catch {
    availableModels.value = []
  } finally {
    modelsLoading.value = false
  }
}

async function saveModel() {
  const sid = sessionId()
  if (!sid || !selectedModel.value) return
  try {
    await strata.updateLlmModel(sid, selectedModel.value)
    void checkLlmStatus()
  } catch {
    // silently ignore
  }
}

// Fetch models when panel opens and LLM is configured
watch(showPanel, (open) => {
  if (open && llmAvailable.value) {
    void fetchModels()
  }
})
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

      <!-- Model selector -->
      <div v-if="llmAvailable" class="model-section">
        <div class="model-header">
          <span class="model-title">LLM Model</span>
          <span v-if="llmModel" class="model-current">{{ llmModel }}</span>
        </div>
        <div class="model-controls">
          <select
            v-if="availableModels.length > 0"
            v-model="selectedModel"
            class="model-select"
            @change="saveModel"
          >
            <option v-for="m in availableModels" :key="m" :value="m">{{ m }}</option>
          </select>
          <input
            v-else
            v-model="selectedModel"
            class="model-input"
            placeholder="Model ID (e.g. gpt-5.4)"
            @keydown.enter="saveModel"
            @blur="saveModel"
          />
          <button
            class="model-refresh"
            :disabled="modelsLoading"
            title="Refresh available models"
            @click="fetchModels"
          >
            {{ modelsLoading ? '...' : '&#x21bb;' }}
          </button>
        </div>
      </div>
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

.model-section {
  border-top: 1px solid #2a2a3c;
  padding-top: 8px;
}

.model-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 6px;
}

.model-title {
  font-size: 12px;
  font-weight: 600;
  color: #a6adc8;
}

.model-current {
  font-size: 10px;
  color: #6c7086;
  background: #313244;
  padding: 1px 6px;
  border-radius: 4px;
}

.model-controls {
  display: flex;
  gap: 6px;
}

.model-select,
.model-input {
  flex: 1;
  padding: 5px 8px;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 12px;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
}

.model-select:focus,
.model-input:focus {
  outline: none;
  border-color: #89b4fa;
}

.model-refresh {
  padding: 4px 8px;
  background: none;
  border: 1px solid #45475a;
  border-radius: 6px;
  color: #6c7086;
  font-size: 14px;
  cursor: pointer;
}

.model-refresh:hover:not(:disabled) {
  border-color: #89b4fa;
  color: #89b4fa;
}

.model-refresh:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
</style>
