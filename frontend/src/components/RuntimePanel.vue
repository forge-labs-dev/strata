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
  refreshSecretsAction,
} = useNotebook()

const showPanel = ref(false)
const refreshing = ref(false)

const envCount = computed(() => Object.keys(notebook.env).length)
const timeoutLabel = computed(() => (notebook.timeout == null ? 'default' : `${notebook.timeout}s`))

// A secret manager is "active" if any env key has a non-manual source
// OR the backend reported a fetch error for a configured manager.
const secretManagerActive = computed(
  () =>
    Object.values(notebook.envSources).some((src) => src !== 'manual') ||
    notebook.envFetchError != null,
)

const secretManagerName = computed(() => {
  for (const src of Object.values(notebook.envSources)) {
    if (src && src !== 'manual') return src
  }
  return null
})

async function handleRefresh() {
  if (refreshing.value || !connected) return
  refreshing.value = true
  try {
    await refreshSecretsAction()
  } catch {
    // Fetch errors land on notebook.envFetchError via the response;
    // a thrown exception here means the POST itself failed (404 etc.).
  } finally {
    refreshing.value = false
  }
}
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
        :env-sources="notebook.envSources"
        title="Notebook Default Env"
        :read-only="!connected"
        @save="updateNotebookEnvAction"
      />
      <div v-if="secretManagerActive" class="secrets-panel">
        <div class="secrets-header">
          <span class="secrets-title">
            Secret manager
            <span v-if="secretManagerName" class="secrets-provider">{{ secretManagerName }}</span>
          </span>
          <button
            class="secrets-refresh"
            :disabled="refreshing || !connected"
            @click="handleRefresh"
          >
            {{ refreshing ? 'Refreshing…' : 'Refresh' }}
          </button>
        </div>
        <p v-if="notebook.envFetchError" class="secrets-error">
          {{ notebook.envFetchError }}
        </p>
        <p v-else-if="notebook.envFetchedAt" class="secrets-meta">
          Last refreshed {{ notebook.envFetchedAt }}
        </p>
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

.secrets-panel {
  border: 1px solid #313244;
  border-radius: 6px;
  padding: 8px 10px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.secrets-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}
.secrets-title {
  font-size: 12px;
  font-weight: 600;
  color: #cdd6f4;
  display: flex;
  align-items: center;
  gap: 6px;
}
.secrets-provider {
  background: #313244;
  color: #a6adc8;
  padding: 1px 6px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 500;
  text-transform: lowercase;
}
.secrets-refresh {
  background: none;
  border: 1px solid #313244;
  border-radius: 4px;
  color: #a6adc8;
  font-size: 11px;
  padding: 2px 8px;
  cursor: pointer;
}
.secrets-refresh:hover:not(:disabled) {
  color: #89b4fa;
  border-color: #89b4fa;
}
.secrets-refresh:disabled {
  opacity: 0.5;
  cursor: default;
}
.secrets-error {
  font-size: 11px;
  color: #f38ba8;
  margin: 0;
  line-height: 1.4;
}
.secrets-meta {
  font-size: 11px;
  color: #6c7086;
  margin: 0;
  line-height: 1.4;
}
</style>
