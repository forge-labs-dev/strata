<script setup lang="ts">
import { computed, ref } from 'vue'
import { useNotebook } from '../stores/notebook'
import { workerTransportLabel } from '../utils/notebookWorkers'
import WorkerConfigEditor from './WorkerConfigEditor.vue'
import WorkerListEditor from './WorkerListEditor.vue'

const {
  notebook,
  connected,
  availableWorkers,
  workerDefinitionsEditable,
  workerHealthLoading,
  workerHealthCheckedAt,
  notebookWorkerError,
  workerRegistryError,
  fetchWorkers,
  updateNotebookWorkerAction,
  updateNotebookWorkersAction,
} = useNotebook()
const showPanel = ref(false)

const workerLabel = computed(() => notebook.worker || 'local')
const registryManagedByServer = computed(() => !workerDefinitionsEditable.value)
const lastCheckedLabel = computed(() => {
  if (!workerHealthCheckedAt.value) {
    return 'Not checked yet'
  }

  return new Date(workerHealthCheckedAt.value).toLocaleTimeString([], {
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  })
})
</script>

<template>
  <div class="workers-panel">
    <button class="workers-toggle" @click="showPanel = !showPanel">
      Worker
      <span class="worker-label">{{ workerLabel }}</span>
      <span class="toggle-icon">{{ showPanel ? '&#9650;' : '&#9660;' }}</span>
    </button>

    <div v-if="showPanel" class="workers-content">
      <div class="workers-health-row">
        <span class="workers-health-text"> Last checked: {{ lastCheckedLabel }} </span>
        <button
          class="workers-refresh"
          :disabled="!connected || workerHealthLoading"
          @click="fetchWorkers(true)"
        >
          {{ workerHealthLoading ? 'Refreshing…' : 'Refresh health' }}
        </button>
      </div>

      <p class="workers-copy">
        Notebook default. Cells can override this individually.
        <span v-if="registryManagedByServer">
          Worker definitions are managed by the server in service mode.
        </span>
        <span v-else> Worker definitions are stored with the notebook in personal mode. </span>
      </p>
      <WorkerConfigEditor
        :worker="notebook.worker"
        :options="availableWorkers"
        title="Notebook Default Worker"
        :read-only="!connected"
        :error="notebookWorkerError"
        @save="updateNotebookWorkerAction"
      />

      <WorkerListEditor
        v-if="workerDefinitionsEditable"
        :workers="notebook.workers"
        title="Notebook Worker Catalog"
        :read-only="!connected || !workerDefinitionsEditable"
        :error="workerRegistryError"
        @save="updateNotebookWorkersAction"
      />
      <div v-else class="workers-copy workers-copy-muted">
        This notebook can select from the visible server-managed workers below, but it cannot change
        the worker registry.
      </div>

      <div v-if="availableWorkers.length" class="workers-catalog">
        <div class="workers-catalog-title">Visible Workers</div>
        <div v-for="worker in availableWorkers" :key="worker.name" class="workers-catalog-row">
          <code>{{ worker.name }}</code>
          <span class="workers-catalog-meta">{{ worker.source || 'unknown' }}</span>
          <span class="workers-catalog-meta">{{ worker.backend }}</span>
          <span class="workers-catalog-meta">{{ workerTransportLabel(worker) }}</span>
          <span class="workers-catalog-meta">{{ worker.health }}</span>
          <span class="workers-catalog-meta" :class="{ disallowed: worker.allowed === false }">
            {{ worker.allowed === false ? 'not allowed' : 'allowed' }}
          </span>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.workers-panel {
  margin-top: 12px;
  border-top: 1px solid #2a2a3c;
  padding-top: 8px;
}

.workers-toggle {
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

.workers-toggle:hover {
  color: #cdd6f4;
}

.worker-label {
  background: #313244;
  color: #89b4fa;
  padding: 1px 6px;
  border-radius: 8px;
  font-size: 11px;
  font-weight: 600;
}

.toggle-icon {
  margin-left: auto;
  font-size: 10px;
}

.workers-content {
  margin-top: 8px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.workers-health-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.workers-health-text {
  font-size: 12px;
  color: #6c7086;
}

.workers-refresh {
  border: 1px solid #313244;
  background: #181825;
  color: #cdd6f4;
  border-radius: 8px;
  padding: 5px 10px;
  font-size: 12px;
  cursor: pointer;
}

.workers-refresh:disabled {
  opacity: 0.55;
  cursor: default;
}

.workers-copy {
  font-size: 12px;
  color: #6c7086;
  line-height: 1.4;
}

.workers-copy-muted {
  margin-top: -2px;
}

.workers-catalog {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.workers-catalog-title {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #a6adc8;
}

.workers-catalog-row {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: #bac2de;
}

.workers-catalog-meta {
  padding: 1px 6px;
  border-radius: 999px;
  background: #313244;
  color: #89b4fa;
  font-size: 11px;
}

.workers-catalog-meta.disallowed {
  color: #f38ba8;
}
</style>
