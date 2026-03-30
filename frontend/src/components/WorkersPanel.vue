<script setup lang="ts">
import { computed, ref } from 'vue'
import { useNotebook } from '../stores/notebook'
import WorkerConfigEditor from './WorkerConfigEditor.vue'
import WorkerListEditor from './WorkerListEditor.vue'

const {
  notebook,
  connected,
  availableWorkers,
  updateNotebookWorkerAction,
  updateNotebookWorkersAction,
} = useNotebook()
const showPanel = ref(false)

const workerLabel = computed(() => notebook.worker || 'local')
</script>

<template>
  <div class="workers-panel">
    <button class="workers-toggle" @click="showPanel = !showPanel">
      Worker
      <span class="worker-label">{{ workerLabel }}</span>
      <span class="toggle-icon">{{ showPanel ? '&#9650;' : '&#9660;' }}</span>
    </button>

    <div v-if="showPanel" class="workers-content">
      <p class="workers-copy">
        Notebook default. Cells can override this individually. Non-local workers
        are saved and surfaced now, but execution still errors until routing lands.
      </p>
      <WorkerConfigEditor
        :worker="notebook.worker"
        :options="availableWorkers"
        title="Notebook Default Worker"
        :read-only="!connected"
        @save="updateNotebookWorkerAction"
      />

      <WorkerListEditor
        :workers="notebook.workers"
        title="Notebook Worker Catalog"
        :read-only="!connected"
        @save="updateNotebookWorkersAction"
      />

      <div v-if="availableWorkers.length" class="workers-catalog">
        <div class="workers-catalog-title">Visible Workers</div>
        <div
          v-for="worker in availableWorkers"
          :key="worker.name"
          class="workers-catalog-row"
        >
          <code>{{ worker.name }}</code>
          <span class="workers-catalog-meta">{{ worker.backend }}</span>
          <span class="workers-catalog-meta">{{ worker.health }}</span>
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

.workers-copy {
  font-size: 12px;
  color: #6c7086;
  line-height: 1.4;
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
</style>
