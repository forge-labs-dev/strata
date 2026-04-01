<script setup lang="ts">
import { computed, ref } from 'vue'
import { useNotebook } from '../stores/notebook'
import type { WorkerHealthHistoryEntry } from '../types/notebook'
import { workerTransportLabel } from '../utils/notebookWorkers'
import WorkerConfigEditor from './WorkerConfigEditor.vue'
import WorkerListEditor from './WorkerListEditor.vue'

const {
  notebook,
  connected,
  availableWorkers,
  workerDefinitionsEditable,
  serverManagedWorkers,
  serverWorkerRegistryAvailable,
  serverWorkerRegistryLoading,
  workerHealthLoading,
  workerHealthCheckedAt,
  notebookWorkerError,
  workerRegistryError,
  serverWorkerRegistryError,
  fetchWorkers,
  updateNotebookWorkerAction,
  updateNotebookWorkersAction,
  updateServerWorkerRegistryAction,
  saveServerWorkerAction,
  deleteServerWorkerAction,
  updateServerWorkerEnabledAction,
  refreshServerWorkerAction,
  isServerWorkerActionLoading,
} = useNotebook()
const showPanel = ref(false)

const workerLabel = computed(() => notebook.worker || 'local')
const registryManagedByServer = computed(() => !workerDefinitionsEditable.value)
const canEditServerRegistry = computed(
  () => registryManagedByServer.value && serverWorkerRegistryAvailable.value,
)
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

function workerCheckedLabel(rawCheckedAt: number | null | undefined): string {
  if (!rawCheckedAt) return 'not checked'
  return new Date(rawCheckedAt).toLocaleTimeString([], {
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  })
}

function workerProbeLabel(count: number | null | undefined): string {
  if (!count) return 'no probes yet'
  return count === 1 ? '1 probe' : `${count} probes`
}

function workerDurationLabel(rawDuration: number | null | undefined): string {
  if (typeof rawDuration !== 'number' || !Number.isFinite(rawDuration)) {
    return 'latency unknown'
  }
  return `${rawDuration} ms`
}

function workerHistoryLabel(entry: WorkerHealthHistoryEntry): string {
  const duration = workerDurationLabel(entry.durationMs)
  return `${workerCheckedLabel(entry.checkedAt)} ${entry.health} · ${duration}`
}

function workerHistoryTitle(entry: WorkerHealthHistoryEntry): string {
  return entry.error ? `${workerHistoryLabel(entry)}\n${entry.error}` : workerHistoryLabel(entry)
}
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
      <WorkerListEditor
        v-else-if="canEditServerRegistry"
        :workers="serverManagedWorkers"
        title="Server Worker Catalog"
        :show-enabled="true"
        :row-actions="true"
        :read-only="!connected || serverWorkerRegistryLoading"
        :error="serverWorkerRegistryError"
        @save="updateServerWorkerRegistryAction"
        @save-one="saveServerWorkerAction"
        @remove-one="deleteServerWorkerAction"
      />
      <div v-else class="workers-copy workers-copy-muted">
        This notebook can select from the visible server-managed workers below, but it cannot change
        the worker registry.
      </div>

      <div v-if="availableWorkers.length" class="workers-catalog">
        <div class="workers-catalog-title">Visible Workers</div>
        <div v-for="worker in availableWorkers" :key="worker.name" class="workers-catalog-card">
          <div class="workers-catalog-row">
            <code>{{ worker.name }}</code>
            <span class="workers-catalog-meta">{{ worker.source || 'unknown' }}</span>
            <span class="workers-catalog-meta">{{ worker.backend }}</span>
            <span class="workers-catalog-meta">{{
              worker.transport || workerTransportLabel(worker)
            }}</span>
            <span
              class="workers-catalog-meta"
              :class="{
                unhealthy: worker.health === 'unavailable',
                unknown: worker.health === 'unknown',
              }"
            >
              {{ worker.health }}
            </span>
            <span
              class="workers-catalog-meta"
              :class="{
                disallowed: worker.allowed === false,
                disabled: worker.enabled === false,
              }"
            >
              {{
                worker.enabled === false
                  ? 'disabled'
                  : worker.allowed === false
                    ? 'not allowed'
                    : 'allowed'
              }}
            </span>
            <div
              v-if="canEditServerRegistry && worker.source === 'server'"
              class="workers-catalog-actions"
            >
              <button
                class="workers-action"
                :disabled="!connected || isServerWorkerActionLoading(worker.name)"
                @click="refreshServerWorkerAction(worker.name)"
              >
                {{ isServerWorkerActionLoading(worker.name) ? 'Refreshing…' : 'Refresh' }}
              </button>
              <button
                class="workers-action"
                :disabled="!connected || isServerWorkerActionLoading(worker.name)"
                @click="updateServerWorkerEnabledAction(worker.name, worker.enabled === false)"
              >
                {{
                  isServerWorkerActionLoading(worker.name)
                    ? 'Saving…'
                    : worker.enabled === false
                      ? 'Enable'
                      : 'Disable'
                }}
              </button>
            </div>
          </div>
          <div class="workers-catalog-detail-row">
            <span class="workers-catalog-detail">
              last checked {{ workerCheckedLabel(worker.healthCheckedAt) }}
            </span>
            <span class="workers-catalog-detail">
              {{ workerProbeLabel(worker.probeCount) }}
            </span>
            <span v-if="worker.consecutiveFailures" class="workers-catalog-detail unhealthy">
              {{ worker.consecutiveFailures }} consecutive failures
            </span>
            <span v-if="worker.lastProbeDurationMs != null" class="workers-catalog-detail">
              last probe {{ workerDurationLabel(worker.lastProbeDurationMs) }}
            </span>
            <span v-if="worker.healthUrl" class="workers-catalog-detail">
              health {{ worker.healthUrl }}
            </span>
          </div>
          <div class="workers-catalog-detail-row">
            <span v-if="worker.lastHealthyAt" class="workers-catalog-detail">
              last healthy {{ workerCheckedLabel(worker.lastHealthyAt) }}
            </span>
            <span v-if="worker.lastUnavailableAt" class="workers-catalog-detail">
              last unavailable {{ workerCheckedLabel(worker.lastUnavailableAt) }}
            </span>
            <span v-if="worker.lastStatusChangeAt" class="workers-catalog-detail">
              status changed {{ workerCheckedLabel(worker.lastStatusChangeAt) }}
            </span>
          </div>
          <div
            v-if="worker.healthHistory && worker.healthHistory.length"
            class="workers-catalog-history"
          >
            <span class="workers-catalog-history-label">recent probes</span>
            <span
              v-for="entry in worker.healthHistory.slice(0, 4)"
              :key="`${worker.name}-${entry.checkedAt}-${entry.health}`"
              class="workers-catalog-history-chip"
              :class="{
                unhealthy: entry.health === 'unavailable',
                unknown: entry.health === 'unknown',
              }"
              :title="workerHistoryTitle(entry)"
            >
              {{ workerHistoryLabel(entry) }}
            </span>
          </div>
          <div v-if="worker.lastError" class="workers-catalog-error">
            {{ worker.lastError }}
          </div>
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

.workers-catalog-card {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 8px 10px;
  border: 1px solid #313244;
  border-radius: 10px;
  background: #11111b;
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
  flex-wrap: wrap;
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

.workers-catalog-meta.disabled,
.workers-catalog-meta.unknown {
  color: #fab387;
}

.workers-catalog-meta.unhealthy {
  color: #f38ba8;
}

.workers-catalog-actions {
  margin-left: auto;
  display: flex;
  gap: 6px;
}

.workers-action {
  border: 1px solid #313244;
  background: #181825;
  color: #cdd6f4;
  border-radius: 8px;
  padding: 4px 8px;
  font-size: 11px;
  cursor: pointer;
}

.workers-action:disabled {
  opacity: 0.55;
  cursor: default;
}

.workers-catalog-detail-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.workers-catalog-detail {
  font-size: 11px;
  color: #6c7086;
}

.workers-catalog-error {
  font-size: 12px;
  color: #f38ba8;
  line-height: 1.4;
}

.workers-catalog-history {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
}

.workers-catalog-history-label {
  font-size: 11px;
  color: #6c7086;
}

.workers-catalog-history-chip {
  padding: 1px 6px;
  border-radius: 999px;
  background: #181825;
  border: 1px solid #313244;
  color: #a6adc8;
  font-size: 10px;
}

.workers-catalog-history-chip.unhealthy {
  color: #f38ba8;
}

.workers-catalog-history-chip.unknown {
  color: #fab387;
}
</style>
