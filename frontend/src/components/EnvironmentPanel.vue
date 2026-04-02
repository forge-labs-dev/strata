<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useNotebook } from '../stores/notebook'

const {
  notebook,
  dependencies,
  dependencyLoading,
  dependencyError,
  environmentLoading,
  environmentError,
  environmentLastAction,
  addDependencyAction,
  removeDependencyAction,
  syncEnvironmentAction,
  fetchEnvironment,
  connected,
} = useNotebook()

const newPackage = ref('')
const showPanel = ref(false)

const syncStateLabel = computed(() => {
  switch (notebook.environment.syncState) {
    case 'ready':
      return 'Ready'
    case 'fallback':
      return 'Fallback'
    case 'failed':
      return 'Failed'
    default:
      return 'Unknown'
  }
})

const syncStateClass = computed(() => `state-${notebook.environment.syncState}`)

const shortLockfileHash = computed(() =>
  notebook.environment.lockfileHash ? notebook.environment.lockfileHash.slice(0, 12) : 'none',
)

const lastSyncedLabel = computed(() => {
  if (!notebook.environment.lastSyncedAt) return 'Not synced yet'
  return new Date(notebook.environment.lastSyncedAt).toLocaleString()
})

const lastActionLabel = computed(() => {
  const action = environmentLastAction.value
  if (!action) return ''

  const verb =
    action.action === 'add'
      ? `Added ${action.packageName}`
      : action.action === 'remove'
        ? `Removed ${action.packageName}`
        : 'Synced environment'
  const stale =
    action.staleCellCount > 0
      ? ` · ${action.staleCellCount} cell${action.staleCellCount === 1 ? '' : 's'} affected`
      : ''
  const lockfile = action.lockfileChanged ? ' · lockfile changed' : ' · lockfile unchanged'
  return `${verb}${lockfile}${stale}`
})

watch(
  showPanel,
  (open) => {
    if (open) {
      void fetchEnvironment()
    }
  },
  { flush: 'post' },
)

async function addPackage() {
  const pkg = newPackage.value.trim()
  if (!pkg) return
  await addDependencyAction(pkg)
  newPackage.value = ''
}

async function removePackage(name: string) {
  await removeDependencyAction(name)
}
</script>

<template>
  <div class="env-panel">
    <button class="env-toggle" @click="showPanel = !showPanel">
      {{ showPanel ? 'Environment' : 'Environment' }}
      <span class="dep-count">{{ notebook.environment.declaredPackageCount }}</span>
      <span class="toggle-icon">{{ showPanel ? '&#9650;' : '&#9660;' }}</span>
    </button>

    <div v-if="showPanel" class="env-content">
      <div class="env-header">
        <div class="env-status">
          <span class="status-dot" :class="syncStateClass"></span>
          <span>{{ syncStateLabel }}</span>
        </div>
        <button
          class="btn-sync"
          :disabled="!connected || dependencyLoading || environmentLoading"
          @click="syncEnvironmentAction"
        >
          {{ environmentLoading ? 'Syncing…' : 'Sync Environment' }}
        </button>
      </div>

      <div class="env-stats">
        <div class="env-stat">
          <span class="env-stat-label">Python</span>
          <span class="env-stat-value">{{ notebook.environment.pythonVersion || 'Unknown' }}</span>
        </div>
        <div class="env-stat">
          <span class="env-stat-label">Declared</span>
          <span class="env-stat-value">{{ notebook.environment.declaredPackageCount }}</span>
        </div>
        <div class="env-stat">
          <span class="env-stat-label">Resolved</span>
          <span class="env-stat-value">{{ notebook.environment.resolvedPackageCount }}</span>
        </div>
        <div class="env-stat">
          <span class="env-stat-label">Lockfile</span>
          <span class="env-stat-value" :title="notebook.environment.lockfileHash">
            {{ shortLockfileHash }}
          </span>
        </div>
      </div>

      <div class="env-meta">
        <div>Last sync: {{ lastSyncedLabel }}</div>
        <div v-if="notebook.environment.venvPython">
          Interpreter: <code>{{ notebook.environment.venvPython }}</code>
        </div>
        <div v-else-if="!notebook.environment.hasLockfile">Lockfile not created yet</div>
      </div>

      <div v-if="lastActionLabel" class="env-action">
        {{ lastActionLabel }}
      </div>

      <div v-if="environmentError || notebook.environment.syncError" class="env-error">
        {{ environmentError || notebook.environment.syncError }}
      </div>

      <div class="add-dep">
        <input
          v-model="newPackage"
          type="text"
          placeholder="Add package (e.g. pandas)"
          class="dep-input"
          :disabled="!connected || dependencyLoading || environmentLoading"
          @keydown.enter="addPackage"
        />
        <button
          class="btn-add"
          :disabled="!connected || dependencyLoading || environmentLoading || !newPackage.trim()"
          @click="addPackage"
        >
          {{ dependencyLoading ? '…' : '+' }}
        </button>
      </div>

      <div v-if="dependencyError" class="dep-error">
        {{ dependencyError }}
      </div>

      <div v-if="dependencies.length === 0" class="dep-empty">No declared packages</div>
      <ul v-else class="dep-list">
        <li v-for="dep in dependencies" :key="dep.name" class="dep-item">
          <div class="dep-details">
            <span class="dep-name">{{ dep.name }}</span>
            <span class="dep-version">{{ dep.version || dep.specifier || 'unpinned' }}</span>
          </div>
          <button
            class="btn-remove"
            title="Remove"
            :disabled="dependencyLoading || environmentLoading"
            @click="removePackage(dep.name)"
          >
            Remove
          </button>
        </li>
      </ul>
    </div>
  </div>
</template>

<style scoped>
.env-panel {
  margin-top: 12px;
  border-top: 1px solid #2a2a3c;
  padding-top: 8px;
}

.env-toggle {
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

.env-toggle:hover {
  color: #cdd6f4;
}

.dep-count {
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

.env-content {
  margin-top: 8px;
}

.env-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 8px;
}

.env-status {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  color: #cdd6f4;
  font-size: 12px;
  font-weight: 600;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 999px;
  background: #6c7086;
}

.state-ready {
  background: #a6e3a1;
}

.state-fallback {
  background: #f9e2af;
}

.state-failed {
  background: #f38ba8;
}

.state-unknown {
  background: #6c7086;
}

.btn-sync {
  border: 1px solid #45475a;
  background: #1e2030;
  color: #cdd6f4;
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 11px;
  cursor: pointer;
}

.btn-sync:hover {
  border-color: #89b4fa;
  color: #89b4fa;
}

.btn-sync:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.env-stats {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 6px;
  margin-bottom: 8px;
}

.env-stat {
  background: #1a1c2a;
  border: 1px solid #313244;
  border-radius: 8px;
  padding: 8px;
}

.env-stat-label {
  display: block;
  color: #6c7086;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  margin-bottom: 3px;
}

.env-stat-value {
  color: #cdd6f4;
  font-size: 12px;
  font-weight: 600;
  word-break: break-all;
}

.env-meta {
  color: #a6adc8;
  font-size: 11px;
  margin-bottom: 8px;
}

.env-meta code {
  color: #89b4fa;
  word-break: break-all;
}

.env-action {
  color: #a6e3a1;
  font-size: 11px;
  margin-bottom: 8px;
  padding: 6px 8px;
  background: rgb(166 227 161 / 10%);
  border: 1px solid rgb(166 227 161 / 18%);
  border-radius: 6px;
}

.env-error,
.dep-error {
  color: #f38ba8;
  font-size: 11px;
  margin-bottom: 6px;
  padding: 6px 8px;
  background: rgb(243 139 168 / 10%);
  border: 1px solid rgb(243 139 168 / 18%);
  border-radius: 6px;
}

.add-dep {
  display: flex;
  gap: 4px;
  margin-bottom: 8px;
}

.dep-input {
  flex: 1;
  padding: 6px 8px;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 12px;
  min-width: 0;
}

.dep-input:focus {
  outline: none;
  border-color: #89b4fa;
}

.dep-input::placeholder {
  color: #585b70;
}

.btn-add {
  background: #89b4fa;
  color: #1e1e2e;
  border: none;
  width: 32px;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 700;
  cursor: pointer;
  flex-shrink: 0;
}

.btn-add:hover {
  background: #74c7ec;
}

.btn-add:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.dep-empty {
  color: #585b70;
  font-size: 12px;
  text-align: center;
  padding: 8px 0;
}

.dep-list {
  list-style: none;
  max-height: 220px;
  overflow-y: auto;
}

.dep-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 6px 0;
  font-size: 12px;
  border-top: 1px solid rgb(49 50 68 / 70%);
}

.dep-item:first-child {
  border-top: none;
}

.dep-details {
  min-width: 0;
}

.dep-name {
  color: #cdd6f4;
  display: block;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.dep-version {
  color: #6c7086;
  font-size: 11px;
}

.btn-remove {
  background: none;
  border: 1px solid #45475a;
  color: #cdd6f4;
  cursor: pointer;
  font-size: 11px;
  padding: 4px 8px;
  border-radius: 6px;
  flex-shrink: 0;
}

.btn-remove:hover {
  color: #f38ba8;
  border-color: #f38ba8;
}

.btn-remove:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
</style>
