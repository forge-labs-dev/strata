<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useStrata, type DiscoveredNotebook } from '../composables/useStrata'
import { preloadNotebookRoute } from '../router'
import { useRecentNotebooks } from '../stores/recentNotebooks'
import { primePrefetchedNotebookSession } from '../utils/notebookSessionPrefetch'
import { clearNotebookPerfMarks, markNotebookPerf, measureNotebookPerf } from '../utils/perf'

const router = useRouter()
const strata = useStrata()
const { entries: recentNotebooks, record, remove } = useRecentNotebooks()

const FALLBACK_NOTEBOOK_PARENT_PATH = '/tmp/strata-notebooks'
const newName = ref('Untitled Notebook')
const newParentPath = ref(FALLBACK_NOTEBOOK_PARENT_PATH)
const availablePythonVersions = ref<string[]>([])
const selectedPythonVersion = ref('')
const pythonSelectionFixed = ref(true)
const showNewForm = ref(false)
const showOpenForm = ref(false)
const discoveredNotebooks = ref<DiscoveredNotebook[]>([])
const discoveryRoot = ref<string | null>(null)
const discoveryLoading = ref(false)
const discoveryError = ref<string | null>(null)
const loading = ref(false)
const error = ref<string | null>(null)
const failedRecentPath = ref<string | null>(null)
const failedRecentName = ref<string | null>(null)

onMounted(async () => {
  try {
    const data = await strata.getNotebookRuntimeConfig()
    const defaultParentPath =
      typeof data?.default_parent_path === 'string' && data.default_parent_path.trim()
        ? data.default_parent_path
        : FALLBACK_NOTEBOOK_PARENT_PATH
    const configuredPythonVersions = Array.isArray(data?.available_python_versions)
      ? data.available_python_versions
          .map((value: unknown) => String(value || '').trim())
          .filter((value: string) => value.length > 0)
      : []
    availablePythonVersions.value = configuredPythonVersions
    selectedPythonVersion.value =
      typeof data?.default_python_version === 'string' && data.default_python_version.trim()
        ? data.default_python_version
        : configuredPythonVersions[0] || ''
    pythonSelectionFixed.value =
      data?.python_selection_fixed === true || configuredPythonVersions.length <= 1
    if (newParentPath.value === FALLBACK_NOTEBOOK_PARENT_PATH) {
      newParentPath.value = defaultParentPath
    }
  } catch (e) {
    console.warn('Failed to load notebook config, using fallback parent path', e)
  }
})

async function createNotebook() {
  if (!newName.value.trim()) return
  loading.value = true
  dismissError()
  void preloadNotebookRoute()
  clearNotebookPerfMarks('create_click', 'create_response', 'create_request_ms', 'create_total_ms')
  markNotebookPerf('create_click')
  try {
    const notebookPath = `${newParentPath.value.replace(/\/+$/, '')}/${newName.value}`
    const data = await strata.createNotebook(
      newParentPath.value,
      newName.value,
      selectedPythonVersion.value || null,
    )
    markNotebookPerf('create_response')
    measureNotebookPerf('create_request_ms', 'create_click', 'create_response')
    const resolvedPath = data.path || notebookPath
    primePrefetchedNotebookSession(data)
    record(data.name, resolvedPath, data.session_id)
    markNotebookPerf('create_route_start')
    await router.push({
      name: 'notebook',
      params: { sessionId: data.session_id },
      query: { path: resolvedPath },
    })
  } catch (e: any) {
    error.value = e.message || 'Failed to create notebook'
  } finally {
    loading.value = false
  }
}

async function loadDiscoveredNotebooks() {
  discoveryLoading.value = true
  discoveryError.value = null
  try {
    const data = await strata.discoverNotebooks()
    discoveredNotebooks.value = data.notebooks
    discoveryRoot.value = data.root
  } catch (e: any) {
    discoveryError.value = e.message || 'Failed to scan notebook directory'
    discoveredNotebooks.value = []
  } finally {
    discoveryLoading.value = false
  }
}

// Refresh the list whenever the "Open Existing" panel becomes visible
// so the user sees recent writes from other notebooks without a reload.
watch(showOpenForm, (visible) => {
  if (visible) void loadDiscoveredNotebooks()
})

async function openNotebook(path?: string) {
  const target = path
  if (!target) return
  loading.value = true
  dismissError()
  void preloadNotebookRoute()
  clearNotebookPerfMarks('open_click', 'open_response', 'open_request_ms', 'open_total_ms')
  markNotebookPerf('open_click')
  try {
    const data = await strata.openNotebook(target)
    markNotebookPerf('open_response')
    measureNotebookPerf('open_request_ms', 'open_click', 'open_response')
    const resolvedPath = data.path || target
    primePrefetchedNotebookSession(data)
    record(data.name, resolvedPath, data.session_id)
    markNotebookPerf('open_route_start')
    await router.push({
      name: 'notebook',
      params: { sessionId: data.session_id },
      query: { path: resolvedPath },
    })
  } catch (e: any) {
    error.value = e.message || 'Failed to open notebook'
    if (path) {
      const failedEntry = recentNotebooks.value.find((entry) => entry.path === path)
      failedRecentPath.value = path
      failedRecentName.value = failedEntry?.name ?? null
    }
  } finally {
    loading.value = false
  }
}

function dismissError() {
  error.value = null
  failedRecentPath.value = null
  failedRecentName.value = null
}

function forgetRecent(path: string) {
  // Local-only: drop from recents list without touching the directory.
  remove(path)
  if (failedRecentPath.value === path) {
    dismissError()
  }
}

async function deleteRecent(path: string, name: string) {
  // Destructive: rm -rf the notebook directory on disk + drop from recents.
  const confirmed = window.confirm(
    `Delete notebook "${name}"?\n\nThis permanently removes the directory:\n${path}\n\nThis cannot be undone.`,
  )
  if (!confirmed) return

  loading.value = true
  dismissError()
  try {
    await strata.deleteNotebookByPath(path)
  } catch {
    // If the backend returns 404, the directory is already gone —
    // fall through and remove from recents anyway.
  }
  remove(path)
  if (failedRecentPath.value === path) {
    dismissError()
  }
  loading.value = false
}

function formatTime(ts: number): string {
  const d = new Date(ts)
  const now = Date.now()
  const diff = now - ts
  if (diff < 60_000) return 'just now'
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m ago`
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h ago`
  return d.toLocaleDateString()
}
</script>

<template>
  <div class="home" data-testid="home-page">
    <div class="home-container">
      <div class="home-header">
        <span class="logo">◆ strata</span>
        <span class="subtitle">notebook</span>
      </div>

      <!-- Error banner -->
      <div v-if="error" class="error-banner">
        <div class="error-copy">
          <span>{{ error }}</span>
          <button
            v-if="failedRecentPath"
            type="button"
            class="btn-inline"
            data-testid="remove-failed-recent"
            @click="forgetRecent(failedRecentPath)"
          >
            Remove
            {{ failedRecentName ? `"${failedRecentName}"` : 'this notebook' }}
            from recents
          </button>
        </div>
        <button class="btn-dismiss" @click="dismissError">&times;</button>
      </div>

      <!-- Actions -->
      <div class="actions">
        <div
          class="action-card"
          data-testid="action-new-notebook"
          @click="showNewForm = true"
          @mouseenter="void preloadNotebookRoute()"
          @focusin="void preloadNotebookRoute()"
        >
          <div class="action-icon">+</div>
          <div class="action-label">New Notebook</div>
        </div>
        <div
          class="action-card"
          data-testid="action-open-notebook"
          @click="showOpenForm = true"
          @mouseenter="void preloadNotebookRoute()"
          @focusin="void preloadNotebookRoute()"
        >
          <div class="action-icon">📂</div>
          <div class="action-label">Open Existing</div>
        </div>
      </div>

      <!-- New notebook form -->
      <div v-if="showNewForm" class="form-card" data-testid="new-notebook-form">
        <h3>New Notebook</h3>
        <label class="form-label">
          Name
          <input
            v-model="newName"
            type="text"
            class="form-input"
            data-testid="new-notebook-name"
            placeholder="My Notebook"
            @keydown.enter="createNotebook"
          />
        </label>
        <label class="form-label">
          Parent directory
          <input
            v-model="newParentPath"
            type="text"
            class="form-input"
            data-testid="new-notebook-parent-path"
            :placeholder="FALLBACK_NOTEBOOK_PARENT_PATH"
          />
        </label>
        <label class="form-label">
          Python version
          <select
            v-model="selectedPythonVersion"
            class="form-input"
            data-testid="new-notebook-python-version"
            :disabled="pythonSelectionFixed || availablePythonVersions.length === 0"
          >
            <option v-for="version in availablePythonVersions" :key="version" :value="version">
              {{ version }}
            </option>
          </select>
          <span class="form-help">
            {{
              pythonSelectionFixed
                ? 'This deployment currently provides a fixed notebook Python version.'
                : 'Select the notebook-level Python version before creation.'
            }}
          </span>
        </label>
        <div class="form-actions">
          <button
            class="btn"
            data-testid="create-notebook-submit"
            :disabled="loading"
            @click="createNotebook"
          >
            Create
          </button>
          <button class="btn btn-secondary" @click="showNewForm = false">Cancel</button>
        </div>
      </div>

      <!-- Open notebook form -->
      <div v-if="showOpenForm" class="form-card" data-testid="open-notebook-form">
        <h3>Open Notebook</h3>
        <div class="discovery-root">
          Scanning <code>{{ discoveryRoot || '(storage root unknown)' }}</code>
          <button
            class="discovery-refresh"
            :disabled="discoveryLoading"
            title="Rescan for notebooks"
            @click="loadDiscoveredNotebooks"
          >
            ↻
          </button>
        </div>
        <div v-if="discoveryLoading" class="discovery-status">Scanning…</div>
        <div v-else-if="discoveryError" class="discovery-error">
          {{ discoveryError }}
        </div>
        <div v-else-if="!discoveredNotebooks.length" class="discovery-status">
          No notebooks found under the storage root.
        </div>
        <ul v-else class="discovery-list" data-testid="open-notebook-list">
          <li
            v-for="nb in discoveredNotebooks"
            :key="nb.path"
            class="discovery-item"
            :class="{ disabled: loading }"
            data-testid="open-notebook-item"
            @click="!loading && openNotebook(nb.path)"
          >
            <div class="discovery-name">{{ nb.name || nb.path.split('/').pop() }}</div>
            <div class="discovery-path">{{ nb.path }}</div>
          </li>
        </ul>
        <div class="form-actions">
          <button class="btn btn-secondary" @click="showOpenForm = false">Cancel</button>
        </div>
      </div>

      <!-- Recent notebooks -->
      <div v-if="recentNotebooks.length > 0" class="recent-section">
        <h3 class="section-title">Recent Notebooks</h3>
        <div class="recent-list">
          <div
            v-for="entry in recentNotebooks"
            :key="entry.path"
            class="recent-item"
            :data-testid="`recent-notebook-${entry.name}`"
            @click="openNotebook(entry.path)"
          >
            <div class="recent-info">
              <span class="recent-name">{{ entry.name }}</span>
              <span class="recent-path">{{ entry.path }}</span>
            </div>
            <div class="recent-meta">
              <span class="recent-time">{{ formatTime(entry.lastOpened) }}</span>
              <button
                type="button"
                class="recent-remove"
                :data-testid="`recent-delete-${entry.name}`"
                :aria-label="`Delete ${entry.name}`"
                title="Delete notebook directory from disk"
                @click.stop="deleteRecent(entry.path, entry.name)"
              >
                Delete
              </button>
            </div>
          </div>
        </div>
      </div>

      <!-- Loading overlay -->
      <div v-if="loading" class="loading-overlay" data-testid="home-loading">
        <div class="spinner"></div>
        <span>Loading notebook...</span>
      </div>
    </div>
  </div>
</template>

<style scoped>
.home {
  display: flex;
  justify-content: center;
  padding: 80px 24px 40px;
  min-height: 100vh;
}

.home-container {
  width: 100%;
  max-width: 640px;
}

.home-header {
  text-align: center;
  margin-bottom: 48px;
}

.logo {
  font-weight: 700;
  font-size: 28px;
  color: #89b4fa;
  letter-spacing: -0.5px;
}

.subtitle {
  font-size: 28px;
  color: #6c7086;
  margin-left: 8px;
  font-weight: 300;
}

.error-banner {
  background: #45252530;
  border: 1px solid #f38ba8;
  border-radius: 8px;
  color: #f38ba8;
  padding: 10px 16px;
  font-size: 13px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 24px;
}

.error-copy {
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-width: 0;
}

.btn-dismiss {
  background: none;
  border: none;
  color: #f38ba8;
  font-size: 18px;
  cursor: pointer;
  padding: 0 4px;
}

.btn-inline {
  align-self: flex-start;
  background: none;
  border: none;
  color: #f9e2af;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  padding: 0;
}

.btn-inline:hover {
  text-decoration: underline;
}

.actions {
  display: flex;
  gap: 16px;
  margin-bottom: 32px;
}

.action-card {
  flex: 1;
  background: #181825;
  border: 1px solid #313244;
  border-radius: 12px;
  padding: 24px;
  text-align: center;
  cursor: pointer;
  transition:
    border-color 0.15s,
    background 0.15s;
}

.action-card:hover {
  border-color: #89b4fa;
  background: #1e1e2e;
}

.action-icon {
  font-size: 28px;
  margin-bottom: 8px;
}

.action-label {
  font-size: 14px;
  font-weight: 600;
  color: #cdd6f4;
}

.form-card {
  background: #181825;
  border: 1px solid #313244;
  border-radius: 12px;
  padding: 24px;
  margin-bottom: 24px;
}

.form-card h3 {
  font-size: 16px;
  color: #cdd6f4;
  margin-bottom: 16px;
}

.form-label {
  display: block;
  font-size: 12px;
  color: #a6adc8;
  margin-bottom: 12px;
}

.form-input {
  display: block;
  width: 100%;
  padding: 8px 12px;
  margin-top: 4px;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 14px;
}

.discovery-root {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  color: #a6adc8;
  margin-bottom: 10px;
}
.discovery-root code {
  color: #cdd6f4;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
}
.discovery-refresh {
  margin-left: auto;
  background: transparent;
  border: 1px solid #45475a;
  color: #a6adc8;
  border-radius: 4px;
  padding: 2px 8px;
  cursor: pointer;
  font-size: 12px;
}
.discovery-refresh:hover:not(:disabled) {
  background: #313244;
  color: #cdd6f4;
}
.discovery-refresh:disabled {
  opacity: 0.4;
  cursor: default;
}
.discovery-status,
.discovery-error {
  font-size: 12px;
  color: #a6adc8;
  padding: 12px 4px;
}
.discovery-error {
  color: #f38ba8;
}
.discovery-list {
  list-style: none;
  padding: 0;
  margin: 0 0 12px 0;
  max-height: 320px;
  overflow-y: auto;
  border: 1px solid #313244;
  border-radius: 6px;
}
.discovery-item {
  padding: 8px 12px;
  cursor: pointer;
  border-bottom: 1px solid #313244;
}
.discovery-item:last-child {
  border-bottom: none;
}
.discovery-item:hover:not(.disabled) {
  background: #313244;
}
.discovery-item.disabled {
  cursor: default;
  opacity: 0.5;
}
.discovery-name {
  color: #cdd6f4;
  font-size: 13px;
  font-weight: 500;
}
.discovery-path {
  color: #6c7086;
  font-size: 11px;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  margin-top: 2px;
}

.form-input:focus {
  outline: none;
  border-color: #89b4fa;
  box-shadow: 0 0 0 2px rgba(137, 180, 250, 0.2);
}

.form-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
  margin-top: 16px;
}

.form-help {
  display: block;
  margin-top: 6px;
  color: #6c7086;
  font-size: 12px;
}

.section-title {
  font-size: 13px;
  color: #6c7086;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 12px;
}

.recent-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.recent-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  padding: 12px 16px;
  background: #181825;
  border: 1px solid transparent;
  border-radius: 8px;
  cursor: pointer;
  transition:
    border-color 0.15s,
    background 0.15s;
}

.recent-item:hover {
  border-color: #313244;
  background: #1e1e2e;
}

.recent-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}

.recent-name {
  font-size: 14px;
  font-weight: 500;
  color: #cdd6f4;
}

.recent-path {
  font-size: 12px;
  color: #6c7086;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.recent-time {
  font-size: 12px;
  color: #6c7086;
  flex-shrink: 0;
}

.recent-meta {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-shrink: 0;
}

.recent-remove {
  background: none;
  border: 1px solid #45475a;
  border-radius: 999px;
  color: #a6adc8;
  cursor: pointer;
  font-size: 12px;
  padding: 4px 10px;
  transition:
    border-color 0.15s,
    color 0.15s,
    background 0.15s;
}

.recent-remove:hover {
  background: #1e1e2e;
  border-color: #f38ba8;
  color: #f38ba8;
}

.loading-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(17, 17, 27, 0.8);
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 16px;
  z-index: 1000;
  color: #a6adc8;
  font-size: 14px;
}

.spinner {
  width: 32px;
  height: 32px;
  border: 3px solid #313244;
  border-top-color: #89b4fa;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
