<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'
import { useStrata } from '../composables/useStrata'
import { useRecentNotebooks } from '../stores/recentNotebooks'

const router = useRouter()
const strata = useStrata()
const { entries: recentNotebooks, record, remove } = useRecentNotebooks()

const FALLBACK_NOTEBOOK_PARENT_PATH = '/tmp/strata-notebooks'
const newName = ref('Untitled Notebook')
const newParentPath = ref(FALLBACK_NOTEBOOK_PARENT_PATH)
const showNewForm = ref(false)
const showOpenForm = ref(false)
const openPath = ref('')
const loading = ref(false)
const error = ref<string | null>(null)

onMounted(async () => {
  try {
    const data = await strata.getNotebookRuntimeConfig()
    const defaultParentPath =
      typeof data?.default_parent_path === 'string' && data.default_parent_path.trim()
        ? data.default_parent_path
        : FALLBACK_NOTEBOOK_PARENT_PATH
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
  error.value = null
  try {
    const notebookPath = `${newParentPath.value.replace(/\/+$/, '')}/${newName.value}`
    const data = await strata.createNotebook(newParentPath.value, newName.value)
    const resolvedPath = data.path || notebookPath
    record(data.name, resolvedPath, data.session_id)
    router.push({
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

async function openNotebook(path?: string) {
  const target = path || openPath.value.trim()
  if (!target) return
  loading.value = true
  error.value = null
  try {
    const data = await strata.openNotebook(target)
    record(data.name, target, data.session_id)
    router.push({
      name: 'notebook',
      params: { sessionId: data.session_id },
      query: { path: target },
    })
  } catch (e: any) {
    error.value = e.message || 'Failed to open notebook'
    // If opening from recent list failed, offer to remove
    if (path) {
      remove(path)
    }
  } finally {
    loading.value = false
  }
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
  <div class="home">
    <div class="home-container">
      <div class="home-header">
        <span class="logo">◆ strata</span>
        <span class="subtitle">notebook</span>
      </div>

      <!-- Error banner -->
      <div v-if="error" class="error-banner">
        {{ error }}
        <button class="btn-dismiss" @click="error = null">&times;</button>
      </div>

      <!-- Actions -->
      <div class="actions">
        <div class="action-card" @click="showNewForm = true">
          <div class="action-icon">+</div>
          <div class="action-label">New Notebook</div>
        </div>
        <div class="action-card" @click="showOpenForm = true">
          <div class="action-icon">📂</div>
          <div class="action-label">Open Existing</div>
        </div>
      </div>

      <!-- New notebook form -->
      <div v-if="showNewForm" class="form-card">
        <h3>New Notebook</h3>
        <label class="form-label">
          Name
          <input
            v-model="newName"
            type="text"
            class="form-input"
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
            :placeholder="FALLBACK_NOTEBOOK_PARENT_PATH"
          />
        </label>
        <div class="form-actions">
          <button class="btn" :disabled="loading" @click="createNotebook">Create</button>
          <button class="btn btn-secondary" @click="showNewForm = false">Cancel</button>
        </div>
      </div>

      <!-- Open notebook form -->
      <div v-if="showOpenForm" class="form-card">
        <h3>Open Notebook</h3>
        <label class="form-label">
          Path to notebook directory
          <input
            v-model="openPath"
            type="text"
            class="form-input"
            placeholder="/path/to/notebook"
            @keydown.enter="openNotebook()"
          />
        </label>
        <div class="form-actions">
          <button class="btn" :disabled="loading" @click="openNotebook()">Open</button>
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
            @click="openNotebook(entry.path)"
          >
            <div class="recent-info">
              <span class="recent-name">{{ entry.name }}</span>
              <span class="recent-path">{{ entry.path }}</span>
            </div>
            <span class="recent-time">{{ formatTime(entry.lastOpened) }}</span>
          </div>
        </div>
      </div>

      <!-- Loading overlay -->
      <div v-if="loading" class="loading-overlay">
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

.btn-dismiss {
  background: none;
  border: none;
  color: #f38ba8;
  font-size: 18px;
  cursor: pointer;
  padding: 0 4px;
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
  margin-left: 16px;
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
