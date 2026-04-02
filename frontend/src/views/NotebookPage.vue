<script setup lang="ts">
import { defineAsyncComponent, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { useNotebook } from '../stores/notebook'
import { useRecentNotebooks } from '../stores/recentNotebooks'
import CellEditor from '../components/CellEditor.vue'

const DagView = defineAsyncComponent(() => import('../components/DagView.vue'))
const EnvironmentPanel = defineAsyncComponent(() => import('../components/EnvironmentPanel.vue'))
const MountsPanel = defineAsyncComponent(() => import('../components/MountsPanel.vue'))
const RuntimePanel = defineAsyncComponent(() => import('../components/RuntimePanel.vue'))
const WorkersPanel = defineAsyncComponent(() => import('../components/WorkersPanel.vue'))
const ProfilingPanel = defineAsyncComponent(() => import('../components/ProfilingPanel.vue'))
const ImpactPreview = defineAsyncComponent(() => import('../components/ImpactPreview.vue'))

const props = defineProps<{ sessionId: string }>()

const router = useRouter()
const { record, findBySessionId } = useRecentNotebooks()

const {
  notebook,
  orderedCells,
  connected,
  connectError,
  openBySessionId,
  openNotebook,
  addCell,
  removeCell,
  executeCellWebSocket,
  cleanupWebSocket,
} = useNotebook()

const editingName = ref(false)
const nameInput = ref<HTMLInputElement | null>(null)
const loading = ref(true)
const reconnectError = ref<string | null>(null)
const recoveryPath = ref<string | null>(null)
const sidebarWidth = ref(340)
let skipNextSessionReconnect: string | null = null

let resizePointerId: number | null = null
let resizeStartX = 0
let resizeStartWidth = 340

function clampSidebarWidth(width: number): number {
  return Math.min(560, Math.max(280, width))
}

onMounted(async () => {
  await connectToSession(props.sessionId)
})

watch(
  () => props.sessionId,
  async (newId) => {
    if (skipNextSessionReconnect === newId) {
      skipNextSessionReconnect = null
      return
    }
    cleanupWebSocket()
    await connectToSession(newId)
  },
)

async function connectToSession(sessionId: string) {
  loading.value = true
  reconnectError.value = null
  recoveryPath.value = null
  try {
    const data = await openBySessionId(sessionId)
    if (data && data.path) {
      record(data.name || notebook.name, data.path, data.session_id || sessionId)
    }
  } catch (e: any) {
    const message = e?.message || 'Failed to reconnect to notebook session'
    const recentEntry = findBySessionId(sessionId)
    if (recentEntry) {
      reconnectError.value = message
      recoveryPath.value = recentEntry.path
      return
    }

    console.error('Failed to open session:', e)
    router.replace({ name: 'home' })
    return
  } finally {
    loading.value = false
  }
}

async function reopenNotebookFromRecent() {
  if (!recoveryPath.value) return

  const path = recoveryPath.value
  loading.value = true
  reconnectError.value = null

  try {
    const data = await openNotebook(path)
    record(data.name || notebook.name, path, data.session_id)
    if (typeof data.session_id === 'string' && data.session_id !== props.sessionId) {
      skipNextSessionReconnect = data.session_id
      await router.replace({ name: 'notebook', params: { sessionId: data.session_id } })
    }
  } catch (e: any) {
    reconnectError.value = e?.message || 'Failed to reopen notebook'
    recoveryPath.value = path
  } finally {
    loading.value = false
  }
}

function stopSidebarResize() {
  resizePointerId = null
  document.body.classList.remove('resizing-sidebar')
  window.removeEventListener('pointermove', handleSidebarResize)
  window.removeEventListener('pointerup', stopSidebarResize)
}

function handleSidebarResize(event: PointerEvent) {
  if (resizePointerId == null) return
  sidebarWidth.value = clampSidebarWidth(resizeStartWidth - (event.clientX - resizeStartX))
}

function startSidebarResize(event: PointerEvent) {
  if (window.innerWidth <= 980) return
  resizePointerId = event.pointerId
  resizeStartX = event.clientX
  resizeStartWidth = sidebarWidth.value
  document.body.classList.add('resizing-sidebar')
  window.addEventListener('pointermove', handleSidebarResize)
  window.addEventListener('pointerup', stopSidebarResize)
}

onUnmounted(() => {
  stopSidebarResize()
  cleanupWebSocket()
})

function startEditName() {
  editingName.value = true
  setTimeout(() => nameInput.value?.select(), 0)
}

async function runCell(cellId: string) {
  const cell = orderedCells.value.find((c) => c.id === cellId)
  if (!cell || !cell.source.trim()) return
  executeCellWebSocket(cellId)
}

async function runAll() {
  for (const cell of orderedCells.value) {
    if (cell.source.trim()) {
      await runCell(cell.id)
    }
  }
}

function goHome() {
  router.push({ name: 'home' })
}
</script>

<template>
  <div class="app">
    <!-- Header -->
    <header class="header">
      <div class="header-left">
        <span class="logo" role="button" tabindex="0" title="Home" @click="goHome">◆ strata</span>
        <span v-if="!editingName" class="notebook-name" @dblclick="startEditName">
          {{ notebook.name }}
        </span>
        <input
          v-else
          ref="nameInput"
          v-model="notebook.name"
          class="name-input"
          @blur="editingName = false"
          @keydown.enter="editingName = false"
        />
      </div>
      <div class="header-right">
        <span class="connection" :class="{ connected: connected }">
          {{ loading ? '◌ Connecting…' : connected ? '● Live' : '○ Not connected' }}
        </span>
        <button class="btn" :disabled="!connected" @click="runAll">▶ Run All</button>
        <button class="btn btn-secondary" :disabled="!connected" @click="addCell()">+ Cell</button>
      </div>
    </header>

    <!-- Connection error banner -->
    <div v-if="connectError && !loading" class="error-banner">
      Server not reachable: {{ connectError }}
    </div>

    <div v-else-if="reconnectError && !loading" class="reconnect-state">
      <h2>Reconnect unavailable</h2>
      <p class="reconnect-message">{{ reconnectError }}</p>
      <p v-if="recoveryPath" class="reconnect-detail">
        Reopen the notebook from its last known path:
        <code>{{ recoveryPath }}</code>
      </p>
      <div class="reconnect-actions">
        <button class="btn" :disabled="loading" @click="reopenNotebookFromRecent">
          Reopen Notebook
        </button>
        <button class="btn btn-secondary" :disabled="loading" @click="goHome">Back Home</button>
      </div>
    </div>

    <!-- Main layout -->
    <div v-else-if="!loading" class="main" :style="{ '--sidebar-width': `${sidebarWidth}px` }">
      <!-- Cells -->
      <div class="cells-panel">
        <CellEditor
          v-for="cell in orderedCells"
          :key="cell.id"
          :cell="cell"
          @run="runCell"
          @delete="removeCell"
          @add-below="addCell"
        />
        <button class="add-cell-btn" :disabled="!connected" @click="addCell()">+ Add cell</button>
      </div>

      <div
        class="sidebar-resizer"
        role="separator"
        aria-label="Resize sidebar"
        aria-orientation="vertical"
        @pointerdown="startSidebarResize"
      ></div>

      <!-- DAG sidebar + profiling -->
      <aside class="sidebar">
        <DagView />
        <MountsPanel />
        <WorkersPanel />
        <RuntimePanel />
        <EnvironmentPanel />
        <ProfilingPanel />
      </aside>
    </div>

    <!-- v1.1: Impact preview dialog -->
    <ImpactPreview />
  </div>
</template>

<style scoped>
.reconnect-state {
  max-width: 720px;
  margin: 48px auto;
  padding: 24px;
  background: #181825;
  border: 1px solid #313244;
  border-radius: 12px;
}

.reconnect-state h2 {
  margin-bottom: 12px;
  font-size: 20px;
  color: #cdd6f4;
}

.reconnect-message {
  color: #f9e2af;
  margin-bottom: 12px;
  line-height: 1.5;
}

.reconnect-detail {
  color: #a6adc8;
  margin-bottom: 16px;
  line-height: 1.5;
}

.reconnect-detail code {
  color: #cdd6f4;
}

.reconnect-actions {
  display: flex;
  gap: 12px;
}
</style>
