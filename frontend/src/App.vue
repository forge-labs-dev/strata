<script setup lang="ts">
import { defineAsyncComponent, onMounted, onUnmounted, ref } from 'vue'
import { useNotebook } from './stores/notebook'
import CellEditor from './components/CellEditor.vue'

const DagView = defineAsyncComponent(() => import('./components/DagView.vue'))
const EnvironmentPanel = defineAsyncComponent(() => import('./components/EnvironmentPanel.vue'))
const MountsPanel = defineAsyncComponent(() => import('./components/MountsPanel.vue'))
const RuntimePanel = defineAsyncComponent(() => import('./components/RuntimePanel.vue'))
const WorkersPanel = defineAsyncComponent(() => import('./components/WorkersPanel.vue'))
const ProfilingPanel = defineAsyncComponent(() => import('./components/ProfilingPanel.vue'))
const ImpactPreview = defineAsyncComponent(() => import('./components/ImpactPreview.vue'))

const {
  notebook,
  orderedCells,
  connected,
  connectError,
  boot,
  openNotebook: openNotebookApi,
  addCell,
  removeCell,
  executeCellWebSocket,
} = useNotebook()

const editingName = ref(false)
const nameInput = ref<HTMLInputElement | null>(null)
const notebookPathInput = ref('')
const showOpenDialog = ref(false)
const booting = ref(true)
const sidebarWidth = ref(340)

let resizePointerId: number | null = null
let resizeStartX = 0
let resizeStartWidth = 340

function clampSidebarWidth(width: number): number {
  return Math.min(560, Math.max(280, width))
}

// Connect to backend immediately on mount
onMounted(async () => {
  await boot()
  booting.value = false
})

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

async function openNotebook() {
  if (!notebookPathInput.value.trim()) return
  try {
    await openNotebookApi(notebookPathInput.value)
    showOpenDialog.value = false
  } catch (err: any) {
    alert(`Failed to open notebook: ${err.message}`)
  }
}

async function retryBoot() {
  booting.value = true
  await boot()
  booting.value = false
}
</script>

<template>
  <div class="app">
    <!-- Header -->
    <header class="header">
      <div class="header-left">
        <span class="logo">◆ strata</span>
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
          {{ booting ? '◌ Connecting…' : connected ? '● Live' : '○ Not connected' }}
        </span>
        <button class="btn btn-secondary" @click="showOpenDialog = true">Open</button>
        <button class="btn" :disabled="!connected" @click="runAll">▶ Run All</button>
        <button class="btn btn-secondary" :disabled="!connected" @click="addCell()">+ Cell</button>
      </div>
    </header>

    <!-- Connection error banner -->
    <div v-if="connectError && !booting" class="error-banner">
      Server not reachable: {{ connectError }}
      <button class="btn btn-secondary" style="margin-left: 12px" @click="retryBoot">Retry</button>
    </div>

    <!-- Main layout -->
    <div class="main" :style="{ '--sidebar-width': `${sidebarWidth}px` }">
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

    <!-- Open notebook dialog -->
    <div v-if="showOpenDialog" class="modal-overlay" @click="showOpenDialog = false">
      <div class="modal-dialog" @click.stop>
        <h2>Open Notebook</h2>
        <input
          v-model="notebookPathInput"
          type="text"
          placeholder="/path/to/notebook"
          class="modal-input"
          @keydown.enter="openNotebook"
        />
        <div class="modal-actions">
          <button class="btn" @click="openNotebook">Open</button>
          <button class="btn btn-secondary" @click="showOpenDialog = false">Cancel</button>
        </div>
      </div>
    </div>
  </div>
</template>

<style>
*,
*::before,
*::after {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

body {
  background: #11111b;
  color: #cdd6f4;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  -webkit-font-smoothing: antialiased;
}

.app {
  display: flex;
  flex-direction: column;
  height: 100vh;
}

.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 16px;
  background: #181825;
  border-bottom: 1px solid #2a2a3c;
  flex-shrink: 0;
}
.header-left {
  display: flex;
  align-items: center;
  gap: 16px;
}
.header-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.logo {
  font-weight: 700;
  font-size: 16px;
  color: #89b4fa;
  letter-spacing: -0.5px;
}
.notebook-name {
  font-size: 14px;
  color: #a6adc8;
  cursor: pointer;
  padding: 2px 6px;
  border-radius: 4px;
}
.notebook-name:hover {
  background: #313244;
}
.name-input {
  font-size: 14px;
  background: #313244;
  border: 1px solid #89b4fa;
  color: #cdd6f4;
  padding: 2px 6px;
  border-radius: 4px;
  outline: none;
}

.connection {
  font-size: 12px;
  color: #6c7086;
}
.connection.connected {
  color: #a6e3a1;
}

.error-banner {
  background: #45252530;
  border-bottom: 1px solid #f38ba8;
  color: #f38ba8;
  padding: 8px 16px;
  font-size: 13px;
  display: flex;
  align-items: center;
}

.btn {
  background: #89b4fa;
  color: #1e1e2e;
  border: none;
  padding: 6px 14px;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
}
.btn:hover {
  background: #74c7ec;
}
.btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
.btn-secondary {
  background: #313244;
  color: #cdd6f4;
}
.btn-secondary:hover {
  background: #45475a;
}

.main {
  display: flex;
  flex: 1;
  overflow: hidden;
  min-height: 0;
}
.cells-panel {
  flex: 1;
  overflow-y: auto;
  padding: 16px;
  min-width: 0;
}
.sidebar-resizer {
  width: 12px;
  flex-shrink: 0;
  position: relative;
  cursor: col-resize;
  touch-action: none;
}
.sidebar-resizer::before {
  content: '';
  position: absolute;
  top: 16px;
  bottom: 16px;
  left: 50%;
  width: 1px;
  transform: translateX(-50%);
  background: #2a2a3c;
}
.sidebar-resizer:hover::before {
  background: #89b4fa;
}
.sidebar {
  width: var(--sidebar-width);
  flex-shrink: 0;
  min-width: 280px;
  max-width: 560px;
  padding: 16px 16px 16px 4px;
  overflow-y: auto;
}

.add-cell-btn {
  width: 100%;
  padding: 12px;
  background: none;
  border: 1px dashed #313244;
  border-radius: 8px;
  color: #6c7086;
  font-size: 13px;
  cursor: pointer;
  margin-top: 4px;
}
.add-cell-btn:hover {
  border-color: #89b4fa;
  color: #89b4fa;
}
.add-cell-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-dialog {
  background: #181825;
  border: 1px solid #313244;
  border-radius: 8px;
  padding: 24px;
  min-width: 400px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
}

.modal-dialog h2 {
  margin-bottom: 16px;
  font-size: 18px;
  color: #cdd6f4;
}

.modal-input {
  width: 100%;
  padding: 8px 12px;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 14px;
  margin-bottom: 16px;
  box-sizing: border-box;
}

.modal-input:focus {
  outline: none;
  border-color: #89b4fa;
  box-shadow: 0 0 0 2px rgba(137, 180, 250, 0.2);
}

.modal-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
}

body.resizing-sidebar {
  cursor: col-resize;
  user-select: none;
}

@media (max-width: 980px) {
  .main {
    flex-direction: column;
  }

  .cells-panel {
    padding-bottom: 8px;
  }

  .sidebar-resizer {
    display: none;
  }

  .sidebar {
    width: auto;
    min-width: 0;
    max-width: none;
    padding: 12px 16px 16px;
    border-top: 1px solid #2a2a3c;
  }
}
</style>
