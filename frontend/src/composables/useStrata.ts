/**
 * Communication layer with the Strata server.
 *
 * Day-one approach: REST calls to /v1/materialize + a mock fallback
 * so the UI works even without a running server.
 */

import { ref } from 'vue'
import type { CellOutput, MaterializeRequest, MaterializeResponse } from '../types/notebook'

const STRATA_BASE = import.meta.env.VITE_STRATA_URL ?? 'http://localhost:8765'

const connected = ref(false)

// ---------------------------------------------------------------------------
// Mock execution — lets us demo the UI without a live server
// ---------------------------------------------------------------------------

function mockExecute(source: string): CellOutput {
  // Simulate a Python cell that produces tabular data
  const lines = source.trim().split('\n')

  // If source looks like it assigns a list/dict, produce mock table
  if (source.includes('range(') || source.includes('[')) {
    const n = 10
    const columns = ['id', 'value', 'name']
    const rows = Array.from({ length: n }, (_, i) => ({
      id: i + 1,
      value: Math.round(Math.random() * 1000),
      name: `item_${i + 1}`,
    }))
    return { contentType: 'json/object', columns, rows, rowCount: n, cacheHit: false }
  }

  // If it references an upstream variable, pretend we got cached data
  if (source.includes('filter') || source.includes('query') || source.includes('select')) {
    const columns = ['id', 'value']
    const rows = Array.from({ length: 5 }, (_, i) => ({
      id: i * 10,
      value: Math.round(Math.random() * 500),
    }))
    return { contentType: 'json/object', columns, rows, rowCount: 5, cacheHit: true }
  }

  // Default: just show the code ran
  return {
    contentType: 'json/object',
    columns: ['result'],
    rows: [{ result: `Executed ${lines.length} line(s)` }],
    rowCount: 1,
    cacheHit: false,
  }
}

// ---------------------------------------------------------------------------
// Real Strata API calls
// ---------------------------------------------------------------------------

async function materialize(req: MaterializeRequest): Promise<MaterializeResponse> {
  const resp = await fetch(`${STRATA_BASE}/v1/materialize`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  })
  if (!resp.ok) {
    throw new Error(`Strata error: ${resp.status} ${await resp.text()}`)
  }
  return resp.json()
}

async function fetchStream(streamId: string): Promise<ArrayBuffer> {
  const resp = await fetch(`${STRATA_BASE}/v1/streams/${streamId}`)
  if (!resp.ok) throw new Error(`Stream error: ${resp.status}`)
  return resp.arrayBuffer()
}

async function throwApiError(resp: Response, fallback: string): Promise<never> {
  let detail = ''
  try {
    const payload = await resp.json()
    if (payload && typeof payload === 'object') {
      detail = String(
        (payload as Record<string, unknown>).detail ||
          (payload as Record<string, unknown>).error ||
          '',
      )
    }
  } catch {
    try {
      detail = (await resp.text()).trim()
    } catch {
      detail = ''
    }
  }

  if (detail) {
    throw new Error(detail)
  }
  throw new Error(`${fallback}: ${resp.status}`)
}

// ---------------------------------------------------------------------------
// Execute a cell — try real server, fall back to mock
// ---------------------------------------------------------------------------

async function executeCell(source: string, _language: string): Promise<CellOutput> {
  // For day-one: always use mock. When server is running, swap to real.
  try {
    const health = await fetch(`${STRATA_BASE}/health`, { signal: AbortSignal.timeout(500) })
    if (health.ok) {
      connected.value = true
      // TODO: Wire to real materialize call once notebook backend is ready
      // For now, even with server up, use mock since we don't have notebook endpoints yet
    }
  } catch {
    connected.value = false
  }

  // Simulate async work
  await new Promise((r) => setTimeout(r, 300 + Math.random() * 700))
  return mockExecute(source)
}

// ---------------------------------------------------------------------------
// Notebook API functions
// ---------------------------------------------------------------------------

async function openNotebook(path: string): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/open`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to open notebook: ${resp.status}`)
  }
  return resp.json()
}

async function createNotebook(parentPath: string, name: string): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/create`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ parent_path: parentPath, name }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to create notebook: ${resp.status}`)
  }
  return resp.json()
}

async function updateCellSource(
  notebookId: string,
  cellId: string,
  source: string,
): Promise<{ cell: any; dag: any; cells?: any[] }> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/cells/${cellId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ source }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to update cell: ${resp.status}`)
  }
  return resp.json()
}

async function addCell(notebookId: string, afterCellId?: string): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/cells`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ after_cell_id: afterCellId || null }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to add cell: ${resp.status}`)
  }
  return resp.json()
}

async function removeCell(notebookId: string, cellId: string): Promise<void> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/cells/${cellId}`, {
    method: 'DELETE',
  })
  if (!resp.ok) {
    throw new Error(`Failed to remove cell: ${resp.status}`)
  }
}

async function reorderCells(notebookId: string, cellIds: string[]): Promise<void> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/cells/reorder`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ cell_ids: cellIds }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to reorder cells: ${resp.status}`)
  }
}

async function updateNotebookMounts(notebookId: string, mounts: any[]): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/mounts`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mounts }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to update notebook mounts: ${resp.status}`)
  }
  return resp.json()
}

async function updateCellMounts(notebookId: string, cellId: string, mounts: any[]): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/cells/${cellId}/mounts`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ mounts }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to update cell mounts: ${resp.status}`)
  }
  return resp.json()
}

async function updateNotebookWorker(notebookId: string, worker: string | null): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/worker`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ worker }),
  })
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to update notebook worker')
  }
  return resp.json()
}

async function updateCellWorker(
  notebookId: string,
  cellId: string,
  worker: string | null,
): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/cells/${cellId}/worker`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ worker }),
  })
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to update cell worker')
  }
  return resp.json()
}

async function updateNotebookTimeout(notebookId: string, timeout: number | null): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/timeout`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ timeout }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to update notebook timeout: ${resp.status}`)
  }
  return resp.json()
}

async function updateCellTimeout(
  notebookId: string,
  cellId: string,
  timeout: number | null,
): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/cells/${cellId}/timeout`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ timeout }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to update cell timeout: ${resp.status}`)
  }
  return resp.json()
}

async function updateNotebookEnv(notebookId: string, env: Record<string, string>): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/env`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ env }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to update notebook env: ${resp.status}`)
  }
  return resp.json()
}

async function updateCellEnv(
  notebookId: string,
  cellId: string,
  env: Record<string, string>,
): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/cells/${cellId}/env`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ env }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to update cell env: ${resp.status}`)
  }
  return resp.json()
}

async function listWorkers(notebookId: string, refresh = false): Promise<any> {
  const params = refresh ? '?refresh=true' : ''
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/workers${params}`)
  if (!resp.ok) {
    throw new Error(`Failed to list workers: ${resp.status}`)
  }
  return resp.json()
}

async function updateWorkers(notebookId: string, workers: any[]): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/workers`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ workers }),
  })
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to update workers')
  }
  return resp.json()
}

async function listAdminNotebookWorkers(refresh = false): Promise<any> {
  const params = refresh ? '?refresh=true' : ''
  const resp = await fetch(`${STRATA_BASE}/v1/admin/notebook-workers${params}`)
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to list admin notebook workers')
  }
  return resp.json()
}

async function updateAdminNotebookWorkers(workers: any[]): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/admin/notebook-workers`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ workers }),
  })
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to update admin notebook workers')
  }
  return resp.json()
}

async function createAdminNotebookWorker(worker: any): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/admin/notebook-workers`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(worker),
  })
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to create admin notebook worker')
  }
  return resp.json()
}

async function replaceAdminNotebookWorker(workerName: string, worker: any): Promise<any> {
  const resp = await fetch(
    `${STRATA_BASE}/v1/admin/notebook-workers/${encodeURIComponent(workerName)}`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(worker),
    },
  )
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to replace admin notebook worker')
  }
  return resp.json()
}

async function patchAdminNotebookWorker(workerName: string, enabled: boolean): Promise<any> {
  const resp = await fetch(
    `${STRATA_BASE}/v1/admin/notebook-workers/${encodeURIComponent(workerName)}`,
    {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled }),
    },
  )
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to update admin notebook worker')
  }
  return resp.json()
}

async function deleteAdminNotebookWorker(workerName: string): Promise<any> {
  const resp = await fetch(
    `${STRATA_BASE}/v1/admin/notebook-workers/${encodeURIComponent(workerName)}`,
    {
      method: 'DELETE',
    },
  )
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to delete admin notebook worker')
  }
  return resp.json()
}

async function refreshAdminNotebookWorker(workerName: string): Promise<any> {
  const resp = await fetch(
    `${STRATA_BASE}/v1/admin/notebook-workers/${encodeURIComponent(workerName)}/refresh`,
    {
      method: 'POST',
    },
  )
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to refresh admin notebook worker')
  }
  return resp.json()
}

// ---------------------------------------------------------------------------
// Dependency API
// ---------------------------------------------------------------------------

async function listDependencies(notebookId: string): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/dependencies`)
  if (!resp.ok) {
    throw new Error(`Failed to list dependencies: ${resp.status}`)
  }
  return resp.json()
}

async function addDependency(notebookId: string, pkg: string): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/${notebookId}/dependencies`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ package: pkg }),
  })
  if (!resp.ok) {
    throw new Error(`Failed to add dependency: ${resp.status}`)
  }
  return resp.json()
}

async function removeDependency(notebookId: string, pkg: string): Promise<any> {
  const resp = await fetch(
    `${STRATA_BASE}/v1/notebooks/${notebookId}/dependencies/${encodeURIComponent(pkg)}`,
    {
      method: 'DELETE',
    },
  )
  if (!resp.ok) {
    throw new Error(`Failed to remove dependency: ${resp.status}`)
  }
  return resp.json()
}

// ---------------------------------------------------------------------------
// Session management
// ---------------------------------------------------------------------------

async function listSessions(): Promise<any[]> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/sessions`)
  if (!resp.ok) {
    await throwApiError(resp, 'Failed to list sessions')
  }
  const data = await resp.json()
  return data.sessions ?? []
}

async function getSession(sessionId: string): Promise<any> {
  const resp = await fetch(`${STRATA_BASE}/v1/notebooks/sessions/${sessionId}`)
  if (!resp.ok) {
    await throwApiError(resp, 'Session not found')
  }
  return resp.json()
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export function useStrata() {
  return {
    connected,
    executeCell,
    materialize,
    fetchStream,
    openNotebook,
    createNotebook,
    updateCellSource,
    addCell,
    removeCell,
    reorderCells,
    updateNotebookMounts,
    updateCellMounts,
    updateNotebookWorker,
    updateCellWorker,
    updateNotebookTimeout,
    updateCellTimeout,
    updateNotebookEnv,
    updateCellEnv,
    listWorkers,
    updateWorkers,
    listAdminNotebookWorkers,
    updateAdminNotebookWorkers,
    createAdminNotebookWorker,
    replaceAdminNotebookWorker,
    patchAdminNotebookWorker,
    deleteAdminNotebookWorker,
    refreshAdminNotebookWorker,
    listDependencies,
    addDependency,
    removeDependency,
    listSessions,
    getSession,
  }
}
