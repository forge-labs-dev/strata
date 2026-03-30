import { reactive, computed, ref } from 'vue'
import type {
  Cell,
  CellId,
  CellOutput,
  CellStatus,
  DagEdge,
  DependencyInfo,
  Notebook,
  WsMessage,
  ImpactPreview,
  ProfilingSummary,
  MountSpec,
  CellAnnotations,
  EditableWorkerSpec,
  ManagedWorkerSpec,
  WorkerCatalogEntry,
  WorkerHealth,
  WorkerSpec,
} from '../types/notebook'
import { useStrata } from '../composables/useStrata'
import { useWebSocket } from '../composables/useWebSocket'
import {
  applyWorkerHealth,
  effectiveWorkerNameForCell,
  isRemoteExecutorLikelyUnreachable,
} from '../utils/notebookWorkers'

let nextOrder = 0

// ---------------------------------------------------------------------------
// Connection state — visible to the UI
// ---------------------------------------------------------------------------
const connected = ref(false)
const connectError = ref<string | null>(null)

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

const notebook = reactive<Notebook>({
  id: '',
  name: 'Untitled Notebook',
  worker: null,
  timeout: null,
  env: {},
  workers: [],
  mounts: [],
  cells: [],
  environment: { pythonVersion: '', lockfileHash: '', packageCount: 0 },
  createdAt: Date.now(),
  updatedAt: Date.now(),
})

// --- Derived state ---------------------------------------------------------

const cellMap = computed(() => {
  const m = new Map<CellId, Cell>()
  for (const c of notebook.cells) m.set(c.id, c)
  return m
})

const orderedCells = computed(() => [...notebook.cells].sort((a, b) => a.order - b.order))

/** Build DAG edges from the variable define/reference relationships */
const dagEdges = computed<DagEdge[]>(() => {
  const edges: DagEdge[] = []
  const defMap = new Map<string, CellId>()
  for (const c of notebook.cells) {
    for (const v of c.defines) defMap.set(v, c.id)
  }
  for (const c of notebook.cells) {
    for (const v of c.references) {
      const fromId = defMap.get(v)
      if (fromId && fromId !== c.id) {
        edges.push({ from_cell_id: fromId, to_cell_id: c.id, variable: v })
      }
    }
  }
  return edges
})

// --- Helper: sessionId accessor -------------------------------------------

function sessionId(): string | undefined {
  return (notebook as any).sessionId
}

// --- Cell mutations (always go through backend when connected) -------------

async function addCell(afterId?: CellId) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  try {
    const data = await strata.addCell(sid, afterId)
    const newCell: Cell = {
      id: data.id,
      source: data.source || '',
      language: data.language || 'python',
      order: data.order ?? nextOrder++,
      status: 'idle',
      worker: data.worker ?? notebook.worker ?? null,
      workerOverride: data.worker_override ?? null,
      timeout: data.timeout ?? notebook.timeout ?? null,
      timeoutOverride: data.timeout_override ?? null,
      env: parseEnvMap(data.env),
      envOverrides: parseEnvMap(data.env_overrides),
      mounts: Array.isArray(data.mounts) ? data.mounts.map(parseMountSpec) : [...notebook.mounts],
      mountOverrides: Array.isArray(data.mount_overrides)
        ? data.mount_overrides.map(parseMountSpec)
        : [],
      annotations: parseBackendAnnotations(data.annotations) || {
        worker: null,
        timeout: null,
        env: {},
        mounts: [],
      },
      upstreamIds: [],
      downstreamIds: [],
      defines: [],
      references: [],
      inputs: [],
      isLeaf: false,
    }
    const idx = afterId
      ? notebook.cells.findIndex((c) => c.id === afterId) + 1
      : notebook.cells.length
    notebook.cells.splice(idx, 0, newCell)
    notebook.updatedAt = Date.now()
  } catch (err) {
    console.error('Failed to add cell:', err)
  }
}

function removeCell(id: CellId) {
  if (notebook.cells.length <= 1) return
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  strata
    .removeCell(sid, id)
    .then(() => {
      const idx = notebook.cells.findIndex((c) => c.id === id)
      if (idx >= 0) {
        notebook.cells.splice(idx, 1)
        notebook.updatedAt = Date.now()
      }
    })
    .catch((err) => {
      console.error('Failed to remove cell:', err)
    })
}

async function updateSource(id: CellId, source: string) {
  const cell = cellMap.value.get(id)
  if (!cell) return
  cell.source = source

  // Regex as instant preview
  cell.defines = extractDefines(source)
  cell.references = extractReferences(source, cell.defines)

  // Send to backend for authoritative analysis
  const sid = sessionId()
  if (sid) {
    const strata = useStrata()
    try {
      const response = await strata.updateCellSource(sid, id, source)
      if (response.cell) {
        cell.defines = response.cell.defines || []
        cell.references = response.cell.references || []
        cell.upstreamIds = response.cell.upstream_ids || []
        cell.downstreamIds = response.cell.downstream_ids || []
        cell.isLeaf = response.cell.is_leaf || false
      }
      if (response.dag) {
        applyBackendDag(response.dag)
      }
      // Sync cell statuses from backend — compute_staleness runs
      // on the server after every edit, so the backend is the
      // authoritative source for which cells are stale.
      if (response.cells && Array.isArray(response.cells)) {
        syncCellsFromBackend(response.cells)
      }
    } catch (err) {
      console.warn('Backend analysis failed, using regex fallback:', err)
      rebuildDag()
    }
  } else {
    rebuildDag()
  }

  if (cell.status === 'ready') {
    markDownstreamStale(id)
  }
  notebook.updatedAt = Date.now()
}

function setCellStatus(id: CellId, status: CellStatus) {
  const cell = cellMap.value.get(id)
  if (cell) cell.status = status
}

function setCellOutput(id: CellId, output: CellOutput) {
  const cell = cellMap.value.get(id)
  if (cell) {
    cell.output = output
    cell.status = output.error ? 'error' : 'ready'
    cell.lastRunAt = Date.now()
  }
}

function parseBackendCausality(raw: any): Cell['causality'] | undefined {
  if (!raw) return undefined
  return {
    reason: raw.reason,
    details: (raw.details || []).map((d: Record<string, any>) => ({
      type: d.type,
      cellId: d.cell_id,
      cellName: d.cell_name,
      fromVersion: d.from_version,
      toVersion: d.to_version,
      package: d.package,
      fromPackageVersion: d.from_package_version,
      toPackageVersion: d.to_package_version,
    })),
  }
}

function parseMountSpec(raw: any): MountSpec {
  return {
    name: raw.name,
    uri: raw.uri,
    mode: raw.mode || 'ro',
    pin: raw.pin ?? null,
  }
}

function parseWorkerSpec(raw: any): WorkerSpec {
  return {
    name: String(raw?.name || ''),
    backend: raw?.backend === 'executor' ? 'executor' : 'local',
    runtimeId: raw?.runtime_id ?? raw?.runtimeId ?? null,
    config: raw?.config && typeof raw.config === 'object' ? raw.config : {},
  }
}

function parseManagedWorkerSpec(raw: any): ManagedWorkerSpec {
  return {
    ...parseWorkerSpec(raw),
    enabled: raw?.enabled !== false,
  }
}

function parseWorkerCatalogEntry(raw: any): WorkerCatalogEntry {
  const transport =
    raw?.transport === 'local' ||
    raw?.transport === 'embedded' ||
    raw?.transport === 'direct' ||
    raw?.transport === 'signed' ||
    raw?.transport === 'executor'
      ? raw.transport
      : undefined
  return {
    ...parseWorkerSpec(raw),
    health: raw?.health === 'healthy' || raw?.health === 'unavailable' ? raw.health : 'unknown',
    source:
      raw?.source === 'builtin' ||
      raw?.source === 'notebook' ||
      raw?.source === 'server' ||
      raw?.source === 'referenced'
        ? raw.source
        : undefined,
    allowed: raw?.allowed !== false,
    enabled: typeof raw?.enabled === 'boolean' ? raw.enabled : undefined,
    transport,
    healthUrl: typeof raw?.health_url === 'string' ? raw.health_url : null,
    healthCheckedAt:
      typeof raw?.health_checked_at === 'number' && Number.isFinite(raw.health_checked_at)
        ? raw.health_checked_at
        : null,
    lastError: typeof raw?.last_error === 'string' && raw.last_error.trim() ? raw.last_error : null,
  }
}

function parseBackendAnnotations(raw: any): CellAnnotations | undefined {
  if (!raw) return undefined
  return {
    worker: raw.worker ?? null,
    timeout: raw.timeout ?? null,
    env: raw.env || {},
    mounts: Array.isArray(raw.mounts) ? raw.mounts.map(parseMountSpec) : [],
  }
}

function parseEnvMap(raw: any): Record<string, string> {
  if (!raw || typeof raw !== 'object') return {}
  return Object.fromEntries(Object.entries(raw).map(([key, value]) => [key, String(value)]))
}

function applyBackendCellState(localCell: Cell, serverCell: any) {
  localCell.defines = serverCell.defines || []
  localCell.references = serverCell.references || []
  localCell.upstreamIds = serverCell.upstream_ids || []
  localCell.downstreamIds = serverCell.downstream_ids || []
  localCell.isLeaf = serverCell.is_leaf || false
  localCell.status = serverCell.status || 'idle'
  localCell.worker = serverCell.worker ?? null
  localCell.workerOverride = serverCell.worker_override ?? null
  localCell.timeout = serverCell.timeout ?? null
  localCell.timeoutOverride = serverCell.timeout_override ?? null
  localCell.env = parseEnvMap(serverCell.env)
  localCell.envOverrides = parseEnvMap(serverCell.env_overrides)
  localCell.mounts = Array.isArray(serverCell.mounts) ? serverCell.mounts.map(parseMountSpec) : []
  localCell.mountOverrides = Array.isArray(serverCell.mount_overrides)
    ? serverCell.mount_overrides.map(parseMountSpec)
    : []
  localCell.annotations = parseBackendAnnotations(serverCell.annotations)
  localCell.stalenessReasons = serverCell.staleness_reasons || serverCell.staleness?.reasons || []
  localCell.causality = parseBackendCausality(serverCell.causality)
}

function syncCellsFromBackend(serverCells: any[]) {
  for (const serverCell of serverCells) {
    const localCell = cellMap.value.get(serverCell.id)
    if (!localCell) continue
    applyBackendCellState(localCell, serverCell)
  }
}

function syncNotebookMountsFromBackend(serverMounts: any[]) {
  notebook.mounts = Array.isArray(serverMounts) ? serverMounts.map(parseMountSpec) : []
}

function syncNotebookWorkerFromBackend(serverWorker: any) {
  notebook.worker = typeof serverWorker === 'string' && serverWorker.trim() ? serverWorker : null
}

function syncNotebookWorkersFromBackend(serverWorkers: any[]) {
  notebook.workers = Array.isArray(serverWorkers)
    ? serverWorkers.map(parseWorkerSpec).filter((worker) => worker.name)
    : []
}

function syncWorkerCatalogFromBackend(serverWorkers: any[]) {
  availableWorkers.value = Array.isArray(serverWorkers)
    ? serverWorkers.map(parseWorkerCatalogEntry).filter((worker) => worker.name)
    : []
}

function syncWorkerHealthCheckedAtFromBackend(rawCheckedAt: unknown) {
  if (typeof rawCheckedAt === 'number' && Number.isFinite(rawCheckedAt)) {
    workerHealthCheckedAt.value = rawCheckedAt
  }
}

function updateWorkerHealth(workerName: string, health: WorkerHealth) {
  availableWorkers.value = applyWorkerHealth(availableWorkers.value, workerName, health)
}

function syncWorkerDefinitionsEditableFromBackend(value: any) {
  workerDefinitionsEditable.value = value !== false
}

function syncServerManagedWorkersFromBackend(serverWorkers: any[]) {
  serverManagedWorkers.value = Array.isArray(serverWorkers)
    ? serverWorkers.map(parseManagedWorkerSpec).filter((worker) => worker.name)
    : []
}

function clearServerWorkerRegistryState() {
  serverManagedWorkers.value = []
  serverWorkerRegistryAvailable.value = false
  serverWorkerRegistryLoading.value = false
  serverWorkerActionLoading.value = {}
  serverWorkerRegistryError.value = null
}

function syncNotebookTimeoutFromBackend(serverTimeout: any) {
  notebook.timeout = typeof serverTimeout === 'number' ? serverTimeout : null
}

function syncNotebookEnvFromBackend(serverEnv: any) {
  notebook.env = parseEnvMap(serverEnv)
}

function moveCell(id: CellId, direction: 'up' | 'down') {
  const sorted = orderedCells.value
  const idx = sorted.findIndex((c) => c.id === id)
  const swapIdx = direction === 'up' ? idx - 1 : idx + 1
  if (swapIdx < 0 || swapIdx >= sorted.length) return
  const tmp = sorted[idx].order
  sorted[idx].order = sorted[swapIdx].order
  sorted[swapIdx].order = tmp
  notebook.updatedAt = Date.now()
}

// --- DAG helpers -----------------------------------------------------------

function rebuildDag() {
  const defMap = new Map<string, CellId>()
  for (const c of notebook.cells) {
    for (const v of c.defines) defMap.set(v, c.id)
  }
  for (const c of notebook.cells) {
    c.upstreamIds = []
    c.downstreamIds = []
  }
  for (const c of notebook.cells) {
    for (const v of c.references) {
      const fromId = defMap.get(v)
      if (fromId && fromId !== c.id) {
        if (!c.upstreamIds.includes(fromId)) c.upstreamIds.push(fromId)
        const upstream = cellMap.value.get(fromId)
        if (upstream && !upstream.downstreamIds.includes(c.id)) {
          upstream.downstreamIds.push(c.id)
        }
      }
    }
  }
}

function markDownstreamStale(id: CellId) {
  const cell = cellMap.value.get(id)
  if (!cell) return
  for (const downId of cell.downstreamIds) {
    const down = cellMap.value.get(downId)
    if (down && down.status === 'ready') {
      down.status = 'stale'
      markDownstreamStale(downId)
    }
  }
}

function applyBackendDag(backendDag: any) {
  for (const cell of notebook.cells) {
    cell.upstreamIds = []
    cell.downstreamIds = []
    cell.isLeaf = backendDag.leaves?.includes(cell.id) || false
  }
  if (backendDag.edges && Array.isArray(backendDag.edges)) {
    for (const edge of backendDag.edges) {
      const fromCell = cellMap.value.get(edge.from_cell_id)
      const toCell = cellMap.value.get(edge.to_cell_id)
      if (fromCell && toCell) {
        if (!toCell.upstreamIds.includes(edge.from_cell_id)) {
          toCell.upstreamIds.push(edge.from_cell_id)
        }
        if (!fromCell.downstreamIds.includes(edge.to_cell_id)) {
          fromCell.downstreamIds.push(edge.to_cell_id)
        }
      }
    }
  }
}

// --- Simple variable extraction -------------------------------------------

function extractDefines(source: string): string[] {
  const defs: string[] = []
  const re = /^([a-zA-Z_]\w*)\s*=[^=]/gm
  let m: RegExpExecArray | null
  while ((m = re.exec(source)) !== null) {
    if (!defs.includes(m[1])) defs.push(m[1])
  }
  return defs
}

function extractReferences(source: string, localDefs: string[]): string[] {
  const refs: string[] = []
  const keywords = new Set([
    'and',
    'as',
    'assert',
    'async',
    'await',
    'break',
    'class',
    'continue',
    'def',
    'del',
    'elif',
    'else',
    'except',
    'finally',
    'for',
    'from',
    'global',
    'if',
    'import',
    'in',
    'is',
    'lambda',
    'nonlocal',
    'not',
    'or',
    'pass',
    'raise',
    'return',
    'try',
    'while',
    'with',
    'yield',
    'True',
    'False',
    'None',
    'print',
    'len',
    'range',
    'int',
    'str',
    'float',
    'list',
    'dict',
    'set',
    'tuple',
    'type',
    'isinstance',
  ])
  const re = /\b([a-zA-Z_]\w*)\b/g
  let m: RegExpExecArray | null
  while ((m = re.exec(source)) !== null) {
    const name = m[1]
    if (!keywords.has(name) && !localDefs.includes(name) && !refs.includes(name)) {
      refs.push(name)
    }
  }
  return refs
}

// --- API Integration -------------------------------------------------------

/**
 * Boot: create scratch notebook, add one empty cell, connect WebSocket.
 * Called once on app mount. Resolves when ready to use.
 */
async function boot(): Promise<void> {
  const strata = useStrata()
  try {
    connectError.value = null
    notebookWorkerError.value = null
    workerRegistryError.value = null
    cellWorkerErrors.value = {}
    workerHealthCheckedAt.value = null
    clearServerWorkerRegistryState()
    // Create a scratch notebook
    const data = await strata.createNotebook('/tmp/strata-notebooks', 'scratch')
    notebook.id = data.id
    notebook.name = data.name
    notebook.worker = data.worker ?? null
    notebook.timeout = data.timeout ?? null
    notebook.env = parseEnvMap(data.env)
    notebook.workers = Array.isArray(data.workers) ? data.workers.map(parseWorkerSpec) : []
    notebook.mounts = Array.isArray(data.mounts) ? data.mounts.map(parseMountSpec) : []
    notebook.createdAt = data.created_at ? new Date(data.created_at).getTime() : Date.now()
    notebook.updatedAt = data.updated_at ? new Date(data.updated_at).getTime() : Date.now()
    ;(notebook as any).sessionId = data.session_id

    // New notebook starts with 0 cells — add one empty cell
    const cellData = await strata.addCell(data.session_id)
    notebook.cells = [
      {
        id: cellData.id,
        source: '',
        language: cellData.language || 'python',
        order: 0,
        status: 'idle' as CellStatus,
        worker: notebook.worker,
        workerOverride: null,
        timeout: notebook.timeout,
        timeoutOverride: null,
        env: { ...notebook.env },
        envOverrides: {},
        mounts: [...notebook.mounts],
        mountOverrides: [],
        annotations: { worker: null, timeout: null, env: {}, mounts: [] },
        upstreamIds: [],
        downstreamIds: [],
        defines: [],
        references: [],
        inputs: [],
        isLeaf: false,
      },
    ]

    // Connect WebSocket and wait for it
    initializeWebSocket()
    await waitForWebSocket()

    connected.value = true
    fetchWorkers()
    fetchDependencies()
  } catch (e: any) {
    console.error('Failed to boot notebook:', e)
    connectError.value = e.message || 'Failed to connect to server'
    connected.value = false
  }
}

/**
 * Open an existing notebook from disk.
 */
async function openNotebook(path: string): Promise<void> {
  const strata = useStrata()
  // Cleanup existing WebSocket
  cleanupWebSocket()
  notebookWorkerError.value = null
  workerRegistryError.value = null
  cellWorkerErrors.value = {}
  workerHealthCheckedAt.value = null
  clearServerWorkerRegistryState()

  const data = await strata.openNotebook(path)
  notebook.id = data.id
  notebook.name = data.name
  notebook.worker = data.worker ?? null
  notebook.timeout = data.timeout ?? null
  notebook.env = parseEnvMap(data.env)
  notebook.workers = Array.isArray(data.workers) ? data.workers.map(parseWorkerSpec) : []
  notebook.mounts = Array.isArray(data.mounts) ? data.mounts.map(parseMountSpec) : []
  notebook.createdAt = data.created_at ? new Date(data.created_at).getTime() : Date.now()
  notebook.updatedAt = data.updated_at ? new Date(data.updated_at).getTime() : Date.now()
  ;(notebook as any).sessionId = data.session_id

  notebook.cells = data.cells.map((c: any) => ({
    id: c.id,
    source: c.source || '',
    language: c.language || 'python',
    order: c.order ?? 0,
    status: (c.status || 'idle') as CellStatus,
    worker: c.worker ?? null,
    workerOverride: c.worker_override ?? null,
    timeout: c.timeout ?? null,
    timeoutOverride: c.timeout_override ?? null,
    env: parseEnvMap(c.env),
    envOverrides: parseEnvMap(c.env_overrides),
    mounts: Array.isArray(c.mounts) ? c.mounts.map(parseMountSpec) : [],
    mountOverrides: Array.isArray(c.mount_overrides) ? c.mount_overrides.map(parseMountSpec) : [],
    annotations: parseBackendAnnotations(c.annotations),
    upstreamIds: c.upstream_ids || [],
    downstreamIds: c.downstream_ids || [],
    defines: c.defines || [],
    references: c.references || [],
    inputs: [],
    isLeaf: c.is_leaf || false,
    stalenessReasons: c.staleness_reasons || c.staleness?.reasons || [],
    causality: parseBackendCausality(c.causality),
  }))
  notebook.cells.sort((a, b) => a.order - b.order)
  if (data.dag) {
    applyBackendDag(data.dag)
  }

  initializeWebSocket()
  await waitForWebSocket()
  connected.value = true

  // Load dependencies after connection is established
  fetchWorkers()
  fetchDependencies()
}

// --- WebSocket integration -------------------------------------------------

// v1.1: Impact preview and profiling state
const currentImpactPreview = ref<ImpactPreview | null>(null)
const profilingSummary = ref<ProfilingSummary | null>(null)

// Environment / dependency state
const dependencies = ref<DependencyInfo[]>([])
const dependencyLoading = ref(false)
const dependencyError = ref<string | null>(null)
const availableWorkers = ref<WorkerCatalogEntry[]>([])
const workerDefinitionsEditable = ref(true)
const workerHealthLoading = ref(false)
const workerHealthCheckedAt = ref<number | null>(null)
const notebookWorkerError = ref<string | null>(null)
const workerRegistryError = ref<string | null>(null)
const serverManagedWorkers = ref<ManagedWorkerSpec[]>([])
const serverWorkerRegistryAvailable = ref(false)
const serverWorkerRegistryLoading = ref(false)
const serverWorkerActionLoading = ref<Record<string, boolean>>({})
const serverWorkerRegistryError = ref<string | null>(null)
const cellWorkerErrors = ref<Record<string, string>>({})

// Inspect REPL state
interface InspectEntry {
  expr: string
  result?: string
  error?: string
  type?: string
  stdout?: string
}
const inspectCellId = ref<CellId | null>(null)
const inspectReady = ref(false)
const inspectHistory = ref<InspectEntry[]>([])

let wsInstance: ReturnType<typeof useWebSocket> | null = null

function initializeWebSocket() {
  if (!wsInstance) {
    const notebookId = (notebook as any).sessionId
    if (!notebookId) return

    wsInstance = useWebSocket(notebookId)

    wsInstance.onMessage('cell_status', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      const cellId = p.cell_id as CellId
      const status = p.status as CellStatus
      setCellStatus(cellId, status)

      const cell = cellMap.value.get(cellId)
      if (cell && p.causality) {
        const rawCausality = p.causality as Record<string, any>
        cell.causality = {
          reason: rawCausality.reason,
          details: (rawCausality.details || []).map((d: Record<string, any>) => ({
            type: d.type,
            cellId: d.cell_id,
            cellName: d.cell_name,
            fromVersion: d.from_version,
            toVersion: d.to_version,
            package: d.package,
            fromPackageVersion: d.from_package_version,
            toPackageVersion: d.to_package_version,
          })),
        }
      } else if (cell && status !== 'stale') {
        cell.causality = undefined
      }

      if (cell && p.staleness_reasons) {
        cell.stalenessReasons = p.staleness_reasons
      }
    })

    wsInstance.onMessage('cell_output', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      const cellId = p.cell_id as CellId
      const outputs = p.outputs as Record<string, any> | undefined

      let output: CellOutput = {
        contentType: 'json/object',
        cacheHit: p.cache_hit || false,
        cacheLoadMs: p.duration_ms,
        artifactUri: p.artifact_uri,
      }

      if (outputs && typeof outputs === 'object') {
        // Only display the '_' variable (last-expression result, like
        // Jupyter's Out[n]).  Other outputs are pass-through artifacts
        // for downstream cells and should not be rendered.
        const firstVar = outputs['_'] || null
        if (firstVar) {
          const contentType = firstVar?.content_type || 'json/object'
          output.contentType = contentType as CellOutput['contentType']

          if (contentType === 'arrow/ipc') {
            const src = firstVar?.data || firstVar
            output.columns = src?.columns
            output.rowCount = src?.rows ?? src?.row_count ?? src?.rowCount

            // preview is a 2D array [[row0col0, row0col1], ...] — convert
            // to array of objects [{col0: val, col1: val}, ...] for the table
            const preview = src?.preview
            if (Array.isArray(preview) && output.columns) {
              output.rows = preview.map((row: unknown[]) => {
                const obj: Record<string, unknown> = {}
                output.columns!.forEach((col, i) => {
                  obj[col] = row[i]
                })
                return obj
              })
            }
          } else if (contentType === 'module/import') {
            // Module outputs are pass-through artifacts, not display values
          } else {
            // JSON/scalar outputs: prefer .data, fall back to .preview
            const val = firstVar?.data ?? firstVar?.preview
            if (val !== undefined) {
              output.scalar = val
            }
          }
        }
      }

      // Carry stdout/stderr forward (cell_output overwrites cell.output)
      const stdout = p.stdout as string | undefined
      const stderr = p.stderr as string | undefined
      const consoleText = [stdout, stderr].filter(Boolean).join('')
      if (consoleText) {
        if (output.scalar && typeof output.scalar === 'object') {
          ;(output.scalar as Record<string, any>).console = consoleText
        } else if (!output.scalar) {
          output.scalar = { console: consoleText }
        }
      }

      // Preserve console text from earlier cell_console messages
      const existingCell = cellMap.value.get(cellId)
      if (existingCell?.output?.scalar && typeof existingCell.output.scalar === 'object') {
        const prev = (existingCell.output.scalar as Record<string, any>).console
        if (prev && !consoleText) {
          if (output.scalar && typeof output.scalar === 'object') {
            ;(output.scalar as Record<string, any>).console = prev
          } else if (!output.scalar) {
            output.scalar = { console: prev }
          }
        }
      }

      const cell = cellMap.value.get(cellId)
      if (cell) {
        cell.durationMs = p.duration_ms
        if (p.execution_method) {
          cell.executorName = p.execution_method
        }
        if (p.execution_method === 'executor') {
          updateWorkerHealth(effectiveWorkerNameForCell(cell), 'healthy')
        }
        // Capture suggest_install for "click to install" UX
        cell.suggestInstall = p.suggest_install || undefined
      }

      setCellOutput(cellId, output)
    })

    wsInstance.onMessage('cell_console', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      const cellId = p.cell_id as CellId
      const text = p.text as string
      const cell = cellMap.value.get(cellId)
      if (cell) {
        if (!cell.output) {
          cell.output = {
            contentType: 'json/object',
            scalar: { console: '' },
          }
        }
        if (
          cell.output.scalar &&
          typeof cell.output.scalar === 'object' &&
          'console' in cell.output.scalar
        ) {
          ;(cell.output.scalar as Record<string, any>).console += text
        } else {
          cell.output.scalar = { console: text }
        }
      }
    })

    wsInstance.onMessage('cell_error', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      const cellId = p.cell_id as CellId
      const error = p.error as string
      const cell = cellMap.value.get(cellId)
      if (cell) {
        cell.status = 'error'
        const workerName = effectiveWorkerNameForCell(cell)
        const workerEntry = availableWorkers.value.find((worker) => worker.name === workerName)
        if (workerEntry?.backend === 'executor' || error.includes('Remote executor')) {
          cell.executorName = 'executor'
          if (isRemoteExecutorLikelyUnreachable(error)) {
            updateWorkerHealth(workerName, 'unavailable')
          } else if (workerEntry?.backend === 'executor') {
            updateWorkerHealth(workerName, 'healthy')
          }
        }
        cell.output = { contentType: 'json/object', error }
        cell.suggestInstall = p.suggest_install || undefined
      }
    })

    wsInstance.onMessage('dag_update', (msg: WsMessage) => {
      const dagData = msg.payload as Record<string, any>
      if (dagData.edges) {
        applyBackendDag({
          edges: dagData.edges,
          leaves: dagData.leaves,
          roots: dagData.roots,
          topological_order: dagData.topological_order,
        })
      }
    })

    wsInstance.onMessage('cascade_prompt', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      const cellId = p.cell_id as CellId
      const planId = p.plan_id as string

      // Always auto-accept cascades. If the backend says upstream cells
      // need to run first, just do it. A confirmation dialog can be added
      // later for expensive re-runs; for now, eliminate friction.
      if (planId) {
        executeCascadeWebSocket(cellId, planId)
      }
    })

    wsInstance.onMessage('notebook_state', (msg: WsMessage) => {
      const state = msg.payload as Record<string, any>
      if ('worker' in state) {
        syncNotebookWorkerFromBackend(state.worker)
      }
      if ('workers' in state) {
        syncNotebookWorkersFromBackend(state.workers)
      }
      if ('timeout' in state) {
        syncNotebookTimeoutFromBackend(state.timeout)
      }
      if ('env' in state) {
        syncNotebookEnvFromBackend(state.env)
      }
      if ('mounts' in state) {
        syncNotebookMountsFromBackend(state.mounts)
      }
      if (state.cells && Array.isArray(state.cells)) {
        syncCellsFromBackend(state.cells)
      }
      if (state.dag) {
        applyBackendDag(state.dag)
      }
    })

    wsInstance.onMessage('impact_preview', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      currentImpactPreview.value = {
        targetCellId: p.target_cell_id,
        upstream: (p.upstream || []).map((s: any) => ({
          cellId: s.cell_id,
          cellName: s.cell_name,
          skip: s.skip,
          reason: s.reason,
          estimatedMs: s.estimated_ms,
        })),
        downstream: (p.downstream || []).map((d: any) => ({
          cellId: d.cell_id,
          cellName: d.cell_name,
          currentStatus: d.current_status,
          newStatus: d.new_status,
        })),
        estimatedMs: p.estimated_ms || 0,
      }
    })

    wsInstance.onMessage('profiling_summary', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      profilingSummary.value = {
        totalExecutionMs: p.total_execution_ms || 0,
        cacheHits: p.cache_hits || 0,
        cacheMisses: p.cache_misses || 0,
        cacheSavingsMs: p.cache_savings_ms || 0,
        totalArtifactBytes: p.total_artifact_bytes || 0,
        cellProfiles: (p.cell_profiles || []).map((cp: any) => ({
          cellId: cp.cell_id,
          cellName: cp.cell_name,
          status: cp.status,
          durationMs: cp.duration_ms,
          cacheHit: cp.cache_hit,
          artifactUri: cp.artifact_uri,
          executionCount: cp.execution_count,
        })),
      }
    })

    wsInstance.onMessage('inspect_result', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      const action = p.action as string

      if (action === 'open') {
        inspectCellId.value = p.cell_id
        inspectReady.value = p.ok === true
        inspectHistory.value = []
        if (!p.ok) {
          inspectHistory.value.push({
            expr: '(open)',
            error: p.error || p.result || 'Failed to open inspect session',
          })
        }
      } else if (action === 'eval') {
        inspectHistory.value.push({
          expr: p.expr || '',
          result: p.ok ? p.result : undefined,
          error: p.ok ? undefined : p.error,
          type: p.type,
          stdout: p.stdout,
        })
      } else if (action === 'close') {
        inspectCellId.value = null
        inspectReady.value = false
      }
    })

    wsInstance.onMessage('dependency_changed', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      if (p.dependencies && Array.isArray(p.dependencies)) {
        dependencies.value = p.dependencies.map((d: any) => ({
          name: d.name,
          version: d.version || '',
          specifier: d.specifier || '',
        }))
      }
      if (p.cells && Array.isArray(p.cells)) {
        syncCellsFromBackend(p.cells)
      }
      if (!p.success && p.error) {
        dependencyError.value = p.error
      } else {
        dependencyError.value = null
      }
      dependencyLoading.value = false
    })

    wsInstance.connect()
  }
}

async function waitForWebSocket(timeoutMs: number = 5000): Promise<void> {
  if (wsInstance && wsInstance.connected()) return
  if (wsInstance) {
    await wsInstance.waitForConnection(timeoutMs)
  }
}

function cleanupWebSocket() {
  if (wsInstance) {
    wsInstance.cleanup()
    wsInstance = null
  }
}

async function executeCellWebSocket(cellId: CellId) {
  if (!wsInstance) {
    console.warn('[notebook] No WebSocket instance, cannot execute cell:', cellId)
    return
  }
  if (!wsInstance.connected()) {
    try {
      await wsInstance.waitForConnection(5000)
    } catch {
      console.warn('[notebook] WebSocket not connected, cannot execute cell:', cellId)
      return
    }
  }
  setCellStatus(cellId, 'running')
  wsInstance.executeCell(cellId)
}

function executeCascadeWebSocket(cellId: CellId, planId: string) {
  if (wsInstance && wsInstance.connected()) {
    wsInstance.executeCascade(cellId, planId)
  }
}

function executeForceWebSocket(cellId: CellId) {
  if (wsInstance && wsInstance.connected()) {
    setCellStatus(cellId, 'running')
    wsInstance.executeForce(cellId)
  }
}

function cancelCellWebSocket(cellId: CellId) {
  if (wsInstance && wsInstance.connected()) {
    wsInstance.cancelCell(cellId)
  }
}

async function fetchDependencies() {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  try {
    const data = await strata.listDependencies(sid)
    dependencies.value = (data.dependencies || []).map((d: any) => ({
      name: d.name,
      version: d.version || '',
      specifier: d.specifier || '',
    }))
  } catch (err) {
    console.error('Failed to fetch dependencies:', err)
  }
}

async function fetchWorkers(forceRefresh = false) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  workerHealthLoading.value = true
  try {
    const data = await strata.listWorkers(sid, forceRefresh)
    if (data.workers && Array.isArray(data.workers)) {
      syncWorkerCatalogFromBackend(data.workers)
    }
    if ('definitions_editable' in data) {
      syncWorkerDefinitionsEditableFromBackend(data.definitions_editable)
    }
    syncWorkerHealthCheckedAtFromBackend(data.health_checked_at)
    if (data.definitions_editable === false) {
      await fetchServerWorkerRegistry(forceRefresh)
    } else {
      clearServerWorkerRegistryState()
    }
  } catch (err) {
    console.error('Failed to fetch workers:', err)
  } finally {
    workerHealthLoading.value = false
  }
}

async function fetchServerWorkerRegistry(forceRefresh = false) {
  const strata = useStrata()
  serverWorkerRegistryLoading.value = true
  try {
    const data = await strata.listAdminNotebookWorkers(forceRefresh)
    syncServerManagedWorkersFromBackend(data.configured_workers)
    serverWorkerRegistryAvailable.value = true
    serverWorkerRegistryError.value = null
  } catch (err) {
    serverManagedWorkers.value = []
    serverWorkerRegistryAvailable.value = false
    serverWorkerRegistryError.value = null
    console.debug('Admin notebook worker registry unavailable:', err)
  } finally {
    serverWorkerRegistryLoading.value = false
  }
}

function setServerWorkerActionLoading(workerName: string, loading: boolean) {
  if (loading) {
    serverWorkerActionLoading.value = {
      ...serverWorkerActionLoading.value,
      [workerName]: true,
    }
    return
  }

  const nextLoading = { ...serverWorkerActionLoading.value }
  delete nextLoading[workerName]
  serverWorkerActionLoading.value = nextLoading
}

function isServerWorkerActionLoading(workerName: string): boolean {
  return serverWorkerActionLoading.value[workerName] === true
}

async function addDependencyAction(pkg: string) {
  const sid = sessionId()
  if (!sid) return
  dependencyLoading.value = true
  dependencyError.value = null
  const strata = useStrata()
  try {
    const data = await strata.addDependency(sid, pkg)
    if (data.dependencies) {
      dependencies.value = data.dependencies.map((d: any) => ({
        name: d.name,
        version: d.version || '',
        specifier: d.specifier || '',
      }))
    }
    if (data.cells && Array.isArray(data.cells)) {
      syncCellsFromBackend(data.cells)
    }
  } catch (err: any) {
    dependencyError.value = err.message || 'Failed to add dependency'
  } finally {
    dependencyLoading.value = false
  }
}

async function removeDependencyAction(pkg: string) {
  const sid = sessionId()
  if (!sid) return
  dependencyLoading.value = true
  dependencyError.value = null
  const strata = useStrata()
  try {
    const data = await strata.removeDependency(sid, pkg)
    if (data.dependencies) {
      dependencies.value = data.dependencies.map((d: any) => ({
        name: d.name,
        version: d.version || '',
        specifier: d.specifier || '',
      }))
    }
    if (data.cells && Array.isArray(data.cells)) {
      syncCellsFromBackend(data.cells)
    }
  } catch (err: any) {
    dependencyError.value = err.message || 'Failed to remove dependency'
  } finally {
    dependencyLoading.value = false
  }
}

async function updateNotebookMountsAction(mounts: MountSpec[]) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  const data = await strata.updateNotebookMounts(sid, mounts)
  if (data.mounts) {
    syncNotebookMountsFromBackend(data.mounts)
  }
  if (data.cells && Array.isArray(data.cells)) {
    syncCellsFromBackend(data.cells)
  }
}

async function updateNotebookWorkerAction(worker: string | null) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  notebookWorkerError.value = null
  try {
    const data = await strata.updateNotebookWorker(sid, worker)
    if ('worker' in data) {
      syncNotebookWorkerFromBackend(data.worker)
    }
    if (data.cells && Array.isArray(data.cells)) {
      syncCellsFromBackend(data.cells)
    }
    if (data.workers && Array.isArray(data.workers)) {
      syncWorkerCatalogFromBackend(data.workers)
      syncWorkerHealthCheckedAtFromBackend(data.health_checked_at)
    } else {
      fetchWorkers()
    }
    if ('definitions_editable' in data) {
      syncWorkerDefinitionsEditableFromBackend(data.definitions_editable)
    }
  } catch (err: any) {
    notebookWorkerError.value = err.message || 'Failed to update notebook worker'
  }
}

async function updateNotebookWorkersAction(workers: EditableWorkerSpec[]) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  workerRegistryError.value = null
  const payload = workers
    .map((worker) => ({
      name: worker.name.trim(),
      backend: worker.backend,
      runtime_id: worker.runtimeId || null,
      config: worker.config || {},
    }))
    .filter((worker) => worker.name)
  try {
    const data = await strata.updateWorkers(sid, payload)
    if (data.configured_workers && Array.isArray(data.configured_workers)) {
      syncNotebookWorkersFromBackend(data.configured_workers)
    }
    if (data.workers && Array.isArray(data.workers)) {
      syncWorkerCatalogFromBackend(data.workers)
      syncWorkerHealthCheckedAtFromBackend(data.health_checked_at)
    }
    if ('definitions_editable' in data) {
      syncWorkerDefinitionsEditableFromBackend(data.definitions_editable)
    }
  } catch (err: any) {
    workerRegistryError.value = err.message || 'Failed to update worker registry'
  }
}

async function updateServerWorkerRegistryAction(workers: EditableWorkerSpec[]) {
  const strata = useStrata()
  serverWorkerRegistryError.value = null
  const payload = workers
    .map((worker) => ({
      name: worker.name.trim(),
      backend: worker.backend,
      runtime_id: worker.runtimeId || null,
      config: worker.config || {},
      enabled: worker.enabled !== false,
    }))
    .filter((worker) => worker.name)
  try {
    const data = await strata.updateAdminNotebookWorkers(payload)
    if (data.configured_workers && Array.isArray(data.configured_workers)) {
      syncServerManagedWorkersFromBackend(data.configured_workers)
    }
    serverWorkerRegistryAvailable.value = true
    syncWorkerHealthCheckedAtFromBackend(data.health_checked_at)
    await fetchWorkers(true)
  } catch (err: any) {
    serverWorkerRegistryError.value = err.message || 'Failed to update server worker registry'
  }
}

async function updateServerWorkerEnabledAction(workerName: string, enabled: boolean) {
  const strata = useStrata()
  serverWorkerRegistryError.value = null
  setServerWorkerActionLoading(workerName, true)
  try {
    const data = await strata.patchAdminNotebookWorker(workerName, enabled)
    if (data.configured_workers && Array.isArray(data.configured_workers)) {
      syncServerManagedWorkersFromBackend(data.configured_workers)
    }
    serverWorkerRegistryAvailable.value = true
    syncWorkerHealthCheckedAtFromBackend(data.health_checked_at)
    await fetchWorkers(true)
  } catch (err: any) {
    serverWorkerRegistryError.value = err.message || `Failed to update worker "${workerName}"`
  } finally {
    setServerWorkerActionLoading(workerName, false)
  }
}

async function refreshServerWorkerAction(workerName: string) {
  const strata = useStrata()
  serverWorkerRegistryError.value = null
  setServerWorkerActionLoading(workerName, true)
  try {
    const data = await strata.refreshAdminNotebookWorker(workerName)
    if (data.configured_workers && Array.isArray(data.configured_workers)) {
      syncServerManagedWorkersFromBackend(data.configured_workers)
    }
    serverWorkerRegistryAvailable.value = true
    syncWorkerHealthCheckedAtFromBackend(data.health_checked_at)
    await fetchWorkers(true)
  } catch (err: any) {
    serverWorkerRegistryError.value = err.message || `Failed to refresh worker "${workerName}"`
  } finally {
    setServerWorkerActionLoading(workerName, false)
  }
}

async function updateNotebookTimeoutAction(timeout: number | null) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  const data = await strata.updateNotebookTimeout(sid, timeout)
  if ('timeout' in data) {
    syncNotebookTimeoutFromBackend(data.timeout)
  }
  if (data.cells && Array.isArray(data.cells)) {
    syncCellsFromBackend(data.cells)
  }
}

async function updateNotebookEnvAction(env: Record<string, string>) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  const data = await strata.updateNotebookEnv(sid, env)
  if ('env' in data) {
    syncNotebookEnvFromBackend(data.env)
  }
  if (data.cells && Array.isArray(data.cells)) {
    syncCellsFromBackend(data.cells)
  }
}

async function updateCellWorkerAction(cellId: CellId, worker: string | null) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  const nextCellWorkerErrors = { ...cellWorkerErrors.value }
  delete nextCellWorkerErrors[cellId]
  cellWorkerErrors.value = nextCellWorkerErrors
  try {
    const data = await strata.updateCellWorker(sid, cellId, worker)
    if (data.cell) {
      const cell = cellMap.value.get(cellId)
      if (cell) {
        applyBackendCellState(cell, data.cell)
      }
    }
    if (data.cells && Array.isArray(data.cells)) {
      syncCellsFromBackend(data.cells)
    }
    if (data.workers && Array.isArray(data.workers)) {
      syncWorkerCatalogFromBackend(data.workers)
      syncWorkerHealthCheckedAtFromBackend(data.health_checked_at)
    } else {
      fetchWorkers()
    }
    if ('definitions_editable' in data) {
      syncWorkerDefinitionsEditableFromBackend(data.definitions_editable)
    }
  } catch (err: any) {
    cellWorkerErrors.value = {
      ...cellWorkerErrors.value,
      [cellId]: err.message || 'Failed to update cell worker',
    }
  }
}

function cellWorkerErrorForCell(cellId: CellId): string | null {
  return cellWorkerErrors.value[cellId] || null
}

async function updateCellTimeoutAction(cellId: CellId, timeout: number | null) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  const data = await strata.updateCellTimeout(sid, cellId, timeout)
  if (data.cell) {
    const cell = cellMap.value.get(cellId)
    if (cell) {
      applyBackendCellState(cell, data.cell)
    }
  }
  if (data.cells && Array.isArray(data.cells)) {
    syncCellsFromBackend(data.cells)
  }
}

async function updateCellEnvAction(cellId: CellId, env: Record<string, string>) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  const data = await strata.updateCellEnv(sid, cellId, env)
  if (data.cell) {
    const cell = cellMap.value.get(cellId)
    if (cell) {
      applyBackendCellState(cell, data.cell)
    }
  }
  if (data.cells && Array.isArray(data.cells)) {
    syncCellsFromBackend(data.cells)
  }
}

async function updateCellMountsAction(cellId: CellId, mounts: MountSpec[]) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  const data = await strata.updateCellMounts(sid, cellId, mounts)
  if (data.cell) {
    const cell = cellMap.value.get(cellId)
    if (cell) {
      applyBackendCellState(cell, data.cell)
    }
  }
  if (data.cells && Array.isArray(data.cells)) {
    syncCellsFromBackend(data.cells)
  }
}

function updateSourceWebSocket(cellId: CellId, source: string) {
  if (wsInstance && wsInstance.connected()) {
    wsInstance.updateCellSource(cellId, source)
  }
}

function requestImpactPreview(cellId: CellId) {
  if (wsInstance && wsInstance.connected()) {
    wsInstance.send('impact_preview_request', { cell_id: cellId })
  }
}

function clearImpactPreview() {
  currentImpactPreview.value = null
}

function requestProfilingSummary() {
  if (wsInstance && wsInstance.connected()) {
    wsInstance.send('profiling_request', {})
  }
}

function openInspect(cellId: CellId) {
  if (wsInstance && wsInstance.connected()) {
    wsInstance.inspectOpen(cellId)
  }
}

function evalInspect(expr: string) {
  if (wsInstance && wsInstance.connected() && inspectCellId.value) {
    wsInstance.inspectEval(inspectCellId.value, expr)
  }
}

function closeInspect() {
  if (wsInstance && wsInstance.connected() && inspectCellId.value) {
    wsInstance.inspectClose(inspectCellId.value)
  }
}

// --- Public API ------------------------------------------------------------

export function useNotebook() {
  return {
    notebook,
    orderedCells,
    dagEdges,
    cellMap,
    connected,
    connectError,
    // Lifecycle
    boot,
    openNotebook,
    // Cell operations (always backend-backed)
    addCell,
    removeCell,
    updateSource,
    setCellStatus,
    setCellOutput,
    moveCell,
    // WebSocket
    cleanupWebSocket,
    executeCellWebSocket,
    executeCascadeWebSocket,
    executeForceWebSocket,
    cancelCellWebSocket,
    updateSourceWebSocket,
    // v1.1: Impact Preview, Profiling
    currentImpactPreview,
    profilingSummary,
    requestImpactPreview,
    clearImpactPreview,
    requestProfilingSummary,
    // Inspect REPL
    inspectCellId,
    inspectReady,
    inspectHistory,
    openInspect,
    evalInspect,
    closeInspect,
    // Environment / Dependencies
    dependencies,
    dependencyLoading,
    dependencyError,
    availableWorkers,
    workerDefinitionsEditable,
    workerHealthLoading,
    workerHealthCheckedAt,
    notebookWorkerError,
    workerRegistryError,
    serverManagedWorkers,
    serverWorkerRegistryAvailable,
    serverWorkerRegistryLoading,
    isServerWorkerActionLoading,
    serverWorkerRegistryError,
    cellWorkerErrorForCell,
    fetchWorkers,
    fetchServerWorkerRegistry,
    fetchDependencies,
    addDependencyAction,
    removeDependencyAction,
    updateNotebookWorkersAction,
    updateServerWorkerRegistryAction,
    updateServerWorkerEnabledAction,
    refreshServerWorkerAction,
    updateNotebookWorkerAction,
    updateNotebookTimeoutAction,
    updateNotebookEnvAction,
    updateCellWorkerAction,
    updateCellTimeoutAction,
    updateCellEnvAction,
    updateNotebookMountsAction,
    updateCellMountsAction,
  }
}
