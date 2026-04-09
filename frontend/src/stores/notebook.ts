import { reactive, computed, ref } from 'vue'
import type {
  Cell,
  CellId,
  CellOutput,
  CellStatus,
  DagEdge,
  DependencyInfo,
  EnvironmentImportPreview,
  Notebook,
  WsMessage,
  ImpactPreview,
  ProfilingSummary,
  MountSpec,
  CellAnnotations,
  EnvironmentActionSummary,
  EnvironmentOperation,
  EditableWorkerSpec,
  ManagedWorkerSpec,
  NotebookEnvironment,
  NotebookRuntimeConfig,
  WorkerCatalogEntry,
  WorkerHealth,
  WorkerHealthHistoryEntry,
  WorkerSpec,
} from '../types/notebook'
import { useStrata } from '../composables/useStrata'
import { useWebSocket } from '../composables/useWebSocket'
import { markNotebookPerf, measureNotebookPerf } from '../utils/perf'
import { consumePrefetchedNotebookSession } from '../utils/notebookSessionPrefetch'
import {
  applyWorkerHealth,
  effectiveWorkerNameForCell,
  isRemoteExecutorLikelyUnreachable,
} from '../utils/notebookWorkers'

let nextOrder = 0
const FALLBACK_NOTEBOOK_PARENT_PATH = '/tmp/strata-notebooks'

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
  environment: {
    pythonVersion: '',
    requestedPythonVersion: '',
    runtimePythonVersion: '',
    lockfileHash: '',
    packageCount: 0,
    declaredPackageCount: 0,
    resolvedPackageCount: 0,
    syncState: 'unknown',
    syncError: null,
    syncNotice: null,
    lastSyncedAt: null,
    lastSyncDurationMs: null,
    hasLockfile: false,
    venvPython: null,
    interpreterSource: 'unknown',
  },
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

function setCellOutput(id: CellId, output: CellOutput, displayOutputs?: CellOutput[]) {
  const cell = cellMap.value.get(id)
  if (cell) {
    cell.displayOutputs = displayOutputs ?? (output.error ? [] : [output])
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

function supportsStalenessDetail(status: CellStatus | undefined): boolean {
  return status === 'idle' || status === 'stale'
}

function parseMountSpec(raw: any): MountSpec {
  return {
    name: raw.name,
    uri: raw.uri,
    mode: raw.mode || 'ro',
    pin: raw.pin ?? null,
  }
}

function parseBackendEnvironment(raw: any): NotebookEnvironment {
  const declaredPackageCount = Number(
    raw?.declared_package_count ??
      raw?.declaredPackageCount ??
      raw?.package_count ??
      raw?.packageCount ??
      0,
  )
  const runtimePythonVersion = String(
    raw?.runtime_python_version ??
      raw?.runtimePythonVersion ??
      raw?.python_version ??
      raw?.pythonVersion ??
      '',
  )
  const requestedPythonVersion = String(
    raw?.requested_python_version ?? raw?.requestedPythonVersion ?? runtimePythonVersion,
  )
  return {
    pythonVersion: runtimePythonVersion,
    requestedPythonVersion,
    runtimePythonVersion,
    lockfileHash: String(raw?.lockfile_hash ?? raw?.lockfileHash ?? ''),
    packageCount: declaredPackageCount,
    declaredPackageCount,
    resolvedPackageCount: Number(raw?.resolved_package_count ?? raw?.resolvedPackageCount ?? 0),
    syncState: ['ready', 'fallback', 'failed', 'pending', 'unknown'].includes(raw?.sync_state)
      ? raw.sync_state
      : 'unknown',
    syncError: typeof raw?.sync_error === 'string' ? raw.sync_error : null,
    syncNotice: typeof raw?.sync_notice === 'string' ? raw.sync_notice : null,
    lastSyncedAt:
      typeof raw?.last_synced_at === 'number'
        ? raw.last_synced_at
        : typeof raw?.lastSyncedAt === 'number'
          ? raw.lastSyncedAt
          : null,
    lastSyncDurationMs:
      typeof raw?.last_sync_duration_ms === 'number'
        ? raw.last_sync_duration_ms
        : typeof raw?.lastSyncDurationMs === 'number'
          ? raw.lastSyncDurationMs
          : null,
    hasLockfile: raw?.has_lockfile === true || raw?.hasLockfile === true,
    venvPython:
      typeof raw?.venv_python === 'string'
        ? raw.venv_python
        : typeof raw?.venvPython === 'string'
          ? raw.venvPython
          : null,
    interpreterSource:
      raw?.interpreter_source === 'venv' || raw?.interpreterSource === 'venv'
        ? 'venv'
        : raw?.interpreter_source === 'path' || raw?.interpreterSource === 'path'
          ? 'path'
          : 'unknown',
  }
}

function parseDependencyInfo(raw: any): DependencyInfo {
  return {
    name: String(raw?.name || ''),
    version: String(raw?.version || ''),
    specifier: String(raw?.specifier || ''),
  }
}

function parseEnvironmentOperation(raw: any): EnvironmentOperation | null {
  if (!raw || typeof raw !== 'object') return null
  const action =
    raw.action === 'add' ||
    raw.action === 'remove' ||
    raw.action === 'sync' ||
    raw.action === 'import'
      ? raw.action
      : null
  if (!action) return null
  const status = raw.status === 'completed' || raw.status === 'failed' ? raw.status : 'running'
  return {
    id: typeof raw.id === 'string' ? raw.id : `${action}-${Date.now()}`,
    action,
    status,
    packageName: typeof raw.package === 'string' ? raw.package : null,
    phase: typeof raw.phase === 'string' ? raw.phase : null,
    command: typeof raw.command === 'string' ? raw.command : '',
    durationMs: typeof raw.duration_ms === 'number' ? raw.duration_ms : null,
    stdout: typeof raw.stdout === 'string' ? raw.stdout : '',
    stderr: typeof raw.stderr === 'string' ? raw.stderr : '',
    stdoutTruncated: raw.stdout_truncated === true,
    stderrTruncated: raw.stderr_truncated === true,
    startedAt: typeof raw.started_at === 'number' ? raw.started_at : Date.now(),
    finishedAt: typeof raw.finished_at === 'number' ? raw.finished_at : null,
    lockfileChanged: raw.lockfile_changed === true,
    staleCellCount: typeof raw.stale_cell_count === 'number' ? raw.stale_cell_count : 0,
    staleCellIds: Array.isArray(raw.stale_cell_ids)
      ? raw.stale_cell_ids
          .map((value: unknown) => String(value || '').trim())
          .filter((value: string) => value.length > 0)
      : [],
    error: typeof raw.error === 'string' ? raw.error : null,
  }
}

function syncEnvironmentOperationFromBackend(raw: any) {
  const parsed = parseEnvironmentOperation(raw)
  if (!parsed) {
    environmentOperation.value = null
    return
  }
  environmentOperation.value = parsed
  const isRunning = parsed.status === 'running'
  dependencyLoading.value = isRunning
  environmentLoading.value = isRunning
}

function syncEnvironmentJobHistoryFromBackend(raw: any) {
  environmentJobHistory.value = Array.isArray(raw)
    ? raw
        .map((entry: any) => parseEnvironmentOperation(entry))
        .filter(
          (entry: EnvironmentOperation | null): entry is EnvironmentOperation => entry !== null,
        )
    : []
}

function syncResolvedDependenciesFromBackend(raw: any) {
  resolvedDependencies.value = Array.isArray(raw)
    ? raw.map((dep: any) => parseDependencyInfo(dep)).filter((dep) => dep.name)
    : []
}

function syncEnvironmentPayloadFromBackend(data: any) {
  if (data?.environment) {
    syncNotebookEnvironmentFromBackend(data.environment)
  }
  if ('environment_job' in (data || {})) {
    syncEnvironmentOperationFromBackend(data.environment_job)
  }
  if ('environment_job_history' in (data || {})) {
    syncEnvironmentJobHistoryFromBackend(data.environment_job_history)
  }
  if (Array.isArray(data?.dependencies)) {
    dependencies.value = data.dependencies.map((dep: any) => parseDependencyInfo(dep))
  }
  if (Array.isArray(data?.resolved_dependencies)) {
    syncResolvedDependenciesFromBackend(data.resolved_dependencies)
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

function parseWorkerTransport(raw: unknown): WorkerCatalogEntry['transport'] | undefined {
  return raw === 'local' ||
    raw === 'embedded' ||
    raw === 'direct' ||
    raw === 'signed' ||
    raw === 'executor'
    ? raw
    : undefined
}

function parseWorkerCatalogEntry(raw: any): WorkerCatalogEntry {
  const transport = parseWorkerTransport(raw?.transport)
  const healthHistory: WorkerHealthHistoryEntry[] = Array.isArray(raw?.health_history)
    ? raw.health_history
        .map(
          (entry: Record<string, unknown>): WorkerHealthHistoryEntry => ({
            checkedAt:
              typeof entry?.checked_at === 'number' && Number.isFinite(entry.checked_at)
                ? entry.checked_at
                : 0,
            health:
              entry?.health === 'healthy' || entry?.health === 'unavailable'
                ? entry.health
                : 'unknown',
            error:
              typeof entry?.error === 'string' && entry.error.trim() ? String(entry.error) : null,
            durationMs:
              typeof entry?.duration_ms === 'number' && Number.isFinite(entry.duration_ms)
                ? entry.duration_ms
                : null,
          }),
        )
        .filter((entry: WorkerHealthHistoryEntry) => entry.checkedAt > 0)
    : []

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
    healthHistory,
    probeCount:
      typeof raw?.probe_count === 'number' && Number.isFinite(raw.probe_count)
        ? raw.probe_count
        : 0,
    healthyProbeCount:
      typeof raw?.healthy_probe_count === 'number' && Number.isFinite(raw.healthy_probe_count)
        ? raw.healthy_probe_count
        : 0,
    unavailableProbeCount:
      typeof raw?.unavailable_probe_count === 'number' &&
      Number.isFinite(raw.unavailable_probe_count)
        ? raw.unavailable_probe_count
        : 0,
    unknownProbeCount:
      typeof raw?.unknown_probe_count === 'number' && Number.isFinite(raw.unknown_probe_count)
        ? raw.unknown_probe_count
        : 0,
    consecutiveFailures:
      typeof raw?.consecutive_failures === 'number' && Number.isFinite(raw.consecutive_failures)
        ? raw.consecutive_failures
        : 0,
    lastHealthyAt:
      typeof raw?.last_healthy_at === 'number' && Number.isFinite(raw.last_healthy_at)
        ? raw.last_healthy_at
        : null,
    lastUnavailableAt:
      typeof raw?.last_unavailable_at === 'number' && Number.isFinite(raw.last_unavailable_at)
        ? raw.last_unavailable_at
        : null,
    lastUnknownAt:
      typeof raw?.last_unknown_at === 'number' && Number.isFinite(raw.last_unknown_at)
        ? raw.last_unknown_at
        : null,
    lastStatusChangeAt:
      typeof raw?.last_status_change_at === 'number' && Number.isFinite(raw.last_status_change_at)
        ? raw.last_status_change_at
        : null,
    lastProbeDurationMs:
      typeof raw?.last_probe_duration_ms === 'number' && Number.isFinite(raw.last_probe_duration_ms)
        ? raw.last_probe_duration_ms
        : null,
  }
}

function applyRemoteExecutionMetadata(cell: Cell, raw: Record<string, any>) {
  const remoteWorker =
    typeof raw.remote_worker === 'string' && raw.remote_worker.trim() ? raw.remote_worker : null
  const remoteTransport = parseWorkerTransport(raw.remote_transport) ?? null
  const remoteBuildId =
    typeof raw.remote_build_id === 'string' && raw.remote_build_id.trim()
      ? raw.remote_build_id
      : null
  const remoteBuildState =
    typeof raw.remote_build_state === 'string' && raw.remote_build_state.trim()
      ? raw.remote_build_state
      : null
  const remoteErrorCode =
    typeof raw.remote_error_code === 'string' && raw.remote_error_code.trim()
      ? raw.remote_error_code
      : null

  if (remoteWorker || remoteTransport || remoteBuildId || remoteBuildState || remoteErrorCode) {
    cell.remoteWorkerName = remoteWorker ?? undefined
    cell.remoteTransport = remoteTransport
    cell.remoteBuildId = remoteBuildId
    cell.remoteBuildState = remoteBuildState
    cell.remoteErrorCode = remoteErrorCode
    return
  }

  if (raw.execution_method !== 'cached') {
    cell.remoteWorkerName = undefined
    cell.remoteTransport = null
    cell.remoteBuildId = null
    cell.remoteBuildState = null
    cell.remoteErrorCode = null
  }
}

function applySerializedExecutionMetadata(cell: Cell, raw: Record<string, any>) {
  cell.executorName =
    typeof raw.execution_method === 'string' && raw.execution_method.trim()
      ? raw.execution_method
      : undefined
  cell.remoteWorkerName =
    typeof raw.remote_worker === 'string' && raw.remote_worker.trim()
      ? raw.remote_worker
      : undefined
  cell.remoteTransport = parseWorkerTransport(raw.remote_transport) ?? null
  cell.remoteBuildId =
    typeof raw.remote_build_id === 'string' && raw.remote_build_id.trim()
      ? raw.remote_build_id
      : null
  cell.remoteBuildState =
    typeof raw.remote_build_state === 'string' && raw.remote_build_state.trim()
      ? raw.remote_build_state
      : null
  cell.remoteErrorCode =
    typeof raw.remote_error_code === 'string' && raw.remote_error_code.trim()
      ? raw.remote_error_code
      : null
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

function parseDisplayOutputPayload(
  raw: any,
  fallbackArtifactUri: string | null = null,
): CellOutput | undefined {
  if (!raw || typeof raw !== 'object') return undefined

  const contentType = String(
    raw.content_type || raw.contentType || 'json/object',
  ) as CellOutput['contentType']
  const output: CellOutput = {
    contentType,
    artifactUri:
      typeof raw.artifact_uri === 'string' && raw.artifact_uri.trim()
        ? raw.artifact_uri
        : fallbackArtifactUri || undefined,
  }

  if (contentType === 'arrow/ipc') {
    const columns = Array.isArray(raw.columns)
      ? raw.columns.map((value: unknown) => String(value))
      : undefined
    const rowCount =
      typeof raw.rows === 'number'
        ? raw.rows
        : typeof raw.row_count === 'number'
          ? raw.row_count
          : typeof raw.rowCount === 'number'
            ? raw.rowCount
            : undefined
    output.columns = columns
    output.rowCount = rowCount

    const preview = raw.preview
    if (Array.isArray(preview) && columns) {
      output.rows = preview.map((row: unknown) => {
        const values = Array.isArray(row) ? row : []
        const obj: Record<string, unknown> = {}
        columns.forEach((col: string, i: number) => {
          obj[col] = values[i]
        })
        return obj
      })
    }
    return output
  }

  if (contentType === 'image/png') {
    output.inlineDataUrl =
      typeof raw.inline_data_url === 'string' && raw.inline_data_url.trim()
        ? raw.inline_data_url
        : null
    output.width = typeof raw.width === 'number' ? raw.width : null
    output.height = typeof raw.height === 'number' ? raw.height : null
    return output
  }

  if (contentType === 'text/markdown') {
    output.markdownText =
      typeof raw.markdown_text === 'string'
        ? raw.markdown_text
        : typeof raw.preview === 'string'
          ? raw.preview
          : null
    return output
  }

  const scalar = raw.data ?? raw.preview
  if (scalar !== undefined) {
    output.scalar = scalar
  }
  return output
}

function parseDisplayOutputPayloads(
  raw: any,
  fallbackArtifactUri: string | null = null,
): CellOutput[] {
  if (!Array.isArray(raw)) return []
  return raw
    .map((entry) => parseDisplayOutputPayload(entry, fallbackArtifactUri))
    .filter((entry): entry is CellOutput => Boolean(entry))
}

function applyDisplayOutputsToCell(
  cell: Cell,
  rawDisplays: any,
  rawDisplay: any,
  fallbackArtifactUri: string | null = null,
) {
  const displayOutputs = parseDisplayOutputPayloads(rawDisplays, fallbackArtifactUri)
  if (!displayOutputs.length) {
    const fallbackOutput = parseDisplayOutputPayload(rawDisplay, fallbackArtifactUri)
    if (fallbackOutput) {
      displayOutputs.push(fallbackOutput)
    }
  }
  cell.displayOutputs = displayOutputs
  cell.output = displayOutputs.at(-1)
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
  localCell.stalenessReasons = supportsStalenessDetail(localCell.status)
    ? serverCell.staleness_reasons || serverCell.staleness?.reasons || []
    : []
  localCell.causality = supportsStalenessDetail(localCell.status)
    ? parseBackendCausality(serverCell.causality)
    : undefined
  applyDisplayOutputsToCell(
    localCell,
    serverCell.display_outputs,
    serverCell.display_output,
    typeof serverCell.artifact_uri === 'string' ? serverCell.artifact_uri : null,
  )
  applySerializedExecutionMetadata(localCell, serverCell)
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
  workerCatalogLoaded.value = true
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

function resetWorkerCatalogState() {
  availableWorkers.value = []
  workerCatalogLoaded.value = false
  workerDefinitionsEditable.value = true
  workerHealthLoading.value = false
  workerHealthCheckedAt.value = null
  notebookWorkerError.value = null
  workerRegistryError.value = null
  cellWorkerErrors.value = {}
  clearServerWorkerRegistryState()
}

function syncNotebookTimeoutFromBackend(serverTimeout: any) {
  notebook.timeout = typeof serverTimeout === 'number' ? serverTimeout : null
}

function syncNotebookEnvFromBackend(serverEnv: any) {
  notebook.env = parseEnvMap(serverEnv)
}

function syncNotebookEnvironmentFromBackend(serverEnvironment: any) {
  notebook.environment = parseBackendEnvironment(serverEnvironment)
}

function parseBackendNotebookRuntimeConfig(raw: any): NotebookRuntimeConfig {
  const availablePythonVersions = Array.isArray(raw?.available_python_versions)
    ? raw.available_python_versions
        .map((value: unknown) => String(value || '').trim())
        .filter((value: string) => value.length > 0)
    : []
  const defaultPythonVersion =
    typeof raw?.default_python_version === 'string' && raw.default_python_version.trim()
      ? raw.default_python_version
      : availablePythonVersions[0] || ''
  return {
    deploymentMode: raw?.deployment_mode === 'service' ? 'service' : 'personal',
    defaultParentPath:
      typeof raw?.default_parent_path === 'string' && raw.default_parent_path.trim()
        ? raw.default_parent_path
        : FALLBACK_NOTEBOOK_PARENT_PATH,
    availablePythonVersions,
    defaultPythonVersion,
    pythonSelectionFixed:
      raw?.python_selection_fixed === true || availablePythonVersions.length <= 1,
  }
}

function setEnvironmentActionSummary(raw: {
  action: 'add' | 'remove' | 'sync' | 'import'
  packageName?: string | null
  lockfileChanged?: boolean
  staleCellCount?: number
}) {
  environmentLastAction.value = {
    action: raw.action,
    packageName: raw.packageName ?? null,
    lockfileChanged: raw.lockfileChanged === true,
    staleCellCount: raw.staleCellCount ?? 0,
    timestamp: Date.now(),
  }
}

function extractOperationLogPayload(raw: any): any | null {
  if (raw && typeof raw === 'object') {
    if (raw.operation_log && typeof raw.operation_log === 'object') {
      return raw.operation_log
    }
    if (raw.detail && typeof raw.detail === 'object') {
      const nested = (raw.detail as Record<string, unknown>).operation_log
      if (nested && typeof nested === 'object') {
        return nested
      }
    }
  }
  return null
}

function beginEnvironmentOperation(
  action: EnvironmentOperation['action'],
  command: string,
  packageName: string | null = null,
) {
  environmentOperation.value = {
    id: `${action}-${Date.now()}`,
    action,
    packageName,
    status: 'running',
    phase: 'starting',
    command,
    durationMs: null,
    stdout: '',
    stderr: '',
    stdoutTruncated: false,
    stderrTruncated: false,
    startedAt: Date.now(),
    finishedAt: null,
    lockfileChanged: false,
    staleCellCount: 0,
    staleCellIds: [],
    error: null,
  }
}

function finishEnvironmentOperation(
  action: EnvironmentOperation['action'],
  status: EnvironmentOperation['status'],
  raw: any,
  fallbackCommand: string,
) {
  const previous = environmentOperation.value
  environmentOperation.value = {
    id: previous?.id || `${action}-${Date.now()}`,
    action,
    packageName: previous?.packageName ?? null,
    status,
    phase: status === 'running' ? previous?.phase || 'running' : status,
    command: typeof raw?.command === 'string' && raw.command.trim() ? raw.command : fallbackCommand,
    durationMs:
      typeof raw?.duration_ms === 'number'
        ? raw.duration_ms
        : action === 'sync'
          ? notebook.environment.lastSyncDurationMs
          : null,
    stdout: typeof raw?.stdout === 'string' ? raw.stdout : '',
    stderr: typeof raw?.stderr === 'string' ? raw.stderr : '',
    stdoutTruncated: raw?.stdout_truncated === true,
    stderrTruncated: raw?.stderr_truncated === true,
    startedAt: previous?.action === action ? previous.startedAt : Date.now(),
    finishedAt: status === 'running' ? null : Date.now(),
    lockfileChanged: previous?.lockfileChanged === true,
    staleCellCount: previous?.staleCellCount ?? 0,
    staleCellIds: previous?.staleCellIds ?? [],
    error: status === 'failed' ? (typeof raw?.error === 'string' ? raw.error : null) : null,
  }
}

function parseBackendCellPayload(raw: any): Cell {
  const cell: Cell = {
    id: raw.id,
    source: raw.source || '',
    language: raw.language || 'python',
    order: raw.order ?? 0,
    status: (raw.status || 'idle') as CellStatus,
    worker: raw.worker ?? null,
    workerOverride: raw.worker_override ?? null,
    timeout: raw.timeout ?? null,
    timeoutOverride: raw.timeout_override ?? null,
    env: parseEnvMap(raw.env),
    envOverrides: parseEnvMap(raw.env_overrides),
    mounts: Array.isArray(raw.mounts) ? raw.mounts.map(parseMountSpec) : [],
    mountOverrides: Array.isArray(raw.mount_overrides)
      ? raw.mount_overrides.map(parseMountSpec)
      : [],
    annotations: parseBackendAnnotations(raw.annotations),
    upstreamIds: raw.upstream_ids || [],
    downstreamIds: raw.downstream_ids || [],
    defines: raw.defines || [],
    references: raw.references || [],
    inputs: [],
    isLeaf: raw.is_leaf || false,
    stalenessReasons: supportsStalenessDetail(raw.status)
      ? raw.staleness_reasons || raw.staleness?.reasons || []
      : [],
    causality: supportsStalenessDetail(raw.status)
      ? parseBackendCausality(raw.causality)
      : undefined,
    output: undefined,
    displayOutputs: [],
  }

  applyDisplayOutputsToCell(
    cell,
    raw.display_outputs,
    raw.display_output,
    typeof raw.artifact_uri === 'string' ? raw.artifact_uri : null,
  )

  applySerializedExecutionMetadata(cell, raw)
  return cell
}

function loadNotebookStateFromBackend(data: any) {
  notebook.id = data.id
  notebook.name = data.name
  notebook.worker = data.worker ?? null
  notebook.timeout = data.timeout ?? null
  notebook.env = parseEnvMap(data.env)
  notebook.workers = Array.isArray(data.workers) ? data.workers.map(parseWorkerSpec) : []
  notebook.mounts = Array.isArray(data.mounts) ? data.mounts.map(parseMountSpec) : []
  syncEnvironmentPayloadFromBackend(data)
  notebook.createdAt = data.created_at ? new Date(data.created_at).getTime() : Date.now()
  notebook.updatedAt = data.updated_at ? new Date(data.updated_at).getTime() : Date.now()
  ;(notebook as any).sessionId = data.session_id

  notebook.cells = (data.cells || []).map(parseBackendCellPayload)
  notebook.cells.sort((a, b) => a.order - b.order)
  if (data.dag) {
    applyBackendDag(data.dag)
  }
}

async function moveCell(id: CellId, direction: 'up' | 'down') {
  const sorted = orderedCells.value
  const idx = sorted.findIndex((c) => c.id === id)
  const swapIdx = direction === 'up' ? idx - 1 : idx + 1
  if (swapIdx < 0 || swapIdx >= sorted.length) return

  // Swap locally for instant feedback
  const tmp = sorted[idx].order
  sorted[idx].order = sorted[swapIdx].order
  sorted[swapIdx].order = tmp
  notebook.updatedAt = Date.now()

  // Persist to backend
  const sid = sessionId()
  if (!sid) return
  try {
    const newOrder = orderedCells.value.map((c) => c.id)
    await useStrata().reorderCells(sid, newOrder)
  } catch (err) {
    // Revert on failure
    const revertTmp = sorted[idx].order
    sorted[idx].order = sorted[swapIdx].order
    sorted[swapIdx].order = revertTmp
    console.error('Failed to reorder cells:', err)
  }
}

async function duplicateCell(id: CellId) {
  const cell = cellMap.value.get(id)
  if (!cell) return
  const source = cell.source
  await addCell(id)
  // Find the newly added cell (right after the original)
  const sorted = orderedCells.value
  const idx = sorted.findIndex((c) => c.id === id)
  const newCell = sorted[idx + 1]
  if (newCell && source) {
    await updateSource(newCell.id, source)
  }
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
 * Boot: create scratch notebook with one empty cell, connect WebSocket.
 * Called once on app mount. Resolves when ready to use.
 */
async function boot(): Promise<void> {
  const strata = useStrata()
  try {
    markNotebookPerf('boot_request_start')
    connectError.value = null
    dependencyError.value = null
    environmentError.value = null
    environmentWarnings.value = []
    environmentLastAction.value = null
    environmentOperation.value = null
    environmentJobHistory.value = []
    resetWorkerCatalogState()
    const runtimeConfig = parseBackendNotebookRuntimeConfig(await strata.getNotebookRuntimeConfig())

    // Create a scratch notebook with one starter cell
    const data = await strata.createNotebook(runtimeConfig.defaultParentPath, 'scratch', null, true)
    markNotebookPerf('boot_response')
    measureNotebookPerf('boot_request_ms', 'boot_request_start', 'boot_response')
    loadNotebookStateFromBackend(data)
    markNotebookPerf('boot_hydrated')
    measureNotebookPerf('boot_hydrate_ms', 'boot_response', 'boot_hydrated')

    // Connect WebSocket in the background; initial notebook render does not
    // need to wait for the live channel as long as execution stays disabled.
    initializeWebSocket()
    connected.value = false
    void waitForWebSocket()
      .then(() => {
        markNotebookPerf('boot_ws_ready')
        measureNotebookPerf('boot_ws_connect_ms', 'boot_hydrated', 'boot_ws_ready')
        measureNotebookPerf('boot_total_ms', 'boot_request_start', 'boot_ws_ready')
        connectError.value = null
        connected.value = true
      })
      .catch((e: any) => {
        connectError.value = e?.message || 'Failed to connect to server'
        connected.value = false
      })
  } catch (e: any) {
    console.error('Failed to boot notebook:', e)
    connectError.value = e.message || 'Failed to connect to server'
    connected.value = false
  }
}

/**
 * Open an existing notebook from disk.
 */
async function openNotebook(path: string): Promise<any> {
  const strata = useStrata()
  markNotebookPerf('store_open_request_start')
  // Cleanup existing WebSocket
  cleanupWebSocket()
  dependencyError.value = null
  environmentError.value = null
  environmentWarnings.value = []
  environmentLastAction.value = null
  environmentOperation.value = null
  environmentJobHistory.value = []
  resetWorkerCatalogState()

  const data = await strata.openNotebook(path)
  markNotebookPerf('store_open_response')
  measureNotebookPerf('store_open_request_ms', 'store_open_request_start', 'store_open_response')
  loadNotebookStateFromBackend(data)
  markNotebookPerf('store_open_hydrated')
  measureNotebookPerf('store_open_hydrate_ms', 'store_open_response', 'store_open_hydrated')

  initializeWebSocket()
  connected.value = false
  void waitForWebSocket()
    .then(() => {
      markNotebookPerf('store_open_ws_ready')
      measureNotebookPerf('store_open_ws_connect_ms', 'store_open_hydrated', 'store_open_ws_ready')
      measureNotebookPerf('store_open_total_ms', 'store_open_request_start', 'store_open_ws_ready')
      connectError.value = null
      connected.value = true
    })
    .catch((e: any) => {
      connectError.value = e?.message || 'Failed to connect to server'
      connected.value = false
    })

  return data
}

/**
 * Reconnect to an existing session by session ID.
 * Used when navigating to /notebook/:sessionId (e.g. page refresh).
 * Returns session data including path/name for recent-notebooks tracking.
 */
async function openBySessionId(sessionId: string): Promise<any> {
  const strata = useStrata()
  markNotebookPerf('store_session_request_start')
  cleanupWebSocket()
  dependencyError.value = null
  environmentError.value = null
  environmentWarnings.value = []
  environmentLastAction.value = null
  environmentOperation.value = null
  environmentJobHistory.value = []
  resetWorkerCatalogState()

  const data = consumePrefetchedNotebookSession(sessionId) ?? (await strata.getSession(sessionId))
  markNotebookPerf('store_session_response')
  measureNotebookPerf(
    'store_session_request_ms',
    'store_session_request_start',
    'store_session_response',
  )
  if (!data.session_id) {
    data.session_id = sessionId
  }
  loadNotebookStateFromBackend(data)
  markNotebookPerf('store_session_hydrated')
  measureNotebookPerf(
    'store_session_hydrate_ms',
    'store_session_response',
    'store_session_hydrated',
  )

  initializeWebSocket()
  connected.value = false
  void waitForWebSocket()
    .then(() => {
      markNotebookPerf('store_session_ws_ready')
      measureNotebookPerf(
        'store_session_ws_connect_ms',
        'store_session_hydrated',
        'store_session_ws_ready',
      )
      measureNotebookPerf(
        'store_session_total_ms',
        'store_session_request_start',
        'store_session_ws_ready',
      )
      connectError.value = null
      connected.value = true
    })
    .catch((e: any) => {
      connectError.value = e?.message || 'Failed to connect to server'
      connected.value = false
    })

  return data
}

async function deleteNotebookAction(): Promise<any> {
  const sid = sessionId()
  if (!sid) {
    throw new Error('Notebook is not open')
  }

  const strata = useStrata()
  const data = await strata.deleteNotebook(sid)

  cleanupWebSocket()
  connected.value = false
  connectError.value = null
  dependencyError.value = null
  environmentError.value = null
  environmentWarnings.value = []
  environmentLastAction.value = null
  environmentOperation.value = null
  environmentJobHistory.value = []
  resetWorkerCatalogState()
  ;(notebook as any).sessionId = undefined

  return data
}

async function updateNotebookNameAction(name: string): Promise<any> {
  const sid = sessionId()
  if (!sid) {
    throw new Error('Notebook is not open')
  }

  const strata = useStrata()
  const data = await strata.renameNotebook(sid, name)
  if (typeof data?.name === 'string' && data.name.trim()) {
    notebook.name = data.name
  }
  return data
}

// --- WebSocket integration -------------------------------------------------

// v1.1: Impact preview and profiling state
const currentImpactPreview = ref<ImpactPreview | null>(null)
const profilingSummary = ref<ProfilingSummary | null>(null)

// Environment / dependency state
const dependencies = ref<DependencyInfo[]>([])
const resolvedDependencies = ref<DependencyInfo[]>([])
const dependencyLoading = ref(false)
const dependencyError = ref<string | null>(null)
const environmentLoading = ref(false)
const environmentError = ref<string | null>(null)
const environmentWarnings = ref<string[]>([])
const environmentLastAction = ref<EnvironmentActionSummary | null>(null)
const environmentOperation = ref<EnvironmentOperation | null>(null)
const environmentJobHistory = ref<EnvironmentOperation[]>([])
const environmentImportPreview = ref<EnvironmentImportPreview | null>(null)
const environmentMutationActive = computed(() => environmentOperation.value?.status === 'running')
const availableWorkers = ref<WorkerCatalogEntry[]>([])
const workerCatalogLoaded = ref(false)
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

// LLM assistant state
interface LlmMessage {
  role: 'user' | 'assistant'
  content: string
  action?: 'generate' | 'explain' | 'describe' | 'chat'
  model?: string
  tokens?: { input: number; output: number }
  timestamp: number
}
const llmAvailable = ref(false)
const llmModel = ref<string | null>(null)
const llmProvider = ref<string | null>(null)
const llmLoading = ref(false)
const llmError = ref<string | null>(null)
const llmMessages = ref<LlmMessage[]>([])

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
      if (cell && status !== 'error' && cell.suggestInstall) {
        cell.suggestInstall = undefined
        if (cell.output?.error) {
          cell.displayOutputs = []
          cell.output = undefined
        }
      }

      if (cell && p.causality && supportsStalenessDetail(status)) {
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
      } else if (cell) {
        cell.causality = undefined
      }

      if (cell && supportsStalenessDetail(status) && p.staleness_reasons) {
        cell.stalenessReasons = p.staleness_reasons
      } else if (cell && !supportsStalenessDetail(status)) {
        cell.stalenessReasons = []
      }
    })

    wsInstance.onMessage('cell_output', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      const cellId = p.cell_id as CellId
      const outputs = p.outputs as Record<string, any> | undefined

      const displayOutputs =
        parseDisplayOutputPayloads(
          p.displays,
          typeof p.artifact_uri === 'string' ? p.artifact_uri : null,
        ) || []
      if (!displayOutputs.length) {
        const displayPayload =
          p.display ?? (outputs && typeof outputs === 'object' ? outputs['_'] : null)
        const fallbackOutput = parseDisplayOutputPayload(
          displayPayload,
          typeof p.artifact_uri === 'string' ? p.artifact_uri : null,
        )
        if (fallbackOutput) {
          displayOutputs.push(fallbackOutput)
        }
      }

      let output: CellOutput = displayOutputs.at(-1) ?? {
        contentType: 'json/object',
        artifactUri: p.artifact_uri,
      }
      output.cacheHit = p.cache_hit || false
      output.cacheLoadMs = p.duration_ms

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
        cell.displayOutputs = displayOutputs
        if (p.execution_method) {
          cell.executorName = p.execution_method
        }
        applyRemoteExecutionMetadata(cell, p)
        if (p.execution_method === 'executor') {
          updateWorkerHealth(effectiveWorkerNameForCell(cell), 'healthy')
        }
        // Capture suggest_install for "click to install" UX
        cell.suggestInstall = p.suggest_install || undefined
      }

      setCellOutput(cellId, output, displayOutputs)
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
          cell.displayOutputs = []
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
        cell.displayOutputs = []
        applyRemoteExecutionMetadata(cell, p)
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
      syncEnvironmentPayloadFromBackend(state)
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
      syncEnvironmentPayloadFromBackend(p)
      if (p.cells && Array.isArray(p.cells)) {
        syncCellsFromBackend(p.cells)
      }
      if (p.action === 'add' || p.action === 'remove') {
        setEnvironmentActionSummary({
          action: p.action,
          packageName: typeof p.package === 'string' ? p.package : null,
          lockfileChanged: p.lockfile_changed === true,
          staleCellCount: Number(p.stale_cell_count ?? 0),
        })
      }
      if (!p.success && p.error) {
        dependencyError.value = p.error
      } else {
        dependencyError.value = null
        environmentError.value = null
        environmentWarnings.value = []
      }
      dependencyLoading.value = false
    })

    function handleEnvironmentJobMessage(payload: Record<string, any>) {
      syncEnvironmentPayloadFromBackend(payload)
      if (payload.cells && Array.isArray(payload.cells)) {
        syncCellsFromBackend(payload.cells)
      }

      const job = parseEnvironmentOperation(payload.environment_job)
      if (!job) return

      if (job.status === 'running') {
        dependencyError.value = null
        environmentError.value = null
        environmentWarnings.value = []
        return
      }

      dependencyLoading.value = false
      environmentLoading.value = false

      if (job.status === 'completed') {
        setEnvironmentActionSummary({
          action: job.action,
          packageName: job.packageName,
          lockfileChanged: payload.lockfile_changed === true || job.lockfileChanged,
          staleCellCount:
            typeof payload.stale_cell_count === 'number'
              ? payload.stale_cell_count
              : job.staleCellCount,
        })
        dependencyError.value = null
        environmentError.value = null
        environmentWarnings.value = Array.isArray(payload.warnings)
          ? payload.warnings.filter(
              (warning: unknown): warning is string => typeof warning === 'string',
            )
          : []
        if (job.action === 'import') {
          environmentImportPreview.value = null
        }
      } else {
        const message = job.error || 'Environment update failed'
        environmentWarnings.value = Array.isArray(payload.warnings)
          ? payload.warnings.filter(
              (warning: unknown): warning is string => typeof warning === 'string',
            )
          : []
        if (job.action === 'add' || job.action === 'remove') {
          dependencyError.value = message
        } else {
          environmentError.value = message
        }
      }
    }

    wsInstance.onMessage('environment_job_started', (msg: WsMessage) => {
      handleEnvironmentJobMessage(msg.payload as Record<string, any>)
    })

    wsInstance.onMessage('environment_job_progress', (msg: WsMessage) => {
      handleEnvironmentJobMessage(msg.payload as Record<string, any>)
    })

    wsInstance.onMessage('environment_job_finished', (msg: WsMessage) => {
      handleEnvironmentJobMessage(msg.payload as Record<string, any>)
    })

    wsInstance.onMessage('error', (msg: WsMessage) => {
      const p = msg.payload as Record<string, any>
      if (p.code === 'ENVIRONMENT_BUSY' && typeof p.error === 'string') {
        environmentError.value = p.error
      }
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
  if (environmentMutationActive.value) {
    environmentError.value =
      environmentOperation.value?.error ||
      'Environment update in progress. Running cells is disabled until it finishes.'
    return
  }
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

async function executeNotebookRunAllWebSocket() {
  if (environmentMutationActive.value) {
    environmentError.value =
      environmentOperation.value?.error ||
      'Environment update in progress. Running cells is disabled until it finishes.'
    return
  }
  if (!wsInstance) {
    console.warn('[notebook] No WebSocket instance, cannot run all cells')
    return
  }
  if (!wsInstance.connected()) {
    try {
      await wsInstance.waitForConnection(5000)
    } catch {
      console.warn('[notebook] WebSocket not connected, cannot run all cells')
      return
    }
  }
  wsInstance.executeNotebookRunAll()
}

function executeCascadeWebSocket(cellId: CellId, planId: string) {
  if (wsInstance && wsInstance.connected()) {
    wsInstance.executeCascade(cellId, planId)
  }
}

function executeForceWebSocket(cellId: CellId) {
  if (environmentMutationActive.value) {
    environmentError.value =
      environmentOperation.value?.error ||
      'Environment update in progress. Running cells is disabled until it finishes.'
    return
  }
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
    syncEnvironmentPayloadFromBackend(data)
    dependencyError.value = null
  } catch (err) {
    console.error('Failed to fetch dependencies:', err)
    dependencyError.value = err instanceof Error ? err.message : 'Failed to fetch dependencies'
  }
}

async function fetchEnvironment() {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  environmentLoading.value = true
  try {
    const data = await strata.getEnvironmentStatus(sid)
    syncEnvironmentPayloadFromBackend(data)
    environmentError.value = null
    environmentWarnings.value = []
  } catch (err) {
    console.error('Failed to fetch environment:', err)
    environmentError.value = err instanceof Error ? err.message : 'Failed to load environment'
  } finally {
    environmentLoading.value = false
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

async function ensureWorkersLoaded(forceRefresh = false) {
  if (!forceRefresh && (workerCatalogLoaded.value || workerHealthLoading.value)) {
    return
  }
  await fetchWorkers(forceRefresh)
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
  environmentError.value = null
  environmentWarnings.value = []
  environmentImportPreview.value = null
  beginEnvironmentOperation('add', `uv add ${pkg}`, pkg)
  const strata = useStrata()
  try {
    const data = await strata.addDependency(sid, pkg)
    syncEnvironmentPayloadFromBackend(data)
    if (data.cells && Array.isArray(data.cells)) {
      syncCellsFromBackend(data.cells)
    }
  } catch (err: any) {
    dependencyError.value = err.message || 'Failed to add dependency'
    syncEnvironmentPayloadFromBackend(err?.payload)
    finishEnvironmentOperation(
      'add',
      'failed',
      extractOperationLogPayload(err?.payload),
      `uv add ${pkg}`,
    )
  }
}

async function removeDependencyAction(pkg: string) {
  const sid = sessionId()
  if (!sid) return
  dependencyLoading.value = true
  dependencyError.value = null
  environmentError.value = null
  environmentWarnings.value = []
  environmentImportPreview.value = null
  beginEnvironmentOperation('remove', `uv remove ${pkg}`, pkg)
  const strata = useStrata()
  try {
    const data = await strata.removeDependency(sid, pkg)
    syncEnvironmentPayloadFromBackend(data)
    if (data.cells && Array.isArray(data.cells)) {
      syncCellsFromBackend(data.cells)
    }
  } catch (err: any) {
    dependencyError.value = err.message || 'Failed to remove dependency'
    syncEnvironmentPayloadFromBackend(err?.payload)
    finishEnvironmentOperation(
      'remove',
      'failed',
      extractOperationLogPayload(err?.payload),
      `uv remove ${pkg}`,
    )
  }
}

async function syncEnvironmentAction() {
  const sid = sessionId()
  if (!sid) return
  environmentLoading.value = true
  environmentError.value = null
  environmentWarnings.value = []
  environmentImportPreview.value = null
  beginEnvironmentOperation('sync', 'uv sync')
  const strata = useStrata()
  try {
    const data = await strata.syncEnvironment(sid)
    syncEnvironmentPayloadFromBackend(data)
    if (data.cells && Array.isArray(data.cells)) {
      syncCellsFromBackend(data.cells)
    }
  } catch (err: any) {
    environmentError.value = err.message || 'Failed to sync environment'
    syncEnvironmentPayloadFromBackend(err?.payload)
    finishEnvironmentOperation(
      'sync',
      'failed',
      extractOperationLogPayload(err?.payload),
      'uv sync',
    )
  }
}

async function exportRequirementsAction(): Promise<string> {
  const sid = sessionId()
  if (!sid) return ''
  environmentLoading.value = true
  environmentError.value = null
  environmentWarnings.value = []
  const strata = useStrata()
  try {
    return await strata.exportRequirements(sid)
  } catch (err: any) {
    environmentError.value = err.message || 'Failed to export requirements.txt'
    return ''
  } finally {
    environmentLoading.value = false
  }
}

async function previewRequirementsImportAction(requirements: string) {
  const sid = sessionId()
  if (!sid) return
  environmentLoading.value = true
  environmentError.value = null
  environmentWarnings.value = []
  const strata = useStrata()
  try {
    const data = await strata.previewRequirementsImport(sid, requirements)
    syncEnvironmentPayloadFromBackend(data)
    environmentWarnings.value = Array.isArray(data.warnings)
      ? data.warnings.filter((warning: unknown): warning is string => typeof warning === 'string')
      : []
    environmentImportPreview.value = {
      kind: 'requirements',
      previewDependencies: Array.isArray(data.preview_dependencies)
        ? data.preview_dependencies.map((dep: any) => parseDependencyInfo(dep))
        : [],
      normalizedRequirements: Array.isArray(data.normalized_requirements)
        ? data.normalized_requirements.filter(
            (entry: unknown): entry is string => typeof entry === 'string',
          )
        : [],
      importedCount: Number(data.imported_count ?? 0),
      warnings: environmentWarnings.value,
      additions: Array.isArray(data.additions)
        ? data.additions.map((dep: any) => parseDependencyInfo(dep))
        : [],
      removals: Array.isArray(data.removals)
        ? data.removals.map((dep: any) => parseDependencyInfo(dep))
        : [],
      unchanged: Array.isArray(data.unchanged)
        ? data.unchanged.map((dep: any) => parseDependencyInfo(dep))
        : [],
    }
  } catch (err: any) {
    environmentImportPreview.value = null
    environmentError.value = err.message || 'Failed to preview requirements.txt import'
  } finally {
    environmentLoading.value = false
  }
}

async function importRequirementsAction(requirements: string) {
  const sid = sessionId()
  if (!sid) return
  environmentLoading.value = true
  environmentError.value = null
  environmentWarnings.value = []
  beginEnvironmentOperation('import', 'uv sync')
  const strata = useStrata()
  try {
    const data = await strata.importRequirements(sid, requirements)
    syncEnvironmentPayloadFromBackend(data)
    if (data.cells && Array.isArray(data.cells)) {
      syncCellsFromBackend(data.cells)
    }
  } catch (err: any) {
    environmentError.value = err.message || 'Failed to import requirements.txt'
    syncEnvironmentPayloadFromBackend(err?.payload)
    finishEnvironmentOperation(
      'import',
      'failed',
      extractOperationLogPayload(err?.payload),
      'uv sync',
    )
  }
}

async function importEnvironmentYamlAction(environmentYaml: string) {
  const sid = sessionId()
  if (!sid) return
  environmentLoading.value = true
  environmentError.value = null
  environmentWarnings.value = []
  beginEnvironmentOperation('import', 'uv sync')
  const strata = useStrata()
  try {
    const data = await strata.importEnvironmentYaml(sid, environmentYaml)
    syncEnvironmentPayloadFromBackend(data)
    if (data.cells && Array.isArray(data.cells)) {
      syncCellsFromBackend(data.cells)
    }
  } catch (err: any) {
    environmentError.value = err.message || 'Failed to import environment.yaml'
    syncEnvironmentPayloadFromBackend(err?.payload)
    finishEnvironmentOperation(
      'import',
      'failed',
      extractOperationLogPayload(err?.payload),
      'uv sync',
    )
  }
}

async function previewEnvironmentYamlImportAction(environmentYaml: string) {
  const sid = sessionId()
  if (!sid) return
  environmentLoading.value = true
  environmentError.value = null
  environmentWarnings.value = []
  const strata = useStrata()
  try {
    const data = await strata.previewEnvironmentYamlImport(sid, environmentYaml)
    syncEnvironmentPayloadFromBackend(data)
    environmentWarnings.value = Array.isArray(data.warnings)
      ? data.warnings.filter((warning: unknown): warning is string => typeof warning === 'string')
      : []
    environmentImportPreview.value = {
      kind: 'environment_yaml',
      previewDependencies: Array.isArray(data.preview_dependencies)
        ? data.preview_dependencies.map((dep: any) => parseDependencyInfo(dep))
        : [],
      normalizedRequirements: Array.isArray(data.normalized_requirements)
        ? data.normalized_requirements.filter(
            (entry: unknown): entry is string => typeof entry === 'string',
          )
        : [],
      importedCount: Number(data.imported_count ?? 0),
      warnings: environmentWarnings.value,
      additions: Array.isArray(data.additions)
        ? data.additions.map((dep: any) => parseDependencyInfo(dep))
        : [],
      removals: Array.isArray(data.removals)
        ? data.removals.map((dep: any) => parseDependencyInfo(dep))
        : [],
      unchanged: Array.isArray(data.unchanged)
        ? data.unchanged.map((dep: any) => parseDependencyInfo(dep))
        : [],
    }
  } catch (err: any) {
    environmentImportPreview.value = null
    environmentError.value = err.message || 'Failed to preview environment.yaml import'
  } finally {
    environmentLoading.value = false
  }
}

function clearEnvironmentImportPreview() {
  environmentImportPreview.value = null
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

function serializeEditableWorker(worker: EditableWorkerSpec) {
  return {
    name: worker.name.trim(),
    backend: worker.backend,
    runtime_id: worker.runtimeId || null,
    config: worker.config || {},
    enabled: worker.enabled !== false,
  }
}

async function saveServerWorkerAction(worker: EditableWorkerSpec, originalName: string | null) {
  const strata = useStrata()
  const payload = serializeEditableWorker(worker)
  if (!payload.name) {
    serverWorkerRegistryError.value = 'Worker name is required'
    return
  }

  serverWorkerRegistryError.value = null
  serverWorkerRegistryLoading.value = true
  try {
    const data = originalName
      ? await strata.replaceAdminNotebookWorker(originalName, payload)
      : await strata.createAdminNotebookWorker(payload)
    if (data.configured_workers && Array.isArray(data.configured_workers)) {
      syncServerManagedWorkersFromBackend(data.configured_workers)
    }
    serverWorkerRegistryAvailable.value = true
    syncWorkerHealthCheckedAtFromBackend(data.health_checked_at)
    await fetchWorkers(true)
  } catch (err: any) {
    serverWorkerRegistryError.value =
      err.message ||
      (originalName
        ? `Failed to update worker "${originalName}"`
        : `Failed to create worker "${payload.name}"`)
  } finally {
    serverWorkerRegistryLoading.value = false
  }
}

async function deleteServerWorkerAction(workerName: string) {
  const strata = useStrata()
  serverWorkerRegistryError.value = null
  serverWorkerRegistryLoading.value = true
  try {
    const data = await strata.deleteAdminNotebookWorker(workerName)
    if (data.configured_workers && Array.isArray(data.configured_workers)) {
      syncServerManagedWorkersFromBackend(data.configured_workers)
    }
    serverWorkerRegistryAvailable.value = true
    syncWorkerHealthCheckedAtFromBackend(data.health_checked_at)
    await fetchWorkers(true)
  } catch (err: any) {
    serverWorkerRegistryError.value = err.message || `Failed to delete worker "${workerName}"`
  } finally {
    serverWorkerRegistryLoading.value = false
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

// --- LLM assistant ---------------------------------------------------------

async function checkLlmStatus() {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()
  try {
    const status = await strata.getLlmStatus(sid)
    llmAvailable.value = status.available
    llmModel.value = status.model ?? null
    llmProvider.value = status.provider ?? null
  } catch {
    llmAvailable.value = false
  }
}

async function llmCompleteAction(
  action: 'generate' | 'explain' | 'describe' | 'chat',
  message: string,
  cellId?: string,
) {
  const sid = sessionId()
  if (!sid) return
  const strata = useStrata()

  llmMessages.value.push({
    role: 'user',
    content: message,
    action,
    timestamp: Date.now(),
  })

  llmLoading.value = true
  llmError.value = null
  try {
    const result = await strata.llmComplete(sid, action, message, cellId)
    llmMessages.value.push({
      role: 'assistant',
      content: result.content,
      model: result.model,
      tokens: result.tokens,
      timestamp: Date.now(),
    })
  } catch (err: any) {
    llmError.value = err.message || 'LLM request failed'
  } finally {
    llmLoading.value = false
  }
}

function clearLlmHistory() {
  llmMessages.value = []
  llmError.value = null
}

async function insertLlmCodeAsCell(code: string, afterCellId?: string) {
  await addCell(afterCellId)
  // Find the newly created cell (last one if no afterCellId, or after the target)
  const sorted = orderedCells.value
  let newCell: Cell | undefined
  if (afterCellId) {
    const idx = sorted.findIndex((c) => c.id === afterCellId)
    newCell = sorted[idx + 1]
  } else {
    newCell = sorted[sorted.length - 1]
  }
  if (newCell) {
    await updateSource(newCell.id, code)
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
    openBySessionId,
    updateNotebookNameAction,
    deleteNotebookAction,
    // Cell operations (always backend-backed)
    addCell,
    removeCell,
    updateSource,
    setCellStatus,
    setCellOutput,
    moveCell,
    duplicateCell,
    // WebSocket
    cleanupWebSocket,
    executeCellWebSocket,
    executeNotebookRunAllWebSocket,
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
    resolvedDependencies,
    environmentLoading,
    environmentError,
    environmentWarnings,
    environmentLastAction,
    environmentOperation,
    environmentJobHistory,
    environmentMutationActive,
    environmentImportPreview,
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
    ensureWorkersLoaded,
    fetchServerWorkerRegistry,
    fetchDependencies,
    fetchEnvironment,
    addDependencyAction,
    removeDependencyAction,
    syncEnvironmentAction,
    exportRequirementsAction,
    previewRequirementsImportAction,
    importRequirementsAction,
    previewEnvironmentYamlImportAction,
    importEnvironmentYamlAction,
    clearEnvironmentImportPreview,
    updateNotebookWorkersAction,
    updateServerWorkerRegistryAction,
    saveServerWorkerAction,
    deleteServerWorkerAction,
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
    // LLM assistant
    llmAvailable,
    llmModel,
    llmProvider,
    llmLoading,
    llmError,
    llmMessages,
    checkLlmStatus,
    llmCompleteAction,
    clearLlmHistory,
    insertLlmCodeAsCell,
  }
}
