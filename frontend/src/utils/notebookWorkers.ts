import type { CellAnnotations, WorkerCatalogEntry, WorkerHealth } from '../types/notebook'

const LOCAL_WORKER_ENTRY: WorkerCatalogEntry = {
  name: 'local',
  backend: 'local',
  runtimeId: null,
  config: {},
  source: 'builtin',
  health: 'healthy',
  allowed: true,
  enabled: true,
  transport: 'local',
}

export function effectiveWorkerNameForCell(cell: {
  annotations?: CellAnnotations
  worker: string | null
}): string {
  return cell.annotations?.worker || cell.worker || 'local'
}

export function resolveEffectiveWorkerEntry(
  availableWorkers: WorkerCatalogEntry[],
  workerName: string | null | undefined,
): WorkerCatalogEntry | null {
  const effectiveName = workerName || 'local'
  if (effectiveName === 'local') {
    return { ...LOCAL_WORKER_ENTRY }
  }
  return availableWorkers.find((worker) => worker.name === effectiveName) ?? null
}

export function workerTransportLabel(
  worker: Pick<WorkerCatalogEntry, 'backend' | 'config' | 'transport'>,
): string {
  if (worker.transport) return worker.transport
  if (worker.backend === 'local') return 'local'
  const url = String(worker.config?.url || '')
  const transport = String(worker.config?.transport || 'direct')
    .trim()
    .toLowerCase()
  if (url.startsWith('embedded://')) return 'embedded'
  if (transport === 'signed' || transport === 'manifest' || transport === 'build') {
    return 'signed'
  }
  if (url.startsWith('http://') || url.startsWith('https://')) return 'direct'
  return 'executor'
}

export function workerWarningForEntry(
  entry: WorkerCatalogEntry | null,
  workerLabel: string,
): string | null {
  if (!entry) return `Worker "${workerLabel}" is unresolved`
  if (entry.enabled === false) return `Worker "${entry.name}" is disabled by server policy`
  if (entry.allowed === false) return `Worker "${entry.name}" is blocked by server policy`
  if (entry.backend === 'executor' && entry.health === 'unavailable') {
    return `Worker "${entry.name}" is currently unreachable`
  }
  return null
}

export function isRemoteExecutorLikelyUnreachable(error: string): boolean {
  const normalized = error.toLowerCase()
  return (
    normalized.includes('remote executor request failed') ||
    normalized.includes('connection refused') ||
    normalized.includes('all connection attempts failed') ||
    normalized.includes('name or service not known') ||
    normalized.includes('temporary failure in name resolution') ||
    normalized.includes('network is unreachable') ||
    normalized.includes('could not connect')
  )
}

export function summarizeRemoteExecutionIssue(
  error: string,
  entry: WorkerCatalogEntry | null,
  workerLabel: string,
  remoteErrorCode?: string | null,
  remoteBuildState?: string | null,
): string | null {
  if (!error || !entry || entry.backend !== 'executor') return null

  if (remoteErrorCode === 'TIMEOUT') {
    return `Remote execution timed out on "${entry.name}"`
  }
  if (remoteErrorCode === 'FINALIZE_FAILED') {
    return 'Remote execution finished, but output upload/finalize failed'
  }
  if (remoteErrorCode === 'INVALID_NOTEBOOK_BUNDLE') {
    return 'Remote execution returned an invalid output bundle'
  }
  if (remoteErrorCode === 'REQUEST_FAILED') {
    return `Could not reach remote worker "${entry.name}"`
  }
  if (remoteBuildState === 'failed') {
    return `Remote build failed on "${entry.name}"`
  }
  if (error.includes('blocked by server policy') || error.includes('not allowed in service mode')) {
    return `Worker "${entry.name}" is blocked by policy`
  }
  if (error.includes('Remote executor request failed')) {
    return `Could not reach remote worker "${entry.name}"`
  }
  if (error.includes('timed out')) {
    return `Remote execution timed out on "${entry.name}"`
  }
  if (error.includes('Failed to finalize notebook bundle build')) {
    return 'Remote execution finished, but output upload/finalize failed'
  }
  if (error.includes('Remote executor')) {
    return `Remote worker "${entry.name}" returned an execution error`
  }
  if (error.includes('Notebook build')) {
    return 'Remote execution did not complete successfully'
  }
  return `Remote execution failed on "${workerLabel}"`
}

export function applyWorkerHealth(
  availableWorkers: WorkerCatalogEntry[],
  workerName: string,
  health: WorkerHealth,
): WorkerCatalogEntry[] {
  if (!workerName || workerName === 'local') return availableWorkers
  return availableWorkers.map((worker) =>
    worker.name === workerName ? { ...worker, health } : worker,
  )
}
