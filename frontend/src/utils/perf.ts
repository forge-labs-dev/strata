const NOTEBOOK_PERF_PREFIX = 'strata:notebook:'

function perf(): Performance | null {
  if (typeof globalThis === 'undefined' || !('performance' in globalThis)) {
    return null
  }
  return globalThis.performance
}

function entryName(name: string): string {
  return `${NOTEBOOK_PERF_PREFIX}${name}`
}

function shouldLogPerf(): boolean {
  return Boolean(import.meta.env?.DEV)
}

export function clearNotebookPerfMarks(...names: string[]): void {
  const performance = perf()
  if (!performance) return
  for (const name of names) {
    const fullName = entryName(name)
    performance.clearMarks(fullName)
    performance.clearMeasures(fullName)
  }
}

export function markNotebookPerf(name: string): void {
  const performance = perf()
  if (!performance) return
  performance.clearMarks(entryName(name))
  performance.mark(entryName(name))
}

export function measureNotebookPerf(name: string, start: string, end: string): number | null {
  const performance = perf()
  if (!performance) return null

  const measureName = entryName(name)
  try {
    performance.clearMeasures(measureName)
    performance.measure(measureName, entryName(start), entryName(end))
    const entries = performance.getEntriesByName(measureName, 'measure')
    const duration = entries.at(-1)?.duration ?? null
    if (duration != null && shouldLogPerf()) {
      console.debug(`[notebook perf] ${name}: ${duration.toFixed(1)}ms`)
    }
    return duration
  } catch {
    return null
  }
}
