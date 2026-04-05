type PrefetchedNotebookSession = Record<string, unknown> & {
  session_id?: string
}

let prefetchedNotebookSession: PrefetchedNotebookSession | null = null

export function primePrefetchedNotebookSession(data: unknown): void {
  if (!data || typeof data !== 'object') {
    prefetchedNotebookSession = null
    return
  }

  const sessionId = (data as PrefetchedNotebookSession).session_id
  prefetchedNotebookSession =
    typeof sessionId === 'string' && sessionId.trim()
      ? ((data as PrefetchedNotebookSession) ?? null)
      : null
}

export function consumePrefetchedNotebookSession(
  sessionId: string,
): PrefetchedNotebookSession | null {
  if (prefetchedNotebookSession?.session_id !== sessionId) {
    return null
  }

  const data = prefetchedNotebookSession
  prefetchedNotebookSession = null
  return data
}

export function clearPrefetchedNotebookSession(): void {
  prefetchedNotebookSession = null
}
