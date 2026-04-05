import assert from 'node:assert/strict'
import test from 'node:test'

import {
  clearPrefetchedNotebookSession,
  consumePrefetchedNotebookSession,
  primePrefetchedNotebookSession,
} from './notebookSessionPrefetch.ts'

test('consumePrefetchedNotebookSession returns matching prefetched session once', () => {
  clearPrefetchedNotebookSession()
  primePrefetchedNotebookSession({ session_id: 'session-a', name: 'Notebook A' })

  assert.deepEqual(consumePrefetchedNotebookSession('session-a'), {
    session_id: 'session-a',
    name: 'Notebook A',
  })
  assert.equal(consumePrefetchedNotebookSession('session-a'), null)
})

test('consumePrefetchedNotebookSession ignores non-matching session ids', () => {
  clearPrefetchedNotebookSession()
  primePrefetchedNotebookSession({ session_id: 'session-a', name: 'Notebook A' })

  assert.equal(consumePrefetchedNotebookSession('session-b'), null)
  assert.deepEqual(consumePrefetchedNotebookSession('session-a'), {
    session_id: 'session-a',
    name: 'Notebook A',
  })
})

test('primePrefetchedNotebookSession clears invalid payloads', () => {
  clearPrefetchedNotebookSession()
  primePrefetchedNotebookSession({ name: 'missing-session-id' })

  assert.equal(consumePrefetchedNotebookSession('session-a'), null)
})
