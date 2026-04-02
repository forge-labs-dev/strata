import test from 'node:test'
import assert from 'node:assert/strict'
import {
  findRecentNotebookBySessionId,
  normalizeRecentNotebookEntries,
  recordRecentNotebookEntries,
} from './recentNotebooks.ts'

test('normalizeRecentNotebookEntries keeps valid entries and orders newest first', () => {
  const normalized = normalizeRecentNotebookEntries([
    { name: 'older', path: '/tmp/older', lastOpened: 10, sessionId: 's1' },
    { name: 'newer', path: '/tmp/newer', lastOpened: 20, sessionId: 's2' },
    { name: 'invalid', path: 123, lastOpened: 5 },
  ])

  assert.deepEqual(normalized, [
    { name: 'newer', path: '/tmp/newer', lastOpened: 20, sessionId: 's2' },
    { name: 'older', path: '/tmp/older', lastOpened: 10, sessionId: 's1' },
  ])
})

test('recordRecentNotebookEntries deduplicates by path and updates session id', () => {
  const updated = recordRecentNotebookEntries(
    [
      { name: 'Notebook', path: '/tmp/notebook', lastOpened: 10, sessionId: 'old-session' },
      { name: 'Other', path: '/tmp/other', lastOpened: 5, sessionId: 'other-session' },
    ],
    'Notebook',
    '/tmp/notebook',
    'new-session',
    99,
  )

  assert.deepEqual(updated, [
    { name: 'Notebook', path: '/tmp/notebook', lastOpened: 99, sessionId: 'new-session' },
    { name: 'Other', path: '/tmp/other', lastOpened: 5, sessionId: 'other-session' },
  ])
})

test('findRecentNotebookBySessionId returns the matching notebook path', () => {
  const match = findRecentNotebookBySessionId(
    [
      { name: 'Notebook', path: '/tmp/notebook', lastOpened: 10, sessionId: 'session-a' },
      { name: 'Other', path: '/tmp/other', lastOpened: 5, sessionId: 'session-b' },
    ],
    'session-b',
  )

  assert.deepEqual(match, {
    name: 'Other',
    path: '/tmp/other',
    lastOpened: 5,
    sessionId: 'session-b',
  })
})
