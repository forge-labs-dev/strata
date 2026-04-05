import assert from 'node:assert/strict'
import test from 'node:test'

import { clearNotebookPerfMarks, markNotebookPerf, measureNotebookPerf } from './perf.ts'

test('measureNotebookPerf returns a duration for matching marks', () => {
  clearNotebookPerfMarks('test:start', 'test:end', 'test:measure')
  markNotebookPerf('test:start')
  markNotebookPerf('test:end')

  const duration = measureNotebookPerf('test:measure', 'test:start', 'test:end')

  assert.equal(typeof duration, 'number')
  assert.ok((duration ?? -1) >= 0)
})

test('measureNotebookPerf returns null when marks are missing', () => {
  clearNotebookPerfMarks('missing:start', 'missing:end', 'missing:measure')

  assert.equal(measureNotebookPerf('missing:measure', 'missing:start', 'missing:end'), null)
})
