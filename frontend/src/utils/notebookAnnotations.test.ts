import assert from 'node:assert/strict'
import test from 'node:test'

import type { CellAnnotations } from '../types/notebook.ts'
import { applySourceAnnotations, renderAnnotationLines } from './notebookAnnotations.ts'

function makeAnnotations(overrides: Partial<CellAnnotations> = {}): CellAnnotations {
  return {
    worker: null,
    timeout: null,
    env: {},
    mounts: [],
    ...overrides,
  }
}

test('renderAnnotationLines emits worker, timeout, mounts, and env entries', () => {
  const lines = renderAnnotationLines(
    makeAnnotations({
      worker: 'gpu-a100',
      timeout: 12,
      mounts: [{ name: 'raw_data', uri: 's3://bucket/raw', mode: 'ro', pin: null }],
      env: { TOKEN: 'secret' },
    }),
  )

  assert.deepEqual(lines, [
    '# @worker gpu-a100',
    '# @timeout 12',
    '# @mount raw_data s3://bucket/raw ro',
    '# @env TOKEN=secret',
  ])
})

test('applySourceAnnotations prepends a new annotation block to plain source', () => {
  const source = 'x = 1\ny = x + 1'

  const next = applySourceAnnotations(
    source,
    makeAnnotations({
      worker: 'gpu-http',
      timeout: 5,
    }),
  )

  assert.equal(next, '# @worker gpu-http\n# @timeout 5\n\nx = 1\ny = x + 1')
})

test('applySourceAnnotations replaces only annotation lines and preserves comments', () => {
  const source = [
    '# existing note',
    '# @worker old-worker',
    '# @env TOKEN=old',
    '',
    'x = 1',
  ].join('\n')

  const next = applySourceAnnotations(
    source,
    makeAnnotations({
      worker: 'gpu-new',
      env: { TOKEN: 'new', MODE: 'test' },
    }),
  )

  assert.equal(
    next,
    [
      '# existing note',
      '',
      '# @worker gpu-new',
      '# @env TOKEN=new',
      '# @env MODE=test',
      '',
      'x = 1',
    ].join('\n'),
  )
})

test('applySourceAnnotations removes annotation lines when overrides are cleared', () => {
  const source = ['# @worker gpu-http', '# @timeout 10', '', 'x = 1'].join('\n')

  const next = applySourceAnnotations(source, makeAnnotations())

  assert.equal(next, 'x = 1')
})
