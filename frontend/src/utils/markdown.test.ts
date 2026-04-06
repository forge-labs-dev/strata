import assert from 'node:assert/strict'
import test from 'node:test'

import { renderMarkdownToHtml } from './markdown.ts'

test('renderMarkdownToHtml renders basic markdown blocks', () => {
  const html = renderMarkdownToHtml(`# Title

- one
- **two**

> quote`)

  assert.match(html, /<h1>Title<\/h1>/)
  assert.match(html, /<ul><li>one<\/li><li><strong>two<\/strong><\/li><\/ul>/)
  assert.match(html, /<blockquote><p>quote<\/p><\/blockquote>/)
})

test('renderMarkdownToHtml escapes raw html and blocks javascript links', () => {
  const html = renderMarkdownToHtml(
    `<script>alert(1)</script>

[safe](https://example.com)
[bad](javascript:alert(1))`,
  )

  assert.match(html, /&lt;script&gt;alert\(1\)&lt;\/script&gt;/)
  assert.doesNotMatch(html, /<script>/)
  assert.match(html, /href="https:\/\/example\.com"/)
  assert.doesNotMatch(html, /javascript:alert/)
})

test('renderMarkdownToHtml renders fenced code blocks and tables', () => {
  const html = renderMarkdownToHtml(
    [
      '```python',
      'print("hi")',
      '```',
      '',
      '| name | score |',
      '| ---- | ----- |',
      '| alice | 10 |',
    ].join('\n'),
  )

  assert.match(html, /<pre><code class="language-python">print\(&quot;hi&quot;\)<\/code><\/pre>/)
  assert.match(html, /<table>/)
  assert.match(html, /<th>name<\/th>/)
  assert.match(html, /<td>alice<\/td>/)
})
