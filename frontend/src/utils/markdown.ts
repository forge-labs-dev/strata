function escapeHtml(text: string): string {
  return text
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;')
}

function sanitizeHref(href: string): string | null {
  const trimmed = href.trim()
  if (!trimmed) return null
  if (
    trimmed.startsWith('#') ||
    trimmed.startsWith('/') ||
    /^https?:/i.test(trimmed) ||
    /^mailto:/i.test(trimmed)
  ) {
    return trimmed
  }
  return null
}

function renderInlineMarkdown(raw: string): string {
  // Process inline code: escape HTML first, then replace backtick patterns
  const escaped = escapeHtml(raw)

  let html = escaped.replace(/`([^`]+)`/g, (_, code: string) => `<code>${code}</code>`)

  html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, label: string, href: string) => {
    const safeHref = sanitizeHref(href)
    if (!safeHref) return label
    return `<a href="${escapeHtml(safeHref)}" target="_blank" rel="noreferrer noopener">${label}</a>`
  })

  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
  html = html.replace(/__([^_]+)__/g, '<strong>$1</strong>')
  html = html.replace(/(?<!\*)\*([^*]+)\*(?!\*)/g, '<em>$1</em>')
  html = html.replace(/(?<!_)_([^_]+)_(?!_)/g, '<em>$1</em>')

  return html
}

function isTableDivider(line: string): boolean {
  const trimmed = line.trim()
  return /^\|?[\s:-]+\|[\s|:-]+\|?$/.test(trimmed)
}

function splitTableRow(line: string): string[] {
  const trimmed = line.trim().replace(/^\|/, '').replace(/\|$/, '')
  return trimmed.split('|').map((cell) => cell.trim())
}

function renderParagraph(lines: string[]): string {
  return `<p>${renderInlineMarkdown(lines.join('\n')).replaceAll('\n', '<br />')}</p>`
}

function renderList(lines: string[], ordered: boolean): string {
  const tag = ordered ? 'ol' : 'ul'
  const itemPattern = ordered ? /^\s*\d+\.\s+(.*)$/ : /^\s*[-*+]\s+(.*)$/
  const items = lines
    .map((line) => itemPattern.exec(line))
    .filter((match): match is RegExpExecArray => Boolean(match))
    .map((match) => `<li>${renderInlineMarkdown(match[1])}</li>`)
    .join('')
  return `<${tag}>${items}</${tag}>`
}

function renderTable(lines: string[]): string {
  const headerCells = splitTableRow(lines[0]).map(
    (cell) => `<th>${renderInlineMarkdown(cell)}</th>`,
  )
  const bodyRows = lines
    .slice(2)
    .map(
      (line) =>
        `<tr>${splitTableRow(line)
          .map((cell) => `<td>${renderInlineMarkdown(cell)}</td>`)
          .join('')}</tr>`,
    )
    .join('')

  return `<table><thead><tr>${headerCells.join('')}</tr></thead><tbody>${bodyRows}</tbody></table>`
}

export function renderMarkdownToHtml(markdown: string): string {
  const normalized = markdown.replace(/\r\n?/g, '\n').trim()
  if (!normalized) return ''

  const lines = normalized.split('\n')
  const blocks: string[] = []

  for (let i = 0; i < lines.length; ) {
    const line = lines[i]
    if (!line.trim()) {
      i += 1
      continue
    }

    const fence = /^```([A-Za-z0-9_-]+)?\s*$/.exec(line)
    if (fence) {
      const codeLines: string[] = []
      let j = i + 1
      while (j < lines.length && !/^```\s*$/.test(lines[j])) {
        codeLines.push(lines[j])
        j += 1
      }
      const language = fence[1] ? ` class="language-${escapeHtml(fence[1])}"` : ''
      blocks.push(`<pre><code${language}>${escapeHtml(codeLines.join('\n'))}</code></pre>`)
      i = j < lines.length ? j + 1 : j
      continue
    }

    const heading = /^(#{1,6})\s+(.*)$/.exec(line)
    if (heading) {
      const level = heading[1].length
      blocks.push(`<h${level}>${renderInlineMarkdown(heading[2])}</h${level}>`)
      i += 1
      continue
    }

    if (/^\s*([-*_])(?:\s*\1){2,}\s*$/.test(line)) {
      blocks.push('<hr />')
      i += 1
      continue
    }

    if (line.trim().includes('|') && i + 1 < lines.length && isTableDivider(lines[i + 1])) {
      const tableLines = [line, lines[i + 1]]
      let j = i + 2
      while (j < lines.length && lines[j].trim().includes('|') && lines[j].trim()) {
        tableLines.push(lines[j])
        j += 1
      }
      blocks.push(renderTable(tableLines))
      i = j
      continue
    }

    if (/^\s*>\s?/.test(line)) {
      const quoteLines: string[] = []
      let j = i
      while (j < lines.length && /^\s*>\s?/.test(lines[j])) {
        quoteLines.push(lines[j].replace(/^\s*>\s?/, ''))
        j += 1
      }
      blocks.push(`<blockquote>${renderMarkdownToHtml(quoteLines.join('\n'))}</blockquote>`)
      i = j
      continue
    }

    if (/^\s*[-*+]\s+/.test(line)) {
      const listLines: string[] = []
      let j = i
      while (j < lines.length && /^\s*[-*+]\s+/.test(lines[j])) {
        listLines.push(lines[j])
        j += 1
      }
      blocks.push(renderList(listLines, false))
      i = j
      continue
    }

    if (/^\s*\d+\.\s+/.test(line)) {
      const listLines: string[] = []
      let j = i
      while (j < lines.length && /^\s*\d+\.\s+/.test(lines[j])) {
        listLines.push(lines[j])
        j += 1
      }
      blocks.push(renderList(listLines, true))
      i = j
      continue
    }

    const paragraphLines = [line]
    let j = i + 1
    while (
      j < lines.length &&
      lines[j].trim() &&
      !/^(#{1,6})\s+/.test(lines[j]) &&
      !/^\s*```/.test(lines[j]) &&
      !/^\s*>\s?/.test(lines[j]) &&
      !/^\s*[-*+]\s+/.test(lines[j]) &&
      !/^\s*\d+\.\s+/.test(lines[j]) &&
      !(lines[j].trim().includes('|') && j + 1 < lines.length && isTableDivider(lines[j + 1])) &&
      !/^\s*([-*_])(?:\s*\1){2,}\s*$/.test(lines[j])
    ) {
      paragraphLines.push(lines[j])
      j += 1
    }
    blocks.push(renderParagraph(paragraphLines))
    i = j
  }

  return blocks.join('\n')
}
