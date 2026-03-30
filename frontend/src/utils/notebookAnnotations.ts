import type { CellAnnotations, MountSpec } from '../types/notebook'

function isCommentOrBlank(line: string): boolean {
  const trimmed = line.trim()
  return !trimmed || trimmed.startsWith('#')
}

function isAnnotationLine(line: string): boolean {
  return /^#\s*@\w+/.test(line.trim())
}

function hasAnyAnnotations(annotations: CellAnnotations): boolean {
  return Boolean(
    annotations.worker ||
    annotations.timeout != null ||
    Object.keys(annotations.env).length ||
    annotations.mounts.length,
  )
}

function renderMountAnnotation(mount: MountSpec): string {
  return `# @mount ${mount.name} ${mount.uri}${mount.mode ? ` ${mount.mode}` : ''}`
}

export function renderAnnotationLines(annotations: CellAnnotations): string[] {
  const lines: string[] = []

  if (annotations.worker) {
    lines.push(`# @worker ${annotations.worker}`)
  }
  if (annotations.timeout != null) {
    lines.push(`# @timeout ${annotations.timeout}`)
  }
  for (const mount of annotations.mounts) {
    lines.push(renderMountAnnotation(mount))
  }
  for (const [key, value] of Object.entries(annotations.env)) {
    lines.push(`# @env ${key}=${value}`)
  }

  return lines
}

export function applySourceAnnotations(source: string, annotations: CellAnnotations): string {
  const lines = source.split('\n')
  let prefixEnd = 0
  while (prefixEnd < lines.length && isCommentOrBlank(lines[prefixEnd])) {
    prefixEnd += 1
  }

  const prefix = lines.slice(0, prefixEnd)
  const rest = lines.slice(prefixEnd)
  const preservedPrefix = prefix.filter((line) => !isAnnotationLine(line))
  const annotationLines = renderAnnotationLines(annotations)

  while (preservedPrefix.length > 0 && preservedPrefix[preservedPrefix.length - 1].trim() === '') {
    preservedPrefix.pop()
  }

  const nextPrefix = [...preservedPrefix]
  if (annotationLines.length > 0) {
    if (nextPrefix.length > 0 && nextPrefix[nextPrefix.length - 1].trim() !== '') {
      nextPrefix.push('')
    }
    nextPrefix.push(...annotationLines)
  }

  if (nextPrefix.length > 0 && rest.length > 0 && nextPrefix[nextPrefix.length - 1].trim() !== '') {
    nextPrefix.push('')
  }

  const nextLines = [...nextPrefix, ...rest]

  while (nextLines.length > 0 && nextLines[0] === '') {
    nextLines.shift()
  }

  if (!hasAnyAnnotations(annotations) && nextLines.length === 0) {
    return ''
  }

  return nextLines.join('\n')
}
