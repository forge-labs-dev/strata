/**
 * Markdown → sanitized HTML rendering for cell outputs and markdown cells.
 *
 * We use ``markdown-it`` (CommonMark + tables + linkify + strikethrough)
 * and run the result through ``DOMPurify`` because both Markdown cell
 * sources and ``Markdown(...)`` outputs are user-controlled and we render
 * via ``v-html``. Sanitization is non-optional — without it a malicious
 * notebook could plant `<script>` tags into the rendered output.
 *
 * The public surface is a single function that takes raw markdown and
 * returns sanitized HTML; consumers stay decoupled from the renderer.
 */

import DOMPurify, { type Config as DOMPurifyConfig } from 'dompurify'
import MarkdownIt from 'markdown-it'

const md = new MarkdownIt({
  // Disallow inline HTML — sanitization would strip most of it anyway,
  // and turning it off keeps the output predictable for both cell
  // sources and dynamic ``Markdown(...)`` outputs.
  html: false,
  // Auto-detect URLs in plain text and turn them into links.
  linkify: true,
  // Smart quotes / em-dashes off — plays badly with code identifiers
  // that get pasted into prose ("don't" → "don’t" breaks copy-paste).
  typographer: false,
  // Convert single newlines inside a paragraph into <br>. Matches the
  // old hand-rolled renderer's behavior so existing markdown outputs
  // don't suddenly reflow.
  breaks: true,
})

// Make every link open in a new tab and drop referrer/opener for the
// classic ``target=_blank`` security pair. markdown-it's default
// ``link_open`` renderer doesn't add these.
const defaultLinkOpen =
  md.renderer.rules.link_open ||
  function (tokens, idx, options, _env, self) {
    return self.renderToken(tokens, idx, options)
  }

md.renderer.rules.link_open = (tokens, idx, options, env, self) => {
  const token = tokens[idx]
  const targetIdx = token.attrIndex('target')
  if (targetIdx < 0) {
    token.attrPush(['target', '_blank'])
  } else {
    token.attrs![targetIdx][1] = '_blank'
  }
  const relIdx = token.attrIndex('rel')
  if (relIdx < 0) {
    token.attrPush(['rel', 'noreferrer noopener'])
  } else {
    token.attrs![relIdx][1] = 'noreferrer noopener'
  }
  return defaultLinkOpen(tokens, idx, options, env, self)
}

// DOMPurify allowlist: keep target/rel on anchors (we just set them).
// Default DOMPurify already strips <script>, on* event handlers, and
// javascript: URLs, so we just need to permit our intentional additions.
const PURIFY_CONFIG: DOMPurifyConfig = {
  ADD_ATTR: ['target', 'rel'],
}

export function renderMarkdownToHtml(markdown: string): string {
  if (!markdown) return ''
  const rawHtml = md.render(markdown)
  return DOMPurify.sanitize(rawHtml, PURIFY_CONFIG) as unknown as string
}
