import { onMounted, onBeforeUnmount, ref, type Ref } from 'vue'
import { EditorState } from '@codemirror/state'
import {
  EditorView,
  keymap,
  lineNumbers,
  highlightActiveLine,
  highlightActiveLineGutter,
} from '@codemirror/view'
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { syntaxHighlighting, defaultHighlightStyle, bracketMatching } from '@codemirror/language'
import { closeBrackets } from '@codemirror/autocomplete'
import type { CellLanguage } from '../types/notebook'

export function useCodemirror(
  container: Ref<HTMLElement | null>,
  opts: {
    initialDoc?: string
    language?: CellLanguage
    onUpdate?: (doc: string) => void
    onRun?: () => void
  } = {},
) {
  const view = ref<EditorView | null>(null)

  onMounted(() => {
    if (!container.value) return

    const langExt = python()

    const runKeymap = keymap.of([
      {
        key: 'Shift-Enter',
        run: () => {
          opts.onRun?.()
          return true
        },
      },
    ])

    const updateListener = EditorView.updateListener.of((update) => {
      if (update.docChanged) {
        opts.onUpdate?.(update.state.doc.toString())
      }
    })

    const state = EditorState.create({
      doc: opts.initialDoc ?? '',
      extensions: [
        lineNumbers(),
        highlightActiveLine(),
        highlightActiveLineGutter(),
        history(),
        bracketMatching(),
        closeBrackets(),
        syntaxHighlighting(defaultHighlightStyle),
        langExt,
        oneDark,
        keymap.of([...defaultKeymap, ...historyKeymap]),
        runKeymap,
        updateListener,
        EditorView.theme({
          '&': { fontSize: '13px' },
          '.cm-content': {
            fontFamily: '"JetBrains Mono", "Fira Code", monospace',
            padding: '8px 0',
          },
          '.cm-gutters': { backgroundColor: '#1e1e2e', border: 'none' },
          '.cm-scroller': { overflow: 'auto' },
        }),
      ],
    })

    view.value = new EditorView({ state, parent: container.value })
  })

  onBeforeUnmount(() => {
    view.value?.destroy()
  })

  function setDoc(doc: string) {
    const v = view.value
    if (!v) return
    v.dispatch({
      changes: { from: 0, to: v.state.doc.length, insert: doc },
    })
  }

  return { view, setDoc }
}
