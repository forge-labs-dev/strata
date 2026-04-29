<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import { useStrata } from '../composables/useStrata'
import { useNotebook } from '../stores/notebook'

const {
  notebook,
  connected,
  orderedCells,
  llmAvailable,
  llmModel,
  llmProvider,
  llmLoading,
  llmError,
  llmMessages,
  checkLlmStatus,
  llmChat,
  clearLlmHistory,
  insertLlmCodeAsCell,
  insertLlmCodeAsCells,
  agentRunning,
  agentProgress,
  agentError,
  agentApprovalPrompts,
  agentAutoApprove,
  runAgentAction,
  cancelAgent,
  respondToApproval,
  resetAgentMemory,
} = useNotebook()
const strata = useStrata()

const showPanel = ref(false)
const userMessage = ref('')
const selectedCellId = ref<string | null>(null)
const messagesEl = ref<HTMLDivElement | null>(null)

// Model selector
const showModelPicker = ref(false)
const availableModels = ref<string[]>([])
const modelsLoading = ref(false)

function sessionId(): string | null {
  return (notebook as any).sessionId ?? null
}

async function fetchModels() {
  const sid = sessionId()
  if (!sid) return
  modelsLoading.value = true
  try {
    const data = await strata.getLlmModels(sid)
    availableModels.value = data.models
  } catch {
    availableModels.value = []
  } finally {
    modelsLoading.value = false
  }
}

function toggleModelPicker() {
  showModelPicker.value = !showModelPicker.value
  if (showModelPicker.value) {
    void fetchModels()
  }
}

async function selectModel(model: string) {
  const sid = sessionId()
  if (!sid) return
  showModelPicker.value = false
  try {
    await strata.updateLlmModel(sid, model)
    void checkLlmStatus()
  } catch {
    // silently ignore
  }
}

// Check status every time panel opens (env vars may have changed)
watch(showPanel, (open) => {
  if (open) {
    void checkLlmStatus()
  }
})

// Auto-scroll on new messages
watch(
  () => llmMessages.value.length,
  async () => {
    await nextTick()
    if (messagesEl.value) {
      messagesEl.value.scrollTop = messagesEl.value.scrollHeight
    }
  },
)

async function chat() {
  const msg = userMessage.value.trim()
  if (!msg) return
  userMessage.value = ''
  await llmChat(msg, selectedCellId.value ?? undefined)
}

async function agent() {
  const msg = userMessage.value.trim()
  if (!msg) return
  userMessage.value = ''
  await runAgentAction(msg)
}

function extractCodeBlocks(content: string): string[] {
  const blocks: string[] = []
  const re = /```(?:python)?\n([\s\S]*?)```/g
  let m: RegExpExecArray | null
  while ((m = re.exec(content)) !== null) {
    const code = m[1].trim()
    if (code) blocks.push(code)
  }
  return blocks
}

async function handleClear() {
  // "Clear" wipes both the visible message thread and the backend's
  // persisted agent conversation memory. Chat-only conversations have
  // no backend state to reset, but we still call it — the backend
  // tolerates an empty history.
  clearLlmHistory()
  await resetAgentMemory()
}

function cellOptionLabel(cell: any, idx: number): string {
  const hint =
    cell.annotations?.name?.trim() || (cell.defines?.length ? cell.defines.join(', ') : '')
  return hint ? `Cell ${idx + 1} — ${hint}` : `Cell ${idx + 1}`
}

function handleInsert(code: string) {
  const lastCell = orderedCells.value.at(-1)
  void insertLlmCodeAsCell(code, lastCell?.id)
}

function handleInsertAll(codes: string[]) {
  const lastCell = orderedCells.value.at(-1)
  void insertLlmCodeAsCells(codes, lastCell?.id)
}

const isBusy = computed(() => llmLoading.value || agentRunning.value)

// Show "Thinking..." only when a stream is in flight but no delta has
// arrived yet. Once deltas start landing, the message content is its own
// indicator that progress is happening.
const showThinking = computed(() => {
  if (!llmLoading.value) return false
  const last = llmMessages.value.at(-1)
  return !last || last.role !== 'assistant' || !last.content
})

const totalTokens = computed(() => {
  let input = 0
  let output = 0
  for (const msg of llmMessages.value) {
    if (msg.tokens) {
      input += msg.tokens.input
      output += msg.tokens.output
    }
  }
  return { input, output, total: input + output }
})
</script>

<template>
  <div class="llm-panel">
    <button class="panel-toggle" @click="showPanel = !showPanel">
      <span class="toggle-label">
        AI Assistant
        <span
          v-if="llmModel"
          class="model-badge model-badge-clickable"
          title="Click to change model"
          @click.stop="toggleModelPicker"
          >{{ llmModel }}</span
        >
      </span>
      <span class="toggle-icon">{{ showPanel ? '\u25B2' : '\u25BC' }}</span>
    </button>

    <div v-if="showPanel" class="panel-content">
      <!-- Not configured -->
      <div v-if="!llmAvailable" class="llm-unconfigured">
        <p>No LLM provider configured for this notebook.</p>
        <p class="llm-hint">
          Set <code>ANTHROPIC_API_KEY</code>, <code>OPENAI_API_KEY</code>, or
          <code>STRATA_AI_API_KEY</code> in the Runtime panel.
        </p>
      </div>

      <template v-else>
        <!-- Model selector -->
        <div class="model-selector">
          <span class="model-selector-label">Model:</span>
          <button class="model-selector-btn" @click="toggleModelPicker">
            {{ llmModel || 'Select model' }}
            <span class="model-chevron">{{ showModelPicker ? '\u25B2' : '\u25BC' }}</span>
          </button>
        </div>
        <div v-if="showModelPicker" class="model-picker">
          <div v-if="modelsLoading" class="model-picker-loading">Loading models...</div>
          <template v-else-if="availableModels.length > 0">
            <button
              v-for="m in availableModels"
              :key="m"
              class="model-picker-item"
              :class="{ active: m === llmModel }"
              @click="selectModel(m)"
            >
              {{ m }}
            </button>
          </template>
          <div v-else class="model-picker-empty">No models found from provider</div>
        </div>
        <!-- Messages -->
        <div ref="messagesEl" class="llm-messages">
          <div v-if="llmMessages.length === 0" class="llm-empty">
            Ask me to generate code, explain errors, or describe cells.
          </div>
          <div v-for="(msg, idx) in llmMessages" :key="idx" class="llm-msg" :class="msg.role">
            <div class="msg-header">
              <span class="msg-role">{{ msg.role === 'user' ? 'You' : 'AI' }}</span>
              <span v-if="msg.tokens" class="msg-tokens">
                {{ msg.tokens.input + msg.tokens.output }} tok
              </span>
            </div>
            <pre class="msg-content">{{ msg.content }}</pre>
            <div
              v-if="
                msg.role === 'assistant' &&
                msg.source === 'agent' &&
                extractCodeBlocks(msg.content).length > 0
              "
              class="msg-actions"
            >
              <button
                v-if="extractCodeBlocks(msg.content).length >= 2"
                class="insert-btn insert-all"
                @click="handleInsertAll(extractCodeBlocks(msg.content))"
              >
                Insert All as Cells ({{ extractCodeBlocks(msg.content).length }})
              </button>
              <button
                v-for="(code, ci) in extractCodeBlocks(msg.content)"
                :key="ci"
                class="insert-btn"
                @click="handleInsert(code)"
              >
                Insert Cell {{ extractCodeBlocks(msg.content).length >= 2 ? ci + 1 : '' }}
              </button>
            </div>
          </div>
          <div v-if="showThinking" class="llm-loading">Thinking...</div>
        </div>

        <!-- Error -->
        <div v-if="llmError" class="llm-error">{{ llmError }}</div>

        <!-- Approval prompts (shown only when manual approval is on) -->
        <div v-if="agentApprovalPrompts.length > 0" class="approval-stack">
          <div
            v-for="prompt in agentApprovalPrompts"
            :key="prompt.request_id"
            class="approval-card"
          >
            <div class="approval-summary">
              <span class="approval-tool">{{ prompt.tool }}</span>
              <span class="approval-detail">{{ prompt.summary }}</span>
            </div>
            <div class="approval-actions">
              <button class="approve-btn" @click="respondToApproval(prompt.request_id, true)">
                Approve
              </button>
              <button class="decline-btn" @click="respondToApproval(prompt.request_id, false)">
                Decline
              </button>
            </div>
          </div>
        </div>

        <!-- Agent progress -->
        <div v-if="agentRunning || agentProgress.length > 0" class="agent-progress">
          <div class="agent-progress-header">
            <span class="agent-label">Agent</span>
            <button v-if="agentRunning" class="agent-cancel-btn" @click="cancelAgent">
              Cancel
            </button>
            <span v-else-if="agentError" class="agent-status error">failed</span>
            <span v-else class="agent-status done">done</span>
          </div>
          <div class="agent-log">
            <div
              v-for="(ev, idx) in agentProgress"
              :key="idx"
              class="agent-event"
              :class="ev.event"
            >
              <span class="event-type">{{ ev.event }}</span>
              <span class="event-detail">{{ ev.detail }}</span>
            </div>
            <div v-if="agentRunning" class="agent-event thinking">
              <span class="event-type">working</span>
              <span class="event-detail">...</span>
            </div>
          </div>
          <div v-if="agentError" class="agent-error">{{ agentError }}</div>
        </div>

        <!-- Cell context selector -->
        <select v-model="selectedCellId" class="llm-cell-select">
          <option :value="null">Entire notebook</option>
          <option v-for="(cell, i) in orderedCells" :key="cell.id" :value="cell.id">
            {{ cellOptionLabel(cell, i) }}
          </option>
        </select>

        <!-- Input with two actions -->
        <div class="llm-input">
          <textarea
            v-model="userMessage"
            placeholder="Ask a question or describe what to build..."
            :disabled="isBusy || !connected"
            rows="2"
            @keydown.enter.exact.prevent="chat"
            @keydown.shift.enter.exact.prevent="agent"
          ></textarea>
          <div class="input-actions">
            <button
              class="chat-btn"
              title="Ask a question (Enter)"
              :disabled="!userMessage.trim() || isBusy || !connected"
              @click="chat"
            >
              Chat
            </button>
            <button
              class="agent-btn"
              title="Take action: create cells, run code, install packages (Shift+Enter)"
              :disabled="!userMessage.trim() || isBusy || !connected"
              @click="agent"
            >
              Agent
            </button>
          </div>
        </div>

        <!-- Footer -->
        <div class="llm-footer">
          <label
            class="auto-approve-toggle"
            title="Skip the approval prompt for delete_cell and add_package"
          >
            <input v-model="agentAutoApprove" type="checkbox" />
            <span>Auto-approve</span>
          </label>
          <span v-if="totalTokens.total > 0" class="token-count">
            {{ totalTokens.total.toLocaleString() }} tokens
          </span>
          <span v-if="llmProvider" class="provider-label">{{ llmProvider }}</span>
          <button
            class="clear-btn"
            title="Reset agent memory and clear messages"
            :disabled="llmMessages.length === 0"
            @click="handleClear"
          >
            Clear
          </button>
        </div>
      </template>
    </div>
  </div>
</template>

<style scoped>
/* Matches the section-header pattern used by RuntimePanel / MountsPanel
 * / WorkersPanel / EnvironmentPanel so the right sidebar reads as one
 * consistent stack of collapsible sections instead of a mix of card
 * buttons. */
.llm-panel {
  margin-top: 12px;
  border-top: 1px solid var(--border-subtle);
  padding-top: 8px;
}

.panel-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  width: 100%;
  background: none;
  border: none;
  color: var(--text-secondary);
  font-size: 12px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  cursor: pointer;
  padding: 4px 0;
}

.panel-toggle:hover {
  color: var(--text-primary);
}

.toggle-label {
  display: flex;
  align-items: center;
  gap: 8px;
}

.toggle-icon {
  font-size: 10px;
  color: var(--text-muted);
}

.model-badge {
  font-size: 10px;
  font-weight: 400;
  color: var(--text-secondary);
  background: var(--bg-input);
  padding: 1px 6px;
  border-radius: 4px;
}

.model-badge-clickable {
  cursor: pointer;
}

.model-badge-clickable:hover {
  background: var(--border-strong);
  color: var(--text-primary);
}

.model-selector {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 8px;
  border-bottom: 1px solid var(--border-subtle);
}

.model-selector-label {
  font-size: 11px;
  color: var(--text-muted);
  flex-shrink: 0;
}

.model-selector-btn {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 4px 8px;
  background: var(--bg-input);
  border: 1px solid var(--border-strong);
  border-radius: 4px;
  color: var(--text-primary);
  font-size: 11px;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  cursor: pointer;
}

.model-selector-btn:hover {
  border-color: var(--accent-primary);
}

.model-chevron {
  font-size: 8px;
  color: var(--text-muted);
}

.model-picker {
  border: 1px solid var(--bg-input);
  border-radius: 6px;
  background: var(--bg-surface);
  margin-top: 4px;
  max-height: 250px;
  overflow-y: auto;
}

.model-picker-item {
  display: block;
  width: 100%;
  text-align: left;
  padding: 6px 12px;
  background: none;
  border: none;
  color: var(--text-secondary);
  font-size: 11px;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  cursor: pointer;
}

.model-picker-item:hover {
  background: var(--bg-input);
  color: var(--text-primary);
}

.model-picker-item.active {
  color: var(--accent-primary);
  font-weight: 600;
}

.model-picker-loading,
.model-picker-empty {
  padding: 8px 12px;
  color: var(--text-muted);
  font-size: 11px;
}

.panel-content {
  margin-top: 8px;
  border: 1px solid var(--bg-input);
  border-radius: 8px;
  background: var(--bg-base);
  overflow: hidden;
}

.llm-unconfigured {
  padding: 16px;
  text-align: center;
  color: var(--text-muted);
  font-size: 13px;
}

.llm-unconfigured code {
  background: var(--bg-input);
  padding: 1px 4px;
  border-radius: 3px;
  font-size: 11px;
}

.llm-hint {
  margin-top: 8px;
  font-size: 11px;
}

.llm-messages {
  max-height: 300px;
  overflow-y: auto;
  padding: 8px;
}

.llm-empty {
  padding: 16px;
  text-align: center;
  color: var(--text-muted);
  font-size: 12px;
}

.llm-msg {
  margin-bottom: 8px;
  padding: 8px;
  border-radius: 6px;
}

.llm-msg.user {
  background: var(--bg-elevated);
}

.llm-msg.assistant {
  background: var(--bg-surface);
  border: 1px solid var(--border-subtle);
}

.msg-header {
  display: flex;
  justify-content: space-between;
  margin-bottom: 4px;
}

.msg-role {
  font-size: 11px;
  font-weight: 600;
  color: var(--accent-primary);
}

.llm-msg.user .msg-role {
  color: var(--text-secondary);
}

.msg-tokens {
  font-size: 10px;
  color: var(--text-muted);
}

.msg-content {
  font-size: 12px;
  line-height: 1.5;
  color: var(--text-primary);
  white-space: pre-wrap;
  word-break: break-word;
  margin: 0;
  font-family: inherit;
}

.msg-actions {
  margin-top: 6px;
  display: flex;
  gap: 6px;
}

.insert-btn {
  font-size: 11px;
  padding: 3px 8px;
  background: var(--accent-primary);
  color: var(--bg-elevated);
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-weight: 600;
}

.insert-btn:hover {
  background: var(--accent-primary-hover);
}

.insert-btn.insert-all {
  background: var(--accent-success);
  color: var(--bg-elevated);
}

.insert-btn.insert-all:hover {
  background: var(--accent-teal);
}

.llm-loading {
  padding: 8px;
  color: var(--text-secondary);
  font-size: 12px;
  font-style: italic;
}

.llm-error {
  padding: 8px;
  color: var(--accent-danger);
  font-size: 12px;
  background: var(--tint-danger);
  border-top: 1px solid var(--tint-danger);
}

.llm-cell-select {
  width: 100%;
  padding: 6px 8px;
  background: var(--bg-elevated);
  border: none;
  border-top: 1px solid var(--border-subtle);
  color: var(--text-secondary);
  font-size: 11px;
  cursor: pointer;
}

.llm-actions button:disabled,
.input-actions button:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.llm-input {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 8px;
  border-top: 1px solid var(--border-subtle);
}

.llm-input textarea {
  width: 100%;
  padding: 6px 8px;
  background: var(--bg-input);
  border: 1px solid var(--border-strong);
  border-radius: 6px;
  color: var(--text-primary);
  font-size: 12px;
  resize: none;
  font-family: inherit;
}

.llm-input textarea:focus {
  outline: none;
  border-color: var(--accent-primary);
}

.input-actions {
  display: flex;
  gap: 6px;
}

.chat-btn {
  flex: 1;
  padding: 6px 12px;
  background: var(--bg-input);
  color: var(--text-primary);
  border: 1px solid var(--border-strong);
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
}

.chat-btn:hover:not(:disabled) {
  background: var(--border-strong);
}

.agent-btn {
  flex: 1;
  padding: 6px 12px;
  background: var(--tint-success);
  color: var(--accent-success);
  border: 1px solid var(--tint-success);
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
}

.agent-btn:hover:not(:disabled) {
  background: var(--tint-success);
}

.llm-footer {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 8px;
  border-top: 1px solid var(--border-subtle);
}

.token-count {
  font-size: 10px;
  color: var(--text-muted);
}

.provider-label {
  font-size: 10px;
  color: var(--text-muted);
  margin-left: auto;
}

.clear-btn {
  font-size: 10px;
  padding: 2px 6px;
  background: none;
  border: 1px solid var(--bg-input);
  border-radius: 4px;
  color: var(--text-muted);
  cursor: pointer;
}

.clear-btn:hover:not(:disabled) {
  border-color: var(--accent-danger);
  color: var(--accent-danger);
}

.clear-btn:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}
.agent-progress {
  border-top: 1px solid var(--border-subtle);
  padding: 8px;
}

.agent-progress-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 6px;
}

.agent-label {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  color: var(--accent-success);
  letter-spacing: 0.06em;
}

.agent-cancel-btn {
  font-size: 10px;
  padding: 2px 8px;
  background: var(--tint-danger);
  border: 1px solid var(--tint-danger);
  border-radius: 4px;
  color: var(--accent-danger);
  cursor: pointer;
}

.agent-cancel-btn:hover {
  background: var(--tint-danger);
}

.agent-status {
  font-size: 10px;
  font-weight: 600;
}

.agent-status.done {
  color: var(--accent-success);
}

.agent-status.error {
  color: var(--accent-danger);
}

.agent-log {
  max-height: 150px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.agent-event {
  display: flex;
  gap: 6px;
  font-size: 11px;
  padding: 2px 4px;
  border-radius: 3px;
}

.event-type {
  flex-shrink: 0;
  font-weight: 600;
  color: var(--text-muted);
  min-width: 60px;
}

.agent-event.tool_call .event-type {
  color: var(--accent-primary);
}

.agent-event.tool_result .event-type {
  color: var(--accent-success);
}

.agent-event.error .event-type {
  color: var(--accent-danger);
}

.agent-event.thinking .event-type {
  color: var(--accent-warning);
}

.event-detail {
  color: var(--text-secondary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.agent-error {
  margin-top: 6px;
  padding: 6px;
  color: var(--accent-danger);
  font-size: 11px;
  background: var(--tint-danger);
  border-radius: 4px;
}

.approval-stack {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 8px;
  border-top: 1px solid var(--border-subtle);
}

.approval-card {
  border: 1px solid var(--accent-warning);
  background: var(--tint-warning, var(--bg-elevated));
  border-radius: 6px;
  padding: 8px;
}

.approval-summary {
  display: flex;
  flex-direction: column;
  gap: 2px;
  margin-bottom: 6px;
}

.approval-tool {
  font-size: 11px;
  font-weight: 700;
  color: var(--accent-warning);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.approval-detail {
  font-size: 11px;
  color: var(--text-secondary);
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  word-break: break-word;
}

.approval-actions {
  display: flex;
  gap: 6px;
}

.approve-btn,
.decline-btn {
  flex: 1;
  padding: 4px 10px;
  font-size: 11px;
  font-weight: 600;
  border-radius: 4px;
  cursor: pointer;
}

.approve-btn {
  background: var(--accent-success);
  color: var(--bg-elevated);
  border: 1px solid var(--accent-success);
}

.decline-btn {
  background: none;
  color: var(--text-secondary);
  border: 1px solid var(--border-strong);
}

.auto-approve-toggle {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 10px;
  color: var(--text-muted);
  cursor: pointer;
  margin-right: auto;
}

.auto-approve-toggle input[type='checkbox'] {
  accent-color: var(--accent-primary);
}
</style>
