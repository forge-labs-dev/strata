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
  runAgentAction,
  cancelAgent,
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
              v-if="msg.role === 'assistant' && extractCodeBlocks(msg.content).length > 0"
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
          <option v-for="cell in orderedCells" :key="cell.id" :value="cell.id">
            {{ cell.defines.length ? cell.defines.join(', ') : `Cell ${cell.id.slice(0, 6)}` }}
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
          <span v-if="totalTokens.total > 0" class="token-count">
            {{ totalTokens.total.toLocaleString() }} tokens
          </span>
          <span v-if="llmProvider" class="provider-label">{{ llmProvider }}</span>
          <button class="clear-btn" :disabled="llmMessages.length === 0" @click="clearLlmHistory">
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
  border-top: 1px solid #2a2a3c;
  padding-top: 8px;
}

.panel-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  width: 100%;
  background: none;
  border: none;
  color: #a6adc8;
  font-size: 12px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  cursor: pointer;
  padding: 4px 0;
}

.panel-toggle:hover {
  color: #cdd6f4;
}

.toggle-label {
  display: flex;
  align-items: center;
  gap: 8px;
}

.toggle-icon {
  font-size: 10px;
  color: #6c7086;
}

.model-badge {
  font-size: 10px;
  font-weight: 400;
  color: #a6adc8;
  background: #313244;
  padding: 1px 6px;
  border-radius: 4px;
}

.model-badge-clickable {
  cursor: pointer;
}

.model-badge-clickable:hover {
  background: #45475a;
  color: #cdd6f4;
}

.model-selector {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 8px;
  border-bottom: 1px solid #2a2a3c;
}

.model-selector-label {
  font-size: 11px;
  color: #6c7086;
  flex-shrink: 0;
}

.model-selector-btn {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 4px 8px;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 4px;
  color: #cdd6f4;
  font-size: 11px;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  cursor: pointer;
}

.model-selector-btn:hover {
  border-color: #89b4fa;
}

.model-chevron {
  font-size: 8px;
  color: #6c7086;
}

.model-picker {
  border: 1px solid #313244;
  border-radius: 6px;
  background: #181825;
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
  color: #a6adc8;
  font-size: 11px;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  cursor: pointer;
}

.model-picker-item:hover {
  background: #313244;
  color: #cdd6f4;
}

.model-picker-item.active {
  color: #89b4fa;
  font-weight: 600;
}

.model-picker-loading,
.model-picker-empty {
  padding: 8px 12px;
  color: #6c7086;
  font-size: 11px;
}

.panel-content {
  margin-top: 8px;
  border: 1px solid #313244;
  border-radius: 8px;
  background: #11111b;
  overflow: hidden;
}

.llm-unconfigured {
  padding: 16px;
  text-align: center;
  color: #6c7086;
  font-size: 13px;
}

.llm-unconfigured code {
  background: #313244;
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
  color: #6c7086;
  font-size: 12px;
}

.llm-msg {
  margin-bottom: 8px;
  padding: 8px;
  border-radius: 6px;
}

.llm-msg.user {
  background: #1e1e2e;
}

.llm-msg.assistant {
  background: #181825;
  border: 1px solid #2a2a3c;
}

.msg-header {
  display: flex;
  justify-content: space-between;
  margin-bottom: 4px;
}

.msg-role {
  font-size: 11px;
  font-weight: 600;
  color: #89b4fa;
}

.llm-msg.user .msg-role {
  color: #a6adc8;
}

.msg-tokens {
  font-size: 10px;
  color: #6c7086;
}

.msg-content {
  font-size: 12px;
  line-height: 1.5;
  color: #cdd6f4;
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
  background: #89b4fa;
  color: #1e1e2e;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-weight: 600;
}

.insert-btn:hover {
  background: #74c7ec;
}

.insert-btn.insert-all {
  background: #a6e3a1;
  color: #1e1e2e;
}

.insert-btn.insert-all:hover {
  background: #94e2d5;
}

.llm-loading {
  padding: 8px;
  color: #a6adc8;
  font-size: 12px;
  font-style: italic;
}

.llm-error {
  padding: 8px;
  color: #f38ba8;
  font-size: 12px;
  background: #45252530;
  border-top: 1px solid #f38ba850;
}

.llm-cell-select {
  width: 100%;
  padding: 6px 8px;
  background: #1e1e2e;
  border: none;
  border-top: 1px solid #2a2a3c;
  color: #a6adc8;
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
  border-top: 1px solid #2a2a3c;
}

.llm-input textarea {
  width: 100%;
  padding: 6px 8px;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 12px;
  resize: none;
  font-family: inherit;
}

.llm-input textarea:focus {
  outline: none;
  border-color: #89b4fa;
}

.input-actions {
  display: flex;
  gap: 6px;
}

.chat-btn {
  flex: 1;
  padding: 6px 12px;
  background: #313244;
  color: #cdd6f4;
  border: 1px solid #45475a;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
}

.chat-btn:hover:not(:disabled) {
  background: #45475a;
}

.agent-btn {
  flex: 1;
  padding: 6px 12px;
  background: #a6e3a120;
  color: #a6e3a1;
  border: 1px solid #a6e3a140;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
}

.agent-btn:hover:not(:disabled) {
  background: #a6e3a140;
}

.llm-footer {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 8px;
  border-top: 1px solid #2a2a3c;
}

.token-count {
  font-size: 10px;
  color: #6c7086;
}

.provider-label {
  font-size: 10px;
  color: #6c7086;
  margin-left: auto;
}

.clear-btn {
  font-size: 10px;
  padding: 2px 6px;
  background: none;
  border: 1px solid #313244;
  border-radius: 4px;
  color: #6c7086;
  cursor: pointer;
}

.clear-btn:hover:not(:disabled) {
  border-color: #f38ba8;
  color: #f38ba8;
}

.clear-btn:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}
.agent-progress {
  border-top: 1px solid #2a2a3c;
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
  color: #a6e3a1;
  letter-spacing: 0.06em;
}

.agent-cancel-btn {
  font-size: 10px;
  padding: 2px 8px;
  background: #f38ba830;
  border: 1px solid #f38ba850;
  border-radius: 4px;
  color: #f38ba8;
  cursor: pointer;
}

.agent-cancel-btn:hover {
  background: #f38ba850;
}

.agent-status {
  font-size: 10px;
  font-weight: 600;
}

.agent-status.done {
  color: #a6e3a1;
}

.agent-status.error {
  color: #f38ba8;
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
  color: #6c7086;
  min-width: 60px;
}

.agent-event.tool_call .event-type {
  color: #89b4fa;
}

.agent-event.tool_result .event-type {
  color: #a6e3a1;
}

.agent-event.error .event-type {
  color: #f38ba8;
}

.agent-event.thinking .event-type {
  color: #f9e2af;
}

.event-detail {
  color: #a6adc8;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.agent-error {
  margin-top: 6px;
  padding: 6px;
  color: #f38ba8;
  font-size: 11px;
  background: #45252530;
  border-radius: 4px;
}
</style>
