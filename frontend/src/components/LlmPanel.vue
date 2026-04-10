<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import { useNotebook } from '../stores/notebook'

const {
  connected,
  orderedCells,
  llmAvailable,
  llmModel,
  llmProvider,
  llmLoading,
  llmError,
  llmMessages,
  checkLlmStatus,
  llmCompleteAction,
  clearLlmHistory,
  insertLlmCodeAsCell,
  insertLlmCodeAsCells,
  agentRunning,
  agentProgress,
  agentError,
  runAgentAction,
  cancelAgent,
} = useNotebook()

const showPanel = ref(false)
const userMessage = ref('')
const selectedCellId = ref<string | null>(null)
const messagesEl = ref<HTMLDivElement | null>(null)

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

async function send(action: 'generate' | 'explain' | 'describe' | 'chat' | 'plan' = 'generate') {
  const msg = userMessage.value.trim()
  if (!msg && (action === 'generate' || action === 'plan')) return
  const finalMessage =
    msg || (action === 'explain' ? 'Explain this cell' : 'Describe what this cell does')
  userMessage.value = ''
  await llmCompleteAction(action, finalMessage, selectedCellId.value ?? undefined)
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

async function runAgent() {
  const msg = userMessage.value.trim()
  if (!msg) return
  userMessage.value = ''
  await runAgentAction(msg)
}

function handleInsertAll(codes: string[]) {
  const lastCell = orderedCells.value.at(-1)
  void insertLlmCodeAsCells(codes, lastCell?.id)
}

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
        <span v-if="llmModel" class="model-badge">{{ llmModel }}</span>
      </span>
      <span class="toggle-icon">{{ showPanel ? '\u25B2' : '\u25BC' }}</span>
    </button>

    <div v-if="showPanel" class="panel-content">
      <!-- Not configured -->
      <div v-if="!llmAvailable" class="llm-unconfigured">
        <p>No LLM provider configured.</p>
        <p class="llm-hint">
          Set <code>ANTHROPIC_API_KEY</code>, <code>OPENAI_API_KEY</code>, or
          <code>STRATA_AI_API_KEY</code> to enable.
        </p>
      </div>

      <template v-else>
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
          <div v-if="llmLoading" class="llm-loading">Thinking...</div>
        </div>

        <!-- Error -->
        <div v-if="llmError" class="llm-error">{{ llmError }}</div>

        <!-- Cell selector -->
        <select v-model="selectedCellId" class="llm-cell-select">
          <option :value="null">All cells (notebook context)</option>
          <option v-for="cell in orderedCells" :key="cell.id" :value="cell.id">
            {{ cell.defines.length ? cell.defines.join(', ') : `Cell ${cell.id.slice(0, 6)}` }}
          </option>
        </select>

        <!-- Quick actions -->
        <div class="llm-actions">
          <button :disabled="llmLoading || !connected" @click="send('explain')">Explain</button>
          <button :disabled="llmLoading || !connected" @click="send('describe')">Describe</button>
          <button
            class="plan-btn"
            :disabled="llmLoading || agentRunning || !connected || !userMessage.trim()"
            title="Type what you want to build, then click Plan"
            @click="send('plan')"
          >
            Plan
          </button>
          <button
            class="agent-btn"
            :disabled="llmLoading || agentRunning || !connected || !userMessage.trim()"
            title="Agent mode: creates cells, runs them, fixes errors automatically"
            @click="runAgent"
          >
            Agent
          </button>
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

        <!-- Input -->
        <div class="llm-input">
          <textarea
            v-model="userMessage"
            placeholder="Describe what you want to build..."
            :disabled="llmLoading || !connected"
            rows="2"
            @keydown.enter.exact.prevent="send('generate')"
          ></textarea>
          <button
            class="send-btn"
            :disabled="!userMessage.trim() || llmLoading || !connected"
            @click="send('generate')"
          >
            Send
          </button>
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
.llm-panel {
  margin-top: 8px;
}

.panel-toggle {
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 12px;
  background: #181825;
  border: 1px solid #313244;
  border-radius: 8px;
  color: #cdd6f4;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
}

.panel-toggle:hover {
  border-color: #89b4fa;
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

.llm-actions {
  display: flex;
  gap: 4px;
  padding: 6px 8px;
  border-top: 1px solid #2a2a3c;
}

.llm-actions button {
  flex: 1;
  padding: 4px 8px;
  background: #313244;
  color: #cdd6f4;
  border: none;
  border-radius: 4px;
  font-size: 11px;
  cursor: pointer;
}

.llm-actions button:hover:not(:disabled) {
  background: #45475a;
}

.llm-actions .plan-btn {
  background: #89b4fa30;
  color: #89b4fa;
  border: 1px solid #89b4fa40;
}

.llm-actions .plan-btn:hover:not(:disabled) {
  background: #89b4fa50;
}

.llm-actions .agent-btn {
  background: #a6e3a130;
  color: #a6e3a1;
  border: 1px solid #a6e3a140;
}

.llm-actions .agent-btn:hover:not(:disabled) {
  background: #a6e3a150;
}

.llm-actions button:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.llm-input {
  display: flex;
  gap: 6px;
  padding: 8px;
  border-top: 1px solid #2a2a3c;
}

.llm-input textarea {
  flex: 1;
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

.send-btn {
  padding: 6px 12px;
  background: #89b4fa;
  color: #1e1e2e;
  border: none;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  align-self: flex-end;
}

.send-btn:hover:not(:disabled) {
  background: #74c7ec;
}

.send-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
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
