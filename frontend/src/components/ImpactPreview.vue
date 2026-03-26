<script setup lang="ts">
import { useNotebook } from '../stores/notebook'

const { currentImpactPreview, clearImpactPreview, executeCellWebSocket } = useNotebook()

function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

function dismiss() {
  clearImpactPreview()
}

function runAnyway() {
  if (currentImpactPreview.value) {
    executeCellWebSocket(currentImpactPreview.value.targetCellId)
    clearImpactPreview()
  }
}
</script>

<template>
  <Teleport to="body">
    <div v-if="currentImpactPreview" class="impact-overlay" @click="dismiss">
      <div class="impact-dialog" @click.stop>
        <h3 class="impact-title">Run Impact Preview</h3>

        <!-- Upstream cells that need to run -->
        <div v-if="currentImpactPreview.upstream.length > 0" class="impact-section">
          <div class="section-label">
            &#x2191; Will re-run {{ currentImpactPreview.upstream.length }} upstream cell{{ currentImpactPreview.upstream.length > 1 ? 's' : '' }}:
          </div>
          <div
            v-for="step in currentImpactPreview.upstream"
            :key="step.cellId"
            class="impact-row"
          >
            <span class="impact-cell-name">{{ step.cellName }}</span>
            <span class="impact-reason" :class="`reason-${step.reason}`">{{ step.reason }}</span>
            <span v-if="step.estimatedMs" class="impact-est">~{{ formatMs(step.estimatedMs) }}</span>
          </div>
        </div>

        <!-- Downstream cells that will become stale -->
        <div v-if="currentImpactPreview.downstream.length > 0" class="impact-section">
          <div class="section-label">
            &#x2193; Will invalidate {{ currentImpactPreview.downstream.length }} downstream cell{{ currentImpactPreview.downstream.length > 1 ? 's' : '' }}:
          </div>
          <div
            v-for="d in currentImpactPreview.downstream"
            :key="d.cellId"
            class="impact-row"
          >
            <span class="impact-cell-name">{{ d.cellName }}</span>
            <span class="impact-status-change">
              {{ d.currentStatus }} &rarr; stale
            </span>
          </div>
        </div>

        <div v-if="currentImpactPreview.estimatedMs > 0" class="impact-estimate">
          Estimated time: ~{{ formatMs(currentImpactPreview.estimatedMs) }}
        </div>

        <div class="impact-actions">
          <button class="btn btn-primary" @click="runAnyway">Run</button>
          <button class="btn btn-secondary" @click="dismiss">Cancel</button>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<style scoped>
.impact-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}
.impact-dialog {
  background: #181825;
  border: 1px solid #313244;
  border-radius: 8px;
  padding: 20px;
  min-width: 400px;
  max-width: 500px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
}
.impact-title {
  font-size: 16px;
  font-weight: 600;
  color: #cdd6f4;
  margin-bottom: 16px;
}

.impact-section {
  margin-bottom: 12px;
}
.section-label {
  font-size: 12px;
  font-weight: 600;
  color: #a6adc8;
  margin-bottom: 6px;
}

.impact-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 4px 8px;
  font-size: 12px;
  font-family: "JetBrains Mono", "Fira Code", monospace;
  background: #1e1e2e;
  border-radius: 4px;
  margin-bottom: 3px;
}
.impact-cell-name {
  flex: 1;
  color: #cdd6f4;
}
.impact-reason {
  padding: 1px 6px;
  border-radius: 3px;
  font-size: 10px;
  font-weight: 600;
}
.reason-stale {
  background: #f9e2af22;
  color: #f9e2af;
}
.reason-missing {
  background: #6c708622;
  color: #6c7086;
}
.reason-target {
  background: #89b4fa22;
  color: #89b4fa;
}
.impact-est {
  color: #6c7086;
  font-size: 11px;
}
.impact-status-change {
  color: #f9e2af;
  font-size: 11px;
}

.impact-estimate {
  font-size: 12px;
  color: #6c7086;
  margin-bottom: 16px;
  padding-top: 8px;
  border-top: 1px solid #2a2a3c;
}

.impact-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
}
.btn {
  padding: 6px 16px;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  border: none;
}
.btn-primary {
  background: #89b4fa;
  color: #1e1e2e;
}
.btn-primary:hover { background: #74c7ec; }
.btn-secondary {
  background: #313244;
  color: #cdd6f4;
}
.btn-secondary:hover { background: #45475a; }
</style>
