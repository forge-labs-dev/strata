<script setup lang="ts">
import { onMounted } from 'vue'
import { useNotebook } from '../stores/notebook'

const { profilingSummary, requestProfilingSummary } = useNotebook()

onMounted(() => {
  requestProfilingSummary()
})

function formatMs(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

function refresh() {
  requestProfilingSummary()
}
</script>

<template>
  <div class="profiling-panel">
    <div class="profiling-header">
      <span>Profiling</span>
      <button class="refresh-btn" title="Refresh" @click="refresh">&#x21BB;</button>
    </div>

    <div v-if="profilingSummary" class="profiling-body">
      <div class="profiling-stats">
        <div class="stat-row">
          <span class="stat-label">Total execution</span>
          <span class="stat-value">{{ formatMs(profilingSummary.totalExecutionMs) }}</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">Cache savings</span>
          <span class="stat-value cache-savings">
            ~{{ formatMs(profilingSummary.cacheSavingsMs) }}
            <span class="stat-detail">({{ profilingSummary.cacheHits }} hits)</span>
          </span>
        </div>
        <div v-if="profilingSummary.totalArtifactBytes > 0" class="stat-row">
          <span class="stat-label">Artifact storage</span>
          <span class="stat-value">{{ formatBytes(profilingSummary.totalArtifactBytes) }}</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">Cells</span>
          <span class="stat-value">
            {{ profilingSummary.cacheHits }} cached, {{ profilingSummary.cacheMisses }} computed
          </span>
        </div>
      </div>

      <div v-if="profilingSummary.cellProfiles.length" class="cell-profiles">
        <div v-for="cp in profilingSummary.cellProfiles" :key="cp.cellId" class="cell-profile-row">
          <span class="cp-name" :title="cp.cellId">{{ cp.cellName }}</span>
          <span class="cp-status" :class="`cp-${cp.status}`">
            {{ cp.cacheHit ? '&#x26A1;' : '&#x25D0;' }}
          </span>
          <span class="cp-duration">{{ cp.durationMs ? formatMs(cp.durationMs) : '-' }}</span>
        </div>
      </div>
    </div>
    <div v-else class="profiling-empty">No profiling data yet. Run some cells first.</div>
  </div>
</template>

<style scoped>
.profiling-panel {
  background: #1e1e2e;
  border: 1px solid #2a2a3c;
  border-radius: 8px;
  margin-top: 8px;
  min-width: 180px;
  overflow: auto;
}
.profiling-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 12px;
  font-size: 12px;
  font-weight: 600;
  color: #6c7086;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  border-bottom: 1px solid #2a2a3c;
}
.refresh-btn {
  background: none;
  border: none;
  color: #6c7086;
  cursor: pointer;
  font-size: 14px;
  padding: 0 4px;
  border-radius: 3px;
}
.refresh-btn:hover {
  color: #89b4fa;
}

.profiling-body {
  padding: 8px;
}
.profiling-stats {
  margin-bottom: 8px;
}
.stat-row {
  display: flex;
  justify-content: space-between;
  padding: 2px 4px;
  font-size: 11px;
}
.stat-label {
  color: #6c7086;
}
.stat-value {
  color: #cdd6f4;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
}
.cache-savings {
  color: #a6e3a1;
}
.stat-detail {
  color: #6c7086;
  font-size: 10px;
}

.cell-profiles {
  border-top: 1px solid #2a2a3c;
  padding-top: 6px;
}
.cell-profile-row {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 2px 4px;
  font-size: 11px;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
}
.cp-name {
  flex: 1;
  color: #cdd6f4;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.cp-status {
  font-size: 10px;
}
.cp-ready {
  color: #a6e3a1;
}
.cp-stale {
  color: #f9e2af;
}
.cp-error {
  color: #f38ba8;
}
.cp-idle {
  color: #6c7086;
}
.cp-duration {
  color: #6c7086;
  min-width: 36px;
  text-align: right;
}

.profiling-empty {
  padding: 12px;
  font-size: 11px;
  color: #6c7086;
  text-align: center;
}
</style>
