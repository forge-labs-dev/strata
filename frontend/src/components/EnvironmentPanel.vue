<script setup lang="ts">
import { ref } from 'vue'
import { useNotebook } from '../stores/notebook'

const {
  dependencies,
  dependencyLoading,
  dependencyError,
  addDependencyAction,
  removeDependencyAction,
  connected,
} = useNotebook()

const newPackage = ref('')
const showPanel = ref(false)

async function addPackage() {
  const pkg = newPackage.value.trim()
  if (!pkg) return
  await addDependencyAction(pkg)
  newPackage.value = ''
}

async function removePackage(name: string) {
  await removeDependencyAction(name)
}
</script>

<template>
  <div class="env-panel">
    <button class="env-toggle" @click="showPanel = !showPanel">
      {{ showPanel ? 'Packages' : 'Packages' }}
      <span class="dep-count">{{ dependencies.length }}</span>
      <span class="toggle-icon">{{ showPanel ? '&#9650;' : '&#9660;' }}</span>
    </button>

    <div v-if="showPanel" class="env-content">
      <!-- Add dependency -->
      <div class="add-dep">
        <input
          v-model="newPackage"
          type="text"
          placeholder="pip package (e.g. pandas)"
          class="dep-input"
          :disabled="!connected || dependencyLoading"
          @keydown.enter="addPackage"
        />
        <button
          class="btn-add"
          :disabled="!connected || dependencyLoading || !newPackage.trim()"
          @click="addPackage"
        >
          {{ dependencyLoading ? '...' : '+' }}
        </button>
      </div>

      <!-- Error -->
      <div v-if="dependencyError" class="dep-error">
        {{ dependencyError }}
      </div>

      <!-- Dependency list -->
      <div v-if="dependencies.length === 0" class="dep-empty">No packages installed</div>
      <ul v-else class="dep-list">
        <li v-for="dep in dependencies" :key="dep.name" class="dep-item">
          <span class="dep-name">{{ dep.name }}</span>
          <span class="dep-version">{{ dep.version || dep.specifier }}</span>
          <button
            class="btn-remove"
            title="Remove"
            :disabled="dependencyLoading"
            @click="removePackage(dep.name)"
          >
            x
          </button>
        </li>
      </ul>
    </div>
  </div>
</template>

<style scoped>
.env-panel {
  margin-top: 12px;
  border-top: 1px solid #2a2a3c;
  padding-top: 8px;
}

.env-toggle {
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
.env-toggle:hover {
  color: #cdd6f4;
}

.dep-count {
  background: #313244;
  color: #89b4fa;
  padding: 1px 6px;
  border-radius: 8px;
  font-size: 11px;
  font-weight: 600;
}

.toggle-icon {
  margin-left: auto;
  font-size: 10px;
}

.env-content {
  margin-top: 8px;
}

.add-dep {
  display: flex;
  gap: 4px;
  margin-bottom: 8px;
}

.dep-input {
  flex: 1;
  padding: 5px 8px;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 4px;
  color: #cdd6f4;
  font-size: 12px;
  min-width: 0;
}
.dep-input:focus {
  outline: none;
  border-color: #89b4fa;
}
.dep-input::placeholder {
  color: #585b70;
}

.btn-add {
  background: #89b4fa;
  color: #1e1e2e;
  border: none;
  width: 28px;
  border-radius: 4px;
  font-size: 14px;
  font-weight: 700;
  cursor: pointer;
  flex-shrink: 0;
}
.btn-add:hover {
  background: #74c7ec;
}
.btn-add:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.dep-error {
  color: #f38ba8;
  font-size: 11px;
  margin-bottom: 6px;
  padding: 4px 6px;
  background: #45252530;
  border-radius: 4px;
}

.dep-empty {
  color: #585b70;
  font-size: 12px;
  text-align: center;
  padding: 8px 0;
}

.dep-list {
  list-style: none;
  max-height: 200px;
  overflow-y: auto;
}

.dep-item {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 3px 0;
  font-size: 12px;
}

.dep-name {
  color: #cdd6f4;
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.dep-version {
  color: #585b70;
  font-size: 11px;
  flex-shrink: 0;
}

.btn-remove {
  background: none;
  border: none;
  color: #585b70;
  cursor: pointer;
  font-size: 11px;
  padding: 0 4px;
  flex-shrink: 0;
  border-radius: 2px;
}
.btn-remove:hover {
  color: #f38ba8;
  background: #45252530;
}
.btn-remove:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
</style>
