<template>
  <router-view />
</template>

<style>
*,
*::before,
*::after {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

body {
  background: #11111b;
  color: #cdd6f4;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  -webkit-font-smoothing: antialiased;
}

.app {
  display: flex;
  flex-direction: column;
  height: 100vh;
}

.header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 16px;
  background: #181825;
  border-bottom: 1px solid #2a2a3c;
  flex-shrink: 0;
}
.header-left {
  display: flex;
  align-items: center;
  gap: 16px;
}
.header-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.logo {
  font-weight: 700;
  font-size: 16px;
  color: #89b4fa;
  letter-spacing: -0.5px;
  cursor: pointer;
}
.logo:hover {
  color: #74c7ec;
}
.notebook-name {
  font-size: 14px;
  color: #a6adc8;
  cursor: pointer;
  padding: 2px 6px;
  border-radius: 4px;
}
.notebook-name:hover {
  background: #313244;
}
.name-input {
  font-size: 14px;
  background: #313244;
  border: 1px solid #89b4fa;
  color: #cdd6f4;
  padding: 2px 6px;
  border-radius: 4px;
  outline: none;
}

.connection {
  font-size: 12px;
  color: #6c7086;
}
.connection.connected {
  color: #a6e3a1;
}

.error-banner {
  background: #45252530;
  border-bottom: 1px solid #f38ba8;
  color: #f38ba8;
  padding: 8px 16px;
  font-size: 13px;
  display: flex;
  align-items: center;
}

.btn {
  background: #89b4fa;
  color: #1e1e2e;
  border: none;
  padding: 6px 14px;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
}
.btn:hover {
  background: #74c7ec;
}
.btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
.btn-secondary {
  background: #313244;
  color: #cdd6f4;
}
.btn-secondary:hover {
  background: #45475a;
}

.main {
  display: flex;
  flex: 1;
  overflow: hidden;
  min-height: 0;
}
.cells-panel {
  flex: 1;
  overflow-y: auto;
  padding: 16px;
  min-width: 0;
}
.sidebar-resizer {
  width: 12px;
  flex-shrink: 0;
  position: relative;
  cursor: col-resize;
  touch-action: none;
}
.sidebar-resizer::before {
  content: '';
  position: absolute;
  top: 16px;
  bottom: 16px;
  left: 50%;
  width: 1px;
  transform: translateX(-50%);
  background: #2a2a3c;
}
.sidebar-resizer:hover::before {
  background: #89b4fa;
}
.sidebar {
  width: var(--sidebar-width);
  flex-shrink: 0;
  min-width: 280px;
  max-width: 560px;
  padding: 16px 16px 16px 4px;
  overflow-y: auto;
}

.add-cell-btn {
  width: 100%;
  padding: 12px;
  background: none;
  border: 1px dashed #313244;
  border-radius: 8px;
  color: #6c7086;
  font-size: 13px;
  cursor: pointer;
  margin-top: 4px;
}
.add-cell-btn:hover {
  border-color: #89b4fa;
  color: #89b4fa;
}
.add-cell-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.modal-overlay {
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

.modal-dialog {
  background: #181825;
  border: 1px solid #313244;
  border-radius: 8px;
  padding: 24px;
  min-width: 400px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
}

.modal-dialog h2 {
  margin-bottom: 16px;
  font-size: 18px;
  color: #cdd6f4;
}

.modal-input {
  width: 100%;
  padding: 8px 12px;
  background: #313244;
  border: 1px solid #45475a;
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 14px;
  margin-bottom: 16px;
  box-sizing: border-box;
}

.modal-input:focus {
  outline: none;
  border-color: #89b4fa;
  box-shadow: 0 0 0 2px rgba(137, 180, 250, 0.2);
}

.modal-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
}

body.resizing-sidebar {
  cursor: col-resize;
  user-select: none;
}

@media (max-width: 980px) {
  .main {
    flex-direction: column;
  }

  .cells-panel {
    padding-bottom: 8px;
  }

  .sidebar-resizer {
    display: none;
  }

  .sidebar {
    width: auto;
    min-width: 0;
    max-width: none;
    padding: 12px 16px 16px;
    border-top: 1px solid #2a2a3c;
  }
}
</style>
