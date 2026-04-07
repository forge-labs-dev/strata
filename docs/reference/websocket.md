# WebSocket Protocol

The notebook UI communicates with the backend via a WebSocket connection for real-time updates.

## Connection

```
ws://localhost:8765/v1/notebooks/ws/{session_id}
```

All messages are JSON with this shape:

```json
{
  "type": "message_type",
  "seq": 1,
  "ts": "2026-01-01T00:00:00Z",
  "payload": { ... }
}
```

## Client → Server Messages

### Cell Execution

| Type | Payload | Description |
|------|---------|-------------|
| `cell_execute` | `{ "cell_id": "..." }` | Run cell (triggers cascade check) |
| `cell_execute_cascade` | `{ "cell_id": "...", "plan_id": "..." }` | Confirm cascade execution |
| `cell_execute_force` | `{ "cell_id": "..." }` | Run cell ignoring staleness |
| `cell_cancel` | `{ "cell_id": "..." }` | Cancel running cell |
| `notebook_run_all` | `{}` | Run all cells in topological order |

### Cell Editing

| Type | Payload | Description |
|------|---------|-------------|
| `cell_source_update` | `{ "cell_id": "...", "source": "..." }` | Source changed |

### State

| Type | Payload | Description |
|------|---------|-------------|
| `notebook_sync` | `{}` | Request full state (for reconnection) |
| `impact_preview_request` | `{ "cell_id": "..." }` | Get upstream/downstream effects |
| `profiling_request` | `{}` | Get execution metrics |

### Inspect REPL

| Type | Payload | Description |
|------|---------|-------------|
| `inspect_open` | `{ "cell_id": "..." }` | Open REPL for cell |
| `inspect_eval` | `{ "cell_id": "...", "expr": "..." }` | Evaluate expression |
| `inspect_close` | `{ "cell_id": "..." }` | Close REPL |

### Dependencies

| Type | Payload | Description |
|------|---------|-------------|
| `dependency_add` | `{ "package": "..." }` | Install package |
| `dependency_remove` | `{ "package": "..." }` | Remove package |

## Server → Client Messages

### Cell Status

| Type | Payload | Description |
|------|---------|-------------|
| `cell_status` | `{ "cell_id": "...", "status": "running" }` | Status changed |
| `cell_output` | `{ "cell_id": "...", "outputs": {...}, "cache_hit": false }` | Execution result |
| `cell_console` | `{ "cell_id": "...", "stream": "stdout", "text": "..." }` | Incremental output |
| `cell_error` | `{ "cell_id": "...", "error": "..." }` | Execution error |

### Cascade

| Type | Payload | Description |
|------|---------|-------------|
| `cascade_prompt` | `{ "cell_id": "...", "plan_id": "...", "steps": [...] }` | Upstream cells need execution |
| `cascade_progress` | `{ "plan_id": "...", "current_cell_id": "...", "completed": 1, "total": 3 }` | Cascade progress |

### DAG

| Type | Payload | Description |
|------|---------|-------------|
| `dag_update` | `{ "edges": [...], "roots": [...], "leaves": [...] }` | DAG changed after cell edit |

### State

| Type | Payload | Description |
|------|---------|-------------|
| `notebook_state` | `{ "id": "...", "cells": [...], "dag": {...} }` | Full state (response to `notebook_sync`) |
| `impact_preview` | `{ "target_cell_id": "...", "upstream": [...], "downstream": [...] }` | Impact analysis result |
| `profiling_summary` | `{ "total_execution_ms": ..., "cell_profiles": [...] }` | Profiling metrics |

### Inspect

| Type | Payload | Description |
|------|---------|-------------|
| `inspect_result` | `{ "action": "eval", "ok": true, "result": "42", "type": "int" }` | REPL result |

### Dependencies

| Type | Payload | Description |
|------|---------|-------------|
| `dependency_changed` | `{ "package": "...", "action": "add", "success": true }` | Package operation result |

### Errors

| Type | Payload | Description |
|------|---------|-------------|
| `error` | `{ "error": "..." }` | Protocol error |
