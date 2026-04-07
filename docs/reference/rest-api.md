# REST API Reference

All notebook endpoints are under `/v1/notebooks`.

!!! note "Session ID vs Notebook ID"
    Route parameters use the **session ID** (a UUID generated when the notebook is opened), not the persistent `notebook_id` from `notebook.toml`. The session ID is returned by the `open` and `create` endpoints.

## Notebook Lifecycle

### Create Notebook

```
POST /v1/notebooks/create
```

```json
{
  "parent_path": "/path/to/directory",
  "name": "My Notebook",
  "python_version": "3.13",
  "starter_cell": true
}
```

Returns notebook state with `session_id`.

### Open Notebook

```
POST /v1/notebooks/open
```

```json
{
  "path": "/path/to/notebook"
}
```

Returns notebook state with `session_id` and `dag`.

### Delete Notebook

```
DELETE /v1/notebooks/{session_id}
```

Deletes the notebook directory and closes the session.

### Rename Notebook

```
PUT /v1/notebooks/{session_id}/name
```

```json
{
  "name": "New Name"
}
```

## Sessions

### List Sessions

```
GET /v1/notebooks/sessions
```

Returns `{ "sessions": [{ "session_id", "name", "path", ... }] }`.

### Get Session

```
GET /v1/notebooks/sessions/{session_id}
```

Returns full notebook state (same shape as `open`). Used for page refresh reconnection.

## Cells

### List Cells

```
GET /v1/notebooks/{session_id}/cells
```

### Add Cell

```
POST /v1/notebooks/{session_id}/cells
```

```json
{
  "after_cell_id": "optional-cell-id"
}
```

### Update Cell Source

```
PUT /v1/notebooks/{session_id}/cells/{cell_id}
```

```json
{
  "source": "x = 1"
}
```

Returns updated cell, DAG, and all cells (with refreshed staleness).

### Delete Cell

```
DELETE /v1/notebooks/{session_id}/cells/{cell_id}
```

### Reorder Cells

```
PUT /v1/notebooks/{session_id}/cells/reorder
```

```json
{
  "cell_ids": ["cell-1", "cell-3", "cell-2"]
}
```

### Execute Cell (REST)

```
POST /v1/notebooks/{session_id}/cells/{cell_id}/execute
```

!!! tip
    For interactive use, prefer the WebSocket `cell_execute` message. The REST endpoint is for programmatic access.

## DAG

### Get DAG

```
GET /v1/notebooks/{session_id}/dag
```

Returns edges, roots, leaves, and topological order.

## Environment

### List Dependencies

```
GET /v1/notebooks/{session_id}/dependencies
```

### Add Dependency

```
POST /v1/notebooks/{session_id}/dependencies
```

```json
{
  "package": "pandas>=2.0"
}
```

### Remove Dependency

```
DELETE /v1/notebooks/{session_id}/dependencies/{package_name}
```

### Get Environment State

```
GET /v1/notebooks/{session_id}/environment
```

### Start Environment Job

```
POST /v1/notebooks/{session_id}/environment/jobs
```

```json
{
  "action": "add",
  "package": "scikit-learn"
}
```

Actions: `add`, `remove`, `sync`, `import`.

### Export Requirements

```
GET /v1/notebooks/{session_id}/environment/requirements.txt
```

### Import Requirements

```
POST /v1/notebooks/{session_id}/environment/requirements.txt
```

## Workers

### List Workers

```
GET /v1/notebooks/{session_id}/workers
```

### Update Notebook Worker

```
PUT /v1/notebooks/{session_id}/worker
```

```json
{
  "worker": "my-worker-name"
}
```

### Update Cell Worker

```
PUT /v1/notebooks/{session_id}/cells/{cell_id}/worker
```

## Mounts

### Update Notebook Mounts

```
PUT /v1/notebooks/{session_id}/mounts
```

### Update Cell Mounts

```
PUT /v1/notebooks/{session_id}/cells/{cell_id}/mounts
```

## Runtime Config

### Get Notebook Runtime Config

```
GET /v1/notebooks/config
```

Returns deployment mode, available Python versions, and default paths.

## Core API

### Materialize

```
POST /v1/materialize
```

```json
{
  "inputs": ["file:///warehouse#db.events"],
  "transform": {
    "executor": "scan@v1",
    "params": { "columns": ["id", "value"] }
  },
  "mode": "stream",
  "name": "my_result"
}
```

### Get Stream

```
GET /v1/streams/{stream_id}
```

Returns Arrow IPC stream.

### Health

```
GET /health
```

### Metrics

```
GET /metrics
GET /metrics/prometheus
```
