# Strata Notebook — Implementation Plan

This plan breaks the v1 scope into six milestones. Each milestone delivers a
runnable increment — you can demo at the end of every milestone, not just at
the end.

**What already exists:**

| Layer | Status | Reusable? |
|-------|--------|-----------|
| Artifact store (`artifact_store.py`) | Production-ready, SQLite + pluggable blob backends | **Yes** — provenance dedup, versioning, name pointers |
| Blob store (`blob_store.py`) | Local + S3/GCS/Azure | **Yes** — local backend is all we need for v1 |
| FastAPI server (`server.py`) | 4900 lines, `/v1/materialize` endpoint | **Partially** — reuse app skeleton, add notebook routes |
| Config (`config.py`) | Pydantic-based, env vars | **Yes** — extend with notebook-specific settings |
| Types (`types.py`) | `TransformSpec`, `MaterializeRequest/Response` | **Yes** — use directly for cell execution |
| Vue frontend prototype | Cells, CodeMirror, DAG sidebar, mock execution | **Yes** — build on top of it |
| TypeScript types (`notebook.ts`) | Full type definitions for cells, DAG, WS protocol | **Yes** — already aligned with design doc |

**What does NOT exist yet:**

- Notebook session layer (parse `notebook.toml`, manage lifecycle)
- Cell executor (subprocess execution with input injection + output capture)
- AST variable analyzer
- WebSocket server
- Warm process pool
- Inspect mode REPL
- Frontend ↔ backend integration (currently mock-only)

---

## M1: Static Notebook (parse, display, edit, save)

**Goal:** Open a notebook directory, see cells in the UI, edit them, save back
to disk. No execution. This proves the on-disk format works end-to-end.

### Backend

Create `src/strata/notebook/` package:

| File | Responsibility |
|------|---------------|
| `src/strata/notebook/__init__.py` | Package init |
| `src/strata/notebook/models.py` | Pydantic models for `notebook.toml` (NotebookMeta, CellMeta) |
| `src/strata/notebook/parser.py` | Parse `notebook.toml` + read cell `.py` files → NotebookState |
| `src/strata/notebook/writer.py` | Write changes back: update `notebook.toml`, write cell files |
| `src/strata/notebook/session.py` | NotebookSession class — holds state for one open notebook |

Add routes to FastAPI (new router, mounted at `/v1/notebooks`):

| Endpoint | Purpose |
|----------|---------|
| `POST /v1/notebooks/open` | Open a notebook directory → returns full state |
| `GET /v1/notebooks/{id}/cells` | List cells with source |
| `PUT /v1/notebooks/{id}/cells/{cell_id}` | Update cell source |
| `POST /v1/notebooks/{id}/cells` | Add a new cell |
| `DELETE /v1/notebooks/{id}/cells/{cell_id}` | Remove a cell |
| `PUT /v1/notebooks/{id}/cells/reorder` | Reorder cells |
| `PUT /v1/notebooks/{id}/name` | Rename notebook |

Add `src/strata/notebook/routes.py` for the FastAPI router.

### Frontend

- Replace the hardcoded mock notebook in `stores/notebook.ts` with a fetch
  from `POST /v1/notebooks/open`
- Wire cell edits to `PUT /v1/notebooks/{id}/cells/{cell_id}` (debounced)
- Add cell, remove cell, reorder → corresponding API calls
- Add a "New Notebook" flow that creates the directory + `notebook.toml` +
  `pyproject.toml`

### Tests

- `tests/notebook/test_parser.py` — round-trip: create dir → parse → modify → write → parse again
- `tests/notebook/test_routes.py` — REST endpoint integration tests

### Scaffold notebook

Create a sample notebook directory at `examples/demo_notebook/` with
`notebook.toml`, `pyproject.toml`, and 3-4 cell files so you can manually
test the full flow.

### Exit criteria

Open the sample notebook in the UI. See cells with syntax-highlighted source.
Edit a cell, refresh the page, see the edit persisted.

---

## M2: Cell Execution (subprocess, output capture)

**Goal:** Hit "Run" on a cell → it executes in the notebook's venv → you see
stdout and output data in the UI. No DAG yet — each cell runs independently.

### Backend

| File | Responsibility |
|------|---------------|
| `src/strata/notebook/executor.py` | `CellExecutor` — spawns `uv run python` in the notebook dir, sends cell source via a wrapper script, captures stdout/stderr and output variables |
| `src/strata/notebook/harness.py` | The Python script that runs inside the subprocess: receives cell source + inputs, executes it, serializes outputs to stdout as JSON/Arrow IPC |
| `src/strata/notebook/serializer.py` | Three-tier serialization: detect type → Arrow IPC / JSON / msgpack / pickle; deserialization counterpart |

The `harness.py` script is the bridge between the notebook server and the cell
code. For M2 it receives no inputs (no DAG yet). It:

1. Receives cell source via stdin (or a temp file)
2. Executes in a clean namespace
3. Inspects top-level variables defined after execution
4. Serializes each as the appropriate tier
5. Writes a JSON manifest + blob files to a temp directory
6. Server reads the manifest and stores results

Add execution route:

| Endpoint | Purpose |
|----------|---------|
| `POST /v1/notebooks/{id}/cells/{cell_id}/execute` | Run a single cell |

The response streams stdout incrementally (SSE or chunked), then returns the
final output.

### Frontend

- Wire the "Run" button in `CellEditor.vue` to the execute endpoint
- Show cell status transitions: `idle` → `running` → `ready` / `error`
- Display stdout/stderr in a console panel below the cell
- Display output data (table for Arrow/DataFrames, JSON viewer for dicts)

### Environment bootstrap

- On `POST /v1/notebooks/open`, run `uv sync` in the notebook directory
  (idempotent, usually <1s) to ensure the venv exists
- Store the venv path in `NotebookSession`

### Tests

- `tests/notebook/test_executor.py` — execute a cell that does `x = 1 + 1`, verify output
- `tests/notebook/test_serializer.py` — round-trip each tier (Arrow, JSON, pickle)
- `tests/notebook/test_harness.py` — end-to-end: source in → outputs out

### Exit criteria

Write `import pandas as pd; df = pd.DataFrame({"a": [1,2,3]})` in a cell,
hit Run, see the DataFrame rendered as a table below the cell.

---

## M3: AST Analysis + DAG Construction

**Goal:** The backend analyzes cell source to find defines/references, builds
the DAG, and the frontend shows real dependency edges. Cells know their inputs
and outputs.

### Backend

| File | Responsibility |
|------|---------------|
| `src/strata/notebook/analyzer.py` | AST-based variable analysis: extract `defines` and `references` per cell; identify leaf nodes; handle edge cases (`_private`, builtins, imports) |
| `src/strata/notebook/dag.py` | Build the variable-level DAG, project to cell-level edges, topological sort, detect cycles |

Integrate into the session lifecycle:

1. On notebook open: analyze all cells → build DAG → populate `cell.defines`,
   `cell.references`, `cell.upstreamIds`, `cell.downstreamIds`, `cell.isLeaf`
2. On cell source update: re-analyze the changed cell → rebuild affected DAG
   edges → push updated DAG to frontend
3. Add `GET /v1/notebooks/{id}/dag` endpoint (returns edges + metadata)

### Frontend

- Replace the regex-based variable extraction in `stores/notebook.ts` with
  backend-provided `defines`/`references` (keep regex as instant preview, but
  reconcile when backend responds)
- Update `DagView.vue` to render real edges from the backend
- Show input status indicators per cell (which inputs are ready/stale/missing)
- Show leaf badge on leaf cells

### Tests

- `tests/notebook/test_analyzer.py` — comprehensive: simple assignments,
  augmented assignments (`df["x"] = ...`), function/class defs, `_private`
  exclusion, builtins exclusion, star imports
- `tests/notebook/test_dag.py` — multi-cell DAG construction, cycle detection,
  leaf identification, re-analysis after edit

### Exit criteria

Create a 3-cell pipeline: `load_data` (defines `df`) → `clean` (reads `df`,
defines `cleaned`) → `explore` (reads `cleaned`). DAG sidebar shows the
correct dependency chain. `explore` shows as a leaf.

---

## M4: Artifact Storage + Provenance Caching

**Goal:** Cell outputs are stored as Strata artifacts. Re-running a cell with
unchanged inputs/source/env returns a cache hit. Opening a notebook shows
cached results instantly.

### Backend

This milestone wires the existing `artifact_store.py` and `blob_store.py`
into the notebook execution path.

| File | Changes |
|------|---------|
| `src/strata/notebook/executor.py` | After execution: compute provenance hash → call `artifact_store.create_version()` → store blob via `blob_store.put()`. Before execution: check `artifact_store.find_by_provenance()` → if hit, skip execution |
| `src/strata/notebook/provenance.py` | `compute_provenance_hash(input_hashes, source_hash, env_hash)` — the core hashing function |
| `src/strata/notebook/env.py` | Parse `pyproject.toml` + `uv.lock` → compute runtime lockfile hash (only runtime deps, not dev) |
| `src/strata/notebook/session.py` | On open: compute provenance for all cells (topological order) → compare with stored artifacts → set status (ready/stale/idle) |

Input injection for execution (this is where DAG meets execution):

1. When executing a cell, resolve its `references` from the DAG
2. For each input variable, look up the upstream cell's artifact
3. Deserialize and inject into the cell's namespace (via `harness.py`)
4. After execution, capture outputs for consumed variables only (demand-driven)

### Frontend

- Show cache hit indicator on cell output (`⚡ cached · 5ms`)
- Show `stale` status when provenance doesn't match
- On notebook open, display cached outputs immediately (no execution needed)
- Show artifact size in cell footer

### Staleness detection

Implement the topological status pass:

1. Walk cells in topological order
2. For each cell, compute current provenance hash
3. Compare with stored artifact's provenance hash
4. If match → `ready`; if mismatch → `stale` with reason:
   - `self` — cell source hash changed
   - `upstream` — an upstream input artifact changed
   - `env` — lockfile hash changed
5. Push status to frontend

### Tests

- `tests/notebook/test_provenance.py` — hash stability, ordering invariance
- `tests/notebook/test_cache_hit.py` — run cell, run again unchanged → cache hit
- `tests/notebook/test_staleness.py` — edit cell → downstream stale; change dep → all stale
- `tests/notebook/test_env.py` — lockfile hash computation, runtime-only filtering

### Exit criteria

Run a 3-cell pipeline. Close and reopen the notebook. All cells show `ready`
with cached outputs displayed instantly. Edit the middle cell's source → it
and its downstream cell show `stale`. Re-run → new artifacts created. Run
again → cache hit.

---

## M5: WebSocket + Cascade Execution

**Goal:** Real-time execution updates via WebSocket. Running a cell with stale
upstream inputs triggers a cascade prompt, then executes the dependency chain.

### Backend

| File | Responsibility |
|------|---------------|
| `src/strata/notebook/ws.py` | WebSocket handler: accept connections, dispatch messages, manage per-notebook rooms |
| `src/strata/notebook/cascade.py` | `plan_cascade(cell_id)` — walk DAG backwards, find cells that need to run, compute estimated duration; `execute_cascade(plan)` — run cells in topological order |

WebSocket message flow (implement the protocol from the design doc):

**Client → server:**
- `cell_execute` — run a cell (triggers cascade check)
- `cell_execute_cascade` — user confirmed cascade plan
- `cell_execute_force` — "Run this only" with stale inputs
- `cell_cancel` — cancel a running cell
- `cell_source_update` — debounced source change (triggers re-analysis)
- `notebook_sync` — reconnection, request full state

**Server → client:**
- `cell_status` — status changed (idle/running/ready/stale/error)
- `cell_output` — cell produced output data
- `cell_console` — incremental stdout/stderr
- `cell_error` — execution failed
- `dag_update` — authoritative DAG from backend AST
- `cascade_prompt` — "this cell needs N upstream cells to run first"
- `cascade_progress` — during cascade, reports which cell is running

Replace the REST execution endpoint with WebSocket-driven execution:
- `POST .../execute` becomes `cell_execute` WS message
- Stdout streams as `cell_console` messages
- Final output arrives as `cell_output` message
- Status transitions arrive as `cell_status` messages

### Frontend

- Create `composables/useWebSocket.ts` — WebSocket connection management,
  reconnection, message dispatch
- Replace REST-based execution in `useStrata.ts` with WS messages
- Show cascade prompt UI when server sends `cascade_prompt`
- Show cascade progress (which cell is running, how many left)
- Stream console output in real-time as `cell_console` messages arrive
- Debounced `cell_source_update` on editor changes → triggers backend
  re-analysis → `dag_update` reconciles frontend state

### Cascade UX

When the user runs a cell with stale inputs:

1. Server sends `cascade_prompt` with the plan (cells to run, estimated time)
2. Frontend shows a confirmation dialog: "This cell needs 3 upstream cells to
   run first. Estimated time: ~12s. [Run all] [Run this only] [Cancel]"
3. "Run all" sends `cell_execute_cascade`
4. "Run this only" sends `cell_execute_force` (result marked `stale:forced`)
5. During cascade, `cascade_progress` messages update the UI

### Tests

- `tests/notebook/test_ws.py` — WebSocket connect, send execute, receive
  status + output messages
- `tests/notebook/test_cascade.py` — cascade planning, topological execution
  order, cache skipping within cascade
- `tests/notebook/test_cancel.py` — cancel mid-execution, verify cleanup

### Exit criteria

Open a 4-cell pipeline. Edit the root cell. All downstream cells show `stale`.
Run the leaf cell → cascade prompt appears → confirm → watch cells execute in
order with real-time console output → all cells show `ready`.

---

## M6: Warm Process Pool + Inspect Mode + Immutability

**Goal:** Fast iteration (warm pool cuts execution overhead from ~1.5s to
~50ms), on-demand REPL for exploring artifacts, and runtime mutation detection.

### Backend

| File | Responsibility |
|------|---------------|
| `src/strata/notebook/pool.py` | `WarmProcessPool` — pre-spawns 1-2 Python processes with common imports; hands them to executor; replaces after use; drains on env change |
| `src/strata/notebook/inspect.py` | Inspect REPL session — spawns a process with a cell's inputs loaded; accepts eval expressions via WS; lazy artifact loading via `ArtifactProxy` |
| `src/strata/notebook/immutability.py` | Defensive copy on input injection (by tier); runtime mutation detection heuristic; warning generation |

Warm process pool integration:

1. On notebook open (after `uv sync`), spawn pool with common imports
2. `executor.py` requests a warm process instead of cold-spawning
3. After execution, kill the process and spawn a replacement (background)
4. On `uv.lock` change, drain pool and recreate

Inspect mode (WS messages):

- `inspect_open` → spawn process with cell's inputs loaded as lazy proxies
- `inspect_eval` → evaluate expression, return result
- `inspect_close` → kill the inspect process

Immutability contract:

1. In `harness.py`, inject inputs with defensive copies (by tier)
2. After execution, run mutation detection heuristic
3. If mutation detected, include warning in `cell_output`

### Frontend

- Add "Inspect" button on cells → opens a REPL panel
- REPL panel with input field, output history, auto-complete (future)
- Show mutation warnings in cell output when detected
- Show execution timing (to demonstrate warm pool benefit)

### Tests

- `tests/notebook/test_pool.py` — pool lifecycle, warm process reuse, env invalidation
- `tests/notebook/test_inspect.py` — open inspect, eval expression, close
- `tests/notebook/test_immutability.py` — mutation detection for DataFrames
  (`inplace=True`), lists (`.append()`), dicts

### Exit criteria

Run a cell that imports pandas → ~50ms overhead (not ~1.5s). Open inspect on
a cell with a DataFrame input → evaluate `df.describe()` → see result. Edit a
cell to use `df.drop(..., inplace=True)` → run → see mutation warning.

---

## Milestone Dependency Graph

```
M1 (static notebook)
 └→ M2 (cell execution)
     └→ M3 (AST + DAG)
         └→ M4 (artifacts + provenance)
             └→ M5 (WebSocket + cascade)
                 └→ M6 (warm pool + inspect + immutability)
```

Each milestone builds directly on the previous one. There is no parallelism
in the critical path, but within each milestone, backend and frontend work
can proceed in parallel once the API contract is agreed.

---

## Estimated Timeline

These are rough estimates assuming one developer working full-time:

| Milestone | Effort | Cumulative |
|-----------|--------|------------|
| M1: Static notebook | 3-4 days | ~1 week |
| M2: Cell execution | 4-5 days | ~2 weeks |
| M3: AST + DAG | 3-4 days | ~2.5 weeks |
| M4: Artifacts + provenance | 5-6 days | ~3.5 weeks |
| M5: WebSocket + cascade | 5-6 days | ~5 weeks |
| M6: Warm pool + inspect + immutability | 5-6 days | ~6 weeks |

M4 and M5 are the heaviest — they integrate the most pieces. M1-M3 are
comparatively fast because they don't touch the artifact store.

---

## What Comes After v1

Once M6 is complete, the v1 scope from the design doc is fully implemented.
The next priorities (v1.1) are:

1. **Causality inspector** — expose the staleness chain already computed in M4
2. **Execution profiling** — per-cell timing, cache hit/miss rates, artifact sizes (data already available from M4/M5)
3. **Run impact preview** — show upstream + downstream consequences before execution (uses cascade planner from M5)

These are all "expose what's already computed" features — they don't require
new primitives, just new UI surfaces and WS message types.
