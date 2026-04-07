# Notebook Concepts

## Architecture

Strata Notebook is a content-addressed compute graph over Python. Every cell output is an artifact, and every cell execution is a `materialize(inputs, transform) → artifact` operation.

```
┌─────────────────────────────────────────────┐
│ Notebook UI (Vue.js + WebSocket)            │
│ (cell editing, run buttons, DAG view)       │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│ Notebook Backend (FastAPI + WebSocket)       │
│ (session mgmt, cascade planner, executor)   │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│ Strata Artifact Store                       │
│ (SQLite metadata + blob storage, provenance │
│  dedup, lineage)                            │
└─────────────────────────────────────────────┘
```

The notebook is an **orchestration layer** — it decides what to run next. The cell harness is an **executor** — it runs Python code. The artifact store decides whether a result already exists and persists it.

## Notebook File Format

Each notebook is a directory on disk:

```
my_notebook/
├── notebook.toml          # Metadata: ID, name, cell list
├── pyproject.toml         # Python dependencies (uv-managed)
├── uv.lock                # Locked dependencies
├── cells/
│   ├── a1b2c3d4.py        # Cell source files
│   └── e5f6g7h8.py
└── .strata/
    └── artifacts/
        ├── artifacts.sqlite       # Artifact metadata
        └── blobs/                 # Serialized cell outputs
```

`notebook.toml` defines the notebook identity and cell ordering:

```toml
notebook_id = "f7bd9094-..."
name = "my_analysis"

[[cells]]
id = "a1b2c3d4"
file = "a1b2c3d4.py"
language = "python"
order = 0

[[cells]]
id = "e5f6g7h8"
file = "e5f6g7h8.py"
language = "python"
order = 1
```

## DAG and Variable Analysis

Each cell's source code is analyzed via Python's AST to extract:

- **Defines** — top-level variable assignments (`x = 1`, `df = pd.read_csv(...)`)
- **References** — free variables used but not defined in this cell

The DAG builder connects references to producers:

- The **last cell** that defines a variable is its producer (handles shadowing)
- Edges flow from producer cells to consumer cells
- **Cycle detection** prevents circular dependencies

The DAG is rebuilt automatically on every cell source change.

## Cell Execution Flow

When you run a cell, this happens:

1. **Compute provenance hash**: `sha256(sorted_input_hashes + source_hash + env_hash)`
2. **Cache check**: Look up the hash in the artifact store → return immediately on hit
3. **Resolve inputs**: Load upstream variable artifacts into a temp directory
4. **Execute**: Spawn a subprocess running the cell harness in the notebook's venv
5. **Harness**: Deserializes inputs → `exec(source, namespace)` → serializes new variables
6. **Store outputs**: Each consumed variable becomes an artifact
7. **Broadcast**: WebSocket sends status, output, and console messages to the UI

## Caching and Provenance

The provenance hash determines cache identity. It includes:

| Component | In hash? | Why |
|-----------|----------|-----|
| Source code | Yes | Different code = different result |
| Upstream artifact hashes | Yes | Different inputs = different result |
| Environment lockfile hash | Yes | Different packages = different result |
| Cell ID | No | Same code in a different cell = same result |
| Execution time | No | Same inputs should produce same output |

When you change a cell's source, its provenance hash changes, and all downstream cells become **stale**.

## Serialization

Cell outputs are serialized based on their Python type:

| Type | Format | File extension |
|------|--------|---------------|
| PyArrow tables, pandas DataFrames, numpy arrays | Arrow IPC | `.arrow` |
| Dicts, lists, scalars (int, float, str, bool, None) | JSON | `.json` |
| Everything else | Pickle | `.pickle` |

The content type is stored in the artifact metadata so the read side knows how to deserialize.

## Cascade Execution

When a cell's upstream dependencies aren't ready, the **cascade planner** generates an execution plan:

1. BFS backwards from the target cell to find all upstream cells needing execution
2. Returns cells in topological order with reasons (stale, missing, or target)
3. The frontend auto-accepts the cascade and executes cells sequentially

This means you can edit an early cell and run a downstream cell — Strata will automatically re-execute the full pipeline.

## Staleness

A cell is **stale** when its cached artifact no longer matches its current provenance. This happens when:

- Its source code changed
- An upstream cell's output changed
- The environment (uv.lock) changed

The **causality chain** explains why a cell is stale, tracing the change back to its root cause (e.g., "upstream cell X changed its source").

## Cell Status Lifecycle

```
idle → running → ready
                ↗
idle → running → error
```

- **idle** — never executed, or stale (needs re-execution)
- **running** — currently executing
- **ready** — last execution succeeded, artifact is current
- **error** — last execution failed
