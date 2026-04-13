# Strata

[![CI](https://github.com/forge-labs-dev/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/forge-labs-dev/strata/actions/workflows/ci.yml)
[![Pre-commit](https://github.com/forge-labs-dev/strata/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/forge-labs-dev/strata/actions/workflows/pre-commit.yml)
[![Docker](https://github.com/forge-labs-dev/strata/actions/workflows/docker.yml/badge.svg)](https://github.com/forge-labs-dev/strata/actions/workflows/docker.yml)

**Content-addressed notebooks for ML and data workflows.**

Strata Notebook is an interactive notebook where every cell output is an
artifact. Same code + same inputs = instant cache hit. Change one cell,
and only that cell and its dependents re-execute — everything else is
served from the artifact store in milliseconds.

**Try it:** [strata-notebook.fly.dev](https://strata-notebook.fly.dev) (hosted preview, no account needed)

**Docs:** [forge-labs-dev.github.io/strata](https://forge-labs-dev.github.io/strata/)

## Quick Start

```bash
# Docker (recommended)
docker compose up -d --build
# Then open http://localhost:8765

# Or from source
uv sync
cd frontend && npm ci && npm run build && cd ..
uv run strata-server
# Then open http://localhost:8765
```

## Notebook Features

- **Content-addressed caching** — same code + same inputs = instant cache hit, zero recomputation
- **Automatic dependency tracking** — DAG built from variable analysis, no manual wiring
- **Cascade execution** — change upstream code, downstream cells auto-invalidate
- **Distributed workers** — annotate `@worker gpu-fly` and the cell dispatches to a remote GPU
- **Prompt cells** — LLM-powered cells with `{{ variable }}` template injection
- **AI assistant** — streaming chat with conversation memory, agent mode for autonomous notebook building
- **Environment management** — per-notebook Python venvs via uv, isolated from each other
- **Rich outputs** — DataFrames, matplotlib plots, markdown, images
- **Cell operations** — reorder, duplicate, fold, keyboard shortcuts
- **Headless runner** — `strata run ./my-notebook` for CI and scheduled execution

## The Cache Advantage

Every notebook platform re-executes from scratch when you change one cell.
Strata doesn't. The artifact store deduplicates by provenance hash —
if the code and inputs haven't changed, the result is served instantly.

```
First run:     load data (10s) → clean (3s) → train (20s) → evaluate (1s)  = 34s
Change model:  load data (✓)   → clean (✓)  → train (20s) → evaluate (1s)  = 21s
Re-run:        load data (✓)   → clean (✓)  → train (✓)   → evaluate (✓)   = <1s
```

This is not a feature bolted on — it's the architecture. Every cell
execution is a `materialize(inputs, transform) → artifact` operation.
The cache is correct by construction because it's keyed on content, not
time.

## Distributed Execution

Each cell can declare which worker it runs on via a single annotation:

```python
# @worker my-gpu
embeddings = model.encode(abstracts, batch_size=256)
```

You define workers in `notebook.toml` — each one points at an HTTP
endpoint that implements the Strata executor protocol. A worker can be
a GPU box on RunPod, a DataFusion cluster on Fly, a beefy EC2 instance,
or anything else that speaks HTTP. The notebook routes the cell to the
declared worker at execution time, and the UI shows a live
"dispatching → my-gpu" badge while it runs.

No deployment code, no infrastructure glue. Bring your own compute,
one annotation per cell.

---

## Strata Core

The notebook is built on **Strata Core**, a standalone materialization
and artifact layer that can be used independently as a Python library
and REST API:

```python
from strata import StrataClient

client = StrataClient()
artifact = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={"executor": "scan@v1", "params": {"columns": ["id", "value"]}},
)
table = client.fetch(artifact.uri)  # Arrow table, cached by provenance
```

Core provides: provenance-based deduplication, immutable versioned
artifacts, lineage tracking, Iceberg table scanning with row-group
caching, pluggable blob storage (local/S3/GCS/Azure), multi-tenancy,
trusted proxy auth, and an executor protocol for external compute.

**[Core documentation →](https://forge-labs-dev.github.io/strata/getting-started/core/)**

---

## Architecture

```
┌─────────────────────────────────────────────┐
│ Notebook UI (Vue.js + WebSocket)            │
│ cells, DAG view, AI assistant, workers      │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│ Notebook Backend (FastAPI)                  │
│ session, cascade, executor, prompt cells    │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│ Strata Core                                 │
│ materialize, artifacts, lineage, dedupe     │
└─────────────────────────────────────────────┘
```

The notebook is an orchestration layer over Core. It decides what to
run next (cascade planning, staleness tracking). The cell harness is an
executor. Core decides whether results already exist and persists them.

## Development

```bash
uv sync                          # Install deps + build Rust extension
uv run pytest                    # Run all tests
pre-commit run --all-files       # Lint + format
cd frontend && npm run dev       # Frontend dev server (hot reload)
```

## License

Apache 2.0
