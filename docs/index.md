# Strata

**A Persistence Substrate and Notebook Runtime for Long-Horizon Computation**

Strata provides a single primitive — `materialize(inputs, transform) → artifact` — that ensures results are immutable, versioned, deduplicated, and traceable. It sits below orchestration and outside execution.

**[Try it now](https://strata-notebook.fly.dev)** — a small hosted preview, no account needed.

---

## Two Surfaces

### Strata Notebook

Interactive notebook with content-addressed caching, automatic dependency tracking, and cascade execution. Each cell output is an artifact. Change upstream code, and downstream cells automatically invalidate.

[:octicons-arrow-right-24: Notebook Quickstart](getting-started/notebook.md){ .md-button }

### Strata Core

Programmatic `materialize()` API with artifact caching, lineage, and executor integration. Same runtime as the notebook, exposed as a Python client and REST API.

[:octicons-arrow-right-24: Core API Quickstart](getting-started/core.md){ .md-button }

---

## Why Strata?

Long-horizon workflows (AI agents, data pipelines, evaluation loops) share these properties:

- **Expensive** — LLM calls, embeddings, large scans
- **Iterative** — evaluate, refine, repeat
- **Branching** — explore multiple variants
- **Failure-prone** — crashes, retries, restarts are normal

What breaks first is not compute — it's **state**. Strata makes state explicit and durable.

## The Layering Model

```
┌─────────────────────────────────────────────┐
│ Orchestration Layer                         │
│ (DAGs, agents, control flow, retries)       │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│ Executors / Compute Engines                 │
│ (SQL engines, ML jobs, LLMs, feature code)  │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│ Strata                                      │
│ (materialize, artifacts, lineage, dedupe)   │
└─────────────────────────────────────────────┘
```

- **Orchestrators** decide what to run next
- **Executors** decide how to compute
- **Strata** decides whether it already exists and persists it

## Quick Start

=== "Docker"

    ```bash
    docker compose up -d --build
    ```

    Then open [http://localhost:8765](http://localhost:8765).

=== "From source"

    ```bash
    uv sync
    uv run strata-server
    ```

    Then open [http://localhost:8765](http://localhost:8765).

See [Installation](getting-started/installation.md) for full details.

## Status

Strata is currently in **alpha**. Both surfaces (Core and Notebook) are functional but the API may change before 1.0.
