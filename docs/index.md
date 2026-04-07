# Strata

**A Persistence Substrate and Notebook Runtime for Long-Horizon Computation**

Strata provides a single primitive — `materialize(inputs, transform) → artifact` — that ensures results are immutable, versioned, deduplicated, and traceable. It sits below orchestration and outside execution.

<div class="grid cards" markdown>

-   :material-notebook:{ .lg .middle } **Strata Notebook**

    ---

    Interactive notebook with content-addressed caching, automatic dependency tracking, and cascade execution.

    [:octicons-arrow-right-24: Notebook Quickstart](getting-started/notebook.md)

-   :material-cube-outline:{ .lg .middle } **Strata Core**

    ---

    Programmatic `materialize()` API with artifact caching, lineage, and executor integration.

    [:octicons-arrow-right-24: Core Quickstart](getting-started/core.md)

</div>

**Try it now:** [strata-notebook.fly.dev](https://strata-notebook.fly.dev) — a small hosted preview, no account needed.

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

```bash
# Docker (recommended)
docker compose up -d --build
# Then open http://localhost:8765

# Or from source
uv sync
uv run strata-server
```

## Status

Strata is currently in **alpha**. Both surfaces (Core and Notebook) are functional but the API may change before 1.0.
