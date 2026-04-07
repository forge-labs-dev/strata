# Strata Design Status

This document is the consolidated status view for Strata's design docs.

Use it to answer two questions quickly:

1. what is already implemented in the codebase today
2. what is still on the roadmap

The detailed design docs remain useful, but some of them were written before
implementation landed. When a detailed design doc contains older milestone or
pre-implementation language, treat this document as the authoritative snapshot
for current status.

## Status Legend

- **Implemented**: shipped and generally reflected in the current codebase
- **Partially implemented**: core design is landed, but the doc still contains
  future work or historical planning language
- **Roadmap**: intentionally not shipped yet
- **Historical**: useful for context, but not the current roadmap source of truth

## Current Snapshot

Today Strata is best understood as two product surfaces sharing one runtime:

- **Strata Core**: implemented alpha surface around materialization, artifacts,
  provenance, and executor-backed computation
- **Strata Notebook**: implemented alpha surface around interactive notebook
  execution, environments, remote workers, and rich display outputs

The cleanest release target today is:

- **ready for alpha**: core API, local/personal notebook workflow
- **advanced / preview**: service mode, hosted personal-mode deployment,
  broader remote-worker operations
- **roadmap**: AI-native notebook execution, named notebook environments,
  richer display/rendering polish

## Design Areas

### Core Materialization API

- Doc: [docs/design/unified-materialize-api.md](design/unified-materialize-api.md)
- Status: **Implemented**

Implemented now:

- unified `POST /v1/materialize` model
- identity transform / Iceberg fetch through the same materialize plane
- artifact vs stream delivery distinction
- provenance-aware caching and named results

Remaining work:

- mostly incremental API and operational polish, not a missing core design slice

### Notebook Foundation

- Doc: [docs/design-notebook.md](design-notebook.md)
- Status: **Partially implemented**

Implemented now:

- notebook directory model with `notebook.toml` + `cells/`
- create / open / rename / delete notebook lifecycle
- cell execution, AST DAG analysis, provenance caching, and staleness tracking
- WebSocket-driven execution and run-all support
- notebook timing instrumentation and browser benchmark harness
- notebook environment panel, remote workers, mounts, and rich display outputs

Still on the roadmap from this design:

- published outputs
- lightweight assertions
- freeze cell
- artifact lineage UI
- reproducible bundle export
- collaboration / branching / scheduling / marketplace-style extensions

Notes:

- this is the broad umbrella notebook design doc
- some milestone language and divergence notes inside it are historical; use this
  status doc for the up-to-date roadmap summary

### Notebook Environments

- Doc: [docs/design-notebook-environments.md](design-notebook-environments.md)
- Status: **Partially implemented**

Implemented now:

- single notebook-owned environment model
- environment status and sync/rebuild visibility
- async environment jobs for add/remove/sync/import
- `requirements.txt` import/export
- best-effort `environment.yaml` import
- create-time Python version selection with requested vs runtime Python display

Still on the roadmap:

- post-creation Python version change as a controlled environment job
- named notebook environments
- environment registry in notebook metadata
- optional cell-level environment selection
- cross-environment execution rules in practice, not just in design

Near-term recommendation:

- keep consolidating the current single-environment model before building named
  environments

### Notebook Display Outputs

- Doc: [docs/design-notebook-display-outputs.md](design-notebook-display-outputs.md)
- Status: **Partially implemented**

Implemented now:

- persisted notebook display outputs separate from DAG artifacts
- PNG image rendering
- markdown rendering
- `display(...)` side effects
- `plt.show()` / `Figure.show()`
- ordered multiple visible outputs per cell

Still on the roadmap:

- `image/svg+xml`
- output action bar / output chrome polish
- markdown renderer polish
- large-artifact lazy-loading / preview behavior
- possible helper ergonomics beyond `Markdown(...)`

### Remote I/O and Remote Workers

- Doc: [docs/design-remote-io-and-workers.md](design-remote-io-and-workers.md)
- Status: **Partially implemented**

Implemented now:

- notebook-level and cell-level mounts
- local, embedded, direct HTTP, and signed/build-backed notebook workers
- service-mode worker policy and admin CRUD
- remote execution metadata persisted in notebook cell state
- reference notebook executor and end-to-end service-mode worker coverage

Still on the roadmap:

- broader production worker fleet management beyond the reference executor
- richer long-term worker operations data
- possible unification of notebook signed execution with the generic build plane
- stronger semantics for cacheable read-write mount behavior

### AI Integration

- Doc: [docs/design-ai-integration.md](design-ai-integration.md)
- Status: **Roadmap**

Not implemented yet:

- AI cell type
- LLM transform executor
- AI-assisted notebook authoring

Interpretation:

- this is still an exploratory design area, not current product scope

### Original Notebook Implementation Plan

- Doc: [docs/implementation-plan.md](implementation-plan.md)
- Status: **Historical**

Meaning:

- the milestone breakdown was useful while the notebook was being built
- much of M1-M6 has now landed in some form
- use the current design docs plus this consolidated status doc for active
  roadmap decisions rather than the old milestone checklist

## Recommended Reading Order

If you are trying to understand the current product, use this order:

1. [README.md](../README.md)
2. [docs/core-quickstart.md](core-quickstart.md) or
   [docs/notebook-quickstart.md](notebook-quickstart.md)
3. [docs/design-status.md](design-status.md)
4. the specific detailed design doc for the area you care about

## Next Active Roadmap Themes

The highest-value roadmap areas now are:

1. release polish and alpha hardening
2. notebook environment follow-through, especially controlled Python-version
   change and continued UX polish
3. display-output polish such as SVG and output chrome
4. remote-worker and service-mode operational maturity

The following are intentionally later:

- named notebook environments
- AI-native notebook execution
- collaboration / multi-user notebook semantics
