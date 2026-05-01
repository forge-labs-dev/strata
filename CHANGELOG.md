# Changelog

All notable changes to Strata will be documented in this file.

The project is still in alpha. Entries here focus on user-visible changes and
release framing rather than exhaustive commit history.

## Unreleased

### Added

- notebook home/create/open flows with recent-notebook tracking
- notebook rename, delete, and duplicate-style management improvements in the UI
- notebook environment status, sync, import/export, and async environment jobs
- notebook Python version selection in the new-notebook flow
- inline notebook display outputs for:
  - PNG images
  - markdown
  - `display(...)` side effects
  - `plt.show()` / `Figure.show()`
  - ordered multiple visible outputs per cell
- local service-mode demo stack, smoke script, and deployment guide
- notebook create/open timing instrumentation and browser benchmark tooling
- markdown cell language for prose / documentation cells
- `library_cells` example notebook walking through cross-cell library code

### Changed

- cells that mix runtime work and library code (defs, classes, literal
  constants) can now share the library code across cells. The planner
  slices the cell's AST, keeps the shareable parts, and validates the
  slice with `symtable` to make sure each kept def/class is
  self-contained. Runtime values flow through the regular artifact
  path; previously a single `df = load()` line would block every def
  in the same cell from being shared.
- the `module_export_blocked` diagnostic now names the specific
  function and unresolved variable instead of the generic "top-level
  runtime state" message, so the fix is obvious.
- `from __future__ import annotations` now correctly relaxes
  cross-cell type-hint references (PEP 563 stringifies annotations,
  so the free-variable check drops them).
- the markdown renderer is now `markdown-it` + `DOMPurify` rather than
  hand-rolled, with consistent output between in-place cell preview
  and `Markdown(...)` display outputs
- the docs are now split into separate Strata Core and Strata Notebook
  quickstarts, with the root README acting as an umbrella landing page
- notebook create now bootstraps the initial environment asynchronously, which
  makes first open substantially faster
- notebook open/create flows reuse prefetched state and lazy-load secondary
  panels to reduce perceived latency
- Fly-hosted notebook defaults now use persistent notebook storage and a larger
  auto-extending volume configuration
- Docker builds now reuse uv and Cargo caches more effectively for faster local
  iteration

### Fixed

- service-mode session discovery/reconnect policy and related UX regressions
- reconnect metadata loss for remote execution state
- run-all only executing the first cell
- missing-package install UX in the cell output area
- local service-mode browser routing and notebook creation flow

## 0.1.0

- initial alpha release
