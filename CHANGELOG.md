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

### Changed

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
