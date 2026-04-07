# Strata Notebook — Named Environments Design

## Overview

See [docs/design-status.md](design-status.md) for the consolidated status across
all design docs.

Strata notebooks currently run with **one Python environment per notebook**:

- notebook root `pyproject.toml`
- notebook root `uv.lock`
- notebook root `.venv`

That model is simple and coherent, and it should remain the default mental model.
This document defines a later extension: **named notebook environments** that remain
notebook-owned and preserve the notebook's content-addressed execution semantics.

Per-cell environment selection is intentionally **not** part of the near-term
implementation plan. The immediate priority is to consolidate the current
single-environment workflow and make it complete, observable, and reliable.

The goal is not to turn Strata into a generic multi-kernel notebook. The goal is to
support cases like:

- a lightweight default environment for data prep and exploration
- a heavier ML environment for a few training cells
- a compatibility environment for legacy package constraints

without breaking caching, staleness, or reproducibility.

Current status:

- the current single-environment notebook model is implemented and much more
  complete than when this doc was first written
- named environments remain roadmap work
- per-cell environment selection is still intentionally deferred

---

## Current State (April 2026)

What exists today:

- one notebook environment managed by `uv`
- environment status in the notebook UI
- dependency add/remove flows
- explicit environment sync
- runtime Python version is displayed after sync
- `requirements.txt` import/export
- best-effort `environment.yaml` import
- environment changes participate in notebook provenance and staleness

What does not exist yet:

- multiple named environments inside one notebook
- cell-level environment selection
- user-facing Python version selection
- cross-environment execution rules
- per-environment warm pools

This document defines that missing model before implementation starts.

Implementation decision for the next phase:

- do **not** implement per-cell environment selection yet
- do **not** treat named environments as the next product milestone
- consolidate the current one-environment notebook model first

---

## Goals

- Keep **one notebook-owned canonical environment format**:
  - `pyproject.toml`
  - `uv.lock`
  - notebook-managed `.venv`
- Support **multiple named environments** within a notebook.
- Preserve notebook guarantees:
  - provenance-based caching
  - explainable staleness
  - deterministic execution inputs
  - explicit environment identity in cache keys
- Make cross-environment behavior predictable instead of best-effort.
- Keep import/export compatibility (`requirements.txt`, `environment.yaml`) as
  adapters around the notebook environment model, not competing sources of truth.

## Non-Goals

- Jupyter-style arbitrary kernels
- Conda-first environment management
- mixing unrelated language runtimes in one notebook
- seamless transport of arbitrary Python objects across different environments
- collaborative multi-user environment editing
- per-cell environment selection in the next implementation phase

---

## Design Principles

1. **Consolidate before extending**

   The current single-environment notebook model must be complete and well-tested
   before named environments become an implementation target.

2. **Notebook-owned, not cell-owned**

   Environments belong to the notebook. Cells select from the notebook's registered
   environments; they do not embed full environment specs.

3. **Canonical source of truth stays `uv`-based**

   Every named environment is represented canonically as:
   - `pyproject.toml`
   - `uv.lock`
   - managed `.venv`

4. **Cross-environment execution is explicit**

   If a value is safe to move across environments, Strata allows it. If not, Strata
   rejects it with a clear error instead of silently degrading into runtime failures.

5. **Environment identity is part of provenance**

   Cache correctness matters more than opportunistic reuse.

6. **The current one-environment model remains first-class**

   A notebook with no explicit named environments is still a normal Strata notebook.

---

## Why Multiple Environments

One environment per notebook is the right default, but it becomes limiting when:

- the notebook needs mutually incompatible dependency stacks
- one part of the notebook needs heavy GPU/ML packages that most cells do not
- a few cells must validate behavior against an older library version
- service-mode remote workers need different dependency footprints for different
  workloads

The missing feature is not "many random env files." It is "a notebook-managed
registry of named environments with clear execution semantics."

---

## Near-Term Recommendation

The next implementation phase should **not** be named environments or per-cell
environment selection.

The next implementation phase should be consolidation of the current model:

- make the single notebook environment fully observable
- let the user choose notebook Python version explicitly
- ensure the runtime Python version reflects the actual notebook venv
- tighten sync/rebuild/error reporting
- make dependency changes clearly explain their notebook impact
- harden import/export compatibility
- keep the environment lifecycle well-covered by tests

Until that is stable, adding more environment dimensions will increase complexity
faster than user value.

---

## Proposed Model

### Near-Term Python Version Model

Python version selection belongs to the notebook environment, not to individual
cells in the current single-environment model.

Near-term product rules:

- the user should choose an initial Python version in the **New Notebook** flow
- the Environment panel should show the current notebook Python setting after
  creation
- the user may later change Python version, but only as a notebook-level
  environment rebuild operation
- no per-cell Python/version override exists

This keeps the current single-environment model coherent while still making Python
an explicit part of notebook setup.

If named environments are implemented later, Python selection becomes
**environment-scoped** inside the notebook's environment registry. In that future
model, each named environment owns its own requested/runtime Python pair.

### Requested Python vs Runtime Python

The model should distinguish between:

- **requested Python**: the notebook's configured Python minor version, such as
  `3.12` or `3.13` in the current single-environment model, or the selected
  version for a specific named environment in the future
- **runtime Python**: the actual interpreter version currently backing the synced
  notebook environment, such as `3.13.2`

The requested version is user intent and should drive environment creation or
rebuild. The runtime version is probed state and should be displayed for debugging,
trust, and support.

### Backend-Validated Availability

Python version selection must be constrained by what the backend can actually
provide.

That means:

- no unrestricted free-form version text box
- the backend should expose a bounded set of selectable versions, or a single
  fixed version if the deployment only supports one
- hosted/container deployments may legitimately expose Python as read-only if the
  runtime image only provides one interpreter

Strata should never present a Python choice that the current deployment cannot
honor.

### New Notebook Flow

The first place the user expresses Python intent should be the **New Notebook**
flow, not a manual edit to `pyproject.toml`.

The creation flow should collect:

- notebook path / name
- requested Python version

`pyproject.toml` should then be generated from that choice. It is the persisted
artifact of the selection, not the first place where the user makes it.

### Post-Creation Python Changes

Changing Python version after notebook creation should remain allowed, but only as
an explicit notebook-level environment mutation.

Changing Python version should:

- update the notebook's requested Python version
- rebuild the notebook `.venv`
- invalidate the warm pool
- recompute environment metadata and notebook staleness
- block cell execution until the environment operation finishes

This should be treated like any other environment mutation: explicit, observable,
and disruptive by design.

### Notebook-Level Environment Registry

Each notebook owns an environment registry:

- one required **default** environment
- zero or more additional named environments

Each environment has:

- a stable notebook-local name
- a canonical filesystem location
- dependency files (`pyproject.toml`, `uv.lock`)
- a synced venv
- environment status metadata

### Future Cell-Level Selection

Each cell has:

- no explicit environment: use notebook default
- or `environment = "<name>"`: use that named environment

This is a future extension, not the next implementation target. When it exists, it
should be an execution choice, not a different language mode.

### Environment Names

Names should be:

- notebook-local
- stable identifiers used in notebook metadata
- restricted to simple slug-like names, e.g. `default`, `ml`, `py311-legacy`

The name is user-facing and part of provenance metadata, but correctness comes from
the environment fingerprint, not the name alone.

---

## On-Disk Layout

### Compatibility-Preserving Layout

Existing notebooks should continue to work unchanged. To preserve that, the
**default** environment remains at the notebook root:

```text
my_analysis/
├── notebook.toml
├── pyproject.toml              # canonical spec for default env
├── uv.lock                     # canonical lock for default env
├── .venv/                      # default env venv
├── cells/
│   ├── load.py
│   ├── clean.py
│   └── train.py
└── .strata/
    └── envs/
        ├── ml/
        │   ├── pyproject.toml
        │   ├── uv.lock
        │   └── .venv/
        └── py311-legacy/
            ├── pyproject.toml
            ├── uv.lock
            └── .venv/
```

### Why Keep the Default Env at the Root

- existing notebooks do not need migration before the feature exists
- `uv sync` at the notebook root still works for the default environment
- repo tooling and local shell workflows still map naturally to the default env
- extra environments remain notebook-private infrastructure under `.strata/envs/`

---

## notebook.toml Changes

### Notebook-Level Environment Metadata

`notebook.toml` should describe the environment registry and the selected default:

```toml
[environment]
default = "default"

[environments.default]
path = "."
requested_python_version = "3.13"
runtime_python_version = "3.13.2"
lockfile_hash = "sha256:abc123..."

[environments.ml]
path = ".strata/envs/ml"
requested_python_version = "3.13"
runtime_python_version = "3.13.4"
lockfile_hash = "sha256:def456..."

[environments.py311-legacy]
path = ".strata/envs/py311-legacy"
requested_python_version = "3.11"
runtime_python_version = "3.11.11"
lockfile_hash = "sha256:789abc..."
```

For the current single-environment model, the near-term equivalent is:

- one notebook-level requested Python version
- one runtime Python version reported from the active `.venv`
- no secondary named environments yet

### Future Cell Metadata

Cells may optionally declare an environment override:

```toml
[[cells]]
id = "train_model"
file = "cells/train_model.py"
language = "python"
environment = "ml"
```

If omitted, the cell uses `environment.default`.

This field should not be added until named environments are implemented for real.

### Runtime-Derived Status

`notebook.toml` should continue to store lightweight environment fingerprints, not
full transient status like "syncing" or "failed". Runtime status belongs in session
state and API responses, not persistent metadata.

---

## Environment Status Model

Each environment should have live status including:

- name
- path
- requested Python version
- runtime Python version
- lockfile hash
- declared package count
- resolved package count
- sync state: `ready`, `syncing`, `error`, `missing`
- last synced time
- last sync error

This extends the current single-environment status model to a list/map keyed by
environment name.

---

## Execution Model

### Effective Environment

For any cell execution, Strata computes:

- effective environment name
- effective environment path
- effective requested Python version
- effective runtime Python version
- effective lockfile hash

That effective environment is part of the execution context alongside:

- worker selection
- mounts
- timeout
- source
- upstream inputs

### Local Execution

For local execution, the effective environment selects:

- which venv interpreter to use
- which warm pool namespace to use
- which dependency status to check before execution

### Remote Execution

For remote workers, the effective environment must be staged explicitly:

- direct HTTP executor: send the selected environment's spec/lock as part of the
  execution bundle
- signed/build-backed execution: the build key must include the selected
  environment identity and lockfile

Remote execution must not silently fall back to the default environment when a cell
selected a different one.

---

## Provenance and Caching

### Environment Identity in Provenance

Cell provenance must include environment identity. At minimum:

- effective environment name
- effective environment lockfile hash
- effective runtime Python version

The name is useful for explainability. The correctness inputs are the lockfile hash
and runtime Python version. Requested Python version may still be recorded in
metadata for UX/debugging, but it is not sufficient on its own for cache
correctness.

### Cache Implications

Two otherwise identical cell runs in different named environments are **not**
cache-compatible.

That means:

- artifacts created in `default` are distinct from artifacts created in `ml`
- a cell changing only its selected environment becomes stale
- changing an environment's lockfile invalidates cells that use that environment,
  but should not invalidate cells using other environments

### Artifact Metadata

Produced artifacts should record the effective environment so that:

- staleness explanations can say which environment changed
- cross-environment compatibility checks can inspect producer vs consumer env
- debugging is possible after notebook reopen or sync

---

## Warm Pool Model

Warm execution state must be partitioned by environment.

Minimum rule:

- one warm pool namespace per `(worker, environment)` pair

Examples:

- local worker + `default`
- local worker + `ml`
- remote worker `gpu-http` + `ml`

Environment changes should invalidate the warm pool only for the affected
environment, not the entire notebook.

---

## Cross-Environment Compatibility Rules

This is the most important part of the design.

### Allowed Across Environments

Portable data artifacts may cross environments:

- Arrow / tabular artifacts
- JSON-compatible values
- other explicitly portable value formats

These are treated as data, not live Python runtime state.

### Blocked Across Environments

The following must be rejected across different environments:

- `module/cell` exported functions and classes
- `module/cell-instance`
- generic `pickle/object` values
- imported module objects
- any artifact format whose meaning depends on Python runtime identity

These values may still work within the same environment, but they are not safe
cross-environment.

### Error Behavior

When a downstream cell consumes an upstream artifact from a different environment and
the artifact is not portable, execution should fail with an explicit message, for
example:

> `Cell train_model uses environment "ml", but upstream value "model" was produced in
environment "default" as module/cell-instance. Only portable data artifacts may
cross environment boundaries.`

This should appear as a notebook execution error, not a low-level deserialization or
`NameError`.

### Compatibility Matrix

| Artifact kind                    | Same env | Different env |
| -------------------------------- | -------- | ------------- |
| Arrow / table                    | Allowed  | Allowed       |
| JSON / portable scalar/container | Allowed  | Allowed       |
| `module/cell`                    | Allowed  | Blocked       |
| `module/cell-instance`           | Allowed  | Blocked       |
| `pickle/object`                  | Allowed  | Blocked       |
| imported module object           | Allowed  | Blocked       |

This is intentionally conservative.

---

## UI Model

### Environment Panel

The current panel should evolve from "Packages" into a full environment manager:

- list of named environments
- default environment marker
- status card per environment
- create / clone / rename / delete env actions
- per-environment package editing
- per-environment `requirements.txt` export
- per-environment import from:
  - `requirements.txt`
  - `environment.yaml`

### Future Cell Infra Panel

Each cell should show:

- effective environment
- optional environment selector
- clear indication when the cell overrides the notebook default

### Future Cell Header

Cells using a non-default environment should show a compact badge, for example:

- `env: ml`
- `env: py311-legacy`

### Cross-Environment Feedback

If a cell graph crosses environment boundaries in an unsafe way, the UI should make
that obvious before or during execution:

- execution error text
- possible static warning when DAG analysis can predict the conflict

---

## Environment Operations

### Create Environment

Initial creation options:

- empty environment from a chosen Python version
- clone from another named environment
- import from `requirements.txt`
- import from `environment.yaml` (best effort)

### Rename Environment

Rename should update:

- notebook environment registry
- cell references in metadata
- on-disk `.strata/envs/<name>` path if safe

### Delete Environment

Deletion rules:

- cannot delete the default environment directly
- cannot delete an environment still referenced by cells
- deletion removes its venv and environment metadata

### Change Default Environment

Changing the notebook default should:

- update notebook metadata
- affect cells without explicit overrides
- mark affected cells stale

### Change Python Version

For the current single-environment model, changing Python version should be a
notebook-level environment operation.

Rules:

- expose only backend-supported versions
- show the requested version separately from the probed runtime version
- require an explicit apply/rebuild action
- block new cell execution while the change is in progress
- invalidate warm execution state after completion

In deployments with only one supported interpreter, the UI should show Python as
fixed instead of pretending there is a real choice.

---

## Import / Export Compatibility

### `requirements.txt`

Supported as:

- import into the current notebook environment today, or into a chosen named
  environment in the future
- export from the current notebook environment today, or from a chosen named
  environment in the future

It is a compatibility format, not the canonical runtime model.

### `environment.yaml`

Supported only as best-effort import into the current notebook environment today,
or into a chosen named environment in the future.

Rules:

- convert supported package dependency information into `pyproject.toml`
- do not silently overwrite the target environment's Python selection from an
  imported `python=...` pin
- if a Python pin is present, surface it as a suggestion or warning and require an
  explicit environment-level Python change if the backend supports it
- warn on unsupported Conda-specific features
- do not promise exact fidelity for non-Python packages or channel semantics

Strata should not claim that `environment.yaml` is a native first-class environment
format.

---

## Service Mode Implications

Named notebook environments remain **notebook-scoped** in both personal and service
mode.

What changes in service mode:

- worker catalogs may be server-managed
- auth and policy may restrict notebook operations
- remote execution is more common

What does not change:

- environment selection is still part of the notebook execution model
- environment identity still participates in provenance
- cross-environment compatibility rules are the same

This keeps notebook environments separate from server-wide worker/admin policy.

---

## Migration Plan

### Existing Notebooks

Any existing notebook becomes:

- one environment named `default`
- root `pyproject.toml`
- root `uv.lock`
- root `.venv`

No user-visible migration should be required before they create a second environment.

### First Additional Environment

When a second named environment is created:

- create `.strata/envs/<name>/`
- add environment registry entries to `notebook.toml`
- leave the root environment untouched as `default`

This keeps migration incremental.

---

## Rollout Phases

### Phase 1: Design and Constraints

- write the design
- align terminology with current environment panel and execution model
- define hard compatibility rules

### Phase 2: Consolidate the Current Single-Environment Model

- environment status and sync state are complete and trustworthy
- initial notebook creation includes Python version selection
- notebook-level Python changes are explicit and safe
- runtime Python version reflects the actual notebook venv
- environment sync/rebuild operations are explicit
- dependency mutations report notebook impact clearly
- `requirements.txt` and `environment.yaml` compatibility is well-tested
- local, remote, and reopen flows remain coherent

### Phase 3: Backend Data Model for Named Environments

- notebook environment registry in metadata/session state
- environment status API for multiple envs
- provenance and artifact metadata updates

### Phase 4: Local Execution

- create/clone/delete environments
- local execution + warm pool partitioning
- staleness and cache correctness

### Phase 5: Remote Execution Parity

- direct HTTP executor parity
- signed/build-backed parity
- remote environment identity in execution metadata

### Phase 6: Optional Cell-Level Selection and UX Polish

- full environment panel UX
- optional cell-level environment selection
- requirements/environment.yaml per-environment actions
- clearer cross-environment warnings/errors

---

## Open Questions

1. Should environment creation default to cloning `default`, or to an empty env?
2. Do we want static DAG warnings for blocked cross-environment object/code edges, or
   only execution-time errors in the first implementation?
3. Should the default environment remain physically at notebook root forever, or only
   as a compatibility layer during migration?
4. How much remote executor/build caching can be safely shared across environments
   with identical lockfile hashes but different logical names?

---

## Recommendation

Do not implement per-cell environment selection now.

The next implementation step should be **single-environment consolidation**, not
named-environment rollout. The backend data model for multiple environments should
wait until the current environment workflow is fully solid.

The most important rule to preserve is simple:

**Portable data may cross environment boundaries. Live Python runtime state may not.**
