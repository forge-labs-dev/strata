# Strata Notebook — Design Spec

## Overview

**Strata notebooks are a content-addressed compute graph over Python with an
interactive notebook UX.**

Each cell is treated as a function over declared inputs: the runtime injects upstream
artifacts, executes the cell, and captures outputs. Results are cached by provenance
hash, served at memory-mapped speed via Arrow IPC, and invalidated automatically when
code, data, or environment changes. Cells can have side effects (print, network, disk),
but the caching and invalidation model only tracks what flows through the DAG. There
is no kernel. There is no hidden state. The graph is the notebook.

The system is designed so that a minimal implementation — cells, DAG, caching, and
execution — is sufficient to deliver value. All other features are incremental.

## Status (April 2026)

See [docs/design-status.md](design-status.md) for the consolidated shipped vs
roadmap view across all design docs.

Current state:

- the notebook foundation is largely implemented and usable in alpha
- create/open/rename/delete, execution, DAG/caching, environments, remote
  workers, and rich display outputs are all in the codebase
- this document is still the broad umbrella design, but some milestone and
  divergence details later in the file are historical

Main roadmap themes still coming out of this doc:

- published outputs
- lightweight assertions
- freeze cell / stricter execution modes
- lineage / bundle-export style extensions
- collaboration and other larger multi-user features

---

## Implementation Scope

This document describes the full design surface. Not all of it ships at once. This
section defines what's required for each milestone.

### v1 (must ship)

The core loop: write cells, run them, see cached results, understand staleness.

- **Cell execution** with provenance-based caching (Arrow IPC fast path)
- **AST-based DAG construction** (backend authoritative, frontend regex preview)
- **Artifact storage** (Arrow IPC + JSON/msgpack; pickle as fallback)
- **Staleness detection** with fine-grained reasons (`stale:self`, `stale:upstream`, `stale:env`)
- **Cascade execution** with prompt-based flow (upstream only)
- **Basic UI**: cells with CodeMirror, run button, output tables, DAG sidebar
- **Environment management** via `uv` (`pyproject.toml` + `uv.lock`)
- **Immutability contract** with defensive copy on input injection
- **Warm process pool** for fast iteration
- **WebSocket protocol** for real-time execution updates
- **Inspect mode** (on-demand REPL with lazy artifact proxies)

### v1.1 (ship soon after)

UX polish that makes the core loop more transparent and trustworthy.

- **Causality inspector** ("why is this stale?" / "why did this run?")
- **Execution & cache profiling** (per-cell timing, cache hit/miss, artifact size)
- **Run impact preview** (upstream + downstream consequences before execution)

### v2+ (when users ask for it)

Extensions that build on the core without changing it. Each is independent.

- **Published outputs** — named artifact endpoint, minimal (no scheduling, no SLAs)
- **Lightweight assertions** — `assert` failure = cell error, no custom UI
- **Freeze cell** — advanced escape hatch, explicitly opts out of DAG invalidation
- **Artifact lineage view** — recursive provenance tree UI
- **Reproducible bundle export** — `strata export` CLI
- **Multi-user security** — pickle trust model, per-principal ACLs
- **Remote executor pools** — GPU routing, executor pool configuration

### Explicitly not v1

These are designed in this document for architectural coherence, but should not
create pressure to implement before launch:

- Team deployment (auth, multi-tenancy, shared cache)
- Remote executors (HTTP protocol, executor pools)
- Schema stability for published outputs
- Assertion aggregation and custom assertion UI
- Bundle export with `--include-artifacts`
- CI mode polish (`--fail-fast`, `--stale-only`, `--allow-forced`)

---

A Strata notebook is a **directory** on disk that the UI presents as a single document.
Users never see the directory structure directly — they interact through the notebook UI.
The directory layout exists to give git, CI, and the Python toolchain something they
already understand.

---

## On-Disk Layout

```
my_analysis/
├── notebook.toml              # cell ordering, metadata, artifact refs
├── pyproject.toml             # standard Python project (uv manages this)
├── uv.lock                   # deterministic lockfile
└── cells/
    ├── load_data.py
    ├── clean.py
    ├── aggregate.py
    └── explore.py
```

### Why a directory?

| Concern            | Single-file (.ipynb)        | Directory                               |
| ------------------ | --------------------------- | --------------------------------------- |
| Git diffs          | JSON noise, merge conflicts | Per-cell .py diffs                      |
| Linting/formatting | Not possible                | `ruff check cells/` just works          |
| Testing            | Not possible                | `uv run pytest` just works              |
| Imports            | Not possible                | `from cells.clean import ...` works     |
| Environment        | Kernel spec (broken)        | `pyproject.toml` + `uv.lock` (standard) |
| Outputs            | Embedded (bloated)          | In Strata artifact store (external)     |

Users see none of this. They click "New Notebook," name it, and start writing cells.

---

## notebook.toml

The only Strata-specific file. Lightweight glue — no outputs, no large blobs.

```toml
[notebook]
id = "nb_a1b2c3d4"
name = "Q1 Revenue Analysis"
created_at = "2026-03-19T10:00:00Z"
updated_at = "2026-03-19T14:30:00Z"

# Ordered list of cells — this IS the display order
[[cells]]
id = "load_data"
file = "cells/load_data.py"
language = "python"

[[cells]]
id = "clean"
file = "cells/clean.py"
language = "python"

[[cells]]
id = "aggregate"
file = "cells/aggregate.py"
language = "python"

[[cells]]
id = "train_model"
file = "cells/train_model.py"
language = "python"
# Optional: execution target (omit for local default)
[cells.executor]
target = "auto"
resources = { gpu = 1, memory = "32gb" }

[[cells]]
id = "explore"
file = "cells/explore.py"
language = "python"

# Artifact references — maps cell ids to their last known artifact
# Only cells with consumed outputs have entries here
# This is what lets "open notebook, everything is already there" work
[artifacts]
load_data = "strata://notebooks/nb_a1b2/cells/load_data@v3"
clean = "strata://notebooks/nb_a1b2/cells/clean@v1"
aggregate = "strata://notebooks/nb_a1b2/cells/aggregate@v2"
# explore has no entry — it's a leaf, nothing consumes its output

# Environment fingerprint — hash of runtime dependencies from uv.lock
# Computed from the transitive closure of [project].dependencies only
# (dev deps excluded — see "What gets hashed: runtime deps only")
# Included in provenance hash for cache invalidation
[environment]
lockfile_hash = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
requested_python_version = "3.12"
runtime_python_version = "3.12.9"

# Cache retention policies (optional — sensible defaults if omitted)
[cache]
max_versions_per_cell = 5
max_age_days = 30
max_notebook_cache_mb = 2000

# Pinned artifact versions (optional — excluded from eviction)
[cache.pins]
# train_model = "v4"
```

In the current single-environment model, Python version is a **notebook-level**
environment setting, not a per-cell one. Users should choose it when creating a
notebook and may later change it only as an explicit environment rebuild operation.
The model distinguishes between:

- `requested_python_version`: the notebook's configured Python target
- `runtime_python_version`: the actual interpreter backing the synced notebook env

If named environments are added later, Python selection becomes environment-level
inside that registry rather than cell-level. The detailed evolution path for this
lives in [design-notebook-environments.md](./design-notebook-environments.md).

### What's stored vs. derived

| Stored in notebook.toml      | Derived at runtime                  |
| ---------------------------- | ----------------------------------- |
| Cell id, file path, language | DAG edges (from AST analysis)       |
| Cell ordering                | Cell status (from artifact store)   |
| Last known artifact URIs     | Defines/references (from AST)       |
| Environment fingerprint      | Provenance hashes                   |
| Notebook name, timestamps    | Whether a cell is a leaf (from DAG) |
| Cache policies, pins         | Outputs (from artifact store)       |

The DAG is **never serialized** — it's rebuilt from cell source code every time.
This means renaming a variable automatically updates the graph. No stale metadata.

---

## Cell Semantics

### One cell type

Every cell is the same thing: a unit of computation with inputs and outputs.

```
cell(upstream_artifacts...) → {variable_name: value}
```

There are no "types" of cells. Instead, a cell's role in the notebook is determined
entirely by its position in the DAG:

- **A cell that defines variables consumed by other cells** → its outputs are cached
  as Strata artifacts (because someone depends on them).
- **A cell that defines variables nobody reads (a leaf)** → its outputs are not cached.
  It runs, shows results in the UI, done. This is the "scratch" use case.
- **A cell that defines nothing at all (returns None)** → purely side-effect. Runs,
  displays print output, no artifacts. Also a leaf.

The UI labels leaf cells with a softer visual treatment (e.g. "leaf" badge, no artifact
history), but this is cosmetic. The backend treats all cells identically. The DAG
determines caching behavior, not metadata.

### Why no explicit cell types?

A "scratch cell" is just a cell whose output nobody reads. If you later add a downstream
cell that references one of its variables, it automatically becomes a cached cell. No
user action, no type change, no config update. The DAG handles it.

This eliminates an entire class of consistency issues:

- No `type` field in `notebook.toml` to get out of sync
- No "is this cell cached or not?" confusion — look at the DAG
- No need to "promote" a scratch cell to a transform cell
- No special naming conventions (`_scratch_01.py` is unnecessary)

### What gets cached

**Cached as Strata artifacts:** Variables that a cell defines AND that at least one
downstream cell references. These are the cell's "consumed exports."

**Not cached:** Variables defined but never referenced by another cell (leaf outputs),
cell-internal variables, side effects, print output.

**Demand-driven caching:** Strata only materializes what's consumed. If cell A defines
`model` but no other cell reads `model`, there's no artifact. The moment cell B adds
`model` to its references, Strata starts caching it. Remove the reference, caching stops.
The DAG is the cache policy.

### Example

```python
# cells/load_data.py
# No inputs — this is a root node
import pandas as pd

raw = pd.read_parquet("s3://bucket/events.parquet")  # internal
events = raw[raw.status == "active"]                  # ← consumed by "clean" → cached
```

```python
# cells/clean.py
# Inputs: events (from load_data) — injected by runtime

cleaned = events.dropna(subset=["user_id"])          # ← consumed by "aggregate" → cached
stats = {"rows": len(cleaned), "nulls_dropped": len(events) - len(cleaned)}  # ← consumed by "explore" → cached
```

```python
# cells/aggregate.py
# Inputs: cleaned (from clean)

revenue = (
    cleaned
    .groupby(cleaned.timestamp.dt.month)
    .amount.sum()
    .reset_index(name="total")
)
# revenue consumed by downstream "export" cell → cached
```

```python
# cells/explore.py
# Inputs: cleaned, stats (from clean) — but defines nothing anyone reads
# This is a LEAF — runs, displays output, no artifacts cached

print(stats)
cleaned.describe()           # shown in UI, not persisted
cleaned.amount.hist()        # shown in UI, not persisted
```

The `explore` cell is what you'd call "scratch" in other systems. But it's not special —
it's just a cell at the edge of the DAG. If tomorrow you add a cell that reads something
from `explore`, it starts getting cached automatically.

### Cell contract

The runtime:

1. Resolves inputs: which upstream cells' outputs does this cell reference?
2. Deserializes upstream artifacts into the cell's namespace
3. Executes the cell source code
4. Inspects what variables were defined
5. For variables consumed by downstream cells: serialize and store via `materialize()`
6. For variables not consumed: display in UI only, discard after session

### Immutability contract

**Rule: Inputs are read-only. Any assignment to a variable name creates a new value.**

This is the single most important correctness invariant in the system. Without it,
caching breaks.

**The problem:** Python allows mutation of objects in place. Consider:

```python
# cells/load_data.py
df = pd.read_parquet("events.parquet")

# cells/transform.py
df["x"] = df["x"] * 2    # mutates df in place — is this a new output or a side effect?
```

Is `transform` redefining `df` or mutating the upstream artifact? If Strata caches
`df` from `load_data`, and `transform` mutates it in place, the cached version is
wrong — it now contains the mutation. If another cell reads `df` from `load_data`,
it gets the mutated version. The DAG says `df` comes from `load_data`, but the value
has been silently changed by `transform`.

**The rule:** Strata treats this as **`transform` defining a new `df`**. The AST
sees `df["x"] = ...` as an assignment to `df` (augmented subscript assignment), so
`df` appears in `transform`'s defines list. Downstream cells that reference `df` get
routed to `transform`, not `load_data`.

But this only works at the AST level. At runtime, the mutation happens in place on
the same Python object. To make the semantics correct, **inputs are defensively
copied on injection:**

```python
# Runtime injects inputs as deep copies
namespace = {}
for var_name, artifact_uri in cell_inputs.items():
    value = deserialize(artifact_uri)
    namespace[var_name] = deep_copy_if_mutable(value)
```

This ensures that mutations inside a cell never affect the cached upstream artifact.

**Copy cost by tier — defensive copy is not always a deep copy:**

| Content type                          | Copy strategy      | Cost                                           | Why                                                                                                                        |
| ------------------------------------- | ------------------ | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `arrow/ipc` (DataFrame, Table)        | **No copy needed** | 0                                              | Deserialization already produces a new PyArrow/Pandas object. The cached blob on disk is untouched by in-memory mutations. |
| `json/object` (dict, list, scalar)    | Shallow copy       | ~microseconds                                  | Small objects. `copy.copy()` suffices — nested structures are rarely mutated in notebook workflows.                        |
| `msgpack/object`                      | Shallow copy       | ~microseconds                                  | Same as JSON tier.                                                                                                         |
| `pickle/object` (model, custom class) | `copy.deepcopy()`  | **Expensive** — can be 100ms+ for large models | This is the real cost. ML models, fitted pipelines, large custom objects hit this path.                                    |

**The pickle copy problem:** A 500MB scikit-learn model going through `deepcopy` can
take 200ms+ and double memory usage. For the common case (model is read-only in the
consuming cell), this is pure waste.

**Mitigation (day one):** If the consuming cell does not assign to the input variable
name (AST check: variable not in `defines`), skip the deep copy and inject the
original deserialized object. This is a **performance optimization with a bounded
safety tradeoff**: it is safe against reassignment (the AST check is sound for that),
but not safe against opaque mutation through method calls or callees that the AST
can't see. The runtime mutation heuristic (see below) catches some of these cases,
but not all. In practice, the common pattern — passing a model to `model.predict()`
— is read-only and benefits from the optimization. The uncommon pattern — calling a
method that mutates the model in place — is not caught, but also doesn't corrupt the
upstream cache (the deserialized object is a fresh load from the blob store, not the
cached copy itself).

**Mitigation (future):** Copy-on-write wrappers that intercept `__setattr__` and
`__setitem__` to lazily copy only when mutation is attempted. Or immutability
wrappers that raise on mutation — making the contract enforceable, not just advisory.

**What the AST can and cannot catch:**

| Pattern                                | AST detects?               | Behavior                  |
| -------------------------------------- | -------------------------- | ------------------------- |
| `df = df.dropna()`                     | Yes (assignment to `df`)   | `df` in defines — correct |
| `df["x"] = df["x"] * 2`                | Yes (subscript assignment) | `df` in defines — correct |
| `df.drop(columns=["x"], inplace=True)` | **No**                     | Mutation not detected     |
| `my_list.append(item)`                 | **No**                     | Mutation not detected     |
| `del df["x"]`                          | **No**                     | Mutation not detected     |
| `some_func(df)` (mutates inside)       | **No**                     | Mutation not detected     |

For the "No" cases, the mutation happens but Strata doesn't know about it. Because
inputs are defensively copied, this doesn't corrupt the upstream cache — but the
mutation is also not persisted as a new artifact. The cell's output (after the cell
finishes) is inspected for all top-level variables, and whatever `df` looks like at
that point is what gets cached. So mutations are captured in the output, just not
tracked as explicit defines in the DAG.

**Runtime mutation detection (heuristic, best-effort):**

Static analysis (AST) can't catch `inplace=True` or `.append()`. The runtime can
detect _some_ mutations — but not all. This is a heuristic warning system, not a
sound mutation tracker. After a cell finishes, the executor compares each input
variable's identity and a content sample against the defensive copy:

```python
for var_name, original in injected_copies.items():
    current = namespace.get(var_name)
    if current is original:
        # Same object — check if content mutated (expensive, sample-based)
        if was_mutated(original, snapshot):
            warnings.append(f"'{var_name}' was mutated in place without reassignment")
    # If current is a different object, that's a reassignment — AST should have caught it
```

If mutation is detected, the cell output includes a runtime warning:

```
⚠ 'df' was mutated without reassignment.
  The output is correct, but the DAG doesn't reflect this change.
  Consider: df = df.drop(...) instead of df.drop(..., inplace=True)
```

This is a warning, not an error. The output is still correct (the snapshot of `df`
at cell completion is what gets cached). But the warning makes the invisible visible
— the user understands why the DAG didn't update, and gets a concrete fix.

**Limitations:** This is a heuristic, not a guarantee. The content check samples the
first/last N rows of a DataFrame (~1ms) — a mutation in the middle of a large frame
may go undetected. For opaque objects, the check is skipped entirely. The goal is to
catch the common cases (`inplace=True`, `.append()`, `del df[col]`) and surface
actionable warnings, not to provide sound mutation tracking. Users who need strict
immutability enforcement should use strict mode (future), which rejects cells that
mutate inputs.

**What about `globals()` / `locals()` / dynamic attribute access?**

These are inherently opaque to static analysis. Strata does not attempt to handle
them. If a cell uses `globals()["df"] = ...` to define a variable, it won't appear
in the DAG. If a cell reads `getattr(module, name)` where `name` is a runtime
value, Strata can't track that dependency.

This is a deliberate choice. Python is a dynamic language; no static analysis can
be complete. Strata's AST analysis covers the 95% case (explicit assignments and
bare name references). The remaining 5% is the user's responsibility. This is the
same tradeoff every Python tool makes — mypy, ruff, IDEs all have the same boundary.

### Variable analysis

Variables are extracted via Python AST analysis (not regex — the frontend uses regex
as a quick approximation, but the backend must use AST for correctness):

- **Defines**: Top-level assignments (`x = ...`, `def f():`, `class C:`)
- **References**: Free variables not defined in the cell and not builtins
- **Private**: Variables starting with `_` are cell-local even if assigned at top level

The DAG is the bipartite graph: cell → defines variables → referenced by other cells.

#### Internal DAG vs. UI DAG

The **internal DAG** is variable-level: cell A defines `df`, cell B references `df`,
so there's an edge A→B through variable `df`. This granularity is necessary for
correctness — Strata needs to know exactly which variables flow between cells to
compute provenance hashes and determine what to cache.

The **UI DAG** is a cell-level projection. Users think in cells, not variables. The
sidebar shows nodes for cells and edges between cells, not individual variables. If
cell A defines both `df` and `stats`, and cell B reads `df` while cell C reads `stats`,
the UI shows two edges: A→B and A→C. Hovering an edge shows which variables flow
along it (e.g., "df" on A→B, "stats" on A→C).

```
Internal (variable-level):          UI (cell-level projection):

  load_data                           load_data
    ├─ df ──→ clean                      │
    └─ stats ──→ explore                 ├──→ clean
                                         └──→ explore
  clean                               clean
    └─ cleaned ──→ aggregate             └──→ aggregate
```

This projection is computed by the frontend from the backend's authoritative DAG
response (which includes per-cell `defines` and `references` lists). The frontend
collapses variable-level edges into cell-level edges for display and expands them
on hover or click for detail.

**Why this matters:** A variable-level DAG with 20 cells and 40 variables produces
a dense, unreadable graph. The cell-level projection keeps it scannable. But when
debugging "why is this cell stale?" or "what data flows into this cell?", the
variable-level detail is one click away.

#### Frontend regex vs. backend AST: handling divergence

The frontend uses regex for instant DAG preview as the user types. The backend uses
Python `ast` for the authoritative DAG. These will disagree in edge cases — augmented
assignments (`x += 1`), walrus operators (`:=`), tuple unpacking (`a, b = ...`),
`for` loop targets, `with ... as x`, etc.

**Design rule: the frontend DAG is advisory; the backend DAG is authoritative.**

The reconciliation protocol:

1. **On every cell save** (debounced ~500ms after typing stops), the frontend sends
   the cell source to `GET /v1/notebooks/{id}/dag`. The backend parses all cells
   with `ast`, returns the authoritative DAG (edges + defines/references per cell).
2. **The frontend replaces its local DAG** with the backend response. No merge, no
   diff — full replacement. The regex-derived DAG is only visible during the debounce
   window while the user is actively typing.
3. **If the backend parse fails** (syntax error), the frontend keeps its current DAG
   and shows a syntax error indicator on the cell. The DAG doesn't update until the
   code is valid Python again.

This means the DAG may "flicker" briefly — e.g., the user types `x = ` (regex sees
a define), then completes `x = foo(` (syntax error, backend rejects), then finishes
`x = foo(bar)` (backend confirms the define). The flicker is confined to the debounce
window and only affects the DAG sidebar, not execution. In practice, 500ms debounce
makes this nearly invisible.

**What the frontend regex should NOT attempt:** Anything beyond simple `name = ...`
patterns. The regex exists for perceived responsiveness, not correctness. Specifically:

- DO extract: `x = ...`, `def f(`, `class C(`
- DO NOT extract: `a, b = ...`, `x += 1`, `x: int = ...`, `for x in ...`
- DO NOT extract references (too error-prone without scope analysis)

For references, the frontend shows nothing until the backend responds. This avoids
the worst case: the frontend showing a dependency edge that doesn't exist, which
would confuse users about why a cell ran or didn't run.

#### DAG rebuild scaling

A full AST parse of all cells + DAG rebuild on every cell save is fine for typical
notebooks (5-30 cells). For large notebooks (100+ cells), the cost adds up:

- Python `ast.parse()` is fast (~1ms per cell), but 100 cells × defines/references
  extraction × topological sort × status recomputation could reach ~50-100ms.
- The real cost is the full `dag_update` WebSocket message with all cells — 100+
  cells with their defines/references/status is a non-trivial payload.

**Day-one approach:** Full rebuild on every save. Debounce at 500ms. This is correct
and simple. At 100 cells, the 50-100ms backend cost is well within the debounce
window.

**If scaling becomes a problem (future):**

1. **Incremental AST analysis:** Only re-parse the changed cell. Re-derive its
   defines/references. Walk the DAG to update edges and status for affected cells
   only (the changed cell + its downstream transitive closure).
2. **Delta `dag_update` messages:** Instead of sending the full DAG, send only the
   changed cells and edges. The frontend applies the delta to its local state.
3. **Lazy status recomputation:** Only recompute status for cells visible in the
   viewport. Cells scrolled off-screen get status on demand.

These are optimizations, not architectural changes. The data model supports
incremental updates — each cell's defines/references are independent, and the
topological sort can be updated incrementally by re-sorting only the affected subgraph.

### Cell output channels and inspection

Every cell has **two output channels**, plus an on-demand inspection mode:

**Channel 1: Console output (stdout/stderr)**

Everything the cell prints during execution — `print()` statements, logging, warnings,
progress bars, tracebacks. Captured by the executor and sent back to the UI.

- Displayed in a **collapsible panel** between the editor and the artifact output
- **Session-only** — not persisted to the artifact store
- Re-running the cell replaces the previous console output
- Useful for quick debugging: `print(df.shape)`, `print(type(model))`

```
┌─ Console ──────────────────── (collapsed by default) ──┐
│ {'total_rows': 112847, 'dropped': 14636}               │
│                                                         │
│       id    amount                                      │
│ count 112847 112847                                     │
│ mean  50423  156.32                                     │
│ std   29087  412.18                                     │
│ min   1      -49.50                                     │
│ max   100000 12000.00                                   │
└─────────────────────────────────────────────────────────┘
```

**Channel 2: Artifact output (exported variables)**

The variables consumed by downstream cells, materialized as Strata artifacts.
This is the main output area — tables, scalars, dicts rendered inline. Persisted,
cached, versioned.

The current implementation is still narrower than the long-term desired output
model. The design for richer cell display outputs — including persisted primary
display output, inline images/plots, and future markdown rendering — lives in
[design-notebook-display-outputs.md](./design-notebook-display-outputs.md).

**Channel 3: Inspect mode (on-demand REPL)**

When `print()` isn't enough and you need to interactively explore a cell's state.

**How it works:**

1. User clicks the "Inspect" button (🔍) on a cell that has been executed
2. Strata spins up a **temporary Python process** in the notebook's venv
3. All the cell's inputs are deserialized from cached artifacts into the namespace
4. The cell's source code is re-executed (or, if cached, the cell's own outputs are
   also loaded)
5. A small REPL appears at the bottom of the cell

```
┌─ Inspect ──────────────────────────────────────────────┐
│ >>> cleaned.dtypes                                     │
│ id           int64                                     │
│ user_id      object                                    │
│ event        object                                    │
│ amount       float64                                   │
│ timestamp    datetime64[ns]                             │
│ status       object                                    │
│ dtype: object                                          │
│                                                        │
│ >>> cleaned[cleaned.amount > 1000].shape                │
│ (4521, 6)                                              │
│                                                        │
│ >>> model.feature_importances_                         │
│ array([1.0])                                           │
│                                                        │
│ >>> _                                                  │
└─────────────────────────────────────────────────────────┘
```

**Key properties:**

- **No persistent kernel.** The process is created on demand and killed when the user
  closes the inspect panel. No kernel to manage, no zombie processes.
- **Fast startup.** Because inputs are Arrow IPC artifacts, deserializing the namespace
  takes milliseconds. You're not re-running the entire upstream chain — just loading
  cached data.
- **Read-only by default.** Inspect mode doesn't change any artifacts. You're exploring
  the state, not modifying it. Any variables you create in the REPL are ephemeral.
- **Works on any cell.** Even leaf cells — the runtime loads their inputs and re-executes
  the cell source to reconstruct the full namespace.
- **Not just variables.** You can call methods, run expressions, import libraries — it's
  a full Python REPL, just pre-loaded with the cell's context.

**Why this is better than Jupyter's approach:**

In Jupyter, the kernel holds all state all the time. That's what causes the "restart
kernel, lose everything" problem. In Strata, state is reconstructed on demand from
immutable artifacts. Close the inspector → process dies → nothing lost, because the
artifacts are still in the store. Open it again → same state reconstructed in
milliseconds.

The tradeoff: there's a cold start to spin up the process and load artifacts, vs.
Jupyter's instant access to an already-running kernel. But you never lose state, and
you never have to restart anything.

#### Inspect startup performance and lazy loading

The cold start has three phases:

| Phase                                  | Cost               | Mitigation                                              |
| -------------------------------------- | ------------------ | ------------------------------------------------------- |
| 1. Spawn Python process                | ~50-100ms          | One-time; reuse process if user inspects multiple cells |
| 2. Load input artifacts into namespace | Varies (see below) | Lazy loading                                            |
| 3. Re-execute cell source (if needed)  | Varies by cell     | Skip if cell's own outputs are cached                   |

Phase 2 is the bottleneck. For a cell with a 2GB DataFrame input, eager loading means
2GB of Arrow IPC read + memory allocation before the REPL is usable.

**Solution: lazy artifact proxies.**

Instead of deserializing all inputs eagerly, the inspect runtime injects **proxy
objects** that load-on-first-access:

```python
class ArtifactProxy:
    """Transparent proxy that loads the artifact on first attribute access."""
    def __init__(self, artifact_uri: str, content_type: str):
        self._uri = artifact_uri
        self._content_type = content_type
        self._loaded = None

    def __getattr__(self, name):
        if self._loaded is None:
            self._loaded = deserialize(self._uri, self._content_type)
        return getattr(self._loaded, name)

    def __repr__(self):
        if self._loaded is None:
            return f"<ArtifactProxy: {self._uri} (not yet loaded)>"
        return repr(self._loaded)
```

With lazy proxies, phase 2 costs ~1ms regardless of input size. The actual
deserialization happens when the user first references the variable in the REPL.
For Arrow IPC, this is still fast (~5ms mmap for most sizes). For large artifacts,
the user sees a brief pause on first access — but only for the variable they
actually touched, not all inputs.

**When to skip lazy proxies:** If the cell needs to be re-executed (phase 3 —
no cached outputs for the cell itself), all inputs must be eagerly loaded because
the cell source code will access them during execution. Lazy proxies only help when
the cell's own outputs are cached and we're reconstructing the full namespace for
exploration.

**Process reuse:** If the user inspects cell A, then clicks inspect on cell B, the
same Python process can be reused — clear the namespace, inject new proxies. This
avoids repeated process spawn overhead during an exploration session.

**When to use which channel:**

| Need                             | Use                   | Why                               |
| -------------------------------- | --------------------- | --------------------------------- |
| Quick peek at a value            | `print()` in the cell | Zero friction, see it on next run |
| Check types, shapes, basic stats | Console output        | Already there after execution     |
| Interactive exploration          | Inspect mode          | Full REPL with cell's namespace   |
| See a table or DataFrame         | Artifact output       | Rendered inline, cached           |
| Deep debugging with breakpoints  | External debugger     | Out of scope for day one          |

### Input status and readiness

Every cell has explicit inputs (variables it references from upstream cells). Each input
has a known state — because inputs are Strata artifacts, we always know whether they exist
and whether they're current. The UI shows this per-cell:

```
inputs: events ●  cleaned ●          all inputs cached and current → ready to run
inputs: events ●  cleaned ◐          cleaned is stale → can run but result may change
inputs: events ●  model ○            model has never been computed → cannot run
```

**Input states:**

| Symbol | State   | Meaning                                                      |
| ------ | ------- | ------------------------------------------------------------ |
| ●      | ready   | Artifact exists and provenance is current                    |
| ◐      | stale   | Artifact exists but provenance has changed (upstream edited) |
| ○      | missing | No artifact — upstream cell has never been executed          |
| ✕      | error   | Upstream cell failed                                         |

This gives users instant visibility into what's blocking a cell, without running anything.

#### Cell status vs. input status: fine-grained staleness

Input status (above) describes individual upstream artifacts. Cell status describes
the cell itself. These are related but distinct — and staleness has subcategories
that matter for the user's mental model.

**Cell status values:**

| Status           | Visual       | Meaning                                                                | User action                 |
| ---------------- | ------------ | ---------------------------------------------------------------------- | --------------------------- |
| `idle`           | gray         | Cell has never been executed                                           | Run it                      |
| `ready`          | green ●      | Artifact exists, provenance matches                                    | Nothing — output is current |
| `stale:self`     | yellow ◐     | Cell source was edited since last run                                  | Re-run this cell            |
| `stale:upstream` | orange ◐     | Cell source unchanged, but an upstream input is stale or was re-run    | Re-run (or cascade)         |
| `stale:env`      | orange ◐     | Environment (uv.lock) changed since last run                           | Re-run (all cells affected) |
| `stale:forced`   | yellow ◐⚠    | Ran with stale inputs ("Run this only") — result exists but is suspect | Re-run with fresh inputs    |
| `running`        | blue spinner | Currently executing                                                    | Wait                        |
| `error`          | red ✕        | Last execution failed                                                  | Fix code, re-run            |

The UI **does not** show all these as separate icons — that would be overwhelming.
Instead:

- The **cell badge** shows the coarse status: `ready` (green), `stale` (yellow/orange),
  `error` (red), `running` (blue), `idle` (gray).
- **Hovering** the badge shows the specific reason: "stale: cell source edited",
  "stale: upstream cell `clean` was modified", "stale: ran with stale inputs",
  "stale: environment changed".
- The **`stale:forced`** case additionally shows a persistent inline warning:
  `⚠ computed from stale inputs — results may be outdated` (as described in the
  cascade section). This is the only staleness subtype with a visible inline warning,
  because it's the only one where the output _exists but may be wrong_.

**Why distinguish `stale:self` from `stale:upstream`?** Because the user action
differs. `stale:self` means "you edited this cell, re-run it to see your changes."
`stale:upstream` means "you didn't touch this cell, but something above it changed —
you probably want to cascade." Conflating them forces users to inspect the DAG to
figure out what happened.

**Implementation:** The staleness reason is computed during the topological status pass.
For each stale cell, the server checks: (a) did the cell's own `source_hash` change?
→ `stale:self`. (b) did `lockfile_hash` change? → `stale:env`. (c) are any input
artifacts stale or have different provenance than when this cell last ran? →
`stale:upstream`. (d) does the artifact's provenance include stale input hashes? →
`stale:forced`. A cell can have multiple reasons; the server returns all of them and
the UI shows the most actionable one.

**How staleness is detected:** On notebook open (or after any cell edit), the server
does a single pass in topological order:

1. For each cell, compute the expected provenance hash:
   ```
   expected_hash = sha256(
       sorted(input_artifact_hashes)
       + cell_source_hash
       + lockfile_hash
   )
   ```
2. Compare against the stored provenance hash on the cell's artifact
3. Match → `ready`. Mismatch → `stale`. No artifact → `missing`.

Staleness propagates automatically. If `load_data` is stale, then `clean`'s expected
hash changes (because its input hash changed), so `clean` becomes stale too, and so
does everything downstream. One topological pass catches everything.

### Cascade execution

When a user runs a cell whose inputs are not all ready, Strata does NOT raise an error.
Instead, it offers to run the required upstream cells automatically.

**Behavior:** Prompt-then-cascade (default).

1. User hits Shift+Enter on `aggregate` cell
2. Strata checks inputs: `cleaned` is stale (upstream `clean` was edited)
3. UI shows cascade prompt:
   ```
   ┌─────────────────────────────────────────────────┐
   │  This cell needs 2 upstream cells to run first  │
   │                                                  │
   │  ◐ clean         ~0.8s   (stale)                │
   │  ● load_data     cached  (ready — skip)          │
   │                                                  │
   │  Estimated: ~0.8s  (1 cell to run, 1 cached)     │
   │                                                  │
   │  [Run all]  [Run this only]  [Cancel]            │
   └─────────────────────────────────────────────────┘
   ```
4. User clicks "Run all" → Strata runs `clean` then `aggregate` in order
5. Cached cells (like `load_data`) are skipped — their artifacts are already valid

**"Run this only" behavior:**

When the user clicks "Run this only" on a cell with stale or missing inputs, Strata
injects whatever artifacts currently exist (even if stale) and executes the cell.

- Stale inputs: the old artifact is deserialized into the namespace. The cell runs.
- Missing inputs: the cell gets a `NameError` — this is the one case where an error
  is unavoidable, because there's literally no data to inject.

The output is displayed with a **warning badge**:

```
⚠ computed from stale inputs — results may be outdated
```

The artifact IS stored, but its provenance hash reflects the stale input hashes. This
means:

- The cell's status shows as `stale:forced` — because its provenance includes
  stale inputs
- Downstream cells that depend on this output also see it as stale
- Staleness continues to propagate correctly through the DAG
- Re-running with fresh inputs later will produce a new artifact version with the
  correct provenance

**Downstream propagation of forced artifacts:**

A forced artifact is tainted — it was computed from inputs that may not reflect the
current state of the notebook. If a downstream cell silently consumes a forced
artifact and produces its own output, the taint propagates invisibly.

To prevent confusion, **downstream cells cannot auto-cascade through a forced
artifact.** If cell C depends on cell B, and B has status `stale:forced`, then:

- Running C with cascade will stop at B and show:
  ```
  ⚠ upstream cell "clean" was computed from stale inputs
  [Re-run clean first]  [Use anyway]  [Cancel]
  ```
- "Re-run clean first" triggers a cascade from B's stale inputs upward — the proper fix.
- "Use anyway" is a second explicit confirmation. The user has now acknowledged the
  taint twice (once at B, once at C). C's artifact is also marked `stale:forced`.
- In CI/headless mode with `--fail-fast`, forced artifacts cause the run to fail.
  Use `--allow-forced` to override.

**Visual treatment:** Forced artifacts get a distinctive visual — a striped/hatched
background on their output area (not just a text warning). This makes them impossible
to mistake for valid outputs during a quick scan of the notebook.

This is the right tradeoff: the user asked to run this specific cell, so give them
what they asked for. They can see the code works, inspect the output shape, verify
logic — all without waiting for the full upstream chain. The double confirmation for
downstream consumption prevents taint from spreading accidentally.

**Why not error?** Strata knows the full dependency graph. Raising a NameError
when the system already knows exactly which cell produces that variable is a waste
of the system's intelligence. Jupyter errors here because it has no DAG. Strata has one.

**Why not auto-cascade silently?** Because upstream cells might be expensive.
A user running a quick leaf cell shouldn't accidentally trigger a 30-minute model
training job three levels up. The prompt gives visibility and control.

**Power-user setting:** Auto-cascade mode skips the prompt and runs everything
automatically. Useful for "run all stale" workflows.

**Run All Stale:** A single action that walks the DAG in topological order and runs
every cell whose provenance hash has changed. This is the "open notebook from cold,
bring everything up to date" button. It shows a preview first:

```
Run All Stale: 3 of 5 cells need to recompute
  ◐ clean         ~0.8s
  ◐ aggregate     ~0.3s
  ◐ export        ~0.1s
  Estimated total: ~1.2s
```

**Execution order:** Always topological. Independent branches can run in parallel
(future optimization). A cell only starts when all its inputs are ready.

---

## Environment Management

### The problem

Jupyter: "which kernel am I using? what's installed? why does `import foo` fail on
my colleague's machine?" There is no reproducible environment story.

### The solution

Each notebook directory is a standard Python project. `uv` manages everything.

```toml
# pyproject.toml (standard, uv-compatible)
[project]
name = "q1-revenue-analysis"
requires-python = ">=3.12"
dependencies = [
    "pandas>=2.0",
    "pyarrow>=14.0",
    "strata-client>=0.1",
]

[project.optional-dependencies]
dev = ["pytest", "ruff"]
```

**uv.lock** is the exact lockfile — deterministic, cross-platform, fast to resolve.

### Lifecycle

| Action               | What happens                                       |
| -------------------- | -------------------------------------------------- |
| **New notebook**     | `uv init` + `uv add strata-client`                 |
| **Open notebook**    | `uv sync` (fast — usually a no-op)                 |
| **User adds import** | UI prompts to `uv add <package>`, lockfile updates |
| **Share notebook**   | Recipient runs `uv sync`, exact same environment   |
| **CI/testing**       | `cd my_analysis && uv run pytest`                  |

### Cache invalidation via environment

The hash of `uv.lock` is part of the provenance hash:

```
provenance = sha256(
    sorted(input_artifact_hashes)
    + cell_source_hash
    + lockfile_hash            ← this is the key
)
```

Change a dependency version → lockfile changes → provenance hash changes →
all downstream artifacts are automatically stale. Keep the same lockfile →
everything stays cached.

This is the correct behavior: if you upgrade `pandas` from 2.0 to 2.1,
your cached DataFrames might serialize differently, so they should be
recomputed. If nothing changed, serve from cache.

#### What gets hashed: runtime deps only

Hashing the entire `uv.lock` is too aggressive. Adding `ruff` to `[project.optional-dependencies.dev]`
would invalidate every cached artifact in the notebook, even though `ruff` never runs
in a cell. The hash must cover only packages that affect cell execution.

**The lockfile hash is computed from the resolved runtime dependency set:**

```python
def compute_lockfile_hash(lockfile: Path, pyproject: Path) -> str:
    """Hash only the packages that are in the runtime dependency closure."""
    runtime_deps = parse_runtime_deps(pyproject)  # [project].dependencies
    resolved = resolve_from_lockfile(lockfile, runtime_deps)  # transitive closure
    # Sort for determinism, include version + hash for each package
    entries = sorted(f"{pkg.name}=={pkg.version}@{pkg.hash}" for pkg in resolved)
    return sha256("\n".join(entries))
```

**What counts as "runtime":**

| Included in hash                     | Excluded from hash                    |
| ------------------------------------ | ------------------------------------- |
| `[project].dependencies`             | `[project.optional-dependencies].dev` |
| Transitive deps of the above         | `ruff`, `pytest`, `mypy`, etc.        |
| `requires-python` version constraint | Editor plugins, pre-commit hooks      |

**Edge case: optional dependency groups.** If a cell imports a package that's in an
optional group (e.g., `[project.optional-dependencies].ml = ["scikit-learn"]`), the
user needs to declare that group as runtime. We don't auto-detect this — the
`pyproject.toml` is the source of truth.

**Fallback:** If parsing the dependency closure fails (unusual lockfile structure,
custom uv configuration), fall back to hashing the entire `uv.lock`. This is the
safe-but-aggressive default. Log a warning so the user knows why adding `ruff`
invalidated their cache.

### Execution

The notebook server spawns cell execution via `uv run`:

```
uv run --directory my_analysis/ python -c "<cell_source>"
```

Or more precisely, the executor protocol:

1. Server sends cell source + input artifacts to the cell executor
2. Cell executor runs inside the notebook's venv (managed by `uv`)
3. Cell executor returns output artifacts
4. Server stores them via `materialize()`

This means each notebook has its own isolated environment. No kernel spec,
no conda env activation, no "which Python am I using?" confusion.

### Warm process pool: fast iteration without a kernel

The no-persistent-kernel design is correct for state management — but raw
process-per-execution has a friction cost. In a tight tweak→run→tweak loop, the
user edits a cell and hits Shift+Enter. Each execution spawns a new Python process
in the notebook's venv, imports libraries, deserializes inputs, runs the cell, and
exits. The import/setup phase can easily take 1-2 seconds for cells that use
`pandas`, `numpy`, or `sklearn`.

Jupyter solves this by keeping a kernel alive — imports persist. But that's also why
Jupyter has state corruption. Strata can get most of the benefit without the downside
by using a **warm process pool**.

**How it works:**

1. When a notebook is opened, Strata pre-spawns 1-2 Python processes in the
   notebook's venv. These processes import the notebook's top-level dependencies
   (parsed from `pyproject.toml`) and then block, waiting for work.
2. When a cell executes, Strata picks a warm process from the pool, sends it the
   cell source + serialized inputs via stdin, and receives outputs via stdout.
3. After execution, the process is **not reused** — it's killed and replaced with a
   fresh warm process. This preserves the "no shared state between executions"
   guarantee.
4. The fresh replacement process starts importing immediately (in the background),
   so it's warm again by the time the user's next Shift+Enter arrives.

**What you get:**

| Without pool                                                                     | With pool                                              |
| -------------------------------------------------------------------------------- | ------------------------------------------------------ |
| `spawn` (~50ms) + `import pandas` (~800ms) + `import sklearn` (~400ms) + execute | `execute` (process already spawned and imports cached) |
| ~1.5s overhead per cell                                                          | ~50ms overhead per cell                                |

**What you don't get:** Shared state. Each execution still starts from a clean
namespace with only the declared inputs. No variable leakage between cells, no
"restart kernel" problem. The warm pool is purely an optimization for process
spawn + import time.

**Pool invalidation:** If `uv.lock` changes (user added a dependency), the pool is
drained and re-created with the new environment. This is the same "uv sync" that
happens anyway.

**Cost:** 1-2 idle Python processes per open notebook, each consuming ~50-100MB of
RAM (with common data science imports). Acceptable for local development. For team
servers with many concurrent notebooks, the pool size can be configured down to 0
(disabling the optimization).

---

## Provenance & Caching Model

### How it maps to Strata

Each cell execution is a `materialize()` call:

```python
artifact = strata.materialize(
    inputs=[
        upstream_artifact_uri_1,
        upstream_artifact_uri_2,
    ],
    transform={
        "executor": "notebook-cell@v1",
        "params": {
            "source_hash": sha256(cell_source),
            "cell_id": "clean",
        }
    },
    env_hash=sha256(uv_lock),       # first-class, not buried in params
)
```

Strata computes:

```
provenance_hash = sha256(sorted(input_hashes) + transform_spec + env_hash)
```

If this hash already exists → **instant cache hit**, no execution needed.

### When things get recomputed

| Change                            | Effect                                      |
| --------------------------------- | ------------------------------------------- |
| Edit cell source                  | This cell + all downstream cells recompute  |
| Edit upstream cell                | This cell + all downstream cells recompute  |
| Change `uv.lock`                  | ALL cells recompute (env hash changed)      |
| Change nothing                    | Everything cached                           |
| Open notebook next week           | Everything cached (artifacts are in Strata) |
| Colleague opens same notebook     | Cache hit if same Strata server             |
| Add downstream consumer to a leaf | Leaf cell starts being cached               |
| Remove all consumers of a cell    | Cell becomes a leaf, caching optional       |

### Open notebook flow

1. Parse `notebook.toml` → get cell list and last known artifact URIs
2. `uv sync` → ensure environment is ready (usually instant)
3. Compute current provenance hash for each cell (topological pass)
4. Compare against stored artifact provenance hashes
5. Cells where hashes match → status = `ready` (green), output loaded from artifact store
6. Cells where hashes differ → status = `stale` (yellow), needs re-run
7. Leaf cells without artifacts → status = `idle`
8. User sees the notebook with all cached results already displayed

No execution needed to "restore" a notebook. This is the key advantage over
every existing notebook system.

### Cell version comparison

Every time a cell runs, it produces a new artifact version. Strata keeps the full
history: `clean@v1`, `clean@v2`, `clean@v3`... The named artifact pointer
(`clean@latest`) always points to the most recent version. The DAG only ever has
**one active version per cell** — no parallel timelines, no branching.

But users can **compare any two versions** of a cell side by side:

**How it works:**

1. User clicks a cell's artifact history in the sidebar (or right-clicks → "Compare versions")
2. Version comparison panel opens, showing two columns:

```
┌─────────────────────────────────────────────────────────────────────┐
│  Compare: clean                                          v1 ↔ v2   │
├─────────────────────────────┬───────────────────────────────────────┤
│  v1 (active)                │  v2 (previous)                       │
├─────────────────────────────┼───────────────────────────────────────┤
│  Code:                      │  Code:                               │
│  cleaned = events.dropna(   │  cleaned = events.dropna(            │
│    subset=["user_id"])      │    subset=["user_id","event"])       │
│  cleaned = cleaned[         │  cleaned = cleaned[                  │
│    cleaned.amount > 0]      │    cleaned.amount > 10]              │
│                             │                                      │
├─────────────────────────────┼───────────────────────────────────────┤
│  Output:                    │  Output:                             │
│  112,847 rows × 6 cols     │  98,203 rows × 6 cols               │
│  amount mean: 156.32       │  amount mean: 189.41                 │
│                             │                                      │
├─────────────────────────────┼───────────────────────────────────────┤
│  Provenance:                │  Provenance:                         │
│  env: sha256:e3b0c4…       │  env: sha256:e3b0c4… (same)         │
│  inputs: events@v3         │  inputs: events@v3 (same)            │
│  source: sha256:a1b2c3…    │  source: sha256:d4e5f6…             │
│  ran: 2 min ago            │  ran: yesterday                      │
├─────────────────────────────┴───────────────────────────────────────┤
│  [Revert to v2]  [Close]                                           │
└─────────────────────────────────────────────────────────────────────┘
```

**What you can see:**

- **Code diff**: What source code produced each version (since source_hash is tracked)
- **Output diff**: Row counts, schema changes, summary statistics side by side
- **Provenance diff**: Same inputs? Same environment? What changed?
- **Timestamps**: When each version was created

**Revert:**

"Revert to v2" does three things:

1. Repoints the named artifact `clean@latest` to v2
2. Restores the cell source code to the version that produced v2
3. Marks all downstream cells as stale (since their input changed)

This is safe because artifacts are immutable — v1 still exists in the store, nothing
is destroyed. Reverting is just changing a pointer.

**Use cases:**

| Scenario                                | Action                                                           |
| --------------------------------------- | ---------------------------------------------------------------- |
| "My cleaning was better yesterday"      | Compare v1 ↔ v2, revert to v2                                    |
| "Did the new filter change the output?" | Compare before/after, check row counts                           |
| "What broke the model?"                 | Walk version history, find where output diverged                 |
| "Try a different approach"              | Edit cell, run → new version. Compare with old. Pick the winner. |

**What this is NOT:**

This is not branching. There's no "run both versions simultaneously" or "fork the
downstream DAG." The DAG always has one active version per cell. Version comparison
is a tool for understanding what changed and choosing which version to keep. It's
`git log` + `git diff` + `git revert`, not `git branch`.

True branching (parallel DAGs, "my experiment vs. yours") is a future extension for
team collaboration — a much larger design surface.

---

## What the UI does vs. what the backend does

### UI (Vue frontend)

- Presents cells in order, with CodeMirror editors
- Quick regex-based variable extraction for live DAG preview
- Shows cell status (idle/running/ready/stale/error)
- Labels leaf cells with softer visual treatment
- Renders output tables from artifact data
- Shows input status indicators (● ◐ ○ ✕) per cell
- Shows cascade prompt when running a cell with unready inputs
- Sends cell execution requests to backend
- Shows DAG visualization in sidebar with leaf nodes distinguished
- Shows cache usage and controls in sidebar
- Manages notebook name, cell ordering, add/remove/reorder

### Backend (Strata server + notebook session layer)

- Authoritative AST-based variable analysis
- DAG construction and topological ordering
- Leaf detection (cells with no downstream consumers)
- Demand-driven caching (only materialize consumed outputs)
- Cell execution via `uv run` in notebook's venv
- Artifact storage via `materialize()`
- Provenance hash computation (including env_hash)
- Cache hit detection
- Cascade planning (walk DAG backwards, find what needs to run)
- Serialization/deserialization of inter-cell variables (three-tier: Arrow IPC, JSON/msgpack, pickle)
- Environment management (`uv sync`, lockfile hashing)
- `notebook.toml` persistence

### Serialization format for inter-cell data

Uses the three-tier content type system (see "Serialization tiers" in the Co-Design
section):

| Type                            | Tier          | Content type                      | Why                                        |
| ------------------------------- | ------------- | --------------------------------- | ------------------------------------------ |
| DataFrame / Table / RecordBatch | 1: Arrow      | `arrow/ipc`                       | Zero-copy, streamable, language-agnostic   |
| Large arrays                    | 1: Arrow      | `arrow/ipc`                       | Columnar, efficient                        |
| Dict / list / scalar            | 2: Structured | `json/object` or `msgpack/object` | Safe, portable, no code execution on deser |
| Trained models / custom objects | 3: Pickle     | `pickle/object`                   | Escape hatch — see security model          |

Arrow IPC is the default and fast path. Tier selection is automatic based on Python type.

---

## Features: Exposing the Graph

All features in this section follow one rule: **they do not introduce new primitives —
they expose or refine existing ones.** See "Implementation Scope" for what ships when.

### v1.1 Features

### Causality Inspector ("Why is this stale?")

The staleness UX (see "Cell status vs. input status") tells users _that_ a cell is
stale. The causality inspector tells them _why_, down to the specific change that
triggered it.

**Feature:** Each cell exposes a causality breakdown accessible via a "Why stale?"
button on the status badge. The backend computes this during the topological status
pass — it already tracks which hashes changed, so the causality chain is available
at zero extra cost.

**Example (upstream change):**

```
Cell: aggregate
Status: stale:upstream

Caused by:
  → upstream cell "clean" changed
    → source hash changed (line 3 edited)
    → input artifact updated: events@v2 → events@v3
```

**Example (environment change):**

```
Cell: aggregate
Status: stale:env

Caused by:
  → dependency changed in uv.lock:
    pandas 2.0.3 → 2.1.0
```

**Implementation:** The topological status pass already compares provenance hashes
component-by-component (source hash, input hashes, lockfile hash). To produce the
causality chain, the server stores the previous provenance components alongside the
current ones. The diff between old and new components _is_ the causality explanation.
No new data structures — just exposing what the staleness detector already computes.

**WebSocket message:** The `cell_status` message gains an optional `causality` field:

```typescript
interface CausalityChain {
  reason: StalenessReason;
  details: CausalityDetail[];
}

interface CausalityDetail {
  type: "source_changed" | "input_changed" | "env_changed";
  /** For source_changed: which cell's source */
  cellId?: string;
  /** For input_changed: old and new artifact versions */
  fromVersion?: string;
  toVersion?: string;
  /** For env_changed: which package changed */
  package?: string;
  fromPackageVersion?: string;
  toPackageVersion?: string;
}
```

**"Why did this run?" — the other half of causality.**

The causality inspector also works _after_ execution. When a cell finishes running,
the UI shows why it ran (not just that it did):

```
aggregate ran because:
  → you edited this cell (source hash changed)
```

or:

```
clean ran because:
  → upstream input events@v2 → events@v3 (cache miss on new inputs)
```

This is the same `CausalityChain` data, just rendered in past tense after execution
instead of present tense before. "Why is this stale?" and "why did this run?" are
two views of the same provenance diff.

### Execution & Cache Profiling

Every cell execution already produces timing and cache metadata. Surface it.

**Feature:** Each cell's meta bar shows execution profile inline:

```
clean        ● cached   0ms     89 MB    local
aggregate    ◐ ran      320ms   12 MB    local
train_model  ◐ ran      12.3s   434 MB   gpu-pool-1
```

**Exposed per cell:**

| Metric             | Source                      | Always visible?                     |
| ------------------ | --------------------------- | ----------------------------------- |
| Execution duration | `durationMs` on Cell        | Yes, after first run                |
| Cache hit/miss     | `cacheHit` on CellOutput    | Yes (icon: ⚡ cached / 🔄 computed) |
| Artifact size      | `size_bytes` on Artifact    | Yes                                 |
| Executor used      | From executor routing       | Only if remote                      |
| Cache load time    | `cacheLoadMs` on CellOutput | Only on cache hit                   |

**Notebook-level summary** in the sidebar:

```
┌─ Profiling ────────────────────────┐
│  Total execution:  13.2s           │
│  Cache savings:    ~45s (3 hits)   │
│  Artifact storage: 535 MB          │
│  Cells: 3 cached, 2 computed       │
└────────────────────────────────────┘
```

**Implementation:** All data already exists in the `Cell`, `CellOutput`, and
`Artifact` types. The frontend renders it — no new backend work. The "cache savings"
estimate uses the cell's historical `durationMs` from its last non-cached run.

### Run Impact Preview

Before executing, show what will happen. This extends the existing cascade prompt
with downstream impact analysis.

**Feature:** When a user hits Shift+Enter on any cell (not just cells with stale
inputs), the UI briefly shows an impact preview:

```
Running "aggregate" will:
  ↑ Re-run 1 upstream cell:
    ◐ clean          ~0.8s   (stale)
  ↓ Invalidate 2 downstream cells:
    ● export         (currently ready → will become stale)
    ● dashboard      (currently ready → will become stale)
  ⏱ Estimated time: ~1.1s

[Run all]  [Run this only]  [Cancel]
```

**Key difference from cascade prompt:** The cascade prompt only appears when inputs
are stale. The impact preview also shows _downstream_ consequences — cells that are
currently `ready` but will become `stale:upstream` after this cell produces a new
artifact version.

**When to show it:** Only when there are downstream consequences or upstream cells to
run. If a leaf cell with all-ready inputs is executed, skip the preview — just run it.
No friction for the simple case.

**Implementation:** The backend's `GET /v1/notebooks/{id}/dag` already knows the full
graph. Computing downstream impact is a forward walk from the target cell. The cascade
plan (backward walk for upstream) already exists. Combine them into an `ImpactPreview`:

```typescript
interface ImpactPreview {
  targetCellId: CellId;
  upstream: CascadeStep[]; // existing type — cells that need to run first
  downstream: DownstreamImpact[]; // cells that will become stale
  estimatedMs: number;
}

interface DownstreamImpact {
  cellId: CellId;
  cellName: string;
  currentStatus: CellStatus;
  /** Status after target cell runs */
  newStatus: "stale:upstream";
}
```

### v2+ Features

These features are well-scoped and reuse existing primitives, but they require
additional UI, API, and testing work. They should not block initial release.
See "Implementation Scope" for the full tiering.

### Published Outputs (MVP)

A cell's output can be promoted into a stable, named endpoint. This bridges
notebook exploration to production consumption without a handoff.

**Feature:** Any cell can be marked as a published output via the UI or
`notebook.toml`:

```toml
[outputs]
monthly_revenue = { cell = "aggregate", mode = "static" }
daily_anomalies = { cell = "detect", mode = "api" }
```

**Two modes:**

| Mode     | Behavior                                                                            | Use case                                                |
| -------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------- |
| `static` | Returns the latest cached artifact. No execution.                                   | Dashboards, downstream consumers that read periodically |
| `api`    | Triggers execution if stale, then returns the artifact. Cascade runs automatically. | On-demand computation, fresh results required           |

**API:**

```
GET /v1/notebooks/{id}/outputs/{name}        → artifact data
GET /v1/notebooks/{id}/outputs/{name}/meta    → schema, provenance, last updated
```

**What this reuses:**

- Artifact store (the output is just a named artifact pointer)
- Provenance hashing (staleness detection decides whether to re-run)
- Cascade execution (api mode triggers the same cascade as the UI)
- DAG (the output's freshness depends on the entire upstream chain)

**What this adds:** A thin routing layer that maps output names to cell artifacts,
plus an HTTP endpoint. The output schema (columns, types) is derived from the
artifact metadata — no user-authored schema definition.

**MVP scope — what this is NOT:**

- No scheduling (use `strata run` + cron externally)
- No monitoring or alerting (use existing Prometheus metrics)
- No SLAs or freshness guarantees
- No versioned endpoints (one name → one artifact)

It's a named artifact endpoint. That's it. Scheduling, monitoring, and schema
pinning are future extensions that layer on top without changing the core model.

### Lightweight Assertions

Standard Python assertions inside cells act as data quality checks.

**v1 behavior:** An `AssertionError` in a cell is a cell error — same as any other
exception. The cell status becomes `error`, the traceback shows the failed assertion
message, and CI (`--fail-fast`) exits non-zero. No special UI, no assertion
aggregation. `assert` just works because it's Python.

```python
# cells/validate.py
assert cleaned["user_id"].notnull().all(), "user_id has nulls"
assert len(revenue) > 0, "revenue table is empty"
```

**Post-v1 enhancement:** Richer assertion UI — collect multiple assertions per cell
(don't stop at first failure), show pass/fail indicators inline, display actual
values on failure. This requires the executor to extract individual `assert`
statements and run them independently, which is additional implementation work
that shouldn't block launch.

### Freeze Cell (advanced)

An escape hatch for power users. Freezing a cell explicitly opts out of
DAG-driven invalidation — the core model that makes everything else work. Use
sparingly.

**Feature:** Freezing a cell:

- Pins its current artifact version
- Skips staleness propagation — upstream changes do not mark it stale
- Downstream cells see it as `ready` with the pinned artifact

**In the UI:** A snowflake icon (❄) on the cell badge. Toggle via right-click →
"Freeze output" or in `notebook.toml`:

```toml
[[cells]]
id = "train_model"
file = "cells/train_model.py"
language = "python"
frozen = true          # artifact pinned, skip invalidation
```

**Relationship to existing concepts:**

| Concept                 | What it does                                       | Scope                                         |
| ----------------------- | -------------------------------------------------- | --------------------------------------------- |
| **Pin** (cache section) | Protects a specific artifact version from eviction | Storage only — cell still re-runs if stale    |
| **Freeze**              | Prevents the cell from becoming stale at all       | Execution — cell never re-runs until unfrozen |

Freezing implies pinning (the frozen artifact won't be evicted), but pinning does
not imply freezing (a pinned cell can still become stale and re-run).

**Use cases:**

- Model training took 2 hours. Don't re-run it because someone edited `load_data`.
- The dataset was correct as of Tuesday. Freeze it while exploring downstream cells.
- Production export uses a known-good artifact. Freeze it while iterating upstream.

**Safety:** The UI shows a clear warning on frozen cells:

```
❄ Frozen — output is fixed, upstream changes are ignored
```

Downstream cells that consume a frozen cell's output are computed against the frozen
artifact. If the frozen artifact is semantically stale (upstream data changed), the
downstream results are also stale in a way the system can't detect. This is the
user's responsibility — freezing is an explicit opt-out from the invalidation model.

**Why this is post-v1:** Freeze introduces hidden staleness that the system cannot
reason about. Every other feature in this doc makes staleness _more_ visible; freeze
makes it _less_ visible. The use cases are real (expensive model training, stable
snapshots), but the debugging complexity is also real. Ship it after users ask for
it, not before.

### Artifact Lineage View

Expose the full provenance chain at the artifact level, not just the cell-level DAG.

**Feature:** Clicking an artifact version shows its complete lineage — the chain of
inputs, transforms, and upstream artifacts that produced it:

```
revenue@v3
  ← transform: notebook-cell@v1 (cells/aggregate.py, sha256:a1b2...)
  ← input: cleaned@v2
    ← transform: notebook-cell@v1 (cells/clean.py, sha256:d4e5...)
    ← input: events@v3
      ← transform: scan@v1
      ← input: s3://bucket/events.parquet (snapshot 12345)
  ← env: uv.lock sha256:e3b0...
```

**Implementation:** Strata already stores lineage on every artifact (`lineage:
list[str]` — input artifact URIs). The lineage view is a recursive walk up the
`lineage` field, rendered as a tree. No new data — just a UI for what the artifact
store already tracks.

**API:**

```
GET /v1/artifacts/{id}/v/{version}/lineage?depth=10
```

Returns the lineage tree up to the specified depth. Default depth is the full chain
(stop at root artifacts with no inputs, like Iceberg scan results).

### Reproducible Bundle Export

Export a notebook as a self-contained, reproducible unit.

**Feature:**

```bash
strata export my_analysis/ --output bundle/
```

**What the bundle contains:**

```
bundle/
├── cells/                  # cell source files (copied)
├── notebook.toml           # metadata (copied, artifact URIs preserved)
├── pyproject.toml           # environment definition (copied)
├── uv.lock                 # exact lockfile (copied)
└── provenance.json         # artifact hashes + lineage for all cells
```

**What it does NOT contain:** Artifact data blobs. The bundle references artifacts by
URI and provenance hash. A recipient with access to the same Strata server (or a
compatible artifact store) can resolve them. Without access, they re-run the notebook
— the environment is fully specified, so the results are reproducible.

**Why not include blobs?** A notebook with 5 cells producing 500MB DataFrames each
would produce a 2.5GB bundle. That's not shareable via git. The bundle is code +
environment + provenance — the minimum needed to reproduce. Artifact data is a cache,
not a deliverable.

**For cases where you DO want data:** An optional `--include-artifacts` flag embeds
artifact blobs as Arrow IPC files in the bundle. This produces a larger but fully
self-contained package — useful for paper submissions, regulatory audits, or
air-gapped environments.

```bash
strata export my_analysis/ --output bundle/ --include-artifacts
```

**Relationship to git:** The bundle is a directory. `git add bundle/ && git push`
works. The provenance file lets anyone verify that re-running the notebook produces
the same hashes — a reproducibility check that existing notebooks can't do.

---

## Co-Design: Evolving Strata Core

Strata was originally designed as an analytical data serving layer — Iceberg scanning,
row-group caching, two-tier QoS for dashboards vs. ETL. The notebook reframes what
Strata is. The serving layer becomes a feature; the core becomes a **computation
persistence engine for interactive work**.

Strata is unreleased. Everything can change. Here's what should.

### New mental model

```
Before:  Strata = cache layer for Iceberg scans, with an artifact system bolted on
After:   Strata = artifact engine for interactive computation, with Iceberg as one data source
```

The primitive stays: `materialize(inputs, transform) → artifact`. But the weight shifts.

### The Arrow IPC advantage — why Strata notebooks feel interactive

> **This is Strata's moat.** Everything else — the DAG, the provenance hashing, the
> environment management — is good design. Arrow IPC is the unfair advantage. It's
> why "open notebook, everything is already there" is instant. It's why remote
> execution has near-zero serialization overhead. It's why 20 cached cells load in
> under 100ms. No other notebook system has a data layer that operates at
> memory-mapped speed.

**The problem with every other notebook:** Cell outputs live in Python process memory.
Close the notebook → gone. Restart the kernel → gone. Re-open next week → re-run
everything from scratch. A 10-minute data pipeline means 10 minutes of waiting every
time you sit down to work. This is why people leave Jupyter kernels running for days.

**What Strata does differently:** Cell outputs are persisted as Arrow IPC on disk (or
S3/GCS/Azure). Arrow IPC is not a serialization format — it's the _in-memory layout
written directly to disk_. Reading it back is a memory-mapped file read with zero
parsing, zero deserialization, zero copy.

The numbers:

| Operation              | Pickle      | Parquet             | Arrow IPC       |
| ---------------------- | ----------- | ------------------- | --------------- |
| Write 1M rows          | ~800ms      | ~400ms              | ~50ms           |
| Read 1M rows           | ~600ms      | ~300ms              | **~5ms** (mmap) |
| Read back to DataFrame | deserialize | decompress + decode | **zero-copy**   |

That 5ms read is why "open notebook, everything is already there" works in practice,
not just in theory. When the user opens a notebook with 20 cached cells, Strata serves
all 20 outputs in under 100ms total. It feels instant because it _is_ instant — there's
no deserialization step.

The Rust extension (`_strata_core`) makes this even faster:

- `read_arrow_ipc_as_stream` — memory-maps the file, converts to stream format,
  never creates Python objects for the actual data
- `concat_ipc_streams` — combines multiple outputs by manipulating bytes directly,
  no Arrow parsing

**How it flows in practice:**

```
1. User runs cell         → Python executes, output is a DataFrame
2. Strata serializes      → DataFrame → Arrow IPC (fast, ~50ms for 1M rows)
3. Strata persists        → Write to disk/S3 as artifact blob
4. User closes notebook   → Nothing lost
5. User re-opens          → Strata reads Arrow IPC (mmap, ~5ms)
6. UI renders table       → Arrow → JSON for display (only first 50 rows)
```

Step 5 is where the magic is. Every other notebook system skips straight from 1 to 6
and prays the kernel is still alive. Non-tabular data (models, dicts, scalars) goes
through the slower tiers — see "Serialization tiers" below.

### Artifact model: beyond Arrow tables

Current Strata assumes artifacts are Arrow IPC tables. Notebook cells produce everything:
DataFrames, trained models, scalars, dicts, plots, images.

**Change:** Artifacts become typed blobs with metadata.

```python
@dataclass
class Artifact:
    id: str
    version: int
    provenance_hash: str

    # New: content type system
    content_type: str          # "arrow/ipc", "pickle/object", "json/scalar", "image/png"
    schema: dict | None        # Arrow schema for tables, JSON schema for dicts, None for opaque
    size_bytes: int

    # Existing
    lineage: list[str]         # input artifact URIs
    created_at: datetime
```

Strata stores and retrieves blobs by content type. The serialization/deserialization
logic lives in the notebook executor, not in Strata core. Strata doesn't need to
understand what's inside — it just needs the provenance hash to deduplicate and the
content type to route to the right deserializer.

Arrow IPC remains the **fast path** for tabular data (zero-copy, streamable). Everything
else goes through a tiered serialization system with explicit security boundaries.

#### Serialization tiers

| Tier              | Content type                    | Formats                    | Security                                       | Use case                                         |
| ----------------- | ------------------------------- | -------------------------- | ---------------------------------------------- | ------------------------------------------------ |
| **1: Arrow**      | `arrow/ipc`                     | Arrow IPC stream           | Safe (declarative schema, no code execution)   | DataFrames, Tables, RecordBatches, large arrays  |
| **2: Structured** | `json/object`, `msgpack/object` | JSON, MessagePack          | Safe (no code execution on deser)              | Dicts, lists, scalars, JSON-serializable objects |
| **3: Pickle**     | `pickle/object`                 | Python pickle (protocol 5) | **Unsafe** (arbitrary code execution on deser) | Trained models, custom classes, anything else    |

**Serialization selection** is automatic, based on the Python type of the variable:

```python
def select_serializer(value) -> str:
    if isinstance(value, (pa.Table, pa.RecordBatch, pd.DataFrame)):
        return "arrow/ipc"
    if isinstance(value, (dict, list, int, float, str, bool, type(None))):
        # Attempt JSON first; fall back to msgpack for bytes/datetime
        return "json/object" if is_json_safe(value) else "msgpack/object"
    # Everything else: pickle
    return "pickle/object"
```

Users can override with an explicit annotation if needed (future extension), but the
default should be correct for 95% of cases.

#### Pickle security model

Pickle allows arbitrary code execution during deserialization. This is a real threat
in shared/team deployments where one user's cell output is deserialized into another
user's inspect session or downstream cell.

**Policy:**

| Deployment               | Pickle allowed? | Rationale                                                                                                                                                                                                                            |
| ------------------------ | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Local** (single user)  | Yes             | User trusts their own code                                                                                                                                                                                                           |
| **Team server** (shared) | Restricted      | Pickle artifacts are tagged with the principal who created them. Deserialization only proceeds if the consuming principal is the same user OR has `pickle:trust` scope. Otherwise, the cell fails with a clear error explaining why. |
| **CI/headless**          | Configurable    | `--allow-pickle` flag, off by default                                                                                                                                                                                                |

**Artifact metadata tracks serialization tier:**

```python
@dataclass
class Artifact:
    # ... existing fields ...
    content_type: str          # "arrow/ipc", "json/object", "pickle/object", etc.
    serializer_version: int    # for forward compat (e.g., pickle protocol version)
    creator_principal: str     # who produced this blob — for pickle trust checks
```

**Mitigation for team deployments:** When a cell produces a pickle artifact, the UI
shows a warning badge: `⚠ pickle — not portable`. This nudges users toward Arrow/JSON
serializable outputs. Over time, we can add custom serializers (e.g., `safetensors`
for ML models, `joblib` for scikit-learn) that are safe and portable, shrinking the
pickle surface area.

**Transitive trust:** Pickle trust is **not transitive.** If Alice has `pickle:trust`
for Bob's artifacts, and Bob's cell consumes a pickle artifact from Carol, Bob's
output is a new artifact created by Bob — Alice trusts Bob, not Carol. The trust
check is always between the consuming principal and the artifact's `creator_principal`,
not the transitive chain of who produced the inputs.

This means:

- Alice runs cell that deserializes Bob's pickle artifact → allowed (Alice trusts Bob)
- That cell's input happened to include data from Carol → irrelevant. The artifact
  Alice is deserializing was created by Bob's execution.
- If Alice tries to directly deserialize Carol's artifact → requires separate trust

The trust graph is flat: principal trusts principal. No chains, no delegation. This
avoids the complexity of transitive trust models and makes the security boundary
auditable — an admin can see exactly who trusts whom.

**What this means in practice:** Most notebook cells produce DataFrames (Tier 1) or
dicts/scalars (Tier 2). Pickle is the escape hatch for ML models and custom objects.
The security boundary is explicit, auditable, and restrictive by default in shared
environments.

### Provenance hash: add environment

Current: `sha256(sorted(input_hashes) + transform_spec)`

**Change:** Environment becomes a first-class component, not buried in params.

```python
provenance = sha256(
    sorted(input_artifact_hashes)     # what data flows in
    + transform_source_hash           # what code runs
    + environment_hash                # what packages are installed (from uv.lock)
)
```

This means Strata's `materialize()` API accepts an `env_hash` parameter directly,
not smuggled through `transform.params`. The artifact store indexes on it. You can
query "show me all artifacts produced under this environment" — useful for debugging
when a dependency upgrade breaks things.

### Executor model: local and remote

Current Strata has external HTTP executors with multipart form data. Good for
distributed compute, overkill for `df.dropna()`.

**Change:** Two executor tiers, selected per-cell via UI and notebook.toml.

**Local executor** (default, for most notebook cells):

- Strata spawns a Python subprocess in the notebook's venv
- Sends cell source + serialized inputs via stdin/pipe
- Receives serialized outputs via stdout/pipe
- No HTTP overhead, no network latency
- Lifecycle tied to the notebook session

**Remote executor** (for heavy compute):

- HTTP protocol (existing Strata executor protocol)
- Used for expensive transforms: model training, large aggregations, GPU workloads
- Cell inputs are shipped as Arrow IPC over the network — same format, no re-serialization
- Outputs are returned the same way and cached locally

**Auto executor** (Strata picks based on resource hints):

- Cell declares what it needs (GPU, memory, CPU cores)
- Strata matches against available executor pool
- Routes to local if requirements are modest, remote if they need more

#### Execution target is notebook metadata, not code

The execution target is configured **per-cell in `notebook.toml`**, not via code
annotations. Cell source stays pure Python — no Strata-specific syntax, no magic
comments. You can take the `.py` file, run it anywhere, it works.

```toml
[[cells]]
id = "train_model"
file = "cells/train_model.py"
language = "python"

# Optional — omit entirely for local (default)
[cells.executor]
target = "auto"                    # "local" | "remote" | "auto"
resources = { gpu = 1, memory = "32gb" }
```

Most cells have no `[cells.executor]` block at all — they run locally. Only cells
that need specific resources declare them.

#### UI for execution target

In the cell meta bar, next to the language badge, a small indicator shows where the
cell runs. Clicking it opens a popover:

```
┌─────────────────────────────────┐
│  Execution Target               │
│                                 │
│  ○ Local (default)              │
│  ● Remote                       │
│  ○ Auto                         │
│                                 │
│  Resources:                     │
│  GPU   [1    ]                  │
│  Memory [32gb ]                 │
│  CPU    [     ] (any)           │
│                                 │
│  Available executors:           │
│  ● gpu-pool-1  (2× A100, idle) │
│  ○ cpu-pool-1  (96 cores, busy)│
│                                 │
│  [Apply]  [Cancel]              │
└─────────────────────────────────┘
```

This keeps execution target discoverable and editable without touching config files.

#### Why Arrow IPC makes remote execution cheap

Arrow IPC is both the on-disk format and the wire format — no re-serialization step.
The bytes in the cache are the bytes sent over the network are the bytes loaded into
the remote executor's memory. Serialization overhead is near-zero (~10ms for 100MB
vs. ~1.4s for pickle — see performance table in "The Arrow IPC advantage" section).
This makes per-cell remote execution practical for interactive workflows where
Jupyter-style "whole kernel on a remote cluster" would be overkill.

#### Executor pool configuration

Remote executors are registered with the Strata server:

```toml
# Global Strata config
[[executors]]
name = "gpu-pool-1"
url = "https://executor-gpu.internal:8080"
resources = { gpu = 2, memory = "64gb", cpu = 16 }
tags = ["training", "inference"]

[[executors]]
name = "cpu-pool-1"
url = "https://executor-cpu.internal:8080"
resources = { memory = "256gb", cpu = 96 }
tags = ["etl", "aggregation"]
```

When a cell's `target = "auto"`, Strata matches the cell's resource requirements
against available executors, picks the best fit, and routes the execution. If no
executor matches, the cell fails with a clear error showing what's needed vs.
what's available.

### Cell as first-class concept in Strata

Currently, Strata has no concept of "cell" — everything is a generic transform.

**Change:** Add a `Cell` entity alongside `Artifact` and `Transform`.

```python
@dataclass
class Cell:
    id: str                    # "load_data"
    notebook_id: str           # "nb_a1b2c3d4"
    source_hash: str           # sha256 of cell source code
    language: str              # "python" (only Python for now)

    # DAG relationships (derived from AST, not stored permanently)
    defines: list[str]         # variables this cell exports
    references: list[str]      # variables this cell imports
    is_leaf: bool              # True if no downstream cell consumes its outputs

    # Link to latest artifact (if consumed outputs exist)
    artifact_uri: str | None   # "strata://notebooks/nb_a1b2/cells/load_data@v3"
```

This means the Strata server can answer questions like:

- "What cells produced this artifact?" (lineage)
- "What happens if I change this cell?" (impact analysis)
- "Which cells are stale?" (status across the notebook)
- "Show me all versions of this cell's output" (artifact history)
- "Which cells are leaves?" (DAG edge detection)

### Named artifacts = cells

Strata already has `ArtifactName` — mutable pointers to artifact versions. Each cell
id naturally becomes an artifact name within the notebook's namespace:

```
strata://notebooks/nb_a1b2c3d4/cells/load_data → artifact af_x7k9 v3
strata://notebooks/nb_a1b2c3d4/cells/clean     → artifact af_m2p1 v1
```

Re-run a cell → new artifact version → name pointer updates. Previous versions
are still accessible for comparison or rollback.

### What to keep from current Strata

| Keep                                        | Reason                          |
| ------------------------------------------- | ------------------------------- |
| Artifact store + SQLite metadata            | Battle-tested, versioning works |
| Provenance hashing + deduplication          | Core to notebook caching        |
| Blob store abstraction (local/S3/GCS/Azure) | Flexible storage backend        |
| Arrow IPC as the fast path                  | Zero-copy for tables            |
| Streaming while building                    | Interactive feedback in UI      |
| Lineage tracking                            | Notebook DAG visualization      |
| Named artifacts                             | Maps to cell → artifact pointer |

### What to deprioritize

| Feature                         | Reason                                       |
| ------------------------------- | -------------------------------------------- |
| Iceberg scanning as core        | Becomes a data source plugin, not the center |
| Row-group level caching         | Only relevant for Iceberg scan use case      |
| Two-tier QoS (interactive/bulk) | Notebook cells don't need admission control  |
| Multi-tenancy                   | Not day-one for notebooks                    |
| Trusted proxy auth              | Not day-one for notebooks                    |
| Remote executor HTTP protocol   | Keep but add inline executor as default      |

These features don't go away — they stay in the codebase for the data serving use case.
But they're not on the critical path for shipping the notebook.

### What to build new

| Feature                           | Reason                                    |
| --------------------------------- | ----------------------------------------- |
| Inline cell executor              | Subprocess in venv, fast, no HTTP         |
| Content type system for artifacts | Support non-tabular outputs               |
| Environment hash in provenance    | Lockfile-aware cache invalidation         |
| Cell entity in artifact store     | First-class notebook concept              |
| Notebook session management       | Open/close/save notebooks                 |
| AST-based variable analysis       | Python `ast` module for DAG construction  |
| `uv` integration                  | Spawn venvs, sync lockfiles, add packages |
| Demand-driven caching             | Only materialize consumed outputs         |

### API surface for the notebook

The notebook needs a small set of new endpoints alongside the existing `/v1/materialize`:

```
# Notebook lifecycle
POST   /v1/notebooks                          # create notebook (uv init)
GET    /v1/notebooks/{id}                      # open notebook (parse toml, uv sync, compute status)
PUT    /v1/notebooks/{id}                      # save notebook.toml
DELETE /v1/notebooks/{id}                      # delete notebook

# Cell operations
POST   /v1/notebooks/{id}/cells/{cell_id}/run  # execute cell (with cascade option)
GET    /v1/notebooks/{id}/cells/{cell_id}/status  # poll status
GET    /v1/notebooks/{id}/dag                  # full DAG with status + leaf detection

# Environment
POST   /v1/notebooks/{id}/env/add              # uv add <package>
POST   /v1/notebooks/{id}/env/remove           # uv remove <package>
GET    /v1/notebooks/{id}/env                  # current deps + lockfile hash

# Cache management
GET    /v1/notebooks/{id}/cache                # cache usage per cell
POST   /v1/notebooks/{id}/cache/compact        # keep latest version only
POST   /v1/notebooks/{id}/cache/clear          # clear old versions

# Still use existing
POST   /v1/materialize                         # still the core primitive under the hood
GET    /v1/artifacts/{id}/v/{version}/data      # fetch artifact data
```

The notebook endpoints are a thin layer that translates cell operations into
`materialize()` calls. The artifact store doesn't change — it just gets more
types of artifacts.

---

## Cache Lifecycle Management

### The problem

Every cell execution produces a new artifact version. Edit a cell 20 times, that's
20 versions. Multiply by every cell in every notebook, with multi-megabyte DataFrames.
The cache store grows indefinitely unless managed.

### Design principle

Users should never think about cache management during normal work. The system handles
it. But users need visibility and control when disk gets large.

### Three tiers

**Tier 1: Automatic — LRU eviction (no user action)**

Strata already has LRU eviction on the disk cache. This stays and becomes the baseline
for the notebook too. Configure a max size (default 10GB), evict least-recently-used
artifact blobs when full.

Critical property: **eviction deletes the blob, not the metadata.** The artifact store
still knows the artifact existed, its provenance hash, its schema, its lineage. If the
user re-runs the cell, it recomputes and caches again. Eviction is always safe — it's a
performance cost (recompute), never data loss.

This means the system self-heals. Run out of space → old blobs evicted → user re-runs
a cell → blob recomputed → back in cache. No manual intervention needed.

**Tier precedence:** Pinned versions are never evicted by LRU. Per-notebook retention
policies (Tier 3) run first as a pre-filter, then LRU operates on what remains. If the
global disk limit is hit and only pinned artifacts remain, warn the user — don't silently
break pins.

**Tier 2: Per-notebook controls (visible in sidebar)**

The UI sidebar shows cache usage per notebook:

```
┌─────────────────────────────┐
│  Cache                      │
│  847 MB · 23 artifacts      │
│  12 versions across 5 cells │
│                             │
│  load_data  v3   312 MB     │
│  clean      v1    89 MB     │
│  aggregate  v2    12 MB     │
│  model      v4   434 MB  ← │
│                             │
│  [Compact to latest]        │
│  [Clear old versions]       │
└─────────────────────────────┘
```

Actions:

| Action                 | What it does                                                                                                                         |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| **Compact to latest**  | Keep only the most recent version of each cell's artifact. Deletes all older versions (except pinned). Fastest way to reclaim space. |
| **Clear old versions** | Keep latest N versions (configurable, default 3). Safer — keeps some history for rollback.                                           |
| **Clear cell cache**   | Right-click a specific cell → delete all its cached artifacts. Cell becomes `idle`.                                                  |
| **Clear all**          | Nuclear option. Wipes all artifacts for this notebook. Everything needs to recompute.                                                |

These are the actions users reach for when they notice disk filling up or want to
share a notebook without gigabytes of cached data.

**Tier 3: Retention policies (set once, forget)**

Configurable in `notebook.toml` or globally in Strata server config:

```toml
# notebook.toml
[cache]
max_versions_per_cell = 5          # keep last 5 versions, evict older
max_age_days = 30                  # evict artifacts untouched for 30 days
max_notebook_cache_mb = 2000       # per-notebook cap (2GB)
```

```toml
# Global Strata config (strata.toml or env vars)
[cache]
max_total_size_gb = 10             # total cache budget across all notebooks
eviction_policy = "lru"            # lru | fifo | size-weighted
```

Most users never touch retention policies — the defaults are sensible. Teams with
large datasets or many notebooks configure once at the server level.

### Version pinning

Sometimes a user wants to keep a specific artifact version permanently — the model
that shipped to production, the dataset snapshot used in a paper. Pinned versions
are excluded from eviction:

```toml
# In notebook.toml:
[cache.pins]
train_model = "v4"     # never evict this version
load_data = "v3"       # or this one
```

In the UI: right-click artifact version → "Pin this version."

Pinned artifacts survive compaction, LRU eviction, and `clear old versions`.
Only explicit `unpin + clear` removes them. Pinning is about eviction protection —
it doesn't affect which version is "active." The latest version is always current;
pinned older versions are just preserved in history.

### Shared cache considerations

When multiple users share a Strata server:

- Each user's notebooks have independent cache namespaces (by notebook ID)
- If two users run the same cell with the same inputs, provenance deduplication
  means only one copy is stored (Strata's existing behavior)
- Eviction respects reference counting: don't evict a blob that's still the
  latest version for another notebook's cell
- Per-tenant quotas (from Strata's existing multi-tenancy) cap storage per team

### What the user sees

For day-to-day work: nothing. Cache is invisible. Cells are either cached (green dot,
instant load) or need to recompute (yellow dot, run it). The system handles the rest.

For housekeeping: a "Cache" section in the sidebar shows usage and one-click actions.
No terminal commands, no manual file deletion, no thinking about storage paths.

---

## Deployment & Production

> **Scope note:** v1 targets local single-user deployment only (`strata serve` on
> localhost). Team server, auth, remote executors, and CI mode are designed here for
> architectural coherence but are not required for launch.

### Deployment model

Strata notebooks are **git-native**. A notebook is a directory. Deploying means:

```bash
git clone https://github.com/team/my-analysis.git
cd my-analysis
uv sync
strata serve --notebook .
```

That's it. The notebook opens in the browser, `uv sync` ensures the environment matches,
and if there's a shared Strata server, all cached artifacts resolve instantly.

There's no notebook storage service, no proprietary file format, no hosted platform
to manage. The notebook lives in git. Collaboration is git branches. Sharing is
`git clone`. This is the "real software" promise.

### Deployment configurations

| Configuration        | Use case          | Setup                                                                                 |
| -------------------- | ----------------- | ------------------------------------------------------------------------------------- |
| **Local**            | Solo developer    | `strata serve --notebook .` on laptop. Cache on local disk.                           |
| **Team server**      | Shared notebooks  | Strata server on shared machine. Cache on S3/GCS. Multiple users connect via browser. |
| **CI/CD**            | Automated runs    | `strata run my-analysis/` in CI. Headless, no UI. Run all cells, check for errors.    |
| **Remote executors** | GPU/heavy compute | Strata server + executor pool. Cells route to appropriate hardware.                   |

### Authentication

For local use: no auth needed. Strata serves on localhost.

For team deployment: Strata already has trusted proxy auth. Deploy behind nginx/Caddy
with OAuth2 (GitHub, Google, etc.) and pass identity headers to Strata:

```
Client → nginx (OAuth2) → Strata server
  X-Strata-Principal: user@company.com
  X-Strata-Tenant: team-data
  X-Strata-Scopes: notebook:read notebook:write
```

ACL rules control who can open which notebooks and run which cells. Strata's existing
deny-first ACL model handles this. The notebook layer just adds scope checks:

- `notebook:read` — open and view notebooks
- `notebook:write` — edit cells, add/remove cells
- `notebook:execute` — run cells
- `admin:cache` — manage cache (compact, clear, pin)

### Resource limits and process isolation

A cell could allocate 100GB of RAM or run forever. The executor needs guardrails:

**For local execution:**

- Per-cell timeout (configurable in notebook.toml, default 5 minutes)
- Memory limit via process `ulimit` or cgroups
- CPU limit for fairness when multiple notebooks run simultaneously

```toml
# notebook.toml
[execution]
timeout_seconds = 300              # per-cell timeout (default 5 min)
max_memory_mb = 4096               # per-cell memory limit (default 4GB)
```

**For remote execution:**

- The executor pool enforces its own resource limits
- Strata passes the cell's declared requirements; the executor rejects if exceeded

**Graceful shutdown:**

If the server restarts while a cell is executing:

1. The artifact is in `building` state with a lease (Strata's existing mechanism)
2. On restart, orphan recovery detects abandoned builds (existing behavior)
3. The cell shows as `error` in the UI with "execution interrupted"
4. User re-runs the cell — clean restart, no stale state

### HTTPS and WebSocket

In production, the frontend connects over WSS (WebSocket Secure):

```
Browser ─── WSS ──→ nginx (TLS termination) ──→ Strata server (plain WS)
```

Standard reverse proxy setup. Strata doesn't handle TLS directly — the proxy does.
This is the same model used by every FastAPI/Uvicorn deployment.

### WebSocket protocol

The notebook UI communicates with the backend over a single WebSocket connection per
session. REST endpoints handle CRUD operations (create/open/save notebooks, fetch
artifact data). The WebSocket handles everything real-time: cell execution, status
updates, DAG changes, cascade flow.

**Connection lifecycle:**

```
1. Frontend opens notebook → REST: GET /v1/notebooks/{id} → returns Notebook state
2. Frontend connects → WS: /v1/notebooks/{id}/ws
3. Server sends initial DAG + cell statuses over WS
4. All subsequent real-time events flow over WS
5. Frontend disconnects → server cleans up session state (inspect processes, etc.)
```

**Message format:** All messages are JSON with a common envelope:

```typescript
interface WsMessage {
  type: WsMessageType
  /** Cell this message relates to (null for notebook-level messages) */
  cellId?: string
  /** Monotonic sequence number for ordering */
  seq: number
  /** Server timestamp (ISO 8601) */
  ts: string
  /** Type-specific payload */
  payload: { ... }
}
```

**Client → Server messages:**

| Type                   | Payload                         | Description                                                                                                                            |
| ---------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `cell_execute`         | `{ cellId, cascade: bool }`     | Run a cell. If `cascade=true`, auto-run upstream deps. If `cascade=false` and inputs are stale, server responds with `cascade_prompt`. |
| `cell_execute_cascade` | `{ cellId, plan: CascadePlan }` | User confirmed cascade — execute the plan.                                                                                             |
| `cell_execute_force`   | `{ cellId }`                    | "Run this only" — execute with stale inputs.                                                                                           |
| `cell_cancel`          | `{ cellId }`                    | Cancel a running cell (best-effort, kills subprocess).                                                                                 |
| `cell_source_update`   | `{ cellId, source: string }`    | Cell source changed (debounced). Server re-analyzes AST, returns `dag_update`.                                                         |
| `notebook_run_all`     | `{ staleOnly: bool }`           | Run all cells (or just stale ones) in topological order.                                                                               |
| `inspect_open`         | `{ cellId }`                    | Open inspect REPL for a cell.                                                                                                          |
| `inspect_eval`         | `{ cellId, expr: string }`      | Evaluate expression in inspect REPL.                                                                                                   |
| `inspect_close`        | `{ cellId }`                    | Close inspect REPL, kill process.                                                                                                      |

**Server → Client messages:**

| Type               | Payload                                                             | Description                                                                                               |
| ------------------ | ------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `cell_status`      | `{ cellId, status, reason? }`                                       | Cell status changed (idle → running → ready). `reason` is the staleness subcategory when status is stale. |
| `cell_output`      | `{ cellId, output: CellOutput }`                                    | Cell produced output (artifact data for display).                                                         |
| `cell_console`     | `{ cellId, stream: "stdout"\|"stderr", text: string }`              | Incremental console output during execution. Streamed as it arrives — not buffered until completion.      |
| `cell_error`       | `{ cellId, error: string, traceback?: string }`                     | Cell execution failed.                                                                                    |
| `dag_update`       | `{ cells: Cell[], edges: DagEdge[] }`                               | Authoritative DAG from backend AST analysis. Replaces the frontend's local DAG.                           |
| `cascade_prompt`   | `{ plan: CascadePlan }`                                             | "This cell needs N upstream cells to run first." UI shows the cascade dialog.                             |
| `cascade_progress` | `{ currentCellId, completedIds: string[], remainingIds: string[] }` | During cascade execution, reports which cell is currently running.                                        |
| `inspect_result`   | `{ cellId, expr: string, result: string, isError: bool }`           | Result of an inspect REPL evaluation.                                                                     |
| `notebook_status`  | `{ cellStatuses: Record<CellId, CellStatus> }`                      | Batch status update (e.g., after notebook open or environment change).                                    |
| `error`            | `{ code: string, message: string }`                                 | Protocol-level error (auth failure, notebook not found, etc.).                                            |

**Console streaming:** `cell_console` messages are sent incrementally as the cell's
subprocess produces output. The frontend appends them to the console panel in real
time. This means `print()` output, progress bars (`tqdm`), and warnings are visible
immediately — not delayed until cell completion. The server buffers with a small
flush interval (~100ms) to avoid flooding the WebSocket with per-character messages.

**Ordering guarantees:** Messages for a single cell are delivered in order (guaranteed
by `seq`). Messages across cells during a cascade may interleave — the frontend uses
`cellId` to route to the correct cell's UI.

**Reconnection:** If the WebSocket drops (network blip, laptop sleep), the frontend
reconnects and sends a `notebook_sync` request. The server responds with a full
`notebook_status` + `dag_update`, rehydrating the frontend to current state. Any
cell that was `running` when the connection dropped will either have completed
(server sends the result) or still be running (server sends current status).
In-progress console output from before the disconnect is lost — but the final
cell output (artifact) is not, since that's persisted.

### Monitoring

Strata already has Prometheus metrics and structured JSON logging. Notebook-specific
metrics to add:

| Metric                           | Type      | Purpose                                  |
| -------------------------------- | --------- | ---------------------------------------- |
| `strata_cell_executions_total`   | counter   | Cell runs by notebook, cell, status      |
| `strata_cell_duration_seconds`   | histogram | Execution time per cell                  |
| `strata_cache_hit_rate`          | gauge     | Cache hits / total requests per notebook |
| `strata_cache_size_bytes`        | gauge     | Cache usage per notebook                 |
| `strata_executor_utilization`    | gauge     | Remote executor busy/idle                |
| `strata_inspect_sessions_active` | gauge     | Open inspect REPLs                       |
| `strata_cascade_runs_total`      | counter   | Cascade executions triggered             |

These plug into existing Grafana dashboards. Strata already has the metrics
infrastructure; notebooks just add new metric names.

### Backup and recovery

Notebooks are directories in git. Backup is `git push`. Recovery is `git clone`.

Artifacts (cached cell outputs) are in Strata's blob store. For local deployments,
they're on disk — back up the cache directory. For team deployments, they're in S3/GCS
— the cloud provider handles durability.

If artifacts are lost (cache evicted, disk failure), nothing breaks. Cells recompute.
The artifacts are a cache, not the source of truth. The source of truth is the cell
source code + the input data.

### Headless / CI mode

Run a notebook without the UI, for automation and testing:

```bash
# Run all cells in topological order, fail on any error
strata run my-analysis/ --fail-fast

# Run only stale cells (useful in CI — only recompute what changed)
strata run my-analysis/ --stale-only

# Run a specific cell and its upstream dependencies
strata run my-analysis/ --cell train_model --cascade

# Export all cell outputs to a directory
strata run my-analysis/ --export-to output/
```

This makes notebooks usable in CI/CD pipelines. A commit that changes a cell triggers
a pipeline that runs `strata run --stale-only`, verifying the notebook still works.
Since unchanged cells are cached, CI runs are fast.

---

## Design Tensions

These are not bugs or missing features — they're fundamental tradeoffs that the design
makes deliberately. Documenting them here so they're visible rather than discovered
later.

**The default posture is interactivity over correctness.** Day-one Strata notebooks
are designed for exploration: run anything, see results fast, deal with staleness
later. Strict mode (future) inverts this for CI and production.

| Behavior                            | Default (interactive)  | Strict mode (CI/prod)      |
| ----------------------------------- | ---------------------- | -------------------------- |
| Run with stale inputs               | Allowed (with warning) | Forbidden                  |
| Forced artifacts downstream         | Double-confirm         | Forbidden                  |
| Pickle serialization                | Allowed                | Forbidden on shared server |
| Missing inputs                      | NameError              | Fail entire run            |
| Implicit globals                    | No warning             | Fail (lint error)          |
| Input mutation without reassignment | Heuristic warning      | Error                      |

### Python vs. determinism

Strata wants two things that are in tension: the flexibility of Python (dynamic
typing, mutation, `eval`, metaclasses, monkey-patching) and the determinism of a
reproducible compute graph (same inputs + same code = same output, always).

**Where we land:** Best-effort AST analysis covers ~95% of real-world Python patterns.
The remaining 5% (dynamic attribute access, `globals()`, runtime code generation) is
the user's responsibility. Defensive copying of inputs prevents mutation from
corrupting the cache even when the AST can't detect it.

This is the same tradeoff that `mypy`, `ruff`, and every Python IDE makes. Full
determinism would require a restricted subset of Python (like Marimo's reactive model),
which we reject because it breaks "take this .py file, run it anywhere."

**When this bites:** A cell that uses `exec()` or `importlib` to dynamically define
variables will have an incomplete DAG. The system still works — it just can't track
those dependencies, so it can't cache or invalidate them correctly. The user sees
the cell as a leaf (no known downstream consumers), which is a safe default.

### Notebook vs. build system

Strata notebooks are, structurally, a build system: a DAG of computations with
caching, invalidation, and reproducibility. The closest analogies are Bazel (content-
addressed caching), dbt (DAG of SQL transforms), and Airflow (orchestrated tasks).

But users expect _notebook_ behavior: immediate feedback, scratch cells, exploration,
"just run this one thing." Build systems optimize for correctness and reproducibility
at the cost of interactivity. Notebooks optimize for interactivity at the cost of
reproducibility.

**Where we land:** We provide build-system correctness (provenance hashing, immutable
artifacts, DAG-driven invalidation) with notebook-level interactivity ("Run this only"
with stale inputs, warm process pools, inspect mode). The cascade prompt is the key
bridge — it gives users build-system intelligence (here's what needs to run) with
notebook-level control (but you can skip it if you want).

**Risk:** Power users who understand the DAG will love this. Users who just want to
run cells top-to-bottom might find the staleness indicators, cascade prompts, and
forced-artifact warnings to be friction rather than help. The UI needs to make the
common case (run a cell, see output) feel as simple as Jupyter, with the DAG
intelligence available but not in the way.

**Strict mode flips the default:** In CI, you want the build system. Cascade is
automatic (no prompt), forced artifacts are forbidden, and any staleness is an error.
The same notebook works in both modes — the code doesn't change, only the execution
policy.

### Simplicity vs. control

We deliberately removed several control surfaces: no cell types, no kernel management,
no explicit cache configuration for individual cells. The DAG determines everything.
This is simpler, but it means users can't override the system's decisions.

**Examples where users might want control:**

- "I know this cell is stale but the result is fine — stop showing it as yellow."
  (Answer: pin the artifact version.)
- "I want this cell to always re-run, never use cache." (Answer: not supported day
  one. Workaround: add a timestamp to the cell source.)
- "I want to run two cells in parallel even though they share an upstream dependency."
  (Answer: the DAG forbids it if one depends on the other. Independent branches can
  parallelize in the future.)

**Where we land:** Start simple. Add control surfaces only when real users hit real
walls. Every knob we add is a knob that can be misconfigured. The DAG-driven defaults
are correct for the vast majority of cases.

### No kernel: interactivity tradeoff

Jupyter's persistent kernel means `import pandas` happens once and every subsequent
cell runs in ~milliseconds. Strata's no-kernel model means every cell execution is
isolated — cleaner, but with a startup cost.

The warm process pool (see "Warm process pool" section) closes most of the gap by
pre-importing libraries. But there's an inherent tradeoff: Jupyter lets you define a
variable in one cell and immediately use it in the REPL or another cell, because it's
all the same Python process. Strata requires that inter-cell data flow through the
artifact store, which means serialization, provenance tracking, and a round-trip
through the cache layer.

For tabular data (Arrow IPC), this round-trip is ~5ms and invisible. For large custom
objects that go through pickle, it's slower. The inspect mode provides a REPL for
exploration, but it's not the same as Jupyter's "the whole notebook is one live
process" model.

**Where we land:** Accept the tradeoff. The benefits (no state corruption, instant
notebook restore, sharable cached results, reproducibility) outweigh the friction.
If users need a long-lived REPL for deep exploration, inspect mode is the answer.
If they need rapid iteration on a single cell, the warm process pool makes it fast
enough. A future "session mode" (persistent process for one notebook, backed by
artifacts, killed on close) could bridge the remaining gap without sacrificing
correctness.

### Multi-output cells: which variable is "the" output?

A cell can define multiple top-level variables (e.g. `x = ...`, `y = ...`). The
analyzer correctly tracks all of them — they're all in `cell.defines`, all available
to downstream cells, and all contribute to provenance. But the DAG visualization
currently labels each node with only the first defined variable (`defines[0]`), which
creates ambiguity: users see `x` in the graph and may not realize `y` is also an
output of that cell.

This is a deeper question than just a label. Options include:

1. **Show all defines in the DAG node** (`x, y`) — honest but noisy for cells that
   define many temporaries.
2. **Tooltip on hover** — keeps the graph clean, discoverable for those who look.
3. **Short label with count badge** (`x (+1)`) — hints at multi-output without
   cluttering.
4. **Let users mark "primary" outputs** — explicit but adds a knob. Could be a
   comment convention (`# output: x`) or a cell-level annotation in `notebook.toml`.
5. **Only track variables that are actually consumed downstream** — the analyzer
   already knows references. Could dim or hide defines that no other cell uses.

The tension is between transparency (show everything the cell defines) and readability
(the graph should be scannable at a glance). This is deferred — the current behavior
is correct, just not maximally informative.

---

## Implementation Status

This section documents where the current implementation diverges from the design
above. The design remains the target — divergences are noted so developers know
what's been built, what was deferred, and what was changed deliberately.

### notebook.toml format

**Design:** Uses `[notebook]` section header with `[[cells]]` array-of-tables
(TOML syntax for arrays of inline tables). Cells have human-readable IDs like
`"load_data"`, `"clean"`, `"aggregate"`.

**Implementation:** Uses flat top-level keys (`notebook_id`, `name`, `cells`)
with `cells` as an inline array. Cell IDs are 8-character UUID prefixes
(e.g., `"77da7050"`). The `[artifacts]`, `[environment]`, and `[cache]` sections
exist in the model but are empty dicts (reserved for future milestones).

```toml
# Actual format
notebook_id = "f7bd9094-..."
name = "my_analysis"
cells = [
    { id = "77da7050", file = "77da7050.py", language = "python", order = 0 },
]
artifacts = {}
environment = {}
cache = {}
```

**Rationale:** UUID-based IDs avoid naming conflicts and simplify cell creation
(no need for unique human-readable names). The flat format is simpler to
read/write with `tomli`/`tomli_w`. Human-readable IDs may be added as optional
aliases in a future milestone.

### Cell file naming

**Design:** Cell files are named after the cell ID: `cells/load_data.py`,
`cells/clean.py`.

**Implementation:** Cell files use the UUID prefix: `cells/77da7050.py`. The
`file` field in `notebook.toml` stores just the filename (e.g., `"77da7050.py"`),
and the parser resolves it relative to `cells/`.

### Serialization tiers

**Design:** Three tiers: `arrow/ipc`, `json/object` or `msgpack/object`, and
`pickle/object`.

**Implementation:** `msgpack/object` is not implemented. The three supported
content types are `arrow/ipc`, `json/object`, and `pickle/object`. The harness
(`harness.py`) detects types and serializes accordingly. `msgpack` can be added
later if needed — JSON covers the structured-data tier adequately for now.

### Artifact ID scheme

**Design:** Uses URI-style artifact names:
`strata://notebooks/nb_a1b2/cells/load_data@v3`.

**Implementation:** Uses flat string IDs:
`nb_{notebook_id}_cell_{cell_id}_var_{variable_name}`. The `@v=N` version
suffix is appended by the blob store for file naming but is not part of the
artifact ID itself. Content type is stored in `transform_spec.params.content_type`
for round-tripping during deserialization.

### Cascade behavior

**Design:** Prompt-then-cascade with three buttons: `[Run all]`,
`[Run this only]`, `[Cancel]`. The server sends a `cascade_prompt` message and
waits for user confirmation.

**Implementation:** The frontend auto-accepts all cascade prompts. The server
sends `cascade_prompt` with a plan, and the frontend's `cascade_prompt` handler
immediately responds with `cell_execute_cascade`. There is no user-facing dialog.
This simplifies the UX for the initial version — the three-button prompt can be
added when users request finer control.

### Impact preview

**Design:** Impact preview (upstream cascade + downstream staleness) is shown
before every cell execution that has consequences.

**Implementation:** Impact preview is implemented as a separate WebSocket message
type (`impact_preview_request` / `impact_preview`) and REST endpoint, but it was
removed from the cell execution path to avoid blocking execution. Users can
request impact previews on demand, but they are not shown automatically before
running a cell.

### REST API routes

**Design:** Routes use the notebook's own ID from `notebook.toml`:
`/v1/notebooks/{id}/cells/{cell_id}/run`.

**Implementation:** The `{notebook_id}` path parameter in routes is actually the
**session ID** (returned by the `create` or `open` endpoint), not the
`notebook_id` from `notebook.toml`. This is because multiple sessions could
theoretically open the same notebook. Key endpoint differences:

| Design                           | Implementation                                          |
| -------------------------------- | ------------------------------------------------------- |
| `POST /v1/notebooks`             | `POST /v1/notebooks/create`                             |
| `GET /v1/notebooks/{id}`         | `POST /v1/notebooks/open`                               |
| `POST .../cells/{cell_id}/run`   | Execution is via WebSocket (`cell_execute`), not REST   |
| `GET .../cells/{cell_id}/status` | Status comes via WebSocket (`cell_status`), not polling |

### Cell status values

**Design:** Fine-grained staleness: `idle`, `ready`, `stale:self`,
`stale:upstream`, `stale:env`, `stale:forced`, `running`, `error`.

**Implementation:** The `StalenessReason` enum exists with `SELF`, `UPSTREAM`,
`ENV`, `FORCED` variants, and `CellStaleness` model is defined. However, the
runtime cell status tracking on `session.notebook_state.cells[i].status` uses
simple string values: `"idle"`, `"running"`, `"ready"`, `"error"`. Full
staleness computation (comparing provenance hashes component-by-component) is
not yet wired into the status update path.

### WebSocket protocol

**Design:** Defines `cell_cancel`, `notebook_run_all`, `notebook_sync` client
messages and `cell_error`, `notebook_status`, `inspect_result` server messages.

**Implementation:** The following are implemented and working:

- Client → Server: `cell_execute`, `cell_execute_cascade`, `cell_execute_force`,
  `cell_source_update`, `notebook_sync`, `impact_preview_request`,
  `inspect_open`, `inspect_eval`, `inspect_close`
- Server → Client: `cell_status`, `cell_output`, `cell_console`,
  `cascade_prompt`, `cascade_progress`, `dag_update`, `impact_preview`

Not yet implemented: `cell_cancel`, `notebook_run_all`, `cell_error` (errors are
sent as `cell_status` with `status="error"`), `notebook_status` (sync sends
individual messages), `inspect_result`.

### Warm process pool

**Design:** Pre-spawn 1-2 Python processes with imports pre-loaded.

**Implementation:** `pool.py` and `pool_worker.py` exist and implement the pool
concept. The pool worker communicates via stdin/stdout JSON protocol. However,
the executor (`executor.py`) currently spawns individual subprocess runs via
`harness.py` with a manifest file rather than using the pool for all executions.
The pool is available but not the default execution path.

### Environment management

**Design:** `uv init` on notebook creation, `uv sync` on open, `uv add` for
package management. Lockfile hash included in provenance.

**Implementation:** `env.py` implements `compute_lockfile_hash()`. The
`provenance.py` module accepts a lockfile hash parameter. However, the notebook
creation flow (`writer.py:create_notebook()`) does not run `uv init`, and
notebook opening does not run `uv sync`. Environment management is designed
but not yet integrated into the notebook lifecycle. The harness runs in
whatever Python environment the server uses.

**Future extension:** Named notebook environments and cell-level environment
selection are designed separately in `docs/design-notebook-environments.md`.

### Defensive copy / immutability contract

**Design:** Inputs are defensively copied before injection. Arrow IPC gets no
copy (deserialization produces a new object), JSON gets shallow copy, pickle
gets deep copy.

**Implementation:** The harness (`harness.py`) deserializes inputs from files
on disk, which inherently produces new objects (no shared memory with the
artifact store). This provides the same isolation guarantee as defensive copying
— mutations in the cell cannot affect stored artifacts. The explicit
`deep_copy_if_mutable()` call described in the design is not needed because the
subprocess boundary provides stronger isolation.

### Mutation detection

**Design:** Heuristic mutation detection after cell execution, comparing input
variable identity and content samples.

**Implementation:** Implemented in `harness.py` as `snapshot_inputs()` and
`detect_mutations()`. Uses `id()` comparison for identity checks and
`_hash_dataframe_sample()` (first 5 + last 5 rows SHA-256) for DataFrame
content comparison. Mutation warnings are returned in the execution result
manifest and forwarded to the client.

### DAG consumed_variables

**Design:** The DAG tracks which variables from each cell are consumed by
downstream cells, driving demand-driven caching.

**Implementation:** Implemented correctly. `dag.py` line 119 populates
`consumed_variables[producer_id].add(var)`. The executor's `_store_outputs`
method only persists variables that appear in `consumed_variables` for the cell,
matching the design's demand-driven caching principle.

### Inspect mode

**Design:** Lazy artifact proxies for fast startup, process reuse across cells.

**Implementation:** `inspect_repl.py` implements the REPL with WebSocket
integration. The lazy `ArtifactProxy` pattern is not yet implemented — inputs
are loaded eagerly. Process reuse across cells is not implemented; each inspect
session spawns a new process.

---

## Future extensions (not day one)

- **Collaboration**: Multiple users editing same notebook, real-time cursors, conflict resolution
- **Branching**: Fork a notebook, run experiments in parallel, merge results
- **Scheduling**: Run a notebook on a cron schedule (`strata run` + crontab)
- **Visualization cells**: Rich chart rendering with Plotly/Matplotlib inline
- **Export**: `strata export my_analysis/ → my_analysis.py` (concatenate cells into a script)
- **Strict mode**: For CI and production — forbid `run this only`, require all inputs ready, reject implicit globals, fail on pickle in shared mode
- **Per-cell import tracking**: Map which cells depend on which packages for selective environment invalidation (instead of all-cells-stale on lockfile change)
- **Session mode**: Optional persistent process for one notebook, backed by artifacts, killed on close — bridges the gap for users who need Jupyter-like iteration speed
- **Notebook marketplace**: Share notebook templates with pre-configured environments
