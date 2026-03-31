# Design: Remote I/O and Cell-Level Remote Execution

## Overview

Two features that together turn Strata Notebook into a distributed compute notebook:

1. **File Mounts** — Declarative read/write access to local and remote filesystems from cells
2. **Remote Workers** — Route individual cells to either the local runtime or an executor-backed remote runtime

These features are deeply linked: remote workers need file mounts to access data, and file mounts need to work identically regardless of where the cell executes.

**UI mockup**: See `docs/mockup-mounts-workers.html` for the sidebar panels (Mounts, Workers) and per-cell infrastructure toolbar (worker dropdown + mount pills).

## Current Status (March 2026)

The design below started as a forward-looking plan. The codebase has now implemented most of the core notebook worker stack:

- notebook-level and cell-level persisted mounts, workers, timeouts, and env vars in `notebook.toml`
- source annotation parsing **and authoring UI** for `# @mount`, `# @worker`, `# @timeout`, and `# @env`
- local execution, named local workers, embedded executor workers, direct HTTP executor workers, and signed/build-backed HTTP executor workers
- service-mode server-managed worker policy, health probing, health history, and admin CRUD APIs
- frontend controls for notebook defaults, cell overrides, worker catalogs, and service-mode admin editing
- structured remote execution metadata persisted in notebook cell state (`remote_worker`, `remote_transport`, `remote_build_id`, `remote_build_state`, `remote_error_code`)
- notebook integration, WS coverage, and service-mode notebook E2E coverage for admin-managed remote workers

The main things that are still intentionally incomplete are:

- a broader production worker fleet/orchestration layer beyond the reference notebook executor
- richer long-term worker operations data beyond the current short health history
- stronger RW mount semantics if we ever want side-effecting cells to become cacheable

## Part 1: File Mounts (Remote I/O)

### Design Principles

- **Declarative**: Mounts are declared in `notebook.toml`, not scattered in cell code
- **Path-based**: Cell code uses standard `pathlib.Path` / `open()` against executor-managed local directories — no SDK imports
- **Provenance-aware**: Read-only mount state participates in cache key computation
- **Backend-agnostic**: Same cell code works against local and remote storage as long as the backend can materialize the mount locally

Transparency here means "the cell sees a local `Path`". It does **not** imply a live, coherent remote filesystem view like FUSE would provide. Remote mounts are materialized into local mirror or staging directories before execution.

### notebook.toml Schema

```toml
notebook_id = "f7bd9094-..."
name = "training_pipeline"

# Notebook-level mount declarations
[[mounts]]
name = "raw_data"
uri = "s3://my-bucket/datasets/v3"
mode = "ro"  # read-only

[[mounts]]
name = "checkpoints"
uri = "s3://my-bucket/checkpoints"
mode = "rw"  # read-write

[[mounts]]
name = "local_data"
uri = "file:///home/user/data"
mode = "ro"

[[mounts]]
name = "scratch"
uri = "file:///tmp/strata-scratch"
mode = "rw"
```

### Cell-Level Override

The primary way to add or override mounts per cell is through the **cell infrastructure toolbar** — a row below the editor showing the cell's worker assignment and mount pills. Clicking the "+" button on the mount pills opens a form to add a mount override. The override is persisted in `notebook.toml` as cell metadata:

```toml
[[cells]]
id = "77da7050"
file = "77da7050.py"
language = "python"
order = 0

[[cells.mounts]]
name = "raw_data"
uri = "s3://different-bucket/v4"
mode = "ro"
```

Cells without overrides show "inherits notebook defaults" in the toolbar.

**Annotation fallback**: For scripting and automation, `# @mount` comment annotations are also supported as a power-user escape hatch:

```python
# @mount raw_data s3://different-bucket/v4 ro

import pandas as pd
df = pd.read_parquet(raw_data / "events.parquet")
```

Runtime precedence (highest wins): `# @mount` annotations > persisted cell metadata (typically edited via the toolbar UI) > notebook-level defaults.

Because toolbar edits are persisted into `notebook.toml`, the UI is not a separate source of truth at execution time. If an annotation overrides a persisted toolbar choice, the UI should surface that conflict explicitly.

### How It Works

**In the harness**, mount names become `pathlib.Path` variables injected into the cell namespace. The harness sees only local paths:

- **Local mounts** (`file://`): Path is used directly (validated to exist)
- **Remote mounts** (`s3://`, `gs://`, `az://`): the executor materializes a local mirror or staging directory before spawning the harness

The manifest gains a `mounts` section:

```json
{
  "source": "...",
  "inputs": {...},
  "output_dir": "/tmp/strata_output",
  "mounts": {
    "raw_data": {
      "uri": "s3://my-bucket/datasets/v3",
      "mode": "ro",
      "local_path": "/tmp/strata_mounts/raw_data"
    },
    "checkpoints": {
      "uri": "s3://my-bucket/checkpoints",
      "mode": "rw",
      "local_path": "/tmp/strata_mounts/checkpoints"
    }
  }
}
```

### Harness Mount Resolution

The harness injects mounts before executing cell source:

```python
# In harness.py — before exec(source, namespace)
for mount_name, mount_spec in manifest.get("mounts", {}).items():
    namespace[mount_name] = Path(mount_spec["local_path"])
```

For remote mounts, the **executor** (not the harness) handles materialization:

1. **Read-only remote mounts**: Executor uses fsspec to create a cached local mirror before spawning the harness. The `local_path` in the manifest points to this mirror.
2. **Read-write remote mounts**: Executor creates a local staging directory. After cell execution, modified files are synced back to the remote URI.

This keeps the harness simple — it only sees local paths. The local directory is a materialized snapshot or staging area, not a live remote filesystem mount.

### Provenance Integration

Mounts affect caching. A cell reading from `s3://bucket/data` should invalidate when the data changes.

**For read-only mounts:**
- **Iceberg tables** (Strata's sweet spot): Use snapshot ID as the mount fingerprint
- **S3/GCS directories**: Use a content manifest hash (list of object keys + ETags + sizes)
- **Local directories**: Use `mtime` of the directory tree (fast but not cryptographic)
- **Explicit pinning**: User can pin a mount to a specific version: `uri = "s3://bucket/data@etag=abc123"`

The mount fingerprint becomes part of the provenance hash:

```
provenance = sha256(
    sorted_input_hashes +
    source_hash +
    env_hash +
    sorted_mount_fingerprints   # NEW
)
```

**For read-write mounts**: These are *side effects*. They do not participate in provenance, and cells that declare any `rw` mount are treated as **non-cacheable** — the executor skips the cache check at step ④ entirely. The mount declarations are still recorded in artifact metadata for lineage and audit purposes. If we later want cacheable read-write semantics, we will need explicit mount snapshots or versioned mount outputs.

### Mount Fingerprinting and Cache-Skip Mechanism

`MountFingerprinter.fingerprint()` returns `str` for read-only mounts and `None` for read-write mounts:

```python
class MountFingerprinter:
    async def fingerprint(self, mount: MountSpec) -> str | None:
        if mount.mode == MountMode.READ_WRITE:
            return None  # signals non-cacheable
        if mount.pin is not None:
            return sha256(f"pin:{mount.pin}")
        scheme, path = parse_mount_uri(mount.uri)
        if scheme == "file":
            return self._fingerprint_local(path)    # mtime + size tree hash
        elif scheme == "s3":
            return await self._fingerprint_s3(path)  # ETag listing hash
        # ... gs, az
```

The executor uses the `None` signal to skip caching:

```python
# In CellExecutor._materialize(), after fingerprinting all mounts:
mount_fingerprints = []
has_rw_mount = False
for name, rm in cell_mounts.items():
    fp = await MountFingerprinter.fingerprint_mount(rm.spec)
    if fp is None:
        has_rw_mount = True
    else:
        mount_fingerprints.append(f"{name}:{fp}")

# ... provenance computation uses mount_fingerprints ...

# ④ Cache check — skip entirely if any mount is read-write
if has_rw_mount:
    use_cache = False
```

For S3 fingerprinting, we use `ListObjectsV2` with ETags rather than downloading content:

```python
async def _fingerprint_s3(self, path: str) -> str:
    fs = S3FileSystem(...)
    listing = fs.get_file_info(Selector(path, recursive=True))
    parts = sorted(f"{info.path}:{info.size}:{info.mtime}" for info in listing)
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()
```

Models are consolidated in the [Data Model](#data-model) section below.

### Implementation Modules

New file: `src/strata/notebook/mounts.py`

```python
"""File mount resolution and management for notebook cells.

Handles:
- Parsing mount specs from notebook.toml and cell annotations
- Resolving remote URIs to local paths (via fsspec)
- Computing mount fingerprints for provenance
- Post-execution sync for read-write mounts
"""

class MountResolver:
    """Resolves mount URIs to local paths for cell execution."""

    async def prepare_mounts(
        self, mounts: list[MountSpec], credentials: dict | None = None
    ) -> dict[str, ResolvedMount]:
        """Materialize mounts to local paths before cell execution."""

    async def sync_back(self, resolved: dict[str, ResolvedMount]) -> None:
        """Sync read-write mounts back to remote after execution."""

class MountFingerprinter:
    """Compute content fingerprints for cache key computation."""

    async def fingerprint(self, mount: MountSpec) -> str | None:
        """Return content hash for a mount's current state."""
```

### Dependency: fsspec

Add `fsspec` as an optional dependency with backend extras:

```toml
# pyproject.toml
[project.optional-dependencies]
mounts = ["fsspec>=2024.1", "s3fs", "gcsfs", "adlfs"]  # per-backend
```

For the harness (which runs in the notebook venv), we don't need fsspec — the executor resolves everything to local paths before spawning the subprocess.

---

## Part 2: Remote Workers (Cell-Level Remote Execution)

### Design Principles

- **UI-first worker selection**: Workers are assigned per cell via a dropdown in the cell infrastructure toolbar, not hidden in code comments
- **Two execution backends**: `local` for direct harness spawn, `executor` for any remote target that runs a Strata executor endpoint
- **Executor protocol + local fallback**: Remote workers are dispatched through Strata's existing executor protocol (signed URLs, QoS, leases). Local execution keeps the current direct harness spawn for zero overhead on the common case.
- **Transparent cell code**: Cell source still sees `Path` mounts and ordinary Python execution
- **Artifact refs, not blob relay**: Workers exchange artifact references or signed URLs, not large in-memory blobs through the notebook coordinator
- **Worker runtime affects cache identity**: The selected worker runtime participates in provenance

### Worker Declaration in notebook.toml

In **personal/dev mode**, workers can be notebook-scoped and persisted in `notebook.toml`:

```toml
[[workers]]
name = "local"
backend = "local"  # default — current behavior

[[workers]]
name = "gpu-cluster"
backend = "executor"
runtime_id = "gpu-a100-4x"
[workers.config]
url = "https://gpu-node-1.internal:8766/v1/execute"
deployment = "ssh-host"
description = "gpu-node-1.internal — 4× A100"

[[workers]]
name = "training"
backend = "executor"
runtime_id = "k8s-train-a10g"
[workers.config]
url = "https://train-executor.ml-workloads.svc/v1/execute"
deployment = "kubernetes"
namespace = "ml-workloads"
image = "ghcr.io/myorg/strata-worker:latest"
gpu_count = 4
memory = "32Gi"
timeout_seconds = 3600

[[workers]]
name = "custom-fleet"
backend = "executor"  # uses Strata's existing executor protocol
[workers.config]
url = "https://executor.internal/v1/execute"
protocol = "v2"  # pull model with signed URLs
```

In **service/multi-tenant mode**, worker definitions are server-managed and `notebook.toml` stores only the selected worker name on each cell. User-supplied executor URLs are not accepted from notebook content in that mode.

### Cell Worker Assignment

The primary way to assign a worker is through the **cell infrastructure toolbar**. Each cell shows a worker dropdown with a health indicator dot, defaulting to `local`. Selecting a different worker persists it in `notebook.toml` as cell metadata:

```toml
[[cells]]
id = "77da7050"
file = "77da7050.py"
language = "python"
order = 0
worker = "gpu-cluster"
timeout = 3600
```

The cell infrastructure toolbar shows:
- **Worker dropdown**: Current worker name with health dot (green/yellow/red). Click to switch.
- **Mount pills**: Compact badges showing which mounts this cell uses, color-coded by scheme.

When a cell is running on a remote worker, the output area shows the worker name and execution progress (e.g., "executing on gpu-a100 (executor v2, pull model)").

**Annotation fallback**: For scripting, `# @worker` and `# @timeout` annotations are also supported:

```python
# @worker gpu-cluster
# @timeout 3600

import torch
model = torch.load(checkpoints / "latest.pt")
results = model.evaluate(test_data)
```

Runtime precedence: `# @worker` / `# @timeout` annotations > persisted cell metadata (typically edited via the toolbar UI) > notebook defaults.

If an annotation overrides the persisted worker selection, the cell toolbar should show that the effective worker comes from source annotations rather than saved metadata.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│ Cell Executor (notebook)                            │
│                                                     │
│  ① Materialize upstreams (via artifact store)       │
│  ② Resolve worker + runtime identity                │
│  ③ Compute provenance (includes worker fingerprint) │
│  ④ Cache check                                      │
│  ⑤ Dispatch:                                        │
│     local     → spawn harness directly              │
│     embedded  → run bundle path in-process          │
│     direct    → POST notebook bundle request        │
│     signed    → create build + signed manifest      │
│  ⑥ Store outputs (via artifact store)               │
└─────────────────────────────────────────────────────┘
        │ (HTTP / signed path only)
        ▼
┌─────────────────────────────────────────────────────┐
│ Notebook Remote Executor                             │
│  - GET /health                                      │
│  - POST /v1/execute         (direct multipart path) │
│  - POST /v1/execute-manifest (signed path)          │
│  - POST /v1/notebook-execute (compat alias)         │
└─────────────────────────────────────────────────────┘
        │ (signed transport only)
        ▼
┌─────────────────────────────────────────────────────┐
│ Core Build / Signed-URL Services                     │
│  - build record + status                            │
│  - signed download/upload URLs                      │
│  - finalize notebook bundle build                   │
└─────────────────────────────────────────────────────┘
```

Current execution paths, chosen per cell:

1. **Local**: Spawn the harness subprocess directly. This is still the fast path for exploration and lightweight compute.
2. **Embedded executor**: Use the executor-style bundle path locally for named executor workers that resolve to `embedded://...`.
3. **Direct HTTP executor**: Send the notebook execution request and input files straight to a remote notebook executor over HTTP.
4. **Signed HTTP executor**: Create a build-backed manifest with signed URLs, let the remote executor download inputs and upload the bundle, then finalize through core build services.

The signed path is the most production-like remote path today, but the codebase intentionally still supports the simpler direct HTTP path for development and local deployments.

### Executor Protocol Bridge

The implementation today uses a narrower bridge than the original plan:

1. Notebook remote execution is expressed as `notebook_cell@v1` metadata carried to the remote executor.
2. For **direct** workers, the notebook server POSTs source, serialized inputs, and metadata directly to `/v1/execute`.
3. For **signed** workers, the notebook server creates a build record plus signed input/output URLs, then POSTs that manifest to `/v1/execute-manifest`.
4. The remote executor resolves mounts locally, runs `harness.py`, emits one `notebook-output-bundle@v1`, and either returns it directly (direct path) or uploads/finalizes it (signed path).
5. The notebook executor always fans the resulting bundle back out into canonical per-variable notebook artifacts on the notebook side.

So the notebook system already shares the core signed-URL/build lifecycle, but it does **not** yet submit notebook cells to the generic transform registry / `BuildRunner` in the same way as regular server transforms. That remains future hardening work if we want a fully unified remote execution plane.

### Multi-Output Cell Bundling

Notebook cells are multi-output by nature: one cell can define multiple variables, and the current notebook artifact model stores canonical artifacts per consumed variable. The remote build path keeps that model by using a two-step representation:

1. The remote `notebook_cell@v1` build produces **one bundle artifact** containing:
   - the harness result manifest
   - serialized output files for every emitted variable
   - stdout/stderr and execution metadata
2. After the bundle build finalizes, the notebook executor downloads or opens the bundle and runs the same per-variable canonicalization step used by local execution.

Conceptually:

```python
# Remote build output artifact (single build artifact)
bundle = {
    "result_manifest": {...},
    "files": {
        "metrics.arrow": b"...",
        "model_info.json": b"...",
    },
}

# Notebook-side canonicalization (same logical step as local _store_outputs)
for var_name, spec in bundle["result_manifest"]["variables"].items():
    store_canonical_notebook_artifact(
        cell_id=cell_id,
        var_name=var_name,
        serialized_file=bundle["files"][spec["file"]],
        provenance_hash=sha256(f"{cell_provenance}:{var_name}"),
    )
```

The single remote build artifact is therefore a transport container, not the long-lived notebook-facing artifact model.

### Bundle Wire Format: `notebook-output-bundle@v1`

The remote executor uploads exactly one gzip-compressed tar archive for each remote cell execution. The signed upload endpoint treats it as opaque bytes; the notebook executor knows how to unpack it.

Archive layout:

```text
bundle.tar.gz
├── manifest.json
├── stdout.txt
├── stderr.txt
└── files/
    ├── metrics.arrow
    ├── model_info.json
    └── _.pickle
```

`manifest.json` schema:

```json
{
  "schema_version": "notebook-output-bundle@v1",
  "success": true,
  "variables": {
    "metrics": {
      "file": "files/metrics.arrow",
      "content_type": "arrow/ipc",
      "rows": 1024,
      "columns": ["loss", "accuracy"],
      "bytes": 18342
    },
    "_": {
      "file": "files/_.pickle",
      "content_type": "pickle/object",
      "bytes": 412
    }
  },
  "stdout_file": "stdout.txt",
  "stderr_file": "stderr.txt",
  "mutation_warnings": [],
  "error": null,
  "traceback": null
}
```

Failure bundles keep the same top-level schema but set:

```json
{
  "schema_version": "notebook-output-bundle@v1",
  "success": false,
  "variables": {},
  "stdout_file": "stdout.txt",
  "stderr_file": "stderr.txt",
  "error": "ModuleNotFoundError: No module named 'torch'",
  "traceback": "..."
}
```

Size rules:

- `max_output_bytes` applies to the compressed uploaded bundle size
- the executor must fail before upload if the bundle exceeds that limit
- the notebook-side fan-out step trusts the manifest metadata but still validates extracted files before creating per-variable artifacts

### Worker Runtime Identity and Provenance

Worker routing is part of execution identity. A cell executed on a GPU image or alternate Python runtime must not share a cache entry with the same source run locally.

The executor includes a deterministic worker runtime fingerprint in provenance:

```python
worker_fingerprint = sha256_json(
    {
        "backend": worker.backend,
        "runtime_id": worker.runtime_id,
        "executor_url": worker.config.get("url"),
        "image": worker.config.get("image"),
    }
)

provenance = sha256(
    sorted_input_hashes +
    source_hash +
    notebook_env_hash +
    sorted_mount_fingerprints +
    [worker_fingerprint]
)
```

For local execution, `worker_fingerprint` is a constant derived from the notebook's own venv hash — which is already captured in `env_hash`. So local provenance is unchanged.

`runtime_id` is a user-specified stable string for cases where the executor URL alone doesn't capture the runtime identity (e.g., the same URL serves different GPU types via a load balancer).

### How Mounts Work on Remote Workers

Mounts are declared in the `BuildManifest` params and resolved **on the executor side**, not the notebook coordinator:

- **Local executor**: Mount paths are local paths (current behavior, resolved by `MountResolver`)
- **Remote executor**: The executor resolves mounts using its own credentials and filesystem access. S3/GCS mounts are accessed natively (the GPU machine has its own IAM role). Notebook-declared `file://` mounts are rejected for remote execution in phase 2.

The executor is responsible for materializing mounts to local paths and passing them in the harness manifest, just like the local path does today. The difference is only *where* this resolution happens.

### Remote `file://` Mount Policy

To keep phase-2 implementation safe and predictable, remote workers do **not** support notebook-declared `file://` mounts.

Rules:

- If the effective worker is `local`, `file://` mounts work normally
- If the effective worker is `executor` and any effective mount uses `file://`, dispatch is rejected before build submission with a validation error
- The error should name the offending mount and advise either switching the cell back to `local` or moving the data to an object-store-backed mount such as `s3://` or `gs://`

This avoids inventing implicit host-directory sync semantics in the first remote-worker implementation. If we want remote host-path sync later, that should be a separate, explicit transport mode.

### Credentials and Trust Boundaries

Remote execution must not implicitly forward notebook user credentials into executors.

- **Local**: Uses the notebook server's own filesystem access and cloud credentials
- **Remote executors**: Use the identity of the machine/pod they run on (IAM role, service account, SSH key). The notebook coordinator only sends signed URLs — no cloud credentials cross the wire.

Mount access is implicitly scoped by the executor's own credentials. A GPU machine with an IAM role that can access `s3://training-data` but not `s3://prod-secrets` enforces that boundary without the notebook needing to know.

### Worker Authorization and Health API

The notebook frontend never talks to executor URLs directly. The notebook server is the control-plane authority for:

- which workers are visible to the current principal
- whether the current notebook is allowed to assign a given worker
- cached health status and short probe history for each worker

Current API shape:

```text
GET    /v1/notebooks/{id}/workers?refresh=true|false
PUT    /v1/notebooks/{id}/workers                  # personal/dev mode only
PUT    /v1/notebooks/{id}/worker                   # notebook default worker
PUT    /v1/notebooks/{id}/cells/{cell_id}/worker   # cell override

GET    /v1/admin/notebook-workers
PUT    /v1/admin/notebook-workers
POST   /v1/admin/notebook-workers
PUT    /v1/admin/notebook-workers/{worker_name}
PATCH  /v1/admin/notebook-workers/{worker_name}    # enable/disable
DELETE /v1/admin/notebook-workers/{worker_name}
POST   /v1/admin/notebook-workers/{worker_name}/refresh
```

`GET /v1/notebooks/{id}/workers` response shape:

```json
{
  "workers": [
    {
      "name": "gpu-cluster",
      "backend": "executor",
      "runtime_id": "gpu-a100-4x",
      "config": {"url": "https://gpu-node-1.internal:8766/v1/execute"},
      "source": "server",
      "allowed": true,
      "enabled": true,
      "transport": "direct",
      "health": "healthy",
      "health_url": "https://gpu-node-1.internal:8766/health",
      "health_checked_at": 1774815252000,
      "last_error": null,
      "health_history": [
        {"checked_at": 1774815252000, "health": "healthy", "error": null}
      ]
    }
  ],
  "definitions_editable": false,
  "health_checked_at": 1774815252000
}
```

Authorization rules:

- In personal/dev mode, notebook-scoped worker definitions may be created and edited through `PUT /v1/notebooks/{id}/workers`
- In service/multi-tenant mode, notebook-scoped worker definition editing is disabled; workers come from a server-managed registry
- In service mode, authorized operators manage the registry through `/v1/admin/notebook-workers*`
- Saving a cell worker assignment and executing a cell both revalidate against the same allowlist/policy
- If a notebook references a worker that is renamed, deleted, or disabled, the notebook catalog exposes it as `source = "referenced"` or `enabled = false`, the UI shows it as unavailable, and execution is rejected

Health rules:

- Health is probed server-side on demand and cached with a short TTL
- The probe is backend-aware: local workers check local readiness; executor workers probe the configured executor endpoint
- The browser only consumes the cached health summary and recent probe trail from notebook APIs

### Cancellation and Cleanup

For notebook cells:

- Local dispatch: the existing `asyncio.CancelledError` handler kills the harness subprocess
- Direct remote dispatch: request failures surface back as notebook cell errors and mark the worker unavailable in the notebook UI
- Signed remote dispatch: cancellation and transport/finalize failures fail the build cleanly; malformed bundles are rejected and do not finalize as `ready`
- RW mount sync-back only happens after successful local execution. Remote executors resolve mounts locally and do not sync back through the notebook coordinator.

### Frontend UI

Two new UI components, shown in the mockup (`docs/mockup-mounts-workers.html`):

**Sidebar panels** (right side, alongside DAG view):
- **Mounts panel**: Lists notebook-level mounts with scheme icon, name, URI (truncated), and ro/rw badge. "+ Add mount" button opens a form for name, URI, mode.
- **Workers panel**: Lists registered workers with health status dot, transport, last check time, recent health history, and inline last-error details. In personal/dev mode it edits notebook-scoped definitions. In service/multi-tenant mode it can either show the server-managed catalog read-only or expose admin CRUD when the caller is authorized.

**Cell infrastructure toolbar** (per cell, between editor and metadata bar):
- **Worker dropdown**: Shows assigned worker name with health dot. Defaults to `local`. Dropdown lists all registered workers with their health.
- **Mount pills**: Compact color-coded badges for each mount this cell uses. Peach for S3, green for local, blue for GCS. Each pill shows mount name and ro/rw mode. "+" button to add a cell-level override. Cells without overrides show "inherits notebook defaults".
- **Source override badges**: Cells with active `# @...` directives surface that in the header and infra panel rather than hiding it in source.

Mount, worker, timeout, and env changes persist to `notebook.toml` via notebook APIs. Worker assignment persists via `PUT /v1/notebooks/{id}/worker` and `PUT /v1/notebooks/{id}/cells/{cell_id}/worker`. Source annotations can now also be authored from the UI and rewrite the leading `# @...` block in the cell source.

### Annotation Parser (Fallback)

New file: `src/strata/notebook/annotations.py`

Annotations are a power-user fallback for when the UI is not available (e.g., scripting, CI, or editing `.py` cell files directly). They are parsed from the leading `#` comment block.

```python
"""Parse cell-level annotations from comment blocks.

Supported annotations:
    # @worker <name>          — Route to named worker
    # @timeout <seconds>      — Override execution timeout
    # @mount <name> <uri> <mode> — Add/override mount
    # @env <KEY>=<value>      — Set environment variable
"""

@dataclass
class CellAnnotations:
    worker: str | None = None
    timeout: float | None = None
    mounts: list[MountSpec] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)

def parse_annotations(source: str) -> CellAnnotations:
    """Extract annotations from the leading comment block of a cell."""
```

**Priority order** (highest wins): annotations > persisted cell metadata > notebook-level defaults.

---

## Data Model

All new types live in `models.py`. Shown together to avoid duplication.

```python
# ── Mount types ──────────────────────────────────────────

class MountMode(StrEnum):
    READ_ONLY = "ro"
    READ_WRITE = "rw"

class MountSpec(BaseModel):
    """A filesystem mount declaration."""
    name: str = Field(..., description="Mount name (becomes a Path variable in cell)")
    uri: str = Field(..., description="URI: file://, s3://, gs://, az://")
    mode: MountMode = Field(default=MountMode.READ_ONLY)
    pin: str | None = Field(default=None, description="Pinned version/etag — disables fingerprinting")

# ── Worker types ─────────────────────────────────────────

class WorkerBackendType(StrEnum):
    LOCAL = "local"
    EXECUTOR = "executor"  # Strata executor protocol (v2 pull model)

class WorkerSpec(BaseModel):
    """A named worker declaration."""
    name: str
    backend: WorkerBackendType = WorkerBackendType.LOCAL
    runtime_id: str | None = Field(
        default=None,
        description="Stable runtime fingerprint override for provenance",
    )
    config: dict[str, Any] = Field(default_factory=dict)

# ── Extended existing types ──────────────────────────────

class CellMeta(BaseModel):
    # ... existing fields (id, file, language, order) ...
    mounts: list[MountSpec] = Field(default_factory=list, description="Cell-level mount overrides")
    worker: str | None = Field(default=None, description="Worker name (from cell toolbar dropdown)")
    timeout: float | None = Field(default=None, description="Execution timeout override (seconds)")

class NotebookToml(BaseModel):
    # ... existing fields (notebook_id, name, cells, etc.) ...
    mounts: list[MountSpec] = Field(default_factory=list, description="Notebook-level mounts")
    workers: list[WorkerSpec] = Field(default_factory=list, description="Registered workers (personal/dev mode; service mode uses server-managed registry)")
```

`WorkerBackendType` intentionally has only two variants: `local` and `executor`. SSH machines and K8s clusters are accessed by running a Strata executor endpoint on them — there is no separate SSH or K8s backend in the notebook coordinator. See [Executor Protocol Bridge](#executor-protocol-bridge) below.

---

## Implementation Status

### Landed in the Codebase

**File mounts**

- `src/strata/notebook/models.py`, `parser.py`, and `writer.py` persist notebook-level mounts plus cell-level overrides in `notebook.toml`
- `src/strata/notebook/mounts.py` handles local paths, remote object-store materialization, RO fingerprinting, RW staging, and sync-back
- `src/strata/notebook/executor.py` includes mount fingerprints in provenance and treats any `rw` mount as non-cacheable
- `src/strata/notebook/harness.py` receives only local paths in the manifest
- the frontend has a notebook-level mounts panel plus cell-level override editing

**Runtime metadata and source annotations**

- `src/strata/notebook/annotations.py` parses `# @mount`, `# @worker`, `# @timeout`, and `# @env`
- the frontend can now author those annotations directly by rewriting the leading source annotation block
- persisted precedence is: source annotations > persisted cell metadata > notebook defaults

**Workers**

- `src/strata/notebook/workers.py` implements effective worker resolution, service-mode policy gating, health probing, health history, transport labeling, and server-managed registry helpers
- `src/strata/notebook/executor.py` supports:
  - local workers
  - named local workers
  - embedded executor workers
  - direct HTTP executor workers
  - signed/build-backed HTTP executor workers
- `src/strata/notebook/remote_bundle.py` and `src/strata/notebook/remote_executor.py` implement the bundle wire format and the reference notebook executor app
- `src/strata/notebook/session.py`, `routes.py`, and `ws.py` persist remote execution metadata into notebook cell state

**Service-mode worker administration**

- notebook-scoped worker definitions are editable only in personal/dev mode
- service mode uses a server-managed registry exposed from `src/strata/server.py`
- the frontend notebook worker panel can switch between notebook-local editing and service-mode admin CRUD when authorized

### Current API Surface

Notebook APIs:

```text
POST /v1/notebooks/open

PUT  /v1/notebooks/{id}/mounts
PUT  /v1/notebooks/{id}/cells/{cell_id}/mounts

GET  /v1/notebooks/{id}/workers
PUT  /v1/notebooks/{id}/workers                  # personal/dev mode only
PUT  /v1/notebooks/{id}/worker
PUT  /v1/notebooks/{id}/cells/{cell_id}/worker

PUT  /v1/notebooks/{id}/timeout
PUT  /v1/notebooks/{id}/cells/{cell_id}/timeout
PUT  /v1/notebooks/{id}/env
PUT  /v1/notebooks/{id}/cells/{cell_id}/env
```

Service-mode worker admin APIs:

```text
GET    /v1/admin/notebook-workers
PUT    /v1/admin/notebook-workers
POST   /v1/admin/notebook-workers
PUT    /v1/admin/notebook-workers/{worker_name}
PATCH  /v1/admin/notebook-workers/{worker_name}
DELETE /v1/admin/notebook-workers/{worker_name}
POST   /v1/admin/notebook-workers/{worker_name}/refresh
```

Remote executor APIs:

```text
GET  /health
POST /v1/execute
POST /v1/notebook-execute      # compatibility alias
POST /v1/execute-manifest
```

### Current Module Map

- `src/strata/notebook/mounts.py`
  - mount materialization
  - mount fingerprinting
  - RW sync-back
- `src/strata/notebook/annotations.py`
  - source annotation parsing
- `src/strata/notebook/workers.py`
  - worker policy
  - worker transport classification
  - worker health probing + history
  - service-mode registry helpers
- `src/strata/notebook/remote_bundle.py`
  - `notebook-output-bundle@v1` pack/unpack + validation
- `src/strata/notebook/remote_executor.py`
  - reference notebook executor app for direct + signed transport
- `src/strata/notebook/executor.py`
  - provenance
  - local/embedded/direct/signed dispatch
  - remote metadata propagation
- `src/strata/notebook/routes.py`
  - notebook-level mounts/workers/env/timeout endpoints
- `src/strata/server.py`
  - service-mode worker admin endpoints
- `frontend/src/components/MountsPanel.vue`
  - notebook-level mount editing
- `frontend/src/components/WorkersPanel.vue`
  - notebook worker catalog, health display, and service-mode admin UI
- `frontend/src/components/CellEditor.vue`
  - cell-level runtime controls and source annotation authoring

### Current Test Coverage

Representative coverage already in the repo:

- `tests/notebook/test_mounts.py`
- `tests/notebook/test_executor.py`
- `tests/notebook/test_remote_bundle.py`
- `tests/notebook/test_remote_executor.py`
- `tests/notebook/test_routes.py`
- `tests/notebook/test_ws.py`
- `tests/notebook/test_e2e_remote_workers.py`
- `tests/test_server_mode_transforms.py`

### Remaining Work

The design is now mostly implemented. The main remaining gaps are:

- unify notebook signed remote execution more deeply with the generic transform/`BuildRunner` plane if we want one production remote execution path instead of the current direct-vs-signed split
- add richer worker operations data than the current short health-history ring buffer
- build a broader server-managed worker admin surface beyond the notebook sidebar
- decide whether remote worker fleet management belongs inside Strata or remains external
- add stronger mount snapshot/versioning semantics if we want cacheable side-effecting `rw` cells

---

## Key Decisions

### Why fsspec (not PyArrow FS)?

PyArrow filesystems are already used in Strata core for Parquet reads, but fsspec is better for the notebook use case:
- Broader ecosystem: HDFS, FTP, HTTP, Hugging Face Hub, etc.
- Local caching layer (`filecache`) built in
- Standard in the data science ecosystem (pandas, dask, xarray all use it)
- PyArrow FS can be used as a backend for fsspec if needed

### Why not FUSE mounts?

FUSE (e.g., s3fs-fuse, gcsfuse) would give the most transparent experience but:
- Requires root/FUSE privileges (not available in many containers)
- Hard to control from Python
- Debugging is painful when FUSE has issues
- fsspec's filecache gives similar UX without kernel involvement

### Why inject paths, not fsspec filesystems?

Cells get `pathlib.Path` objects, not `fsspec.filesystem` objects. This means:
- Cell code uses standard Python I/O: `pd.read_parquet(mount_path / "file.parquet")`
- No SDK imports needed
- Works with any library that takes file paths
- The complexity of remote resolution is hidden in the executor

The tradeoff: remote mounts are materialized local directories, not live remote filesystems. Large remote directories can't be lazily listed or coherently shared without additional machinery. For that case, we could optionally expose an `fsspec.filesystem` under a different name (e.g., `raw_data_fs`) alongside the materialized path.
