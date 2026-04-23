# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Strata?

Strata is a **materialization and persistence layer** for long-running, iterative, and expensive computations.

It provides a single primitive:
```
materialize(inputs, transform) → artifact
```

This primitive ensures that:
- Results are **immutable and versioned**
- Identical computations are **deduplicated** (via provenance hash)
- Lineage is **explicit and inspectable**
- Reuse is **correct by construction**

Strata is designed to sit **below orchestration** and **outside execution**. It is not a workflow engine, scheduler, DAG runner, or query engine. Those responsibilities belong elsewhere.

### Why This Matters

Long-horizon workflows (AI agents, data pipelines, evaluation loops) have these properties:
- **Expensive**: LLM calls, embeddings, large scans
- **Iterative**: evaluate → refine → repeat
- **Branching**: explore multiple variants
- **Failure-prone**: crashes, retries, restarts are normal

What breaks first is not compute—it's **state**. Strata makes state explicit and durable.

### The Layering Model

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

### Iceberg Table Scanning

Strata also provides snapshot-aware scanning for Apache Iceberg tables. It caches Parquet row groups as Arrow IPC streams, keyed by immutable snapshot IDs—eliminating cache invalidation complexity. The server streams results with bounded memory, uses two-tier QoS to prevent bulk queries from starving dashboards, and a Rust extension accelerates I/O. Supports local filesystem, S3, GCS, and Azure Blob Storage backends.

## Build & Development Commands

```bash
# Install dependencies and build Rust extension
uv sync

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_smoke.py -v

# Run a specific test
uv run pytest tests/test_smoke.py::TestPlanner::test_basic_planning -v

# Format and lint
pre-commit run --all-files

# Type check (ty — Astral's fast Python type checker)
uv run ty check src/

# Start the server (development)
uv run python -m strata

# Start server with custom warehouse
uv run python -m strata --warehouse "file:///path/to/warehouse"
```

## Architecture Overview

### Request Flow (Unified Materialize API)

1. **Client** sends `POST /v1/materialize` with inputs (table URIs) and transform (e.g., `scan@v1`)
2. **Server** checks artifact cache via provenance hash → returns immediately if cache hit
3. **Planner** (`planner.py`) resolves Iceberg snapshot → produces `ReadPlan` with row-group `Task`s
4. **Fetcher** (`cache.py`) checks disk cache, reads Parquet if miss, writes Arrow IPC to cache
5. **Server** streams Arrow IPC bytes via `GET /v1/streams/{stream_id}` while persisting to artifact store
6. On completion, artifact is finalized and available for future cache hits

### Two-Tier Pruning

Filters are applied at two levels:
1. **Iceberg file-level**: Uses manifest statistics to skip entire files (`filters_to_iceberg_expression` in `types.py`)
2. **Parquet row-group level**: Uses min/max statistics to skip row groups (`_should_prune_row_group` in `planner.py`)

### Cache Key Structure

```
hash(tenant_id | table_identity | snapshot_id | file_path | row_group_id | projection_fingerprint)
```

`TableIdentity` is canonical (`catalog.namespace.table`) to avoid cache duplication from URI variations.

### Deployment Modes

Strata runs in one of two deployment modes, selected via `deployment_mode`
(env: `STRATA_DEPLOYMENT_MODE`, default `service`):

- **`personal`** — single-user local deployment. Writes are enabled
  (`writes_enabled=True`), artifacts persist to `~/.strata/artifacts` by
  default, and the server refuses to bind to non-loopback addresses unless
  `allow_remote_clients_in_personal=True` is set. Intended for a developer
  running `strata-server` on their own laptop alongside the notebook UI.
- **`service`** — multi-user hosted deployment. Writes are disabled at the
  server surface (`writes_enabled=False`); client-initiated artifact
  creation must flow through server-side transforms. Expected to sit
  behind a trusted proxy that authenticates requests and injects
  `X-Strata-Principal` / `X-Strata-Tenant` headers.

**Mode-dependent flags** (coherence enforced by `validate_mode_coherence` in
`config.py`; invalid combinations raise at startup):

| Flag                       | Personal        | Service             |
| -------------------------- | --------------- | ------------------- |
| `writes_enabled`           | always `True`   | always `False`      |
| `auth_mode`                | must be `none`  | typically `trusted_proxy` |
| `multi_tenant_enabled`     | must be `False` | `True` or `False`   |
| `require_tenant_header`    | must be `False` | `True` or `False`   |
| `artifact_dir` default     | `~/.strata/artifacts` | none (must be explicit if blob backend is local) |
| Non-loopback bind          | only with `allow_remote_clients_in_personal=True` | unrestricted |

**Mode-independent flags**: `rate_limit_enabled`, ACL rules, S3/GCS/Azure
blob backend configuration, tracing, logging. These apply in either mode.

**Notebook-specific personal-only endpoints** (see
`src/strata/notebook/routes.py`): session discovery/reconnect and notebook
deletion are gated to personal mode via `_require_personal_mode_*`
helpers — the service-mode frontend doesn't expose these either.

### Multi-Tenancy

Strata supports multi-tenant deployments with complete isolation between tenants:

- **Tenant identification**: `X-Tenant-ID` header (injected by API gateway after JWT validation)
- **Cache isolation**: Tenant ID is hashed into cache keys and directory paths
- **Per-tenant QoS**: Each tenant gets their own `ResizableLimiter` pools (interactive/bulk slots)
- **Per-tenant metrics**: Scans, cache hits, bytes tracked per tenant
- **Validation**: Tenant IDs must be 1-64 chars, alphanumeric with `_` and `-`

Key modules:
- **tenant.py** - `TenantConfig`, `TenantQuotas`, context management (`get_tenant_id()`, `set_tenant_id()`)
- **tenant_registry.py** - `TenantRegistry` with LRU eviction (max 1000 tenants tracked), `get_or_create_limiters()`

### Trusted Proxy Authorization

Strata uses a trusted proxy model for authentication and authorization. It does NOT handle authentication itself—it trusts identity headers injected by an upstream proxy.

**Threat model**: Only the proxy can reach Strata (private network / security group / k8s NetworkPolicy).

**Identity headers** (injected by proxy):
- `X-Strata-Principal` - Stable user/service ID (required when auth enabled)
- `X-Strata-Tenant` - Team/org ID (optional, same as multi-tenancy tenant)
- `X-Strata-Scopes` - Space-separated permission scopes (e.g., `scan:create admin:cache`)
- `X-Strata-Proxy-Token` - Shared secret for proxy verification

**ACL evaluation order**:
1. Deny rules checked first - if any match, access denied
2. Allow rules checked - if any match, access allowed
3. Default action applied (`allow` or `deny`)

**Enforcement points**:
- `POST /v1/materialize` - ACL check on table/artifact access
- `GET /v1/streams/{id}` - Stream ownership check (only creator can retrieve)
- `POST /v1/cache/clear` - Requires `admin:cache` scope

**Key types**:
- `Principal` - Authenticated identity (`id`, `tenant`, `scopes`)
- `TableRef` - Canonical table reference for ACL pattern matching (`catalog:namespace.table`)
- `AclRule` - Single ACL rule (`principal`, `tenant`, `tables` patterns)
- `AclConfig` - ACL configuration (`default`, `deny_rules`, `allow_rules`)

Key modules:
- **auth.py** - `AuthError`, `verify_proxy_token()`, `parse_principal()`, `AclEvaluator`, context management (`get_principal()`, `set_principal()`)
- **types.py** - `Principal`, `TableRef` types; `ReadPlan.owner_principal`, `ReadPlan.owner_tenant` fields
- **config.py** - `AclRule`, `AclConfig`, auth settings (`auth_mode`, `proxy_token`, etc.)

### Artifact Store & Transforms

Strata supports materializing query results as reusable artifacts. The artifact system enables:
- **Deduplication**: Same inputs + transform → return existing artifact (via provenance hash)
- **Chaining**: Artifacts can be inputs to other transforms (DAG pipelines)
- **Naming**: Human-readable aliases for artifact versions (e.g., `daily_summary`)
- **Lineage**: Track input dependencies and downstream dependents

**Artifact lifecycle (via unified API)**:
1. `POST /v1/materialize` - Unified endpoint for all data access
2. `GET /v1/streams/{stream_id}` - Stream data immediately (for `mode="stream"`)
3. `GET /v1/artifacts/{id}/v/{version}` - Get artifact metadata
4. `GET /v1/artifacts/{id}/v/{version}/data` - Fetch persisted artifact data (for `mode="artifact"` or cache hits)

**Key types**:
- `ArtifactVersion` - Immutable artifact metadata (id, version, state, provenance_hash, schema, row_count)
- `ArtifactName` - Mutable name pointer to specific artifact version
- `TransformSpec` - Transform specification (executor, params, inputs)
- `BuildState` - Async build lifecycle state (pending, building, ready, failed)

**Key modules**:
- **artifact_store.py** - SQLite-backed artifact metadata, name pointers, lineage queries, blob I/O delegation
- **blob_store.py** - Pluggable blob storage abstraction (LocalBlobStore, S3BlobStore, GCSBlobStore, AzureBlobStore)
- **transforms/registry.py** - Transform definitions (executor URL, timeout, max output size)
- **transforms/runner.py** - Background build runner, executor HTTP protocol, lease-based claiming

**Blob storage backends**:
- `LocalBlobStore` - Local filesystem (default), atomic writes via temp+rename
- `S3BlobStore` - Amazon S3 / S3-compatible (MinIO, LocalStack) via PyArrow S3FileSystem
- `GCSBlobStore` - Google Cloud Storage via PyArrow GcsFileSystem
- `AzureBlobStore` - Azure Blob Storage via azure-storage-blob SDK (requires `strata[azure]`)
- Configure via `STRATA_ARTIFACT_BLOB_BACKEND` (`local`, `s3`, `gcs`, or `azure`)
- S3: `STRATA_ARTIFACT_S3_BUCKET`, `STRATA_ARTIFACT_S3_PREFIX`
- GCS: `STRATA_ARTIFACT_GCS_BUCKET`, `STRATA_ARTIFACT_GCS_PREFIX`, `STRATA_GCS_PROJECT_ID`
- Azure: `STRATA_ARTIFACT_AZURE_CONTAINER`, `STRATA_ARTIFACT_AZURE_PREFIX`, `STRATA_AZURE_CONNECTION_STRING` (or account_key/SAS/DefaultAzureCredential)

**Additional modules**:
- **transforms/build_store.py** - Build state tracking, lease management, orphan recovery
- **transforms/build_metrics.py** - Build duration, throughput, queue wait metrics

**Executor protocol v1** (push model - Strata sends inputs):
```
POST {executor_url}/v1/execute
Content-Type: multipart/form-data
X-Strata-Executor-Protocol: v1

Parts:
  - metadata (application/json): build_id, tenant, principal, transform spec
  - input0, input1, ... (application/vnd.apache.arrow.stream)

Response: Arrow IPC stream
```

**Executor protocol v2** (pull model - executor pulls via signed URLs):
- Strata sends a `BuildManifest` with signed URLs for inputs and output
- Executor downloads inputs from `inputs[].url`, uploads result to `output.url`, then POSTs to `finalize_url`
- Benefits: No bandwidth bottleneck at Strata, supports large inputs/outputs, executor can parallelize downloads
- Key module: **transforms/signed_urls.py** - `SignedDownloadURL`, `SignedUploadURL`, `BuildManifest`, HMAC-SHA256 signing
- Enable via `pull_model_enabled=True` and `signed_url_expiry_seconds` config

### Key Modules

- **types.py** - Core types: `CacheKey`, `ReadPlan`, `Task`, `Filter`, `TableIdentity`, `Principal`, `TableRef`
- **planner.py** - `ReadPlanner` resolves snapshots, applies pruning, builds plans; S3 path normalization utilities
- **cache.py** - `DiskCache` + `CachedFetcher` for row-group caching with LRU eviction
- **server.py** - FastAPI endpoints, streaming responses, two-tier QoS admission control, prefetch, artifact API
- **config.py** - Configuration with S3 support (`s3_region`, `s3_endpoint_url`, etc.) and timeout settings
- **artifact_store.py** - Artifact metadata, blob storage, name pointers, provenance deduplication
- **metadata_cache.py** - Two-level `ManifestCache` (filtered + unfiltered) and `ParquetMetadataCache`
- **metadata_store.py** - SQLite-backed persistent metadata storage
- **fetcher.py** - `PyArrowFetcher` reads Parquet row groups with S3 filesystem support
- **fast_io.py** - Python wrapper for Rust extension functions
- **tracing.py** - OpenTelemetry integration (optional, requires `strata[otel]`)
- **logging.py** - Structured JSON logging with correlation IDs (request_id, scan_id, trace_id)
- **auth.py** - Trusted proxy authentication, principal parsing, ACL evaluation
- **tenant_acl.py** - Tenant-scoped authorization helpers for multi-tenant isolation
- **rust/src/lib.rs** - Arrow IPC stream manipulation (concatenation, format conversion)

### Transforms Modules

- **transforms/registry.py** - `TransformRegistry`, `TransformDefinition` (executor URL, timeouts, limits)
- **transforms/runner.py** - `BuildRunner` background worker, executor HTTP protocol, concurrency control
- **transforms/build_store.py** - `BuildStore`, `BuildState`, lease-based claiming, orphan recovery
- **transforms/build_metrics.py** - Build throughput, latency, queue wait time metrics
- **transforms/build_qos.py** - Per-tenant build concurrency limits
- **transforms/signed_urls.py** - Pull model signed URL generation (`BuildManifest`, `SignedDownloadURL`, `SignedUploadURL`)

### Observability Modules

- **rate_limiter.py** - Token bucket rate limiting with global, per-client, per-endpoint limits
- **health.py** - Comprehensive health checks for disk, metadata, memory, thread pools
- **circuit_breaker.py** - Circuit breaker pattern for external dependency protection
- **cache_metrics.py** - Cache eviction tracking with pressure levels
- **cache_stats.py** - Time-windowed cache hit/miss histogram
- **pool_metrics.py** - Thread pool utilization metrics

### Rust Extension

Located in `rust/`, built via maturin. Narrow scope — only two live
entry points, both on genuine hot paths:

- `read_file_bytes` — mmap-based cache read (wrapped by `fast_io.read_file_mmap`)
- `concat_ipc_streams` — byte-level Arrow IPC stream concatenation for
  buffered multi-row-group responses, skipping deserialize/reserialize
  (wrapped by `fast_io.concat_stream_bytes`)

The extension is exposed as `strata._strata_core` and wrapped by `fast_io.py`.
Everything else in the data plane stays in Python / PyArrow / orjson —
those are already C/C++ under the hood.

## Client SDK Usage

The Python client provides a simple API for fetching data:

```python
from strata.client import StrataClient, lt, gt

# Connect to server
client = StrataClient(base_url="http://localhost:8765")

# Materialize an Iceberg table (uses scan@v1 transform)
artifact = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={
        "executor": "scan@v1",
        "params": {"columns": ["id", "value"], "filters": [{"column": "id", "op": "gt", "value": 100}]}
    },
)
print(f"Cache hit: {artifact.cache_hit}")
print(f"Artifact URI: {artifact.uri}")

# Fetch the artifact data as Arrow table
table = client.fetch(artifact.uri)

# Close when done
client.close()
```

**Integration modules**:
```python
# Pandas integration
from strata.integration.pandas import fetch_to_pandas
df = fetch_to_pandas("file:///warehouse#db.events")

# Polars integration
from strata.integration.polars import fetch_to_polars
df = fetch_to_polars("file:///warehouse#db.events")

# DuckDB integration
from strata.integration.duckdb import StrataScanner
with StrataScanner() as scanner:
    scanner.register("events", "file:///warehouse#db.events")
    result = scanner.query("SELECT * FROM events WHERE id > 100")
```

## Testing Patterns

Tests create temporary Iceberg warehouses using fixtures:

```python
@pytest.fixture
def temp_warehouse(tmp_path):
    # Creates SqlCatalog + sample table with PyArrow data
    ...
    return {"warehouse_path": ..., "table_uri": ..., "catalog": ..., "table": ...}
```

Most tests use `test_db.events` table with columns: `id`, `value`, `name`, `timestamp`.

## Configuration

`StrataConfig` loads from `pyproject.toml` under `[tool.strata]` or environment variables:

- `STRATA_HOST`, `STRATA_PORT` - Server binding
- `STRATA_CACHE_DIR` - Disk cache location
- `STRATA_MAX_CACHE_SIZE_BYTES` - Cache size limit
- S3: `STRATA_S3_REGION`, `STRATA_S3_ENDPOINT_URL`, `STRATA_S3_ACCESS_KEY`, `STRATA_S3_SECRET_KEY`, `STRATA_S3_ANONYMOUS`
- QoS: `interactive_slots`, `bulk_slots`, `interactive_max_bytes`, `interactive_max_columns`
- Tracing: `STRATA_TRACING_ENABLED`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`
- Logging: `STRATA_LOG_LEVEL`, `STRATA_LOG_FORMAT` (json or text)
- Timeouts: `plan_timeout_seconds`, `scan_timeout_seconds`, `fetch_timeout_seconds`, `s3_connect_timeout_seconds`, `s3_request_timeout_seconds`
- Rate limiting: `rate_limit_enabled`, `rate_limit_global_rps`, `rate_limit_client_rps`, `rate_limit_scan_rps`
- Multi-tenancy: `STRATA_MULTI_TENANT_ENABLED`, `STRATA_TENANT_HEADER`, `STRATA_REQUIRE_TENANT_HEADER`
- Auth: `STRATA_AUTH_MODE`, `STRATA_PROXY_TOKEN`, `STRATA_PRINCIPAL_HEADER`, `STRATA_SCOPES_HEADER`, `STRATA_HIDE_FORBIDDEN_AS_NOT_FOUND`

## Important Invariants

1. **Immutability guarantees correctness**: Iceberg snapshots and Parquet row groups are immutable, so cached results are valid forever for a given cache key.

2. **Conservative pruning**: If pruning cannot be proven safe, read more data rather than risk dropping rows. See `_should_prune_row_group` exception handling.

3. **Bounded memory streaming**: Response size is O(single row group), not O(query result). Server yields chunks immediately.

4. **Pre-flight size checks**: Estimated response size is computed from Parquet metadata before streaming begins—oversized scans return HTTP 413.

5. **Two-tier QoS isolation**: Interactive (dashboard) and bulk (ETL) queries use separate semaphores to prevent starvation. Classification based on estimated bytes and column count. In multi-tenant mode, each tenant gets their own limiter pools for complete QoS isolation.

6. **S3 path normalization**: S3 paths are normalized (double slashes, `.`, `..` resolved) to ensure consistent cache keys. See `_normalize_s3_path` in `planner.py`.

7. **Authorization is deny-first**: When `auth_mode=trusted_proxy`, deny rules are evaluated before allow rules. This ensures explicit denials cannot be bypassed by allow rules.

8. **Cache remains shared across principals**: ACL only gates the ability to request scans and retrieve results—cache artifacts are still shared. This preserves Strata's main performance advantage.

## Testing

Key test files:
- `test_smoke.py` - Core functionality (planning, caching, streaming)
- `test_hardening.py` - Edge cases, error handling, resource limits
- `test_qos.py` - Two-tier admission control, fast-fail behavior
- `test_prefetch.py` - Background prefetching
- `test_s3_config.py` - S3 configuration and filesystem creation
- `test_s3_moto.py` - S3 path handling (uses moto for mocking)
- `test_semaphore_leak.py` - Resource cleanup on errors
- `test_rate_limiter.py` - Token bucket rate limiting
- `test_health.py` - Dependency health checks
- `test_circuit_breaker.py` - Circuit breaker pattern
- `test_cache_metrics.py` - Cache eviction tracking
- `test_cache_stats.py` - Cache hit/miss histogram
- `test_timeout_config.py` - Timeout configuration
- `test_tracing.py` - OpenTelemetry integration
- `test_multitenancy.py` - Tenant context, registry, cache isolation, validation
- `test_auth.py` - Trusted proxy auth, principal parsing, ACL evaluation, scan ownership

Benchmarks in `benchmarks/`:
- `stress_test.py` - Multi-user load testing with QoS validation
- `bench_restart.py` - Cold/warm start performance

## Strata Notebook

Strata Notebook is a content-addressed compute graph over Python with an interactive notebook UX. It uses Strata's artifact store as the sole persistence layer for inter-cell variable passing — every cell output is an artifact, every cell execution is a `materialize(inputs, transform) → artifact` operation.

### How It Fits the Layering Model

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

The notebook is an **orchestration layer** over Strata. It decides what to run next (cascade planning, staleness tracking). The cell harness is an **executor**. Strata's artifact store decides whether results already exist and persists them.

### Notebook File Format

```
notebook_dir/
├── notebook.toml          # Stable config: notebook_id, name, cells, workers, mounts, env, ai
├── pyproject.toml         # uv config (requires-python, [tool.uv])
├── uv.lock                # Locked dependencies
├── cells/
│   ├── {cell_id}.py       # Cell source code (8-char UUID prefix)
│   └── ...
└── .strata/               # Gitignored — runtime state, not committed
    ├── runtime.json       # Per-cell display outputs, provenance hashes, env metadata
    ├── console/           # Per-cell stdout/stderr snapshots ({cell_id}.json)
    └── artifacts/
        ├── artifacts.sqlite
        └── blobs/
            └── nb_{notebook_id}_cell_{cell_id}_var_{var}@v=N.arrow
```

**Split between `notebook.toml` and `.strata/runtime.json`** — the former holds only
stable config that should be version-controlled; anything that changes on every
execution or background sync (display outputs, last `uv sync` timestamp, per-cell
provenance hashes) lives in `.strata/runtime.json`. `updated_at` on `notebook.toml`
is reserved for structural edits (add/remove/reorder cell; change env/worker/
timeout/mounts). Runtime writers (`update_cell_display_outputs`,
`update_cell_console_output`, `persist_cell_provenance`) never touch `notebook.toml`.

On first open of a legacy notebook (pre-split) `parse_notebook()` runs a one-time
migration via `runtime_state.migrate_from_legacy_notebook_toml()` that moves the
`[artifacts]` / `[environment]` / `[cache]` sections out of `notebook.toml` and
into `runtime.json`.

`notebook.toml` example:
```toml
notebook_id = "f7bd9094-..."
name = "my_analysis"
cells = [
    { id = "77da7050", file = "77da7050.py", language = "python", order = 0 },
    { id = "1d019ae7", file = "1d019ae7.py", language = "python", order = 0.5 },
]
```

**Sensitive env blanking**: `_serialize_env` strips values from keys matching
`KEY`, `SECRET`, `TOKEN`, `PASSWORD`, `CREDENTIAL` patterns before persisting.
`update_notebook_env` additionally skips the `[env]` block entirely when every
entry is empty or a blanked sensitive placeholder — so typing an API key in the
Runtime panel doesn't churn committed notebooks. When sensitive keys coexist
with real config, the blanked slot stays as a "which vars are configured"
reminder.

### Cell Execution Flow

Python cells (the default path):

1. **Provenance hash**: `sha256(sorted_input_hashes + source_hash + env_hash)`
2. **Cache check**: `artifact_store.find_by_provenance(hash)` → return immediately on hit
3. **Resolve inputs**: For each upstream variable, load artifact blob from store, write to temp dir
4. **Execute**: Spawn subprocess running `harness.py` in notebook's venv
5. **Harness**: Deserializes inputs → `exec(source, namespace)` → serializes new variables
6. **Store outputs**: Each consumed variable becomes an artifact: `nb_{notebook_id}_cell_{cell_id}_var_{var_name}`
7. **Broadcast**: WebSocket sends `cell_status`, `cell_output`, `cell_console` to connected clients

Prompt cells (`prompt_executor.execute_prompt_cell`): skip the subprocess harness;
render the `{{ var }}` template with upstream values, dispatch via
`llm.chat_completion` (Anthropic native `/v1/messages` tool-use when a schema
is set, OpenAI-compat `/v1/chat/completions` otherwise), validate the response
against `@output_schema` with retry-on-failure, then store the (possibly parsed
JSON) response as the output artifact. Provenance hash includes a schema
fingerprint so schema edits invalidate cached responses.

Loop cells (`@loop max_iter=N carry=var`) extend the Python path: one harness
subprocess per iteration, the `carry` variable is threaded between iterations,
each iteration's state is stored as `…@iter=k` artifacts, and the final state
becomes the cell's canonical artifact. WebSocket emits `cell_iteration_progress`
after each iteration.

Serialization formats (determined by value type, see `serializer.py::ContentType`):
- `arrow/ipc` — Anything Arrow-representable: pyarrow Tables/RecordBatch,
  pandas DataFrames/Series, numpy ndarrays (any dim), numpy scalars,
  and typed Python primitives (datetime, Decimal, UUID, bytes, complex).
  Shape is encoded in schema metadata (`strata.arrow.shape` =
  "table" | "tensor" | "scalar"). One wire format; the reader
  reconstructs the exact Python type on the way out.
- `json/object` — dicts, lists, scalars (int, float, str, bool, None)
- `pickle/object` — everything else (cloudpickle by default)
- `image/png` — display-only figure/image outputs
- `text/markdown` — display-only markdown outputs
- `module/import`, `module/cell`, `module/cell-instance` — module objects and cell-defined classes

Content type is stored in the artifact's `transform_spec.params.content_type` so the read side knows how to deserialize.

All preview values, manifest writes, and TOML persistence go through
`serializer.to_serialization_safe` — a single boundary that coerces
None/datetime/Decimal/numpy scalars to JSON- and TOML-safe primitives.

The object codec is configurable via `STRATA_NOTEBOOK_OBJECT_CODEC`
(default: `cloudpickle`; falls back to stdlib `pickle` if cloudpickle
is missing).

### DAG & Variable Analysis

Each cell is analyzed via AST to extract `defines` (top-level assignments) and `references` (free variables). The DAG builder connects references to producers:

- **Variable producer**: Last cell that defines each variable (handles shadowing)
- **`consumed_variables[cell_id]`**: Variables from this cell that downstream cells reference (drives what gets stored as artifacts)
- **Topological order**: Valid execution sequence via Kahn's algorithm
- **Cycle detection**: Self-references and cycles raise errors

The DAG is rebuilt on every cell source change (`re_analyze_cell` → `_analyze_and_build_dag`).

### Source Annotations

Cells can carry metadata in leading `#` comments, parsed by
`annotations.py`. Annotations always win over persisted notebook
defaults — they are the canonical cell-level config surface.

Python cells:
- `# @name Human Readable Name` — display name shown in DAG
- `# @worker <name>` — route execution to a named worker
- `# @timeout 30` — override execution timeout (seconds)
- `# @env KEY=value` — per-cell environment variable
- `# @mount data s3://bucket/prefix ro` — declare a filesystem mount
- `# @loop max_iter=N carry=var [start_from=<cell>@iter=k]` — loop cell
- `# @loop_until <expr>` — loop termination predicate

Prompt cells (parsed by `prompt_analyzer.py`):
- `# @name <identifier>` — output variable name (default: `result`)
- `# @model <model_id>` — override the notebook-level LLM model
- `# @temperature <float>` — sampling temperature (default 0.0)
- `# @max_tokens <int>` — output-token ceiling
- `# @system <text>` — system prompt
- `# @output json` — force JSON response; auto-applied when `@output_schema` is set
- `# @output_schema {...}` — inline JSON Schema pinning the response shape.
  OpenAI gets `response_format: json_schema` with strict mode (auto-injects
  `additionalProperties: false`, relaxes strict when the user's `required` list
  leaves properties optional). Anthropic dispatches to native `/v1/messages`
  tool-use with the schema as `input_schema`. Other providers fall back to
  `response_format: json_object` (valid JSON, shape not enforced).
- `# @validate_retries N` — total attempts for the validate-and-retry loop
  (1 initial + N-1 retries; default 3). After every LLM call the response is
  validated against `@output_schema` with `jsonschema`; on failure, the prior
  response and path-addressed errors are fed back as a retry turn. Cumulative
  input/output tokens flow into the artifact's `transform_spec`; retry count
  surfaces via `CellExecutionResult.validation_retries` for the UI.

Mounts inject `pathlib.Path` variables into the cell namespace. URI
schemes: `file://`, `s3://`, `gs://`, `az://`. Notebook-level mounts
live in `notebook.toml`; `# @mount` overrides them per-cell.
`MountSpec.options` carries fsspec storage options (e.g. `anon=true`
for public S3). See `mounts.py::MountResolver`.

### Annotation Validation

`annotation_validation.py` cross-checks parsed annotations against
notebook-wide context and emits `AnnotationDiagnostic` warnings:

- `worker_unknown` — `@worker` name not in catalog
- `mount_uri_unsupported` — unrecognized URI scheme
- `mount_shadows_notebook` — overrides a notebook-level mount (info)
- `timeout_not_numeric` — non-numeric or non-positive `@timeout`
- `env_malformed` — `@env` missing `KEY=value` format
- `loop_missing_max_iter` / `loop_missing_carry` / `loop_carry_unknown` /
  `loop_until_syntax_error` / `loop_start_from_unknown` — loop-cell issues
- `prompt_output_schema_invalid` — `@output_schema` wasn't valid JSON or
  wasn't a JSON object (prompt cells)
- `module_export_blocked` — cell defines a def/class but non-literal
  top-level runtime logic blocks cross-cell sharing; emitted pre-flight
  so users see it before execution fails

Validation runs on notebook open, reload (worker catalog change), and
after each WS source flush — never on every keystroke. Diagnostics
are advisory: they surface as a header pill and backend log warnings,
but never block execution.

### Cascade Execution

When a cell's upstream dependencies aren't "ready" (idle, stale, or error), the `CascadePlanner` generates a plan:

1. BFS backwards from target cell to find all upstream cells needing execution
2. Returns cells in topological order with reasons (stale/missing/target)
3. WebSocket sends `cascade_prompt` → frontend auto-accepts → `cell_execute_cascade` → sequential execution

Backend tracks cell status in `session.notebook_state.cells[i].status` (mutable field) to avoid false cascade triggers.

### REST API (`/v1/notebooks`)

- `POST /create` — Create notebook (parent_path, name) → returns notebook state + session_id
- `POST /open` — Open existing notebook directory
- `GET /{id}/cells` — List cells
- `PUT /{id}/cells/{cell_id}` — Update source, re-analyze, return cell + DAG
- `POST /{id}/cells` — Add cell (optional `after_cell_id`)
- `DELETE /{id}/cells/{cell_id}` — Remove cell
- `POST /{id}/cells/{cell_id}/execute` — Execute cell (REST endpoint)
- `GET /{id}/dag` — Get DAG (edges, topological_order, leaves, roots, variable_producer)

Note: `{id}` in routes is the **session ID** (from `session_id` field in create/open response), not the notebook_id from `notebook.toml`.

### WebSocket Protocol (`/v1/notebooks/ws/{notebook_id}`)

**Client → Server**:
- `cell_execute` — Run cell (triggers cascade check)
- `cell_execute_cascade` — Confirmed cascade execution
- `cell_execute_force` — Run cell ignoring staleness
- `cell_source_update` — Source changed
- `notebook_sync` — Request full state
- `notebook_run_all` — Run every non-empty cell in order
- `impact_preview_request` — Get upstream/downstream effects
- `inspect_open/eval/close` — REPL operations

**Server → Client**:
- `cell_status` — Status changed (with causality chain)
- `cell_output` — Execution result (outputs, stdout, stderr, cache_hit)
- `cell_error` — Execution failed (surfaces stacktrace / provider error body)
- `cell_console` — Incremental stdout/stderr during execution
- `cell_iteration_progress` — Per-iteration progress from `@loop` cells
- `cascade_prompt` — Upstream cells need execution (plan_id, steps)
- `cascade_progress` — Progress during cascade
- `dag_update` — DAG changed after cell edit
- `impact_preview` — Upstream + downstream effects of running a cell
- `notebook_state` — Full state snapshot (response to `notebook_sync`)

### Notebook Backend Modules

- **models.py** — `NotebookToml`, `CellState`, `NotebookState`, `CellStaleness`, `AnnotationDiagnostic`, `ArtifactInfo`
- **parser.py** — `parse_notebook()` reads notebook.toml + cell files
- **writer.py** — `create_notebook()`, `write_cell()`, `add_cell_to_notebook()`, `remove_cell_from_notebook()`
- **session.py** — `NotebookSession` (state, DAG, artifact manager, execution history), `SessionManager`
- **analyzer.py** — `VariableAnalyzer` AST visitor extracts defines/references
- **annotations.py** — Parse `# @worker|@mount|@timeout|@env|@name` directives
- **annotation_validation.py** — Cross-reference validation → `AnnotationDiagnostic` warnings
- **runtime_state.py** — `.strata/runtime.json` reader/writer, legacy-section migration, per-cell provenance persistence
- **mounts.py** — `MountResolver` materializes file/s3/gs/az mounts via fsspec
- **dag.py** — `NotebookDag`, `build_dag()` with topological sort and cycle detection
- **cascade.py** — `CascadePlanner` determines upstream cells that need re-execution
- **impact.py** — `ImpactAnalyzer` shows full consequences (upstream cascade + downstream staleness)
- **executor.py** — `CellExecutor` orchestrates execution: provenance check → resolve inputs → spawn harness → store outputs
- **harness.py**, **pool_worker.py** — Subprocess entry points (load manifest, exec source, serialize outputs)
- **serializer.py** — Content-type detection, serialize/deserialize, `to_serialization_safe` boundary, `ContentType` StrEnum
- **artifact_integration.py** — `NotebookArtifactManager` wraps ArtifactStore with notebook-specific ID scheme
- **provenance.py** — `compute_provenance_hash()`, `compute_source_hash()` (AST-normalized, whitespace-insensitive)
- **env.py** — `compute_lockfile_hash()` for environment-based cache invalidation
- **causality.py** — `CausalityChain` explains why a cell is stale
- **routes.py** — FastAPI REST router (`/v1/notebooks`)
- **ws.py** — WebSocket handler with per-notebook connections and execution state
- **inspect_repl.py** — Interactive REPL for exploring cell artifacts
- **pool.py** — Warm process pool for faster subprocess reuse
- **workers.py** — Worker catalog, health probes, transport resolution
- **remote_executor.py**, **remote_bundle.py** — HTTP executor protocol for remote workers
- **prompt_analyzer.py**, **prompt_executor.py**, **llm.py** — LLM-powered prompt cells
- **immutability.py** — Mutation detection on input variables (`MutationWarning` TypedDict)
- **dependencies.py** — `uv add`/`uv remove`, requirements/environment.yaml import, lockfile sync

### Frontend (`frontend/`)

Vue 3 + TypeScript + Vite. Dev server connects to `http://localhost:8765` (via `VITE_STRATA_URL`).

**Store** (`stores/notebook.ts`):
- `boot()` — Creates scratch notebook + adds one cell + connects WebSocket
- `updateSource(id, src)` is **local-only**: updates `cell.source` and
  marks the cell dirty. No network call during typing.
- Dirty cells flush via WS `cell_source_update` after 2s of idle, on
  editor blur (focusout), or immediately before Shift+Enter execution.
  The backend broadcasts `dag_update` + `cell_status` asynchronously.
- Other cell mutations (add, remove, reorder) still go through REST.
- `cascade_prompt` handler auto-accepts (no user confirmation needed)
- `executeCellWebSocket()` waits for WS connection before sending

**Composables**:
- `useWebSocket(notebookId)` — WebSocket lifecycle, reconnection with exponential backoff, message dispatch
- `useStrata()` — REST client for notebook CRUD operations
- `useCodemirror()` — CodeMirror editor integration with Shift+Enter to run

**Components**:
- `CellEditor.vue` — Cell with editor, status indicator, run/add/delete actions, cache badges, causality tooltips
- `DagView.vue` — Visual DAG rendering
- `ImpactPreview.vue` — Cascade plan + downstream impact display
- `ProfilingPanel.vue` — Execution metrics and per-cell profiling
- `InspectPanel.vue` — REPL for exploring cell outputs

### Running the Notebook

```bash
# Start backend (serves both API and built frontend)
uv run uvicorn strata.server:app --host 0.0.0.0 --port 8765

# Start frontend dev server (with hot reload, proxies to backend)
cd frontend && npm run dev
```

### Key Invariants

1. **Artifact store is the sole source of truth** for inter-cell variable passing. There is no in-memory cache layer — all cell outputs are persisted as artifacts and read back from the store.

2. **Cell IDs are backend-generated** (8-char UUID prefix). The frontend never generates cell IDs.

3. **DAG is authoritative on the backend**. The frontend sends source updates via WebSocket (debounced, fire-and-forget). Backend re-analyzes, rebuilds the DAG, and broadcasts `dag_update` + `cell_status` messages asynchronously. The frontend merges authoritative defines/references/upstream/downstream and staleness from these broadcasts — it never blocks on a round-trip during typing.

4. **Cell status is tracked on the session object**. After execution, `session.notebook_state.cells[i].status` is updated to "ready" or "error". This is critical — the cascade planner checks these statuses to decide if upstream cells need re-execution.

5. **Annotations beat persisted config**. `# @worker`, `# @mount`, `# @timeout`, `# @env` in cell source always override notebook-level defaults at resolution time. There is no UI editor for per-cell persisted overrides — annotations are the single per-cell configuration surface.

6. **`notebook.toml` is committed config, `.strata/` is runtime state**. Anything that changes on every execution or background sync (display outputs, console snapshots, per-cell provenance hashes, `uv sync` timestamps) lives in `.strata/runtime.json` or `.strata/console/` and is gitignored. `notebook.toml` writers bump `updated_at` only on structural edits (add/remove/reorder cell; change worker/timeout/env/mounts/ai). Runtime writers never touch `notebook.toml`.

7. **Prompt-cell console is a Cell field, not part of the output**. `CellState.console_stdout` / `console_stderr` ride alongside the display output instead of being folded into it. Frontend renders them in a separate panel so `@output_schema` cells keep a clean structured display value.
