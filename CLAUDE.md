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

# Start the server (development)
uv run python -m strata

# Start server with custom warehouse
uv run python -m strata --warehouse "file:///path/to/warehouse"
```

## Architecture Overview

### Request Flow

1. **Client** sends `POST /v1/scan` with table URI, optional filters, columns
2. **Planner** (`planner.py`) resolves Iceberg snapshot → produces `ReadPlan` with row-group `Task`s
3. **Fetcher** (`cache.py`) checks disk cache, reads Parquet if miss, writes Arrow IPC to cache
4. **Server** (`server.py`) streams cached Arrow IPC bytes via `GET /v1/scan/{id}/batches`

### Two-Tier Pruning

Filters are applied at two levels:
1. **Iceberg file-level**: Uses manifest statistics to skip entire files (`filters_to_iceberg_expression` in `types.py`)
2. **Parquet row-group level**: Uses min/max statistics to skip row groups (`_should_prune_row_group` in `planner.py`)

### Cache Key Structure

```
hash(tenant_id | table_identity | snapshot_id | file_path | row_group_id | projection_fingerprint)
```

`TableIdentity` is canonical (`catalog.namespace.table`) to avoid cache duplication from URI variations.

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
- `POST /v1/scan` - ACL check on table access
- `GET /v1/scan/{id}/batches` - Scan ownership check (only creator can retrieve)
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

**Artifact lifecycle**:
1. `POST /v1/artifacts/materialize` - Start materialization (returns `build_id` if async)
2. `GET /v1/artifacts/builds/{build_id}` - Poll build status (for service-mode async builds)
3. `GET /v1/artifacts/{id}/v/{version}` - Get artifact metadata
4. `GET /v1/artifacts/{id}/v/{version}/data` - Stream artifact data as Arrow IPC

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

Located in `rust/`, built via maturin. Provides:
- `read_arrow_ipc_as_stream` - Memory-mapped file read → IPC stream conversion
- `concat_ipc_streams` - Fast stream concatenation by byte manipulation
- `file_to_stream_format` - IPC file → stream format conversion

The extension is exposed as `strata._strata_core` and wrapped by `fast_io.py`.

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
