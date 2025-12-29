# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Strata?

Strata is a snapshot-aware serving layer for Apache Iceberg tables. It caches Parquet row groups as Arrow IPC streams, keyed by immutable snapshot IDs—eliminating cache invalidation complexity. The server streams results with bounded memory, uses two-tier QoS to prevent bulk queries from starving dashboards, and a Rust extension accelerates I/O. Supports both local filesystem and S3 storage backends.

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

Strata supports multi-tenant deployments with complete cache isolation between tenants:

- **Tenant identification**: `X-Tenant-ID` header (injected by API gateway after JWT validation)
- **Cache isolation**: Tenant ID is hashed into cache keys and directory paths
- **Per-tenant metrics**: Scans, cache hits, bytes tracked per tenant
- **Validation**: Tenant IDs must be 1-64 chars, alphanumeric with `_` and `-`

Key modules:
- **tenant.py** - `TenantConfig`, `TenantQuotas`, context management (`get_tenant_id()`, `set_tenant_id()`)
- **tenant_registry.py** - `TenantRegistry` with LRU eviction (max 1000 tenants tracked)

### Key Modules

- **types.py** - Core types: `CacheKey`, `ReadPlan`, `Task`, `Filter`, `TableIdentity`
- **planner.py** - `ReadPlanner` resolves snapshots, applies pruning, builds plans; S3 path normalization utilities
- **cache.py** - `DiskCache` + `CachedFetcher` for row-group caching with LRU eviction
- **server.py** - FastAPI endpoints, streaming responses, two-tier QoS admission control, prefetch
- **config.py** - Configuration with S3 support (`s3_region`, `s3_endpoint_url`, etc.) and timeout settings
- **metadata_cache.py** - Two-level `ManifestCache` (filtered + unfiltered) and `ParquetMetadataCache`
- **metadata_store.py** - SQLite-backed persistent metadata storage
- **fetcher.py** - `PyArrowFetcher` reads Parquet row groups with S3 filesystem support
- **fast_io.py** - Python wrapper for Rust extension functions
- **tracing.py** - OpenTelemetry integration (optional, requires `strata[otel]`)
- **logging.py** - Structured JSON logging with correlation IDs (request_id, scan_id, trace_id)
- **rust/src/lib.rs** - Arrow IPC stream manipulation (concatenation, format conversion)

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

## Important Invariants

1. **Immutability guarantees correctness**: Iceberg snapshots and Parquet row groups are immutable, so cached results are valid forever for a given cache key.

2. **Conservative pruning**: If pruning cannot be proven safe, read more data rather than risk dropping rows. See `_should_prune_row_group` exception handling.

3. **Bounded memory streaming**: Response size is O(single row group), not O(query result). Server yields chunks immediately.

4. **Pre-flight size checks**: Estimated response size is computed from Parquet metadata before streaming begins—oversized scans return HTTP 413.

5. **Two-tier QoS isolation**: Interactive (dashboard) and bulk (ETL) queries use separate semaphores to prevent starvation. Classification based on estimated bytes and column count.

6. **S3 path normalization**: S3 paths are normalized (double slashes, `.`, `..` resolved) to ensure consistent cache keys. See `_normalize_s3_path` in `planner.py`.

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

Benchmarks in `benchmarks/`:
- `stress_test.py` - Multi-user load testing with QoS validation
- `bench_restart.py` - Cold/warm start performance
