# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Strata?

Strata is a snapshot-aware serving layer for Apache Iceberg tables. It caches Parquet row groups as Arrow IPC streams, keyed by immutable snapshot IDs—eliminating cache invalidation complexity. The server streams results with bounded memory, and a Rust extension accelerates I/O.

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
hash(table_identity | snapshot_id | file_path | row_group_id | projection_fingerprint)
```

`TableIdentity` is canonical (`catalog.namespace.table`) to avoid cache duplication from URI variations.

### Key Modules

- **types.py** - Core types: `CacheKey`, `ReadPlan`, `Task`, `Filter`, `TableIdentity`
- **planner.py** - `ReadPlanner` resolves snapshots, applies pruning, builds plans
- **cache.py** - `DiskCache` + `CachedFetcher` for row-group caching
- **server.py** - FastAPI endpoints, streaming responses, resource limits
- **metadata_cache.py** - Two-level `ManifestCache` (filtered + unfiltered) and `ParquetMetadataCache`
- **fast_io.py** - Python wrapper for Rust extension functions
- **rust/src/lib.rs** - Arrow IPC stream manipulation (concatenation, format conversion)

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
- S3 access: `STRATA_S3_REGION`, `STRATA_S3_ACCESS_KEY`, `STRATA_S3_SECRET_KEY`, `STRATA_S3_ANONYMOUS`

## Important Invariants

1. **Immutability guarantees correctness**: Iceberg snapshots and Parquet row groups are immutable, so cached results are valid forever for a given cache key.

2. **Conservative pruning**: If pruning cannot be proven safe, read more data rather than risk dropping rows. See `_should_prune_row_group` exception handling.

3. **Bounded memory streaming**: Response size is O(single row group), not O(query result). Server yields chunks immediately.

4. **Pre-flight size checks**: Estimated response size is computed from Parquet metadata before streaming begins—oversized scans return HTTP 413.
