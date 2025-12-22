# Strata

[![CI](https://github.com/fangchenli/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/ci.yml)
[![Pre-commit](https://github.com/fangchenli/strata/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/pre-commit.yml)
[![Docker](https://github.com/fangchenli/strata/actions/workflows/docker.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/docker.yml)

Snapshot-aware serving layer for Iceberg tables that provides durable, shared, read-optimized state (cache) for ephemeral analytical compute, returning Arrow batches to clients.

## Features

- **Snapshot-aware caching**: Cache keys include `snapshot_id`, ensuring immutable cached objects with no invalidation required
- **Row-group level caching**: Fine-grained caching at the Parquet row-group level using Arrow IPC format
- **Filter pruning**: Prune row groups using Parquet min/max statistics for numeric and timestamp columns
- **Streaming Arrow IPC**: Streams Arrow results without buffering entire scans in memory. Memory footprint scales with row group size, not query result size—a real production differentiator for large scans
- **Pre-flight size estimation**: Rejects oversized scans upfront (HTTP 413) using Parquet metadata, preventing wasted work
- **DuckDB integration**: Query cached data directly with DuckDB SQL
- **Polars integration**: Zero-copy DataFrame access via Arrow
- **Metrics**: Structured JSON logging for cache hits/misses, bytes transferred, and timing

## Installation

Requires Python 3.12+ and Rust (for the native extension).

```bash
# Using uv (recommended)
uv sync

# Or with pip
pip install -e .
```

## Quick Start

### 1. Start the Server

```bash
# Using the CLI
strata-server

# Or as a module
python -m strata
```

### 2. Query Data

```python
from strata import StrataClient
from strata.client import lt, gt

client = StrataClient()

# Scan a table
for batch in client.scan("file:///path/to/warehouse#namespace.table"):
    print(f"Got {batch.num_rows} rows")

# With column projection
for batch in client.scan(
    "file:///warehouse#db.events",
    columns=["id", "timestamp", "value"]
):
    process(batch)

# With filters (enables row-group pruning)
for batch in client.scan(
    "file:///warehouse#db.events",
    filters=[gt("value", 100), lt("timestamp", some_datetime)]
):
    process(batch)
```

### 3. DuckDB Integration

```python
from strata.duckdb_ext import StrataScanner

scanner = StrataScanner()
scanner.register("events", "file:///warehouse#db.events")

result = scanner.query("""
    SELECT category, COUNT(*), AVG(value)
    FROM events
    GROUP BY category
""")
print(result.to_pandas())
```

### 4. Polars Integration

```python
import polars as pl
from strata.polars_ext import scan_to_polars

df = scan_to_polars(
    "file:///warehouse#db.events",
    columns=["id", "value", "category"],
)
print(df.group_by("category").agg(pl.col("value").mean()))
```

## Configuration

Configure via `pyproject.toml`:

```toml
[tool.strata]
host = "0.0.0.0"
port = 8765
cache_dir = "/tmp/strata-cache"
max_cache_size_bytes = 10737418240  # 10 GB
batch_size = 65536

[tool.strata.catalog_properties]
type = "sql"
uri = "sqlite:///catalog.db"
```

Or programmatically:

```python
from strata.config import StrataConfig

config = StrataConfig(
    host="127.0.0.1",
    port=8765,
    cache_dir="/tmp/strata-cache",
)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                               │
│  (Python SDK / DuckDB / HTTP)                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Strata Server                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Planner   │  │   Fetcher   │  │   Disk Cache        │  │
│  │             │  │  (Python/   │  │   (Arrow IPC)       │  │
│  │ - Iceberg   │  │   Rust)     │  │                     │  │
│  │ - Pruning   │  │             │  │ key = hash(         │  │
│  │             │  │             │  │   table, snapshot,  │  │
│  └─────────────┘  └─────────────┘  │   file, rg, proj)   │  │
│                                     └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Storage Layer                            │
│  (Local filesystem, S3 - via pyiceberg)                     │
└─────────────────────────────────────────────────────────────┘
```

### Cache Key

```
hash(table_uri, snapshot_id, file_path, row_group_id, projection_fingerprint)
```

Since Iceberg snapshots are immutable, cached objects never need invalidation.

### Modules

- `server.py` - FastAPI HTTP server with streaming responses
- `client.py` - Python client SDK
- `planner.py` - Read planning with row-group pruning
- `fetcher.py` - Parquet reading with Rust acceleration
- `cache.py` - Disk cache using Arrow IPC
- `fast_io.py` - Rust-accelerated I/O operations
- `iceberg.py` - Iceberg catalog integration via pyiceberg
- `config.py` - Configuration with pyproject.toml support
- `types.py` - Core types (CacheKey, ReadPlan, Task, Filter)
- `metrics.py` - Structured metrics logging
- `metadata_store.py` - Persistent metadata storage
- `metadata_cache.py` - In-memory metadata caching
- `duckdb_ext.py` - DuckDB integration
- `polars_ext.py` - Polars integration

## API

### HTTP Endpoints

```
POST /v1/scan              Create a scan, returns scan metadata + estimated_bytes
GET  /v1/scan/{id}/batches Stream Arrow IPC batches
DELETE /v1/scan/{id}       Delete scan resources
POST /v0/cache/clear       Clear disk cache
GET  /health               Health check
GET  /metrics              Aggregate metrics
```

### Streaming Contract

The `/v1/scan/{id}/batches` endpoint provides **all-or-error** semantics:

- **Success**: Client receives complete Arrow IPC stream (schema + all batches + EOS marker)
- **Failure**: Connection is aborted, client receives truncated/error response

This means:
- Clients never receive silently truncated data
- Arrow IPC decode fails on incomplete streams, forcing error handling
- Clients should retry on transport errors

Resource limits that trigger abort:
- `scan_timeout_seconds`: Exceeded during streaming
- `max_response_bytes`: Cumulative response exceeds limit

Pre-flight checks (clean HTTP errors before streaming starts):
- `max_response_bytes`: Estimated size checked via HTTP 413
- `max_row_groups_per_scan`: Task count checked via HTTP 400

### Metrics Output

```json
{"event": "scan_complete", "timestamp": 1703123456.789, "scan_id": "123-abc",
 "cache_hits": 5, "cache_misses": 2, "bytes_from_cache": 1048576,
 "bytes_from_storage": 524288, "planning_time_ms": 12.5, "fetch_time_ms": 45.2}
```

## Development

```bash
# Install dev dependencies
uv sync --group dev

# Run tests
uv run pytest

# Run the hello world demo
uv run python examples/hello_world.py

# Format and lint (via pre-commit)
pre-commit run --all-files
```

### CI/CD

The project uses GitHub Actions for continuous integration:

- **CI** - Tests on Python 3.12, 3.13, and 3.14 across Ubuntu, macOS, and Windows
- **Pre-commit** - Linting and formatting with ruff
- **Docker** - Builds and tests the Docker image

### Docker

```bash
# Build and run with Docker Compose
docker compose up -d

# Or build manually
docker build -t strata .
docker run -p 8765:8765 -v /path/to/warehouse:/data strata
```

## Future Work

- **Distributed cache**: Shared cache across multiple servers
- **Query pushdown**: Push more filters to Parquet/Iceberg layer
- **Write path**: Support writing data back to Iceberg tables

## License

Apache-2.0
