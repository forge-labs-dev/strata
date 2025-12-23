# Strata

[![CI](https://github.com/fangchenli/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/ci.yml)
[![Pre-commit](https://github.com/fangchenli/strata/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/pre-commit.yml)
[![Docker](https://github.com/fangchenli/strata/actions/workflows/docker.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/docker.yml)

**A Snapshot-Aware Serving Layer for Iceberg Tables**

Modern data lakes are built on immutable files and versioned metadata, yet every query engine still treats reads as a fresh execution problem.

Strata changes that.

Strata is a snapshot-aware serving layer for Apache Iceberg tables. It turns Iceberg snapshots into reusable, persistent read artifacts that can be fetched efficiently, streamed safely, and reused across queries, processes, and restarts.

Instead of re-planning and re-reading the same Parquet data over and over, Strata:

- Understands Iceberg snapshot semantics
- Materializes stable row-group read units
- Persists them across process lifetimes
- Serves them directly in Arrow IPC stream format

The result is predictable, low-latency reads for repeated queries—without changing file formats, rewriting engines, or sacrificing correctness.

## Why Strata Exists

Iceberg snapshots are immutable. Parquet row groups are immutable.
Arrow IPC streams are already the wire format most engines consume.

Yet today, every scan:

- Re-resolves manifests
- Re-reads metadata
- Re-parses Parquet
- And discards the result on restart

Strata sits between storage and execution and treats immutable data like immutable data should be treated: as cacheable, restart-safe serving artifacts.

## What Strata Is (and Isn't)

Strata is **not** a query engine.
Strata is **not** a SQL layer.
Strata is **not** just an in-memory cache.

Strata is a long-lived service that:

- Plans reads using Iceberg metadata
- Prunes at file and row-group granularity
- Persists Arrow IPC streams on disk
- Streams results with bounded memory

Engines like DuckDB, Polars, and Spark can fetch data from Strata as if they were reading a local Arrow stream—except the expensive work has already been done.

## When Strata Helps Most

Strata shines when:

- The same Iceberg snapshot is queried repeatedly
- Workloads are interactive or dashboard-driven
- Cold starts are expensive
- Object storage latency dominates

Typical speedups range from 2–3× on warm reads, with correctness guaranteed by Iceberg snapshot immutability.

## Design Principles

- **Correctness first**: Cache keys include snapshot identity
- **Conservative pruning**: If in doubt, read rather than risk dropping data
- **Bounded memory**: Large scans stream incrementally
- **No magic**: All data served is valid Arrow IPC

Strata is intentionally simple—because the right abstraction often is.

## How Strata Works

Strata sits between query engines and Iceberg-backed storage.
It does not execute queries. Instead, it plans, materializes, and serves snapshot-consistent read units.

At a high level, a Strata scan has three phases:

1. **Plan** – Resolve what needs to be read (cheap, metadata-only)
2. **Fetch** – Read immutable row groups (expensive, I/O-bound)
3. **Serve** – Stream Arrow IPC bytes to the client (cheap, CPU-light)

Because Iceberg snapshots and Parquet row groups are immutable, Strata can safely persist the results of phases (1) and (2).

### 1. Planning: Snapshot-Aware Read Planning

When a client requests a scan, Strata:

1. Resolves the Iceberg table and snapshot ID
2. Loads (or reuses) the snapshot's manifest resolution
3. Applies conservative pruning using:
   - Iceberg file-level metadata
   - Parquet row-group statistics (min/max, row counts)
4. Produces a `ReadPlan` consisting of independent row-group tasks

Each task represents:
- A specific data file
- A specific row group
- A specific column projection
- A specific snapshot

This makes every task fully deterministic and cacheable.

**Key property**: Planning is metadata-only. No Parquet data is read during this phase.

### 2. Fetching: Immutable Row-Group Materialization

Each task is executed independently:

**If a cached result exists:**
- Strata reads raw bytes directly from disk

**Otherwise:**
- Strata reads the Parquet row group
- Projects requested columns
- Writes the result as an Arrow IPC stream
- Persists it to disk

The cache key includes:
- Table identity
- Snapshot ID
- File path
- Row group ID
- Projection fingerprint

Because all of these inputs are immutable, cached results remain valid forever.

**Important distinction**: Strata caches execution results, not raw Parquet files.

### 3. Serving: True Streaming, Bounded Memory

For multi-row-group scans, Strata does not buffer the full result.

Instead:
- Each cached or freshly fetched row group is an Arrow IPC stream
- Streams are concatenated incrementally
- Bytes are yielded as soon as they are produced

This ensures:
- O(single row group) memory usage
- Safe handling of multi-gigabyte scans
- Immediate backpressure on slow clients

If a client disconnects, exceeds size limits, or times out:
- Streaming stops immediately
- Resources are released
- Partial results are discarded (by design)

### Metadata Caching and Restart Behavior

Strata maintains two distinct caches:

**Persistent (on disk):**
- Iceberg manifest resolutions
- Parquet file metadata
- Arrow IPC row-group streams

These survive process restarts.

**In-memory (per process):**
- Open Parquet handles
- Hot metadata
- Active scan state

These reset on restart.

As a result:
- **Cold start**: full planning + fetch
- **Warm cache**: fast planning + zero I/O
- **Post-restart**: slower planning, fast fetch

This behavior is intentional and predictable.

### Why This Is Safe

Strata relies on three guarantees:

1. Iceberg snapshots are immutable
2. Parquet row groups are immutable
3. Arrow IPC streams are deterministic

As long as these hold (and they do), cached results are correct by construction.

If anything cannot be proven safe to prune or reuse, Strata defaults to reading more data—never less.

### What Strata Does Not Do

Strata deliberately avoids:
- SQL parsing
- Query optimization
- Joins, aggregations, or filtering on data
- Speculative caching

Those responsibilities belong to query engines.

Strata's job is to serve snapshot-consistent data efficiently.

### Mental Model

If it helps, think of Strata as:

> "A content-addressable CDN for Iceberg snapshots, where the objects are Arrow streams."

Or, more bluntly:

> "What query engines wish object storage behaved like."

## Features

- **Snapshot-aware caching**: Cache keys include `snapshot_id`, ensuring immutable cached objects with no invalidation required
- **Row-group level caching**: Fine-grained caching at the Parquet row-group level using Arrow IPC format
- **Two-tier filter pruning**: Prunes files using Iceberg manifest statistics, then row groups using Parquet min/max statistics
- **Streaming Arrow IPC**: Streams Arrow results without buffering entire scans in memory. Memory footprint scales with row group size, not query result size
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

# With filters (enables two-tier pruning)
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
- `planner.py` - Read planning with two-tier pruning
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

- **CI** - Tests on Python 3.12, 3.13, and 3.14 across Ubuntu and macOS
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
