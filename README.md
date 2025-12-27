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
- **Two-tier QoS**: Separate admission control for interactive (dashboard) and bulk (ETL) queries, preventing starvation
- **Parallel row group fetching**: Configurable parallelism with out-of-order completion and reordering buffer for maximum I/O throughput
- **Prefetch**: Background prefetching of first row group during scan creation for lower latency
- **S3 support**: Native S3 storage backend via PyArrow S3FileSystem with configurable timeouts
- **DuckDB integration**: Query cached data directly with DuckDB SQL
- **Polars integration**: Zero-copy DataFrame access via Arrow
- **Rate limiting**: Token bucket rate limiting with global, per-client, and per-endpoint limits
- **Health checks**: Comprehensive dependency health monitoring (disk, metadata store, memory, thread pools)
- **Circuit breaker**: Protection against cascading failures from external dependencies
- **Metrics**: Structured JSON logging with Prometheus export for cache, QoS, rate limiting, and health metrics
- **OpenTelemetry tracing**: Optional distributed tracing for observability (install with `pip install strata[otel]`)

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

# QoS settings
interactive_slots = 8      # Slots for dashboard queries
bulk_slots = 4             # Slots for ETL/export queries
interactive_max_bytes = 10485760  # 10 MB threshold
interactive_max_columns = 10

# Fetch parallelism
fetch_parallelism = 4      # Max concurrent row group fetches per scan

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
    # S3 configuration
    s3_endpoint_url="http://localhost:9000",  # MinIO
    s3_access_key="minioadmin",
    s3_secret_key="minioadmin",
)
```

### S3 Configuration

For S3 storage backends, configure via environment variables:

```bash
# AWS S3
export STRATA_S3_REGION=us-west-2
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# MinIO / S3-compatible
export STRATA_S3_ENDPOINT_URL=http://localhost:9000
export STRATA_S3_ACCESS_KEY=minioadmin
export STRATA_S3_SECRET_KEY=minioadmin

# Public buckets
export STRATA_S3_ANONYMOUS=true
```

### Parallel Fetching

Control how many row groups are fetched concurrently within a single scan:

```bash
# Default: 4 concurrent fetches per scan
export STRATA_FETCH_PARALLELISM=4

# For high-latency storage (S3), increase parallelism:
export STRATA_FETCH_PARALLELISM=8

# For local SSD, lower values may be sufficient:
export STRATA_FETCH_PARALLELISM=2
```

The fetch thread pool is sized automatically based on `fetch_parallelism * (interactive_slots + bulk_slots)`.

### OpenTelemetry Tracing

Strata supports distributed tracing via OpenTelemetry. Install the optional dependencies:

```bash
pip install strata[otel]
# or with uv
uv sync --extra otel
```

Configure via environment variables:

```bash
# Enable tracing with OTLP exporter
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_SERVICE_NAME=strata

# Optional: adjust sampling
export OTEL_TRACES_SAMPLER=parentbased_traceidratio
export OTEL_TRACES_SAMPLER_ARG=0.1  # 10% sampling

# Disable tracing (even if OTel is installed)
export STRATA_TRACING_ENABLED=false
```

Traced operations:
- `plan_scan` - Scan planning with manifest resolution
- `resolve_manifests` - Iceberg manifest resolution (cache miss)
- `fetch_row_group` - Parquet row group fetches (cache miss)
- HTTP endpoints via FastAPI auto-instrumentation

### Structured Logging

Strata uses structured JSON logging with automatic correlation IDs:

```bash
# Log format: json (default) or text (for development)
export STRATA_LOG_FORMAT=json

# Log level: DEBUG, INFO, WARNING, ERROR
export STRATA_LOG_LEVEL=INFO
```

Log entries include:
- `request_id` - Unique ID for each HTTP request (auto-generated or from `X-Request-ID` header)
- `scan_id` - Scan identifier for correlation across log entries
- `trace_id`, `span_id` - OpenTelemetry trace context (when tracing enabled)

Example log output:
```json
{"level": "info", "logger": "strata.server", "message": "Scan created", "request_id": "a1b2c3d4e5f6g7h8", "scan_id": "scan-123", "table_uri": "file:///warehouse#db.events", "tasks": 10, "planning_ms": 42.5}
```

The `X-Request-ID` header is echoed back in responses for client-side correlation.

### Rate Limiting

Strata includes token bucket rate limiting to protect against overload:

```python
from strata.config import StrataConfig

config = StrataConfig(
    rate_limit_enabled=True,
    rate_limit_global_rps=1000.0,      # Global requests/sec
    rate_limit_global_burst=100.0,      # Max burst above rate
    rate_limit_client_rps=100.0,        # Per-client requests/sec
    rate_limit_client_burst=20.0,       # Per-client burst
    rate_limit_scan_rps=50.0,           # Scan endpoint limit
    rate_limit_warm_rps=10.0,           # Cache warm endpoint limit
)
```

When rate limited, clients receive HTTP 429 with a `Retry-After` header.

### Health Checks

The `/health/dependencies` endpoint provides detailed health status for all dependencies:

```json
{
  "status": "healthy",
  "checks": [
    {"name": "disk_cache", "status": "healthy", "latency_ms": 0.5},
    {"name": "metadata_store", "status": "healthy", "latency_ms": 1.2},
    {"name": "arrow_memory", "status": "healthy", "latency_ms": 0.1},
    {"name": "thread_pools", "status": "healthy", "latency_ms": 0.1},
    {"name": "rate_limiter", "status": "healthy", "latency_ms": 0.0},
    {"name": "cache_evictions", "status": "healthy", "latency_ms": 0.0}
  ],
  "summary": {"total": 6, "healthy": 6, "degraded": 0, "unhealthy": 0}
}
```

Health status levels:
- **healthy**: All systems operating normally
- **degraded**: System functional but performance may be impacted (e.g., disk >90% full)
- **unhealthy**: Critical issues that may prevent operation

### Circuit Breaker

Strata uses circuit breakers to protect against cascading failures from external dependencies:

```python
from strata.circuit_breaker import get_circuit_breaker, CircuitBreakerConfig

# Get or create a circuit breaker for S3 operations
breaker = get_circuit_breaker("s3", CircuitBreakerConfig(
    failure_threshold=5,        # Open after 5 failures
    success_threshold=3,        # Close after 3 successes in half-open
    reset_timeout_seconds=30.0, # Try half-open after 30s
))

# Use as context manager
with breaker:
    result = call_s3_operation()

# Or as decorator
@breaker
def fetch_from_s3():
    ...
```

Circuit breaker states:
- **CLOSED**: Normal operation, requests pass through
- **OPEN**: Dependency is failing, requests fail fast with `CircuitOpenError`
- **HALF_OPEN**: Testing if dependency has recovered

Monitor circuit breakers via `/v1/debug/circuit-breakers` endpoint.

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
- `rate_limiter.py` - Token bucket rate limiting
- `health.py` - Dependency health checks
- `circuit_breaker.py` - Circuit breaker for external dependencies
- `cache_metrics.py` - Cache eviction tracking
- `cache_stats.py` - Cache hit/miss histogram
- `pool_metrics.py` - Thread pool metrics

## API

### HTTP Endpoints

```
POST /v1/scan              Create a scan, returns scan metadata + estimated_bytes
GET  /v1/scan/{id}/batches Stream Arrow IPC batches
DELETE /v1/scan/{id}       Delete scan resources
POST /v1/cache/warm        Warm cache for specified tables
POST /v1/cache/clear       Clear disk cache
GET  /v1/cache/histogram   Cache hit/miss statistics with time windows
GET  /v1/cache/evictions   Cache eviction metrics and pressure level
GET  /health               Liveness check
GET  /health/ready         Readiness check (capacity, stuck scans)
GET  /health/dependencies  Detailed dependency health checks
GET  /v1/config/timeouts   View all timeout configuration
GET  /metrics              Aggregate metrics (JSON)
GET  /metrics/prometheus   Prometheus format metrics
GET  /v1/debug/rate-limits Rate limiter statistics
GET  /v1/debug/circuit-breakers Circuit breaker status
GET  /v1/debug/pools       Thread pool metrics
GET  /v1/debug/memory      Memory profiling (requires tracemalloc)
```

### Cache Warming

The `/v1/cache/warm` endpoint preloads data into the cache for faster subsequent queries:

```bash
curl -X POST http://localhost:8765/v1/cache/warm \
  -H "Content-Type: application/json" \
  -d '{
    "tables": ["file:///warehouse#db.events", "file:///warehouse#db.users"],
    "columns": ["id", "timestamp", "value"],
    "max_row_groups": 100,
    "concurrent": 4
  }'
```

Response:
```json
{
  "tables_warmed": 2,
  "row_groups_cached": 85,
  "row_groups_skipped": 15,
  "bytes_written": 1073741824,
  "elapsed_ms": 5432.1,
  "errors": []
}
```

Use cases:
- Warm cache after server restart
- Preload data before dashboard traffic spikes
- Ensure low latency for critical tables

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

JSON format (`GET /metrics`):

```json
{
  "scan_count": 100,
  "cache_hits": 85,
  "cache_misses": 15,
  "cache_hit_rate": 0.85,
  "qos": {
    "interactive_slots": 8,
    "interactive_active": 3,
    "bulk_slots": 4,
    "bulk_active": 1
  },
  "prefetch": {
    "started": 100,
    "used": 95,
    "wasted": 5,
    "efficiency": 0.95
  }
}
```

Prometheus format available at `GET /metrics/prometheus`.

### Grafana Dashboard

A pre-built Grafana dashboard is available at `grafana/strata-dashboard.json`. It provides comprehensive visualization of Strata metrics:

**Overview Row:**
- Total scans, active scans, cache hit rate, cache size, rows returned, server status

**Cache Performance:**
- Cache hit rate over time
- Data throughput (cache vs storage)
- Cache size vs limit
- Cache evictions

**QoS (Quality of Service):**
- Interactive/bulk slot usage vs limits
- Rejected queries

**Prefetch Performance:**
- Prefetch efficiency (used/started ratio)
- Prefetch operations breakdown (started, used, wasted, skipped)
- Prefetches in flight

**Scan Performance:**
- Active scans over time
- Row groups pruned
- Row throughput (rows/sec)

**Errors & Aborts:**
- Timeout aborts, size limit aborts, client disconnects
- Error rate over time

**Metadata Cache:**
- Parquet/manifest cache hit rates
- Cache entry counts

**To import the dashboard:**

1. In Grafana, go to Dashboards → Import
2. Upload `grafana/strata-dashboard.json` or paste its contents
3. Select your Prometheus data source
4. Click Import

The dashboard supports:
- Multiple Prometheus data sources via dropdown
- Instance filtering for multi-instance deployments
- Auto-refresh (10s default)

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
