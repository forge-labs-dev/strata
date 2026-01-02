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

**Is Strata for you?**

| Good fit | Not a fit |
|----------|-----------|
| Dashboard/BI reads against Iceberg | Joins, aggregations (use a query engine) |
| Repeated scans on immutable snapshots | Real-time mutable tables |
| Serving Arrow to DuckDB/Polars | Sub-second write latency requirements |

## Quick Start (2 minutes)

**1. Install** (Python 3.12+, Rust required)
```bash
uv sync  # or: pip install -e .
```

**2. Start server** (in one terminal)
```bash
strata-server
```

**3. Run the demo** (in another terminal)
```bash
uv run python examples/hello_world.py
```

This creates a 100K-row Iceberg table and runs cold → warm → restart benchmarks. You'll see cache speedup immediately.

**Or use your own table:**
```python
from strata import StrataClient

client = StrataClient()
for batch in client.scan("file:///path/to/warehouse#namespace.table"):
    print(f"Got {batch.num_rows} rows")
```

**Streaming guarantee:** All-or-error. You get a complete Arrow IPC stream or the connection aborts—never silently truncated data.

---

## Why Strata?

Iceberg snapshots are immutable. Parquet row groups are immutable. Yet today, every scan re-resolves manifests, re-reads metadata, re-parses Parquet, and discards the result on restart.

Strata sits between storage and execution and treats immutable data like immutable data should be treated: as cacheable, restart-safe serving artifacts.

**When Strata shines:**
- Same Iceberg snapshot queried repeatedly
- Interactive or dashboard-driven workloads
- Cold starts are expensive
- Object storage latency dominates

### Benchmark It Yourself

```bash
uv run python benchmarks/bench_restart.py --rows 100000
```

This runs cold start → warm cache → server restart → warm cache, proving cache persistence works. You'll see output like:

```
Phase              Total       Plan      Fetch     Hits     Miss
1. Cold Start     245.3ms    42.1ms    198.2ms        0        5
2. Warm Cache      18.7ms    12.3ms      4.1ms        5        0
3. Post-Restart    21.2ms    15.8ms      3.9ms        5        0
```

For multi-scale comparison: `uv run python benchmarks/bench_restart.py --scale`

## Features

**Core** — what every user gets:
- Snapshot-aware caching (cache keys include `snapshot_id`, no invalidation)
- Row-group level caching in Arrow IPC format
- Two-tier filter pruning (Iceberg manifests → Parquet statistics)
- Streaming with bounded memory (O(row group), not O(result))
- S3 and local filesystem support
- DuckDB & Polars integration

**Operations** — production readiness:
- Two-tier QoS (interactive vs bulk query isolation)
- Rate limiting, health checks, pre-flight size limits
- Prometheus metrics, structured JSON logging
- Multi-tenancy with cache isolation

**Optional** — enable if needed:
- OpenTelemetry tracing (`pip install strata[otel]`)
- Circuit breakers for external dependencies

## Usage Examples

### Column Projection and Filters

```python
from strata import StrataClient
from strata.client import lt, gt

client = StrataClient()

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

### DuckDB Integration

```python
from strata.integration.duckdb import StrataScanner

scanner = StrataScanner()
scanner.register("events", "file:///warehouse#db.events")

result = scanner.query("""
    SELECT category, COUNT(*), AVG(value)
    FROM events
    GROUP BY category
""")
print(result.to_pandas())
```

### Polars Integration

```python
import polars as pl
from strata.integration.polars import scan_to_polars

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

# QoS settings
interactive_slots = 8
bulk_slots = 4

# Fetch parallelism
fetch_parallelism = 4
```

Or programmatically:

```python
from strata.config import StrataConfig

config = StrataConfig(
    host="127.0.0.1",
    port=8765,
    cache_dir="/tmp/strata-cache",
    s3_endpoint_url="http://localhost:9000",  # MinIO
)
```

### S3 Configuration

```bash
# AWS S3
export STRATA_S3_REGION=us-west-2
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# MinIO / S3-compatible
export STRATA_S3_ENDPOINT_URL=http://localhost:9000
export STRATA_S3_ACCESS_KEY=minioadmin
export STRATA_S3_SECRET_KEY=minioadmin
```

See [Configuration Reference](#configuration-reference) for all options.

---

## How It Works

Strata sits between query engines and Iceberg-backed storage. It does not execute queries—it plans, materializes, and serves snapshot-consistent read units.

### Three Phases

1. **Plan** – Resolve what to read (metadata-only, cheap)
2. **Fetch** – Read immutable row groups (I/O-bound, expensive)
3. **Serve** – Stream Arrow IPC bytes (CPU-light, cheap)

Because Iceberg snapshots and Parquet row groups are immutable, Strata can safely persist the results of phases 1 and 2.

### Cache Key Structure

```
hash(tenant_id, table_uri, snapshot_id, file_path, row_group_id, projection_fingerprint)
```

Since Iceberg snapshots are immutable, cached objects never need invalidation. In multi-tenant mode, tenant ID is included in the cache key for complete isolation.

### Design Principles

- **Correctness first**: Cache keys include snapshot identity
- **Conservative pruning**: If in doubt, read rather than risk dropping data
- **Bounded memory**: Large scans stream incrementally (O(single row group))
- **No magic**: All data served is valid Arrow IPC

### What Strata Is Not

Strata is **not** a query engine, SQL layer, or in-memory cache. It's a long-lived service that:
- Plans reads using Iceberg metadata
- Prunes at file and row-group granularity
- Persists Arrow IPC streams on disk
- Streams results with bounded memory

Engines like DuckDB, Polars, and Spark can fetch data from Strata as if reading a local Arrow stream—except the expensive work has already been done.

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

---

## API Reference

### HTTP Endpoints

```
POST /v1/scan              Create a scan, returns metadata + estimated_bytes
GET  /v1/scan/{id}/batches Stream Arrow IPC batches
DELETE /v1/scan/{id}       Delete scan resources
POST /v1/cache/warm        Warm cache for specified tables
POST /v1/cache/clear       Clear disk cache
GET  /health               Liveness check
GET  /health/ready         Readiness check
GET  /health/dependencies  Detailed dependency health
GET  /metrics              Aggregate metrics (JSON)
GET  /metrics/prometheus   Prometheus format
GET  /v1/admin/tenants     List all tenants (multi-tenant mode)
GET  /v1/admin/tenants/{id} Get tenant metrics

# Artifact API (materialized views / transforms)
POST /v1/artifacts/materialize           Start artifact materialization
GET  /v1/artifacts/{id}/v/{version}      Get artifact metadata
GET  /v1/artifacts/{id}/v/{version}/data Stream artifact data as Arrow IPC
GET  /v1/artifacts/builds/{build_id}     Poll async build status
POST /v1/artifacts/upload/{id}/v/{ver}   Upload artifact data (personal mode)
POST /v1/artifacts/finalize              Finalize artifact after upload
GET  /v1/artifacts/{id}/v/{ver}/lineage  Get artifact input dependencies
GET  /v1/artifacts/{id}/v/{ver}/dependents Find downstream dependents
PUT  /v1/artifacts/names/{name}          Set name alias for artifact version
GET  /v1/artifacts/names/{name}          Get artifact by name
```

### Cache Warming

Preload data for faster subsequent queries:

```bash
curl -X POST http://localhost:8765/v1/cache/warm \
  -H "Content-Type: application/json" \
  -d '{
    "tables": ["file:///warehouse#db.events"],
    "columns": ["id", "timestamp", "value"],
    "max_row_groups": 100
  }'
```

---

## Configuration Reference

### Security & Authorization

Strata uses a **trusted proxy** authentication model. It does not handle authentication itself—it trusts identity headers injected by an upstream proxy (nginx, Envoy, Kong, etc.) that handles JWT/OAuth validation.

**Threat model assumption:** Only the proxy can reach Strata (via private network, security group, or k8s NetworkPolicy). Clients cannot call Strata directly.

**Enable trusted proxy auth:**

```bash
export STRATA_AUTH_MODE=trusted_proxy
export STRATA_PROXY_TOKEN=your-shared-secret  # Proxy must send this token
```

**Identity headers** (injected by proxy after authentication):

| Header | Required | Description |
|--------|----------|-------------|
| `X-Strata-Principal` | Yes | Stable user/service ID |
| `X-Strata-Tenant` | No | Team/org ID (same as multi-tenancy) |
| `X-Strata-Scopes` | No | Space-separated scopes (e.g., `scan:create admin:cache`) |
| `X-Strata-Proxy-Token` | Yes* | Shared secret for proxy verification |

*Required when `STRATA_PROXY_TOKEN` is configured.

**ACL configuration** (in `pyproject.toml`):

```toml
[tool.strata.acl]
default = "deny"  # Default action when no rules match

# Deny rules are checked first
deny = [
  { principal = "*", tables = ["file:finance.*", "s3:pii.*"] }
]

# Allow rules are checked second
allow = [
  { principal = "bi-dashboard", tables = ["file:db.*"] },
  { tenant = "data-platform", tables = ["file:analytics.*"] }
]
```

**Security guarantees:**
- Requests without valid proxy token return 401
- Deny rules override allow rules (deny-first evaluation)
- Scan ownership enforced (only creator can retrieve batches)
- Admin endpoints require `admin:cache` scope
- `hide_forbidden_as_not_found=true` (default) returns 404 instead of 403 to prevent information disclosure

**Proxy configuration example (nginx):**

```nginx
location /strata/ {
    auth_request /auth;  # Your auth endpoint

    # CRITICAL: Strip client-supplied headers and inject trusted values
    proxy_set_header X-Strata-Principal $upstream_principal;
    proxy_set_header X-Strata-Tenant $upstream_tenant;
    proxy_set_header X-Strata-Scopes $upstream_scopes;
    proxy_set_header X-Strata-Proxy-Token "your-shared-secret";

    proxy_pass http://strata:8765/;
}
```

Rate limiting is also enabled by default to protect against abuse.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `STRATA_HOST` | Server host (default: 0.0.0.0) |
| `STRATA_PORT` | Server port (default: 8765) |
| `STRATA_CACHE_DIR` | Disk cache location |
| `STRATA_MAX_CACHE_SIZE_BYTES` | Cache size limit |
| `STRATA_FETCH_PARALLELISM` | Concurrent row group fetches (default: 4) |
| `STRATA_S3_REGION` | AWS region |
| `STRATA_S3_ENDPOINT_URL` | S3-compatible endpoint |
| `STRATA_S3_ACCESS_KEY` | S3 access key |
| `STRATA_S3_SECRET_KEY` | S3 secret key |
| `STRATA_S3_ANONYMOUS` | Public bucket access |
| `STRATA_LOG_FORMAT` | json or text |
| `STRATA_LOG_LEVEL` | DEBUG, INFO, WARNING, ERROR |
| `STRATA_MULTI_TENANT_ENABLED` | Enable multi-tenancy (default: false) |
| `STRATA_TENANT_HEADER` | Header name for tenant ID (default: X-Tenant-ID) |
| `STRATA_REQUIRE_TENANT_HEADER` | Reject requests without tenant header |
| `STRATA_AUTH_MODE` | Auth mode: `none` or `trusted_proxy` (default: none) |
| `STRATA_PROXY_TOKEN` | Expected proxy token for verification |
| `STRATA_PRINCIPAL_HEADER` | Header for principal ID (default: X-Strata-Principal) |
| `STRATA_SCOPES_HEADER` | Header for scopes (default: X-Strata-Scopes) |
| `STRATA_HIDE_FORBIDDEN_AS_NOT_FOUND` | Return 404 instead of 403 (default: true) |

### OpenTelemetry Tracing

```bash
pip install strata[otel]

export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_SERVICE_NAME=strata
```

### Rate Limiting

```python
config = StrataConfig(
    rate_limit_enabled=True,
    rate_limit_global_rps=1000.0,
    rate_limit_client_rps=100.0,
)
```

### Health Checks

`GET /health/dependencies` returns:

```json
{
  "status": "healthy",
  "checks": [
    {"name": "disk_cache", "status": "healthy", "latency_ms": 0.5},
    {"name": "metadata_store", "status": "healthy", "latency_ms": 1.2}
  ]
}
```

### Circuit Breakers

```python
from strata.circuit_breaker import get_circuit_breaker

breaker = get_circuit_breaker("s3")
with breaker:
    result = call_s3_operation()
```

Monitor via `GET /v1/debug/circuit-breakers`.

### Multi-Tenancy

Enable multi-tenant mode for SaaS deployments with complete cache and QoS isolation:

```bash
export STRATA_MULTI_TENANT_ENABLED=true
export STRATA_REQUIRE_TENANT_HEADER=true  # Optional: reject requests without tenant header
```

**How it works:**
1. API gateway authenticates requests (JWT, OAuth, API key)
2. Gateway extracts tenant ID from token and injects `X-Tenant-ID` header
3. Strata validates header format, isolates cache and QoS per tenant

```
Client → API Gateway → Strata
         (validates   (trusts header,
          JWT, adds    isolates cache + QoS)
          X-Tenant-ID)
```

**Cache isolation:** Each tenant gets a separate cache namespace. Same data queried by different tenants produces different cache keys.

**QoS isolation:** Each tenant gets their own interactive and bulk slot pools. One tenant consuming all their slots doesn't affect other tenants. Configure per-tenant slot counts via `TenantConfig`:

```python
from strata.tenant import TenantConfig
from strata.tenant_registry import get_tenant_registry

registry = get_tenant_registry()
registry.register_tenant(TenantConfig(
    tenant_id="premium-tenant",
    interactive_slots=64,  # Premium gets more slots
    bulk_slots=16,
))
```

**Per-tenant metrics:** Track scans, cache hits, bytes, and QoS slot usage per tenant via `/v1/admin/tenants` and `/metrics`.

**Tenant ID validation:** IDs must be 1-64 characters, alphanumeric with underscores and hyphens. Invalid IDs return HTTP 400.

---

## Development

```bash
uv sync --group dev
uv run pytest
pre-commit run --all-files
```

### Docker

```bash
docker compose up -d
# Or
docker build -t strata .
docker run -p 8765:8765 -v /path/to/warehouse:/data strata
```

### Grafana Dashboard

Import `grafana/strata-dashboard.json` for comprehensive metrics visualization.

## Modules

| Module | Description |
|--------|-------------|
| `server.py` | FastAPI HTTP server |
| `client.py` | Python SDK |
| `planner.py` | Read planning with two-tier pruning |
| `fetcher.py` | Parquet reading with Rust acceleration |
| `cache.py` | Disk cache using Arrow IPC |
| `iceberg.py` | Iceberg catalog integration |
| `config.py` | Configuration |
| `types.py` | Core types (CacheKey, ReadPlan, Task, Principal) |
| `tenant.py` | Multi-tenancy context and config |
| `tenant_registry.py` | Per-tenant metrics tracking |
| `auth.py` | Trusted proxy authentication and ACL evaluation |
| `tenant_acl.py` | Tenant-scoped authorization helpers |
| `artifact_store.py` | Artifact metadata, blob storage, lineage tracking |
| `transforms/registry.py` | Transform definitions (executor URL, timeouts) |
| `transforms/runner.py` | Background build runner, executor HTTP protocol |
| `transforms/build_store.py` | Build state tracking, lease management |
| `rate_limiter.py` | Token bucket rate limiting |
| `health.py` | Dependency health checks |
| `circuit_breaker.py` | Circuit breaker pattern |

## Future Work

- Distributed cache across servers
- Query pushdown to Parquet/Iceberg

Strata is read-only by design. Write operations belong in your Iceberg writer (Spark, Flink, pyiceberg).

## License

Apache-2.0
