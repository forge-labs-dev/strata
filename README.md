# Strata

[![CI](https://github.com/fangchenli/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/ci.yml)
[![Pre-commit](https://github.com/fangchenli/strata/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/pre-commit.yml)
[![Docker](https://github.com/fangchenli/strata/actions/workflows/docker.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/docker.yml)

**A Persistence Substrate for Long-Horizon Computation**

Strata is a materialization and persistence layer for long-running, iterative, and expensive computations.

It provides a single primitive:

```
materialize(inputs, transform) → artifact
```

This primitive ensures that:
- Results are **immutable and versioned**
- Identical computations are **deduplicated**
- Lineage is **explicit and inspectable**
- Reuse is **correct by construction**

Strata is designed to sit **below orchestration** and **outside execution**.

## The Problem

Modern data and AI workflows increasingly have these properties:
- **Long-horizon**: minutes to hours, not milliseconds
- **Iterative**: evaluate → refine → repeat
- **Branching**: explore multiple variants
- **Expensive**: LLM calls, embeddings, large scans
- **Failure-prone**: crashes, retries, restarts are normal

What breaks first is not compute—it's **state**.

Typical systems rely on implicit task state, in-memory caches, ad-hoc checkpointing, and best-effort retries. These approaches fail under iteration, branching, and scale.

## The Solution

Once results are treated as first-class artifacts:
- Retries become **safe**
- Reuse becomes **trivial**
- Crashes become **recoverable**
- Lineage becomes **inspectable**

Strata makes this explicit and reliable.

## What Strata Is Not

Strata is **not**:
- A workflow engine or DAG runner
- A scheduler or agent framework
- A query engine or SQL layer

Those responsibilities belong elsewhere. Strata is the persistence substrate they should build on.

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

**Is Strata for you?**

| Good fit | Not a fit |
|----------|-----------|
| Long-running AI/ML pipelines | Simple single-shot computations |
| Iterative evaluation workflows | Systems that need control flow |
| Expensive computations worth caching | Real-time streaming workloads |
| Multi-step data transformations | Sub-second latency requirements |
| Dashboard reads against Iceberg tables | Joins/aggregations (use a query engine) |

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

### Materialize (the core primitive)

```python
from strata import StrataClient

client = StrataClient()

# Create a materialized artifact from an Iceberg table
artifact = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={
        "ref": "duckdb_sql@v1",
        "params": {"sql": "SELECT category, COUNT(*) as cnt FROM input0 GROUP BY 1"},
    },
    name="category_counts",  # Optional: assign a name
)

# Access the result
print(f"Artifact: {artifact.uri}")
print(f"Cache hit: {artifact.cache_hit}")
df = artifact.to_pandas()

# Chain artifacts - use the result as input to another transform
filtered = client.materialize(
    inputs=[artifact.uri],
    transform={
        "ref": "duckdb_sql@v1",
        "params": {"sql": "SELECT * FROM input0 WHERE cnt > 100"},
    },
)
```

**Key behaviors:**
- Same inputs + transform → returns existing artifact (no recomputation)
- Artifacts are immutable and versioned
- Names are mutable pointers to specific versions

### Scan Iceberg Tables

```python
from strata import StrataClient

client = StrataClient()
for batch in client.scan("file:///path/to/warehouse#namespace.table"):
    print(f"Got {batch.num_rows} rows")
```

**Streaming guarantee:** All-or-error. You get a complete Arrow IPC stream or the connection aborts—never silently truncated data.

---

## Core Concepts

### Materialization Model

Every expensive computation is a **materialization** defined by:
- **Pinned inputs**: Iceberg snapshots or artifact versions
- **Transform identity**: executor + parameters + code hash

From these, Strata derives a **provenance hash**. If the same materialization is requested again, Strata returns the existing artifact—no recomputation occurs.

### Artifact States

Materialization has observable states:
- `ready` — artifact exists and is usable
- `building` — computation in progress
- `failed` — computation failed with error

There is no hidden state inside user code. This makes it possible to poll progress, retry safely, resume after crashes, and reason about system behavior.

### Why Immutability Matters

Artifacts are:
- **Immutable**: never change once created
- **Versioned**: each computation produces a new version
- **Addressable**: can be referenced as inputs to other transforms
- **Reusable**: across processes, runs, and systems

### Iceberg Table Scanning

Strata also provides snapshot-aware scanning for Apache Iceberg tables:
- Caches Parquet row groups as Arrow IPC streams
- Keys by immutable snapshot IDs (no invalidation needed)
- Streams with bounded memory (O(row group), not O(result))
- Two-tier QoS prevents bulk queries from starving dashboards

**When scanning shines:**
- Same snapshot queried repeatedly
- Interactive or dashboard-driven workloads
- Cold starts are expensive
- Object storage latency dominates

## Features

**Materialization** — the core primitive:
- Provenance-based deduplication (same inputs + transform → existing artifact)
- Explicit artifact states (`ready`, `building`, `failed`)
- Lineage tracking (inputs → outputs, dependents)
- Named aliases for artifact versions
- External executor protocol (HTTP-based, push model)

**Iceberg Scanning** — snapshot-aware table access:
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
- Trusted proxy authentication with ACL

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

### Materialization Flow

1. **Request**: `POST /v1/artifacts/materialize` with inputs + transform spec
2. **Dedupe check**: Compute provenance hash, check for existing artifact
3. **Build** (if needed): Claim lease, invoke executor, store result
4. **Serve**: `GET /v1/artifacts/{id}/v/{version}/data` streams Arrow IPC

```
materialize(inputs, transform)
         │
         ▼
┌─────────────────┐
│ Provenance Hash │ ← hash(inputs, transform)
└────────┬────────┘
         │
    ┌────┴────┐
    │ exists? │
    └────┬────┘
         │
    yes  │  no
    ┌────┴────┐
    │         │
    ▼         ▼
 return    build
 existing  new artifact
```

### Iceberg Scanning Flow

1. **Plan** – Resolve what to read (metadata-only, cheap)
2. **Fetch** – Read immutable row groups (I/O-bound, expensive)
3. **Serve** – Stream Arrow IPC bytes (CPU-light, cheap)

Because Iceberg snapshots and Parquet row groups are immutable, Strata can safely persist the results.

### Cache Key Structure

```
hash(tenant_id, table_uri, snapshot_id, file_path, row_group_id, projection_fingerprint)
```

Since Iceberg snapshots are immutable, cached objects never need invalidation.

### Design Principles

- **Immutability over mutation**: Results never change once created
- **Explicit state over implicit progress**: Materialization is observable and inspectable
- **Persistence before orchestration**: Durable results are the foundation; control flow is layered on top
- **Correctness first**: Cache keys include snapshot/provenance identity
- **Conservative pruning**: If in doubt, read rather than risk dropping data
- **Bounded memory**: Large results stream incrementally

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Orchestration Layer                       │
│  (Your DAGs, agents, pipelines - control flow lives here)   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Strata Server                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Artifact   │  │  Iceberg    │  │   Persistence       │  │
│  │  Store      │  │  Scanner    │  │   Layer             │  │
│  │             │  │             │  │                     │  │
│  │ - Materialize│ │ - Planner   │  │ - Artifact blobs    │  │
│  │ - Dedupe    │  │ - Fetcher   │  │ - Row-group cache   │  │
│  │ - Lineage   │  │ - Pruning   │  │ - Metadata (SQLite) │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    External Executors                        │
│  (SQL engines, ML jobs, LLMs - computation lives here)      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Storage Layer                            │
│  (Local filesystem, S3, Iceberg tables)                     │
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

## Executor Protocol

Executors are external HTTP services that perform the actual computation. Strata pushes inputs to executors and stores their outputs as artifacts.

### Protocol v1 (Push Model)

**Request:**
```
POST {executor_url}/v1/execute
Content-Type: multipart/form-data
X-Strata-Executor-Protocol: v1

Parts:
  - metadata (application/json)
  - input0, input1, ... (application/vnd.apache.arrow.stream)
```

**Metadata JSON:**
```json
{
  "build_id": "build-abc123",
  "tenant": "team-data",
  "principal": "user@example.com",
  "transform": {
    "ref": "duckdb_sql@v1",
    "params": {"sql": "SELECT * FROM input0 WHERE value > 100"}
  },
  "inputs": [
    {"name": "input0", "type": "artifact", "uri": "strata://artifact/abc@v=1"},
    {"name": "input1", "type": "table", "uri": "file:///warehouse#db.events"}
  ]
}
```

**Response (success):**
```
HTTP/1.1 200 OK
Content-Type: application/vnd.apache.arrow.stream
X-Strata-Logs: <base64-encoded logs>  # Optional

<Arrow IPC stream bytes>
```

**Response (error):**
```
HTTP/1.1 4xx/5xx
Content-Type: application/json

{"success": false, "error": "Invalid SQL syntax", "logs": "..."}
```

### Implementing an Executor

A minimal executor in Python (using FastAPI):

```python
from fastapi import FastAPI, Request
from fastapi.responses import Response
import pyarrow as pa
import pyarrow.ipc as ipc
import duckdb
import io

app = FastAPI()

@app.post("/v1/execute")
async def execute(request: Request):
    form = await request.form()

    # Parse metadata
    metadata = json.loads(await form["metadata"].read())
    sql = metadata["transform"]["params"]["sql"]

    # Read input Arrow streams
    tables = {}
    for key in form:
        if key.startswith("input"):
            data = await form[key].read()
            reader = ipc.open_stream(io.BytesIO(data))
            tables[key] = reader.read_all()

    # Execute query
    conn = duckdb.connect()
    for name, table in tables.items():
        conn.register(name, table)
    result = conn.execute(sql).arrow()

    # Return Arrow IPC stream
    sink = io.BytesIO()
    with ipc.new_stream(sink, result.schema) as writer:
        writer.write_table(result)

    return Response(
        content=sink.getvalue(),
        media_type="application/vnd.apache.arrow.stream",
    )
```

### Executor Registration

Register executors in `pyproject.toml`:

```toml
[tool.strata.transforms]
duckdb_sql = { url = "http://localhost:9000", timeout = 300, max_output_bytes = 1073741824 }
embedding = { url = "http://embedding-service:8080", timeout = 600 }
```

Or programmatically:

```python
from strata.transforms.registry import TransformRegistry, TransformDefinition

registry = TransformRegistry()
registry.register(TransformDefinition(
    ref="duckdb_sql@v1",
    executor_url="http://localhost:9000",
    timeout_seconds=300.0,
    max_output_bytes=1024 * 1024 * 1024,
))
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

- Distributed artifact storage across servers
- Executor SDK for building custom transforms
- Query pushdown to Parquet/Iceberg
- Garbage collection for orphaned artifacts

Strata focuses on persistence and materialization. Orchestration, scheduling, and control flow belong in layers above.

## License

Apache-2.0
