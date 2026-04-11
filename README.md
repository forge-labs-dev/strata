# Strata

[![CI](https://github.com/fangchenli/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/ci.yml)
[![Pre-commit](https://github.com/fangchenli/strata/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/pre-commit.yml)
[![Docker](https://github.com/fangchenli/strata/actions/workflows/docker.yml/badge.svg)](https://github.com/fangchenli/strata/actions/workflows/docker.yml)

**A Persistence Substrate and Notebook Runtime for Long-Horizon Computation**

Strata is a persistence substrate for long-running, iterative, and expensive
computations, with both a core materialization API and an interactive notebook
surface built on top of it.

Strata currently has two user-facing surfaces built on the same runtime:

- **Strata Core**: the materialization, artifact, lineage, and executor layer
- **Strata Notebook**: the interactive notebook product built on top of that substrate

Strata is currently in **alpha**. The repo, package, and runtime are shared,
but the docs and release framing should be read as two separate surfaces with
different maturity levels.

**Try it:** [strata-notebook.fly.dev](https://strata-notebook.fly.dev) (hosted preview, no account needed)

**Docs:** [forge-labs-dev.github.io/strata](https://forge-labs-dev.github.io/strata/)

## Quick Start

```bash
# Docker (recommended)
docker compose up -d --build
# Then open http://localhost:8765

# Or from source
uv sync
cd frontend && npm ci && npm run build && cd ..
uv run strata-server
```

## Notebook Features

- **Content-addressed caching** — same code + same inputs = instant cache hit
- **Automatic dependency tracking** — DAG built from variable analysis
- **Cascade execution** — change upstream, downstream auto-invalidates
- **Rich outputs** — DataFrames, matplotlib plots, markdown
- **Environment management** — per-notebook Python venvs via uv
- **AI assistant** — Chat mode for questions, Agent mode for autonomous notebook building
- **Prompt cells** — LLM-powered cells with `{{ variable }}` injection
- **Cell operations** — reorder, duplicate, fold, keyboard shortcuts

## Choose Your Path

- **Strata Notebook** — Start with the
  [Notebook Quickstart](https://forge-labs-dev.github.io/strata/getting-started/notebook/)
  for the interactive notebook UI.
- **Strata Core** — Start with the
  [Core Quickstart](https://forge-labs-dev.github.io/strata/getting-started/core/)
  for the programmatic `materialize()` API.
- **Deployment** — See
  [Docker](https://forge-labs-dev.github.io/strata/deployment/docker/),
  [Fly.io](https://forge-labs-dev.github.io/strata/deployment/fly/), or
  [Codespaces](https://forge-labs-dev.github.io/strata/deployment/codespaces/).

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

| Good fit                               | Not a fit                               |
| -------------------------------------- | --------------------------------------- |
| Long-running AI/ML pipelines           | Simple single-shot computations         |
| Iterative evaluation workflows         | Systems that need control flow          |
| Expensive computations worth caching   | Real-time streaming workloads           |
| Multi-step data transformations        | Sub-second latency requirements         |
| Dashboard reads against Iceberg tables | Joins/aggregations (use a query engine) |

## Core Quick Start (2 minutes)

The rest of this README focuses on **Strata Core**.

If you want the notebook product instead, use
[docs/notebook-quickstart.md](docs/notebook-quickstart.md).

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
        "executor": "duckdb_sql@v1",
        "params": {"sql": "SELECT category, COUNT(*) as cnt FROM input0 GROUP BY 1"},
    },
    name="category_counts",  # Optional: assign a name
)

# Access the result
print(f"Artifact: {artifact.uri}")
print(f"Cache hit: {artifact.cache_hit}")

# Fetch downloads the data
table = client.fetch(artifact.uri)
df = table.to_pandas()

# Chain artifacts - use the result as input to another transform
filtered = client.materialize(
    inputs=[artifact.uri],
    transform={
        "executor": "duckdb_sql@v1",
        "params": {"sql": "SELECT * FROM input0 WHERE cnt > 100"},
    },
)
```

**Key behaviors:**

- Same inputs + transform → returns existing artifact (no recomputation)
- Artifacts are immutable and versioned
- Names are mutable pointers to specific versions

### Fetch Iceberg Tables

```python
from strata import StrataClient

client = StrataClient()

# Materialize creates/finds an artifact from a table
artifact = client.materialize(
    inputs=["file:///path/to/warehouse#namespace.table"],
    transform={"executor": "scan@v1", "params": {}},
)
# Fetch downloads the data
table = client.fetch(artifact.uri)
print(f"Got {table.num_rows} rows")

# With column projection and filters
artifact = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={
        "executor": "scan@v1",
        "params": {
            "columns": ["id", "timestamp", "value"],
            "filters": [{"column": "value", "op": ">", "value": 100}],
        },
    },
)
table = client.fetch(artifact.uri)
```

**Cache guarantee:** Results are cached as artifacts. Repeat requests with the same provenance (table + snapshot + columns + filters) return immediately from cache.

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

### Iceberg Table Fetching

Strata provides snapshot-aware fetching for Apache Iceberg tables via the unified `/v1/materialize` endpoint:

- Uses built-in `scan@v1` transform for direct table reads
- Caches results as artifacts keyed by provenance hash (table + snapshot + columns + filters)
- Streams with bounded memory (O(row group), not O(result))
- Two-tier QoS prevents bulk queries from starving dashboards

**When fetching shines:**

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
- External executor protocol (push model for simplicity, pull model for scale)
- Pluggable blob storage (local filesystem, S3/S3-compatible, or GCS)

**Iceberg Fetching** — snapshot-aware table access via unified API:

- Provenance-based caching (keys include table + snapshot + columns + filters)
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

## Known Limitations

Current release framing:

- Strata is still **alpha**
- Strata Core and Strata Notebook live in one repo/package today, but should be
  read as two separate surfaces with different docs and different maturity
- the smoothest end-user path today is still **local/personal notebook usage**
- service mode is functional, but better thought of as an advanced/shared-backend
  deployment mode than a default first-time experience

Notebook-specific limitations:

- are documented in
  [docs/notebook-quickstart.md](docs/notebook-quickstart.md),
  [docs/design-notebook.md](docs/design-notebook.md),
  [docs/design-notebook-environments.md](docs/design-notebook-environments.md),
  and
  [docs/design-notebook-display-outputs.md](docs/design-notebook-display-outputs.md)

Operational limitations:

- hosted notebook deployments need persistent volume sizing and notebook storage
  configured correctly
- service mode needs explicit auth/proxy/deployment setup and should still be
  treated as advanced deployment work

See also:

- [CHANGELOG.md](CHANGELOG.md)
- [docs/design-status.md](docs/design-status.md)
- [docs/core-quickstart.md](docs/core-quickstart.md)
- [docs/notebook-quickstart.md](docs/notebook-quickstart.md)
- [docs/service-mode-deployment.md](docs/service-mode-deployment.md)

## Usage Examples

### Column Projection and Filters

```python
from strata import StrataClient

client = StrataClient()

# With column projection
artifact = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={
        "executor": "scan@v1",
        "params": {"columns": ["id", "timestamp", "value"]},
    },
)
table = client.fetch(artifact.uri)
print(f"Got {table.num_rows} rows")

# With filters (enables two-tier pruning)
artifact = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={
        "executor": "scan@v1",
        "params": {
            "filters": [
                {"column": "value", "op": ">", "value": 100},
                {"column": "timestamp", "op": "<", "value": "2024-01-01"},
            ],
        },
    },
)
table = client.fetch(artifact.uri)
print(f"Filtered to {table.num_rows} rows")
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
from strata.integration.polars import fetch_to_polars

# fetch_to_polars handles materialize + fetch internally
df = fetch_to_polars(
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

### Iceberg Fetching Flow

For Iceberg tables, the unified `/v1/materialize` endpoint with `scan@v1` transform:

1. **Plan** – Resolve what to read (metadata-only, cheap)
2. **Fetch** – Read immutable row groups (I/O-bound, expensive)
3. **Stream** – Tee Arrow IPC bytes to client AND persist as artifact

Because Iceberg snapshots and Parquet row groups are immutable, Strata can safely persist the results. Repeat requests with the same provenance (table + snapshot + columns + filters) return the cached artifact.

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
# Unified Materialize API (primary interface)
POST /v1/materialize                     Materialize an artifact (fetch or transform)
GET  /v1/streams/{stream_id}             Stream Arrow IPC data

# Artifact Management
GET  /v1/artifacts/{id}/v/{version}      Get artifact metadata
GET  /v1/artifacts/{id}/v/{version}/data Download artifact data as Arrow IPC
GET  /v1/artifacts/builds/{build_id}     Poll async build status
POST /v1/artifacts/upload/{id}/v/{ver}   Upload artifact data (personal mode)
POST /v1/artifacts/finalize              Finalize artifact after upload
GET  /v1/artifacts/{id}/v/{ver}/lineage  Get artifact input dependencies
GET  /v1/artifacts/{id}/v/{ver}/dependents Find downstream dependents
PUT  /v1/artifacts/names/{name}          Set name alias for artifact version
GET  /v1/artifacts/names/{name}          Get artifact by name

# Cache Management
POST /v1/cache/warm        Warm cache for specified tables
POST /v1/cache/clear       Clear disk cache

# Health & Observability
GET  /health               Liveness check
GET  /health/ready         Readiness check
GET  /health/dependencies  Detailed dependency health
GET  /metrics              Aggregate metrics (JSON)
GET  /metrics/prometheus   Prometheus format

# Multi-Tenant Admin
GET  /v1/admin/tenants     List all tenants (multi-tenant mode)
GET  /v1/admin/tenants/{id} Get tenant metrics
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

Executors are external HTTP services that perform the actual computation. Strata supports two execution models:

1. **Push Model** - Strata sends inputs directly to the executor (simpler, good for small inputs)
2. **Pull Model** - Strata sends signed URLs, executor pulls inputs and pushes output (scalable, good for large inputs)

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
    "params": { "sql": "SELECT * FROM input0 WHERE value > 100" }
  },
  "inputs": [
    {
      "name": "input0",
      "type": "artifact",
      "uri": "strata://artifact/abc@v=1"
    },
    { "name": "input1", "type": "table", "uri": "file:///warehouse#db.events" }
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

### Protocol v2 (Pull Model)

For large inputs, executors can pull data via signed URLs instead of receiving it in the request:

**Build Manifest (sent to executor):**

```json
{
  "build_id": "build-abc123",
  "metadata": {
    "transform": {"ref": "duckdb_sql@v1", "params": {...}},
    "tenant": "team-data",
    "principal": "user@example.com"
  },
  "inputs": [
    {
      "url": "http://strata:8765/v1/artifacts/download?artifact_id=xyz&version=1&signature=...",
      "artifact_id": "xyz",
      "version": 1,
      "expires_at": 1704067200
    }
  ],
  "output": {
    "url": "http://strata:8765/v1/artifacts/upload?build_id=abc123&signature=...",
    "max_bytes": 1073741824,
    "expires_at": 1704067200
  },
  "finalize_url": "http://strata:8765/v1/builds/build-abc123/finalize"
}
```

**Executor workflow:**

1. Download each input from `inputs[].url` (Arrow IPC stream)
2. Execute transform
3. Upload result to `output.url` (Arrow IPC stream)
4. POST to `finalize_url` to complete the build

**Benefits:**

- No bandwidth bottleneck at Strata during execution
- Native retries for failed downloads/uploads
- Supports very large inputs/outputs
- Executor can parallelize input downloads

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

| Header                 | Required | Description                                              |
| ---------------------- | -------- | -------------------------------------------------------- |
| `X-Strata-Principal`   | Yes      | Stable user/service ID                                   |
| `X-Strata-Tenant`      | No       | Team/org ID (same as multi-tenancy)                      |
| `X-Strata-Scopes`      | No       | Space-separated scopes (e.g., `scan:create admin:cache`) |
| `X-Strata-Proxy-Token` | Yes\*    | Shared secret for proxy verification                     |

\*Required when `STRATA_PROXY_TOKEN` is configured.

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

| Variable                             | Description                                                |
| ------------------------------------ | ---------------------------------------------------------- |
| `STRATA_HOST`                        | Server host (default: 0.0.0.0)                             |
| `STRATA_PORT`                        | Server port (default: 8765)                                |
| `STRATA_CACHE_DIR`                   | Disk cache location                                        |
| `STRATA_MAX_CACHE_SIZE_BYTES`        | Cache size limit                                           |
| `STRATA_FETCH_PARALLELISM`           | Concurrent row group fetches (default: 4)                  |
| `STRATA_S3_REGION`                   | AWS region                                                 |
| `STRATA_S3_ENDPOINT_URL`             | S3-compatible endpoint                                     |
| `STRATA_S3_ACCESS_KEY`               | S3 access key                                              |
| `STRATA_S3_SECRET_KEY`               | S3 secret key                                              |
| `STRATA_S3_ANONYMOUS`                | Public bucket access                                       |
| `STRATA_LOG_FORMAT`                  | json or text                                               |
| `STRATA_LOG_LEVEL`                   | DEBUG, INFO, WARNING, ERROR                                |
| `STRATA_MULTI_TENANT_ENABLED`        | Enable multi-tenancy (default: false)                      |
| `STRATA_TENANT_HEADER`               | Header name for tenant ID (default: X-Tenant-ID)           |
| `STRATA_REQUIRE_TENANT_HEADER`       | Reject requests without tenant header                      |
| `STRATA_AUTH_MODE`                   | Auth mode: `none` or `trusted_proxy` (default: none)       |
| `STRATA_PROXY_TOKEN`                 | Expected proxy token for verification                      |
| `STRATA_PRINCIPAL_HEADER`            | Header for principal ID (default: X-Strata-Principal)      |
| `STRATA_SCOPES_HEADER`               | Header for scopes (default: X-Strata-Scopes)               |
| `STRATA_HIDE_FORBIDDEN_AS_NOT_FOUND` | Return 404 instead of 403 (default: true)                  |
| `STRATA_ARTIFACT_BLOB_BACKEND`       | Artifact storage: `local`, `s3`, or `gcs` (default: local) |
| `STRATA_ARTIFACT_S3_BUCKET`          | S3 bucket for artifact blobs                               |
| `STRATA_ARTIFACT_S3_PREFIX`          | Key prefix in bucket (default: artifacts)                  |
| `STRATA_ARTIFACT_GCS_BUCKET`         | GCS bucket for artifact blobs                              |
| `STRATA_ARTIFACT_GCS_PREFIX`         | Key prefix in bucket (default: artifacts)                  |
| `STRATA_GCS_PROJECT_ID`              | GCP project ID (optional)                                  |
| `STRATA_GCS_CREDENTIALS_JSON`        | Path to service account JSON key file                      |
| `STRATA_GCS_ANONYMOUS`               | Use anonymous access for public buckets                    |
| `STRATA_GCS_ENDPOINT_OVERRIDE`       | Custom GCS endpoint for testing                            |

### Artifact Blob Storage

Artifacts can be stored on local disk (default), S3/S3-compatible storage, or Google Cloud Storage.

**Local storage (default):**

```bash
export STRATA_ARTIFACT_DIR=/var/lib/strata/artifacts
```

**S3 storage:**

```bash
export STRATA_ARTIFACT_BLOB_BACKEND=s3
export STRATA_ARTIFACT_S3_BUCKET=my-artifacts-bucket
export STRATA_ARTIFACT_S3_PREFIX=strata-artifacts

# S3 credentials (same as Iceberg scanning)
export STRATA_S3_REGION=us-west-2
export STRATA_S3_ACCESS_KEY=...
export STRATA_S3_SECRET_KEY=...
```

**S3-compatible (MinIO, LocalStack):**

```bash
export STRATA_ARTIFACT_BLOB_BACKEND=s3
export STRATA_ARTIFACT_S3_BUCKET=artifacts
export STRATA_S3_ENDPOINT_URL=http://localhost:9000
export STRATA_S3_ACCESS_KEY=minioadmin
export STRATA_S3_SECRET_KEY=minioadmin
```

Or in `pyproject.toml`:

```toml
[tool.strata]
artifact_blob_backend = "s3"
artifact_s3_bucket = "my-artifacts-bucket"
artifact_s3_prefix = "strata-artifacts"
```

**GCS storage:**

```bash
export STRATA_ARTIFACT_BLOB_BACKEND=gcs
export STRATA_ARTIFACT_GCS_BUCKET=my-artifacts-bucket
export STRATA_ARTIFACT_GCS_PREFIX=strata-artifacts

# GCS credentials (Application Default Credentials or service account)
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
# Or use STRATA_GCS_CREDENTIALS_JSON=/path/to/service-account.json
```

**GCS with anonymous access (public buckets):**

```bash
export STRATA_ARTIFACT_BLOB_BACKEND=gcs
export STRATA_ARTIFACT_GCS_BUCKET=public-bucket
export STRATA_GCS_ANONYMOUS=true
```

Or in `pyproject.toml`:

```toml
[tool.strata]
artifact_blob_backend = "gcs"
artifact_gcs_bucket = "my-artifacts-bucket"
artifact_gcs_prefix = "strata-artifacts"
gcs_project_id = "my-project"
```

**Azure Blob Storage:**

```bash
pip install strata[azure]

export STRATA_ARTIFACT_BLOB_BACKEND=azure
export STRATA_ARTIFACT_AZURE_CONTAINER=my-container
export STRATA_ARTIFACT_AZURE_PREFIX=strata-artifacts

# Connection string auth (easiest for local development)
export STRATA_AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"

# Or account key auth
export STRATA_AZURE_ACCOUNT_NAME=mystorageaccount
export STRATA_AZURE_ACCOUNT_KEY=myaccountkey

# Or use DefaultAzureCredential (managed identity, env vars, CLI, etc.)
export STRATA_AZURE_USE_DEFAULT_CREDENTIAL=true
```

**Azure with Azurite emulator (local development):**

```bash
export STRATA_ARTIFACT_BLOB_BACKEND=azure
export STRATA_ARTIFACT_AZURE_CONTAINER=test-container
export STRATA_AZURE_CONNECTION_STRING="UseDevelopmentStorage=true"
# Or with custom endpoint:
export STRATA_AZURE_ENDPOINT_URL=http://127.0.0.1:10000/devstoreaccount1
```

Or in `pyproject.toml`:

```toml
[tool.strata]
artifact_blob_backend = "azure"
artifact_azure_container = "my-container"
artifact_azure_prefix = "strata-artifacts"
azure_account_name = "mystorageaccount"
```

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
    { "name": "disk_cache", "status": "healthy", "latency_ms": 0.5 },
    { "name": "metadata_store", "status": "healthy", "latency_ms": 1.2 }
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

| Module                      | Description                                       |
| --------------------------- | ------------------------------------------------- |
| `server.py`                 | FastAPI HTTP server                               |
| `client.py`                 | Python SDK                                        |
| `planner.py`                | Read planning with two-tier pruning               |
| `fetcher.py`                | Parquet reading with Rust acceleration            |
| `cache.py`                  | Disk cache using Arrow IPC                        |
| `iceberg.py`                | Iceberg catalog integration                       |
| `config.py`                 | Configuration                                     |
| `types.py`                  | Core types (CacheKey, ReadPlan, Task, Principal)  |
| `tenant.py`                 | Multi-tenancy context and config                  |
| `tenant_registry.py`        | Per-tenant metrics tracking                       |
| `auth.py`                   | Trusted proxy authentication and ACL evaluation   |
| `tenant_acl.py`             | Tenant-scoped authorization helpers               |
| `artifact_store.py`         | Artifact metadata, blob storage, lineage tracking |
| `transforms/registry.py`    | Transform definitions (executor URL, timeouts)    |
| `transforms/runner.py`      | Background build runner, executor HTTP protocol   |
| `transforms/build_store.py` | Build state tracking, lease management            |
| `rate_limiter.py`           | Token bucket rate limiting                        |
| `health.py`                 | Dependency health checks                          |
| `circuit_breaker.py`        | Circuit breaker pattern                           |

## Future Work

- **Executor SDK** - Python library for building custom executors with less boilerplate
- **Artifact retention policies** - TTL and version-count-based automatic cleanup
- **Webhook notifications** - Notify external systems when builds complete
- **Query pushdown** - Push filters to Parquet/Iceberg for reduced I/O

Strata focuses on persistence and materialization. Orchestration, scheduling, and control flow belong in layers above.

## License

Apache-2.0
