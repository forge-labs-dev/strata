# Unified Materialize API Design

## Status: Implemented
## Author: Strata Team
## Date: 2026-01-02

---

## Overview

This document describes the unified `/v1/materialize` API that replaces the legacy `/v1/scan` endpoint. The key insight is that scanning an Iceberg table is conceptually equivalent to materializing data with an identity transform.

### Previous State

Previously, Strata had two distinct data access patterns:

1. **Scan API** (`/v1/scan`): Streams Iceberg table data with bounded memory
   - Input: table URI + optional filters/columns
   - Output: Arrow IPC stream via `/v1/scan/{scan_id}/batches`
   - Caching: Row-group level, keyed by `(table|snapshot|file|row_group|projection)`
   - No persistence: data streams directly to client

2. **Materialize API** (`/v1/artifacts/materialize`): Creates persistent artifacts
   - Input: input URIs + transform specification
   - Output: Artifact URI (`strata://artifact/{id}@v={version}`)
   - Caching: Artifact level, keyed by provenance hash
   - Persistence: Arrow IPC blob stored in blob store
   - Lineage: Tracks input versions for staleness detection

### Problems Solved

1. **Mental model complexity**: Users now use a single API for all data access
2. **Full lineage**: All queries are tracked in the artifact graph
3. **Query-level caching**: Filtered scans are cached at the query level
4. **Unified surface**: Single API for all data access patterns

---

## Proposal: Unified Materialize

### Core Concept

All data access in Strata becomes a `materialize` operation:

```
materialize(inputs, transform) → artifact
```

Where:
- **inputs**: List of URIs (Iceberg tables, existing artifacts)
- **transform**: Specification of how to combine/transform inputs
- **artifact**: Immutable, versioned result with lineage

Scanning a table becomes:

```python
materialize(
    inputs=["file:///warehouse#db.events"],
    transform={
        "executor": "identity@v1",
        "params": {
            "columns": ["id", "name"],
            "filters": [{"column": "id", "op": ">", "value": 100}]
        }
    }
)
```

### Identity Transform

The `identity@v1` transform is a special built-in that:

1. Reads from exactly one Iceberg table input
2. Applies optional column projection and row filtering
3. Returns the data unchanged (identity function)
4. Is executed by Strata directly (no external executor needed)

This is semantically equivalent to current `/v1/scan` but:
- Results become first-class artifacts with lineage
- Provenance hash enables query-level deduplication
- Named artifacts provide stable references to specific query results

### Unified Request Schema

```python
class MaterializeRequest(BaseModel):
    """Unified request to materialize data."""

    # Inputs (required)
    inputs: list[str]  # URIs: table or artifact

    # Transform specification
    transform: TransformSpec

    # Output options
    name: str | None = None  # Optional name for result
    mode: Literal["artifact", "stream"] = "stream"  # Delivery mode (default: stream)

    # Streaming options (when mode="stream")
    stream_timeout_seconds: float | None = None

class TransformSpec(BaseModel):
    """Transform to apply to inputs."""

    executor: str  # "identity@v1", "duckdb_sql@v1", etc.
    params: dict[str, Any]  # Executor-specific parameters
```

### Response Schema

```python
class MaterializeResponse(BaseModel):
    """Response from materialize request."""

    # Always present
    hit: bool  # True if result already existed
    artifact_uri: str  # "strata://artifact/{id}@v={version}"
    state: str  # "ready", "building", "streaming"

    # For cache misses in personal mode
    build_spec: BuildSpec | None = None

    # For cache misses in server mode
    build_id: str | None = None

    # For streaming mode
    stream_id: str | None = None  # ID to fetch stream
    stream_url: str | None = None  # URL to fetch data
```

### Mode: Artifact vs Stream

The `mode` parameter controls **how you receive data**, not whether it's cached.

**Key insight**: Artifacts are always created and persisted. The mode only affects the
response format and consumption pattern. This preserves the unified model where
everything is an artifact with lineage.

**Artifact Mode (default)**:
- Returns artifact URI for later retrieval via `GET /v2/artifacts/{id}/v/{version}/data`
- Client fetches data on-demand after build completes
- Best for: Workflows where you want to reference the result later

**Stream Mode**:
- Returns streaming URL for immediate consumption via `GET /v2/streams/{stream_id}`
- Data streams while the artifact is being built (tee to client + blob store)
- Bounded memory, O(single row group) for the streaming path
- Best for: Interactive queries, backwards compatibility with scan API

Both modes:
- Create an artifact record with provenance hash
- Persist the result to blob storage
- Enable cache hits on subsequent identical requests
- Track lineage for staleness detection

```python
# Artifact mode: fetch later
response = materialize(
    inputs=["file:///warehouse#db.events"],
    transform={"executor": "identity@v1", "params": {}},
    mode="artifact"
)
# response.artifact_uri = "strata://artifact/abc123@v=1"
# response.state = "ready" (if cache hit) or "building" (if miss)
# Client calls GET /v1/artifacts/abc123/v/1/data when ready

# Stream mode: consume immediately
response = materialize(
    inputs=["file:///warehouse#db.events"],
    transform={"executor": "identity@v1", "params": {}},
    mode="stream"
)
# response.artifact_uri = "strata://artifact/abc123@v=1"  # Still creates artifact!
# response.stream_url = "/v1/streams/{stream_id}"
# Client streams from stream_url while artifact builds in parallel
```

**Why always persist?**

1. **Unified model**: Everything is an artifact. No special cases.
2. **Automatic caching**: Repeat request → cache hit, even if first was streamed.
3. **Full lineage**: All queries tracked in artifact graph.
4. **Retry safety**: If streaming fails mid-way, client can retry and get cached result.

### Provenance Hash for Identity Transform

For identity transforms, the provenance hash includes:

```python
def compute_identity_provenance(table_uri: str, snapshot_id: int,
                                 columns: list[str], filters: list[Filter]) -> str:
    """Compute provenance hash for identity transform."""
    hasher = hashlib.sha256()

    # Input: table identity + snapshot
    hasher.update(f"table:{table_identity}@{snapshot_id}".encode())

    # Transform: executor ref
    hasher.update(b"executor:identity@v1")

    # Params: sorted columns + normalized filters
    hasher.update(f"columns:{sorted(columns)}".encode())
    hasher.update(f"filters:{normalize_filters(filters)}".encode())

    return hasher.hexdigest()
```

This ensures that:
- Same table + snapshot + columns + filters → same artifact
- Different snapshot → different artifact (staleness detection works)
- Different column order → same artifact (columns are sorted)

---

## API Design

### POST /v1/materialize

Unified endpoint for all data access (replaces `/v1/scan` and `/v1/artifacts/materialize`):

```http
POST /v1/materialize
Content-Type: application/json

{
    "inputs": ["file:///warehouse#db.events"],
    "transform": {
        "executor": "identity@v1",
        "params": {
            "columns": ["id", "name", "value"],
            "filters": [{"column": "id", "op": ">", "value": 100}]
        }
    },
    "name": "filtered_events",
    "mode": "artifact"
}
```

Response (cache hit):
```json
{
    "hit": true,
    "artifact_uri": "strata://artifact/abc123@v=1",
    "state": "ready"
}
```

Response (cache miss, personal mode):
```json
{
    "hit": false,
    "artifact_uri": "strata://artifact/def456@v=1",
    "state": "building",
    "build_spec": {
        "artifact_id": "def456",
        "version": 1,
        "executor": "identity@v1",
        "params": {...},
        "input_uris": [...]
    }
}
```

Response (streaming mode):
```json
{
    "hit": false,
    "artifact_uri": "strata://artifact/ghi789@v=1",
    "state": "building",
    "stream_id": "ghi789",
    "stream_url": "/v1/streams/ghi789"
}
```

Note: Even in streaming mode, `artifact_uri` points to a real artifact that will be
persisted. The `stream_url` provides immediate access while the artifact builds.

### GET /v1/streams/{stream_id}

Fetch streaming results:

```http
GET /v1/streams/ghi789
Accept: application/vnd.apache.arrow.stream

HTTP/1.1 200 OK
Content-Type: application/vnd.apache.arrow.stream
Transfer-Encoding: chunked

<Arrow IPC stream bytes>
```

### GET /v1/artifacts/{artifact_id}/v/{version}/data

Fetch persisted artifact data:

```http
GET /v1/artifacts/abc123/v/1/data
Accept: application/vnd.apache.arrow.stream

HTTP/1.1 200 OK
Content-Type: application/vnd.apache.arrow.stream

<Arrow IPC stream bytes>
```

### GET /v1/builds/{build_id}

Check build status (for artifact mode async waiting):

```http
GET /v1/builds/abc123

HTTP/1.1 200 OK
Content-Type: application/json

{
    "build_id": "abc123",
    "state": "building",
    "artifact_uri": "strata://artifact/abc123@v=1",
    "progress": {
        "bytes_processed": 50000000,
        "estimated_total_bytes": 100000000
    },
    "error": null
}
```

States:
- `building`: Build in progress
- `ready`: Build complete, artifact available
- `failed`: Build failed, see `error` field

Long-poll variant (blocks until state changes or timeout):

```http
GET /v1/builds/abc123?wait=true&timeout=30

# Blocks up to 30s until state != "building", then returns
```

---

## Client SDK Usage Patterns

The client SDK abstracts build waiting. Most users never poll manually.

### Pattern 1: Blocking Fetch (Simplest)

```python
# Materialize in artifact mode
response = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={"executor": "identity@v1", "params": {}},
    mode="artifact"
)

# fetch() blocks until artifact is ready, then returns data
# Internally: polls /v1/builds/{build_id} if state="building"
data = client.fetch(response.artifact_uri)
```

### Pattern 2: Explicit Polling

```python
response = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={"executor": "duckdb_sql@v1", "params": {"sql": "..."}},
    mode="artifact"
)

if response.state == "building":
    # Poll until ready (useful for progress tracking)
    while True:
        status = client.get_build_status(response.build_id)
        print(f"Progress: {status.progress.bytes_processed} / {status.progress.estimated_total_bytes}")
        if status.state == "ready":
            break
        elif status.state == "failed":
            raise BuildError(status.error)
        time.sleep(1)

# Now fetch the data
data = client.fetch(response.artifact_uri)
```

### Pattern 3: Fire-and-Forget + Later Retrieval

```python
# Kick off the build
response = client.materialize(
    inputs=[...],
    transform={...},
    name="nightly_aggregates",  # Named artifact
    mode="artifact"
)

# Do other work... (build runs in background)

# Later (maybe in a different process):
data = client.get("nightly_aggregates")  # Fetches by name, blocks if still building
```

### Pattern 4: Async/Await

```python
async def run_pipeline():
    response = await client.materialize_async(
        inputs=[...],
        transform={...},
        mode="artifact"
    )

    # await blocks until ready (uses long-poll internally)
    data = await client.fetch_async(response.artifact_uri)
    return data
```

---

## Identity Transform Specification

### Executor Reference

```
identity@v1
```

### Parameters

```python
class IdentityParams(BaseModel):
    """Parameters for identity@v1 transform."""

    # Column projection (None = all columns)
    columns: list[str] | None = None

    # Row filters (all filters are AND'd)
    filters: list[FilterSpec] | None = None

    # Snapshot selection (None = current)
    snapshot_id: int | None = None

class FilterSpec(BaseModel):
    """Row filter specification."""
    column: str
    op: Literal["=", "!=", "<", "<=", ">", ">="]
    value: Any
```

### Execution

The identity transform is executed internally by Strata:

1. **Planning**: Use existing `ReadPlanner` to generate row-group tasks
2. **Fetching**: Use existing `CachedFetcher` for row-group caching
3. **Streaming**: Yield Arrow batches with bounded memory
4. **Persistence**: If artifact mode, write concatenated Arrow IPC to blob store

```python
async def execute_identity(input_uri: str, params: IdentityParams) -> AsyncIterator[bytes]:
    """Execute identity transform (internal)."""
    # Plan using existing planner
    plan = planner.plan(
        table_uri=input_uri,
        snapshot_id=params.snapshot_id,
        columns=params.columns,
        filters=params.filters,
    )

    # Stream row groups using existing fetcher
    for task in plan.tasks:
        yield fetcher.fetch_as_stream_bytes(task)
```

---

## Migration Path

The legacy `/v1/scan` endpoint is deprecated and will be removed in a future version.
Use the unified `/v1/materialize` endpoint with `identity@v1` transform instead.

### Implementation Status

1. ✅ Added unified `/v1/materialize` endpoint with identity transform support
2. ✅ Added `/v1/streams/{stream_id}` endpoint for streaming mode
3. ✅ Updated client SDK with `fetch()` and `fetch_artifact()` methods
4. ✅ Updated all integration modules (pandas, polars, duckdb) to use new API
5. ✅ Updated tests to use new API
6. ⚠️ `/v1/scan` endpoint marked as deprecated (kept for backward compatibility)

---

## Benefits

### 1. Unified Mental Model

- All data access is `materialize(inputs, transform)`
- Scanning is just identity transform
- Transforms compose: filter → aggregate → join

### 2. Query-Level Caching

- Same query (table + snapshot + filters + columns) → same artifact
- Dashboard queries hit cache on refresh (if snapshot unchanged)
- No more row-group-only caching for filtered queries

### 3. Lineage for Everything

- Scan results appear in artifact graph
- "What tables does this dashboard depend on?" → fully answerable
- Staleness detection works for all queries

### 4. Simplified Client SDK

```python
# Before: Two APIs
client.scan("file:///warehouse#db.events", columns=["id"])
client.materialize([...], transform={...})

# After: One API
client.materialize("file:///warehouse#db.events", columns=["id"])
client.materialize([...], transform={...})
```

### 5. Named Query Results

```python
# Create stable reference to a query
client.materialize(
    "file:///warehouse#db.events",
    columns=["id", "name"],
    filters=[{"column": "active", "op": "=", "value": True}],
    name="active_users"
)

# Later: fetch by name (gets cached artifact)
data = client.get("active_users")
```

---

## Design Decisions

### 1. Mode Controls Delivery, Not Persistence

**Decision**: Both `artifact` and `stream` modes create and persist artifacts.

The `mode` parameter only affects how the client receives data:
- `artifact`: Client fetches via `GET /v2/artifacts/{id}/v/{version}/data` after build
- `stream`: Client streams via `GET /v2/streams/{stream_id}` during build

This preserves the unified model where everything is a first-class artifact with lineage
and caching. There are no "ephemeral" results that bypass the artifact graph.

**Rationale**:
- Consistent mental model (everything is an artifact)
- Automatic caching for all queries
- Full lineage tracking
- Retry safety (failed stream can be retried, hits cache)

### 2. Default Mode

**Decision**: Default to `stream`.

Streaming provides immediate data access with bounded memory, which is the common
case for interactive queries. Users who want artifact-first workflows (e.g., to
get the artifact URI for later reference) can explicitly set `mode="artifact"`.

### 3. QoS Classification

**Decision**: Use `estimated_bytes` and column count for identity transforms.

Identity transforms are classified using `estimated_bytes` and column count from
the planning phase, same as the current two-tier QoS system. Interactive tier for
small queries (dashboards), bulk tier for large queries (ETL).

### 4. Transform Versioning

**Decision**: New version for breaking changes (`identity@v1` → `identity@v2`).

- Semantic-preserving bug fixes stay in same version
- Breaking changes (different results for same input) get new version
- Old artifacts remain valid (immutable, reference their transform version)
- Follows semantic versioning principles

### 5. No TTL for v1

**Decision**: Skip ephemeral/TTL artifacts in initial implementation.

Artifacts are always persisted. We can add `ttl_seconds` for automatic cleanup
in a future version if there's demand. For now, rely on existing garbage
collection (`max_age_days`) for unreferenced artifacts.

---

## Implementation Checklist

- [x] Define `IdentityParams`, `TransformSpec`, `FilterSpec` schemas in `types.py`
- [x] Register `identity@v1` as built-in transform (no external executor)
- [x] Implement identity executor (wraps existing planner/fetcher)
- [x] Add unified `/v1/materialize` endpoint with `mode` parameter
- [x] Implement streaming mode with tee to blob store
- [x] Add `/v1/streams/{stream_id}` endpoint
- [x] Update client SDK with `fetch()` and `fetch_artifact()` methods
- [x] Update pandas/polars/duckdb integrations to use new API
- [x] Update all tests to use new API
- [x] Update documentation
- [x] Mark `/v1/scan` endpoints as deprecated
- [ ] Add `/v1/builds/{build_id}` endpoint with long-poll support (future)
- [ ] Remove deprecated `/v1/scan` endpoints (future breaking change)

---

## Appendix: Example Workflows

### Workflow 1: Dashboard Query

```python
# Dashboard queries a filtered table
response = client.materialize(
    inputs=["file:///warehouse#analytics.page_views"],
    transform={
        "executor": "identity@v1",
        "params": {
            "columns": ["user_id", "page", "timestamp"],
            "filters": [{"column": "timestamp", "op": ">=", "value": "2024-01-01"}]
        }
    },
    name="dashboard_page_views",  # Stable reference
    mode="artifact"  # Cache for next refresh
)

# First call: cache miss, builds artifact
# Subsequent calls: cache hit (if snapshot unchanged)
```

### Workflow 2: Ad-Hoc Exploration

```python
# Data scientist explores a large table
response = client.materialize(
    inputs=["file:///warehouse#raw.logs"],
    transform={
        "executor": "identity@v1",
        "params": {"columns": ["message", "level"]}
    },
    mode="stream"  # Stream immediately, artifact builds in parallel
)

# Stream data directly with bounded memory
async for batch in client.stream(response.stream_id):
    process(batch)

# If user runs same query again: cache hit!
# response.hit = True, no re-execution needed
```

### Workflow 3: Transform Pipeline

```python
# Step 1: Filter raw data (identity transform)
raw = client.materialize(
    inputs=["file:///warehouse#raw.events"],
    transform={"executor": "identity@v1", "params": {"filters": [...]}},
    name="filtered_events"
)

# Step 2: Aggregate (SQL transform)
agg = client.materialize(
    inputs=[raw.artifact_uri],
    transform={
        "executor": "duckdb_sql@v1",
        "params": {"sql": "SELECT date, COUNT(*) FROM input0 GROUP BY date"}
    },
    name="daily_counts"
)

# Both artifacts tracked with full lineage
# daily_counts depends on filtered_events depends on raw.events
```
