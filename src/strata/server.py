"""FastAPI server for Strata."""

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Annotated

import pyarrow as pa
import pyarrow.ipc as ipc
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse

from strata import fast_io
from strata.cache import CachedFetcher
from strata.config import StrataConfig
from strata.metrics import MetricsCollector, ScanMetrics, Timer
from strata.planner import ReadPlanner
from strata.types import ReadPlan, ScanRequest, ScanResponse

# Graceful shutdown configuration
DRAIN_TIMEOUT_SECONDS = 30  # Max time to wait for active scans to complete


class ResourceLimitError(Exception):
    """Raised when a resource limit is exceeded."""

    pass


def _eager_warmup(config: StrataConfig) -> dict:
    """Eagerly warm up expensive resources at startup.

    This eliminates cold-start latency by pre-initializing:
    1. Heavy module imports (pyiceberg, pyarrow)
    2. SQLite metadata store (connection + schema validation)
    3. Memory-resident caches

    Returns timing information for observability.
    """
    warmup_times = {}
    total_start = time.perf_counter()

    # 1. Pre-import heavy modules (these are already imported at module level,
    #    but we explicitly touch them to ensure all submodules are loaded)
    import_start = time.perf_counter()
    import pyarrow.parquet  # noqa: F401 - heavy, loads libparquet
    from pyiceberg.catalog.sql import SqlCatalog  # noqa: F401 - loads SQLAlchemy
    from pyiceberg.table import Table  # noqa: F401 - loads table machinery
    warmup_times["imports_ms"] = (time.perf_counter() - import_start) * 1000

    # 2. Initialize and warm up SQLite metadata store
    sqlite_start = time.perf_counter()
    try:
        from strata.metadata_cache import get_metadata_store

        store = get_metadata_store(config.cache_dir)
        # Run a simple query to warm up SQLite (page cache, WAL)
        stats = store.stats()
        warmup_times["sqlite_ms"] = (time.perf_counter() - sqlite_start) * 1000
        warmup_times["sqlite_entries"] = stats.get("parquet_meta_entries", 0)
    except Exception:
        warmup_times["sqlite_ms"] = (time.perf_counter() - sqlite_start) * 1000
        warmup_times["sqlite_error"] = True

    # 3. Pre-create in-memory caches (backed by SQLite)
    cache_start = time.perf_counter()
    from strata.metadata_cache import get_manifest_cache, get_parquet_cache

    get_parquet_cache(cache_dir=config.cache_dir)
    get_manifest_cache(cache_dir=config.cache_dir)
    warmup_times["caches_ms"] = (time.perf_counter() - cache_start) * 1000

    warmup_times["total_ms"] = (time.perf_counter() - total_start) * 1000
    return warmup_times


class ServerState:
    """Shared server state."""

    def __init__(self, config: StrataConfig) -> None:
        import os

        self.config = config
        # Check if metrics logging is disabled via environment
        metrics_enabled = os.environ.get("STRATA_METRICS_ENABLED", "true").lower() != "false"
        self.metrics = MetricsCollector(enabled=metrics_enabled)
        self.planner = ReadPlanner(config)
        self.fetcher = CachedFetcher(config, metrics=self.metrics)

        # Active scans (scan_id -> ReadPlan)
        self.scans: dict[str, ReadPlan] = {}

        # Resource limits - semaphore for concurrent scan limiting
        self._scan_semaphore = asyncio.Semaphore(config.max_concurrent_scans)

        # Approximate active scan counter for observability only.
        # Note: This is not thread-safe in async context (+=/-= are not atomic).
        # It's accurate enough for metrics/logging but should NOT be used for
        # control flow decisions. For authoritative count, derive from semaphore.
        self._active_scans = 0

        # Graceful shutdown state
        self._draining = False  # True when server is shutting down
        self._shutdown_event = asyncio.Event()  # Signaled when shutdown begins


# Global state (initialized in lifespan)
_state: ServerState | None = None


def get_state() -> ServerState:
    if _state is None:
        raise RuntimeError("Server not initialized")
    return _state


def _get_active_scan_count(state: ServerState) -> int:
    """Get authoritative active scan count from semaphore.

    The semaphore's internal value tracks available slots:
    - Full capacity (no active scans): _value == max_concurrent_scans
    - All slots used: _value == 0

    Active scans = max_concurrent_scans - available_slots
    """
    max_scans = state.config.max_concurrent_scans
    # Note: accessing _value is implementation detail but safe for read-only use
    available = state._scan_semaphore._value
    return max_scans - available


async def _graceful_shutdown(state: ServerState) -> None:
    """Wait for active scans to complete during shutdown."""
    state._draining = True
    state._shutdown_event.set()

    active = _get_active_scan_count(state)
    if active > 0:
        state.metrics.log_event(
            "shutdown_draining",
            active_scans=active,
            timeout_seconds=DRAIN_TIMEOUT_SECONDS,
        )

        # Wait for active scans to complete (with timeout)
        start = time.perf_counter()
        while _get_active_scan_count(state) > 0:
            elapsed = time.perf_counter() - start
            if elapsed > DRAIN_TIMEOUT_SECONDS:
                state.metrics.log_event(
                    "shutdown_timeout",
                    remaining_scans=_get_active_scan_count(state),
                )
                break
            await asyncio.sleep(0.1)

        if _get_active_scan_count(state) == 0:
            state.metrics.log_event("shutdown_drained")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize server state on startup, graceful shutdown on exit."""
    global _state
    config = StrataConfig.load()

    # Eager warmup: pre-initialize expensive resources before accepting requests
    # This makes the first request as fast as subsequent "warm" requests
    warmup_times = _eager_warmup(config)

    _state = ServerState(config)

    # Cleanup stale metadata entries on startup
    stale_removed = 0
    try:
        from strata.metadata_cache import get_metadata_store

        store = get_metadata_store(config.cache_dir)
        stale_removed = store.cleanup_stale_parquet_meta()
    except Exception:
        pass  # Don't fail startup if cleanup fails

    # Log startup with warmup timing info
    _state.metrics.log_event(
        "server_started",
        host=config.host,
        port=config.port,
        warmup_ms=warmup_times.get("total_ms", 0),
        warmup_imports_ms=warmup_times.get("imports_ms", 0),
        warmup_sqlite_ms=warmup_times.get("sqlite_ms", 0),
        warmup_caches_ms=warmup_times.get("caches_ms", 0),
        sqlite_entries=warmup_times.get("sqlite_entries", 0),
        stale_entries_removed=stale_removed,
    )

    yield

    # Graceful shutdown: wait for active scans to complete
    await _graceful_shutdown(_state)

    _state.metrics.log_event("server_stopped")
    _state = None


app = FastAPI(
    title="Strata",
    description="Snapshot-aware serving layer for Iceberg tables",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    """Basic health check endpoint (liveness probe).

    Returns 200 if the server process is running.
    Use /health/ready for readiness checks that verify dependencies.
    """
    return {"status": "ok"}


@app.get("/health/ready")
async def health_ready():
    """Readiness probe - checks if server can handle requests.

    Verifies:
    - Server state is initialized
    - Not at capacity (has scan slots available)
    - Metadata store is accessible (if configured)

    Returns 200 if ready, 503 if not ready.
    """
    from strata.metadata_cache import get_metadata_store

    checks = {}
    all_healthy = True

    # Check 1: Server state initialized
    try:
        state = get_state()
        checks["server_initialized"] = True
    except RuntimeError:
        checks["server_initialized"] = False
        all_healthy = False
        return Response(
            content='{"status": "not_ready", "checks": {"server_initialized": false}}',
            status_code=503,
            media_type="application/json",
        )

    # Check 2: Not draining (shutting down)
    if state._draining:
        checks["draining"] = True
        all_healthy = False
    else:
        checks["draining"] = False

    # Check 3: Not at capacity
    active = state._active_scans
    max_scans = state.config.max_concurrent_scans
    has_capacity = active < max_scans
    checks["has_capacity"] = has_capacity
    checks["active_scans"] = active
    checks["max_concurrent_scans"] = max_scans
    if not has_capacity:
        all_healthy = False

    # Check 3: Metadata store accessible
    try:
        store = get_metadata_store()
        # Quick sanity check - get stats (lightweight operation)
        store.stats()
        checks["metadata_store"] = True
    except Exception as e:
        checks["metadata_store"] = False
        checks["metadata_store_error"] = str(e)
        all_healthy = False

    status = "ready" if all_healthy else "degraded"
    status_code = 200 if all_healthy else 503

    return Response(
        content=f'{{"status": "{status}", "checks": {__import__("json").dumps(checks)}}}',
        status_code=status_code,
        media_type="application/json",
    )


@app.get("/metrics")
async def metrics():
    """Get aggregate metrics including resource utilization."""
    state = get_state()
    stats = state.metrics.get_aggregate_stats()
    # Add resource utilization info
    stats["resource_limits"] = {
        "max_concurrent_scans": state.config.max_concurrent_scans,
        "active_scans": state._active_scans,
        "max_tasks_per_scan": state.config.max_tasks_per_scan,
        "plan_timeout_seconds": state.config.plan_timeout_seconds,
        "scan_timeout_seconds": state.config.scan_timeout_seconds,
        "max_response_bytes": state.config.max_response_bytes,
    }
    return stats


@app.get("/metrics/prometheus")
async def metrics_prometheus():
    """Prometheus-format metrics endpoint.

    Returns metrics in Prometheus text exposition format for scraping.
    Includes:
    - Cache hit/miss counters
    - Active scan gauge
    - Request latency histograms (TODO: requires histogram support)
    - Resource utilization gauges
    """
    from strata.metadata_cache import get_metadata_store

    state = get_state()
    stats = state.metrics.get_aggregate_stats()

    lines = [
        "# HELP strata_cache_hits_total Total number of cache hits",
        "# TYPE strata_cache_hits_total counter",
        f'strata_cache_hits_total {stats.get("cache_hits", 0)}',
        "",
        "# HELP strata_cache_misses_total Total number of cache misses",
        "# TYPE strata_cache_misses_total counter",
        f'strata_cache_misses_total {stats.get("cache_misses", 0)}',
        "",
        "# HELP strata_scans_total Total number of completed scans",
        "# TYPE strata_scans_total counter",
        f'strata_scans_total {stats.get("scan_count", 0)}',
        "",
        "# HELP strata_active_scans Current number of active scans",
        "# TYPE strata_active_scans gauge",
        f"strata_active_scans {state._active_scans}",
        "",
        "# HELP strata_max_concurrent_scans Maximum allowed concurrent scans",
        "# TYPE strata_max_concurrent_scans gauge",
        f"strata_max_concurrent_scans {state.config.max_concurrent_scans}",
        "",
        "# HELP strata_bytes_from_cache_total Total bytes served from cache",
        "# TYPE strata_bytes_from_cache_total counter",
        f'strata_bytes_from_cache_total {stats.get("bytes_from_cache", 0)}',
        "",
        "# HELP strata_bytes_from_storage_total Total bytes read from storage",
        "# TYPE strata_bytes_from_storage_total counter",
        f'strata_bytes_from_storage_total {stats.get("bytes_from_storage", 0)}',
        "",
        "# HELP strata_rows_returned_total Total rows returned across all scans",
        "# TYPE strata_rows_returned_total counter",
        f'strata_rows_returned_total {stats.get("rows_returned", 0)}',
        "",
        "# HELP strata_row_groups_pruned_total Total row groups pruned by filters",
        "# TYPE strata_row_groups_pruned_total counter",
        f'strata_row_groups_pruned_total {stats.get("row_groups_pruned", 0)}',
        "",
        "# HELP strata_draining Server is draining (shutting down)",
        "# TYPE strata_draining gauge",
        f"strata_draining {1 if state._draining else 0}",
        "",
        "# HELP strata_stream_aborts_timeout_total Streams aborted due to timeout",
        "# TYPE strata_stream_aborts_timeout_total counter",
        f'strata_stream_aborts_timeout_total {stats.get("stream_aborts_timeout", 0)}',
        "",
        "# HELP strata_stream_aborts_size_total Streams aborted due to size limit",
        "# TYPE strata_stream_aborts_size_total counter",
        f'strata_stream_aborts_size_total {stats.get("stream_aborts_size", 0)}',
        "",
        "# HELP strata_client_disconnects_total Client disconnects during streaming",
        "# TYPE strata_client_disconnects_total counter",
        f'strata_client_disconnects_total {stats.get("client_disconnects", 0)}',
    ]

    # Add metadata store stats if available
    try:
        store = get_metadata_store()
        store_stats = store.stats()
        lines.extend([
            "",
            "# HELP strata_metadata_manifest_hits_total Manifest cache hits in metadata store",
            "# TYPE strata_metadata_manifest_hits_total counter",
            f'strata_metadata_manifest_hits_total {store_stats.get("manifest_hits", 0)}',
            "",
            "# HELP strata_metadata_manifest_misses_total Manifest cache misses in metadata store",
            "# TYPE strata_metadata_manifest_misses_total counter",
            f'strata_metadata_manifest_misses_total {store_stats.get("manifest_misses", 0)}',
            "",
            "# HELP strata_metadata_parquet_hits_total Parquet metadata cache hits",
            "# TYPE strata_metadata_parquet_hits_total counter",
            f'strata_metadata_parquet_hits_total {store_stats.get("parquet_meta_hits", 0)}',
            "",
            "# HELP strata_metadata_parquet_misses_total Parquet metadata cache misses",
            "# TYPE strata_metadata_parquet_misses_total counter",
            f'strata_metadata_parquet_misses_total {store_stats.get("parquet_meta_misses", 0)}',
            "",
            "# HELP strata_metadata_stale_invalidations_total Stale entries invalidated",
            "# TYPE strata_metadata_stale_invalidations_total counter",
            f'strata_metadata_stale_invalidations_total {store_stats.get("stale_invalidations", 0)}',
        ])
    except Exception:
        pass  # Metadata store not available

    # Add in-memory cache stats
    pq_cache_stats = state.planner.parquet_cache.stats()
    manifest_cache_stats = state.planner.manifest_cache.stats()

    lines.extend([
        "",
        "# HELP strata_parquet_cache_hits_total In-memory parquet cache hits",
        "# TYPE strata_parquet_cache_hits_total counter",
        f'strata_parquet_cache_hits_total {pq_cache_stats.get("hits", 0)}',
        "",
        "# HELP strata_parquet_cache_misses_total In-memory parquet cache misses",
        "# TYPE strata_parquet_cache_misses_total counter",
        f'strata_parquet_cache_misses_total {pq_cache_stats.get("misses", 0)}',
        "",
        "# HELP strata_parquet_cache_size Current entries in parquet cache",
        "# TYPE strata_parquet_cache_size gauge",
        f'strata_parquet_cache_size {pq_cache_stats.get("size", 0)}',
        "",
        "# HELP strata_manifest_cache_hits_total In-memory manifest cache hits",
        "# TYPE strata_manifest_cache_hits_total counter",
        f'strata_manifest_cache_hits_total {manifest_cache_stats.get("hits", 0)}',
        "",
        "# HELP strata_manifest_cache_misses_total In-memory manifest cache misses",
        "# TYPE strata_manifest_cache_misses_total counter",
        f'strata_manifest_cache_misses_total {manifest_cache_stats.get("misses", 0)}',
        "",
        "# HELP strata_manifest_cache_size Current entries in manifest cache",
        "# TYPE strata_manifest_cache_size gauge",
        f'strata_manifest_cache_size {manifest_cache_stats.get("size", 0)}',
    ])

    return Response(
        content="\n".join(lines) + "\n",
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


# =============================================================================
# API v1 Endpoints (stable contracts)
#
# v1 API Guarantees:
# - Response types are stable (ScanResponse, error format)
# - One Arrow IPC stream per scan (schema in first message)
# - Error codes: 400 (bad request), 404 (not found), 413 (too large),
#                503 (capacity/draining), 504 (timeout)
# - Cache key format is versioned (CACHE_VERSION in cache.py)
#
# v0 endpoints are aliases to v1 for backwards compatibility.
# =============================================================================


@app.post("/v1/scan", response_model=ScanResponse)
async def create_scan_v1(request: ScanRequest):
    """Create a new scan and return metadata.

    The scan is planned but not executed. Use the returned scan_id
    to fetch batches via /v1/scan/{scan_id}/batches.

    Resource limits enforced:
    - plan_timeout_seconds: Aborts planning if it takes too long
    - max_tasks_per_scan: Rejects scans that would read too many row groups

    Error codes:
    - 400: Invalid request (bad table URI, too many row groups)
    - 503: Server draining or at capacity
    - 504: Planning timeout
    """
    state = get_state()

    # Reject new scans during shutdown
    if state._draining:
        raise HTTPException(
            status_code=503,
            detail="Server is shutting down. Not accepting new scans.",
        )

    try:
        # Plan the scan with timeout
        plan_timeout = state.config.plan_timeout_seconds

        def do_plan():
            return state.planner.plan(
                table_uri=request.table_uri,
                snapshot_id=request.snapshot_id,
                columns=request.columns,
                filters=request.parse_filters(),
            )

        with Timer() as timer:
            try:
                # Run planning in thread pool with timeout
                plan = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, do_plan),
                    timeout=plan_timeout,
                )
            except asyncio.TimeoutError:
                raise HTTPException(
                    status_code=504,
                    detail=(
                        f"Planning timed out after {plan_timeout}s. "
                        "The table may have too many files or the catalog may be slow."
                    ),
                )

        # Enforce task limit to prevent OOM from huge scans
        max_tasks = state.config.max_tasks_per_scan
        if len(plan.tasks) > max_tasks:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Scan would read {len(plan.tasks)} row groups, "
                    f"exceeding limit of {max_tasks}. "
                    "Use filters to reduce scope or contact admin to increase limit."
                ),
            )

        # Pre-flight size check: reject oversized scans before streaming
        # This gives clients a clean 413 error instead of a truncated stream
        max_response = state.config.max_response_bytes
        if plan.estimated_bytes > max_response:
            raise HTTPException(
                status_code=413,
                detail=(
                    f"Estimated response size ({plan.estimated_bytes:,} bytes) "
                    f"exceeds limit ({max_response:,} bytes). "
                    "Use filters or column projection to reduce scope."
                ),
            )

        # Store the plan
        state.scans[plan.scan_id] = plan

        # Get columns from schema captured during planning (no IO)
        if plan.schema is not None:
            columns = plan.schema.names
        else:
            columns = plan.columns or []

        return ScanResponse(
            scan_id=plan.scan_id,
            snapshot_id=plan.snapshot_id,
            num_tasks=len(plan.tasks),
            total_row_groups=plan.total_row_groups,
            pruned_row_groups=plan.pruned_row_groups,
            columns=columns,
            planning_time_ms=timer.elapsed_ms,
            estimated_bytes=plan.estimated_bytes,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


class ClientDisconnectedError(Exception):
    """Raised when the client disconnects mid-scan."""

    pass


@app.get("/v1/scan/{scan_id}/batches")
async def get_batches_v1(scan_id: str, request: Request):
    """Return Arrow IPC stream containing all batches for a scan.

    Returns a single Arrow IPC stream with all batches written sequentially.
    Content-Type: application/vnd.apache.arrow.stream

    v1 Contract:
    - Schema is included in the stream (first message)
    - All batches have the same schema
    - Empty scans return empty response (0 bytes)

    Memory efficiency:
    - Uses true streaming - chunks are sent as they're produced
    - Memory usage is O(single row group) instead of O(total response)
    - Large scans can stream gigabytes with bounded memory

    Resource limits enforced:
    - max_concurrent_scans: Limits simultaneous scan executions
    - scan_timeout_seconds: Aborts connection if scan exceeds time limit
    - max_response_bytes: Aborts connection if cumulative size exceeds limit

    Error handling during streaming:
    - Timeout/size violations abort the connection (raise exception)
    - Client receives incomplete stream / transport error
    - This is intentional: partial results are dangerous, client must retry
    - Arrow IPC decode will fail on truncated stream, forcing error handling

    Client disconnect:
    - Server detects disconnect and stops processing early
    - Resources are released, no error logged (client already gone)

    Error codes:
    - 404: Scan not found
    - 503: Server at capacity (too many concurrent scans)
    - Transport error: Timeout or size limit exceeded during streaming
    """
    state = get_state()

    if scan_id not in state.scans:
        raise HTTPException(status_code=404, detail=f"Scan {scan_id} not found")

    plan = state.scans[scan_id]

    # Try to acquire semaphore (with timeout to prevent indefinite wait)
    try:
        await asyncio.wait_for(
            state._scan_semaphore.acquire(),
            timeout=10.0,  # Wait up to 10s for a slot
        )
    except TimeoutError:
        raise HTTPException(
            status_code=503,
            detail=(
                f"Server at capacity ({state.config.max_concurrent_scans} concurrent scans). "
                "Try again later."
            ),
        )

    # Empty scan - release semaphore and return valid empty IPC stream
    if not plan.tasks:
        state._scan_semaphore.release()
        scan_metrics = ScanMetrics(
            scan_id=scan_id,
            snapshot_id=plan.snapshot_id,
            table_id=str(plan.table_identity),
            planning_time_ms=plan.planning_time_ms,
            total_row_groups=plan.total_row_groups,
            pruned_row_groups=plan.pruned_row_groups,
        )
        state.metrics.log_scan_complete(scan_metrics)

        # Return valid empty Arrow IPC stream (schema + EOS, no batches)
        # This ensures clients can parse the stream without special-casing
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, plan.schema)
        writer.close()  # Writes EOS marker
        empty_stream = sink.getvalue().to_pybytes()

        return Response(content=empty_stream, media_type="application/vnd.apache.arrow.stream")

    # Create streaming response with cleanup
    state._active_scans += 1

    async def stream_batches():
        """Async generator that streams IPC chunks with resource tracking.

        Cancellation handling:
        - Checks client disconnect at top of each chunk iteration
        - Checks timeout/size limits before each fetch and before each yield
        - Uses stop_flag to propagate cancellation to sync fetch_segments()
        - finally block always runs for cleanup (semaphore release)

        Known limitation (v1):
        - fetch_segments() is sync, so we can't detect disconnect while
          blocked inside a fetch. If client disconnects during a slow fetch,
          that fetch completes before we notice. The stop_flag prevents
          subsequent fetches. To close this gap, run fetches via
          asyncio.to_thread() with cancellation support.
        """
        start_time = time.perf_counter()
        bytes_out = 0  # Bytes actually sent to client (authoritative for limit)
        tasks_completed = 0
        max_response = state.config.max_response_bytes
        timeout = state.config.scan_timeout_seconds
        stop_flag = False  # Shared flag to stop fetch_segments()

        scan_metrics = ScanMetrics(
            scan_id=scan_id,
            snapshot_id=plan.snapshot_id,
            table_id=str(plan.table_identity),
            planning_time_ms=plan.planning_time_ms,
            total_row_groups=plan.total_row_groups,
            pruned_row_groups=plan.pruned_row_groups,
        )

        def log_scan_terminal(outcome: str, **extra) -> None:
            """Log terminal event for this scan with timing and metrics.

            Every scan should have exactly one terminal log entry for operational
            visibility. Outcomes: 'complete', 'client_disconnect', 'timeout', 'size_exceeded'.
            """
            scan_metrics.total_time_ms = (time.perf_counter() - start_time) * 1000
            scan_metrics.fetch_time_ms = (
                scan_metrics.total_time_ms - scan_metrics.planning_time_ms
            )
            scan_metrics.rows_returned = sum(
                t.num_rows for t in plan.tasks[:tasks_completed]
            )

            if outcome == "complete":
                # Normal completion - use structured scan_complete event
                state.metrics.log_scan_complete(scan_metrics)
            else:
                # Abnormal termination - log with outcome and partial metrics
                state.metrics.log_event(
                    f"scan_{outcome}",
                    **scan_metrics.to_dict(),
                    tasks_completed=tasks_completed,
                    tasks_total=len(plan.tasks),
                    bytes_sent=bytes_out,
                    **extra,
                )

        try:
            # Async generator that fetches segments on demand
            # Uses asyncio.to_thread() to avoid blocking the event loop during I/O.
            # This is critical for S3/object storage where fetches can take 50-200ms.
            # For local disk/SSD this adds ~10μs overhead per fetch (negligible).
            async def fetch_segments_async():
                nonlocal tasks_completed, stop_flag
                bytes_in_estimate = 0  # Track estimated input bytes
                for task in plan.tasks:
                    # Check stop flag before expensive I/O
                    if stop_flag:
                        return

                    # Check timeout before expensive I/O
                    elapsed = time.perf_counter() - start_time
                    if elapsed > timeout:
                        stop_flag = True
                        return

                    # Check estimated size before I/O (early rejection)
                    # This uses task.estimated_bytes from Parquet metadata
                    bytes_in_estimate += task.estimated_bytes
                    if bytes_in_estimate > max_response:
                        stop_flag = True
                        return

                    # Fetch segment in thread pool to avoid blocking event loop
                    # This allows other requests to be processed during S3 fetches
                    segment = await asyncio.to_thread(
                        state.fetcher.fetch_as_stream_bytes, task
                    )
                    tasks_completed += 1

                    # Track cache metrics
                    if task.cached:
                        scan_metrics.cache_hits += 1
                        scan_metrics.bytes_from_cache += task.bytes_read
                    else:
                        scan_metrics.cache_misses += 1
                        scan_metrics.bytes_from_storage += task.bytes_read

                    yield segment

            # Stream chunks from the IPC concatenator
            # Process segments one at a time for true streaming with async I/O.
            # Each segment is a complete IPC stream; we yield chunks as produced.
            async for segment in fetch_segments_async():
                for chunk in fast_io.stream_concat_ipc_segments(iter([segment])):
                    # Check client disconnect first (cheap - just checks a flag)
                    # This sets stop_flag to prevent further fetches
                    if await request.is_disconnected():
                        stop_flag = True
                        state.metrics.record_client_disconnect()
                        log_scan_terminal("client_disconnect")
                        # Client already gone - just stop, no one to notify
                        return

                    # Check timeout - abort connection so client sees error
                    elapsed = time.perf_counter() - start_time
                    if elapsed > timeout:
                        stop_flag = True
                        state.metrics.record_stream_abort_timeout()
                        log_scan_terminal("timeout", timeout_seconds=timeout)
                        # Raise to abort connection - client gets incomplete stream error
                        # This is correct: partial results are dangerous, client must retry
                        raise RuntimeError(
                            f"Scan timeout after {elapsed:.1f}s "
                            f"({tasks_completed}/{len(plan.tasks)} tasks)"
                        )

                    # Track bytes actually sent to client
                    bytes_out += len(chunk)

                    # Check size limit - abort connection so client sees error
                    if bytes_out > max_response:
                        stop_flag = True
                        state.metrics.record_stream_abort_size()
                        log_scan_terminal("size_exceeded", max_bytes=max_response)
                        # Raise to abort connection - client gets incomplete stream error
                        # This is correct: partial results are dangerous, client must retry
                        raise RuntimeError(
                            f"Response size {bytes_out} exceeds limit {max_response}"
                        )

                    yield chunk

            # Normal completion - log terminal event
            log_scan_terminal("complete")

        finally:
            # Always release resources
            state._active_scans -= 1
            state._scan_semaphore.release()

    return StreamingResponse(
        stream_batches(),
        media_type="application/vnd.apache.arrow.stream",
    )


@app.delete("/v1/scan/{scan_id}")
async def delete_scan_v1(scan_id: str):
    """Delete a scan and free resources.

    Error codes:
    - 404: Scan not found
    """
    state = get_state()

    if scan_id not in state.scans:
        raise HTTPException(status_code=404, detail=f"Scan {scan_id} not found")

    del state.scans[scan_id]
    return {"status": "deleted", "scan_id": scan_id}


@app.get("/v1/cache/stats")
async def get_cache_stats_v1():
    """Get cache statistics.

    Returns information about what's in the cache and why.
    Operators can use this to understand cache behavior and debug issues.
    """
    state = get_state()
    stats = state.fetcher.cache.get_stats()
    return stats.to_dict()


@app.get("/v1/metadata/stats")
async def get_metadata_stats_v1():
    """Get metadata store and cache statistics.

    Returns hit/miss counters and entry counts for:
    - SQLite metadata store (manifest cache, parquet metadata)
    - In-memory LRU caches (parquet metadata, manifest resolution)

    Useful for:
    - Proving cache value (hit rates)
    - Debugging performance issues
    - Capacity planning
    """
    from strata.metadata_cache import get_metadata_store

    state = get_state()

    result = {
        "parquet_cache": state.planner.parquet_cache.stats(),
        "manifest_cache": state.planner.manifest_cache.stats(),
    }

    # Add SQLite store stats if available
    try:
        store = get_metadata_store()
        result["metadata_store"] = store.stats()
    except Exception:
        result["metadata_store"] = None

    return result


@app.post("/v1/metadata/cleanup")
async def cleanup_metadata_v1():
    """Remove stale metadata entries from the SQLite store.

    Scans all cached parquet metadata entries and removes those where:
    - The file no longer exists on disk
    - The file has been modified (different mtime or size)

    This is automatically run on server startup, but can be triggered
    manually if needed (e.g., after bulk file operations).

    Returns the number of stale entries removed.
    """
    from strata.metadata_cache import get_metadata_store

    try:
        store = get_metadata_store()
        removed = store.cleanup_stale_parquet_meta()
        return {
            "status": "completed",
            "stale_entries_removed": removed,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/v1/cache/entries")
async def list_cache_entries_v1():
    """List all cache entries with metadata.

    Returns detailed information about each cached entry.
    """
    state = get_state()
    entries = state.fetcher.cache.list_entries()
    return {"entries": [e.to_dict() for e in entries]}


@app.get("/v1/debug/cache/inspect")
async def inspect_cache_v1(
    prefix: Annotated[str | None, Query(description="Hash prefix to filter entries (hex string, e.g., 'a1b2')")] = None,
    table_id: Annotated[str | None, Query(description="Filter by table identifier")] = None,
    snapshot_id: Annotated[int | None, Query(description="Filter by snapshot ID")] = None,
    limit: Annotated[int, Query(description="Maximum number of entries to return", ge=1, le=1000)] = 100,
):
    """Inspect cache entries with detailed diagnostics (admin endpoint).

    This endpoint provides low-level cache inspection for debugging and
    operational troubleshooting. Use it to:
    - Verify specific entries are cached
    - Debug cache key hashing issues
    - Understand cache distribution by prefix
    - Inspect metadata for specific tables/snapshots

    Query parameters:
    - prefix: Filter by cache key hash prefix (hex string)
    - table_id: Filter by table identifier
    - snapshot_id: Filter by snapshot ID
    - limit: Max entries to return (default 100, max 1000)

    Returns detailed information including:
    - Cache key hash (for debugging key generation)
    - File path on disk
    - Metadata (table, snapshot, row group, columns)
    - File size and creation time
    """
    import json as json_module
    from strata.cache import CACHE_FILE_EXTENSION, CACHE_META_EXTENSION, CACHE_VERSION

    state = get_state()
    cache = state.fetcher.cache

    results = []
    versioned_dir = cache.cache_dir / f"v{CACHE_VERSION}"

    if not versioned_dir.exists():
        return {
            "cache_version": CACHE_VERSION,
            "cache_dir": str(cache.cache_dir),
            "entries": [],
            "total_matched": 0,
            "truncated": False,
        }

    # If prefix is provided, narrow the search
    if prefix:
        # Normalize to lowercase
        prefix = prefix.lower()
        # Build search paths based on prefix length
        if len(prefix) >= 4:
            # Can go directly to specific subdirectory
            search_dir = versioned_dir / prefix[:2] / prefix[2:4]
            if not search_dir.exists():
                return {
                    "cache_version": CACHE_VERSION,
                    "cache_dir": str(cache.cache_dir),
                    "prefix_filter": prefix,
                    "entries": [],
                    "total_matched": 0,
                    "truncated": False,
                }
            search_paths = [search_dir]
        elif len(prefix) >= 2:
            # Search within first-level subdirectory
            search_dir = versioned_dir / prefix[:2]
            if not search_dir.exists():
                return {
                    "cache_version": CACHE_VERSION,
                    "cache_dir": str(cache.cache_dir),
                    "prefix_filter": prefix,
                    "entries": [],
                    "total_matched": 0,
                    "truncated": False,
                }
            search_paths = [search_dir]
        else:
            # Search everything but filter by prefix
            search_paths = [versioned_dir]
    else:
        search_paths = [versioned_dir]

    matched_count = 0
    truncated = False

    for search_path in search_paths:
        for meta_path in search_path.rglob(f"*{CACHE_META_EXTENSION}"):
            # Extract hash from filename
            cache_hash = meta_path.stem.replace(".meta", "")

            # Apply prefix filter
            if prefix and not cache_hash.startswith(prefix):
                continue

            try:
                meta_data = json_module.loads(meta_path.read_text())

                # Apply table_id filter
                if table_id and meta_data.get("table_id") != table_id:
                    continue

                # Apply snapshot_id filter
                if snapshot_id is not None and meta_data.get("snapshot_id") != snapshot_id:
                    continue

                matched_count += 1

                if len(results) >= limit:
                    truncated = True
                    continue  # Keep counting but don't add more

                # Get data file info
                data_path = meta_path.with_suffix(CACHE_FILE_EXTENSION)
                file_size = data_path.stat().st_size if data_path.exists() else None
                file_exists = data_path.exists()

                results.append({
                    "hash": cache_hash,
                    "hash_prefix": cache_hash[:8],
                    "file_path": str(data_path.relative_to(cache.cache_dir)),
                    "file_exists": file_exists,
                    "file_size_bytes": file_size,
                    "metadata": meta_data,
                })

            except Exception as e:
                # Include corrupted entries for debugging
                results.append({
                    "hash": cache_hash,
                    "file_path": str(meta_path.relative_to(cache.cache_dir)),
                    "error": str(e),
                    "corrupted": True,
                })
                matched_count += 1

    # Sort by hash for consistent output
    results.sort(key=lambda x: x.get("hash", ""))

    response = {
        "cache_version": CACHE_VERSION,
        "cache_dir": str(cache.cache_dir),
        "entries": results,
        "total_matched": matched_count,
        "truncated": truncated,
    }

    if prefix:
        response["prefix_filter"] = prefix
    if table_id:
        response["table_id_filter"] = table_id
    if snapshot_id is not None:
        response["snapshot_id_filter"] = snapshot_id

    return response


@app.post("/v1/cache/clear")
async def clear_cache_v1():
    """Clear the disk cache."""
    state = get_state()
    try:
        state.fetcher.cache.clear()
        state.metrics.reset()
        return {"status": "cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# API v0 Aliases (deprecated, forwards to v1)
#
# These exist for backwards compatibility only. New clients should use /v1/.
# =============================================================================


@app.post("/v0/scan", response_model=ScanResponse)
async def create_scan_v0(request: ScanRequest):
    """Deprecated: Use /v1/scan instead."""
    return await create_scan_v1(request)


@app.get("/v0/scan/{scan_id}/batches")
async def get_batches_v0(scan_id: str, request: Request):
    """Deprecated: Use /v1/scan/{scan_id}/batches instead."""
    return await get_batches_v1(scan_id, request)


@app.delete("/v0/scan/{scan_id}")
async def delete_scan_v0(scan_id: str):
    """Deprecated: Use /v1/scan/{scan_id} instead."""
    return await delete_scan_v1(scan_id)


@app.get("/v0/cache/stats")
async def get_cache_stats_v0():
    """Deprecated: Use /v1/cache/stats instead."""
    return await get_cache_stats_v1()


@app.get("/v0/metadata/stats")
async def get_metadata_stats_v0():
    """Deprecated: Use /v1/metadata/stats instead."""
    return await get_metadata_stats_v1()


@app.post("/v0/metadata/cleanup")
async def cleanup_metadata_v0():
    """Deprecated: Use /v1/metadata/cleanup instead."""
    return await cleanup_metadata_v1()


@app.get("/v0/cache/entries")
async def list_cache_entries_v0():
    """Deprecated: Use /v1/cache/entries instead."""
    return await list_cache_entries_v1()


@app.post("/v0/cache/clear")
async def clear_cache_v0():
    """Deprecated: Use /v1/cache/clear instead."""
    return await clear_cache_v1()


def main():
    """Run the server."""
    import uvicorn

    config = StrataConfig.load()
    uvicorn.run(
        "strata.server:app",
        host=config.host,
        port=config.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
