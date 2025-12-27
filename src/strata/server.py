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
from strata.cache_warmer import CacheWarmer
from strata.config import StrataConfig
from strata.gc_tracker import get_gc_stats, get_recent_gc_pauses, install_gc_tracker
from strata.logging import (
    configure_logging,
    get_logger,
    request_context_middleware,
    set_request_context,
)
from strata.memory_profiler import get_detailed_memory_report, get_memory_snapshot
from strata.metrics import MetricsCollector, ScanMetrics, Timer
from strata.planner import ReadPlanner
from strata.cache_metrics import get_eviction_tracker
from strata.cache_stats import get_cache_histogram
from strata.health import HealthStatus, run_health_checks
from strata.pool_metrics import get_connection_metrics, get_pool_tracker
from strata.rate_limiter import (
    RateLimitConfig,
    get_rate_limiter,
    init_rate_limiter,
)
from strata.slow_ops import get_latency_stats, record_latency
from strata.tracing import init_tracing, instrument_fastapi, trace_span
from strata.types import (
    ReadPlan,
    ScanRequest,
    ScanResponse,
    Task,
    WarmAsyncRequest,
    WarmAsyncResponse,
    WarmJobProgress,
    WarmJobStatus,
    WarmRequest,
    WarmResponse,
)

logger = get_logger(__name__)

# Graceful shutdown configuration
DRAIN_TIMEOUT_SECONDS = 30  # Max time to wait for active scans to complete

# Readiness probe thresholds
SATURATION_THRESHOLD_SECONDS = 30.0  # Fail readiness if saturated for this long
STUCK_SCAN_THRESHOLD_SECONDS = 60.0  # Fail readiness if scan makes no progress for this long


class ResourceLimitError(Exception):
    """Raised when a resource limit is exceeded."""

    pass


def _eager_warmup(config: StrataConfig) -> dict:
    """Eagerly warm up expensive resources at startup.

    This eliminates cold-start latency by pre-initializing:
    1. GC pause tracking (must be first to catch early GC events)
    2. Arrow memory pool configuration (must be done before any Arrow ops)
    3. Heavy module imports (pyiceberg, pyarrow)
    4. SQLite metadata store (connection + schema validation)
    5. Memory-resident caches

    Returns timing information for observability.
    """
    warmup_times = {}
    total_start = time.perf_counter()

    # 0. Install GC pause tracker FIRST (to catch all GC events including during warmup)
    # This gives us precise pause duration measurements for diagnosing latency stalls.
    install_gc_tracker()
    warmup_times["gc_tracker"] = True

    # 1. Configure Arrow memory pool BEFORE any Arrow allocations
    # This must happen before importing pyarrow.parquet which triggers allocations
    try:
        pool_name = config.configure_arrow_memory_pool()
        if pool_name:
            warmup_times["arrow_memory_pool"] = pool_name
    except ValueError as e:
        warmup_times["arrow_memory_pool_error"] = str(e)

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
        from concurrent.futures import Future, ThreadPoolExecutor

        self.config = config

        # Dedicated thread pool for planning operations.
        # The default executor has only 8-16 workers (min(32, cpu_count + 4)),
        # which becomes a bottleneck under high concurrency (50+ users).
        # Planning involves reading Parquet metadata from disk/cache, so we use
        # a larger pool to handle concurrent planning requests without queueing.
        self._planning_executor = ThreadPoolExecutor(
            max_workers=64,
            thread_name_prefix="strata-planner",
        )

        # Dedicated thread pool for row group fetch operations.
        # Sized based on fetch_parallelism * expected concurrent scans.
        # Each scan can have up to fetch_parallelism concurrent fetches.
        # With default 4 parallelism and 12 concurrent scans (8 interactive + 4 bulk),
        # we need at most 48 workers to avoid queuing.
        fetch_workers = config.fetch_parallelism * (config.interactive_slots + config.bulk_slots)
        self._fetch_executor = ThreadPoolExecutor(
            max_workers=fetch_workers,
            thread_name_prefix="strata-fetch",
        )
        # Check if metrics logging is disabled via environment
        metrics_enabled = os.environ.get("STRATA_METRICS_ENABLED", "true").lower() != "false"
        self.metrics = MetricsCollector(enabled=metrics_enabled)
        self.planner = ReadPlanner(config)
        self.fetcher = CachedFetcher(config, metrics=self.metrics)

        # Active scans (scan_id -> ReadPlan)
        self.scans: dict[str, ReadPlan] = {}

        # QoS: Two-tier admission control with separate semaphores
        # This prevents bulk queries from starving interactive (dashboard) queries.
        # Interactive: small, fast queries (dashboards) - get dedicated slots
        # Bulk: large, slow queries (ETL, exports) - separate pool
        self._interactive_semaphore = asyncio.Semaphore(config.interactive_slots)
        self._bulk_semaphore = asyncio.Semaphore(config.bulk_slots)

        # Track which tier each scan is using for proper cleanup
        self._scan_tier: dict[str, str] = {}  # scan_id -> "interactive" | "bulk"

        # Legacy semaphore kept for backwards compatibility in metrics
        # but no longer used for admission control
        self._scan_semaphore = asyncio.Semaphore(config.max_concurrent_scans)

        # Approximate active scan counter for observability only.
        # Note: This is not thread-safe in async context (+=/-= are not atomic).
        # It's accurate enough for metrics/logging but should NOT be used for
        # control flow decisions. For authoritative count, derive from semaphores.
        self._active_scans = 0
        self._active_interactive = 0
        self._active_bulk = 0

        # Graceful shutdown state
        self._draining = False  # True when server is shutting down
        self._shutdown_event = asyncio.Event()  # Signaled when shutdown begins

        # QoS rejection counters (fast-fail when slots unavailable)
        self._interactive_rejected = 0  # Interactive queries rejected (503)
        self._bulk_rejected = 0  # Bulk queries rejected (503)

        # Prefetch management: limit concurrent prefetches to avoid resource exhaustion
        # when clients spam POST /scan without consuming the streams.
        # Max 4 concurrent prefetches (independent of streaming concurrency).
        self._prefetch_semaphore = asyncio.Semaphore(4)
        # Track prefetch futures by scan_id for cancellation on scan deletion
        self._prefetch_futures: dict[str, Future] = {}
        # Prefetch metrics for observability
        self._prefetch_started = 0  # Total prefetches started
        self._prefetch_used = 0  # Prefetches consumed by streaming
        self._prefetch_wasted = 0  # Prefetches discarded (scan deleted/abandoned)
        self._prefetch_skipped = 0  # Prefetches skipped (server busy)

        # Readiness tracking for capacity-based health checks
        # Track when each tier became saturated (no slots available)
        self._interactive_saturated_since: float | None = None
        self._bulk_saturated_since: float | None = None
        # Track scan progress: scan_id -> (start_time, last_bytes_streamed)
        self._scan_progress: dict[str, tuple[float, int]] = {}

        # Register thread pools for metrics tracking
        pool_tracker = get_pool_tracker()
        pool_tracker.register_pool("planning", self._planning_executor)
        pool_tracker.register_pool("fetch", self._fetch_executor)

        # Cache warmer for background warming jobs (initialized async in lifespan)
        self._cache_warmer: CacheWarmer | None = None


# Global state (initialized in lifespan)
_state: ServerState | None = None


def get_state() -> ServerState:
    if _state is None:
        raise RuntimeError("Server not initialized")
    return _state


def _get_active_scan_count(state: ServerState) -> int:
    """Get authoritative active scan count from semaphores.

    With two-tier QoS, active scans = interactive_active + bulk_active.
    """
    interactive_max = state.config.interactive_slots
    bulk_max = state.config.bulk_slots
    interactive_active = interactive_max - state._interactive_semaphore._value
    bulk_active = bulk_max - state._bulk_semaphore._value
    return interactive_active + bulk_active


def _classify_query(plan) -> str:
    """Classify a query as 'interactive' or 'bulk' based on its characteristics.

    Interactive queries are small, fast dashboard-style queries:
    - Estimated response size <= interactive_max_bytes (default 10MB)
    - Number of columns <= interactive_max_columns (default 10)

    Everything else is bulk (ETL, exports, analyst queries).
    """
    state = get_state()
    config = state.config

    # Check estimated response size
    if plan.estimated_bytes > config.interactive_max_bytes:
        return "bulk"

    # Check column count (None means all columns = likely bulk)
    if plan.columns is None:
        return "bulk"
    if len(plan.columns) > config.interactive_max_columns:
        return "bulk"

    return "interactive"


def _get_qos_metrics(state: ServerState) -> dict:
    """Get QoS tier metrics."""
    interactive_max = state.config.interactive_slots
    bulk_max = state.config.bulk_slots
    interactive_active = interactive_max - state._interactive_semaphore._value
    bulk_active = bulk_max - state._bulk_semaphore._value
    return {
        "interactive_slots": interactive_max,
        "interactive_active": interactive_active,
        "interactive_available": state._interactive_semaphore._value,
        "interactive_rejected": state._interactive_rejected,
        "bulk_slots": bulk_max,
        "bulk_active": bulk_active,
        "bulk_available": state._bulk_semaphore._value,
        "bulk_rejected": state._bulk_rejected,
    }


def _get_cache_size_bytes(state: ServerState) -> int:
    """Get current cache size in bytes."""
    from strata.cache import DiskCache

    cache = state.fetcher.cache
    if isinstance(cache, DiskCache):
        return cache.get_size_bytes()
    return 0


def _get_cache_entry_count(state: ServerState) -> int:
    """Get current number of cache entries."""
    from strata.cache import DiskCache

    cache = state.fetcher.cache
    if isinstance(cache, DiskCache):
        return len(cache.list_entries())
    return 0


def _update_saturation_tracking(state: ServerState) -> None:
    """Update saturation tracking based on current semaphore state.

    Called periodically (e.g., from health checks) to track how long
    the server has been at capacity. This is used by /health/ready to
    detect unhealthy saturation conditions.
    """
    now = time.time()

    # Check interactive tier saturation
    interactive_available = state._interactive_semaphore._value
    if interactive_available == 0:
        if state._interactive_saturated_since is None:
            state._interactive_saturated_since = now
    else:
        state._interactive_saturated_since = None

    # Check bulk tier saturation
    bulk_available = state._bulk_semaphore._value
    if bulk_available == 0:
        if state._bulk_saturated_since is None:
            state._bulk_saturated_since = now
    else:
        state._bulk_saturated_since = None


def _check_readiness(state: ServerState) -> tuple[bool, dict]:
    """Check if server is ready to accept new requests.

    Returns (is_ready, details) where details contains diagnostic info.

    Checks:
    1. Server not draining (shutting down)
    2. Has some capacity (not all slots exhausted for too long)
    3. No stuck scans (scans making no progress for too long)
    4. Logger queue not jammed (metrics can be written)
    """
    now = time.time()
    checks = {}
    issues = []

    # Update saturation tracking
    _update_saturation_tracking(state)

    # Check 1: Not draining
    if state._draining:
        checks["draining"] = True
        issues.append("server is draining (shutting down)")
    else:
        checks["draining"] = False

    # Check 2: Capacity - fail if BOTH tiers saturated for too long
    interactive_saturated_duration = (
        now - state._interactive_saturated_since if state._interactive_saturated_since else 0.0
    )
    bulk_saturated_duration = (
        now - state._bulk_saturated_since if state._bulk_saturated_since else 0.0
    )

    checks["interactive_saturated_seconds"] = round(interactive_saturated_duration, 1)
    checks["bulk_saturated_seconds"] = round(bulk_saturated_duration, 1)

    # Only fail if BOTH tiers are saturated beyond threshold
    # (if one tier has capacity, we can still serve some queries)
    both_saturated = (
        interactive_saturated_duration > SATURATION_THRESHOLD_SECONDS
        and bulk_saturated_duration > SATURATION_THRESHOLD_SECONDS
    )
    if both_saturated:
        checks["capacity_exhausted"] = True
        issues.append(
            f"both tiers saturated for >{SATURATION_THRESHOLD_SECONDS}s "
            f"(interactive={interactive_saturated_duration:.1f}s, "
            f"bulk={bulk_saturated_duration:.1f}s)"
        )
    else:
        checks["capacity_exhausted"] = False

    # Check 3: Stuck scans - find scans with no progress for too long
    stuck_scans = []
    for scan_id, (start_time, last_bytes) in list(state._scan_progress.items()):
        age = now - start_time
        if age > STUCK_SCAN_THRESHOLD_SECONDS:
            stuck_scans.append({"scan_id": scan_id, "age_seconds": round(age, 1)})

    checks["stuck_scans"] = len(stuck_scans)
    if stuck_scans:
        checks["stuck_scan_details"] = stuck_scans[:5]  # Limit to first 5
        issues.append(f"{len(stuck_scans)} scan(s) stuck with no progress")

    # Check 4: Logger queue health
    # If dropped_logs is increasing rapidly, the logger is overwhelmed
    dropped_logs = state.metrics.dropped_logs
    checks["dropped_logs"] = dropped_logs
    # Note: We don't fail on dropped_logs alone since it's a soft limit,
    # but we report it for observability

    # Overall readiness
    is_ready = len(issues) == 0
    checks["ready"] = is_ready
    if issues:
        checks["issues"] = issues

    return is_ready, checks


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

    # Shutdown the executors
    state._planning_executor.shutdown(wait=False)
    state._fetch_executor.shutdown(wait=False)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize server state on startup, graceful shutdown on exit."""
    global _state
    config = StrataConfig.load()

    # Configure structured logging first
    configure_logging()

    # Initialize OpenTelemetry tracing (no-op if not installed/configured)
    tracing_enabled = init_tracing()

    # Eager warmup: pre-initialize expensive resources before accepting requests
    # This makes the first request as fast as subsequent "warm" requests
    warmup_times = _eager_warmup(config)

    _state = ServerState(config)

    # Initialize rate limiter
    rate_limit_config = RateLimitConfig(
        enabled=config.rate_limit_enabled,
        global_requests_per_second=config.rate_limit_global_rps,
        global_burst=config.rate_limit_global_burst,
        client_requests_per_second=config.rate_limit_client_rps,
        client_burst=config.rate_limit_client_burst,
        scan_requests_per_second=config.rate_limit_scan_rps,
        warm_requests_per_second=config.rate_limit_warm_rps,
    )
    init_rate_limiter(rate_limit_config)

    # Initialize cache warmer for background warming jobs
    _state._cache_warmer = CacheWarmer(
        planner=_state.planner,
        fetcher=_state.fetcher,
        metrics=_state.metrics,
    )
    await _state._cache_warmer.start()

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
        tracing_enabled=tracing_enabled,
        arrow_memory_pool=warmup_times.get("arrow_memory_pool"),
    )

    yield

    # Stop cache warmer (cancel background jobs)
    if _state._cache_warmer:
        await _state._cache_warmer.stop()

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

# Add request context middleware (sets request_id, adds to response headers)
app.middleware("http")(request_context_middleware)


# Connection tracking middleware
@app.middleware("http")
async def connection_tracking_middleware(request: Request, call_next):
    """Track HTTP connection metrics."""
    connection_metrics = get_connection_metrics()

    # Check for Connection: keep-alive header
    connection_header = request.headers.get("connection", "").lower()
    has_keepalive = connection_header != "close"

    connection_metrics.request_started(has_keepalive=has_keepalive)
    try:
        response = await call_next(request)
        return response
    finally:
        connection_metrics.request_completed()


# Rate limiting middleware
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Apply rate limiting to incoming requests."""
    rate_limiter = get_rate_limiter()

    # Skip rate limiting if not initialized or for health/metrics endpoints
    if rate_limiter is None:
        return await call_next(request)

    path = request.url.path
    if path in ("/health", "/ready", "/metrics", "/v1/debug/pools", "/v1/debug/memory"):
        return await call_next(request)

    # Use client IP as identifier (X-Forwarded-For if behind proxy)
    client_ip = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    if not client_ip:
        client_ip = request.client.host if request.client else "unknown"

    result = rate_limiter.check(client_id=client_ip, endpoint=path)

    if not result.allowed:
        retry_after = int(result.retry_after_seconds or 1)
        return Response(
            content=f"Rate limit exceeded ({result.limit_type}). Retry after {retry_after}s.",
            status_code=429,
            headers={
                "Retry-After": str(retry_after),
                "X-RateLimit-Limit-Type": result.limit_type or "unknown",
            },
        )

    response = await call_next(request)

    # Add rate limit headers to response
    if result.tokens_remaining is not None:
        response.headers["X-RateLimit-Remaining"] = str(int(result.tokens_remaining))

    return response


# Instrument FastAPI with OpenTelemetry (no-op if OTel not installed)
instrument_fastapi(app)


@app.get("/health")
async def health():
    """Basic health check endpoint (liveness probe).

    Returns 200 if the server process is running.
    Use /health/ready for readiness checks that verify dependencies.
    """
    return {"status": "ok"}


@app.get("/health/dependencies")
async def health_dependencies():
    """Comprehensive health check for all dependencies.

    Checks the health of:
    - disk_cache: Cache directory accessibility and disk space
    - metadata_store: SQLite connectivity and entry counts
    - arrow_memory: PyArrow memory pool usage
    - thread_pools: Planning and fetch executor utilization
    - rate_limiter: Rate limiting status and rejection rate
    - cache_evictions: Cache eviction pressure level

    Each check returns:
    - status: healthy, degraded, or unhealthy
    - latency_ms: Time taken to perform the check
    - details: Check-specific information

    Overall status is the worst of all individual checks.

    Returns:
    - 200 if all dependencies are healthy
    - 200 with degraded status if some checks show degraded state
    - 503 if any dependency is unhealthy
    """
    state = get_state()

    report = run_health_checks(
        cache_dir=state.config.cache_dir,
        max_cache_size_bytes=state.config.max_cache_size_bytes,
        planning_executor=state._planning_executor,
        fetch_executor=state._fetch_executor,
    )

    status_code = 503 if report.status == HealthStatus.UNHEALTHY else 200

    return Response(
        content=__import__("json").dumps(report.to_dict()),
        status_code=status_code,
        media_type="application/json",
    )


@app.get("/health/ready")
async def health_ready():
    """Readiness probe - checks if server can handle requests.

    This is the Kubernetes readiness probe endpoint. Returns 503 when:
    - Server is draining (shutting down)
    - Both QoS tiers saturated for >30 seconds (no capacity)
    - Scans stuck with no progress for >60 seconds
    - Metadata store inaccessible

    Returns 200 if ready, 503 if not ready.
    Use this as your Kubernetes readiness probe.
    """
    import json

    from strata.metadata_cache import get_metadata_store

    # Check server initialized
    try:
        state = get_state()
    except RuntimeError:
        return Response(
            content='{"status": "not_ready", "checks": {"server_initialized": false}}',
            status_code=503,
            media_type="application/json",
        )

    # Run comprehensive readiness checks
    is_ready, checks = _check_readiness(state)
    checks["server_initialized"] = True

    # Also check metadata store accessibility
    try:
        store = get_metadata_store()
        store.stats()  # Quick sanity check
        checks["metadata_store"] = True
    except Exception as e:
        checks["metadata_store"] = False
        checks["metadata_store_error"] = str(e)
        is_ready = False
        if "issues" not in checks:
            checks["issues"] = []
        checks["issues"].append(f"metadata store error: {e}")

    # Add QoS capacity info for observability
    qos = _get_qos_metrics(state)
    checks["interactive_available"] = qos["interactive_available"]
    checks["bulk_available"] = qos["bulk_available"]
    checks["active_scans"] = _get_active_scan_count(state)

    status = "ready" if is_ready else "not_ready"
    status_code = 200 if is_ready else 503

    return Response(
        content=json.dumps({"status": status, "checks": checks}),
        status_code=status_code,
        media_type="application/json",
    )


@app.get("/metrics")
async def metrics():
    """Get aggregate metrics including resource utilization."""
    import asyncio
    import gc

    state = get_state()
    stats = state.metrics.get_aggregate_stats()

    # Add Arrow memory pool info
    pool = pa.default_memory_pool()
    stats["arrow_memory"] = {
        "pool_backend": pool.backend_name,
        "bytes_allocated": pool.bytes_allocated(),
        "max_memory": pool.max_memory(),
    }

    # Add GC stats for diagnosing periodic stalls
    # Include both gc.get_stats() (collection counts) and gc_tracker (pause durations)
    gc_builtin = gc.get_stats()
    stats["gc"] = {
        # Built-in GC stats (counts only)
        "gen0_collections": gc_builtin[0]["collections"],
        "gen1_collections": gc_builtin[1]["collections"],
        "gen2_collections": gc_builtin[2]["collections"],
        "gen0_collected": gc_builtin[0]["collected"],
        "gen1_collected": gc_builtin[1]["collected"],
        "gen2_collected": gc_builtin[2]["collected"],
        "gen0_uncollectable": gc_builtin[0]["uncollectable"],
        "gen1_uncollectable": gc_builtin[1]["uncollectable"],
        "gen2_uncollectable": gc_builtin[2]["uncollectable"],
    }

    # Add GC pause duration tracking (from gc.callbacks)
    gc_pause_stats = get_gc_stats()
    if gc_pause_stats:
        stats["gc_pauses"] = gc_pause_stats
    # Add resource utilization info
    stats["resource_limits"] = {
        "max_concurrent_scans": state.config.max_concurrent_scans,
        "active_scans": state._active_scans,
        "max_tasks_per_scan": state.config.max_tasks_per_scan,
        "plan_timeout_seconds": state.config.plan_timeout_seconds,
        "scan_timeout_seconds": state.config.scan_timeout_seconds,
        "max_response_bytes": state.config.max_response_bytes,
    }
    # Add prefetch metrics for observability
    stats["prefetch"] = {
        "started": state._prefetch_started,
        "used": state._prefetch_used,
        "wasted": state._prefetch_wasted,
        "skipped": state._prefetch_skipped,
        "in_flight": len(state._prefetch_futures),
    }
    # Add QoS tier metrics
    stats["qos"] = _get_qos_metrics(state)
    # Get cache size and entry count in thread pool to avoid blocking (involves filesystem ops)
    loop = asyncio.get_event_loop()
    cache_bytes, cache_entries = await asyncio.gather(
        loop.run_in_executor(None, _get_cache_size_bytes, state),
        loop.run_in_executor(None, _get_cache_entry_count, state),
    )
    # Add disk cache metrics
    stats["disk_cache"] = {
        "bytes_current": cache_bytes,
        "entries_current": cache_entries,
        "bytes_max": state.config.max_cache_size_bytes,
        "evictions_count": stats.get("cache_evictions_count", 0),
        "evicted_bytes": stats.get("cache_evicted_bytes", 0),
    }

    # Add thread pool metrics
    pool_tracker = get_pool_tracker()
    stats["thread_pools"] = {name: s.to_dict() for name, s in pool_tracker.get_all_stats().items()}

    # Add connection metrics
    connection_metrics = get_connection_metrics()
    stats["connections"] = connection_metrics.get_stats()

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
        f"strata_cache_hits_total {stats.get('cache_hits', 0)}",
        "",
        "# HELP strata_cache_misses_total Total number of cache misses",
        "# TYPE strata_cache_misses_total counter",
        f"strata_cache_misses_total {stats.get('cache_misses', 0)}",
        "",
        "# HELP strata_scans_total Total number of completed scans",
        "# TYPE strata_scans_total counter",
        f"strata_scans_total {stats.get('scan_count', 0)}",
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
        f"strata_bytes_from_cache_total {stats.get('bytes_from_cache', 0)}",
        "",
        "# HELP strata_bytes_from_storage_total Total bytes read from storage",
        "# TYPE strata_bytes_from_storage_total counter",
        f"strata_bytes_from_storage_total {stats.get('bytes_from_storage', 0)}",
        "",
        "# HELP strata_rows_returned_total Total rows returned across all scans",
        "# TYPE strata_rows_returned_total counter",
        f"strata_rows_returned_total {stats.get('rows_returned', 0)}",
        "",
        "# HELP strata_row_groups_pruned_total Total row groups pruned by filters",
        "# TYPE strata_row_groups_pruned_total counter",
        f"strata_row_groups_pruned_total {stats.get('row_groups_pruned', 0)}",
        "",
        "# HELP strata_draining Server is draining (shutting down)",
        "# TYPE strata_draining gauge",
        f"strata_draining {1 if state._draining else 0}",
        "",
        "# HELP strata_stream_aborts_timeout_total Streams aborted due to timeout",
        "# TYPE strata_stream_aborts_timeout_total counter",
        f"strata_stream_aborts_timeout_total {stats.get('stream_aborts_timeout', 0)}",
        "",
        "# HELP strata_stream_aborts_size_total Streams aborted due to size limit",
        "# TYPE strata_stream_aborts_size_total counter",
        f"strata_stream_aborts_size_total {stats.get('stream_aborts_size', 0)}",
        "",
        "# HELP strata_client_disconnects_total Client disconnects during streaming",
        "# TYPE strata_client_disconnects_total counter",
        f"strata_client_disconnects_total {stats.get('client_disconnects', 0)}",
        "",
        "# HELP strata_cache_evictions_total Total cache entries evicted",
        "# TYPE strata_cache_evictions_total counter",
        f"strata_cache_evictions_total {stats.get('cache_evictions_count', 0)}",
        "",
        "# HELP strata_cache_evicted_bytes_total Total bytes evicted from cache",
        "# TYPE strata_cache_evicted_bytes_total counter",
        f"strata_cache_evicted_bytes_total {stats.get('cache_evicted_bytes', 0)}",
        "",
        "# HELP strata_cache_bytes_written_total Total bytes written to cache",
        "# TYPE strata_cache_bytes_written_total counter",
        f"strata_cache_bytes_written_total {stats.get('bytes_written_to_cache', 0)}",
        "",
        "# HELP strata_cache_bytes_current Current cache size in bytes",
        "# TYPE strata_cache_bytes_current gauge",
        f"strata_cache_bytes_current {_get_cache_size_bytes(state)}",
        "",
        "# HELP strata_cache_entries_current Current number of cache entries",
        "# TYPE strata_cache_entries_current gauge",
        f"strata_cache_entries_current {_get_cache_entry_count(state)}",
        "",
        "# HELP strata_cache_max_bytes Maximum cache size limit in bytes",
        "# TYPE strata_cache_max_bytes gauge",
        f"strata_cache_max_bytes {state.config.max_cache_size_bytes}",
        "",
        "# HELP strata_prefetch_started_total Total prefetches started",
        "# TYPE strata_prefetch_started_total counter",
        f"strata_prefetch_started_total {state._prefetch_started}",
        "",
        "# HELP strata_prefetch_used_total Prefetches successfully used by streaming",
        "# TYPE strata_prefetch_used_total counter",
        f"strata_prefetch_used_total {state._prefetch_used}",
        "",
        "# HELP strata_prefetch_wasted_total Prefetches wasted (scan deleted/abandoned)",
        "# TYPE strata_prefetch_wasted_total counter",
        f"strata_prefetch_wasted_total {state._prefetch_wasted}",
        "",
        "# HELP strata_prefetch_skipped_total Prefetches skipped (server busy)",
        "# TYPE strata_prefetch_skipped_total counter",
        f"strata_prefetch_skipped_total {state._prefetch_skipped}",
        "",
        "# HELP strata_prefetch_in_flight Current prefetches in flight",
        "# TYPE strata_prefetch_in_flight gauge",
        f"strata_prefetch_in_flight {len(state._prefetch_futures)}",
    ]

    # Add GC stats for diagnosing periodic stalls
    import gc

    gc_stats = gc.get_stats()
    lines.extend(
        [
            "",
            "# HELP strata_gc_collections_total GC collections by generation",
            "# TYPE strata_gc_collections_total counter",
            f'strata_gc_collections_total{{generation="0"}} {gc_stats[0]["collections"]}',
            f'strata_gc_collections_total{{generation="1"}} {gc_stats[1]["collections"]}',
            f'strata_gc_collections_total{{generation="2"}} {gc_stats[2]["collections"]}',
            "",
            "# HELP strata_gc_collected_total Objects collected by generation",
            "# TYPE strata_gc_collected_total counter",
            f'strata_gc_collected_total{{generation="0"}} {gc_stats[0]["collected"]}',
            f'strata_gc_collected_total{{generation="1"}} {gc_stats[1]["collected"]}',
            f'strata_gc_collected_total{{generation="2"}} {gc_stats[2]["collected"]}',
        ]
    )

    # Add GC pause duration metrics (from gc.callbacks tracker)
    gc_pause_stats = get_gc_stats()
    if gc_pause_stats:
        lines.extend(
            [
                "",
                "# HELP strata_gc_pause_total_ms Total GC pause time in milliseconds",
                "# TYPE strata_gc_pause_total_ms counter",
                f"strata_gc_pause_total_ms {gc_pause_stats.get('total_pause_ms', 0)}",
                "",
                "# HELP strata_gc_pause_max_ms Maximum single GC pause in milliseconds",
                "# TYPE strata_gc_pause_max_ms gauge",
                f"strata_gc_pause_max_ms {gc_pause_stats.get('max_pause_ms', 0)}",
                "",
                "# HELP strata_gc_pauses_total Total number of GC pauses",
                "# TYPE strata_gc_pauses_total counter",
                f"strata_gc_pauses_total {gc_pause_stats.get('total_pauses', 0)}",
            ]
        )
        # Per-generation pause stats
        for gen in ["gen0", "gen1", "gen2"]:
            gen_stats = gc_pause_stats.get(gen, {})
            gen_num = gen[-1]  # "0", "1", or "2"
            lines.extend(
                [
                    "",
                    "# HELP strata_gc_pause_count GC pause count by generation",
                    "# TYPE strata_gc_pause_count counter",
                    f'strata_gc_pause_count{{generation="{gen_num}"}} {gen_stats.get("count", 0)}',
                    "# HELP strata_gc_pause_total_ms_by_gen Total pause time by generation",
                    "# TYPE strata_gc_pause_total_ms_by_gen counter",
                    f'strata_gc_pause_total_ms_by_gen{{generation="{gen_num}"}} '
                    f"{gen_stats.get('total_ms', 0)}",
                    "# HELP strata_gc_pause_max_ms_by_gen Max pause time by generation",
                    "# TYPE strata_gc_pause_max_ms_by_gen gauge",
                    f'strata_gc_pause_max_ms_by_gen{{generation="{gen_num}"}} '
                    f"{gen_stats.get('max_ms', 0)}",
                ]
            )

    # Add metadata store stats if available
    try:
        store = get_metadata_store()
        store_stats = store.stats()
        lines.extend(
            [
                "",
                "# HELP strata_metadata_manifest_hits_total Manifest cache hits in metadata store",
                "# TYPE strata_metadata_manifest_hits_total counter",
                f"strata_metadata_manifest_hits_total {store_stats.get('manifest_hits', 0)}",
                "",
                "# HELP strata_metadata_manifest_misses_total Manifest cache misses",
                "# TYPE strata_metadata_manifest_misses_total counter",
                f"strata_metadata_manifest_misses_total {store_stats.get('manifest_misses', 0)}",
                "",
                "# HELP strata_metadata_parquet_hits_total Parquet metadata cache hits",
                "# TYPE strata_metadata_parquet_hits_total counter",
                f"strata_metadata_parquet_hits_total {store_stats.get('parquet_meta_hits', 0)}",
                "",
                "# HELP strata_metadata_parquet_misses_total Parquet metadata cache misses",
                "# TYPE strata_metadata_parquet_misses_total counter",
                f"strata_metadata_parquet_misses_total {store_stats.get('parquet_meta_misses', 0)}",
                "",
                "# HELP strata_metadata_stale_invalidations_total Stale entries invalidated",
                "# TYPE strata_metadata_stale_invalidations_total counter",
                f"strata_metadata_stale_invalidations_total "
                f"{store_stats.get('stale_invalidations', 0)}",
            ]
        )
    except Exception:
        pass  # Metadata store not available

    # Add in-memory cache stats
    pq_cache_stats = state.planner.parquet_cache.stats()
    manifest_cache_stats = state.planner.manifest_cache.stats()

    lines.extend(
        [
            "",
            "# HELP strata_parquet_cache_hits_total In-memory parquet cache hits",
            "# TYPE strata_parquet_cache_hits_total counter",
            f"strata_parquet_cache_hits_total {pq_cache_stats.get('hits', 0)}",
            "",
            "# HELP strata_parquet_cache_misses_total In-memory parquet cache misses",
            "# TYPE strata_parquet_cache_misses_total counter",
            f"strata_parquet_cache_misses_total {pq_cache_stats.get('misses', 0)}",
            "",
            "# HELP strata_parquet_cache_size Current entries in parquet cache",
            "# TYPE strata_parquet_cache_size gauge",
            f"strata_parquet_cache_size {pq_cache_stats.get('size', 0)}",
            "",
            "# HELP strata_manifest_cache_hits_total In-memory manifest cache hits",
            "# TYPE strata_manifest_cache_hits_total counter",
            f"strata_manifest_cache_hits_total {manifest_cache_stats.get('hits', 0)}",
            "",
            "# HELP strata_manifest_cache_misses_total In-memory manifest cache misses",
            "# TYPE strata_manifest_cache_misses_total counter",
            f"strata_manifest_cache_misses_total {manifest_cache_stats.get('misses', 0)}",
            "",
            "# HELP strata_manifest_cache_size Current entries in manifest cache",
            "# TYPE strata_manifest_cache_size gauge",
            f"strata_manifest_cache_size {manifest_cache_stats.get('size', 0)}",
        ]
    )

    # Add QoS tier metrics
    qos = _get_qos_metrics(state)
    lines.extend(
        [
            "",
            "# HELP strata_qos_interactive_slots Max interactive query slots",
            "# TYPE strata_qos_interactive_slots gauge",
            f"strata_qos_interactive_slots {qos['interactive_slots']}",
            "",
            "# HELP strata_qos_interactive_active Current interactive queries running",
            "# TYPE strata_qos_interactive_active gauge",
            f"strata_qos_interactive_active {qos['interactive_active']}",
            "",
            "# HELP strata_qos_bulk_slots Max bulk query slots",
            "# TYPE strata_qos_bulk_slots gauge",
            f"strata_qos_bulk_slots {qos['bulk_slots']}",
            "",
            "# HELP strata_qos_bulk_active Current bulk queries running",
            "# TYPE strata_qos_bulk_active gauge",
            f"strata_qos_bulk_active {qos['bulk_active']}",
            "",
            "# HELP strata_qos_interactive_rejected_total Interactive queries rejected (503)",
            "# TYPE strata_qos_interactive_rejected_total counter",
            f"strata_qos_interactive_rejected_total {qos['interactive_rejected']}",
            "",
            "# HELP strata_qos_bulk_rejected_total Bulk queries rejected (503)",
            "# TYPE strata_qos_bulk_rejected_total counter",
            f"strata_qos_bulk_rejected_total {qos['bulk_rejected']}",
        ]
    )

    # Add fetch parallelism metrics
    lines.extend(
        [
            "",
            "# HELP strata_fetch_parallelism Max concurrent row group fetches per scan",
            "# TYPE strata_fetch_parallelism gauge",
            f"strata_fetch_parallelism {state.config.fetch_parallelism}",
            "",
            "# HELP strata_fetch_executor_workers Number of workers in fetch thread pool",
            "# TYPE strata_fetch_executor_workers gauge",
            f"strata_fetch_executor_workers {state._fetch_executor._max_workers}",
        ]
    )

    # Add rate limiter metrics
    rate_limiter = get_rate_limiter()
    if rate_limiter is not None:
        rl_stats = rate_limiter.get_stats()
        rl_rejected_global = rl_stats.get("rejected_global", 0)
        rl_rejected_client = rl_stats.get("rejected_client", 0)
        rl_rejected_endpoint = rl_stats.get("rejected_endpoint", 0)
        lines.extend(
            [
                "",
                "# HELP strata_rate_limit_requests_total Total requests processed",
                "# TYPE strata_rate_limit_requests_total counter",
                f"strata_rate_limit_requests_total {rl_stats.get('total_requests', 0)}",
                "",
                "# HELP strata_rate_limit_allowed_total Requests allowed",
                "# TYPE strata_rate_limit_allowed_total counter",
                f"strata_rate_limit_allowed_total {rl_stats.get('allowed_requests', 0)}",
                "",
                "# HELP strata_rate_limit_rejected_total Requests rejected by reason",
                "# TYPE strata_rate_limit_rejected_total counter",
                f'strata_rate_limit_rejected_total{{reason="global"}} {rl_rejected_global}',
                f'strata_rate_limit_rejected_total{{reason="client"}} {rl_rejected_client}',
                f'strata_rate_limit_rejected_total{{reason="endpoint"}} {rl_rejected_endpoint}',
                "",
                "# HELP strata_rate_limit_active_clients Tracked clients",
                "# TYPE strata_rate_limit_active_clients gauge",
                f"strata_rate_limit_active_clients {rl_stats.get('active_clients', 0)}",
            ]
        )

    # Add cache eviction metrics
    eviction_tracker = get_eviction_tracker()
    eviction_stats = eviction_tracker.get_stats()
    pressure_map = {"low": 0, "medium": 1, "high": 2, "critical": 3}
    pressure_value = pressure_map.get(eviction_stats.pressure_level, 0)
    lines.extend(
        [
            "",
            "# HELP strata_cache_eviction_events_total Total eviction events",
            "# TYPE strata_cache_eviction_events_total counter",
            f"strata_cache_eviction_events_total {eviction_stats.total_evictions}",
            "",
            "# HELP strata_cache_files_evicted_total Total files evicted",
            "# TYPE strata_cache_files_evicted_total counter",
            f"strata_cache_files_evicted_total {eviction_stats.total_files_evicted}",
            "",
            "# HELP strata_cache_eviction_bytes_total Total bytes evicted",
            "# TYPE strata_cache_eviction_bytes_total counter",
            f"strata_cache_eviction_bytes_total {eviction_stats.total_bytes_evicted}",
            "",
            "# HELP strata_cache_eviction_rate Evictions per minute",
            "# TYPE strata_cache_eviction_rate gauge",
            f"strata_cache_eviction_rate {eviction_stats.eviction_rate_per_minute}",
            "",
            "# HELP strata_cache_eviction_pressure Pressure level (0-3)",
            "# TYPE strata_cache_eviction_pressure gauge",
            f"strata_cache_eviction_pressure {pressure_value}",
        ]
    )

    # Add thread pool metrics
    pool_tracker = get_pool_tracker()
    for pool_name, pool_stats in pool_tracker.get_all_stats().items():
        active = pool_stats.active_workers
        max_w = pool_stats.max_workers
        util = pool_stats.utilization_pct / 100.0  # Convert percentage to ratio
        lines.extend(
            [
                "",
                "# HELP strata_thread_pool_active_workers Active workers",
                "# TYPE strata_thread_pool_active_workers gauge",
                f'strata_thread_pool_active_workers{{pool="{pool_name}"}} {active}',
                "# HELP strata_thread_pool_max_workers Max workers",
                "# TYPE strata_thread_pool_max_workers gauge",
                f'strata_thread_pool_max_workers{{pool="{pool_name}"}} {max_w}',
                "# HELP strata_thread_pool_utilization Utilization ratio",
                "# TYPE strata_thread_pool_utilization gauge",
                f'strata_thread_pool_utilization{{pool="{pool_name}"}} {util}',
            ]
        )

    # Add connection metrics
    conn_metrics = get_connection_metrics()
    conn_stats = conn_metrics.get_stats()
    lines.extend(
        [
            "",
            "# HELP strata_http_requests_total Total HTTP requests",
            "# TYPE strata_http_requests_total counter",
            f"strata_http_requests_total {conn_stats.get('total_requests', 0)}",
            "",
            "# HELP strata_http_connections_active Current active HTTP connections",
            "# TYPE strata_http_connections_active gauge",
            f"strata_http_connections_active {conn_stats.get('concurrent_requests', 0)}",
            "",
            "# HELP strata_http_connections_keepalive Requests with keep-alive",
            "# TYPE strata_http_connections_keepalive counter",
            f"strata_http_connections_keepalive {conn_stats.get('keepalive_requests', 0)}",
        ]
    )

    # Add Arrow memory metrics
    pool = pa.default_memory_pool()
    lines.extend(
        [
            "",
            "# HELP strata_arrow_memory_bytes_allocated Current Arrow memory allocated",
            "# TYPE strata_arrow_memory_bytes_allocated gauge",
            f"strata_arrow_memory_bytes_allocated {pool.bytes_allocated()}",
            "",
            "# HELP strata_arrow_memory_max_bytes Maximum Arrow memory ever allocated",
            "# TYPE strata_arrow_memory_max_bytes gauge",
            f"strata_arrow_memory_max_bytes {pool.max_memory()}",
        ]
    )

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
            with trace_span(
                "plan_scan",
                table_uri=request.table_uri,
                snapshot_id=request.snapshot_id,
                columns_count=len(request.columns) if request.columns else None,
            ) as span:
                plan = state.planner.plan(
                    table_uri=request.table_uri,
                    snapshot_id=request.snapshot_id,
                    columns=request.columns,
                    filters=request.parse_filters(),
                )
                span.set_attribute("scan_id", plan.scan_id)
                span.set_attribute("row_groups_total", plan.total_row_groups)
                span.set_attribute("row_groups_pruned", plan.pruned_row_groups)
                span.set_attribute("tasks_count", len(plan.tasks))
                span.set_attribute("estimated_bytes", plan.estimated_bytes)
                return plan

        with Timer() as timer:
            try:
                # Run planning in dedicated thread pool with timeout.
                # Using a dedicated executor (64 workers) instead of the default
                # (8-16 workers) prevents thread pool starvation under high concurrency.
                plan = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(state._planning_executor, do_plan),
                    timeout=plan_timeout,
                )
            except TimeoutError:
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

        # Add scan_id to request context for downstream logging
        set_request_context(scan_id=plan.scan_id)

        # Record planning latency to histogram
        record_latency("plan", timer.elapsed_ms)

        # Log slow planning (>100ms threshold)
        if timer.elapsed_ms > 100:
            logger.warning(
                "Slow planning detected",
                scan_id=plan.scan_id,
                table_uri=request.table_uri,
                planning_ms=round(timer.elapsed_ms, 2),
                tasks=len(plan.tasks),
                total_row_groups=plan.total_row_groups,
            )

        # Log scan creation with structured data
        logger.info(
            "Scan created",
            scan_id=plan.scan_id,
            table_uri=request.table_uri,
            snapshot_id=plan.snapshot_id,
            tasks=len(plan.tasks),
            total_row_groups=plan.total_row_groups,
            pruned_row_groups=plan.pruned_row_groups,
            estimated_bytes=plan.estimated_bytes,
            planning_ms=round(timer.elapsed_ms, 2),
        )

        # Start prefetching first row group in background to reduce TTFB.
        # This overlaps network I/O with client processing the scan response.
        # The prefetched bytes are stored in plan.prefetched_first and used
        # by get_batches_v1 when streaming begins.
        #
        # Safeguards:
        # - Prefetch semaphore limits concurrent prefetches (avoid resource exhaustion)
        # - Futures tracked for cancellation on scan deletion
        # - Metrics track prefetch usage vs waste
        # - Adaptive disable: skip prefetch when server is at capacity to avoid
        #   amplifying load (prefetch becomes wasted work under high concurrency)
        #
        # Server is "busy" when:
        # - All QoS slots are nearly exhausted (interactive + bulk - 1)
        # - OR prefetch semaphore queue is building up
        total_slots = state.config.interactive_slots + state.config.bulk_slots
        used_slots = (state.config.interactive_slots - state._interactive_semaphore._value) + (
            state.config.bulk_slots - state._bulk_semaphore._value
        )
        server_busy = used_slots >= total_slots - 1

        if plan.tasks and state._prefetch_semaphore._value > 0 and not server_busy:
            # Only prefetch if we have capacity and server isn't busy
            scan_id = plan.scan_id

            def prefetch_first():
                try:
                    # Acquire semaphore (blocking in thread pool)
                    # This is synchronous because we're in a thread

                    # Use a simple lock approach since we're in a thread
                    # The semaphore check above is a fast-path optimization
                    first_task = plan.tasks[0]
                    bytes_data = state.fetcher.fetch_as_stream_bytes(first_task)

                    # Only store if scan still exists (wasn't deleted)
                    if scan_id in state.scans:
                        plan.prefetched_first = bytes_data
                        state._prefetch_started += 1
                    else:
                        state._prefetch_wasted += 1
                except Exception:
                    # Prefetch failure is non-fatal; streaming will fetch normally
                    pass
                finally:
                    # Clean up future tracking
                    state._prefetch_futures.pop(scan_id, None)

            # Track the future for potential cancellation
            future = asyncio.get_event_loop().run_in_executor(None, prefetch_first)
            state._prefetch_futures[scan_id] = future
        elif plan.tasks and server_busy:
            # Track skipped prefetches for observability
            state._prefetch_skipped += 1

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

    # QoS: Classify query and acquire appropriate tier semaphore
    # This prevents bulk queries from starving interactive (dashboard) queries.
    tier = _classify_query(plan)
    if tier == "interactive":
        semaphore = state._interactive_semaphore
        tier_name = "interactive"
        max_slots = state.config.interactive_slots
        queue_timeout = state.config.interactive_queue_timeout
    else:
        semaphore = state._bulk_semaphore
        tier_name = "bulk"
        max_slots = state.config.bulk_slots
        queue_timeout = state.config.bulk_queue_timeout

    # Try to acquire tier-specific semaphore with fast-fail timeout
    # Interactive queries get longer wait (dashboards should succeed)
    # Bulk queries fail fast (2s default) to prevent 30s tail latencies
    try:
        await asyncio.wait_for(
            semaphore.acquire(),
            timeout=queue_timeout,
        )
    except TimeoutError:
        # Track rejection for metrics
        if tier == "interactive":
            state._interactive_rejected += 1
        else:
            state._bulk_rejected += 1
        raise HTTPException(
            status_code=503,
            detail=(
                f"Server at capacity ({max_slots} {tier_name} slots, "
                f"waited {queue_timeout:.1f}s). Try again later."
            ),
        )

    # Track which tier this scan is using for proper cleanup
    state._scan_tier[scan_id] = tier

    # Increment tier-specific active counter
    if tier == "interactive":
        state._active_interactive += 1
    else:
        state._active_bulk += 1

    # Empty scan - release tier semaphore and return valid empty IPC stream
    if not plan.tasks:
        semaphore.release()
        state._scan_tier.pop(scan_id, None)
        if tier == "interactive":
            state._active_interactive -= 1
        else:
            state._active_bulk -= 1
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

    # Track resources for this scan.
    # IMPORTANT: We track _active_scans and hold the semaphore for the duration
    # of streaming. The generator's finally block handles cleanup.
    #
    # Resource lifecycle:
    # 1. Semaphore acquired above (line ~622)
    # 2. _active_scans incremented when generator STARTS (first iteration)
    # 3. Both released in generator's finally block
    #
    # This ensures cleanup happens even if:
    # - Client disconnects before streaming starts (generator never consumed)
    # - Client disconnects mid-stream (generator cancelled)
    # - Server-side timeout or error occurs
    #
    # Note: If the StreamingResponse is created but never consumed (client
    # disconnects before response headers are sent), Starlette will call
    # aclose() on the generator, which triggers the finally block.
    #
    # We use a mutable flag to track whether the generator has started.
    # If it hasn't started when finally runs, we know we need to release
    # the semaphore but NOT decrement _active_scans (since we never incremented it).
    generator_started = False

    async def stream_batches():
        """Async generator that streams IPC chunks with resource tracking.

        Cancellation handling:
        - Checks client disconnect at top of each chunk iteration
        - Checks timeout/size limits before each fetch and before each yield
        - Uses stop_flag to propagate cancellation to sync fetch_segments()
        - finally block always runs for cleanup (semaphore release)

        Resource tracking:
        - _active_scans is incremented at generator start (inside try block)
        - Semaphore was already acquired by the caller
        - Both are released in finally block, guaranteeing cleanup

        Known limitation (v1):
        - fetch_segments() is sync, so we can't detect disconnect while
          blocked inside a fetch. If client disconnects during a slow fetch,
          that fetch completes before we notice. The stop_flag prevents
          subsequent fetches. To close this gap, run fetches via
          asyncio.to_thread() with cancellation support.
        """
        nonlocal generator_started

        # Track that we've started - this must be inside try/finally to ensure
        # cleanup on any exit path (normal, exception, or cancellation)
        generator_started = True
        state._active_scans += 1

        start_time = time.perf_counter()
        first_byte_sent = False  # Track TTFB
        ttfb_ms = 0.0  # Time to first byte
        max_fetch_ms = 0.0  # Max single fetch time

        # Register scan for progress tracking (used by /health/ready)
        state._scan_progress[scan_id] = (time.time(), 0)
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
            nonlocal ttfb_ms, max_fetch_ms
            scan_metrics.total_time_ms = (time.perf_counter() - start_time) * 1000
            scan_metrics.fetch_time_ms = scan_metrics.total_time_ms - scan_metrics.planning_time_ms
            scan_metrics.rows_returned = sum(t.num_rows for t in plan.tasks[:tasks_completed])

            # Record latencies to histogram for percentile tracking
            record_latency("total_request", scan_metrics.total_time_ms)
            if ttfb_ms > 0:
                record_latency("ttfb", ttfb_ms)
            if max_fetch_ms > 0:
                record_latency("fetch", max_fetch_ms)

            # Log slow operations (thresholds: total>500ms, ttfb>250ms, fetch>200ms)
            slow_stages = []
            if scan_metrics.total_time_ms > 500:
                slow_stages.append(f"total={scan_metrics.total_time_ms:.0f}ms")
            if ttfb_ms > 250:
                slow_stages.append(f"ttfb={ttfb_ms:.0f}ms")
            if max_fetch_ms > 200:
                slow_stages.append(f"max_fetch={max_fetch_ms:.0f}ms")

            if slow_stages and outcome == "complete":
                logger.warning(
                    "Slow scan detected",
                    scan_id=scan_id,
                    table_id=str(plan.table_identity),
                    slow_stages=", ".join(slow_stages),
                    total_ms=round(scan_metrics.total_time_ms, 1),
                    ttfb_ms=round(ttfb_ms, 1),
                    max_fetch_ms=round(max_fetch_ms, 1),
                    tasks=len(plan.tasks),
                    bytes_out=bytes_out,
                    tier=tier_name,
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
            # Parallel fetch configuration from config
            # Overlaps I/O with processing for better throughput on S3/cold storage.
            # Bounded by memory: each segment is ~row_group_size (typically 10-100MB).
            fetch_parallelism = state.config.fetch_parallelism

            # Async generator with out-of-order fetch completion and reordering buffer.
            # Fetches happen in parallel using dedicated thread pool, but segments
            # are yielded in order to maintain Arrow IPC stream correctness.
            # This maximizes I/O parallelism while preserving output order.
            async def fetch_segments_async():
                nonlocal tasks_completed, stop_flag, max_fetch_ms
                bytes_in_estimate = 0  # Track estimated input bytes

                # In-flight fetches: dict[idx -> (task, Future, start_time)]
                # Using dict allows O(1) lookup when any fetch completes
                in_flight: dict[int, tuple[Task, asyncio.Future, float]] = {}

                # Reordering buffer: completed segments waiting to be yielded
                # Holds out-of-order completions until earlier ones finish
                completed: dict[int, tuple[Task, bytes]] = {}

                # Next index to yield (maintains output order)
                next_yield_idx = 0

                def start_fetch(idx: int, task: "Task") -> tuple[asyncio.Future, float]:
                    """Start a fetch in the dedicated thread pool, return Future and start time."""
                    loop = asyncio.get_event_loop()
                    fetch_start = time.perf_counter()
                    future = loop.run_in_executor(
                        state._fetch_executor, state.fetcher.fetch_as_stream_bytes, task
                    )
                    return future, fetch_start

                def record_metrics(task: "Task", fetch_duration_ms: float = 0.0) -> None:
                    """Record cache hit/miss metrics for a task."""
                    nonlocal tasks_completed, max_fetch_ms
                    tasks_completed += 1
                    if task.cached:
                        scan_metrics.cache_hits += 1
                        scan_metrics.bytes_from_cache += task.bytes_read
                    else:
                        scan_metrics.cache_misses += 1
                        scan_metrics.bytes_from_storage += task.bytes_read

                    # Track max fetch time
                    if fetch_duration_ms > 0:
                        max_fetch_ms = max(max_fetch_ms, fetch_duration_ms)
                        # Log slow fetches (>200ms threshold)
                        if fetch_duration_ms > 200:
                            logger.warning(
                                "Slow fetch detected",
                                scan_id=scan_id,
                                fetch_ms=round(fetch_duration_ms, 1),
                                task_idx=tasks_completed,
                                cached=task.cached,
                                bytes_read=task.bytes_read,
                            )

                # Iterate through all tasks
                task_iter = iter(enumerate(plan.tasks))

                for idx, task in task_iter:
                    if stop_flag:
                        break

                    # Check limits before starting fetch
                    bytes_in_estimate += task.estimated_bytes
                    if bytes_in_estimate > max_response:
                        stop_flag = True
                        break

                    # Use prefetched first row group if available
                    if idx == 0 and plan.prefetched_first is not None:
                        segment = plan.prefetched_first
                        plan.prefetched_first = None
                        state._prefetch_used += 1  # Track successful prefetch use
                        record_metrics(task)
                        next_yield_idx = 1
                        yield segment
                        continue

                    # Start fetch in thread pool
                    future, fetch_start = start_fetch(idx, task)
                    in_flight[idx] = (task, future, fetch_start)

                    # Once we hit parallelism limit, wait for any to complete
                    while len(in_flight) >= fetch_parallelism:
                        # Check timeout
                        elapsed = time.perf_counter() - start_time
                        if elapsed > timeout:
                            stop_flag = True
                            break

                        # Wait for ANY in-flight fetch to complete (out-of-order)
                        done, _ = await asyncio.wait(
                            [f for _, f, _ in in_flight.values()],
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        # Process completed fetches
                        for future in done:
                            # Find which idx completed
                            completed_idx = None
                            for i, (t, f, _) in in_flight.items():
                                if f is future:
                                    completed_idx = i
                                    break

                            if completed_idx is None:
                                continue

                            task_completed, _, fetch_start = in_flight.pop(completed_idx)
                            fetch_duration_ms = (time.perf_counter() - fetch_start) * 1000
                            try:
                                segment = future.result()
                                record_metrics(task_completed, fetch_duration_ms)
                                # Store in reorder buffer
                                completed[completed_idx] = (task_completed, segment)
                            except Exception:
                                stop_flag = True
                                break

                        if stop_flag:
                            break

                        # Yield segments in order from reorder buffer
                        while next_yield_idx in completed:
                            _, segment = completed.pop(next_yield_idx)
                            next_yield_idx += 1
                            yield segment

                    if stop_flag:
                        break

                # Drain remaining in-flight fetches
                while in_flight and not stop_flag:
                    # Check timeout
                    elapsed = time.perf_counter() - start_time
                    if elapsed > timeout:
                        stop_flag = True
                        break

                    # Wait for ANY remaining fetch to complete
                    done, _ = await asyncio.wait(
                        [f for _, f, _ in in_flight.values()],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    for future in done:
                        # Find which idx completed
                        completed_idx = None
                        for i, (t, f, _) in in_flight.items():
                            if f is future:
                                completed_idx = i
                                break

                        if completed_idx is None:
                            continue

                        task_completed, _, fetch_start = in_flight.pop(completed_idx)
                        fetch_duration_ms = (time.perf_counter() - fetch_start) * 1000
                        try:
                            segment = future.result()
                            record_metrics(task_completed, fetch_duration_ms)
                            completed[completed_idx] = (task_completed, segment)
                        except Exception:
                            stop_flag = True
                            break

                    if stop_flag:
                        break

                    # Yield segments in order
                    while next_yield_idx in completed:
                        _, segment = completed.pop(next_yield_idx)
                        next_yield_idx += 1
                        yield segment

                # Yield any remaining completed segments in order
                while next_yield_idx in completed:
                    _, segment = completed.pop(next_yield_idx)
                    next_yield_idx += 1
                    yield segment

                # Cancel any remaining in-flight futures on early exit
                for _, future, _ in in_flight.values():
                    future.cancel()

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

                    # Track TTFB (time to first byte)
                    if not first_byte_sent:
                        first_byte_sent = True
                        ttfb_ms = (time.perf_counter() - start_time) * 1000
                        # Log slow TTFB (>250ms threshold)
                        if ttfb_ms > 250:
                            logger.warning(
                                "Slow TTFB detected",
                                scan_id=scan_id,
                                ttfb_ms=round(ttfb_ms, 1),
                                tasks=len(plan.tasks),
                                tier=tier_name,
                            )

                    # Update progress tracking (for /health/ready stuck scan detection)
                    state._scan_progress[scan_id] = (time.time(), bytes_out)

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

        except GeneratorExit:
            # Generator was closed by Starlette (client disconnected before
            # we could check is_disconnected(), or response was cancelled).
            # This is normal - log it and let finally clean up.
            state.metrics.record_client_disconnect()
            logger.debug("Scan generator closed by client disconnect", scan_id=scan_id)

        except asyncio.CancelledError:
            # Task was cancelled (e.g., server shutdown or request timeout).
            # Log and re-raise to let asyncio handle cancellation properly.
            state.metrics.record_client_disconnect()
            logger.debug("Scan cancelled", scan_id=scan_id)
            raise

        finally:
            # Always release resources - this runs on:
            # - Normal completion (return from generator)
            # - Client disconnect (GeneratorExit)
            # - Server-side exception (RuntimeError for timeout/size)
            # - Cancellation (CancelledError)
            #
            # Note: If generator_started is False, we were closed before
            # the first iteration (client disconnected very early).
            # In that case, we never incremented _active_scans.
            if generator_started:
                state._active_scans -= 1

            # Remove from progress tracking
            state._scan_progress.pop(scan_id, None)

            # Release the tier-specific semaphore
            scan_tier = state._scan_tier.pop(scan_id, None)
            if scan_tier == "interactive":
                state._active_interactive -= 1
                state._interactive_semaphore.release()
            elif scan_tier == "bulk":
                state._active_bulk -= 1
                state._bulk_semaphore.release()
            else:
                # Fallback: shouldn't happen but release legacy semaphore if tier unknown
                state._scan_semaphore.release()

    return StreamingResponse(
        stream_batches(),
        media_type="application/vnd.apache.arrow.stream",
    )


@app.delete("/v1/scan/{scan_id}")
async def delete_scan_v1(scan_id: str):
    """Delete a scan and free resources.

    Also cancels any pending prefetch for this scan to avoid wasted work.

    Error codes:
    - 404: Scan not found
    """
    state = get_state()

    if scan_id not in state.scans:
        raise HTTPException(status_code=404, detail=f"Scan {scan_id} not found")

    # Cancel any pending prefetch for this scan
    prefetch_future = state._prefetch_futures.pop(scan_id, None)
    if prefetch_future is not None:
        prefetch_future.cancel()
        state._prefetch_wasted += 1

    # Check if prefetch was completed but never used
    plan = state.scans[scan_id]
    if plan.prefetched_first is not None:
        state._prefetch_wasted += 1
        plan.prefetched_first = None  # Release the bytes

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


@app.get("/v1/cache/evictions")
async def get_cache_evictions_v1(
    include_events: Annotated[
        bool,
        Query(description="Include recent eviction events"),
    ] = False,
    limit: Annotated[
        int,
        Query(description="Max number of recent events to include", ge=1, le=100),
    ] = 10,
):
    """Get cache eviction metrics and monitoring data.

    Returns eviction statistics including:
    - Total evictions and bytes evicted (lifetime)
    - Evictions in last minute/hour
    - Eviction rate (per minute)
    - Pressure level indicator (low/medium/high/critical)

    Use include_events=true to get recent eviction events for debugging.

    Pressure levels:
    - low: < 1 eviction per minute (healthy)
    - medium: 1-5 evictions per minute (monitor)
    - high: 5-10 evictions per minute (consider increasing cache size)
    - critical: 10+ evictions per minute (cache is thrashing)
    """
    tracker = get_eviction_tracker()
    stats = tracker.get_stats()
    result = stats.to_dict()

    if include_events:
        result["recent_events"] = tracker.get_recent_events(limit)

    return result


@app.get("/v1/cache/histogram")
async def get_cache_histogram_v1():
    """Get cache hit/miss statistics over time windows.

    Returns hit rate trends for understanding cache effectiveness:
    - lifetime: Total hits, misses, hit rate, bytes served
    - windows: Statistics for 1 minute, 5 minutes, and 1 hour windows
    - top_tables: Top 5 tables by cache access count

    Each window includes:
    - hits/misses: Access counts
    - hit_rate: Hits / total (0.0 to 1.0)
    - bytes_from_cache/bytes_from_storage: Data served from each source

    Use this to:
    - Track cache warm-up progress (watch hit rate climb)
    - Identify cache thrashing (sudden hit rate drops)
    - Find hot tables that dominate cache usage
    """
    histogram = get_cache_histogram()
    return histogram.get_summary()


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


@app.get("/v1/debug/latency")
async def get_latency_histograms_v1():
    """Get latency histograms for each operation stage.

    Returns latency distribution data for:
    - plan: Table planning (catalog + metadata)
    - ttfb: Time to first byte
    - fetch: Individual row group fetch
    - total_request: End-to-end request time

    Each stage includes:
    - Histogram buckets with counts
    - Estimated percentiles (p50, p95, p99)
    - Count, sum, avg, max

    This is useful for:
    - Identifying which stage dominates tail latency
    - Understanding latency distribution over time
    - Detecting bimodal latency patterns
    """
    stats = get_latency_stats()

    # Add percentile estimates for key stages
    from strata.slow_ops import get_latency_percentiles

    result = {"histograms": stats}

    for stage in ["plan", "ttfb", "fetch", "total_request"]:
        if stage in stats:
            result["histograms"][stage]["percentiles"] = get_latency_percentiles(stage)

    return result


@app.get("/v1/debug/gc/pauses")
async def get_gc_pauses_v1(
    limit: Annotated[int, Query(description="Maximum pauses to return", ge=1, le=1000)] = 100,
):
    """Get recent GC pause events for debugging.

    Returns detailed timing information about recent garbage collection pauses.
    This is useful for:
    - Correlating latency spikes with GC activity
    - Understanding GC pause duration distribution
    - Diagnosing periodic latency stalls

    Returns:
    - pauses: List of recent GC pauses (most recent first)
      - timestamp: Unix timestamp when GC completed
      - generation: GC generation (0, 1, or 2)
      - duration_ms: Pause duration in milliseconds
    - stats: Aggregate statistics (p50, p95, p99 if enough data)
    """
    pauses = get_recent_gc_pauses(limit=limit)
    stats = get_gc_stats()

    return {
        "pauses": pauses,
        "stats": stats,
    }


@app.get("/v1/debug/pools")
async def get_pool_metrics_v1():
    """Get thread pool metrics for debugging.

    Returns utilization and queue depth for server thread pools:
    - planning: Thread pool for Iceberg catalog/metadata operations
    - fetch: Thread pool for Parquet row group I/O

    Each pool includes:
    - max_workers: Pool capacity
    - active_workers: Currently executing workers
    - queue_depth: Tasks waiting for a worker
    - utilization_pct: (active_workers / max_workers) * 100

    High queue_depth indicates pool saturation (bottleneck).
    """
    pool_tracker = get_pool_tracker()
    return pool_tracker.get_summary()


@app.get("/v1/debug/connections")
async def get_connection_metrics_v1():
    """Get HTTP connection metrics for debugging.

    Returns:
    - active_requests: Currently in-flight requests
    - total_requests: Total requests since server start
    - max_concurrent_requests: Peak concurrency observed
    - request_rate_per_sec: Average request rate
    - keepalive_pct: Percentage of requests using keep-alive

    High active_requests with low throughput may indicate connection issues.
    """
    connection_metrics = get_connection_metrics()
    return connection_metrics.get_stats()


@app.get("/v1/debug/memory")
async def get_memory_debug_v1(
    detailed: Annotated[
        bool,
        Query(description="Include detailed breakdown (slower, includes object type counts)"),
    ] = False,
):
    """Get memory profiling information for debugging.

    Returns memory statistics across multiple levels:
    - Arrow: Memory pool allocations (bytes_allocated, max_memory, pool_backend)
    - Python: GC tracked objects, objects by generation
    - Process: RSS and VMS memory (if available)

    Use detailed=true for comprehensive analysis including:
    - Top object types by count
    - GC thresholds and collection stats
    - Memory recommendations

    Note: detailed=true is more expensive and enumerates all GC objects.
    """
    if detailed:
        return get_detailed_memory_report()
    else:
        snapshot = get_memory_snapshot()
        return snapshot.to_dict()


@app.get("/v1/debug/rate-limits")
async def get_rate_limits_debug_v1():
    """Get rate limiter statistics for debugging.

    Returns:
    - total_requests: Total requests processed
    - allowed_requests: Requests that passed rate limiting
    - rejected_global: Requests rejected by global limit
    - rejected_client: Requests rejected by per-client limit
    - rejected_endpoint: Requests rejected by per-endpoint limit
    - active_clients: Number of tracked client buckets
    - global_tokens_available: Current global bucket tokens
    - enabled: Whether rate limiting is enabled
    """
    rate_limiter = get_rate_limiter()
    if rate_limiter is None:
        return {"error": "Rate limiter not initialized", "enabled": False}
    return rate_limiter.get_stats()


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
    prefix: Annotated[
        str | None, Query(description="Hash prefix to filter entries (hex, e.g., 'a1b2')")
    ] = None,
    table_id: Annotated[str | None, Query(description="Filter by table identifier")] = None,
    snapshot_id: Annotated[int | None, Query(description="Filter by snapshot ID")] = None,
    limit: Annotated[int, Query(description="Maximum entries to return", ge=1, le=1000)] = 100,
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

                results.append(
                    {
                        "hash": cache_hash,
                        "hash_prefix": cache_hash[:8],
                        "file_path": str(data_path.relative_to(cache.cache_dir)),
                        "file_exists": file_exists,
                        "file_size_bytes": file_size,
                        "metadata": meta_data,
                    }
                )

            except Exception as e:
                # Include corrupted entries for debugging
                results.append(
                    {
                        "hash": cache_hash,
                        "file_path": str(meta_path.relative_to(cache.cache_dir)),
                        "error": str(e),
                        "corrupted": True,
                    }
                )
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


@app.post("/v1/cache/warm", response_model=WarmResponse)
async def warm_cache_v1(request: WarmRequest):
    """Warm the cache for specified tables.

    Preloads row group data into the cache so subsequent queries are fast.
    This is useful for:
    - Warming cache after server restart
    - Preloading data before a batch of dashboards query it
    - Ensuring low latency for critical tables

    The operation runs synchronously and returns when all row groups
    have been fetched and cached (or skipped if already cached).

    Request body:
    - tables: List of table URIs to warm (e.g., "file:///warehouse#ns.table")
    - columns: Optional column projection (None = all columns)
    - max_row_groups: Optional limit per table (None = all row groups)
    - concurrent: Max concurrent fetches (default 4)

    Returns:
    - tables_warmed: Number of tables processed
    - row_groups_cached: Total row groups written to cache
    - row_groups_skipped: Already in cache (cache hits)
    - bytes_written: Total bytes written to cache
    - elapsed_ms: Total time taken
    - errors: Any errors encountered (list of error messages)
    """
    state = get_state()

    start_time = time.perf_counter()
    tables_warmed = 0
    row_groups_cached = 0
    row_groups_skipped = 0
    bytes_written = 0
    errors: list[str] = []

    # Limit concurrency for cache warming
    warming_semaphore = asyncio.Semaphore(request.concurrent)

    async def fetch_task(task: Task) -> tuple[bool, int]:
        """Fetch a single task, return (was_cached, bytes_written)."""
        async with warming_semaphore:
            try:
                # Run fetch in thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, state.fetcher.fetch_as_stream_bytes, task)
                if task.cached:
                    return (True, 0)  # Already cached
                else:
                    return (False, task.bytes_read)
            except Exception:
                return (False, 0)

    for table_uri in request.tables:
        try:
            # Plan the table
            plan = state.planner.plan(
                table_uri=table_uri,
                snapshot_id=None,  # Current snapshot
                columns=request.columns,
                filters=[],
            )

            # Limit row groups if specified
            tasks = plan.tasks
            if request.max_row_groups is not None:
                tasks = tasks[: request.max_row_groups]

            if not tasks:
                tables_warmed += 1
                continue

            # Fetch all tasks concurrently (bounded by semaphore)
            results = await asyncio.gather(
                *[fetch_task(task) for task in tasks],
                return_exceptions=True,
            )

            for result in results:
                if isinstance(result, Exception):
                    continue
                was_cached, written = result
                if was_cached:
                    row_groups_skipped += 1
                else:
                    row_groups_cached += 1
                    bytes_written += written

            tables_warmed += 1

        except Exception as e:
            errors.append(f"{table_uri}: {e!s}")

    elapsed_ms = (time.perf_counter() - start_time) * 1000

    # Log the warming operation
    state.metrics.log_event(
        "cache_warm",
        tables_warmed=tables_warmed,
        row_groups_cached=row_groups_cached,
        row_groups_skipped=row_groups_skipped,
        bytes_written=bytes_written,
        elapsed_ms=elapsed_ms,
        errors_count=len(errors),
    )

    return WarmResponse(
        tables_warmed=tables_warmed,
        row_groups_cached=row_groups_cached,
        row_groups_skipped=row_groups_skipped,
        bytes_written=bytes_written,
        elapsed_ms=elapsed_ms,
        errors=errors,
    )


@app.post("/v1/cache/warm/async", response_model=WarmAsyncResponse)
async def warm_cache_async_v1(request: WarmAsyncRequest):
    """Start an async/background cache warming job.

    Unlike POST /v1/cache/warm (which blocks until complete), this endpoint
    starts a background job and returns immediately with a job ID for tracking.

    This is useful for:
    - Warming large tables without blocking the request
    - Scheduling warmup before batch operations
    - Warming specific snapshots (not just current)

    Request body:
    - tables: List of table URIs to warm
    - columns: Optional column projection (None = all columns)
    - snapshot_id: Optional specific snapshot (None = current)
    - max_row_groups: Optional limit per table (None = all)
    - concurrent: Max concurrent fetches within job (default 4)
    - priority: Job priority (higher = more urgent, default 0)

    Returns:
    - job_id: Unique ID for tracking progress via GET /v1/cache/warm/jobs/{id}
    - status: Initial job status (pending or running)
    - tables_count: Number of tables in the job
    - message: Human-readable status message
    """
    state = get_state()

    if state._cache_warmer is None:
        raise HTTPException(status_code=503, detail="Cache warmer not initialized")

    job_id = await state._cache_warmer.start_job(request)

    return WarmAsyncResponse(
        job_id=job_id,
        status=WarmJobStatus.PENDING,
        tables_count=len(request.tables),
        message=f"Warming job started with {len(request.tables)} tables",
    )


@app.get("/v1/cache/warm/jobs")
async def list_warm_jobs_v1(
    include_completed: Annotated[bool, Query(description="Include completed/failed jobs")] = False,
):
    """List all cache warming jobs.

    Returns a list of all warming jobs with their current status and progress.
    By default only shows pending and running jobs.

    Query params:
    - include_completed: Include completed/failed/cancelled jobs (default false)

    Returns:
    - jobs: List of job progress objects
    """
    state = get_state()

    if state._cache_warmer is None:
        return {"jobs": []}

    jobs = state._cache_warmer.list_jobs(include_completed=include_completed)
    return {"jobs": [j.model_dump() for j in jobs]}


@app.get("/v1/cache/warm/jobs/{job_id}", response_model=WarmJobProgress)
async def get_warm_job_v1(job_id: str):
    """Get progress for a specific warming job.

    Returns detailed progress information for a warming job including:
    - Current status (pending, running, completed, failed, cancelled)
    - Tables completed vs total
    - Row groups cached vs skipped
    - Bytes written
    - Elapsed time
    - Current table being warmed
    - Any errors encountered

    Path params:
    - job_id: Job ID returned from POST /v1/cache/warm/async
    """
    state = get_state()

    if state._cache_warmer is None:
        raise HTTPException(status_code=404, detail="Job not found")

    progress = state._cache_warmer.get_progress(job_id)
    if progress is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return progress


@app.delete("/v1/cache/warm/jobs/{job_id}")
async def cancel_warm_job_v1(job_id: str):
    """Cancel a running warming job.

    Cancels the job and stops any in-progress warming operations.
    Already-cached data is not removed.

    Path params:
    - job_id: Job ID to cancel

    Returns:
    - cancelled: True if job was cancelled
    - message: Human-readable result message
    """
    state = get_state()

    if state._cache_warmer is None:
        raise HTTPException(status_code=404, detail="Job not found")

    cancelled = await state._cache_warmer.cancel_job(job_id)

    if cancelled:
        return {"cancelled": True, "message": f"Job {job_id} cancelled"}
    else:
        raise HTTPException(
            status_code=404,
            detail="Job not found or already completed",
        )


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
