#!/usr/bin/env python3
"""Soak test for Strata production readiness.

Long-running stability test to detect memory leaks, latency drift, and resource exhaustion.

Features:
- Duration: 1-2 hours (configurable)
- Memory tracking with psutil
- Latency drift detection (baseline vs current p95)
- Periodic stress spikes (2x load every 15 min)
- Cache stability monitoring

Success criteria:
- Memory growth < 10% after warmup
- p95 latency drift < 20% from baseline
- Final active_scans = 0
- Cache eviction rate stabilizes
- Zero 5xx errors after warmup

Usage:
    # Quick 15-minute test
    python benchmarks/soak_test.py --duration 0.25

    # Standard 1-hour test
    python benchmarks/soak_test.py

    # Extended 2-hour test
    python benchmarks/soak_test.py --duration 2

    # With custom users
    python benchmarks/soak_test.py --users 50 --duration 1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import httpx
import psutil
import pyarrow as pa
import pyarrow.ipc as ipc

# =============================================================================
# Configuration
# =============================================================================


class Phase(Enum):
    """Soak test phases."""

    WARMUP = "warmup"
    STEADY = "steady"
    SPIKE = "spike"
    COOLDOWN = "cooldown"


@dataclass
class SoakConfig:
    """Configuration for soak test."""

    # Server settings
    base_url: str = "http://127.0.0.1:8765"
    start_server: bool = True
    server_host: str = "127.0.0.1"
    server_port: int = 0  # Auto-find

    # Directories
    warehouse_dir: Path | None = None
    cache_dir: Path | None = None
    keep_dirs: bool = False

    # Duration
    duration_hours: float = 1.0  # 1 hour default
    warmup_minutes: float = 5.0  # 5 min warmup
    cooldown_minutes: float = 2.0  # 2 min cooldown

    # Stress spikes
    spike_interval_minutes: float = 15.0  # Every 15 min
    spike_duration_seconds: float = 30.0  # 30s spike
    spike_multiplier: float = 2.0  # 2x load during spike

    # Concurrency
    base_users: int = 30  # Base concurrent users
    dashboard_ratio: float = 0.8  # 80% dashboard
    analyst_ratio: float = 0.15  # 15% analyst
    bulk_ratio: float = 0.05  # 5% bulk

    # Table sizes (moderate for long-running test)
    num_tables: int = 8
    rows_per_table: int = 50_000
    payload_bytes: int = 100

    # Cache
    cache_size_bytes: int = 200 * 1024 * 1024  # 200MB

    # Metrics collection
    metrics_interval_s: float = 30.0  # Sample every 30s
    drift_window_minutes: float = 5.0  # 5-min window for drift detection
    results_dir: Path = field(default_factory=lambda: Path("benchmarks/results"))

    # Request settings
    request_timeout_s: float = 60.0
    connect_timeout_s: float = 5.0
    max_connections: int = 100

    # Success criteria
    max_memory_growth_pct: float = 10.0  # Max 10% memory growth after warmup
    max_latency_drift_pct: float = 20.0  # Max 20% p95 drift from baseline
    min_prefetch_efficiency: float = 0.5  # Min 50% prefetch used

    # Misc
    seed: int = 42
    dry_run: bool = False

    def __post_init__(self):
        self.results_dir = Path(self.results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)

    @property
    def duration_s(self) -> float:
        return self.duration_hours * 3600

    @property
    def warmup_s(self) -> float:
        return self.warmup_minutes * 60

    @property
    def cooldown_s(self) -> float:
        return self.cooldown_minutes * 60

    @property
    def spike_interval_s(self) -> float:
        return self.spike_interval_minutes * 60

    @property
    def dashboard_users(self) -> int:
        return int(self.base_users * self.dashboard_ratio)

    @property
    def analyst_users(self) -> int:
        return int(self.base_users * self.analyst_ratio)

    @property
    def bulk_users(self) -> int:
        return max(1, self.base_users - self.dashboard_users - self.analyst_users)


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class ResourceSample:
    """Resource sample at a point in time.

    Captures multiple resource dimensions to detect:
    - Memory leaks (RSS growth)
    - Allocator fragmentation (RSS vs cache bytes)
    - File descriptor leaks (num_fds growth)
    - Thread leaks (num_threads growth)
    - Cache bloat (cache_bytes vs expected)
    """

    timestamp: float
    elapsed_s: float
    phase: str

    # Process-level metrics (from psutil)
    rss_bytes: int
    num_fds: int  # File descriptors (Linux) or -1 if unavailable
    num_threads: int

    # Server-reported cache metrics (from /metrics endpoint)
    cache_bytes: int = 0  # Actual bytes in cache
    cache_entries: int = 0  # Number of cached row groups
    cache_evictions: int = 0  # Total evictions since start

    def to_dict(self) -> dict:
        return {
            "type": "resource",
            "timestamp": self.timestamp,
            "elapsed_s": self.elapsed_s,
            "phase": self.phase,
            "rss_mb": self.rss_bytes / (1024 * 1024),
            "num_fds": self.num_fds,
            "num_threads": self.num_threads,
            "cache_mb": self.cache_bytes / (1024 * 1024),
            "cache_entries": self.cache_entries,
            "cache_evictions": self.cache_evictions,
        }


@dataclass
class LatencySample:
    """Latency sample for drift detection."""

    timestamp: float
    elapsed_s: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    request_count: int
    success_count: int
    error_count: int
    phase: str

    def to_dict(self) -> dict:
        return {
            "type": "latency",
            "timestamp": self.timestamp,
            "elapsed_s": self.elapsed_s,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
            "request_count": self.request_count,
            "success_count": self.success_count,
            "error_count": self.error_count,
            "phase": self.phase,
        }


@dataclass
class SoakResults:
    """Aggregated soak test results."""

    duration_hours: float
    total_requests: int
    success_rate: float

    # Memory metrics
    baseline_rss_mb: float
    peak_rss_mb: float
    final_rss_mb: float
    memory_growth_pct: float

    # Latency metrics
    baseline_p95_ms: float
    final_p95_ms: float
    max_p95_ms: float
    latency_drift_pct: float

    # Stability metrics
    final_active_scans: int
    cache_hit_rate: float
    prefetch_efficiency: float

    # Spike behavior
    spike_count: int
    spike_recovery_ok: bool

    # Resource metrics (fds, threads, cache)
    baseline_fds: float = -1  # -1 means unavailable
    final_fds: int = -1
    baseline_threads: float = 0
    final_threads: int = 0
    final_cache_mb: float = 0
    final_cache_entries: int = 0
    total_cache_evictions: int = 0

    # Error breakdown (post-warmup)
    post_warmup_5xx: int = 0  # HTTP 5xx errors after warmup
    post_warmup_timeouts: int = 0  # Timeout errors after warmup
    post_warmup_arrow_errors: int = 0  # Arrow decode errors (schema/corruption)
    post_warmup_other_errors: int = 0  # Other errors after warmup

    # Success criteria
    memory_ok: bool = True
    latency_ok: bool = True
    no_leak: bool = True
    no_fd_leak: bool = True  # FDs didn't grow significantly
    no_thread_leak: bool = True  # Threads didn't grow significantly
    no_errors_post_warmup: bool = True  # Zero 5xx/timeouts after warmup
    overall_pass: bool = True

    # Data quality
    insufficient_data: bool = False  # True if not enough samples for reliable metrics

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


# =============================================================================
# Server Management (reused from stress_test.py)
# =============================================================================


class ServerProcess:
    """Manages a Strata server as a subprocess."""

    def __init__(
        self,
        host: str,
        port: int,
        cache_dir: Path,
        max_cache_size_bytes: int | None = None,
    ):
        self.host = host
        self.port = port
        self.cache_dir = cache_dir
        self.max_cache_size_bytes = max_cache_size_bytes
        self._process: subprocess.Popen | None = None

    def start(self, timeout: float = 30.0):
        """Start the server as a subprocess."""
        env = os.environ.copy()
        env["STRATA_HOST"] = self.host
        env["STRATA_PORT"] = str(self.port)
        env["STRATA_CACHE_DIR"] = str(self.cache_dir)
        env["STRATA_METRICS_ENABLED"] = "true"

        if self.max_cache_size_bytes is not None:
            env["STRATA_MAX_CACHE_SIZE_BYTES"] = str(self.max_cache_size_bytes)

        self._process = subprocess.Popen(
            [sys.executable, "-m", "strata.server"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        self._wait_for_ready(timeout)

    def _wait_for_ready(self, timeout: float = 30.0):
        """Wait for the server to respond to health checks."""
        start = time.perf_counter()
        url = f"http://{self.host}:{self.port}/health"

        while time.perf_counter() - start < timeout:
            try:
                response = httpx.get(url, timeout=1.0)
                if response.status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(0.1)

        raise TimeoutError(f"Server did not start within {timeout}s")

    def stop(self):
        """Stop the server subprocess."""
        if self._process:
            self._process.terminate()
            try:
                self._process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
            self._process = None

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process else None


def find_free_port() -> int:
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# =============================================================================
# Warehouse Generation (simplified from stress_test.py)
# =============================================================================


def generate_soak_warehouse(config: SoakConfig) -> dict[str, Any]:
    """Generate warehouse for soak testing."""
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DoubleType,
        IntegerType,
        LongType,
        NestedField,
        StringType,
    )

    warehouse_path = config.warehouse_dir
    warehouse_path.mkdir(parents=True, exist_ok=True)

    catalog = SqlCatalog(
        "strata",
        **{
            "uri": f"sqlite:///{warehouse_path / 'catalog.db'}",
            "warehouse": str(warehouse_path),
        },
    )

    try:
        catalog.create_namespace("soak")
    except Exception:
        pass

    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "ts", LongType(), required=False),
        NestedField(3, "user_id", IntegerType(), required=False),
        NestedField(4, "category", StringType(), required=False),
        NestedField(5, "value", DoubleType(), required=False),
        NestedField(6, "payload", StringType(), required=False),
    )

    tables_info = []
    categories = ["electronics", "clothing", "food", "books", "sports", "home", "auto"]
    random.seed(config.seed)

    for i in range(config.num_tables):
        table_name = f"table_{i:02d}"
        table_id = f"soak.{table_name}"

        print(f"  Creating {table_name} ({config.rows_per_table:,} rows)...")

        try:
            table = catalog.load_table(table_id)
        except Exception:
            table = catalog.create_table(table_id, schema)

            chunk_size = min(50_000, config.rows_per_table)
            base_ts = 1704067200000000

            for chunk_start in range(0, config.rows_per_table, chunk_size):
                chunk_end = min(chunk_start + chunk_size, config.rows_per_table)
                actual_chunk_size = chunk_end - chunk_start

                data = pa.table(
                    {
                        "id": pa.array(range(chunk_start, chunk_end), type=pa.int64()),
                        "ts": pa.array(
                            [base_ts + (chunk_start + j) * 1000 for j in range(actual_chunk_size)],
                            type=pa.int64(),
                        ),
                        "user_id": pa.array(
                            [random.randint(1, 10000) for _ in range(actual_chunk_size)],
                            type=pa.int32(),
                        ),
                        "category": pa.array(
                            [random.choice(categories) for _ in range(actual_chunk_size)],
                            type=pa.string(),
                        ),
                        "value": pa.array(
                            [random.uniform(0.0, 1000.0) for _ in range(actual_chunk_size)],
                            type=pa.float64(),
                        ),
                        "payload": pa.array(
                            [
                                f"data_{chunk_start + j:08d}_" + "x" * config.payload_bytes
                                for j in range(actual_chunk_size)
                            ],
                            type=pa.string(),
                        ),
                    }
                )
                table.append(data)

        snapshot_id = table.current_snapshot().snapshot_id

        tables_info.append(
            {
                "name": table_name,
                "table_id": table_id,
                "table_uri": f"file://{warehouse_path}#soak.{table_name}",
                "snapshot_id": snapshot_id,
            }
        )

    return {
        "catalog": catalog,
        "warehouse_path": warehouse_path,
        "tables": tables_info,
    }


# =============================================================================
# Soak Test Driver
# =============================================================================


def _percentile(data: list[float], p: float) -> float:
    """Compute percentile."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


class SoakDriver:
    """Drives soak test workload."""

    def __init__(self, config: SoakConfig, tables_info: list[dict], server_pid: int | None):
        self.config = config
        self.tables_info = tables_info
        self.server_pid = server_pid
        self.rng = random.Random(config.seed)

        # Column sets
        self.dashboard_columns = ["id", "ts", "value"]
        self.analyst_columns = ["id", "ts", "user_id", "category", "value"]
        self.bulk_columns = ["id", "ts", "user_id", "category", "value", "payload"]
        self.categories = ["electronics", "clothing", "food", "books", "sports", "home", "auto"]

        # Results collection
        # Each request is tracked as (latency_ms, error_or_none, phase) to enable per-phase analysis
        self.request_results: list[tuple[float, str | None, str]] = []
        self.resource_samples: list[ResourceSample] = []
        self.latency_samples: list[LatencySample] = []
        self._lock = asyncio.Lock()

        # Phase tracking
        self.current_phase = Phase.WARMUP
        self.spike_count = 0
        # Track spike events: list of (spike_start_elapsed_s, spike_end_elapsed_s)
        self.spike_events: list[tuple[float, float]] = []

        # HTTP client
        self._client: httpx.AsyncClient | None = None

        # Stop event
        self._stop_event = asyncio.Event()

    async def start(self):
        """Start the HTTP client."""
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            limits=httpx.Limits(
                max_connections=self.config.max_connections,
                max_keepalive_connections=self.config.max_connections // 2,
            ),
            timeout=httpx.Timeout(
                connect=self.config.connect_timeout_s,
                read=self.config.request_timeout_s,
                write=self.config.request_timeout_s,
                pool=self.config.connect_timeout_s,
            ),
        )

    async def stop(self):
        """Stop the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def get_process_resources(self) -> tuple[int, int, int]:
        """Get server process resource metrics.

        Returns:
            Tuple of (rss_bytes, num_fds, num_threads).
            Returns (0, -1, 0) if process is unavailable.
        """
        if self.server_pid is None:
            return 0, -1, 0
        try:
            proc = psutil.Process(self.server_pid)
            rss = proc.memory_info().rss
            num_threads = proc.num_threads()
            # num_fds is Linux-only; returns -1 on macOS/Windows
            try:
                num_fds = proc.num_fds()
            except AttributeError:
                # macOS/Windows: use open_files count as approximation
                try:
                    num_fds = len(proc.open_files())
                except (psutil.AccessDenied, psutil.NoSuchProcess):
                    num_fds = -1
            return rss, num_fds, num_threads
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0, -1, 0

    async def execute_scan(self, user_type: str) -> tuple[float, str | None]:
        """Execute a single scan and return (latency_ms, error).

        For dashboard users, randomly decodes 10-20% of responses using
        pyarrow.ipc.open_stream to catch schema issues and simulate
        client-side parsing costs.
        """
        start = time.perf_counter()
        scan_id = None
        error = None

        # Decide whether to decode Arrow for this request (dashboard only, 15% of requests)
        should_decode = user_type == "dashboard" and self.rng.random() < 0.15

        try:
            # Select table and columns based on user type
            table = self.rng.choice(self.tables_info)

            if user_type == "dashboard":
                columns = self.dashboard_columns
                filters = [
                    {"column": "category", "op": "=", "value": self.rng.choice(self.categories)}
                ]
            elif user_type == "analyst":
                columns = self.analyst_columns
                min_val = self.rng.uniform(0, 500)
                filters = [
                    {"column": "value", "op": ">=", "value": min_val},
                    {"column": "value", "op": "<=", "value": min_val + 300},
                ]
            else:  # bulk
                columns = self.bulk_columns
                filters = []

            # POST /v1/scan
            response = await self._client.post(
                "/v1/scan",
                json={
                    "table_uri": table["table_uri"],
                    "snapshot_id": table["snapshot_id"],
                    "columns": columns,
                    "filters": filters,
                },
            )

            if response.status_code != 200:
                error = f"HTTP {response.status_code}"
            else:
                scan_id = response.json()["scan_id"]

                # Stream response
                async with self._client.stream("GET", f"/v1/scan/{scan_id}/batches") as stream:
                    stream.raise_for_status()

                    if should_decode:
                        # Collect bytes and decode Arrow to catch schema issues
                        # and simulate client-side parsing costs
                        chunks = []
                        async for chunk in stream.aiter_bytes(chunk_size=1024 * 1024):
                            chunks.append(chunk)

                        if chunks:
                            # Decode the Arrow IPC stream
                            data = b"".join(chunks)
                            reader = ipc.open_stream(pa.BufferReader(data))
                            # Read all batches to simulate full client processing
                            total_rows = 0
                            for batch in reader:
                                total_rows += batch.num_rows
                            # Schema validation happens implicitly during read
                    else:
                        # Just drain bytes without decoding
                        async for _ in stream.aiter_bytes(chunk_size=1024 * 1024):
                            pass

        except httpx.TimeoutException:
            error = "timeout"
        except httpx.HTTPStatusError as e:
            error = f"HTTP {e.response.status_code}"
        except pa.ArrowInvalid as e:
            # Arrow decoding error - schema issue or corrupt data
            error = f"arrow_decode: {str(e)[:80]}"
        except Exception as e:
            error = str(e)[:100]
        finally:
            if scan_id:
                try:
                    await self._client.delete(f"/v1/scan/{scan_id}")
                except Exception:
                    pass

        latency_ms = (time.perf_counter() - start) * 1000
        return latency_ms, error

    async def user_loop(self, user_id: int, user_type: str, get_duration: callable):
        """Run user loop."""
        # Stagger start
        await asyncio.sleep(self.rng.uniform(0, 1.0))

        while not self._stop_event.is_set():
            duration = get_duration()
            if duration <= 0:
                break

            latency_ms, error = await self.execute_scan(user_type)

            async with self._lock:
                self.request_results.append((latency_ms, error, self.current_phase.value))

            # Think time
            if user_type == "dashboard":
                await asyncio.sleep(self.rng.uniform(0.5, 1.5))
            elif user_type == "analyst":
                await asyncio.sleep(self.rng.uniform(2.0, 5.0))
            else:
                await asyncio.sleep(self.rng.uniform(5.0, 15.0))

    async def collect_samples(self, start_time: float):
        """Collect resource and latency samples periodically."""
        while not self._stop_event.is_set():
            await asyncio.sleep(self.config.metrics_interval_s)

            elapsed = time.perf_counter() - start_time
            timestamp = time.time()

            # Get process-level resources
            rss, num_fds, num_threads = self.get_process_resources()

            # Get server-reported cache metrics
            server_metrics = await self._get_metrics()
            disk_cache = server_metrics.get("disk_cache", {})
            cache_bytes = disk_cache.get("bytes_current", 0)
            cache_evictions = disk_cache.get("evictions_count", 0)
            # Entry count not directly exposed; use evictions as proxy for activity
            cache_entries = 0  # Not available from /metrics

            # Resource sample (combines process + server metrics)
            self.resource_samples.append(
                ResourceSample(
                    timestamp=timestamp,
                    elapsed_s=elapsed,
                    phase=self.current_phase.value,
                    rss_bytes=rss,
                    num_fds=num_fds,
                    num_threads=num_threads,
                    cache_bytes=cache_bytes,
                    cache_entries=cache_entries,
                    cache_evictions=cache_evictions,
                )
            )

            # Latency sample from recent requests
            async with self._lock:
                recent_results = self.request_results[-1000:]  # Last 1000
                total_results = len(self.request_results)

            if recent_results:
                recent_latencies = [r[0] for r in recent_results]
                recent_errors = sum(1 for r in recent_results if r[1] is not None)
                total_errors = sum(1 for r in self.request_results if r[1] is not None)
                self.latency_samples.append(
                    LatencySample(
                        timestamp=timestamp,
                        elapsed_s=elapsed,
                        p50_ms=_percentile(recent_latencies, 0.5),
                        p95_ms=_percentile(recent_latencies, 0.95),
                        p99_ms=_percentile(recent_latencies, 0.99),
                        request_count=total_results,
                        success_count=total_results - total_errors,
                        error_count=total_errors,
                        phase=self.current_phase.value,
                    )
                )

                # Print status with extended resource info
                rss_mb = rss / (1024 * 1024)
                cache_mb = cache_bytes / (1024 * 1024)
                p95 = _percentile(recent_latencies, 0.95)
                fd_str = str(num_fds) if num_fds >= 0 else "n/a"
                print(
                    f"  [{elapsed/60:5.1f}m] {self.current_phase.value:8s} "
                    f"RSS={rss_mb:5.0f}MB cache={cache_mb:5.0f}MB fds={fd_str:>4} "
                    f"p95={p95:6.1f}ms reqs={total_results} errs={recent_errors}"
                )

    async def run_soak_test(self) -> SoakResults:
        """Run the full soak test."""
        print(f"\n  Starting soak test")
        print(f"  Duration: {self.config.duration_hours}h")
        print(f"  Users: {self.config.base_users} ({self.config.dashboard_users} dashboard)")
        print(f"  Spikes: every {self.config.spike_interval_minutes}m")

        start_time = time.perf_counter()
        total_duration = self.config.duration_s

        # Calculate phase boundaries
        warmup_end = self.config.warmup_s
        cooldown_start = total_duration - self.config.cooldown_s

        def get_remaining():
            elapsed = time.perf_counter() - start_time
            return max(0, total_duration - elapsed)

        # Track spike timing
        last_spike_time = 0.0
        spike_start_elapsed = 0.0
        in_spike = False

        # Warmup phase
        print(f"\n  Phase: WARMUP ({self.config.warmup_minutes}m)")
        self.current_phase = Phase.WARMUP

        # Start sample collection
        sample_task = asyncio.create_task(self.collect_samples(start_time))

        # Create user tasks
        user_tasks = []
        user_id = 0

        for _ in range(self.config.dashboard_users):
            user_tasks.append(
                asyncio.create_task(self.user_loop(user_id, "dashboard", get_remaining))
            )
            user_id += 1

        for _ in range(self.config.analyst_users):
            user_tasks.append(
                asyncio.create_task(self.user_loop(user_id, "analyst", get_remaining))
            )
            user_id += 1

        for _ in range(self.config.bulk_users):
            user_tasks.append(asyncio.create_task(self.user_loop(user_id, "bulk", get_remaining)))
            user_id += 1

        # Extra users for spikes (initially paused)
        spike_users = []

        # Run test
        try:
            while time.perf_counter() - start_time < total_duration:
                elapsed = time.perf_counter() - start_time

                # Update phase
                if elapsed < warmup_end:
                    self.current_phase = Phase.WARMUP
                elif elapsed >= cooldown_start:
                    if self.current_phase != Phase.COOLDOWN:
                        print(f"\n  Phase: COOLDOWN ({self.config.cooldown_minutes}m)")
                    self.current_phase = Phase.COOLDOWN
                else:
                    # Check for spike
                    time_since_spike = elapsed - last_spike_time

                    if in_spike and time_since_spike >= self.config.spike_duration_seconds:
                        # End spike
                        in_spike = False
                        self.current_phase = Phase.STEADY
                        # Record spike end time
                        self.spike_events.append((spike_start_elapsed, elapsed))
                        print("  Spike ended")
                        # Cancel spike users and await them properly
                        for task in spike_users:
                            task.cancel()
                        await asyncio.gather(*spike_users, return_exceptions=True)
                        spike_users = []
                    elif (
                        not in_spike
                        and time_since_spike >= self.config.spike_interval_s
                        and elapsed >= warmup_end
                    ):
                        # Start spike
                        in_spike = True
                        self.spike_count += 1
                        spike_start_elapsed = elapsed
                        last_spike_time = elapsed
                        self.current_phase = Phase.SPIKE
                        print(f"\n  Phase: SPIKE #{self.spike_count} (2x load for 30s)")
                        # Add spike users
                        extra_users = int(self.config.base_users * (self.config.spike_multiplier - 1))
                        for i in range(extra_users):
                            spike_users.append(
                                asyncio.create_task(
                                    self.user_loop(1000 + i, "dashboard", get_remaining)
                                )
                            )
                    elif not in_spike and self.current_phase != Phase.STEADY:
                        if self.current_phase == Phase.WARMUP:
                            print(f"\n  Phase: STEADY")
                        self.current_phase = Phase.STEADY

                await asyncio.sleep(1.0)

        except KeyboardInterrupt:
            print("\n  Interrupted by user")
        finally:
            self._stop_event.set()

            # Cancel all tasks
            all_tasks = user_tasks + spike_users + [sample_task]
            for task in all_tasks:
                task.cancel()

            # Await all cancelled tasks to ensure clean shutdown
            # This prevents warnings and ensures HTTP streams are closed
            print("\n  Draining in-flight requests...")
            await asyncio.gather(*all_tasks, return_exceptions=True)

            # Brief pause to let server finish processing any remaining requests
            await asyncio.sleep(0.5)

        # Wait for active scans to drain before reading metrics
        await self._wait_for_drain()

        # Get final metrics
        final_metrics = await self._get_metrics()

        # Compute results
        return self._compute_results(start_time, final_metrics)

    async def _wait_for_drain(self, timeout_s: float = 10.0) -> None:
        """Wait for server to drain active scans.

        Polls the metrics endpoint until active_scans reaches 0 or timeout.

        Args:
            timeout_s: Maximum time to wait for drain
        """
        start = time.perf_counter()
        while time.perf_counter() - start < timeout_s:
            try:
                metrics = await self._get_metrics()
                active = metrics.get("resource_limits", {}).get("active_scans", 0)
                if active == 0:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.2)
        print("  Warning: drain timeout - some scans may still be active")

    async def _get_metrics(self) -> dict:
        """Get server metrics."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{self.config.base_url}/metrics")
                response.raise_for_status()
                return response.json()
        except Exception:
            return {}

    def _check_spike_recovery(self, baseline_p95: float) -> bool:
        """Check if latency recovered after each spike.

        For each spike, verify that p95 latency returns within 20% of baseline
        within 2 minutes after the spike ends.

        Args:
            baseline_p95: Baseline p95 latency from warmup phase

        Returns:
            True if all spikes recovered properly, False otherwise
        """
        if not self.spike_events or baseline_p95 <= 0:
            # No spikes occurred or no baseline - consider it a pass
            return True

        recovery_window_s = 120.0  # 2 minutes to recover
        max_recovery_threshold = 1.20  # Within 20% of baseline

        for spike_start, spike_end in self.spike_events:
            # Find latency samples in the recovery window (spike_end to spike_end + 2min)
            recovery_samples = [
                s
                for s in self.latency_samples
                if spike_end <= s.elapsed_s <= spike_end + recovery_window_s
                and s.phase == Phase.STEADY.value
            ]

            if not recovery_samples:
                # No samples in recovery window - can't verify, assume OK
                continue

            # Check if any sample in the window shows recovery
            # (p95 within 20% of baseline)
            recovered = False
            for sample in recovery_samples:
                if sample.p95_ms <= baseline_p95 * max_recovery_threshold:
                    recovered = True
                    break

            if not recovered:
                # This spike didn't recover in time
                return False

        return True

    def _compute_results(self, start_time: float, final_metrics: dict) -> SoakResults:
        """Compute soak test results."""
        duration_hours = (time.perf_counter() - start_time) / 3600

        # Minimum samples required for reliable baseline (at least 2 samples)
        min_baseline_samples = 2

        # Resource analysis - use last 2 minutes of warmup for baseline
        warmup_samples = [s for s in self.resource_samples if s.phase == Phase.WARMUP.value]
        steady_samples = [s for s in self.resource_samples if s.phase == Phase.STEADY.value]

        # Compute baseline from last N samples of warmup (covering ~2 min window)
        baseline_window_samples = 4  # ~2 min at 30s interval
        warmup_tail = warmup_samples[-baseline_window_samples:] if warmup_samples else []
        baseline_rss = (
            sum(s.rss_bytes for s in warmup_tail) / len(warmup_tail)
            if len(warmup_tail) >= min_baseline_samples
            else 0
        )
        peak_rss = max((s.rss_bytes for s in self.resource_samples), default=0)
        final_rss = self.resource_samples[-1].rss_bytes if self.resource_samples else 0

        memory_growth_pct = (
            ((final_rss - baseline_rss) / baseline_rss * 100) if baseline_rss > 0 else 0
        )

        # FD and thread analysis - check for leaks (monotonic growth)
        baseline_fds = (
            sum(s.num_fds for s in warmup_tail if s.num_fds >= 0) / len(warmup_tail)
            if warmup_tail and all(s.num_fds >= 0 for s in warmup_tail)
            else -1
        )
        final_fds = (
            self.resource_samples[-1].num_fds if self.resource_samples else -1
        )
        baseline_threads = (
            sum(s.num_threads for s in warmup_tail) / len(warmup_tail)
            if len(warmup_tail) >= min_baseline_samples
            else 0
        )
        final_threads = (
            self.resource_samples[-1].num_threads if self.resource_samples else 0
        )

        # Cache metrics from final sample
        final_cache_bytes = (
            self.resource_samples[-1].cache_bytes if self.resource_samples else 0
        )
        final_cache_entries = (
            self.resource_samples[-1].cache_entries if self.resource_samples else 0
        )
        total_evictions = (
            self.resource_samples[-1].cache_evictions if self.resource_samples else 0
        )

        # Latency analysis - use last 2 minutes of warmup for baseline
        warmup_latency = [s for s in self.latency_samples if s.phase == Phase.WARMUP.value]
        steady_latency = [s for s in self.latency_samples if s.phase == Phase.STEADY.value]

        # Compute baseline p95 from last N samples of warmup
        warmup_latency_tail = warmup_latency[-baseline_window_samples:] if warmup_latency else []
        baseline_p95 = (
            sum(s.p95_ms for s in warmup_latency_tail) / len(warmup_latency_tail)
            if len(warmup_latency_tail) >= min_baseline_samples
            else 0
        )

        # Compute final p95 from last N samples of steady state
        steady_latency_tail = steady_latency[-baseline_window_samples:] if steady_latency else []
        final_p95 = (
            sum(s.p95_ms for s in steady_latency_tail) / len(steady_latency_tail)
            if len(steady_latency_tail) >= min_baseline_samples
            else 0
        )
        max_p95 = max((s.p95_ms for s in self.latency_samples), default=0)

        # Check if we have sufficient data for reliable metrics
        insufficient_data = (
            len(warmup_tail) < min_baseline_samples
            or len(warmup_latency_tail) < min_baseline_samples
            or baseline_rss == 0
            or baseline_p95 == 0
        )

        latency_drift_pct = (
            ((final_p95 - baseline_p95) / baseline_p95 * 100) if baseline_p95 > 0 else 0
        )

        # Other metrics
        total_requests = len(self.request_results)
        total_errors = sum(1 for r in self.request_results if r[1] is not None)
        success_rate = (total_requests - total_errors) / total_requests if total_requests > 0 else 0

        # Count errors by phase and type (post-warmup = steady, spike, cooldown)
        post_warmup_5xx = 0
        post_warmup_timeouts = 0
        post_warmup_arrow_errors = 0
        post_warmup_other_errors = 0
        for latency, error, phase in self.request_results:
            if error is None or phase == Phase.WARMUP.value:
                continue
            # Classify error type
            if "HTTP 5" in error:
                post_warmup_5xx += 1
            elif "timeout" in error.lower():
                post_warmup_timeouts += 1
            elif "arrow_decode" in error:
                post_warmup_arrow_errors += 1
            else:
                post_warmup_other_errors += 1

        final_active = final_metrics.get("resource_limits", {}).get("active_scans", 0)

        cache_hits = final_metrics.get("cache_hits", 0)
        cache_misses = final_metrics.get("cache_misses", 0)
        cache_hit_rate = cache_hits / (cache_hits + cache_misses) if (cache_hits + cache_misses) > 0 else 0

        prefetch = final_metrics.get("prefetch", {})
        prefetch_started = prefetch.get("started", 0)
        prefetch_used = prefetch.get("used", 0)
        prefetch_efficiency = prefetch_used / prefetch_started if prefetch_started > 0 else 0

        # Check if spikes recovered properly
        # For each spike, check if p95 returns within 20% of baseline within 2 minutes after spike
        spike_recovery_ok = self._check_spike_recovery(baseline_p95)

        # Success criteria
        memory_ok = memory_growth_pct <= self.config.max_memory_growth_pct
        # Latency drift: only fail if latency INCREASED beyond threshold
        # Improvement (negative drift) is always acceptable
        latency_ok = latency_drift_pct <= self.config.max_latency_drift_pct
        no_leak = final_active == 0
        # Zero 5xx, timeouts, and Arrow decode errors after warmup
        # (other errors like client disconnects are tolerated)
        no_errors_post_warmup = (
            post_warmup_5xx == 0 and post_warmup_timeouts == 0 and post_warmup_arrow_errors == 0
        )

        # FD leak check: if we have baseline, final shouldn't exceed 2x baseline
        # (accounts for normal fluctuation during load)
        no_fd_leak = True
        if baseline_fds > 0 and final_fds > 0:
            no_fd_leak = final_fds <= baseline_fds * 2

        # Thread leak check: final threads shouldn't exceed 2x baseline
        no_thread_leak = True
        if baseline_threads > 0 and final_threads > 0:
            no_thread_leak = final_threads <= baseline_threads * 2

        # If insufficient data, we can't reliably pass - mark as fail
        overall_pass = (
            memory_ok
            and latency_ok
            and no_leak
            and no_fd_leak
            and no_thread_leak
            and no_errors_post_warmup
            and spike_recovery_ok
            and not insufficient_data
        )

        return SoakResults(
            duration_hours=duration_hours,
            total_requests=total_requests,
            success_rate=success_rate,
            baseline_rss_mb=baseline_rss / (1024 * 1024),
            peak_rss_mb=peak_rss / (1024 * 1024),
            final_rss_mb=final_rss / (1024 * 1024),
            memory_growth_pct=memory_growth_pct,
            baseline_p95_ms=baseline_p95,
            final_p95_ms=final_p95,
            max_p95_ms=max_p95,
            latency_drift_pct=latency_drift_pct,
            final_active_scans=final_active,
            cache_hit_rate=cache_hit_rate,
            prefetch_efficiency=prefetch_efficiency,
            spike_count=self.spike_count,
            spike_recovery_ok=spike_recovery_ok,
            baseline_fds=baseline_fds,
            final_fds=final_fds,
            baseline_threads=baseline_threads,
            final_threads=final_threads,
            final_cache_mb=final_cache_bytes / (1024 * 1024),
            final_cache_entries=final_cache_entries,
            total_cache_evictions=total_evictions,
            post_warmup_5xx=post_warmup_5xx,
            post_warmup_timeouts=post_warmup_timeouts,
            post_warmup_arrow_errors=post_warmup_arrow_errors,
            post_warmup_other_errors=post_warmup_other_errors,
            memory_ok=memory_ok,
            latency_ok=latency_ok,
            no_leak=no_leak,
            no_fd_leak=no_fd_leak,
            no_thread_leak=no_thread_leak,
            no_errors_post_warmup=no_errors_post_warmup,
            overall_pass=overall_pass,
            insufficient_data=insufficient_data,
        )


# =============================================================================
# Main
# =============================================================================


def print_results(results: SoakResults):
    """Print formatted results."""
    print("\n" + "=" * 80)
    print("SOAK TEST RESULTS")
    print("=" * 80)

    print(f"\nDuration: {results.duration_hours:.2f} hours")
    print(f"Total requests: {results.total_requests:,}")
    print(f"Success rate: {results.success_rate * 100:.1f}%")

    print("\n" + "-" * 40)
    print("MEMORY")
    print("-" * 40)
    print(f"Baseline RSS: {results.baseline_rss_mb:.1f} MB")
    print(f"Peak RSS: {results.peak_rss_mb:.1f} MB")
    print(f"Final RSS: {results.final_rss_mb:.1f} MB")
    print(f"Growth: {results.memory_growth_pct:+.1f}%")

    print("\n" + "-" * 40)
    print("LATENCY")
    print("-" * 40)
    print(f"Baseline p95: {results.baseline_p95_ms:.1f} ms")
    print(f"Final p95: {results.final_p95_ms:.1f} ms")
    print(f"Max p95: {results.max_p95_ms:.1f} ms")
    print(f"Drift: {results.latency_drift_pct:+.1f}%")

    print("\n" + "-" * 40)
    print("RESOURCES")
    print("-" * 40)
    fd_baseline = f"{results.baseline_fds:.0f}" if results.baseline_fds >= 0 else "n/a"
    fd_final = str(results.final_fds) if results.final_fds >= 0 else "n/a"
    print(f"File descriptors: {fd_baseline} -> {fd_final}")
    print(f"Threads: {results.baseline_threads:.0f} -> {results.final_threads}")
    print(f"Cache size: {results.final_cache_mb:.1f} MB ({results.final_cache_entries} entries)")
    print(f"Cache evictions: {results.total_cache_evictions}")

    print("\n" + "-" * 40)
    print("STABILITY")
    print("-" * 40)
    print(f"Final active scans: {results.final_active_scans}")
    print(f"Cache hit rate: {results.cache_hit_rate * 100:.1f}%")
    print(f"Prefetch efficiency: {results.prefetch_efficiency * 100:.1f}%")
    print(f"Spikes completed: {results.spike_count}")

    print("\n" + "-" * 40)
    print("POST-WARMUP ERRORS")
    print("-" * 40)
    print(f"5xx errors: {results.post_warmup_5xx}")
    print(f"Timeouts: {results.post_warmup_timeouts}")
    print(f"Arrow decode errors: {results.post_warmup_arrow_errors}")
    print(f"Other errors: {results.post_warmup_other_errors}")

    print("\n" + "-" * 40)
    print("SUCCESS CRITERIA")
    print("-" * 40)
    mem_status = "PASS" if results.memory_ok else "FAIL"
    print(f"Memory growth < 10%: {mem_status} ({results.memory_growth_pct:+.1f}%)")
    lat_status = "PASS" if results.latency_ok else "FAIL"
    print(f"Latency drift < 20%: {lat_status} ({results.latency_drift_pct:+.1f}%)")
    leak_status = "PASS" if results.no_leak else "FAIL"
    print(f"No active scan leak: {leak_status} (active={results.final_active_scans})")
    fd_status = "PASS" if results.no_fd_leak else "FAIL"
    print(f"No FD leak: {fd_status} ({fd_baseline} -> {fd_final})")
    thread_status = "PASS" if results.no_thread_leak else "FAIL"
    threads_str = f"{results.baseline_threads:.0f} -> {results.final_threads}"
    print(f"No thread leak: {thread_status} ({threads_str})")
    critical_errors = (
        results.post_warmup_5xx + results.post_warmup_timeouts + results.post_warmup_arrow_errors
    )
    err_status = "PASS" if results.no_errors_post_warmup else "FAIL"
    print(f"Zero critical errors after warmup: {err_status} ({critical_errors})")
    recovery_status = "PASS" if results.spike_recovery_ok else "FAIL"
    print(f"Spike recovery (<2min to baseline): {recovery_status} ({results.spike_count} spikes)")

    if results.insufficient_data:
        print("\nWARNING: Insufficient data for reliable baseline metrics")
        print("         Run longer or increase warmup duration")

    print("\n" + "=" * 80)
    if results.insufficient_data:
        print("OVERALL: FAIL (insufficient data)")
    else:
        print(f"OVERALL: {'PASS' if results.overall_pass else 'FAIL'}")
    print("=" * 80)


async def main():
    parser = argparse.ArgumentParser(
        description="Soak test for Strata production readiness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=1.0,
        help="Test duration in hours (default: 1.0)",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=30,
        help="Base concurrent users (default: 30)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Server port (default: auto-find)",
    )
    parser.add_argument(
        "--no-start-server",
        action="store_true",
        help="Use existing server",
    )
    parser.add_argument(
        "--keep-dirs",
        action="store_true",
        help="Keep temporary directories",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Quick test run (5 minutes)",
    )
    args = parser.parse_args()

    # Create config
    config = SoakConfig(
        duration_hours=args.duration if not args.dry_run else 5 / 60,  # 5 min for dry-run
        base_users=args.users,
        start_server=not args.no_start_server,
        server_port=args.port or find_free_port(),
        keep_dirs=args.keep_dirs,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        config.warmup_minutes = 1.0
        config.cooldown_minutes = 0.5
        config.spike_interval_minutes = 2.0
        config.num_tables = 4
        config.rows_per_table = 10_000

    # Create temp directories
    temp_dir = Path(tempfile.mkdtemp(prefix="strata_soak_"))
    config.warehouse_dir = temp_dir / "warehouse"
    config.cache_dir = temp_dir / "cache"
    config.cache_dir.mkdir(parents=True)

    print("=" * 80)
    print("STRATA SOAK TEST")
    print("=" * 80)
    print(f"\nDuration: {config.duration_hours}h")
    print(f"Users: {config.base_users}")
    print(f"Spikes: every {config.spike_interval_minutes}m")

    server = None
    try:
        # Generate warehouse
        print(f"\n[1/3] Generating warehouse at {config.warehouse_dir}...")
        warehouse = generate_soak_warehouse(config)
        print(f"  Created {len(warehouse['tables'])} tables")

        # Start server
        if config.start_server:
            print(f"\n[2/3] Starting Strata server...")
            server = ServerProcess(
                config.server_host,
                config.server_port,
                config.cache_dir,
                config.cache_size_bytes,
            )
            server.start()
            config.base_url = f"http://{config.server_host}:{config.server_port}"
            print(f"  Server running at {config.base_url}")
            print(f"  Cache limit: {config.cache_size_bytes // (1024 * 1024)} MB")

        # Run test
        print(f"\n[3/3] Running soak test...")
        if config.dry_run:
            print("  (dry run - 5 min)")

        driver = SoakDriver(config, warehouse["tables"], server.pid if server else None)
        await driver.start()

        try:
            results = await driver.run_soak_test()
        finally:
            await driver.stop()

        # Print results
        print_results(results)

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = config.results_dir / f"soak_test_{timestamp}.jsonl"

        with open(results_file, "w") as f:
            # Write samples
            for sample in driver.resource_samples:
                f.write(json.dumps(sample.to_dict()) + "\n")
            for sample in driver.latency_samples:
                f.write(json.dumps(sample.to_dict()) + "\n")
            # Write final results
            f.write(json.dumps({"type": "results", **results.to_dict()}) + "\n")

        print(f"\nResults written to: {results_file}")

    finally:
        if server:
            print("\nStopping server...")
            server.stop()

        if not config.keep_dirs and temp_dir.exists():
            print(f"Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)


if __name__ == "__main__":
    asyncio.run(main())
