"""Slow operation logging and latency tracking for Strata.

This module provides:
1. SlowOpTracker - Logs operations exceeding configurable thresholds
2. LatencyHistogram - Tracks latency distribution per stage
3. ScanTimer - Context manager for timing scan stages with structured output

Example usage:
    tracker = SlowOpTracker()

    with tracker.time_stage("plan", scan_id=scan_id, table=table_id):
        plan = planner.plan(...)

    # Check for slow operation
    tracker.check_and_log()

Thresholds (configurable):
- plan: 100ms
- ttfb (time to first byte): 250ms
- batch_encode: 100ms
- io_read: 500ms
- total_request: 500ms
"""

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field

logger = logging.getLogger("strata.slow_ops")


# Default thresholds in milliseconds
DEFAULT_THRESHOLDS = {
    "plan": 100.0,  # Planning (catalog + metadata fetch)
    "ttfb": 250.0,  # Time to first byte (first batch ready)
    "batch_encode": 100.0,  # Single batch encode time
    "io_read": 500.0,  # Single I/O read time
    "total_request": 500.0,  # End-to-end request time
    "fetch": 200.0,  # Single row group fetch
    "scan_open": 50.0,  # Scan open/setup time
    "scan_close": 50.0,  # Scan close/cleanup time
}

# Histogram bucket boundaries in milliseconds
HISTOGRAM_BUCKETS = [0, 10, 50, 100, 250, 500, 1000, 5000, float("inf")]
BUCKET_LABELS = ["0-10ms", "10-50ms", "50-100ms", "100-250ms", "250-500ms", "500-1s", "1-5s", "5s+"]


@dataclass
class StageTimings:
    """Timing breakdown for a single operation (e.g., one scan)."""

    scan_id: str = ""
    table_id: str = ""
    snapshot_id: int = 0
    request_id: str = ""

    # Timing stages (in milliseconds)
    plan_ms: float = 0.0
    scan_open_ms: float = 0.0
    ttfb_ms: float = 0.0  # Time to first byte
    fetch_total_ms: float = 0.0  # Total time spent fetching
    encode_total_ms: float = 0.0  # Total time spent encoding
    scan_close_ms: float = 0.0
    total_ms: float = 0.0

    # Per-batch/fetch breakdown
    fetch_count: int = 0
    fetch_max_ms: float = 0.0
    encode_count: int = 0
    encode_max_ms: float = 0.0

    # Data metrics
    bytes_streamed: int = 0
    rows_streamed: int = 0
    tasks_count: int = 0
    columns_count: int = 0
    filters_count: int = 0

    # Context
    phase: str = ""  # warmup, steady, spike, cooldown
    tier: str = ""  # interactive, bulk

    def to_dict(self) -> dict:
        """Convert to dictionary for logging."""
        return {
            "scan_id": self.scan_id,
            "table_id": self.table_id,
            "snapshot_id": self.snapshot_id,
            "request_id": self.request_id,
            "plan_ms": round(self.plan_ms, 2),
            "scan_open_ms": round(self.scan_open_ms, 2),
            "ttfb_ms": round(self.ttfb_ms, 2),
            "fetch_total_ms": round(self.fetch_total_ms, 2),
            "fetch_count": self.fetch_count,
            "fetch_max_ms": round(self.fetch_max_ms, 2),
            "encode_total_ms": round(self.encode_total_ms, 2),
            "encode_count": self.encode_count,
            "encode_max_ms": round(self.encode_max_ms, 2),
            "scan_close_ms": round(self.scan_close_ms, 2),
            "total_ms": round(self.total_ms, 2),
            "bytes_streamed": self.bytes_streamed,
            "rows_streamed": self.rows_streamed,
            "tasks_count": self.tasks_count,
            "columns_count": self.columns_count,
            "filters_count": self.filters_count,
            "phase": self.phase,
            "tier": self.tier,
        }


class LatencyHistogram:
    """Thread-safe histogram for latency distribution tracking.

    Tracks counts per bucket for a given stage (e.g., "plan", "fetch").
    Buckets: 0-10ms, 10-50ms, 50-100ms, 100-250ms, 250-500ms, 500-1s, 1-5s, 5s+
    """

    def __init__(self):
        self._lock = threading.Lock()
        # stage -> bucket_idx -> count
        self._counts: dict[str, list[int]] = defaultdict(lambda: [0] * len(BUCKET_LABELS))
        # stage -> (count, sum_ms, max_ms)
        self._stats: dict[str, tuple[int, float, float]] = defaultdict(lambda: (0, 0.0, 0.0))

    def record(self, stage: str, duration_ms: float) -> None:
        """Record a latency observation."""
        bucket_idx = self._get_bucket_idx(duration_ms)

        with self._lock:
            self._counts[stage][bucket_idx] += 1
            count, sum_ms, max_ms = self._stats[stage]
            self._stats[stage] = (count + 1, sum_ms + duration_ms, max(max_ms, duration_ms))

    def _get_bucket_idx(self, duration_ms: float) -> int:
        """Get bucket index for a duration."""
        for i, upper in enumerate(HISTOGRAM_BUCKETS[1:]):
            if duration_ms < upper:
                return i
        return len(BUCKET_LABELS) - 1

    def get_histogram(self, stage: str) -> dict:
        """Get histogram for a stage."""
        with self._lock:
            counts = self._counts.get(stage, [0] * len(BUCKET_LABELS))
            count, sum_ms, max_ms = self._stats.get(stage, (0, 0.0, 0.0))

        return {
            "buckets": dict(zip(BUCKET_LABELS, counts)),
            "count": count,
            "sum_ms": round(sum_ms, 2),
            "avg_ms": round(sum_ms / count, 2) if count > 0 else 0.0,
            "max_ms": round(max_ms, 2),
        }

    def get_all_histograms(self) -> dict[str, dict]:
        """Get histograms for all stages."""
        with self._lock:
            stages = list(self._counts.keys())
        return {stage: self.get_histogram(stage) for stage in stages}

    def get_percentiles(self, stage: str) -> dict:
        """Estimate percentiles from histogram buckets.

        This is an approximation since we only have bucket counts,
        not individual values. Uses bucket midpoints.
        """
        with self._lock:
            counts = self._counts.get(stage, [0] * len(BUCKET_LABELS))
            total = sum(counts)

        if total == 0:
            return {"p50_ms": 0, "p95_ms": 0, "p99_ms": 0}

        # Bucket midpoints for estimation
        midpoints = [5, 30, 75, 175, 375, 750, 3000, 7500]

        cumsum = 0
        percentiles = {}
        for pct, name in [(0.50, "p50_ms"), (0.95, "p95_ms"), (0.99, "p99_ms")]:
            target = int(total * pct)
            for i, count in enumerate(counts):
                cumsum += count
                if cumsum >= target:
                    percentiles[name] = midpoints[i]
                    break
            else:
                percentiles[name] = midpoints[-1]
            cumsum = 0  # Reset for next percentile

        return percentiles

    def reset(self) -> None:
        """Reset all counters."""
        with self._lock:
            self._counts.clear()
            self._stats.clear()


@dataclass
class SlowOpTracker:
    """Tracks operation timings and logs slow operations.

    Usage:
        tracker = SlowOpTracker()

        # Time individual stages
        with tracker.time_stage("plan"):
            ...

        with tracker.time_stage("fetch"):
            ...

        # Check and log if any stage was slow
        tracker.check_and_log(scan_id="...", table_id="...")
    """

    thresholds: dict[str, float] = field(default_factory=lambda: DEFAULT_THRESHOLDS.copy())
    histogram: LatencyHistogram = field(default_factory=LatencyHistogram)

    # Current operation timings
    _timings: StageTimings = field(default_factory=StageTimings)
    _stage_times: dict[str, float] = field(default_factory=dict)
    _start_time: float = 0.0

    def start(self, **context) -> None:
        """Start timing a new operation."""
        self._timings = StageTimings(**context)
        self._stage_times = {}
        self._start_time = time.perf_counter()

    def time_stage(self, stage: str):
        """Context manager for timing a stage."""
        return _StageTimer(self, stage)

    def record_stage(self, stage: str, duration_ms: float) -> None:
        """Record a stage timing."""
        self._stage_times[stage] = duration_ms
        self.histogram.record(stage, duration_ms)

        # Update timings object
        if stage == "plan":
            self._timings.plan_ms = duration_ms
        elif stage == "scan_open":
            self._timings.scan_open_ms = duration_ms
        elif stage == "ttfb":
            self._timings.ttfb_ms = duration_ms
        elif stage == "scan_close":
            self._timings.scan_close_ms = duration_ms

    def record_fetch(self, duration_ms: float) -> None:
        """Record a single fetch timing."""
        self._timings.fetch_count += 1
        self._timings.fetch_total_ms += duration_ms
        self._timings.fetch_max_ms = max(self._timings.fetch_max_ms, duration_ms)
        self.histogram.record("fetch", duration_ms)

    def record_encode(self, duration_ms: float) -> None:
        """Record a single encode timing."""
        self._timings.encode_count += 1
        self._timings.encode_total_ms += duration_ms
        self._timings.encode_max_ms = max(self._timings.encode_max_ms, duration_ms)
        self.histogram.record("batch_encode", duration_ms)

    def finish(self, **metrics) -> StageTimings:
        """Finish timing and return the timings object.

        Args:
            **metrics: Additional metrics to include (bytes_streamed, rows_streamed, etc.)
        """
        self._timings.total_ms = (time.perf_counter() - self._start_time) * 1000
        self.histogram.record("total_request", self._timings.total_ms)

        # Apply additional metrics
        for key, value in metrics.items():
            if hasattr(self._timings, key):
                setattr(self._timings, key, value)

        return self._timings

    def check_slow_stages(self) -> list[tuple[str, float, float]]:
        """Check which stages exceeded their thresholds.

        Returns:
            List of (stage, actual_ms, threshold_ms) for slow stages.
        """
        slow = []

        checks = [
            ("plan", self._timings.plan_ms),
            ("ttfb", self._timings.ttfb_ms),
            ("total_request", self._timings.total_ms),
            ("scan_open", self._timings.scan_open_ms),
            ("scan_close", self._timings.scan_close_ms),
        ]

        for stage, actual in checks:
            threshold = self.thresholds.get(stage, float("inf"))
            if actual > threshold:
                slow.append((stage, actual, threshold))

        # Check max fetch
        fetch_threshold = self.thresholds.get("fetch", float("inf"))
        if self._timings.fetch_max_ms > fetch_threshold:
            slow.append(("fetch", self._timings.fetch_max_ms, fetch_threshold))

        # Check max encode
        encode_threshold = self.thresholds.get("batch_encode", float("inf"))
        if self._timings.encode_max_ms > encode_threshold:
            slow.append(("batch_encode", self._timings.encode_max_ms, encode_threshold))

        return slow

    def check_and_log(self) -> bool:
        """Check for slow stages and log if any found.

        Returns:
            True if any slow stages were logged.
        """
        slow_stages = self.check_slow_stages()

        if slow_stages:
            # Build slow stages summary
            slow_summary = {
                stage: f"{actual:.1f}ms (>{threshold:.0f}ms)"
                for stage, actual, threshold in slow_stages
            }

            logger.warning(
                "Slow operation detected",
                extra={
                    "slow_stages": slow_summary,
                    **self._timings.to_dict(),
                },
            )
            return True

        return False

    def get_timings(self) -> StageTimings:
        """Get current timings object."""
        return self._timings


class _StageTimer:
    """Context manager for timing a single stage."""

    def __init__(self, tracker: SlowOpTracker, stage: str):
        self.tracker = tracker
        self.stage = stage
        self.start_time = 0.0

    def __enter__(self) -> "_StageTimer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args) -> None:
        duration_ms = (time.perf_counter() - self.start_time) * 1000
        self.tracker.record_stage(self.stage, duration_ms)


# Global histogram for server-wide latency tracking
_global_histogram: LatencyHistogram | None = None
_histogram_lock = threading.Lock()


def get_global_histogram() -> LatencyHistogram:
    """Get or create the global latency histogram."""
    global _global_histogram
    with _histogram_lock:
        if _global_histogram is None:
            _global_histogram = LatencyHistogram()
        return _global_histogram


def record_latency(stage: str, duration_ms: float) -> None:
    """Record a latency observation to the global histogram."""
    get_global_histogram().record(stage, duration_ms)


def get_latency_stats() -> dict:
    """Get latency statistics from the global histogram."""
    histogram = get_global_histogram()
    return histogram.get_all_histograms()


def get_latency_percentiles(stage: str) -> dict:
    """Get percentiles for a specific stage."""
    histogram = get_global_histogram()
    return histogram.get_percentiles(stage)


def reset_latency_stats() -> None:
    """Reset the global latency histogram."""
    histogram = get_global_histogram()
    histogram.reset()
