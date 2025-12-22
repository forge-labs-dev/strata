"""Structured metrics logging for Strata."""

import json
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import TextIO


@dataclass
class ScanMetrics:
    """Metrics for a single scan operation."""

    scan_id: str
    snapshot_id: int
    table_id: str = ""  # Canonical table identity (catalog.namespace.table)
    planning_time_ms: float = 0.0
    fetch_time_ms: float = 0.0
    total_time_ms: float = 0.0

    # Cache metrics
    cache_hits: int = 0
    cache_misses: int = 0
    bytes_from_cache: int = 0
    bytes_from_storage: int = 0

    # Row group metrics
    total_row_groups: int = 0
    pruned_row_groups: int = 0
    rows_returned: int = 0

    def to_dict(self) -> dict:
        return {
            "scan_id": self.scan_id,
            "table_id": self.table_id,
            "snapshot_id": self.snapshot_id,
            "planning_time_ms": round(self.planning_time_ms, 2),
            "fetch_time_ms": round(self.fetch_time_ms, 2),
            "total_time_ms": round(self.total_time_ms, 2),
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "bytes_from_cache": self.bytes_from_cache,
            "bytes_from_storage": self.bytes_from_storage,
            "cache_hit_rate": (
                round(self.cache_hits / (self.cache_hits + self.cache_misses), 3)
                if (self.cache_hits + self.cache_misses) > 0
                else 0.0
            ),
            "total_row_groups": self.total_row_groups,
            "pruned_row_groups": self.pruned_row_groups,
            "rows_returned": self.rows_returned,
        }


@dataclass
class MetricsCollector:
    """Collects and logs metrics for Strata operations."""

    output: TextIO = field(default_factory=lambda: sys.stdout)
    enabled: bool = True
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    # Aggregate counters
    total_cache_hits: int = 0
    total_cache_misses: int = 0
    total_bytes_from_cache: int = 0
    total_bytes_from_storage: int = 0
    total_bytes_written_to_cache: int = 0
    total_fetches: int = 0
    total_rows_fetched: int = 0
    total_scans: int = 0
    total_row_groups_pruned: int = 0

    # Stream abort counters
    stream_aborts_timeout: int = 0
    stream_aborts_size: int = 0
    client_disconnects: int = 0

    def record_fetch(
        self,
        bytes_read: int,
        rows_read: int,
        elapsed_ms: float,
        from_cache: bool,
    ) -> None:
        """Record a fetch operation."""
        with self._lock:
            self.total_fetches += 1
            self.total_rows_fetched += rows_read

            if from_cache:
                self.total_cache_hits += 1
                self.total_bytes_from_cache += bytes_read
            else:
                self.total_cache_misses += 1
                self.total_bytes_from_storage += bytes_read

    def record_cache_write(self, bytes_written: int) -> None:
        """Record a cache write operation."""
        with self._lock:
            self.total_bytes_written_to_cache += bytes_written

    def record_stream_abort_timeout(self) -> None:
        """Record a stream abort due to timeout."""
        with self._lock:
            self.stream_aborts_timeout += 1

    def record_stream_abort_size(self) -> None:
        """Record a stream abort due to size limit."""
        with self._lock:
            self.stream_aborts_size += 1

    def record_client_disconnect(self) -> None:
        """Record a client disconnect during streaming."""
        with self._lock:
            self.client_disconnects += 1

    def log_scan_complete(self, metrics: ScanMetrics) -> None:
        """Log completion of a scan operation."""
        # Update aggregate counters
        with self._lock:
            self.total_scans += 1
            self.total_row_groups_pruned += metrics.pruned_row_groups

        if not self.enabled:
            return

        log_entry = {
            "event": "scan_complete",
            "timestamp": time.time(),
            **metrics.to_dict(),
        }
        self._write_log(log_entry)

    def log_event(self, event: str, **kwargs) -> None:
        """Log a generic event."""
        if not self.enabled:
            return

        log_entry = {
            "event": event,
            "timestamp": time.time(),
            **kwargs,
        }
        self._write_log(log_entry)

    def _write_log(self, entry: dict) -> None:
        """Write a log entry as JSON."""
        with self._lock:
            json.dump(entry, self.output)
            self.output.write("\n")
            self.output.flush()

    def get_aggregate_stats(self) -> dict:
        """Get aggregate statistics."""
        with self._lock:
            total_requests = self.total_cache_hits + self.total_cache_misses
            return {
                "scan_count": self.total_scans,
                "total_fetches": self.total_fetches,
                "total_rows_fetched": self.total_rows_fetched,
                "cache_hits": self.total_cache_hits,
                "cache_misses": self.total_cache_misses,
                "cache_hit_rate": (
                    round(self.total_cache_hits / total_requests, 3) if total_requests > 0 else 0.0
                ),
                "bytes_from_cache": self.total_bytes_from_cache,
                "bytes_from_storage": self.total_bytes_from_storage,
                "bytes_written_to_cache": self.total_bytes_written_to_cache,
                "row_groups_pruned": self.total_row_groups_pruned,
                # Stream abort metrics
                "stream_aborts_timeout": self.stream_aborts_timeout,
                "stream_aborts_size": self.stream_aborts_size,
                "client_disconnects": self.client_disconnects,
            }

    def reset(self) -> None:
        """Reset all counters."""
        with self._lock:
            self.total_cache_hits = 0
            self.total_cache_misses = 0
            self.total_bytes_from_cache = 0
            self.total_bytes_from_storage = 0
            self.total_bytes_written_to_cache = 0
            self.total_fetches = 0
            self.total_rows_fetched = 0
            self.total_scans = 0
            self.total_row_groups_pruned = 0
            self.stream_aborts_timeout = 0
            self.stream_aborts_size = 0
            self.client_disconnects = 0


class Timer:
    """Context manager for timing operations."""

    def __init__(self) -> None:
        self.start_time: float = 0.0
        self.elapsed_ms: float = 0.0

    def __enter__(self) -> "Timer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args) -> None:
        self.elapsed_ms = (time.perf_counter() - self.start_time) * 1000
