"""Cache statistics with time-windowed histograms.

Tracks cache hit/miss rates over configurable time windows,
providing insight into cache effectiveness over time.
"""

import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import Any


@dataclass
class CacheEvent:
    """A single cache access event."""

    timestamp: float
    is_hit: bool
    bytes_accessed: int
    table_id: str | None = None


@dataclass
class WindowStats:
    """Statistics for a single time window."""

    window_seconds: int
    hits: int
    misses: int
    bytes_from_cache: int
    bytes_from_storage: int

    @property
    def total(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.hits / self.total

    @property
    def miss_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.misses / self.total

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_seconds": self.window_seconds,
            "hits": self.hits,
            "misses": self.misses,
            "total": self.total,
            "hit_rate": round(self.hit_rate, 4),
            "miss_rate": round(self.miss_rate, 4),
            "bytes_from_cache": self.bytes_from_cache,
            "bytes_from_storage": self.bytes_from_storage,
        }


class CacheStatsHistogram:
    """Tracks cache statistics over multiple time windows.

    Maintains rolling statistics for configurable time windows
    (e.g., 1 minute, 5 minutes, 1 hour) to show cache hit rate trends.
    """

    def __init__(
        self,
        windows: list[int] | None = None,
        max_events: int = 10000,
    ) -> None:
        """Initialize the histogram.

        Args:
            windows: List of window sizes in seconds. Default: [60, 300, 3600]
            max_events: Maximum events to retain in memory
        """
        self.windows = windows or [60, 300, 3600]  # 1m, 5m, 1h
        self._lock = Lock()
        self._events: deque[CacheEvent] = deque(maxlen=max_events)

        # Lifetime counters
        self._total_hits = 0
        self._total_misses = 0
        self._total_bytes_cache = 0
        self._total_bytes_storage = 0

        # Per-table stats (table_id -> {hits, misses})
        self._table_stats: dict[str, dict[str, int]] = {}

    def record_hit(
        self,
        bytes_accessed: int,
        table_id: str | None = None,
    ) -> None:
        """Record a cache hit."""
        event = CacheEvent(
            timestamp=time.time(),
            is_hit=True,
            bytes_accessed=bytes_accessed,
            table_id=table_id,
        )
        with self._lock:
            self._events.append(event)
            self._total_hits += 1
            self._total_bytes_cache += bytes_accessed
            if table_id:
                if table_id not in self._table_stats:
                    self._table_stats[table_id] = {"hits": 0, "misses": 0}
                self._table_stats[table_id]["hits"] += 1

    def record_miss(
        self,
        bytes_accessed: int,
        table_id: str | None = None,
    ) -> None:
        """Record a cache miss."""
        event = CacheEvent(
            timestamp=time.time(),
            is_hit=False,
            bytes_accessed=bytes_accessed,
            table_id=table_id,
        )
        with self._lock:
            self._events.append(event)
            self._total_misses += 1
            self._total_bytes_storage += bytes_accessed
            if table_id:
                if table_id not in self._table_stats:
                    self._table_stats[table_id] = {"hits": 0, "misses": 0}
                self._table_stats[table_id]["misses"] += 1

    def get_window_stats(self, window_seconds: int) -> WindowStats:
        """Get statistics for a specific time window."""
        now = time.time()
        cutoff = now - window_seconds

        hits = 0
        misses = 0
        bytes_cache = 0
        bytes_storage = 0

        with self._lock:
            for event in self._events:
                if event.timestamp >= cutoff:
                    if event.is_hit:
                        hits += 1
                        bytes_cache += event.bytes_accessed
                    else:
                        misses += 1
                        bytes_storage += event.bytes_accessed

        return WindowStats(
            window_seconds=window_seconds,
            hits=hits,
            misses=misses,
            bytes_from_cache=bytes_cache,
            bytes_from_storage=bytes_storage,
        )

    def get_all_window_stats(self) -> list[WindowStats]:
        """Get statistics for all configured windows."""
        return [self.get_window_stats(w) for w in self.windows]

    def get_lifetime_stats(self) -> dict[str, Any]:
        """Get lifetime statistics."""
        with self._lock:
            total = self._total_hits + self._total_misses
            hit_rate = self._total_hits / total if total > 0 else 0.0
            return {
                "hits": self._total_hits,
                "misses": self._total_misses,
                "total": total,
                "hit_rate": round(hit_rate, 4),
                "bytes_from_cache": self._total_bytes_cache,
                "bytes_from_storage": self._total_bytes_storage,
            }

    def get_table_stats(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get per-table statistics, sorted by total accesses."""
        with self._lock:
            table_list = []
            for table_id, stats in self._table_stats.items():
                total = stats["hits"] + stats["misses"]
                hit_rate = stats["hits"] / total if total > 0 else 0.0
                table_list.append(
                    {
                        "table_id": table_id,
                        "hits": stats["hits"],
                        "misses": stats["misses"],
                        "total": total,
                        "hit_rate": round(hit_rate, 4),
                    }
                )

        # Sort by total accesses descending
        table_list.sort(key=lambda x: x["total"], reverse=True)
        return table_list[:limit]

    def get_summary(self) -> dict[str, Any]:
        """Get a comprehensive summary of cache statistics."""
        return {
            "lifetime": self.get_lifetime_stats(),
            "windows": [w.to_dict() for w in self.get_all_window_stats()],
            "top_tables": self.get_table_stats(limit=5),
        }

    def reset(self) -> None:
        """Reset all statistics."""
        with self._lock:
            self._events.clear()
            self._total_hits = 0
            self._total_misses = 0
            self._total_bytes_cache = 0
            self._total_bytes_storage = 0
            self._table_stats.clear()


# Global histogram instance
_cache_histogram: CacheStatsHistogram | None = None


def get_cache_histogram() -> CacheStatsHistogram:
    """Get the global cache statistics histogram."""
    global _cache_histogram
    if _cache_histogram is None:
        _cache_histogram = CacheStatsHistogram()
    return _cache_histogram


def reset_cache_histogram() -> None:
    """Reset the global cache histogram (for testing)."""
    global _cache_histogram
    _cache_histogram = None
