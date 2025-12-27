"""Cache eviction metrics and monitoring.

Tracks detailed cache eviction events including:
- Eviction counts and bytes over time
- Eviction rate (per minute/hour)
- Eviction pressure indicator
- Recent eviction events for debugging
"""

import time
from collections import deque
from dataclasses import dataclass, field
from threading import Lock


@dataclass
class EvictionEvent:
    """Record of a cache eviction event."""

    timestamp: float  # Unix timestamp
    files_evicted: int
    bytes_evicted: int
    cache_size_before: int
    cache_size_after: int
    reason: str = "size_limit"  # "size_limit", "manual", "ttl"


@dataclass
class EvictionStats:
    """Aggregate eviction statistics."""

    total_evictions: int
    total_files_evicted: int
    total_bytes_evicted: int
    evictions_last_minute: int
    evictions_last_hour: int
    bytes_evicted_last_minute: int
    bytes_evicted_last_hour: int
    eviction_rate_per_minute: float
    last_eviction_at: float | None
    pressure_level: str  # "low", "medium", "high", "critical"

    def to_dict(self) -> dict:
        return {
            "total_evictions": self.total_evictions,
            "total_files_evicted": self.total_files_evicted,
            "total_bytes_evicted": self.total_bytes_evicted,
            "evictions_last_minute": self.evictions_last_minute,
            "evictions_last_hour": self.evictions_last_hour,
            "bytes_evicted_last_minute": self.bytes_evicted_last_minute,
            "bytes_evicted_last_hour": self.bytes_evicted_last_hour,
            "eviction_rate_per_minute": round(self.eviction_rate_per_minute, 2),
            "last_eviction_at": self.last_eviction_at,
            "pressure_level": self.pressure_level,
        }


class CacheEvictionTracker:
    """Tracks cache eviction events and computes metrics."""

    def __init__(self, max_events: int = 1000) -> None:
        self._lock = Lock()
        self._events: deque[EvictionEvent] = deque(maxlen=max_events)
        self._total_evictions = 0
        self._total_files_evicted = 0
        self._total_bytes_evicted = 0

    def record_eviction(
        self,
        files_evicted: int,
        bytes_evicted: int,
        cache_size_before: int,
        cache_size_after: int,
        reason: str = "size_limit",
    ) -> None:
        """Record an eviction event."""
        event = EvictionEvent(
            timestamp=time.time(),
            files_evicted=files_evicted,
            bytes_evicted=bytes_evicted,
            cache_size_before=cache_size_before,
            cache_size_after=cache_size_after,
            reason=reason,
        )
        with self._lock:
            self._events.append(event)
            self._total_evictions += 1
            self._total_files_evicted += files_evicted
            self._total_bytes_evicted += bytes_evicted

    def get_stats(self) -> EvictionStats:
        """Get aggregate eviction statistics."""
        now = time.time()
        one_minute_ago = now - 60
        one_hour_ago = now - 3600

        with self._lock:
            events = list(self._events)

        # Count recent evictions
        evictions_minute = 0
        evictions_hour = 0
        bytes_minute = 0
        bytes_hour = 0
        last_eviction = None

        for event in events:
            if event.timestamp >= one_minute_ago:
                evictions_minute += 1
                bytes_minute += event.bytes_evicted
            if event.timestamp >= one_hour_ago:
                evictions_hour += 1
                bytes_hour += event.bytes_evicted
            if last_eviction is None or event.timestamp > last_eviction:
                last_eviction = event.timestamp

        # Calculate rate (evictions per minute over last hour)
        rate = evictions_hour / 60.0 if evictions_hour > 0 else 0.0

        # Determine pressure level based on eviction rate
        if rate >= 10:
            pressure = "critical"  # 10+ evictions per minute
        elif rate >= 5:
            pressure = "high"  # 5-10 evictions per minute
        elif rate >= 1:
            pressure = "medium"  # 1-5 evictions per minute
        else:
            pressure = "low"  # < 1 eviction per minute

        return EvictionStats(
            total_evictions=self._total_evictions,
            total_files_evicted=self._total_files_evicted,
            total_bytes_evicted=self._total_bytes_evicted,
            evictions_last_minute=evictions_minute,
            evictions_last_hour=evictions_hour,
            bytes_evicted_last_minute=bytes_minute,
            bytes_evicted_last_hour=bytes_hour,
            eviction_rate_per_minute=rate,
            last_eviction_at=last_eviction,
            pressure_level=pressure,
        )

    def get_recent_events(self, limit: int = 10) -> list[dict]:
        """Get recent eviction events for debugging."""
        with self._lock:
            events = list(self._events)

        # Return most recent first
        events = events[-limit:][::-1]
        return [
            {
                "timestamp": e.timestamp,
                "files_evicted": e.files_evicted,
                "bytes_evicted": e.bytes_evicted,
                "cache_size_before": e.cache_size_before,
                "cache_size_after": e.cache_size_after,
                "reason": e.reason,
            }
            for e in events
        ]

    def reset(self) -> None:
        """Reset all statistics."""
        with self._lock:
            self._events.clear()
            self._total_evictions = 0
            self._total_files_evicted = 0
            self._total_bytes_evicted = 0


# Global tracker instance
_eviction_tracker: CacheEvictionTracker | None = None


def get_eviction_tracker() -> CacheEvictionTracker:
    """Get the global eviction tracker."""
    global _eviction_tracker
    if _eviction_tracker is None:
        _eviction_tracker = CacheEvictionTracker()
    return _eviction_tracker


def reset_eviction_tracker() -> None:
    """Reset the global eviction tracker (for testing)."""
    global _eviction_tracker
    _eviction_tracker = None
