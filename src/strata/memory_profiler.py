"""Memory profiling utilities for Strata.

This module provides detailed memory profiling information for diagnosing
memory-related performance issues, including:
- PyArrow memory pool allocations
- Python memory statistics
- Process-level memory usage
- Memory allocation patterns

These are exposed via /v1/debug/memory for operational diagnostics.
"""

import gc
from dataclasses import dataclass
from typing import Any

import pyarrow as pa


@dataclass
class MemorySnapshot:
    """Point-in-time memory snapshot."""

    # Arrow memory pool
    arrow_bytes_allocated: int
    arrow_max_memory: int
    arrow_pool_backend: str

    # Python memory (from sys.getsizeof approximations)
    python_gc_tracked: int  # Objects tracked by GC
    python_gc_objects_by_gen: list[int]  # Count per generation

    # Process memory (if available)
    process_rss_bytes: int | None
    process_vms_bytes: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "arrow": {
                "bytes_allocated": self.arrow_bytes_allocated,
                "max_memory": self.arrow_max_memory,
                "pool_backend": self.arrow_pool_backend,
                "allocated_mb": round(self.arrow_bytes_allocated / (1024 * 1024), 2),
                "max_mb": round(self.arrow_max_memory / (1024 * 1024), 2),
            },
            "python": {
                "gc_tracked_objects": self.python_gc_tracked,
                "gc_objects_by_gen": self.python_gc_objects_by_gen,
            },
            "process": {
                "rss_bytes": self.process_rss_bytes,
                "vms_bytes": self.process_vms_bytes,
                "rss_mb": (
                    round(self.process_rss_bytes / (1024 * 1024), 2)
                    if self.process_rss_bytes
                    else None
                ),
                "vms_mb": (
                    round(self.process_vms_bytes / (1024 * 1024), 2)
                    if self.process_vms_bytes
                    else None
                ),
            },
        }


def get_memory_snapshot() -> MemorySnapshot:
    """Capture current memory state across Arrow, Python, and process levels.

    This is a relatively cheap operation suitable for periodic sampling.

    Returns:
        MemorySnapshot with current memory statistics
    """
    # Arrow memory pool stats
    pool = pa.default_memory_pool()
    arrow_bytes = pool.bytes_allocated()
    arrow_max = pool.max_memory()
    arrow_backend = pool.backend_name

    # Python GC stats
    gc_stats = gc.get_stats()
    gc_tracked = sum(s["collections"] for s in gc_stats)
    gc_objects = [len(gc.get_objects(i)) for i in range(3)]

    # Process memory (via psutil if available)
    rss_bytes = None
    vms_bytes = None
    try:
        import psutil

        process = psutil.Process()
        mem_info = process.memory_info()
        rss_bytes = mem_info.rss
        vms_bytes = mem_info.vms
    except ImportError:
        # psutil not available - try /proc/self/statm on Linux
        try:
            with open("/proc/self/statm") as f:
                parts = f.read().split()
                page_size = 4096  # Typical page size
                vms_bytes = int(parts[0]) * page_size
                rss_bytes = int(parts[1]) * page_size
        except (FileNotFoundError, IndexError, ValueError):
            pass

    return MemorySnapshot(
        arrow_bytes_allocated=arrow_bytes,
        arrow_max_memory=arrow_max,
        arrow_pool_backend=arrow_backend,
        python_gc_tracked=gc_tracked,
        python_gc_objects_by_gen=gc_objects,
        process_rss_bytes=rss_bytes,
        process_vms_bytes=vms_bytes,
    )


def get_arrow_allocations() -> dict[str, Any]:
    """Get detailed Arrow memory allocation information.

    Returns information about all available Arrow memory pools
    and their current allocation state.
    """
    result = {
        "default_pool": {
            "backend": pa.default_memory_pool().backend_name,
            "bytes_allocated": pa.default_memory_pool().bytes_allocated(),
            "max_memory": pa.default_memory_pool().max_memory(),
        },
        "available_pools": [],
    }

    # Check which pools are available
    pools_to_check = [
        ("system", pa.system_memory_pool),
    ]

    # Try optional pools
    try:
        pools_to_check.append(("jemalloc", pa.jemalloc_memory_pool))
    except AttributeError:
        pass

    try:
        pools_to_check.append(("mimalloc", pa.mimalloc_memory_pool))
    except AttributeError:
        pass

    for name, pool_fn in pools_to_check:
        try:
            pool = pool_fn()
            result["available_pools"].append(
                {
                    "name": name,
                    "bytes_allocated": pool.bytes_allocated(),
                    "max_memory": pool.max_memory(),
                }
            )
        except Exception:
            pass

    return result


def get_python_memory_stats() -> dict[str, Any]:
    """Get detailed Python memory statistics.

    Includes information about:
    - Object counts by type (top types)
    - GC thresholds and counts
    - Reference cycle information
    """
    # Get GC info
    gc_stats = gc.get_stats()
    thresholds = gc.get_threshold()

    # Count objects by type (can be expensive for large heaps)
    type_counts: dict[str, int] = {}
    try:
        for obj in gc.get_objects():
            type_name = type(obj).__name__
            type_counts[type_name] = type_counts.get(type_name, 0) + 1
    except Exception:
        pass

    # Get top 20 types by count
    top_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:20]

    return {
        "gc_thresholds": {
            "gen0": thresholds[0],
            "gen1": thresholds[1],
            "gen2": thresholds[2],
        },
        "gc_stats": [
            {
                "generation": i,
                "collections": s["collections"],
                "collected": s["collected"],
                "uncollectable": s["uncollectable"],
            }
            for i, s in enumerate(gc_stats)
        ],
        "gc_is_enabled": gc.isenabled(),
        "gc_freeze_count": gc.get_freeze_count() if hasattr(gc, "get_freeze_count") else None,
        "top_object_types": [{"type": t, "count": c} for t, c in top_types],
        "total_objects": sum(type_counts.values()),
    }


def get_detailed_memory_report() -> dict[str, Any]:
    """Get comprehensive memory report for debugging.

    This is more expensive than get_memory_snapshot() and should
    only be called on-demand (not for periodic sampling).

    Returns:
        Dictionary with Arrow, Python, and process memory details
    """
    snapshot = get_memory_snapshot()
    arrow_details = get_arrow_allocations()
    python_details = get_python_memory_stats()

    return {
        "snapshot": snapshot.to_dict(),
        "arrow_details": arrow_details,
        "python_details": python_details,
        "recommendations": _get_memory_recommendations(snapshot, python_details),
    }


def _get_memory_recommendations(snapshot: MemorySnapshot, python_stats: dict) -> list[str]:
    """Generate memory-related recommendations based on current state."""
    recommendations = []

    # Check Arrow memory
    if snapshot.arrow_bytes_allocated > 1024 * 1024 * 1024:  # > 1GB
        recommendations.append(
            f"Arrow has {snapshot.arrow_bytes_allocated / (1024**3):.1f}GB allocated. "
            "Consider checking for retained batches or memory leaks."
        )

    # Check if max memory is much higher than current (fragmentation)
    if snapshot.arrow_max_memory > 2 * snapshot.arrow_bytes_allocated > 0:
        recommendations.append(
            f"Arrow max memory ({snapshot.arrow_max_memory / (1024**3):.1f}GB) is much higher "
            f"than current ({snapshot.arrow_bytes_allocated / (1024**3):.1f}GB). "
            "This may indicate memory fragmentation."
        )

    # Check GC objects
    total_objects = python_stats.get("total_objects", 0)
    if total_objects > 1_000_000:
        recommendations.append(
            f"Python GC is tracking {total_objects:,} objects. "
            "High object count can slow down GC. Consider object pooling."
        )

    # Check process RSS
    if snapshot.process_rss_bytes and snapshot.process_rss_bytes > 4 * 1024**3:
        recommendations.append(
            f"Process RSS is {snapshot.process_rss_bytes / (1024**3):.1f}GB. "
            "Consider increasing memory limits or optimizing memory usage."
        )

    if not recommendations:
        recommendations.append("Memory usage looks healthy.")

    return recommendations
