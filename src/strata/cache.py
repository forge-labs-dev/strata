"""Disk cache for Arrow IPC row group data."""

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import pyarrow as pa
import pyarrow.ipc as ipc

from strata.cache_metrics import get_eviction_tracker
from strata.cache_stats import get_cache_histogram
from strata.config import StrataConfig
from strata.fetcher import Fetcher, create_fetcher
from strata.metrics import MetricsCollector
from strata.tracing import trace_span
from strata.types import CacheKey, ReadPlan, Task

# Cache file extension (Arrow IPC Stream format for zero-copy serving)
CACHE_FILE_EXTENSION = ".arrowstream"
# Metadata sidecar file extension
CACHE_META_EXTENSION = ".meta.json"

# Cache version - bump this when cache format changes to invalidate old caches.
# This is baked into the cache directory structure so old and new caches coexist.
# Version history:
#   1: Initial version (Arrow IPC stream format, SHA-256 keyed)
#   2: Multi-tenancy support (tenant_id in cache key, tenant-prefixed directories)
CACHE_VERSION = 2


@dataclass
class CacheEntryMetadata:
    """Metadata for a cached entry."""

    table_id: str
    snapshot_id: int
    file_path: str
    row_group_id: int
    columns: list[str] | None  # None means all columns
    num_rows: int
    size_bytes: int
    created_at: str  # ISO format timestamp

    def to_dict(self) -> dict:
        return {
            "table_id": self.table_id,
            "snapshot_id": self.snapshot_id,
            "file_path": self.file_path,
            "row_group_id": self.row_group_id,
            "columns": self.columns,
            "num_rows": self.num_rows,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CacheEntryMetadata":
        return cls(
            table_id=d["table_id"],
            snapshot_id=d["snapshot_id"],
            file_path=d["file_path"],
            row_group_id=d["row_group_id"],
            columns=d.get("columns"),
            num_rows=d["num_rows"],
            size_bytes=d["size_bytes"],
            created_at=d["created_at"],
        )


@dataclass
class CacheStats:
    """Aggregate statistics for the cache."""

    total_entries: int
    total_size_bytes: int
    max_size_bytes: int
    usage_percent: float
    oldest_entry: str | None  # ISO timestamp
    newest_entry: str | None  # ISO timestamp
    entries_by_table: dict[str, int]  # table_id -> count
    entries_by_snapshot: dict[str, int]  # "table_id:snapshot_id" -> count

    def to_dict(self) -> dict:
        return {
            "total_entries": self.total_entries,
            "total_size_bytes": self.total_size_bytes,
            "max_size_bytes": self.max_size_bytes,
            "usage_percent": round(self.usage_percent, 2),
            "oldest_entry": self.oldest_entry,
            "newest_entry": self.newest_entry,
            "entries_by_table": self.entries_by_table,
            "entries_by_snapshot": self.entries_by_snapshot,
        }


class Cache(Protocol):
    """Protocol for cache implementations."""

    def get(self, key: CacheKey) -> pa.RecordBatch | None:
        """Get a cached record batch, or None if not cached."""
        ...

    def put(self, key: CacheKey, batch: pa.RecordBatch) -> None:
        """Store a record batch in the cache."""
        ...

    def contains(self, key: CacheKey) -> bool:
        """Check if a key is in the cache."""
        ...

    def clear(self) -> None:
        """Clear all cached data."""
        ...


class DiskCache:
    """Disk-based cache using Arrow IPC Stream format.

    Each cached row group is stored as a separate .arrowstream file,
    named by the SHA-256 hash of the cache key.

    Key optimization: We store data in Arrow IPC Stream format (same as
    network transfer format). This means cache hits are pure file reads
    with zero parsing - the bytes go directly from disk to network.

    Hot path for cache hit:
        disk -> read_file_bytes -> network (no Arrow parsing!)
    """

    def __init__(
        self,
        config: StrataConfig,
        metrics: MetricsCollector | None = None,
    ) -> None:
        self.cache_dir = config.cache_dir
        self.max_size_bytes = config.max_cache_size_bytes
        self.granularity = config.cache_granularity
        self.metrics = metrics or MetricsCollector()

        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _key_path(self, key: CacheKey) -> Path:
        """Get the file path for a cache key.

        Directory structure: cache_dir/v{VERSION}/{tenant_prefix}/{hash[:2]}/{hash[2:4]}/{hash}.arrowstream

        Tenant isolation:
        - Each tenant's cache entries are stored under a tenant-prefixed directory
        - Tenant prefix is first 8 chars of SHA-256(tenant_id) for even distribution
        - This prevents one tenant from accessing another tenant's cached data

        The version prefix ensures cache format changes don't cause corruption:
        - Old server instances continue reading from v1/
        - New instances with v2 write to v2/, ignoring v1/
        - Clear old versions manually or via cleanup scripts
        """
        import hashlib

        hex_digest = key.to_hex(self.granularity)
        # Tenant prefix for isolation (hash first 8 chars for consistent directory naming)
        tenant_prefix = hashlib.sha256(key.tenant_id.encode()).hexdigest()[:8]
        # Version prefix + tenant prefix + two-level directory structure
        subdir = (
            self.cache_dir
            / f"v{CACHE_VERSION}"
            / tenant_prefix
            / hex_digest[:2]
            / hex_digest[2:4]
        )
        subdir.mkdir(parents=True, exist_ok=True)
        return subdir / f"{hex_digest}{CACHE_FILE_EXTENSION}"

    def _meta_path(self, data_path: Path) -> Path:
        """Get the metadata sidecar path for a data file."""
        return data_path.with_suffix(CACHE_META_EXTENSION)

    def get(self, key: CacheKey) -> pa.RecordBatch | None:
        """Get a cached record batch.

        Note: This parses the cached stream format. For the hot path,
        use get_as_stream_bytes() which avoids parsing entirely.
        """
        path = self._key_path(key)
        if not path.exists():
            return None

        try:
            # Read stream format and parse
            stream_bytes = path.read_bytes()
            reader = ipc.open_stream(pa.BufferReader(stream_bytes))
            batches = list(reader)
            if not batches:
                return None
            return batches[0]
        except Exception:
            # Corrupted cache file, remove it
            path.unlink(missing_ok=True)
            return None

    def get_as_stream_bytes(self, key: CacheKey) -> bytes | None:
        """Get cached data as Arrow IPC stream bytes (zero-copy hot path).

        This is THE hot-path optimization. Since we store data in stream
        format, cache hits require zero Arrow parsing:
            disk -> mmap -> bytes -> network

        Uses memory-mapped I/O via Rust when available for faster reads,
        especially for large files and repeated access (OS page cache reuse).

        The bytes are already in the exact format needed for network transfer.
        """
        path = self._key_path(key)
        if not path.exists():
            return None

        try:
            # Use mmap-based read for better performance on large files
            # and OS page cache reuse on repeated access
            from strata import fast_io

            return fast_io.read_file_mmap(str(path))
        except Exception:
            # Corrupted cache file, remove it
            path.unlink(missing_ok=True)
            return None

    def get_path(self, key: CacheKey) -> Path | None:
        """Get the cache file path for a key, if it exists.

        Useful for zero-copy streaming scenarios where the caller
        handles the file directly.
        """
        path = self._key_path(key)
        if path.exists():
            return path
        return None

    def put(self, key: CacheKey, batch: pa.RecordBatch) -> None:
        """Store a record batch in the cache (crash-safe via atomic rename).

        Stores in Arrow IPC Stream format for zero-copy serving on cache hits.
        Thread-safe: uses unique temp file names to avoid races between writers.
        """
        import uuid

        path = self._key_path(key)
        # Use unique suffix to avoid race between concurrent writers
        unique_suffix = uuid.uuid4().hex[:8]
        tmp_path = path.with_suffix(f".{unique_suffix}.tmp")
        meta_path = self._meta_path(path)
        meta_tmp_path = meta_path.with_suffix(f".{unique_suffix}.tmp")

        try:
            # Serialize to stream format (same as network transfer format)
            sink = pa.BufferOutputStream()
            writer = ipc.new_stream(sink, batch.schema)
            writer.write_batch(batch)
            writer.close()
            stream_bytes = sink.getvalue().to_pybytes()

            # Write to temp file first
            tmp_path.write_bytes(stream_bytes)

            # Write metadata sidecar
            metadata = CacheEntryMetadata(
                table_id=key.table_id,
                snapshot_id=key.snapshot_id,
                file_path=key.file_path,
                row_group_id=key.row_group_id,
                columns=None,  # Could be extracted from projection_fingerprint if needed
                num_rows=batch.num_rows,
                size_bytes=batch.nbytes,
                created_at=datetime.now(UTC).isoformat(),
            )
            meta_tmp_path.write_text(json.dumps(metadata.to_dict()))

            # Atomic rename both files
            # If another thread already wrote, that's fine - we just overwrite with same data
            os.replace(tmp_path, path)
            os.replace(meta_tmp_path, meta_path)

            self.metrics.record_cache_write(batch.nbytes)

            # Evict old entries if over size limit
            self._evict_if_needed()
        except Exception:
            # Failed to write, clean up temp files
            tmp_path.unlink(missing_ok=True)
            meta_tmp_path.unlink(missing_ok=True)
            raise

    def contains(self, key: CacheKey) -> bool:
        """Check if a key is in the cache."""
        return self._key_path(key).exists()

    def clear(self) -> None:
        """Clear all cached data (preserves metadata.sqlite)."""
        import shutil

        for item in self.cache_dir.iterdir():
            # Skip metadata database - it's managed by MetadataStore
            if item.name == "metadata.sqlite":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

    def get_size_bytes(self) -> int:
        """Get the current cache size in bytes (current version only)."""
        total = 0
        versioned_dir = self.cache_dir / f"v{CACHE_VERSION}"
        if versioned_dir.exists():
            for path in versioned_dir.rglob(f"*{CACHE_FILE_EXTENSION}"):
                total += path.stat().st_size
        return total

    def get_stats(self) -> CacheStats:
        """Get aggregate cache statistics (current version only).

        This provides operators with visibility into cache contents.
        """
        total_entries = 0
        total_size = 0
        timestamps: list[str] = []
        by_table: dict[str, int] = {}
        by_snapshot: dict[str, int] = {}

        versioned_dir = self.cache_dir / f"v{CACHE_VERSION}"
        if not versioned_dir.exists():
            return CacheStats(
                total_entries=0,
                total_size_bytes=0,
                max_size_bytes=self.max_size_bytes,
                usage_percent=0.0,
                oldest_entry=None,
                newest_entry=None,
                entries_by_table={},
                entries_by_snapshot={},
            )

        for meta_path in versioned_dir.rglob(f"*{CACHE_META_EXTENSION}"):
            try:
                meta = CacheEntryMetadata.from_dict(json.loads(meta_path.read_text()))
                total_entries += 1
                total_size += meta.size_bytes
                timestamps.append(meta.created_at)

                # Count by table
                by_table[meta.table_id] = by_table.get(meta.table_id, 0) + 1

                # Count by snapshot
                snap_key = f"{meta.table_id}:{meta.snapshot_id}"
                by_snapshot[snap_key] = by_snapshot.get(snap_key, 0) + 1
            except Exception:
                # Skip corrupted metadata files
                continue

        # Sort timestamps to find oldest/newest
        timestamps.sort()
        oldest = timestamps[0] if timestamps else None
        newest = timestamps[-1] if timestamps else None

        usage_pct = (total_size / self.max_size_bytes * 100) if self.max_size_bytes > 0 else 0

        return CacheStats(
            total_entries=total_entries,
            total_size_bytes=total_size,
            max_size_bytes=self.max_size_bytes,
            usage_percent=usage_pct,
            oldest_entry=oldest,
            newest_entry=newest,
            entries_by_table=by_table,
            entries_by_snapshot=by_snapshot,
        )

    def list_entries(self) -> list[CacheEntryMetadata]:
        """List all cache entries with their metadata (current version only)."""
        entries = []
        versioned_dir = self.cache_dir / f"v{CACHE_VERSION}"
        if not versioned_dir.exists():
            return entries
        for meta_path in versioned_dir.rglob(f"*{CACHE_META_EXTENSION}"):
            try:
                meta = CacheEntryMetadata.from_dict(json.loads(meta_path.read_text()))
                entries.append(meta)
            except Exception:
                continue
        return entries

    def _evict_if_needed(self) -> None:
        """Evict oldest entries if cache exceeds max size.

        Uses oldest-first eviction based on file mtime (write time).
        Note: This is NOT LRU since get() doesn't update mtime.
        Only evicts from current version directory.
        """
        current_size = self.get_size_bytes()
        if current_size <= self.max_size_bytes:
            return

        size_before = current_size

        # Get all cache files sorted by modification time (oldest first)
        versioned_dir = self.cache_dir / f"v{CACHE_VERSION}"
        if not versioned_dir.exists():
            return
        files = []
        for path in versioned_dir.rglob(f"*{CACHE_FILE_EXTENSION}"):
            files.append((path, path.stat().st_mtime, path.stat().st_size))
        files.sort(key=lambda x: x[1])

        # Evict until under limit (target 80% to avoid evicting on every put)
        target_size = int(self.max_size_bytes * 0.8)
        evicted_count = 0
        evicted_bytes = 0
        while current_size > target_size and files:
            path, _, size = files.pop(0)
            path.unlink(missing_ok=True)
            # Also remove metadata sidecar
            self._meta_path(path).unlink(missing_ok=True)
            current_size -= size
            evicted_count += 1
            evicted_bytes += size

        # Record eviction metrics
        if evicted_count > 0:
            self.metrics.record_cache_eviction(evicted_count, evicted_bytes)
            # Record detailed eviction event
            tracker = get_eviction_tracker()
            tracker.record_eviction(
                files_evicted=evicted_count,
                bytes_evicted=evicted_bytes,
                cache_size_before=size_before,
                cache_size_after=current_size,
                reason="size_limit",
            )


class CachedFetcher:
    """Fetcher that caches results using a Cache backend.

    This wraps a Fetcher and Cache to provide transparent caching.
    """

    def __init__(
        self,
        config: StrataConfig,
        fetcher: Fetcher | None = None,
        cache: Cache | None = None,
        metrics: MetricsCollector | None = None,
    ) -> None:
        self.config = config
        self.metrics = metrics or MetricsCollector()

        # Create fetcher with S3 filesystem if configured
        if fetcher is None:
            s3_filesystem = None
            if config.s3_region or config.s3_access_key or config.s3_anonymous:
                s3_filesystem = config.get_s3_filesystem()
            self.fetcher = create_fetcher(self.metrics, s3_filesystem=s3_filesystem)
        else:
            self.fetcher = fetcher

        self.cache = cache or DiskCache(config, self.metrics)

    def fetch(self, task: Task) -> pa.RecordBatch:
        """Fetch a row group, using cache if available."""
        histogram = get_cache_histogram()

        # Check cache first
        cached_batch = self.cache.get(task.cache_key)
        if cached_batch is not None:
            task.cached = True
            task.bytes_read = cached_batch.nbytes
            self.metrics.record_fetch(
                bytes_read=cached_batch.nbytes,
                rows_read=cached_batch.num_rows,
                elapsed_ms=0.0,
                from_cache=True,
            )
            # Record hit in histogram
            histogram.record_hit(
                bytes_accessed=cached_batch.nbytes,
                table_id=task.cache_key.table_id,
            )
            return cached_batch

        # Fetch from storage with tracing
        with trace_span(
            "fetch_row_group",
            file_path=task.file_path,
            row_group_id=task.row_group_id,
            cache_hit=False,
        ) as span:
            batch = self.fetcher.fetch(task)
            span.set_attribute("bytes_read", batch.nbytes)
            span.set_attribute("num_rows", batch.num_rows)

        # Record miss in histogram
        histogram.record_miss(
            bytes_accessed=batch.nbytes,
            table_id=task.cache_key.table_id,
        )

        # Store in cache
        self.cache.put(task.cache_key, batch)

        return batch

    def execute_plan(self, plan: ReadPlan) -> list[pa.RecordBatch]:
        """Execute a read plan and return all batches."""
        batches = []
        for task in plan.tasks:
            batch = self.fetch(task)
            batches.append(batch)
        return batches

    def stream_plan(self, plan: ReadPlan):
        """Execute a read plan and yield batches one at a time."""
        for task in plan.tasks:
            yield self.fetch(task)

    def stream_plan_as_ipc(self, plan: ReadPlan):
        """Execute a read plan and yield Arrow IPC bytes for each batch."""
        for task in plan.tasks:
            batch = self.fetch(task)
            # Serialize to IPC stream format
            sink = pa.BufferOutputStream()
            writer = ipc.new_stream(sink, batch.schema)
            writer.write_batch(batch)
            writer.close()
            yield sink.getvalue().to_pybytes()

    def fetch_as_stream_bytes(self, task: Task) -> bytes:
        """Fetch row group as Arrow IPC stream bytes (optimized hot path).

        For cache hits, this uses the Rust-accelerated path (if available)
        to avoid creating Python objects for the actual data:
            disk -> Rust mmap -> Rust serialize -> bytes

        For cache misses, fetches from storage, caches, then returns bytes.

        Returns:
            bytes: Arrow IPC stream format, ready for network transfer
        """
        histogram = get_cache_histogram()

        # Check if DiskCache (not just Cache protocol) for optimized path
        if isinstance(self.cache, DiskCache):
            stream_bytes = self.cache.get_as_stream_bytes(task.cache_key)
            if stream_bytes is not None:
                task.cached = True
                task.bytes_read = len(stream_bytes)
                self.metrics.record_fetch(
                    bytes_read=len(stream_bytes),
                    rows_read=0,  # We don't parse the batch, so row count unknown
                    elapsed_ms=0.0,
                    from_cache=True,
                )
                # Record hit in histogram
                histogram.record_hit(
                    bytes_accessed=len(stream_bytes),
                    table_id=task.cache_key.table_id,
                )
                return stream_bytes

        # Cache miss or non-DiskCache: fetch, cache, serialize
        # Note: self.fetch() may record its own metrics with batch.nbytes,
        # but we override task.bytes_read below to reflect actual IPC stream size.
        batch = self.fetch(task)

        # Serialize to IPC stream format
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        stream_bytes = sink.getvalue().to_pybytes()

        # Set task metrics to reflect actual output (IPC stream bytes)
        # This overrides bytes_read set by fetch() to use stream size for consistency
        task.bytes_read = len(stream_bytes)
        # task.cached already set by fetch() (True if cache hit, False if miss)

        return stream_bytes
