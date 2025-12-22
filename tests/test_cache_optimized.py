"""Tests for optimized cache methods and Rust acceleration integration."""

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from strata import fast_io
from strata.cache import CachedFetcher, DiskCache
from strata.config import StrataConfig
from strata.types import CacheKey, TableIdentity


@pytest.fixture
def strata_config(tmp_path):
    """Create a test configuration."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return StrataConfig(cache_dir=cache_dir)


@pytest.fixture
def sample_batch():
    """Create a sample record batch."""
    return pa.RecordBatch.from_pydict(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
            "name": ["a", "b", "c", "d", "e"],
        }
    )


@pytest.fixture
def cache_key():
    """Create a sample cache key."""
    identity = TableIdentity.from_table_id("test_db.events")
    return CacheKey(
        table_identity=identity,
        snapshot_id=123,
        file_path="/data/file.parquet",
        row_group_id=0,
        projection_fingerprint="abc123",
    )


class TestDiskCacheGetAsStreamBytes:
    """Tests for DiskCache.get_as_stream_bytes() method."""

    def test_returns_none_for_missing_key(self, strata_config, cache_key):
        """Test that get_as_stream_bytes returns None for missing keys."""
        cache = DiskCache(strata_config)

        result = cache.get_as_stream_bytes(cache_key)

        assert result is None

    def test_returns_stream_bytes_for_cached_data(self, strata_config, cache_key, sample_batch):
        """Test that get_as_stream_bytes returns valid stream bytes for cached data."""
        cache = DiskCache(strata_config)

        # Store data in cache
        cache.put(cache_key, sample_batch)

        # Get as stream bytes
        stream_bytes = cache.get_as_stream_bytes(cache_key)

        # Should return bytes
        assert isinstance(stream_bytes, bytes)
        assert len(stream_bytes) > 0

        # Should be valid Arrow IPC stream format
        reader = ipc.open_stream(pa.BufferReader(stream_bytes))
        batches = list(reader)
        assert len(batches) == 1
        assert batches[0].num_rows == sample_batch.num_rows

    def test_stream_bytes_data_matches_original(self, strata_config, cache_key, sample_batch):
        """Test that stream bytes contain the same data as the original batch."""
        cache = DiskCache(strata_config)
        cache.put(cache_key, sample_batch)

        stream_bytes = cache.get_as_stream_bytes(cache_key)

        # Parse and verify data
        reader = ipc.open_stream(pa.BufferReader(stream_bytes))
        result_batch = list(reader)[0]

        assert result_batch.column("id").to_pylist() == [1, 2, 3, 4, 5]
        assert result_batch.column("value").to_pylist() == [1.0, 2.0, 3.0, 4.0, 5.0]
        assert result_batch.column("name").to_pylist() == ["a", "b", "c", "d", "e"]


class TestDiskCacheGetPath:
    """Tests for DiskCache.get_path() method."""

    def test_returns_none_for_missing_key(self, strata_config, cache_key):
        """Test that get_path returns None for missing keys."""
        cache = DiskCache(strata_config)

        result = cache.get_path(cache_key)

        assert result is None

    def test_returns_path_for_cached_data(self, strata_config, cache_key, sample_batch):
        """Test that get_path returns a valid path for cached data."""
        cache = DiskCache(strata_config)
        cache.put(cache_key, sample_batch)

        path = cache.get_path(cache_key)

        assert path is not None
        assert path.exists()
        assert path.suffix == ".arrowstream"


class TestCachedFetcherFetchAsStreamBytes:
    """Tests for CachedFetcher.fetch_as_stream_bytes() method."""

    def test_returns_stream_bytes_for_cache_hit(self, strata_config, cache_key, sample_batch):
        """Test that fetch_as_stream_bytes returns stream bytes for cache hits."""
        cache = DiskCache(strata_config)
        cache.put(cache_key, sample_batch)

        fetcher = CachedFetcher(strata_config, cache=cache)

        # Create a mock task
        from strata.types import Task

        task = Task(
            file_path="/data/file.parquet",
            row_group_id=0,
            columns=None,
            cache_key=cache_key,
            num_rows=sample_batch.num_rows,
        )

        # Fetch as stream bytes
        stream_bytes = fetcher.fetch_as_stream_bytes(task)

        # Should return valid stream bytes
        assert isinstance(stream_bytes, bytes)
        assert len(stream_bytes) > 0

        # Task should be marked as cached
        assert task.cached is True
        assert task.bytes_read > 0

        # Should be valid Arrow IPC stream
        reader = ipc.open_stream(pa.BufferReader(stream_bytes))
        batches = list(reader)
        assert len(batches) == 1
        assert batches[0].num_rows == sample_batch.num_rows


class TestRustAccelerationIntegration:
    """Tests for Rust acceleration integration with cache."""

    def test_rust_path_produces_valid_output(self, strata_config, cache_key, sample_batch):
        """Test that Rust acceleration produces valid output when available."""
        cache = DiskCache(strata_config)
        cache.put(cache_key, sample_batch)

        # Get stream bytes (uses Rust if available)
        stream_bytes = cache.get_as_stream_bytes(cache_key)

        # Should work regardless of Rust availability
        assert stream_bytes is not None

        # Verify the data is correct
        reader = ipc.open_stream(pa.BufferReader(stream_bytes))
        result_batch = list(reader)[0]

        assert result_batch.num_rows == sample_batch.num_rows
        assert result_batch.column("id").to_pylist() == sample_batch.column("id").to_pylist()

    def test_rust_availability_is_reported(self):
        """Test that Rust availability is correctly reported."""
        # This should not raise
        is_available = fast_io.is_rust_available()
        assert isinstance(is_available, bool)

        # If CI has Rust, it should be available
        # This is informational - test passes either way
        print(f"Rust acceleration available: {is_available}")
