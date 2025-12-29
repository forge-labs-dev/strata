"""Tests for cache granularity configuration."""

import pyarrow as pa
import pytest

from strata.cache import DiskCache
from strata.config import StrataConfig
from strata.types import CacheGranularity, CacheKey, TableIdentity


@pytest.fixture
def sample_batch():
    """Create a sample record batch."""
    return pa.RecordBatch.from_pydict(
        {
            "id": [1, 2, 3],
            "value": [1.0, 2.0, 3.0],
            "name": ["a", "b", "c"],
        }
    )


@pytest.fixture
def table_identity():
    """Create a sample table identity."""
    return TableIdentity.from_table_id("test_db.events")


class TestCacheGranularityConfig:
    """Tests for cache granularity configuration."""

    def test_default_granularity_is_row_group_projection(self, tmp_path):
        """Test that default granularity is ROW_GROUP_PROJECTION."""
        config = StrataConfig(cache_dir=tmp_path / "cache")
        assert config.cache_granularity == CacheGranularity.ROW_GROUP_PROJECTION

    def test_can_configure_row_group_granularity(self, tmp_path):
        """Test that ROW_GROUP granularity can be configured."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            cache_granularity=CacheGranularity.ROW_GROUP,
        )
        assert config.cache_granularity == CacheGranularity.ROW_GROUP


class TestCacheKeyGranularity:
    """Tests for CacheKey with different granularity options."""

    def test_row_group_projection_includes_projection(self, table_identity):
        """Test that ROW_GROUP_PROJECTION includes projection in key."""
        key1 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="abc123",
        )
        key2 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="def456",
        )

        # With ROW_GROUP_PROJECTION, different projections = different keys
        hex1 = key1.to_hex(CacheGranularity.ROW_GROUP_PROJECTION)
        hex2 = key2.to_hex(CacheGranularity.ROW_GROUP_PROJECTION)
        assert hex1 != hex2

    def test_row_group_ignores_projection(self, table_identity):
        """Test that ROW_GROUP ignores projection in key."""
        key1 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="abc123",
        )
        key2 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="def456",
        )

        # With ROW_GROUP, different projections = same key
        hex1 = key1.to_hex(CacheGranularity.ROW_GROUP)
        hex2 = key2.to_hex(CacheGranularity.ROW_GROUP)
        assert hex1 == hex2

    def test_row_group_still_differentiates_row_groups(self, table_identity):
        """Test that ROW_GROUP still differentiates different row groups."""
        key1 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="abc123",
        )
        key2 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=1,  # Different row group
            projection_fingerprint="abc123",
        )

        # Different row groups = different keys (even with ROW_GROUP granularity)
        hex1 = key1.to_hex(CacheGranularity.ROW_GROUP)
        hex2 = key2.to_hex(CacheGranularity.ROW_GROUP)
        assert hex1 != hex2


class TestDiskCacheGranularity:
    """Tests for DiskCache with different granularity options."""

    def test_row_group_projection_caches_separately(self, tmp_path, sample_batch, table_identity):
        """Test that ROW_GROUP_PROJECTION caches different projections separately."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            cache_granularity=CacheGranularity.ROW_GROUP_PROJECTION,
        )
        cache = DiskCache(config)

        key1 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="proj1",
        )
        key2 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="proj2",
        )

        # Put data for key1
        cache.put(key1, sample_batch)

        # key1 should hit, key2 should miss
        assert cache.get(key1) is not None
        assert cache.get(key2) is None

    def test_row_group_shares_cache_across_projections(
        self, tmp_path, sample_batch, table_identity
    ):
        """Test that ROW_GROUP shares cache across different projections."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            cache_granularity=CacheGranularity.ROW_GROUP,
        )
        cache = DiskCache(config)

        key1 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="proj1",
        )
        key2 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="proj2",  # Different projection
        )

        # Put data for key1
        cache.put(key1, sample_batch)

        # Both keys should hit (same row group, ignoring projection)
        assert cache.get(key1) is not None
        assert cache.get(key2) is not None  # Shares cache with key1!

    def test_row_group_still_separates_different_row_groups(
        self, tmp_path, sample_batch, table_identity
    ):
        """Test that ROW_GROUP still separates different row groups."""
        config = StrataConfig(
            cache_dir=tmp_path / "cache",
            cache_granularity=CacheGranularity.ROW_GROUP,
        )
        cache = DiskCache(config)

        key1 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="proj1",
        )
        key2 = CacheKey(
            tenant_id="_default",
            table_identity=table_identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=1,  # Different row group
            projection_fingerprint="proj1",
        )

        # Put data for key1
        cache.put(key1, sample_batch)

        # key1 should hit, key2 should miss (different row groups)
        assert cache.get(key1) is not None
        assert cache.get(key2) is None
