"""Tests for cache statistics histogram."""

import time

import pytest


class TestWindowStats:
    """Tests for WindowStats dataclass."""

    def test_hit_rate_calculation(self):
        """Test hit rate calculation."""
        from strata.cache_stats import WindowStats

        stats = WindowStats(
            window_seconds=60,
            hits=80,
            misses=20,
            bytes_from_cache=1000,
            bytes_from_storage=200,
        )

        assert stats.total == 100
        assert stats.hit_rate == 0.8
        assert stats.miss_rate == 0.2

    def test_zero_division(self):
        """Test hit rate with no accesses."""
        from strata.cache_stats import WindowStats

        stats = WindowStats(
            window_seconds=60,
            hits=0,
            misses=0,
            bytes_from_cache=0,
            bytes_from_storage=0,
        )

        assert stats.total == 0
        assert stats.hit_rate == 0.0
        assert stats.miss_rate == 0.0

    def test_to_dict(self):
        """Test conversion to dict."""
        from strata.cache_stats import WindowStats

        stats = WindowStats(
            window_seconds=60,
            hits=75,
            misses=25,
            bytes_from_cache=1500,
            bytes_from_storage=500,
        )

        d = stats.to_dict()
        assert d["window_seconds"] == 60
        assert d["hits"] == 75
        assert d["misses"] == 25
        assert d["total"] == 100
        assert d["hit_rate"] == 0.75
        assert d["miss_rate"] == 0.25


class TestCacheStatsHistogram:
    """Tests for CacheStatsHistogram."""

    def test_initial_state(self):
        """Test histogram starts empty."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram()
        stats = histogram.get_lifetime_stats()

        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["total"] == 0

    def test_record_hit(self):
        """Test recording a cache hit."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram()
        histogram.record_hit(bytes_accessed=1024, table_id="db.table1")

        stats = histogram.get_lifetime_stats()
        assert stats["hits"] == 1
        assert stats["misses"] == 0
        assert stats["bytes_from_cache"] == 1024

    def test_record_miss(self):
        """Test recording a cache miss."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram()
        histogram.record_miss(bytes_accessed=2048, table_id="db.table1")

        stats = histogram.get_lifetime_stats()
        assert stats["hits"] == 0
        assert stats["misses"] == 1
        assert stats["bytes_from_storage"] == 2048

    def test_hit_rate_calculation(self):
        """Test hit rate over multiple accesses."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram()

        # 3 hits, 1 miss = 75% hit rate
        histogram.record_hit(bytes_accessed=100)
        histogram.record_hit(bytes_accessed=100)
        histogram.record_hit(bytes_accessed=100)
        histogram.record_miss(bytes_accessed=100)

        stats = histogram.get_lifetime_stats()
        assert stats["hits"] == 3
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 0.75

    def test_window_stats(self):
        """Test getting stats for a time window."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram()

        # Record some events
        histogram.record_hit(bytes_accessed=100)
        histogram.record_miss(bytes_accessed=200)

        # Get 60-second window stats
        window_stats = histogram.get_window_stats(60)

        assert window_stats.window_seconds == 60
        assert window_stats.hits == 1
        assert window_stats.misses == 1
        assert window_stats.bytes_from_cache == 100
        assert window_stats.bytes_from_storage == 200

    def test_all_window_stats(self):
        """Test getting stats for all windows."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram(windows=[60, 300, 3600])
        histogram.record_hit(bytes_accessed=100)

        all_stats = histogram.get_all_window_stats()

        assert len(all_stats) == 3
        assert all_stats[0].window_seconds == 60
        assert all_stats[1].window_seconds == 300
        assert all_stats[2].window_seconds == 3600

    def test_table_stats(self):
        """Test per-table statistics."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram()

        # Record accesses for different tables
        histogram.record_hit(bytes_accessed=100, table_id="db.table1")
        histogram.record_hit(bytes_accessed=100, table_id="db.table1")
        histogram.record_miss(bytes_accessed=100, table_id="db.table1")
        histogram.record_hit(bytes_accessed=100, table_id="db.table2")

        table_stats = histogram.get_table_stats()

        # table1 has more accesses, should be first
        assert len(table_stats) >= 2
        assert table_stats[0]["table_id"] == "db.table1"
        assert table_stats[0]["total"] == 3
        assert table_stats[0]["hit_rate"] == pytest.approx(0.6667, abs=0.01)
        assert table_stats[1]["table_id"] == "db.table2"
        assert table_stats[1]["total"] == 1

    def test_summary(self):
        """Test getting full summary."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram()
        histogram.record_hit(bytes_accessed=100, table_id="db.table1")
        histogram.record_miss(bytes_accessed=200)

        summary = histogram.get_summary()

        assert "lifetime" in summary
        assert "windows" in summary
        assert "top_tables" in summary
        assert summary["lifetime"]["hits"] == 1
        assert summary["lifetime"]["misses"] == 1

    def test_reset(self):
        """Test resetting the histogram."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram()
        histogram.record_hit(bytes_accessed=100, table_id="db.table1")
        histogram.record_miss(bytes_accessed=200)

        histogram.reset()
        stats = histogram.get_lifetime_stats()

        assert stats["hits"] == 0
        assert stats["misses"] == 0
        assert stats["total"] == 0

    def test_max_events_limit(self):
        """Test events are limited to max_events."""
        from strata.cache_stats import CacheStatsHistogram

        histogram = CacheStatsHistogram(max_events=5)

        # Record 10 events
        for i in range(10):
            histogram.record_hit(bytes_accessed=100)

        # Only last 5 should be in window stats, but lifetime counters
        # should still reflect all 10
        stats = histogram.get_lifetime_stats()
        assert stats["hits"] == 10  # Lifetime counter unaffected by max_events


class TestGlobalHistogram:
    """Tests for global histogram functions."""

    def test_get_and_reset(self):
        """Test getting and resetting global histogram."""
        from strata.cache_stats import get_cache_histogram, reset_cache_histogram

        reset_cache_histogram()
        hist1 = get_cache_histogram()
        hist2 = get_cache_histogram()

        # Should return same instance
        assert hist1 is hist2

        reset_cache_histogram()
        hist3 = get_cache_histogram()

        # After reset, should be new instance
        assert hist3 is not hist1


class TestCacheHistogramIntegration:
    """Integration tests for cache histogram with server."""

    @pytest.mark.asyncio
    async def test_histogram_endpoint(self, tmp_path):
        """Test /v1/cache/histogram endpoint."""
        from httpx import ASGITransport, AsyncClient

        import strata.server as server_module
        from strata.cache_metrics import reset_eviction_tracker
        from strata.cache_stats import reset_cache_histogram
        from strata.config import StrataConfig
        from strata.pool_metrics import reset_metrics
        from strata.rate_limiter import reset_rate_limiter
        from strata.server import ServerState, app

        reset_metrics()
        reset_rate_limiter()
        reset_eviction_tracker()
        reset_cache_histogram()
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        config = StrataConfig(cache_dir=str(cache_dir))
        server_module._state = ServerState(config)

        try:
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/v1/cache/histogram")
                assert response.status_code == 200
                data = response.json()

                assert "lifetime" in data
                assert "windows" in data
                assert "top_tables" in data

                assert "hits" in data["lifetime"]
                assert "misses" in data["lifetime"]
                assert "hit_rate" in data["lifetime"]

                # Should have 3 default windows
                assert len(data["windows"]) == 3
        finally:
            server_module._state._planning_executor.shutdown(wait=False)
            server_module._state._fetch_executor.shutdown(wait=False)
            server_module._state = None
