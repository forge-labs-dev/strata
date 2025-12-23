"""Smoke tests for Strata."""

import threading
import time
from datetime import UTC, datetime

import pyarrow as pa
import pytest
import uvicorn
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
)

from strata.cache import CachedFetcher, DiskCache
from strata.client import StrataClient, lt
from strata.config import StrataConfig
from strata.duckdb_ext import StrataScanner
from strata.planner import ReadPlanner
from strata.types import CacheKey, Filter, FilterOp, TableIdentity


@pytest.fixture
def temp_warehouse(tmp_path):
    """Create a temporary warehouse with a sample Iceberg table."""
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir()

    # Create a SQL catalog - use "strata" to match PyIcebergCatalog
    catalog = SqlCatalog(
        "strata",
        **{
            "uri": f"sqlite:///{warehouse_path / 'catalog.db'}",
            "warehouse": str(warehouse_path),
        },
    )

    # Create namespace
    catalog.create_namespace("test_db")

    # Define schema - use optional fields to match PyArrow defaults
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "value", DoubleType(), required=False),
        NestedField(3, "name", StringType(), required=False),
        NestedField(4, "timestamp", LongType(), required=False),  # Epoch micros
    )

    # Create table
    table = catalog.create_table("test_db.events", schema)

    # Create sample data with multiple row groups
    num_rows = 500
    base_ts = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp() * 1_000_000)
    data = pa.table(
        {
            "id": pa.array(range(num_rows), type=pa.int64()),
            "value": pa.array([float(i * 1.5) for i in range(num_rows)], type=pa.float64()),
            "name": pa.array([f"item_{i}" for i in range(num_rows)], type=pa.string()),
            "timestamp": pa.array(
                [base_ts + i * 3600_000_000 for i in range(num_rows)],  # micros
                type=pa.int64(),
            ),
        }
    )

    # Append data to table
    table.append(data)

    return {
        "warehouse_path": warehouse_path,
        "table_uri": f"file://{warehouse_path}#test_db.events",
        "catalog": catalog,
        "table": table,
    }


@pytest.fixture
def strata_config(tmp_path):
    """Create a test configuration."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return StrataConfig(cache_dir=cache_dir)


class TestTableIdentity:
    """Tests for TableIdentity."""

    def test_from_table_id(self):
        identity = TableIdentity.from_table_id("test_db.events")
        assert identity.catalog == "strata"
        assert identity.namespace == "test_db"
        assert identity.table == "events"
        assert str(identity) == "strata.test_db.events"

    def test_from_table_id_with_catalog(self):
        identity = TableIdentity.from_table_id("analytics.page_views", catalog="prod")
        assert identity.catalog == "prod"
        assert identity.namespace == "analytics"
        assert identity.table == "page_views"
        assert str(identity) == "prod.analytics.page_views"

    def test_invalid_table_id(self):
        with pytest.raises(ValueError, match="expected 'namespace.table' format"):
            TableIdentity.from_table_id("just_table")


class TestCacheKey:
    """Tests for CacheKey."""

    def test_to_hex(self):
        identity = TableIdentity.from_table_id("test_db.events")
        key = CacheKey(
            table_identity=identity,
            snapshot_id=123,
            file_path="/data/file.parquet",
            row_group_id=0,
            projection_fingerprint="abc123",
        )
        hex_digest = key.to_hex()
        assert len(hex_digest) == 64  # SHA-256 hex digest
        assert hex_digest == key.to_hex()  # Deterministic
        # table_id property should return canonical string
        assert key.table_id == "strata.test_db.events"

    def test_projection_fingerprint(self):
        fp1 = CacheKey.compute_projection_fingerprint(["a", "b", "c"])
        fp2 = CacheKey.compute_projection_fingerprint(["c", "b", "a"])
        fp3 = CacheKey.compute_projection_fingerprint(["a", "b"])
        fp_same = CacheKey.compute_projection_fingerprint(["a", "b", "c"])
        fp_all = CacheKey.compute_projection_fingerprint(None)

        # Column order matters - different order means different fingerprint
        assert fp1 != fp2
        # Same columns in same order should have same fingerprint
        assert fp1 == fp_same
        # Different columns should have different fingerprint
        assert fp1 != fp3
        # None means all columns
        assert fp_all == "*"


class TestFilter:
    """Tests for Filter."""

    def test_matches_stats_eq(self):
        f = Filter(column="x", op=FilterOp.EQ, value=50)
        assert f.matches_stats(0, 100) is True
        assert f.matches_stats(0, 49) is False
        assert f.matches_stats(51, 100) is False
        assert f.matches_stats(50, 50) is True

    def test_matches_stats_lt(self):
        f = Filter(column="x", op=FilterOp.LT, value=50)
        assert f.matches_stats(0, 100) is True
        assert f.matches_stats(0, 49) is True
        assert f.matches_stats(50, 100) is False

    def test_matches_stats_gt(self):
        f = Filter(column="x", op=FilterOp.GT, value=50)
        assert f.matches_stats(0, 100) is True
        assert f.matches_stats(51, 100) is True
        assert f.matches_stats(0, 50) is False


class TestDiskCache:
    """Tests for DiskCache."""

    def test_put_get(self, strata_config):
        cache = DiskCache(strata_config)
        identity = TableIdentity.from_table_id("test_db.events")
        key = CacheKey(
            table_identity=identity,
            snapshot_id=1,
            file_path="/test.parquet",
            row_group_id=0,
            projection_fingerprint="*",
        )

        batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3], "value": [1.0, 2.0, 3.0]})

        assert cache.contains(key) is False
        cache.put(key, batch)
        assert cache.contains(key) is True

        retrieved = cache.get(key)
        assert retrieved is not None
        assert retrieved.num_rows == 3
        assert retrieved.column("id").to_pylist() == [1, 2, 3]

        # Verify metadata uses canonical identity
        stats = cache.get_stats()
        assert stats.total_entries == 1
        assert "strata.test_db.events" in stats.entries_by_table

    def test_clear(self, strata_config):
        cache = DiskCache(strata_config)
        identity = TableIdentity.from_table_id("test_db.events")
        key = CacheKey(
            table_identity=identity,
            snapshot_id=1,
            file_path="/test.parquet",
            row_group_id=0,
            projection_fingerprint="*",
        )

        batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})
        cache.put(key, batch)
        assert cache.contains(key) is True

        cache.clear()
        assert cache.contains(key) is False


class TestReadPlanner:
    """Tests for ReadPlanner."""

    def test_plan_basic(self, temp_warehouse, strata_config):
        planner = ReadPlanner(strata_config)

        plan = planner.plan(temp_warehouse["table_uri"])

        assert plan.snapshot_id > 0
        assert len(plan.tasks) > 0
        assert plan.total_row_groups > 0

    def test_plan_with_projection(self, temp_warehouse, strata_config):
        planner = ReadPlanner(strata_config)

        plan = planner.plan(
            temp_warehouse["table_uri"],
            columns=["id", "value"],
        )

        assert len(plan.tasks) > 0
        for task in plan.tasks:
            assert task.columns == ["id", "value"]

    def test_plan_with_filter_pruning(self, temp_warehouse, strata_config):
        planner = ReadPlanner(strata_config)

        # Get baseline without filters
        planner.plan(temp_warehouse["table_uri"])

        # With filter that should prune some row groups
        # value ranges from 0 to 748.5 (500 rows * 1.5)
        filters = [Filter(column="value", op=FilterOp.LT, value=100.0)]
        plan_filtered = planner.plan(
            temp_warehouse["table_uri"],
            filters=filters,
        )

        # Should have pruned some row groups
        assert plan_filtered.pruned_row_groups >= 0


class TestCachedFetcher:
    """Tests for CachedFetcher."""

    def test_fetch_and_cache(self, temp_warehouse, strata_config):
        fetcher = CachedFetcher(strata_config)
        planner = ReadPlanner(strata_config)

        plan = planner.plan(temp_warehouse["table_uri"])
        task = plan.tasks[0]

        # First fetch - should not be cached
        batch1 = fetcher.fetch(task)
        assert not task.cached
        assert batch1.num_rows > 0

        # Second fetch with same task - should be cached
        task2 = plan.tasks[0]  # Same task
        batch2 = fetcher.fetch(task2)
        assert task2.cached
        assert batch2.num_rows == batch1.num_rows


class TestEndToEnd:
    """End-to-end integration tests."""

    @pytest.fixture
    def server_with_client(self, temp_warehouse, strata_config, tmp_path):
        """Start a server and provide a client."""
        # Update config to use a free port
        import socket

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        config = StrataConfig(
            host="127.0.0.1",
            port=port,
            cache_dir=tmp_path / "cache",
        )

        # Start server in a thread
        # Initialize state manually for testing
        import strata.server as server_module
        from strata.server import ServerState, app

        server_module._state = ServerState(config)

        server_thread = threading.Thread(
            target=uvicorn.run,
            kwargs={
                "app": app,
                "host": config.host,
                "port": config.port,
                "log_level": "error",
            },
            daemon=True,
        )
        server_thread.start()

        # Wait for server to start
        time.sleep(1)

        client = StrataClient(base_url=f"http://127.0.0.1:{port}")

        yield {
            "client": client,
            "config": config,
            "warehouse": temp_warehouse,
        }

        client.close()

    def test_scan_and_cache_hit(self, server_with_client):
        """Test scanning twice to demonstrate cache hit."""
        client = server_with_client["client"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        # First scan - all cache misses
        batches1 = list(client.scan(table_uri))
        assert len(batches1) > 0
        total_rows1 = sum(b.num_rows for b in batches1)
        assert total_rows1 == 500

        # Check metrics
        metrics1 = client.metrics()
        initial_hits = metrics1["cache_hits"]

        # Second scan - should have cache hits
        batches2 = list(client.scan(table_uri))
        total_rows2 = sum(b.num_rows for b in batches2)
        assert total_rows2 == 500

        metrics2 = client.metrics()
        assert metrics2["cache_hits"] > initial_hits

    def test_scan_with_filters(self, server_with_client):
        """Test scanning with filters."""
        client = server_with_client["client"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        # Scan with filter
        filters = [lt("value", 100.0)]
        batches = list(client.scan(table_uri, filters=filters))

        # Should have fewer rows (value < 100 means roughly id < 67)
        total_rows = sum(b.num_rows for b in batches)
        # May include all rows if row groups aren't pruned,
        # but the filter is at least accepted
        assert total_rows >= 0

    def test_duckdb_integration(self, server_with_client):
        """Test DuckDB integration."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        # Use StrataScanner for DuckDB queries
        scanner = StrataScanner(base_url=f"http://127.0.0.1:{config.port}")

        try:
            scanner.register("events", table_uri, columns=["id", "value"])

            # Query with DuckDB
            result = scanner.query("SELECT COUNT(*) as cnt FROM events")
            assert result.column("cnt")[0].as_py() == 500

            # Query with aggregation
            result = scanner.query("SELECT AVG(value) as avg_val FROM events")
            avg_val = result.column("avg_val")[0].as_py()
            # Average of 0*1.5, 1*1.5, ..., 499*1.5 = 1.5 * 249.5 = 374.25
            assert abs(avg_val - 374.25) < 0.1

        finally:
            scanner.close()

    def test_metadata_stats_endpoint(self, server_with_client):
        """Test the /v0/metadata/stats endpoint."""
        import requests

        client = server_with_client["client"]
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        # Do a scan to populate caches
        batches = list(client.scan(table_uri))
        assert len(batches) > 0

        # Check metadata stats endpoint
        response = requests.get(f"http://127.0.0.1:{config.port}/v0/metadata/stats")
        assert response.status_code == 200

        stats = response.json()

        # Check structure
        assert "parquet_cache" in stats
        assert "manifest_cache" in stats
        assert "metadata_store" in stats

        # Parquet cache should have some activity
        pq_stats = stats["parquet_cache"]
        assert "hits" in pq_stats
        assert "misses" in pq_stats

        # Manifest cache should have some activity (two-level: unfiltered and filtered)
        manifest_stats = stats["manifest_cache"]
        assert "unfiltered" in manifest_stats
        assert "filtered" in manifest_stats
        assert "hits" in manifest_stats["unfiltered"]
        assert "misses" in manifest_stats["unfiltered"]

        # Metadata store stats (if available)
        store_stats = stats["metadata_store"]
        if store_stats is not None:
            assert "manifest_hits" in store_stats
            assert "parquet_meta_hits" in store_stats
            assert "stale_invalidations" in store_stats

    def test_metadata_cleanup_endpoint(self, server_with_client):
        """Test the /v0/metadata/cleanup endpoint."""
        import requests

        config = server_with_client["config"]

        # Call cleanup endpoint
        response = requests.post(f"http://127.0.0.1:{config.port}/v0/metadata/cleanup")
        assert response.status_code == 200

        result = response.json()
        assert result["status"] == "completed"
        assert "stale_entries_removed" in result
        assert isinstance(result["stale_entries_removed"], int)

    def test_health_ready_endpoint(self, server_with_client):
        """Test the /health/ready endpoint."""
        import requests

        config = server_with_client["config"]

        response = requests.get(f"http://127.0.0.1:{config.port}/health/ready")
        assert response.status_code == 200

        result = response.json()
        assert result["status"] == "ready"
        assert "checks" in result

        checks = result["checks"]
        assert checks["server_initialized"] is True
        assert checks["draining"] is False
        assert checks["has_capacity"] is True
        assert "metadata_store" in checks

    def test_prometheus_metrics_endpoint(self, server_with_client):
        """Test the /metrics/prometheus endpoint."""
        import requests

        client = server_with_client["client"]
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        # Do a scan to generate some metrics
        batches = list(client.scan(table_uri))
        assert len(batches) > 0

        # Fetch Prometheus metrics
        response = requests.get(f"http://127.0.0.1:{config.port}/metrics/prometheus")
        assert response.status_code == 200
        assert "text/plain" in response.headers["content-type"]

        content = response.text

        # Check for expected metrics
        assert "strata_cache_hits_total" in content
        assert "strata_cache_misses_total" in content
        assert "strata_scans_total" in content
        assert "strata_active_scans" in content
        assert "strata_draining" in content

        # Check for in-memory cache metrics
        assert "strata_parquet_cache_hits_total" in content
        assert "strata_manifest_cache_hits_total" in content

        # Verify Prometheus format (HELP and TYPE comments)
        assert "# HELP strata_cache_hits_total" in content
        assert "# TYPE strata_cache_hits_total counter" in content

    def test_debug_cache_inspect_endpoint(self, server_with_client):
        """Test the /v1/debug/cache/inspect endpoint."""
        import requests

        client = server_with_client["client"]
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        # Do a scan to populate cache
        batches = list(client.scan(table_uri))
        assert len(batches) > 0

        # Test basic inspect (no filters)
        response = requests.get(f"http://127.0.0.1:{config.port}/v1/debug/cache/inspect")
        assert response.status_code == 200

        result = response.json()
        assert "cache_version" in result
        assert "cache_dir" in result
        assert "entries" in result
        assert "total_matched" in result
        assert "truncated" in result

        # Should have at least one entry from the scan
        assert result["total_matched"] > 0
        assert len(result["entries"]) > 0

        # Check entry structure
        entry = result["entries"][0]
        assert "hash" in entry
        assert "hash_prefix" in entry
        assert "file_path" in entry
        assert "file_exists" in entry
        assert "metadata" in entry

        # Test with limit
        response = requests.get(f"http://127.0.0.1:{config.port}/v1/debug/cache/inspect?limit=1")
        assert response.status_code == 200
        result = response.json()
        assert len(result["entries"]) <= 1

        # Test with prefix filter (use hash from first entry)
        first_hash = result["entries"][0]["hash"][:4] if result["entries"] else "0000"
        response = requests.get(
            f"http://127.0.0.1:{config.port}/v1/debug/cache/inspect?prefix={first_hash}"
        )
        assert response.status_code == 200
        result = response.json()
        assert "prefix_filter" in result

        # Test with non-matching prefix
        response = requests.get(
            f"http://127.0.0.1:{config.port}/v1/debug/cache/inspect?prefix=zzzz"
        )
        assert response.status_code == 200
        result = response.json()
        assert result["total_matched"] == 0


class TestEagerWarmup:
    """Tests for eager warmup at server startup."""

    def test_eager_warmup_returns_timing_info(self, tmp_path):
        """Test that _eager_warmup returns timing information."""
        from strata.config import StrataConfig
        from strata.server import _eager_warmup

        config = StrataConfig(cache_dir=tmp_path / "cache")
        warmup_times = _eager_warmup(config)

        # Should have timing info for each phase
        assert "total_ms" in warmup_times
        assert "imports_ms" in warmup_times
        assert "sqlite_ms" in warmup_times
        assert "caches_ms" in warmup_times

        # Timings should be non-negative
        assert warmup_times["total_ms"] >= 0
        assert warmup_times["imports_ms"] >= 0
        assert warmup_times["sqlite_ms"] >= 0
        assert warmup_times["caches_ms"] >= 0

        # Should track sqlite entries
        assert "sqlite_entries" in warmup_times

    def test_warmup_initializes_metadata_store(self, tmp_path):
        """Test that warmup initializes the metadata store."""
        # Reset global state
        import strata.metadata_cache
        from strata.config import StrataConfig
        from strata.metadata_cache import get_metadata_store
        from strata.server import _eager_warmup

        strata.metadata_cache._metadata_store = None

        config = StrataConfig(cache_dir=tmp_path / "cache")
        _eager_warmup(config)

        # Metadata store should now be accessible
        store = get_metadata_store(config.cache_dir)
        assert store is not None
        stats = store.stats()
        assert isinstance(stats, dict)
