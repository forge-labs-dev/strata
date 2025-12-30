"""Tests for AsyncStrataClient."""

import asyncio

import pyarrow as pa
import pytest

from strata.client import AsyncStrataClient, gt, lt


class TestAsyncStrataClient:
    """Tests for AsyncStrataClient class."""

    @pytest.mark.asyncio
    async def test_context_manager(self, server_with_client):
        """AsyncStrataClient works as async context manager."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            table = await client.scan_to_table(table_uri)
            assert isinstance(table, pa.Table)
            assert table.num_rows == 500

    @pytest.mark.asyncio
    async def test_health(self, server_with_client):
        """health() returns server health status."""
        config = server_with_client["config"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            health = await client.health()
            assert "status" in health

    @pytest.mark.asyncio
    async def test_metrics(self, server_with_client):
        """metrics() returns server metrics."""
        config = server_with_client["config"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            metrics = await client.metrics()
            assert isinstance(metrics, dict)

    @pytest.mark.asyncio
    async def test_scan_yields_batches(self, server_with_client):
        """scan() yields RecordBatches asynchronously."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            batches = []
            async for batch in client.scan(table_uri):
                assert isinstance(batch, pa.RecordBatch)
                batches.append(batch)

            assert len(batches) > 0
            total_rows = sum(b.num_rows for b in batches)
            assert total_rows == 500

    @pytest.mark.asyncio
    async def test_scan_to_table(self, server_with_client):
        """scan_to_table() returns Arrow Table."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            table = await client.scan_to_table(table_uri)
            assert isinstance(table, pa.Table)
            assert table.num_rows == 500

    @pytest.mark.asyncio
    async def test_scan_to_batches(self, server_with_client):
        """scan_to_batches() returns list of RecordBatches."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            batches = await client.scan_to_batches(table_uri)
            assert isinstance(batches, list)
            assert len(batches) > 0
            assert all(isinstance(b, pa.RecordBatch) for b in batches)

    @pytest.mark.asyncio
    async def test_column_projection(self, server_with_client):
        """scan respects column projection."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            table = await client.scan_to_table(table_uri, columns=["id", "value"])
            assert table.num_columns == 2
            assert "id" in table.column_names
            assert "value" in table.column_names
            assert "name" not in table.column_names

    @pytest.mark.asyncio
    async def test_with_filters(self, server_with_client):
        """scan accepts filters for row-group pruning."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            # Filters are for row-group pruning
            table = await client.scan_to_table(table_uri, filters=[gt("id", 99), lt("id", 200)])
            assert isinstance(table, pa.Table)


class TestAsyncConcurrency:
    """Tests for concurrent async operations."""

    @pytest.mark.asyncio
    async def test_concurrent_scans(self, server_with_client):
        """Multiple scans can run concurrently."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            # Run multiple scans concurrently
            results = await asyncio.gather(
                client.scan_to_table(table_uri, columns=["id"]),
                client.scan_to_table(table_uri, columns=["value"]),
                client.scan_to_table(table_uri, columns=["name"]),
            )

            assert len(results) == 3
            for table in results:
                assert isinstance(table, pa.Table)
                assert table.num_rows == 500
                assert table.num_columns == 1

    @pytest.mark.asyncio
    async def test_concurrent_scans_different_projections(self, server_with_client):
        """Concurrent scans with different projections work correctly."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            table1, table2 = await asyncio.gather(
                client.scan_to_table(table_uri, columns=["id", "value"]),
                client.scan_to_table(table_uri, columns=["name"]),
            )

            assert table1.num_columns == 2
            assert table2.num_columns == 1
            assert list(table1.column_names) == ["id", "value"]
            assert list(table2.column_names) == ["name"]


class TestAsyncClientManualClose:
    """Tests for manual client lifecycle management."""

    @pytest.mark.asyncio
    async def test_manual_close(self, server_with_client):
        """Client can be manually closed."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        client = AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}")
        try:
            table = await client.scan_to_table(table_uri)
            assert table.num_rows == 500
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_multiple_operations_same_client(self, server_with_client):
        """Single client can perform multiple operations."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            # Health check
            health = await client.health()
            assert "status" in health

            # First scan
            table1 = await client.scan_to_table(table_uri, columns=["id"])
            assert table1.num_rows == 500

            # Second scan
            table2 = await client.scan_to_table(table_uri, columns=["value"])
            assert table2.num_rows == 500

            # Metrics
            metrics = await client.metrics()
            assert isinstance(metrics, dict)
