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
            table = await client.fetch(table_uri)
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
    async def test_fetch_returns_table(self, server_with_client):
        """fetch() returns Arrow Table."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            table = await client.fetch(table_uri)
            assert isinstance(table, pa.Table)
            assert table.num_rows == 500

    @pytest.mark.asyncio
    async def test_fetch_artifact(self, server_with_client):
        """fetch_artifact() returns Artifact."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            artifact = await client.fetch_artifact(table_uri, columns=["id"])
            assert artifact.artifact_id is not None
            table = await artifact.to_table()
            assert table.num_rows == 500
            assert set(table.column_names) == {"id"}

    @pytest.mark.asyncio
    async def test_column_projection(self, server_with_client):
        """fetch respects column projection."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            table = await client.fetch(table_uri, columns=["id", "value"])
            assert table.num_columns == 2
            assert "id" in table.column_names
            assert "value" in table.column_names
            assert "name" not in table.column_names

    @pytest.mark.asyncio
    async def test_with_filters(self, server_with_client):
        """fetch accepts filters for row-group pruning."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            # Filters are for row-group pruning
            table = await client.fetch(table_uri, filters=[gt("id", 99), lt("id", 200)])
            assert isinstance(table, pa.Table)


class TestAsyncConcurrency:
    """Tests for concurrent async operations."""

    @pytest.mark.asyncio
    async def test_concurrent_fetches(self, server_with_client):
        """Multiple fetches can run concurrently."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            # Run multiple fetches concurrently
            results = await asyncio.gather(
                client.fetch(table_uri, columns=["id"]),
                client.fetch(table_uri, columns=["value"]),
                client.fetch(table_uri, columns=["name"]),
            )

            assert len(results) == 3
            for table in results:
                assert isinstance(table, pa.Table)
                assert table.num_rows == 500
                assert table.num_columns == 1

    @pytest.mark.asyncio
    async def test_concurrent_fetches_different_projections(self, server_with_client):
        """Concurrent fetches with different projections work correctly."""
        config = server_with_client["config"]
        table_uri = server_with_client["warehouse"]["table_uri"]

        async with AsyncStrataClient(base_url=f"http://127.0.0.1:{config.port}") as client:
            table1, table2 = await asyncio.gather(
                client.fetch(table_uri, columns=["id", "value"]),
                client.fetch(table_uri, columns=["name"]),
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
            table = await client.fetch(table_uri)
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

            # First fetch
            table1 = await client.fetch(table_uri, columns=["id"])
            assert table1.num_rows == 500

            # Second fetch
            table2 = await client.fetch(table_uri, columns=["value"])
            assert table2.num_rows == 500

            # Metrics
            metrics = await client.metrics()
            assert isinstance(metrics, dict)
