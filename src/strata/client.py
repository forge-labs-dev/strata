"""Python client for Strata server.

Provides both sync and async clients for interacting with Strata:

    # Sync client (for scripts, notebooks, CLI tools)
    from strata.client import StrataClient

    with StrataClient() as client:
        table = client.scan_to_table("file:///warehouse#db.events")

    # Async client (for FastAPI, asyncio applications)
    from strata.client import AsyncStrataClient

    async with AsyncStrataClient() as client:
        table = await client.scan_to_table("file:///warehouse#db.events")
"""

from collections.abc import AsyncIterator, Iterator
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc

from strata.config import StrataConfig
from strata.types import Filter, FilterOp, serialize_filter


class StrataClient:
    """Client for interacting with a Strata server.

    Example:
        client = StrataClient()
        for batch in client.scan("file:///data/warehouse#db.table"):
            print(batch.num_rows)
    """

    def __init__(
        self,
        config: StrataConfig | None = None,
        base_url: str | None = None,
    ) -> None:
        """Initialize the client.

        Args:
            config: Strata configuration (uses StrataConfig.load() if None)
            base_url: Override the server URL from config
        """
        self.config = config or StrataConfig.load()
        self.base_url = base_url or self.config.server_url
        self._client = httpx.Client(base_url=self.base_url, timeout=300.0)

    def __enter__(self) -> "StrataClient":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    def health(self) -> dict:
        """Check server health."""
        response = self._client.get("/health")
        response.raise_for_status()
        return response.json()

    def metrics(self) -> dict:
        """Get server metrics."""
        response = self._client.get("/metrics")
        response.raise_for_status()
        return response.json()

    def scan(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Scan a table and yield RecordBatches.

        Args:
            table_uri: Table identifier (e.g., "file:///warehouse#db.table")
            snapshot_id: Specific snapshot to read (None for current)
            columns: Columns to project (None for all)
            filters: Filters for row-group pruning

        Yields:
            Arrow RecordBatches from the scan
        """
        # Create the scan
        request_body = {
            "table_uri": table_uri,
            "snapshot_id": snapshot_id,
            "columns": columns,
            "filters": [serialize_filter(f) for f in filters] if filters else None,
        }

        response = self._client.post("/v1/scan", json=request_body)
        response.raise_for_status()
        scan_info = response.json()
        scan_id = scan_info["scan_id"]

        try:
            # Fetch the complete IPC stream
            response = self._client.get(f"/v1/scan/{scan_id}/batches")
            response.raise_for_status()

            # Parse the single IPC stream containing all batches
            if response.content:
                reader = ipc.open_stream(pa.BufferReader(response.content))
                yield from reader
        finally:
            # Clean up the scan
            try:
                self._client.delete(f"/v1/scan/{scan_id}")
            except Exception:
                pass

    def scan_to_table(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> pa.Table:
        """Scan a table and return as a single Arrow Table.

        Args:
            table_uri: Table identifier
            snapshot_id: Specific snapshot to read (None for current)
            columns: Columns to project (None for all)
            filters: Filters for row-group pruning

        Returns:
            Arrow Table containing all data
        """
        batches = list(self.scan(table_uri, snapshot_id, columns, filters))
        if not batches:
            return pa.table({})
        return pa.Table.from_batches(batches)

    def clear_cache(self) -> dict:
        """Clear the server's disk cache."""
        response = self._client.post("/v1/cache/clear")
        response.raise_for_status()
        return response.json()


class AsyncStrataClient:
    """Async client for interacting with a Strata server.

    Use this client in async contexts like FastAPI, asyncio applications,
    or when you need to run multiple scans concurrently.

    Example:
        async with AsyncStrataClient() as client:
            table = await client.scan_to_table("file:///warehouse#db.table")

        # Or for concurrent scans:
        async with AsyncStrataClient() as client:
            tables = await asyncio.gather(
                client.scan_to_table("file:///warehouse#db.events"),
                client.scan_to_table("file:///warehouse#db.users"),
            )
    """

    def __init__(
        self,
        config: StrataConfig | None = None,
        base_url: str | None = None,
    ) -> None:
        """Initialize the async client.

        Args:
            config: Strata configuration (uses StrataConfig.load() if None)
            base_url: Override the server URL from config
        """
        self.config = config or StrataConfig.load()
        self.base_url = base_url or self.config.server_url
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=300.0)

    async def __aenter__(self) -> "AsyncStrataClient":
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    async def health(self) -> dict:
        """Check server health."""
        response = await self._client.get("/health")
        response.raise_for_status()
        return response.json()

    async def metrics(self) -> dict:
        """Get server metrics."""
        response = await self._client.get("/metrics")
        response.raise_for_status()
        return response.json()

    async def scan(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Scan a table and yield RecordBatches asynchronously.

        Uses HTTP streaming to reduce memory pressure - bytes are accumulated
        as they arrive rather than waiting for the complete response.

        Args:
            table_uri: Table identifier (e.g., "file:///warehouse#db.table")
            snapshot_id: Specific snapshot to read (None for current)
            columns: Columns to project (None for all)
            filters: Filters for row-group pruning

        Yields:
            Arrow RecordBatches from the scan
        """
        # Create the scan
        request_body = {
            "table_uri": table_uri,
            "snapshot_id": snapshot_id,
            "columns": columns,
            "filters": [serialize_filter(f) for f in filters] if filters else None,
        }

        response = await self._client.post("/v1/scan", json=request_body)
        response.raise_for_status()
        scan_info = response.json()
        scan_id = scan_info["scan_id"]

        try:
            # Stream the response to reduce memory pressure
            # Accumulate chunks as they arrive rather than waiting for complete response
            async with self._client.stream("GET", f"/v1/scan/{scan_id}/batches") as response:
                response.raise_for_status()

                # Accumulate streamed chunks
                chunks = []
                async for chunk in response.aiter_bytes():
                    chunks.append(chunk)

                # Parse the IPC stream once complete
                # Note: Arrow IPC requires the full stream to parse messages,
                # but streaming reduces memory by not buffering in httpx
                if chunks:
                    content = b"".join(chunks)
                    reader = ipc.open_stream(pa.BufferReader(content))
                    for batch in reader:
                        yield batch
        finally:
            # Clean up the scan
            try:
                await self._client.delete(f"/v1/scan/{scan_id}")
            except Exception:
                pass

    async def scan_to_table(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> pa.Table:
        """Scan a table and return as a single Arrow Table.

        Args:
            table_uri: Table identifier
            snapshot_id: Specific snapshot to read (None for current)
            columns: Columns to project (None for all)
            filters: Filters for row-group pruning

        Returns:
            Arrow Table containing all data
        """
        batches = [batch async for batch in self.scan(table_uri, snapshot_id, columns, filters)]
        if not batches:
            return pa.table({})
        return pa.Table.from_batches(batches)

    async def scan_to_batches(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> list[pa.RecordBatch]:
        """Scan a table and return all batches as a list.

        Convenience method when you need all batches but not as a Table.

        Args:
            table_uri: Table identifier
            snapshot_id: Specific snapshot to read (None for current)
            columns: Columns to project (None for all)
            filters: Filters for row-group pruning

        Returns:
            List of Arrow RecordBatches
        """
        return [batch async for batch in self.scan(table_uri, snapshot_id, columns, filters)]

    async def clear_cache(self) -> dict:
        """Clear the server's disk cache."""
        response = await self._client.post("/v1/cache/clear")
        response.raise_for_status()
        return response.json()


def eq(column: str, value: Any) -> Filter:
    """Create an equality filter."""
    return Filter(column=column, op=FilterOp.EQ, value=value)


def ne(column: str, value: Any) -> Filter:
    """Create a not-equal filter."""
    return Filter(column=column, op=FilterOp.NE, value=value)


def lt(column: str, value: Any) -> Filter:
    """Create a less-than filter."""
    return Filter(column=column, op=FilterOp.LT, value=value)


def le(column: str, value: Any) -> Filter:
    """Create a less-than-or-equal filter."""
    return Filter(column=column, op=FilterOp.LE, value=value)


def gt(column: str, value: Any) -> Filter:
    """Create a greater-than filter."""
    return Filter(column=column, op=FilterOp.GT, value=value)


def ge(column: str, value: Any) -> Filter:
    """Create a greater-than-or-equal filter."""
    return Filter(column=column, op=FilterOp.GE, value=value)
