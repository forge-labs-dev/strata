"""Python client for Strata server."""

from collections.abc import Iterator
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
