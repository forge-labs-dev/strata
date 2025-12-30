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

Retry Behavior:
    Both clients implement automatic retry with exponential backoff and jitter
    for 429 (Too Many Requests) responses. This turns server overload from a
    hard failure into self-throttling behavior.

    Default settings:
    - max_retries: 3 (total attempts = 4)
    - base_delay: 1.0 seconds
    - max_delay: 30.0 seconds
    - jitter: 0.0-1.0 seconds random addition

    Backoff formula: min(base_delay * 2^attempt + jitter, max_delay)

    Example with defaults:
    - Attempt 1 fails: wait 1.0-2.0s
    - Attempt 2 fails: wait 2.0-3.0s
    - Attempt 3 fails: wait 4.0-5.0s
    - Attempt 4 fails: raise exception

    To disable retries, set max_retries=0.
"""

import asyncio
import random
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc

from strata.config import StrataConfig
from strata.types import Filter, FilterOp, serialize_filter


@dataclass
class RetryConfig:
    """Configuration for retry behavior on 429 responses.

    Attributes:
        max_retries: Maximum number of retry attempts (0 to disable)
        base_delay: Initial delay in seconds before first retry
        max_delay: Maximum delay between retries (caps exponential growth)
        jitter: Maximum random jitter to add (spreads out retries)
    """

    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    jitter: float = 1.0

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given retry attempt (0-indexed).

        Uses exponential backoff: base_delay * 2^attempt + random_jitter
        Capped at max_delay.
        """
        exponential = self.base_delay * (2**attempt)
        jitter = random.uniform(0, self.jitter)
        return min(exponential + jitter, self.max_delay)


class StrataClient:
    """Client for interacting with a Strata server.

    Implements automatic retry with exponential backoff for 429 responses,
    turning server overload into self-throttling behavior.

    Example:
        client = StrataClient()
        for batch in client.scan("file:///data/warehouse#db.table"):
            print(batch.num_rows)

        # Disable retries
        client = StrataClient(retry_config=RetryConfig(max_retries=0))
    """

    def __init__(
        self,
        config: StrataConfig | None = None,
        base_url: str | None = None,
        retry_config: RetryConfig | None = None,
    ) -> None:
        """Initialize the client.

        Args:
            config: Strata configuration (uses StrataConfig.load() if None)
            base_url: Override the server URL from config
            retry_config: Retry configuration for 429 responses (uses defaults if None)
        """
        self.config = config or StrataConfig.load()
        self.base_url = base_url or self.config.server_url
        self.retry_config = retry_config or RetryConfig()
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

    def _fetch_batches_with_retry(self, scan_id: str) -> bytes:
        """Fetch batches with retry on 429 responses.

        Uses exponential backoff with jitter to spread out retries.
        Respects Retry-After header from server when present.

        Args:
            scan_id: The scan ID to fetch batches for

        Returns:
            Raw IPC stream bytes

        Raises:
            httpx.HTTPStatusError: If all retries exhausted or non-429 error
        """
        last_response = None
        for attempt in range(self.retry_config.max_retries + 1):
            response = self._client.get(f"/v1/scan/{scan_id}/batches")

            if response.status_code != 429:
                response.raise_for_status()
                return response.content

            last_response = response

            # Check if we have retries left
            if attempt >= self.retry_config.max_retries:
                break

            # Calculate delay: prefer server's Retry-After, fall back to exponential backoff
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                    # Add jitter to spread out retries
                    delay += random.uniform(0, self.retry_config.jitter)
                except ValueError:
                    delay = self.retry_config.calculate_delay(attempt)
            else:
                delay = self.retry_config.calculate_delay(attempt)

            time.sleep(delay)

        # All retries exhausted
        if last_response is not None:
            last_response.raise_for_status()
        raise httpx.HTTPStatusError("Max retries exceeded", request=None, response=last_response)

    def scan(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Scan a table and yield RecordBatches.

        Automatically retries on 429 (server overload) with exponential backoff.

        Args:
            table_uri: Table identifier (e.g., "file:///warehouse#db.table")
            snapshot_id: Specific snapshot to read (None for current)
            columns: Columns to project (None for all)
            filters: Filters for row-group pruning

        Yields:
            Arrow RecordBatches from the scan

        Raises:
            httpx.HTTPStatusError: If request fails after all retries
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
            # Fetch the complete IPC stream with retry on 429
            content = self._fetch_batches_with_retry(scan_id)

            # Parse the single IPC stream containing all batches
            if content:
                reader = ipc.open_stream(pa.BufferReader(content))
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

    Implements automatic retry with exponential backoff for 429 responses,
    turning server overload into self-throttling behavior.

    Example:
        async with AsyncStrataClient() as client:
            table = await client.scan_to_table("file:///warehouse#db.table")

        # Or for concurrent scans:
        async with AsyncStrataClient() as client:
            tables = await asyncio.gather(
                client.scan_to_table("file:///warehouse#db.events"),
                client.scan_to_table("file:///warehouse#db.users"),
            )

        # Disable retries
        async with AsyncStrataClient(retry_config=RetryConfig(max_retries=0)) as client:
            ...
    """

    def __init__(
        self,
        config: StrataConfig | None = None,
        base_url: str | None = None,
        retry_config: RetryConfig | None = None,
    ) -> None:
        """Initialize the async client.

        Args:
            config: Strata configuration (uses StrataConfig.load() if None)
            base_url: Override the server URL from config
            retry_config: Retry configuration for 429 responses (uses defaults if None)
        """
        self.config = config or StrataConfig.load()
        self.base_url = base_url or self.config.server_url
        self.retry_config = retry_config or RetryConfig()
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

    async def _fetch_batches_with_retry(self, scan_id: str) -> bytes:
        """Fetch batches with retry on 429 responses.

        Uses exponential backoff with jitter to spread out retries.
        Respects Retry-After header from server when present.

        Args:
            scan_id: The scan ID to fetch batches for

        Returns:
            Raw IPC stream bytes

        Raises:
            httpx.HTTPStatusError: If all retries exhausted or non-429 error
        """
        last_response = None
        for attempt in range(self.retry_config.max_retries + 1):
            # Use streaming to accumulate response
            async with self._client.stream("GET", f"/v1/scan/{scan_id}/batches") as response:
                if response.status_code != 429:
                    response.raise_for_status()
                    chunks = []
                    async for chunk in response.aiter_bytes():
                        chunks.append(chunk)
                    return b"".join(chunks)

                last_response = response

            # Check if we have retries left
            if attempt >= self.retry_config.max_retries:
                break

            # Calculate delay: prefer server's Retry-After, fall back to exponential backoff
            retry_after = last_response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                    # Add jitter to spread out retries
                    delay += random.uniform(0, self.retry_config.jitter)
                except ValueError:
                    delay = self.retry_config.calculate_delay(attempt)
            else:
                delay = self.retry_config.calculate_delay(attempt)

            await asyncio.sleep(delay)

        # All retries exhausted
        if last_response is not None:
            last_response.raise_for_status()
        raise httpx.HTTPStatusError("Max retries exceeded", request=None, response=last_response)

    async def scan(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Scan a table and yield RecordBatches asynchronously.

        Automatically retries on 429 (server overload) with exponential backoff.
        Uses HTTP streaming to reduce memory pressure.

        Args:
            table_uri: Table identifier (e.g., "file:///warehouse#db.table")
            snapshot_id: Specific snapshot to read (None for current)
            columns: Columns to project (None for all)
            filters: Filters for row-group pruning

        Yields:
            Arrow RecordBatches from the scan

        Raises:
            httpx.HTTPStatusError: If request fails after all retries
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
            # Fetch with retry on 429
            content = await self._fetch_batches_with_retry(scan_id)

            # Parse the IPC stream
            if content:
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
