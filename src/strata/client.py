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

    # -----------------------------------------------------------------------
    # Artifact Methods (Personal Mode Only)
    # -----------------------------------------------------------------------

    def materialize(
        self,
        inputs: list[str],
        executor: str,
        params: dict,
        name: str | None = None,
    ) -> tuple[bool, str, dict | None]:
        """Materialize a computed artifact.

        Checks if the artifact exists (cache hit) or needs to be built locally.
        The server NEVER executes transforms - this is client-side only.

        Args:
            inputs: List of input URIs (table URIs or artifact URIs)
            executor: Executor URI (e.g., "local://duckdb_sql@v1")
            params: Executor-specific parameters
            name: Optional name to assign to the result

        Returns:
            Tuple of (hit, artifact_uri, build_spec):
            - hit: True if artifact exists in cache
            - artifact_uri: URI of the artifact
            - build_spec: If hit=False, the spec for building locally
        """
        request_body = {
            "inputs": inputs,
            "transform": {"executor": executor, "params": params},
            "name": name,
        }
        response = self._client.post("/v1/artifacts/materialize", json=request_body)
        response.raise_for_status()
        data = response.json()
        return (data["hit"], data["artifact_uri"], data.get("build_spec"))

    def run_local(
        self,
        build_spec: dict,
        input_tables: dict[str, pa.Table],
    ) -> pa.Table:
        """Execute a transform locally.

        This is the client-side execution for materialize cache misses.
        Delegates to the executors module which handles DuckDB and other
        local executors.

        Args:
            build_spec: BuildSpec from materialize() response
            input_tables: Mapping of input URI -> Arrow Table

        Returns:
            Result Arrow Table

        Raises:
            ValueError: If executor is not supported
        """
        from strata.executors import run_local

        return run_local(build_spec, input_tables)

    def upload_artifact(
        self,
        artifact_id: str,
        version: int,
        table: pa.Table,
        name: str | None = None,
    ) -> dict:
        """Upload an artifact after local computation.

        Args:
            artifact_id: Artifact ID from build spec
            version: Version from build spec
            table: Arrow Table to upload
            name: Optional name to assign

        Returns:
            Upload finalize response with artifact_uri and byte_size
        """
        # Serialize table to Arrow IPC stream
        sink = pa.BufferOutputStream()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        blob = sink.getvalue().to_pybytes()

        # Upload blob
        upload_response = self._client.post(
            f"/v1/artifacts/upload/{artifact_id}/v/{version}",
            content=blob,
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        upload_response.raise_for_status()

        # Get schema JSON for finalize
        schema_json = table.schema.to_string()

        # Finalize
        finalize_response = self._client.post(
            "/v1/artifacts/finalize",
            json={
                "artifact_id": artifact_id,
                "version": version,
                "arrow_schema": schema_json,
                "row_count": table.num_rows,
                "name": name,
            },
        )
        finalize_response.raise_for_status()
        return finalize_response.json()

    def materialize_local(
        self,
        inputs: list[str],
        sql: str,
        name: str | None = None,
    ) -> pa.Table:
        """Convenience method: materialize with DuckDB SQL, handling cache logic.

        This is the main entry point for local development workflows:
        1. Check if result is cached
        2. If miss, fetch inputs, execute SQL locally, upload result
        3. Return the result table

        Args:
            inputs: List of table URIs to use as inputs
            sql: DuckDB SQL query (use input0, input1, etc. as table names)
            name: Optional name to assign to the result

        Returns:
            Result Arrow Table (from cache or freshly computed)

        Example:
            result = client.materialize_local(
                inputs=["file:///warehouse#db.events"],
                sql="SELECT user_id, count(*) FROM input0 GROUP BY user_id",
                name="user_counts",
            )
        """
        # Check cache
        hit, artifact_uri, build_spec = self.materialize(
            inputs=inputs,
            executor="local://duckdb_sql@v1",
            params={"sql": sql},
            name=name,
        )

        if hit:
            # Fetch cached artifact
            return self.fetch_artifact(artifact_uri)

        # Cache miss - compute locally
        if build_spec is None:
            raise ValueError("Expected build_spec on cache miss")

        # Fetch input tables
        input_tables = {}
        for uri in build_spec.get("input_uris", []):
            input_tables[uri] = self.scan_to_table(uri)

        # Execute locally
        result = self.run_local(build_spec, input_tables)

        # Upload result
        self.upload_artifact(
            artifact_id=build_spec["artifact_id"],
            version=build_spec["version"],
            table=result,
            name=name,
        )

        return result

    def fetch_artifact(self, artifact_uri: str) -> pa.Table:
        """Fetch an artifact by URI.

        Args:
            artifact_uri: Artifact URI (strata://artifact/{id}@v={version})

        Returns:
            Arrow Table containing the artifact data
        """
        import re

        # Parse URI
        match = re.match(r"^strata://artifact/([^@]+)@v=(\d+)$", artifact_uri)
        if not match:
            raise ValueError(f"Invalid artifact URI: {artifact_uri}")

        artifact_id = match.group(1)
        version = int(match.group(2))

        # Fetch data
        response = self._client.get(f"/v1/artifacts/{artifact_id}/v/{version}/data")
        response.raise_for_status()

        # Parse Arrow IPC
        reader = ipc.open_stream(pa.BufferReader(response.content))
        return reader.read_all()

    def resolve_name(self, name: str) -> dict:
        """Resolve a name to its artifact.

        Args:
            name: Name to resolve

        Returns:
            Dict with artifact_uri, version, updated_at
        """
        response = self._client.get(f"/v1/names/{name}")
        response.raise_for_status()
        return response.json()

    def set_name(self, name: str, artifact_id: str, version: int) -> dict:
        """Set or update a name pointer.

        Args:
            name: Name to set
            artifact_id: Target artifact ID
            version: Target version

        Returns:
            Dict with name_uri and artifact_uri
        """
        response = self._client.post(
            "/v1/names",
            json={"name": name, "artifact_id": artifact_id, "version": version},
        )
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------------------------------
    # Artifact Lifecycle Management
    # -------------------------------------------------------------------------

    def list_artifacts(
        self,
        limit: int = 100,
        offset: int = 0,
        state: str | None = None,
        name_prefix: str | None = None,
    ) -> dict:
        """List artifacts with optional filtering.

        Args:
            limit: Maximum number of artifacts to return (default 100)
            offset: Number of artifacts to skip for pagination
            state: Filter by state ("ready", "building", "failed")
            name_prefix: Filter by artifacts with names starting with prefix

        Returns:
            Dict with 'artifacts' list and pagination info
        """
        params = {"limit": limit, "offset": offset}
        if state is not None:
            params["state"] = state
        if name_prefix is not None:
            params["name_prefix"] = name_prefix

        response = self._client.get("/v1/artifacts", params=params)
        response.raise_for_status()
        return response.json()

    def delete_artifact(self, artifact_id: str, version: int) -> dict:
        """Delete an artifact version.

        Deletes the artifact blob and metadata. Also removes any name pointers
        that reference this specific version.

        Args:
            artifact_id: Artifact ID
            version: Version number

        Returns:
            Dict with deletion status
        """
        response = self._client.delete(f"/v1/artifacts/{artifact_id}/v/{version}")
        response.raise_for_status()
        return response.json()

    def garbage_collect(self, max_age_days: float = 7.0) -> dict:
        """Garbage collect unreferenced artifacts.

        Deletes artifacts that:
        1. Have no name pointer referencing them
        2. Are older than max_age_days
        3. Are in "ready" or "failed" state

        Args:
            max_age_days: Maximum age in days for unreferenced artifacts (default 7)

        Returns:
            Dict with GC statistics (deleted_count, deleted_bytes, cutoff_timestamp)
        """
        response = self._client.post(
            "/v1/artifacts/gc",
            params={"max_age_days": max_age_days},
        )
        response.raise_for_status()
        return response.json()

    def get_artifact_usage(self) -> dict:
        """Get artifact store usage metrics.

        Returns:
            Dict with usage statistics including total_bytes, total_versions,
            unreferenced_count, etc.
        """
        response = self._client.get("/v1/artifacts/usage")
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------------------------------
    # Staleness Detection
    # -------------------------------------------------------------------------

    def get_name_status(self, name: str) -> dict:
        """Get status of a named artifact including staleness info.

        Checks whether the named artifact's inputs have changed since
        it was last built. Use this to determine if a rebuild is needed.

        Args:
            name: Name to check (without strata://name/ prefix)

        Returns:
            Dict with fields:
            - name: The artifact name
            - artifact_uri: URI of the pinned artifact
            - version: Pinned version number
            - state: Artifact state
            - input_versions: Dict mapping input URI -> version when built
            - is_stale: True if any input has changed
            - stale_reason: Human-readable explanation if stale
            - changed_inputs: List of inputs that changed

        Example:
            >>> status = client.get_name_status("daily_revenue")
            >>> if status["is_stale"]:
            ...     print(status["stale_reason"])
            Rebuild needed: file:///warehouse#db.events: 123 → 456
        """
        response = self._client.get(f"/v1/artifacts/names/{name}/status")
        response.raise_for_status()
        return response.json()

    def explain_materialize(
        self,
        inputs: list[str],
        executor: str,
        params: dict,
        name: str | None = None,
    ) -> dict:
        """Explain what materialize would do without doing it (dry run).

        Use this to check whether a computation would be a cache hit or miss,
        and if stale, which specific inputs have changed.

        Args:
            inputs: List of input URIs (table URIs or artifact URIs)
            executor: Executor URI (e.g., "local://duckdb_sql@v1")
            params: Executor-specific parameters
            name: Optional name to check staleness against

        Returns:
            Dict with fields:
            - would_hit: True if result would be cached
            - artifact_uri: URI of existing artifact if hit, or previous version if stale
            - would_build: True if client would need to compute locally
            - is_stale: True if named artifact exists but needs rebuild
            - stale_reason: Explanation of why rebuild is needed
            - changed_inputs: List of inputs that changed
            - resolved_input_versions: Current versions of all inputs

        Example:
            >>> result = client.explain_materialize(
            ...     inputs=["file:///warehouse#db.events"],
            ...     executor="local://duckdb_sql@v1",
            ...     params={"sql": "SELECT * FROM input0"},
            ...     name="my_transform",
            ... )
            >>> if result["is_stale"]:
            ...     print(result["stale_reason"])
            Rebuild needed: file:///warehouse#db.events: 123 → 456
        """
        request_body = {
            "inputs": inputs,
            "transform": {"executor": executor, "params": params},
            "name": name,
        }
        response = self._client.post("/v1/artifacts/explain-materialize", json=request_body)
        response.raise_for_status()
        return response.json()

    def is_artifact_stale(self, name: str) -> bool:
        """Check if a named artifact is stale (convenience method).

        Args:
            name: Name to check

        Returns:
            True if the artifact's inputs have changed since it was built

        Raises:
            httpx.HTTPStatusError: If name not found (404)
        """
        status = self.get_name_status(name)
        return status.get("is_stale", False)


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
