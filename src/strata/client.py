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
from strata.types import Filter, FilterOp, FilterSpec, serialize_filter


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


@dataclass
class Artifact:
    """An immutable, versioned artifact from Strata.

    Returned by `client.materialize()` or `client.fetch_artifact()`.
    Provides access to artifact data and metadata.

    Attributes:
        artifact_id: Unique artifact identifier
        version: Artifact version number
        cache_hit: True if artifact was returned from cache
        execution: How the artifact was obtained ("cache", "local", "server", "stream")
        build_id: Build ID if artifact was built asynchronously
        name: Name pointer if one was assigned

    Example:
        artifact = client.materialize(
            inputs=["file:///warehouse#db.events"],
            transform={
                "ref": "duckdb_sql@v1",
                "params": {"sql": "SELECT category, COUNT(*) FROM input0 GROUP BY 1"},
            },
            name="category_counts",
        )
        print(f"Artifact: {artifact.uri}")
        print(f"Cache hit: {artifact.cache_hit}")
        df = artifact.to_pandas()

        # Or use fetch for table scans:
        artifact = client.fetch_artifact("file:///warehouse#db.events")
        table = artifact.to_table()
    """

    _client: "StrataClient"
    artifact_id: str
    version: int
    cache_hit: bool = False
    execution: str = "cache"  # "cache" | "local" | "server" | "stream"
    build_id: str | None = None
    name: str | None = None
    _stream_data: bytes | None = None  # Cached stream data from fetch()

    @property
    def uri(self) -> str:
        """Artifact URI (strata://artifact/{id}@v={version})."""
        return f"strata://artifact/{self.artifact_id}@v={self.version}"

    @property
    def name_uri(self) -> str | None:
        """Name URI if a name was assigned (strata://name/{name})."""
        return f"strata://name/{self.name}" if self.name else None

    def info(self) -> dict:
        """Get artifact metadata.

        Returns:
            Dict with artifact_id, version, state, size_bytes, row_count,
            created_at, arrow_schema, etc.
        """
        response = self._client._client.get(f"/v1/artifacts/{self.artifact_id}/v/{self.version}")
        response.raise_for_status()
        return response.json()

    def to_table(self) -> pa.Table:
        """Download artifact data as Arrow Table."""
        # Use cached stream data if available (from fetch with stream mode)
        if self._stream_data is not None:
            if not self._stream_data:
                return pa.table({})
            reader = ipc.open_stream(pa.BufferReader(self._stream_data))
            return reader.read_all()
        return self._client._fetch_artifact_data(self.artifact_id, self.version)

    def to_pandas(self):
        """Download artifact data as pandas DataFrame."""
        return self.to_table().to_pandas()

    def to_polars(self):
        """Download artifact data as Polars DataFrame."""
        import polars as pl

        return pl.from_arrow(self.to_table())

    def lineage(self, direction: str = "upstream", max_depth: int = 10) -> dict:
        """Get artifact lineage (dependency graph).

        Args:
            direction: "upstream" (inputs) or "downstream" (dependents)
            max_depth: Maximum traversal depth

        Returns:
            Dict with 'nodes' and 'edges' describing the lineage graph
        """
        response = self._client._client.get(
            f"/v1/artifacts/{self.artifact_id}/v/{self.version}/lineage",
            params={"direction": direction, "max_depth": max_depth},
        )
        response.raise_for_status()
        return response.json()

    def dependents(self, max_depth: int = 10) -> dict:
        """Get artifacts that depend on this artifact.

        Args:
            max_depth: Maximum traversal depth

        Returns:
            Dict with 'dependents' list
        """
        response = self._client._client.get(
            f"/v1/artifacts/{self.artifact_id}/v/{self.version}/dependents",
            params={"max_depth": max_depth},
        )
        response.raise_for_status()
        return response.json()


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

    # -----------------------------------------------------------------------
    # Unified Materialize API
    # -----------------------------------------------------------------------

    def fetch(
        self,
        table_uri: str,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
        snapshot_id: int | None = None,
        name: str | None = None,
        mode: str = "stream",
        wait: bool = True,
        timeout: float = 300.0,
    ) -> pa.Table:
        """Fetch data from an Iceberg table using the unified materialize API.

        This is the recommended way to read table data. It uses the unified
        /v1/materialize endpoint with identity@v1 transform, which:
        - Caches results as artifacts for query-level deduplication
        - Supports both streaming and artifact modes
        - Tracks lineage and provenance

        Args:
            table_uri: Table identifier (e.g., "file:///warehouse#db.table")
            columns: Columns to project (None for all columns)
            filters: Row filters for pruning (applied at Parquet row-group level)
            snapshot_id: Specific snapshot to read (None for current snapshot)
            name: Optional name to assign to the resulting artifact
            mode: "stream" (default) for immediate data, "artifact" for async build
            wait: If mode="artifact", wait for build to complete (default True)
            timeout: Maximum seconds to wait for build (default 300)

        Returns:
            Arrow Table containing the query results

        Example:
            # Simple table scan
            table = client.fetch("file:///warehouse#db.events")

            # With projection and filters
            from strata.client import lt, eq
            table = client.fetch(
                "file:///warehouse#db.events",
                columns=["id", "value", "timestamp"],
                filters=[lt("value", 100.0), eq("status", "active")],
            )

            # Named artifact for caching
            table = client.fetch(
                "file:///warehouse#db.events",
                name="daily_events",
            )
        """
        artifact = self._fetch_via_materialize(
            table_uri=table_uri,
            columns=columns,
            filters=filters,
            snapshot_id=snapshot_id,
            name=name,
            mode=mode,
            wait=wait,
            timeout=timeout,
        )
        return artifact.to_table()

    def fetch_artifact(
        self,
        table_uri: str,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
        snapshot_id: int | None = None,
        name: str | None = None,
        mode: str = "stream",
        wait: bool = True,
        timeout: float = 300.0,
    ) -> "Artifact":
        """Fetch data from an Iceberg table, returning an Artifact handle.

        Like fetch(), but returns an Artifact object instead of the data directly.
        Useful when you need access to artifact metadata, lineage, or want to
        defer data download.

        Args:
            table_uri: Table identifier (e.g., "file:///warehouse#db.table")
            columns: Columns to project (None for all columns)
            filters: Row filters for pruning
            snapshot_id: Specific snapshot to read (None for current)
            name: Optional name to assign to the resulting artifact
            mode: "stream" (default) or "artifact"
            wait: If mode="artifact", wait for build to complete
            timeout: Maximum seconds to wait for build

        Returns:
            Artifact object with access to data and metadata

        Example:
            artifact = client.fetch_artifact(
                "file:///warehouse#db.events",
                name="daily_events",
            )
            print(f"Artifact: {artifact.uri}")
            print(f"Cache hit: {artifact.cache_hit}")

            # Check lineage
            lineage = artifact.lineage()

            # Get data when needed
            table = artifact.to_table()
        """
        return self._fetch_via_materialize(
            table_uri=table_uri,
            columns=columns,
            filters=filters,
            snapshot_id=snapshot_id,
            name=name,
            mode=mode,
            wait=wait,
            timeout=timeout,
        )

    def _fetch_via_materialize(
        self,
        table_uri: str,
        columns: list[str] | None,
        filters: list[Filter] | None,
        snapshot_id: int | None,
        name: str | None,
        mode: str,
        wait: bool,
        timeout: float,
    ) -> "Artifact":
        """Internal: fetch table data via unified /v1/materialize endpoint."""
        # Build identity@v1 transform params
        identity_params: dict[str, Any] = {}
        if columns:
            identity_params["columns"] = columns
        if filters:
            identity_params["filters"] = [
                {"column": f.column, "op": f.op.value, "value": f.value} for f in filters
            ]
        if snapshot_id is not None:
            identity_params["snapshot_id"] = snapshot_id

        # Build request
        request_body = {
            "inputs": [table_uri],
            "transform": {
                "executor": "identity@v1",
                "params": identity_params,
            },
            "mode": mode,
        }
        if name:
            request_body["name"] = name

        # Send request to unified endpoint
        response = self._client.post("/v1/materialize", json=request_body)
        response.raise_for_status()
        data = response.json()

        artifact_uri = data["artifact_uri"]
        artifact_id, version = self._parse_artifact_uri(artifact_uri)
        hit = data.get("hit", False)
        state = data.get("state", "ready")
        stream_id = data.get("stream_id")
        stream_url = data.get("stream_url")
        build_id = data.get("build_id")

        # Cache hit - artifact is ready
        if hit or state == "ready":
            # On cache hit with stream mode, fetch the data via stream_url
            stream_data = None
            if stream_url and mode == "stream":
                stream_data = self._fetch_stream_with_retry(stream_url)

            return Artifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=hit,
                execution="cache" if hit else "server",
                name=name,
                _stream_data=stream_data,
            )

        # Stream mode - fetch data via stream URL
        if mode == "stream" and stream_url:
            # Fetch the stream with retry support
            content = self._fetch_stream_with_retry(stream_url)

            # Return artifact (data was streamed and persisted)
            return Artifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=False,
                execution="stream",
                name=name,
                _stream_data=content,  # Cache for to_table() call
            )

        # Artifact mode - poll for completion if wait=True
        if build_id and wait:
            start_time = time.time()
            while True:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Build {build_id} timed out after {timeout}s")

                # Check build status
                status_resp = self._client.get(f"/v1/builds/{build_id}")
                if status_resp.status_code == 404:
                    # Build endpoint may not exist yet, try artifact status
                    status_resp = self._client.get(f"/v1/artifacts/{artifact_id}/v/{version}")

                status_resp.raise_for_status()
                build_status = status_resp.json()

                current_state = build_status.get("state", "building")
                if current_state == "ready":
                    return Artifact(
                        _client=self,
                        artifact_id=artifact_id,
                        version=version,
                        cache_hit=False,
                        execution="server",
                        build_id=build_id,
                        name=name,
                    )
                elif current_state == "failed":
                    error_msg = build_status.get("error_message", "Unknown error")
                    raise RuntimeError(f"Build failed: {error_msg}")

                time.sleep(0.5)

        # Not waiting or no build_id - return artifact in building state
        return Artifact(
            _client=self,
            artifact_id=artifact_id,
            version=version,
            cache_hit=False,
            execution="server",
            build_id=build_id or stream_id,
            name=name,
        )

    def _fetch_stream_with_retry(self, stream_url: str) -> bytes:
        """Fetch stream data with retry on 429 responses."""
        last_response = None
        for attempt in range(self.retry_config.max_retries + 1):
            response = self._client.get(stream_url)

            if response.status_code != 429:
                response.raise_for_status()
                return response.content

            last_response = response

            if attempt >= self.retry_config.max_retries:
                break

            # Calculate delay
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after) + random.uniform(0, self.retry_config.jitter)
                except ValueError:
                    delay = self.retry_config.calculate_delay(attempt)
            else:
                delay = self.retry_config.calculate_delay(attempt)

            time.sleep(delay)

        if last_response is not None:
            last_response.raise_for_status()
        raise httpx.HTTPStatusError("Max retries exceeded", request=None, response=last_response)

    def clear_cache(self) -> dict:
        """Clear the server's disk cache."""
        response = self._client.post("/v1/cache/clear")
        response.raise_for_status()
        return response.json()

    # -----------------------------------------------------------------------
    # Artifact API
    # -----------------------------------------------------------------------

    def materialize(
        self,
        inputs: list[str],
        transform: dict[str, Any],
        name: str | None = None,
        mode: str = "auto",
        refresh: bool = False,
        wait: bool = True,
        poll_interval: float = 0.5,
        timeout: float = 300.0,
    ) -> Artifact:
        """Materialize a computed artifact.

        This is the main entry point for creating artifacts. The client
        automatically chooses local or server execution based on server
        capabilities and the `mode` parameter.

        Args:
            inputs: List of input URIs (table URIs or artifact URIs)
            transform: Transform specification with "ref" and "params"
                Example: {"ref": "duckdb_sql@v1", "params": {"sql": "..."}}
            name: Optional name to assign to the result
            mode: Execution mode - "auto", "local", or "server"
            refresh: Force recompute even if cached
            wait: Wait for async builds to complete (server mode)
            poll_interval: Seconds between build status polls
            timeout: Maximum seconds to wait for build

        Returns:
            Artifact object with access to data and metadata

        Example:
            # SQL transform
            result = client.materialize(
                inputs=["file:///warehouse#db.events"],
                transform={
                    "ref": "duckdb_sql@v1",
                    "params": {"sql": "SELECT category, COUNT(*) FROM input0 GROUP BY 1"},
                },
                name="category_counts",
            )
            df = result.to_pandas()

            # Chain artifacts
            result2 = client.materialize(
                inputs=[result.uri],
                transform={
                    "ref": "duckdb_sql@v1",
                    "params": {"sql": "SELECT * FROM input0 WHERE count > 100"},
                },
            )
        """
        # Determine execution mode
        effective_mode = self._resolve_execution_mode(mode, transform)

        if effective_mode == "local":
            return self._materialize_local(
                inputs=inputs,
                transform=transform,
                name=name,
                refresh=refresh,
            )
        else:
            return self._materialize_server(
                inputs=inputs,
                transform=transform,
                name=name,
                refresh=refresh,
                wait=wait,
                poll_interval=poll_interval,
                timeout=timeout,
            )

    def _resolve_execution_mode(self, mode: str, transform: dict[str, Any]) -> str:
        """Resolve execution mode based on server capabilities."""
        if mode in ("local", "server"):
            return mode

        # mode == "auto": check server capabilities
        # For now, default to server if transform ref doesn't start with "local://"
        ref = transform.get("ref", "")
        if ref.startswith("local://"):
            return "local"

        # Try server first, fall back to local if not supported
        # In future: query /v1/capabilities to check server support
        return "server"

    def _materialize_local(
        self,
        inputs: list[str],
        transform: dict[str, Any],
        name: str | None,
        refresh: bool,
    ) -> Artifact:
        """Execute transform locally and upload result."""
        # Request materialize to check cache / get build spec
        # Map 'ref' to 'executor' for server compatibility
        server_transform = dict(transform)
        if "ref" in server_transform:
            server_transform["executor"] = server_transform.pop("ref")

        request_body = {
            "inputs": inputs,
            "transform": server_transform,
            "name": name,
        }
        if refresh:
            request_body["refresh"] = True

        response = self._client.post("/v1/artifacts/materialize", json=request_body)
        response.raise_for_status()
        data = response.json()

        artifact_uri = data["artifact_uri"]
        artifact_id, version = self._parse_artifact_uri(artifact_uri)

        # Cache hit
        if data.get("hit") or data.get("state") == "ready":
            return Artifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=True,
                execution="cache",
                name=name,
            )

        # Cache miss - need to build locally
        build_spec = data.get("build_spec")
        if build_spec is None:
            raise ValueError("Expected build_spec for local execution")

        # Fetch input tables
        input_tables = {}
        for uri in build_spec.get("input_uris", inputs):
            if uri.startswith("strata://artifact/"):
                input_tables[uri] = self._fetch_artifact_by_uri(uri)
            else:
                input_tables[uri] = self.scan_to_table(uri)

        # Execute locally
        result_table = self._run_local(build_spec, input_tables)

        # Upload result
        self._upload_artifact(artifact_id, version, result_table, name)

        return Artifact(
            _client=self,
            artifact_id=artifact_id,
            version=version,
            cache_hit=False,
            execution="local",
            name=name,
        )

    def _parse_artifact_uri(self, uri: str) -> tuple[str, int]:
        """Parse artifact URI into (artifact_id, version)."""
        import re

        match = re.match(r"^strata://artifact/([^@]+)@v=(\d+)$", uri)
        if not match:
            raise ValueError(f"Invalid artifact URI: {uri}")
        return match.group(1), int(match.group(2))

    def _materialize_server(
        self,
        inputs: list[str],
        transform: dict[str, Any],
        name: str | None,
        refresh: bool,
        wait: bool,
        poll_interval: float,
        timeout: float,
    ) -> Artifact:
        """Request server-side execution."""
        # Map 'ref' to 'executor' for server compatibility
        server_transform = dict(transform)
        if "ref" in server_transform:
            server_transform["executor"] = server_transform.pop("ref")

        request_body = {
            "inputs": inputs,
            "transform": server_transform,
            "name": name,
        }
        if refresh:
            request_body["refresh"] = True

        response = self._client.post("/v1/artifacts/materialize", json=request_body)
        response.raise_for_status()
        data = response.json()

        # Parse artifact_uri to get artifact_id and version
        artifact_uri = data["artifact_uri"]
        artifact_id, version = self._parse_artifact_uri(artifact_uri)
        build_id = data.get("build_id")
        hit = data.get("hit", False)
        state = data.get("state", "ready")

        # Cache hit or already ready
        if hit or state == "ready":
            return Artifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=hit,
                execution="cache" if hit else "server",
                name=name,
            )

        # Build in progress
        if not wait:
            return Artifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=False,
                execution="server",
                build_id=build_id,
                name=name,
            )

        # Poll for completion if server is building
        if build_id:
            start_time = time.time()
            while True:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Build {build_id} timed out after {timeout}s")

                status_resp = self._client.get(f"/v1/artifacts/builds/{build_id}")
                status_resp.raise_for_status()
                build_status = status_resp.json()

                if build_status["state"] == "ready":
                    return Artifact(
                        _client=self,
                        artifact_id=artifact_id,
                        version=version,
                        cache_hit=False,
                        execution="server",
                        build_id=build_id,
                        name=name,
                    )
                elif build_status["state"] == "failed":
                    error_msg = build_status.get("error_message", "Unknown error")
                    raise RuntimeError(f"Build failed: {error_msg}")

                time.sleep(poll_interval)

        # Server returned build_spec but no build_id = personal mode
        # Fall back to local execution
        build_spec = data.get("build_spec")
        if build_spec:
            # Fetch input tables
            input_tables = {}
            for uri in build_spec.get("input_uris", []):
                if uri.startswith("strata://artifact/"):
                    input_tables[uri] = self._fetch_artifact_by_uri(uri)
                else:
                    input_tables[uri] = self.scan_to_table(uri)

            # Execute locally
            result_table = self._run_local(build_spec, input_tables)

            # Upload result
            self._upload_artifact(artifact_id, version, result_table, name)

            return Artifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=False,
                execution="local",
                name=name,
            )

        # Should not reach here - server should return either hit, build_id, or build_spec
        raise RuntimeError("Server returned neither hit, build_id, nor build_spec")

    def _run_local(
        self,
        build_spec: dict,
        input_tables: dict[str, pa.Table],
    ) -> pa.Table:
        """Execute a transform locally."""
        from strata.executors import run_local

        return run_local(build_spec, input_tables)

    def _upload_artifact(
        self,
        artifact_id: str,
        version: int,
        table: pa.Table,
        name: str | None = None,
    ) -> dict:
        """Upload an artifact after local computation."""
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

        # Finalize
        finalize_response = self._client.post(
            "/v1/artifacts/finalize",
            json={
                "artifact_id": artifact_id,
                "version": version,
                "arrow_schema": table.schema.to_string(),
                "row_count": table.num_rows,
                "name": name,
            },
        )
        finalize_response.raise_for_status()
        return finalize_response.json()

    def _fetch_artifact_data(self, artifact_id: str, version: int) -> pa.Table:
        """Fetch artifact data by ID and version."""
        response = self._client.get(f"/v1/artifacts/{artifact_id}/v/{version}/data")
        response.raise_for_status()
        reader = ipc.open_stream(pa.BufferReader(response.content))
        return reader.read_all()

    def _fetch_artifact_by_uri(self, artifact_uri: str) -> pa.Table:
        """Fetch artifact data by URI."""
        import re

        match = re.match(r"^strata://artifact/([^@]+)@v=(\d+)$", artifact_uri)
        if not match:
            raise ValueError(f"Invalid artifact URI: {artifact_uri}")

        artifact_id = match.group(1)
        version = int(match.group(2))
        return self._fetch_artifact_data(artifact_id, version)

    def get_artifact(self, artifact_id: str, version: int) -> Artifact:
        """Get an existing artifact by ID and version.

        Args:
            artifact_id: Artifact ID
            version: Version number

        Returns:
            Artifact object

        Raises:
            httpx.HTTPStatusError: If artifact not found (404)
        """
        # Verify artifact exists
        response = self._client.get(f"/v1/artifacts/{artifact_id}/v/{version}")
        response.raise_for_status()
        return Artifact(_client=self, artifact_id=artifact_id, version=version)

    def get_artifact_by_name(self, name: str) -> Artifact:
        """Get an artifact by its name.

        Args:
            name: Artifact name

        Returns:
            Artifact object

        Raises:
            httpx.HTTPStatusError: If name not found (404)
        """
        resolved = self.resolve_name(name)
        artifact_id, version = self._parse_artifact_uri(resolved["artifact_uri"])
        return Artifact(
            _client=self,
            artifact_id=artifact_id,
            version=version,
            name=name,
        )

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
        transform: dict[str, Any],
        name: str | None = None,
    ) -> dict:
        """Explain what materialize would do without doing it (dry run).

        Use this to check whether a computation would be a cache hit or miss,
        and if stale, which specific inputs have changed.

        Args:
            inputs: List of input URIs (table URIs or artifact URIs)
            transform: Transform specification with "ref" and "params"
                Example: {"ref": "duckdb_sql@v1", "params": {"sql": "..."}}
            name: Optional name to check staleness against

        Returns:
            Dict with fields:
            - cache_hit: True if result would be cached
            - artifact_uri: URI of existing artifact if hit
            - provenance_hash: Hash of the transform specification
            - is_stale: True if named artifact exists but needs rebuild
            - stale_reason: Explanation of why rebuild is needed
            - execution: "cache", "local", or "server"

        Example:
            >>> result = client.explain_materialize(
            ...     inputs=["file:///warehouse#db.events"],
            ...     transform={"ref": "duckdb_sql@v1", "params": {"sql": "SELECT * FROM input0"}},
            ...     name="my_transform",
            ... )
            >>> if result["is_stale"]:
            ...     print(result["stale_reason"])
        """
        # Map 'ref' to 'executor' for server compatibility
        server_transform = dict(transform)
        if "ref" in server_transform:
            server_transform["executor"] = server_transform.pop("ref")

        request_body = {
            "inputs": inputs,
            "transform": server_transform,
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

    # -----------------------------------------------------------------------
    # Unified Materialize API
    # -----------------------------------------------------------------------

    async def fetch(
        self,
        table_uri: str,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
        snapshot_id: int | None = None,
        name: str | None = None,
        mode: str = "stream",
        wait: bool = True,
        timeout: float = 300.0,
    ) -> pa.Table:
        """Fetch data from an Iceberg table using the unified materialize API.

        This is the recommended way to read table data. It uses the unified
        /v1/materialize endpoint with identity@v1 transform, which:
        - Caches results as artifacts for query-level deduplication
        - Supports both streaming and artifact modes
        - Tracks lineage and provenance

        Args:
            table_uri: Table identifier (e.g., "file:///warehouse#db.table")
            columns: Columns to project (None for all columns)
            filters: Row filters for pruning (applied at Parquet row-group level)
            snapshot_id: Specific snapshot to read (None for current snapshot)
            name: Optional name to assign to the resulting artifact
            mode: "stream" (default) for immediate data, "artifact" for async build
            wait: If mode="artifact", wait for build to complete (default True)
            timeout: Maximum seconds to wait for build (default 300)

        Returns:
            Arrow Table containing the query results

        Example:
            # Simple table scan
            table = await client.fetch("file:///warehouse#db.events")

            # With projection and filters
            from strata.client import lt, eq
            table = await client.fetch(
                "file:///warehouse#db.events",
                columns=["id", "value", "timestamp"],
                filters=[lt("value", 100.0), eq("status", "active")],
            )
        """
        artifact = await self._fetch_via_materialize(
            table_uri=table_uri,
            columns=columns,
            filters=filters,
            snapshot_id=snapshot_id,
            name=name,
            mode=mode,
            wait=wait,
            timeout=timeout,
        )
        return await artifact.to_table()

    async def fetch_artifact(
        self,
        table_uri: str,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
        snapshot_id: int | None = None,
        name: str | None = None,
        mode: str = "stream",
        wait: bool = True,
        timeout: float = 300.0,
    ) -> "AsyncArtifact":
        """Fetch data from an Iceberg table, returning an AsyncArtifact handle.

        Like fetch(), but returns an AsyncArtifact object instead of the data directly.
        Useful when you need access to artifact metadata, lineage, or want to
        defer data download.

        Args:
            table_uri: Table identifier (e.g., "file:///warehouse#db.table")
            columns: Columns to project (None for all columns)
            filters: Row filters for pruning
            snapshot_id: Specific snapshot to read (None for current)
            name: Optional name to assign to the resulting artifact
            mode: "stream" (default) or "artifact"
            wait: If mode="artifact", wait for build to complete
            timeout: Maximum seconds to wait for build

        Returns:
            AsyncArtifact object with access to data and metadata
        """
        return await self._fetch_via_materialize(
            table_uri=table_uri,
            columns=columns,
            filters=filters,
            snapshot_id=snapshot_id,
            name=name,
            mode=mode,
            wait=wait,
            timeout=timeout,
        )

    async def _fetch_via_materialize(
        self,
        table_uri: str,
        columns: list[str] | None,
        filters: list[Filter] | None,
        snapshot_id: int | None,
        name: str | None,
        mode: str,
        wait: bool,
        timeout: float,
    ) -> "AsyncArtifact":
        """Internal: fetch table data via unified /v1/materialize endpoint."""
        # Build identity@v1 transform params
        identity_params: dict[str, Any] = {}
        if columns:
            identity_params["columns"] = columns
        if filters:
            identity_params["filters"] = [
                {"column": f.column, "op": f.op.value, "value": f.value} for f in filters
            ]
        if snapshot_id is not None:
            identity_params["snapshot_id"] = snapshot_id

        # Build request
        request_body = {
            "inputs": [table_uri],
            "transform": {
                "executor": "identity@v1",
                "params": identity_params,
            },
            "mode": mode,
        }
        if name:
            request_body["name"] = name

        # Send request to unified endpoint
        response = await self._client.post("/v1/materialize", json=request_body)
        response.raise_for_status()
        data = response.json()

        artifact_uri = data["artifact_uri"]
        artifact_id, version = self._parse_artifact_uri(artifact_uri)
        hit = data.get("hit", False)
        state = data.get("state", "ready")
        stream_id = data.get("stream_id")
        stream_url = data.get("stream_url")
        build_id = data.get("build_id")

        # Cache hit - artifact is ready
        if hit or state == "ready":
            return AsyncArtifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=hit,
                execution="cache" if hit else "server",
                name=name,
            )

        # Stream mode - fetch data via stream URL
        if mode == "stream" and stream_url:
            content = await self._fetch_stream_with_retry(stream_url)
            return AsyncArtifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=False,
                execution="stream",
                name=name,
                _stream_data=content,
            )

        # Artifact mode - poll for completion if wait=True
        if build_id and wait:
            start_time = time.time()
            while True:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Build {build_id} timed out after {timeout}s")

                status_resp = await self._client.get(f"/v1/builds/{build_id}")
                if status_resp.status_code == 404:
                    status_resp = await self._client.get(f"/v1/artifacts/{artifact_id}/v/{version}")

                status_resp.raise_for_status()
                build_status = status_resp.json()

                current_state = build_status.get("state", "building")
                if current_state == "ready":
                    return AsyncArtifact(
                        _client=self,
                        artifact_id=artifact_id,
                        version=version,
                        cache_hit=False,
                        execution="server",
                        build_id=build_id,
                        name=name,
                    )
                elif current_state == "failed":
                    error_msg = build_status.get("error_message", "Unknown error")
                    raise RuntimeError(f"Build failed: {error_msg}")

                await asyncio.sleep(0.5)

        return AsyncArtifact(
            _client=self,
            artifact_id=artifact_id,
            version=version,
            cache_hit=False,
            execution="server",
            build_id=build_id or stream_id,
            name=name,
        )

    async def _fetch_stream_with_retry(self, stream_url: str) -> bytes:
        """Fetch stream data with retry on 429 responses."""
        last_response = None
        for attempt in range(self.retry_config.max_retries + 1):
            response = await self._client.get(stream_url)

            if response.status_code != 429:
                response.raise_for_status()
                return response.content

            last_response = response

            if attempt >= self.retry_config.max_retries:
                break

            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after) + random.uniform(0, self.retry_config.jitter)
                except ValueError:
                    delay = self.retry_config.calculate_delay(attempt)
            else:
                delay = self.retry_config.calculate_delay(attempt)

            await asyncio.sleep(delay)

        if last_response is not None:
            last_response.raise_for_status()
        raise httpx.HTTPStatusError("Max retries exceeded", request=None, response=last_response)

    async def clear_cache(self) -> dict:
        """Clear the server's disk cache."""
        response = await self._client.post("/v1/cache/clear")
        response.raise_for_status()
        return response.json()

    # -----------------------------------------------------------------------
    # Artifact API
    # -----------------------------------------------------------------------

    async def materialize(
        self,
        inputs: list[str],
        transform: dict[str, Any],
        name: str | None = None,
        mode: str = "auto",
        refresh: bool = False,
        wait: bool = True,
        poll_interval: float = 0.5,
        timeout: float = 300.0,
    ) -> "AsyncArtifact":
        """Materialize a computed artifact.

        This is the main entry point for creating artifacts. The client
        automatically chooses local or server execution based on server
        capabilities and the `mode` parameter.

        Args:
            inputs: List of input URIs (table URIs or artifact URIs)
            transform: Transform specification with "ref" and "params"
                Example: {"ref": "duckdb_sql@v1", "params": {"sql": "..."}}
            name: Optional name to assign to the result
            mode: Execution mode - "auto", "local", or "server"
            refresh: Force recompute even if cached
            wait: Wait for async builds to complete (server mode)
            poll_interval: Seconds between build status polls
            timeout: Maximum seconds to wait for build

        Returns:
            AsyncArtifact object with access to data and metadata

        Example:
            result = await client.materialize(
                inputs=["file:///warehouse#db.events"],
                transform={
                    "ref": "duckdb_sql@v1",
                    "params": {"sql": "SELECT category, COUNT(*) FROM input0 GROUP BY 1"},
                },
                name="category_counts",
            )
            df = (await result.to_table()).to_pandas()
        """
        # For async client, we only support server mode for now
        # (local execution would block the event loop)
        return await self._materialize_server(
            inputs=inputs,
            transform=transform,
            name=name,
            refresh=refresh,
            wait=wait,
            poll_interval=poll_interval,
            timeout=timeout,
        )

    def _parse_artifact_uri(self, uri: str) -> tuple[str, int]:
        """Parse artifact URI into (artifact_id, version)."""
        import re

        match = re.match(r"^strata://artifact/([^@]+)@v=(\d+)$", uri)
        if not match:
            raise ValueError(f"Invalid artifact URI: {uri}")
        return match.group(1), int(match.group(2))

    async def _materialize_server(
        self,
        inputs: list[str],
        transform: dict[str, Any],
        name: str | None,
        refresh: bool,
        wait: bool,
        poll_interval: float,
        timeout: float,
    ) -> "AsyncArtifact":
        """Request server-side execution."""
        # Map 'ref' to 'executor' for server compatibility
        server_transform = dict(transform)
        if "ref" in server_transform:
            server_transform["executor"] = server_transform.pop("ref")

        request_body = {
            "inputs": inputs,
            "transform": server_transform,
            "name": name,
        }
        if refresh:
            request_body["refresh"] = True

        response = await self._client.post("/v1/artifacts/materialize", json=request_body)
        response.raise_for_status()
        data = response.json()

        artifact_uri = data["artifact_uri"]
        artifact_id, version = self._parse_artifact_uri(artifact_uri)
        build_id = data.get("build_id")
        status = data.get("state", "ready")

        # Cache hit or already ready
        if status == "ready":
            return AsyncArtifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=True,
                execution="cache",
                name=name,
            )

        # Build in progress
        if not wait:
            return AsyncArtifact(
                _client=self,
                artifact_id=artifact_id,
                version=version,
                cache_hit=False,
                execution="server",
                build_id=build_id,
                name=name,
            )

        # Poll for completion
        if build_id:
            start_time = time.time()
            while True:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Build {build_id} timed out after {timeout}s")

                status_resp = await self._client.get(f"/v1/artifacts/builds/{build_id}")
                status_resp.raise_for_status()
                build_status = status_resp.json()

                if build_status["state"] == "ready":
                    return AsyncArtifact(
                        _client=self,
                        artifact_id=artifact_id,
                        version=version,
                        cache_hit=False,
                        execution="server",
                        build_id=build_id,
                        name=name,
                    )
                elif build_status["state"] == "failed":
                    error_msg = build_status.get("error_message", "Unknown error")
                    raise RuntimeError(f"Build failed: {error_msg}")

                await asyncio.sleep(poll_interval)

        return AsyncArtifact(
            _client=self,
            artifact_id=artifact_id,
            version=version,
            cache_hit=False,
            execution="server",
            name=name,
        )

    async def get_artifact(self, artifact_id: str, version: int) -> "AsyncArtifact":
        """Get an existing artifact by ID and version."""
        response = await self._client.get(f"/v1/artifacts/{artifact_id}/v/{version}")
        response.raise_for_status()
        return AsyncArtifact(_client=self, artifact_id=artifact_id, version=version)

    async def get_artifact_by_name(self, name: str) -> "AsyncArtifact":
        """Get an artifact by its name."""
        response = await self._client.get(f"/v1/names/{name}")
        response.raise_for_status()
        resolved = response.json()

        artifact_id, version = self._parse_artifact_uri(resolved["artifact_uri"])
        return AsyncArtifact(
            _client=self,
            artifact_id=artifact_id,
            version=version,
            name=name,
        )


@dataclass
class AsyncArtifact:
    """An immutable, versioned artifact from Strata (async version).

    Returned by `AsyncStrataClient.materialize()` or `AsyncStrataClient.fetch_artifact()`.
    Provides async access to artifact data and metadata.
    """

    _client: "AsyncStrataClient"
    artifact_id: str
    version: int
    cache_hit: bool = False
    execution: str = "cache"  # "cache" | "server" | "stream"
    build_id: str | None = None
    name: str | None = None
    _stream_data: bytes | None = None  # Cached stream data from fetch()

    @property
    def uri(self) -> str:
        """Artifact URI (strata://artifact/{id}@v={version})."""
        return f"strata://artifact/{self.artifact_id}@v={self.version}"

    @property
    def name_uri(self) -> str | None:
        """Name URI if a name was assigned."""
        return f"strata://name/{self.name}" if self.name else None

    async def info(self) -> dict:
        """Get artifact metadata."""
        response = await self._client._client.get(
            f"/v1/artifacts/{self.artifact_id}/v/{self.version}"
        )
        response.raise_for_status()
        return response.json()

    async def to_table(self) -> pa.Table:
        """Download artifact data as Arrow Table."""
        # Use cached stream data if available (from fetch with stream mode)
        if self._stream_data is not None:
            if not self._stream_data:
                return pa.table({})
            reader = ipc.open_stream(pa.BufferReader(self._stream_data))
            return reader.read_all()
        response = await self._client._client.get(
            f"/v1/artifacts/{self.artifact_id}/v/{self.version}/data"
        )
        response.raise_for_status()
        reader = ipc.open_stream(pa.BufferReader(response.content))
        return reader.read_all()

    async def to_pandas(self):
        """Download artifact data as pandas DataFrame."""
        table = await self.to_table()
        return table.to_pandas()

    async def to_polars(self):
        """Download artifact data as Polars DataFrame."""
        import polars as pl

        table = await self.to_table()
        return pl.from_arrow(table)

    async def lineage(self, direction: str = "upstream", max_depth: int = 10) -> dict:
        """Get artifact lineage (dependency graph)."""
        response = await self._client._client.get(
            f"/v1/artifacts/{self.artifact_id}/v/{self.version}/lineage",
            params={"direction": direction, "max_depth": max_depth},
        )
        response.raise_for_status()
        return response.json()

    async def dependents(self, max_depth: int = 10) -> dict:
        """Get artifacts that depend on this artifact."""
        response = await self._client._client.get(
            f"/v1/artifacts/{self.artifact_id}/v/{self.version}/dependents",
            params={"max_depth": max_depth},
        )
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
