"""Background build runner for server-mode transforms.

The build runner is responsible for:
1. Polling for pending builds
2. Acquiring inputs (artifacts or Iceberg scans)
3. Streaming inputs to external executors
4. Persisting outputs as new artifact versions
5. Updating build state on success/failure

Concurrency controls:
- Global semaphore limits total concurrent builds
- Per-tenant semaphore limits builds per tenant
- Build-level timeout from transform definition

Executor HTTP Protocol (Push Model):
    Request: POST {executor_url}/v1/execute
    Content-Type: multipart/form-data

    Parts:
    1. metadata (application/json):
       {
         "build_id": "...",
         "tenant": "...",
         "principal": "...",
         "provenance_hash": "...",
         "transform": {"ref": "...", "code_hash": "...", "params": {...}},
         "inputs": [{"name": "input0", "format": "arrow_ipc_stream"}, ...]
       }

    2. One part per input (application/vnd.apache.arrow.stream):
       - Field name: input0, input1, ...
       - Body: Arrow IPC stream bytes

    Response:
    - Status: 200
    - Content-Type: application/vnd.apache.arrow.stream
    - Body: Output Arrow IPC stream bytes
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import tempfile
import time
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from strata.artifact_store import ArtifactStore
    from strata.config import StrataConfig
    from strata.transforms.build_store import BuildState, BuildStore
    from strata.transforms.registry import TransformDefinition, TransformRegistry

logger = logging.getLogger(__name__)


@dataclass
class RunnerConfig:
    """Configuration for the build runner.

    Attributes:
        poll_interval_ms: How often to poll for pending builds (default 500ms)
        max_concurrent_builds: Global limit on concurrent builds
        max_builds_per_tenant: Per-tenant limit on concurrent builds
        default_timeout_seconds: Default build timeout if not in registry
        default_max_output_bytes: Default max output size if not in registry
    """

    poll_interval_ms: int = 500
    max_concurrent_builds: int = 10
    max_builds_per_tenant: int = 3
    default_timeout_seconds: float = 300.0
    default_max_output_bytes: int = 1024 * 1024 * 1024  # 1 GB


@dataclass
class BuildRunner:
    """Background runner for server-mode builds.

    The runner polls for pending builds and executes them asynchronously
    using external executors. It manages concurrency limits and handles
    errors gracefully.

    Usage:
        runner = BuildRunner(config, artifact_store, build_store, registry)
        await runner.start()
        # ... server runs ...
        await runner.stop()
    """

    config: RunnerConfig
    artifact_store: "ArtifactStore"
    build_store: "BuildStore"
    transform_registry: "TransformRegistry"
    artifact_dir: Path

    # Internal state
    _running: bool = field(default=False, init=False)
    _task: asyncio.Task | None = field(default=None, init=False)
    _global_sem: asyncio.Semaphore = field(init=False)
    _tenant_sems: dict[str, asyncio.Semaphore] = field(default_factory=dict, init=False)
    _running_builds: set[str] = field(default_factory=set, init=False)
    _build_tasks: dict[str, asyncio.Task] = field(default_factory=dict, init=False)

    def __post_init__(self):
        self._global_sem = asyncio.Semaphore(self.config.max_concurrent_builds)

    async def start(self) -> None:
        """Start the build runner background loop."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info(
            "Build runner started",
            extra={
                "max_concurrent": self.config.max_concurrent_builds,
                "max_per_tenant": self.config.max_builds_per_tenant,
                "poll_interval_ms": self.config.poll_interval_ms,
            },
        )

    async def stop(self) -> None:
        """Stop the build runner and cancel pending tasks."""
        if not self._running:
            return

        self._running = False

        # Cancel the main loop
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        # Cancel all running build tasks
        for build_id, task in list(self._build_tasks.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            # Mark as failed due to shutdown
            self.build_store.fail_build(
                build_id,
                error_message="Build cancelled due to server shutdown",
                error_code="SERVER_SHUTDOWN",
            )

        self._build_tasks.clear()
        self._running_builds.clear()
        logger.info("Build runner stopped")

    async def _run_loop(self) -> None:
        """Main polling loop for pending builds."""
        poll_interval = self.config.poll_interval_ms / 1000.0

        while self._running:
            try:
                # Fetch pending builds
                pending = self.build_store.list_pending_builds(limit=50)

                for build in pending:
                    # Skip if already running
                    if build.build_id in self._running_builds:
                        continue

                    # Submit build for execution
                    self._submit_build(build)

                # Also retry builds stuck in "building" state (from crashes)
                # These will be picked up on startup
                building = self._get_stuck_builds()
                for build in building:
                    if build.build_id not in self._running_builds:
                        self._submit_build(build)

            except Exception as e:
                logger.error(f"Error in build runner loop: {e}")

            await asyncio.sleep(poll_interval)

    def _get_stuck_builds(self) -> list["BuildState"]:
        """Get builds that are stuck in 'building' state (from crashes).

        These are builds that were started but never completed, possibly
        due to a server crash. We retry them on startup.
        """
        # For now, use the build store's method to find building builds
        # that have been in that state for too long
        conn = self.build_store._get_connection()
        try:
            # Find builds that have been "building" for more than 5 minutes
            # (likely stuck from a crash)
            stuck_threshold = time.time() - 300  # 5 minutes
            cursor = conn.execute(
                """
                SELECT build_id, artifact_id, version, state, executor_ref,
                       executor_url, tenant_id, principal_id, created_at,
                       started_at, completed_at, error_message, error_code,
                       input_byte_count, output_byte_count
                FROM artifact_builds
                WHERE state = 'building' AND started_at < ?
                ORDER BY created_at ASC
                LIMIT 10
                """,
                (stuck_threshold,),
            )
            from strata.transforms.build_store import BuildState

            return [
                BuildState(
                    build_id=row["build_id"],
                    artifact_id=row["artifact_id"],
                    version=row["version"],
                    state=row["state"],
                    executor_ref=row["executor_ref"],
                    executor_url=row["executor_url"],
                    tenant_id=row["tenant_id"],
                    principal_id=row["principal_id"],
                    created_at=row["created_at"],
                    started_at=row["started_at"],
                    completed_at=row["completed_at"],
                    error_message=row["error_message"],
                    error_code=row["error_code"],
                    input_byte_count=row["input_byte_count"],
                    output_byte_count=row["output_byte_count"],
                )
                for row in cursor.fetchall()
            ]
        finally:
            conn.close()

    def _submit_build(self, build: "BuildState") -> None:
        """Submit a build for async execution."""
        if build.build_id in self._running_builds:
            return

        self._running_builds.add(build.build_id)
        task = asyncio.create_task(self._execute_build_with_semaphores(build))
        self._build_tasks[build.build_id] = task

        # Clean up when done
        def cleanup(t):
            self._running_builds.discard(build.build_id)
            self._build_tasks.pop(build.build_id, None)

        task.add_done_callback(cleanup)

    async def _execute_build_with_semaphores(self, build: "BuildState") -> None:
        """Execute a build with concurrency controls."""
        tenant_id = build.tenant_id or "__default__"

        # Get or create per-tenant semaphore
        if tenant_id not in self._tenant_sems:
            self._tenant_sems[tenant_id] = asyncio.Semaphore(
                self.config.max_builds_per_tenant
            )

        tenant_sem = self._tenant_sems[tenant_id]

        # Acquire both semaphores
        async with self._global_sem:
            async with tenant_sem:
                await self._execute_build(build)

    async def _execute_build(self, build: "BuildState") -> None:
        """Execute a single build.

        This is the main build execution logic:
        1. Mark build as started
        2. Get transform definition from registry
        3. Load artifact metadata to get inputs and transform
        4. Acquire inputs (artifacts or Iceberg scans)
        5. Stream inputs to executor
        6. Persist output as new artifact version
        7. Update build state on success/failure
        """
        build_id = build.build_id
        temp_files: list[Path] = []

        try:
            # Mark as started (skip if already building from retry)
            if build.state == "pending":
                if not self.build_store.start_build(build_id):
                    logger.warning(f"Build {build_id} already started or completed")
                    return

            # Get fresh build state
            build = self.build_store.get_build(build_id)
            if build is None or build.state not in ("pending", "building"):
                return

            # Get transform definition
            transform_defn = self.transform_registry.get(build.executor_ref)
            if transform_defn is None:
                raise ValueError(f"Transform not found in registry: {build.executor_ref}")

            # Get artifact metadata to retrieve inputs and transform
            artifact = self.artifact_store.get_artifact(build.artifact_id, build.version)
            if artifact is None:
                raise ValueError(f"Artifact not found: {build.artifact_id}@v={build.version}")

            # Parse inputs and transform from artifact metadata
            if artifact.transform_spec is None:
                raise ValueError("Artifact has no transform spec")

            transform_data = json.loads(artifact.transform_spec)
            input_uris = transform_data.get("inputs", [])

            # Prepare input files (materialize each input to temp file)
            input_files: list[tuple[str, Path]] = []
            for i, input_uri in enumerate(input_uris):
                input_name = f"input{i}"
                input_path = await self._acquire_input(input_uri, temp_files)
                input_files.append((input_name, input_path))

            # Get executor URL and timeout
            executor_url = transform_defn.executor_url
            timeout = transform_defn.timeout_seconds or self.config.default_timeout_seconds
            max_output = transform_defn.max_output_bytes or self.config.default_max_output_bytes

            # Prepare executor request metadata
            metadata = {
                "build_id": build_id,
                "tenant": build.tenant_id,
                "principal": build.principal_id,
                "provenance_hash": artifact.provenance_hash,
                "transform": {
                    "ref": build.executor_ref,
                    "code_hash": hashlib.sha256(
                        artifact.transform_spec.encode()
                    ).hexdigest()[:16],
                    "params": transform_data.get("params", {}),
                },
                "inputs": [
                    {"name": name, "format": "arrow_ipc_stream"}
                    for name, _ in input_files
                ],
            }

            # Execute transform via external executor
            output_path = await self._call_executor(
                executor_url=executor_url,
                metadata=metadata,
                input_files=input_files,
                timeout=timeout,
                max_output_bytes=max_output,
                temp_files=temp_files,
            )

            # Read output schema and row count
            output_bytes = output_path.stat().st_size
            schema_json, row_count = self._read_arrow_metadata(output_path)

            # Move output to final artifact location
            final_path = self.artifact_store._blob_path(build.artifact_id, build.version)
            output_path.rename(final_path)

            # Finalize artifact
            self.artifact_store.finalize_artifact(
                artifact_id=build.artifact_id,
                version=build.version,
                schema_json=schema_json,
                row_count=row_count,
                byte_size=output_bytes,
            )

            # Set name if present in transform spec
            # (Name is stored elsewhere, check artifact_names for pending name)
            # For now, names are set by the materialize endpoint

            # Mark build as complete
            self.build_store.complete_build(
                build_id=build_id,
                output_byte_count=output_bytes,
            )

            logger.info(
                f"Build {build_id} completed successfully",
                extra={
                    "artifact_id": build.artifact_id,
                    "version": build.version,
                    "output_bytes": output_bytes,
                    "row_count": row_count,
                },
            )

        except asyncio.CancelledError:
            # Don't mark as failed if cancelled - will be retried
            raise

        except Exception as e:
            error_msg = str(e)
            error_code = type(e).__name__

            # Truncate long error messages
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."

            logger.error(
                f"Build {build_id} failed: {error_msg}",
                extra={"traceback": traceback.format_exc()},
            )

            self.build_store.fail_build(
                build_id=build_id,
                error_message=error_msg,
                error_code=error_code,
            )

            # Also mark artifact as failed
            self.artifact_store.fail_artifact(build.artifact_id, build.version)

        finally:
            # Clean up temp files
            for temp_file in temp_files:
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                except Exception:
                    pass

    async def _acquire_input(
        self,
        input_uri: str,
        temp_files: list[Path],
    ) -> Path:
        """Acquire an input and write it to a temp file.

        Supports:
        - strata://artifact/{id}@v={version} - read artifact blob
        - strata://name/{name} - resolve name and read artifact blob
        - file:// or s3:// - Iceberg table scan (to be implemented)

        Returns:
            Path to temp file containing Arrow IPC stream
        """
        # Handle artifact URIs
        if input_uri.startswith("strata://artifact/"):
            # Parse artifact URI: strata://artifact/{id}@v={version}
            import re

            match = re.match(r"strata://artifact/([^@]+)@v=(\d+)", input_uri)
            if not match:
                raise ValueError(f"Invalid artifact URI: {input_uri}")

            artifact_id = match.group(1)
            version = int(match.group(2))

            # Read artifact blob
            blob = self.artifact_store.read_blob(artifact_id, version)
            if blob is None:
                raise ValueError(f"Artifact blob not found: {input_uri}")

            # Write to temp file
            temp_file = Path(tempfile.mktemp(suffix=".arrow", dir=self.artifact_dir))
            temp_file.write_bytes(blob)
            temp_files.append(temp_file)
            return temp_file

        # Handle name URIs
        if input_uri.startswith("strata://name/"):
            name = input_uri.split("/", 3)[-1]
            artifact = self.artifact_store.resolve_name(name)
            if artifact is None:
                raise ValueError(f"Name not found: {name}")

            # Read artifact blob
            blob = self.artifact_store.read_blob(artifact.id, artifact.version)
            if blob is None:
                raise ValueError(f"Artifact blob not found for name: {name}")

            # Write to temp file
            temp_file = Path(tempfile.mktemp(suffix=".arrow", dir=self.artifact_dir))
            temp_file.write_bytes(blob)
            temp_files.append(temp_file)
            return temp_file

        # Handle Iceberg table URIs (file:// or s3://)
        if input_uri.startswith("file://") or input_uri.startswith("s3://"):
            # For Iceberg scans, we need to run the internal scan pipeline
            # and write the output to a temp file
            temp_file = await self._scan_to_file(input_uri, temp_files)
            return temp_file

        raise ValueError(f"Unsupported input URI: {input_uri}")

    async def _scan_to_file(
        self,
        table_uri: str,
        temp_files: list[Path],
    ) -> Path:
        """Run an Iceberg scan and write output to a temp file.

        This uses the internal scan pipeline to read the table and
        write the Arrow IPC stream to a file.
        """
        # Import here to avoid circular imports
        from strata.planner import ReadPlanner
        from strata.cache import CachedFetcher

        # Create a temp file for output
        temp_file = Path(tempfile.mktemp(suffix=".arrow", dir=self.artifact_dir))
        temp_files.append(temp_file)

        # Run in thread pool to avoid blocking event loop
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self._scan_to_file_sync,
            table_uri,
            temp_file,
        )

        return temp_file

    def _scan_to_file_sync(self, table_uri: str, output_path: Path) -> None:
        """Synchronous helper to scan a table and write to file."""
        # This is a simplified implementation
        # In production, you'd want to reuse the existing scan infrastructure
        from strata.planner import ReadPlanner
        from strata.config import StrataConfig

        # Get config from somewhere - for now use a simple approach
        # In practice, you'd pass the config through
        config = StrataConfig.load()

        planner = ReadPlanner(
            warehouse_paths=[],  # Will be extracted from URI
            catalog_properties=config.catalog_properties,
            cache_dir=config.cache_dir,
            batch_size=config.batch_size,
            metadata_db=config.metadata_db,
        )

        # Plan the scan
        plan = planner.plan(
            table_uri=table_uri,
            snapshot_id=None,  # Current snapshot
            columns=None,  # All columns
            filters=None,
        )

        # Read all tasks and write to file
        import pyarrow as pa
        import pyarrow.ipc as ipc

        # Collect all batches
        batches = []
        schema = None

        for task in plan.tasks:
            # Read cached data for this task
            # This is simplified - real implementation would use CachedFetcher
            cache_path = config.cache_dir / f"{task.cache_key}.arrow"
            if cache_path.exists():
                with pa.ipc.open_stream(cache_path) as reader:
                    if schema is None:
                        schema = reader.schema
                    for batch in reader:
                        batches.append(batch)

        # Write to output file
        if batches:
            table = pa.Table.from_batches(batches, schema=schema)
            with pa.ipc.new_stream(str(output_path), schema) as writer:
                for batch in table.to_batches():
                    writer.write_batch(batch)
        else:
            # Write empty table with schema from plan
            raise ValueError(f"No data found for table: {table_uri}")

    async def _call_executor(
        self,
        executor_url: str,
        metadata: dict,
        input_files: list[tuple[str, Path]],
        timeout: float,
        max_output_bytes: int,
        temp_files: list[Path],
    ) -> Path:
        """Call the external executor with inputs and receive output.

        Uses multipart/form-data to stream inputs to the executor.
        The executor returns an Arrow IPC stream which is written to
        a temp file with size enforcement.

        Args:
            executor_url: Base URL of the executor
            metadata: Build metadata JSON
            input_files: List of (name, path) tuples for inputs
            timeout: Request timeout in seconds
            max_output_bytes: Maximum output size in bytes
            temp_files: List to append temp files for cleanup

        Returns:
            Path to temp file containing output Arrow IPC stream

        Raises:
            ValueError: If output exceeds max_output_bytes
            httpx.HTTPStatusError: If executor returns non-200 status
            asyncio.TimeoutError: If request times out
        """
        # Prepare multipart files
        files = {
            "metadata": ("metadata.json", json.dumps(metadata), "application/json"),
        }

        for name, path in input_files:
            files[name] = (
                f"{name}.arrow",
                path.read_bytes(),
                "application/vnd.apache.arrow.stream",
            )

        # Create output temp file
        output_path = Path(tempfile.mktemp(suffix=".arrow", dir=self.artifact_dir))
        temp_files.append(output_path)

        # Make request with timeout
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await asyncio.wait_for(
                client.post(
                    f"{executor_url}/v1/execute",
                    files=files,
                ),
                timeout=timeout,
            )

            response.raise_for_status()

            # Stream response to file with size limit
            bytes_written = 0
            with open(output_path, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    bytes_written += len(chunk)
                    if bytes_written > max_output_bytes:
                        raise ValueError(
                            f"Output exceeds maximum size: {bytes_written} > {max_output_bytes}"
                        )
                    f.write(chunk)

        return output_path

    def _read_arrow_metadata(self, path: Path) -> tuple[str, int]:
        """Read Arrow IPC file metadata.

        Returns:
            Tuple of (schema_json, row_count)
        """
        import pyarrow.ipc as ipc

        with ipc.open_stream(str(path)) as reader:
            schema = reader.schema
            row_count = 0
            for batch in reader:
                row_count += batch.num_rows

        # Convert schema to JSON
        schema_json = schema.to_string()

        return schema_json, row_count


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_runner: BuildRunner | None = None


def get_build_runner() -> BuildRunner | None:
    """Get the build runner singleton."""
    return _runner


def set_build_runner(runner: BuildRunner | None) -> None:
    """Set the build runner singleton."""
    global _runner
    _runner = runner


def reset_build_runner() -> None:
    """Reset the build runner singleton (for testing)."""
    global _runner
    _runner = None
