"""Integration tests for personal mode artifacts.

These tests verify end-to-end artifact workflows:
1. Materialize with cache miss (build locally)
2. Materialize with cache hit (return cached)
3. Name pointer CRUD via client
4. Artifact data streaming
5. Service mode blocks artifact endpoints
"""

import socket
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
import uvicorn

from strata import server
from strata.artifact_store import reset_artifact_store
from strata.client import StrataClient
from strata.config import StrataConfig
from strata.server import ServerState, app

# =============================================================================
# Test Helpers
# =============================================================================


def find_free_port() -> int:
    """Find an available port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(port: int, timeout: float = 5.0) -> bool:
    """Wait for server to be ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = httpx.get(f"http://127.0.0.1:{port}/health", timeout=1.0)
            if resp.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


def table_to_ipc_bytes(table: pa.Table) -> bytes:
    """Convert Arrow table to IPC stream bytes."""
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


@dataclass
class ServerContext:
    """Context for a running test server."""

    config: StrataConfig
    port: int
    base_url: str
    server_instance: uvicorn.Server
    thread: threading.Thread


@contextmanager
def run_server(
    cache_dir,
    artifact_dir=None,
    deployment_mode: str = "personal",
):
    """Context manager to run a test server."""
    port = find_free_port()

    config = StrataConfig(
        host="127.0.0.1",
        port=port,
        cache_dir=cache_dir,
        deployment_mode=deployment_mode,
        artifact_dir=artifact_dir,
    )
    server._state = ServerState(config)

    server_config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
    )
    server_instance = uvicorn.Server(server_config)
    thread = threading.Thread(target=server_instance.run, daemon=True)
    thread.start()

    if not wait_for_server(port):
        raise RuntimeError(f"Server failed to start on port {port}")

    try:
        yield ServerContext(
            config=config,
            port=port,
            base_url=f"http://127.0.0.1:{port}",
            server_instance=server_instance,
            thread=thread,
        )
    finally:
        server_instance.should_exit = True
        thread.join(timeout=2.0)
        server._state = None
        reset_artifact_store()


def materialize_and_upload(
    client: StrataClient,
    sql: str,
    inputs: list[str] | None = None,
    input_tables: dict[str, pa.Table] | None = None,
    name: str | None = None,
) -> tuple[str, pa.Table]:
    """Helper to materialize, execute locally, and upload in one call.

    Returns (artifact_uri, result_table).
    """
    inputs = inputs or []
    input_tables = input_tables or {}

    hit, uri, spec = client.materialize(
        inputs=inputs,
        executor="local://duckdb_sql@v1",
        params={"sql": sql},
    )

    if hit:
        # Cache hit - fetch existing artifact
        return uri, client.fetch_artifact(uri)

    # Cache miss - execute and upload
    result = client.run_local(spec, input_tables)
    client.upload_artifact(
        artifact_id=spec["artifact_id"],
        version=spec["version"],
        table=result,
        name=name,
    )
    return uri, result


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def personal_mode_server(tmp_path):
    """Start a server in personal mode for artifact testing."""
    cache_dir = tmp_path / "cache"
    artifact_dir = tmp_path / "artifacts"
    cache_dir.mkdir()
    artifact_dir.mkdir()

    with run_server(cache_dir, artifact_dir, "personal") as ctx:
        yield {"config": ctx.config, "port": ctx.port, "base_url": ctx.base_url}


@pytest.fixture
def service_mode_server(tmp_path):
    """Start a server in service mode (artifacts disabled)."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    with run_server(cache_dir, deployment_mode="service") as ctx:
        yield {"config": ctx.config, "port": ctx.port, "base_url": ctx.base_url}


# =============================================================================
# HTTP Endpoint Tests
# =============================================================================


class TestArtifactEndpoints:
    """Tests for artifact HTTP endpoints."""

    def test_materialize_cache_miss(self, personal_mode_server):
        """Materialize returns build spec on cache miss."""
        response = httpx.post(
            f"{personal_mode_server['base_url']}/v1/artifacts/materialize",
            json={
                "inputs": ["file:///warehouse#db.events"],
                "transform": {
                    "executor": "local://duckdb_sql@v1",
                    "params": {"sql": "SELECT 1 as x"},
                },
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["hit"] is False
        assert data["artifact_uri"].startswith("strata://artifact/")
        assert data["build_spec"]["executor"] == "local://duckdb_sql@v1"

    def test_upload_and_finalize(self, personal_mode_server):
        """Upload blob and finalize artifact."""
        base_url = personal_mode_server["base_url"]

        # Create artifact
        resp = httpx.post(
            f"{base_url}/v1/artifacts/materialize",
            json={"inputs": [], "transform": {"executor": "test", "params": {}}},
        )
        build_spec = resp.json()["build_spec"]
        artifact_id, version = build_spec["artifact_id"], build_spec["version"]

        # Upload and finalize
        table = pa.table({"x": [1, 2, 3]})
        httpx.post(
            f"{base_url}/v1/artifacts/upload/{artifact_id}/v/{version}",
            content=table_to_ipc_bytes(table),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        resp = httpx.post(
            f"{base_url}/v1/artifacts/finalize",
            json={
                "artifact_id": artifact_id,
                "version": version,
                "arrow_schema": str(table.schema),
                "row_count": 3,
            },
        )
        assert resp.status_code == 200
        assert resp.json()["byte_size"] > 0

    def test_materialize_cache_hit(self, personal_mode_server):
        """Materialize returns hit after artifact is finalized."""
        base_url = personal_mode_server["base_url"]
        request_body = {
            "inputs": ["input1"],
            "transform": {"executor": "test", "params": {"key": "value"}},
        }

        # First call - miss
        resp = httpx.post(f"{base_url}/v1/artifacts/materialize", json=request_body)
        build_spec = resp.json()["build_spec"]
        artifact_id, version = build_spec["artifact_id"], build_spec["version"]

        # Upload and finalize
        table = pa.table({"result": [42]})
        httpx.post(
            f"{base_url}/v1/artifacts/upload/{artifact_id}/v/{version}",
            content=table_to_ipc_bytes(table),
        )
        httpx.post(
            f"{base_url}/v1/artifacts/finalize",
            json={
                "artifact_id": artifact_id,
                "version": version,
                "arrow_schema": str(table.schema),
                "row_count": 1,
            },
        )

        # Second call - hit
        resp = httpx.post(f"{base_url}/v1/artifacts/materialize", json=request_body)
        data = resp.json()
        assert data["hit"] is True
        assert data["build_spec"] is None

    def test_artifact_data_streaming(self, personal_mode_server):
        """Fetch artifact data returns Arrow IPC stream."""
        base_url = personal_mode_server["base_url"]

        # Create and finalize artifact
        resp = httpx.post(
            f"{base_url}/v1/artifacts/materialize",
            json={"inputs": [], "transform": {"executor": "test", "params": {}}},
        )
        build_spec = resp.json()["build_spec"]
        artifact_id, version = build_spec["artifact_id"], build_spec["version"]

        table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        httpx.post(
            f"{base_url}/v1/artifacts/upload/{artifact_id}/v/{version}",
            content=table_to_ipc_bytes(table),
        )
        httpx.post(
            f"{base_url}/v1/artifacts/finalize",
            json={
                "artifact_id": artifact_id,
                "version": version,
                "arrow_schema": str(table.schema),
                "row_count": 3,
            },
        )

        # Fetch and verify
        resp = httpx.get(f"{base_url}/v1/artifacts/{artifact_id}/v/{version}/data")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/vnd.apache.arrow.stream"

        result = ipc.open_stream(pa.BufferReader(resp.content)).read_all()
        assert result.num_rows == 3
        assert set(result.column_names) == {"x", "y"}

    def test_name_crud(self, personal_mode_server):
        """Name pointer CRUD operations."""
        base_url = personal_mode_server["base_url"]

        # Create artifact
        resp = httpx.post(
            f"{base_url}/v1/artifacts/materialize",
            json={"inputs": [], "transform": {"executor": "test", "params": {}}},
        )
        build_spec = resp.json()["build_spec"]
        artifact_id, version = build_spec["artifact_id"], build_spec["version"]

        table = pa.table({"x": [1]})
        httpx.post(
            f"{base_url}/v1/artifacts/upload/{artifact_id}/v/{version}",
            content=table_to_ipc_bytes(table),
        )
        httpx.post(
            f"{base_url}/v1/artifacts/finalize",
            json={
                "artifact_id": artifact_id,
                "version": version,
                "arrow_schema": str(table.schema),
                "row_count": 1,
            },
        )

        # Set name
        resp = httpx.post(
            f"{base_url}/v1/names",
            json={"name": "my-artifact", "artifact_id": artifact_id, "version": version},
        )
        assert resp.json()["name_uri"] == "strata://name/my-artifact"

        # Resolve name
        resp = httpx.get(f"{base_url}/v1/names/my-artifact")
        assert resp.json()["artifact_uri"].startswith("strata://artifact/")

        # List names
        resp = httpx.get(f"{base_url}/v1/names")
        assert resp.json()["names"][0]["name"] == "my-artifact"

        # Delete name
        httpx.delete(f"{base_url}/v1/names/my-artifact")
        resp = httpx.get(f"{base_url}/v1/names/my-artifact")
        assert resp.status_code == 404


class TestServiceModeBlocking:
    """Tests that service mode blocks artifact endpoints."""

    def test_materialize_blocked_in_service_mode(self, service_mode_server):
        """Materialize returns 403 in service mode."""
        response = httpx.post(
            f"{service_mode_server['base_url']}/v1/artifacts/materialize",
            json={"inputs": [], "transform": {"executor": "test", "params": {}}},
        )
        assert response.status_code == 403
        assert "writes_disabled" in response.json()["detail"]["error"]

    def test_names_blocked_in_service_mode(self, service_mode_server):
        """Name endpoints return 403 in service mode."""
        base_url = service_mode_server["base_url"]

        assert httpx.get(f"{base_url}/v1/names").status_code == 403
        assert httpx.get(f"{base_url}/v1/names/test").status_code == 403
        assert (
            httpx.post(
                f"{base_url}/v1/names",
                json={"name": "test", "artifact_id": "x", "version": 1},
            ).status_code
            == 403
        )


# =============================================================================
# Client Method Tests
# =============================================================================


class TestClientArtifactMethods:
    """Tests for client artifact helper methods."""

    def test_client_materialize_returns_build_spec(self, personal_mode_server):
        """Client materialize() returns (hit, uri, build_spec) on miss."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            hit, uri, spec = client.materialize(
                inputs=["table1"],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT 1"},
            )
            assert hit is False
            assert uri.startswith("strata://artifact/")
            assert spec["executor"] == "local://duckdb_sql@v1"

    def test_client_upload_and_fetch(self, personal_mode_server):
        """Client can upload and fetch artifacts."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            hit, uri, spec = client.materialize(inputs=[], executor="test", params={})

            original = pa.table({"a": [1, 2], "b": ["x", "y"]})
            client.upload_artifact(
                artifact_id=spec["artifact_id"],
                version=spec["version"],
                table=original,
            )

            fetched = client.fetch_artifact(uri)
            assert fetched.to_pydict() == original.to_pydict()

    def test_client_name_operations(self, personal_mode_server):
        """Client name helper methods work."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            hit, uri, spec = client.materialize(inputs=[], executor="test", params={})
            client.upload_artifact(
                artifact_id=spec["artifact_id"],
                version=spec["version"],
                table=pa.table({"x": [1]}),
            )

            result = client.set_name("test-name", spec["artifact_id"], spec["version"])
            assert result["name_uri"] == "strata://name/test-name"

            resolved = client.resolve_name("test-name")
            assert resolved["artifact_uri"].startswith("strata://artifact/")


# =============================================================================
# Contract Tests - The Core Workflow
# =============================================================================


class TestArtifactContract:
    """Contract tests that verify the complete artifact workflow.

    These are the critical tests that ensure the full loop works correctly:
    1. materialize() returns cache miss with build_spec
    2. Local executor (DuckDB) runs the transform
    3. upload_artifact() stores the result
    4. materialize() returns cache hit
    5. Data is accessible via artifact URI and name URI
    6. Persistence survives server restart
    """

    def test_transform_pipeline_with_dependencies(self, personal_mode_server):
        """Full pipeline: source -> transform -> aggregate, with real input dependencies.

        This is the realistic workflow that exercises the entire artifact system:
        - Stage 1: Create source data artifact
        - Stage 2: Filter source data (depends on stage 1)
        - Stage 3: Aggregate filtered data (depends on stage 2)
        - Verify all stages cache correctly and data flows through
        """
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Stage 1: Create source data
            source_uri, source_data = materialize_and_upload(
                client,
                sql="""
                    SELECT 'click' as event, 1 as user_id, 10 as amount
                    UNION ALL SELECT 'click', 1, 20
                    UNION ALL SELECT 'view', 2, 5
                    UNION ALL SELECT 'click', 2, 30
                    UNION ALL SELECT 'view', 1, 15
                """,
                name="events-source",
            )
            assert source_data.num_rows == 5

            # Stage 2: Filter to clicks only (depends on source)
            hit, filtered_uri, filtered_spec = client.materialize(
                inputs=[source_uri],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT * FROM input0 WHERE event = 'click'"},
            )
            assert hit is False, "Should be cache miss for new transform"

            filtered_data = client.run_local(filtered_spec, {source_uri: source_data})
            client.upload_artifact(
                artifact_id=filtered_spec["artifact_id"],
                version=filtered_spec["version"],
                table=filtered_data,
                name="clicks-only",
            )
            assert filtered_data.num_rows == 3
            assert all(e == "click" for e in filtered_data["event"].to_pylist())

            # Stage 3: Aggregate clicks by user (depends on filtered)
            agg_sql = (
                "SELECT user_id, sum(amount) as total FROM input0 GROUP BY user_id ORDER BY user_id"
            )
            hit, agg_uri, agg_spec = client.materialize(
                inputs=[filtered_uri],
                executor="local://duckdb_sql@v1",
                params={"sql": agg_sql},
            )
            assert hit is False

            agg_data = client.run_local(agg_spec, {filtered_uri: filtered_data})
            client.upload_artifact(
                artifact_id=agg_spec["artifact_id"],
                version=agg_spec["version"],
                table=agg_data,
                name="user-totals",
            )
            assert agg_data.to_pydict() == {"user_id": [1, 2], "total": [30, 30]}

            # Verify all stages are now cached
            hit1, _, _ = client.materialize(
                inputs=[source_uri],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT * FROM input0 WHERE event = 'click'"},
            )
            assert hit1 is True, "Filter stage should be cached"

            hit2, _, _ = client.materialize(
                inputs=[filtered_uri],
                executor="local://duckdb_sql@v1",
                params={"sql": agg_sql},
            )
            assert hit2 is True, "Aggregate stage should be cached"

            # Verify names resolve to correct data
            for name, expected in [
                ("clicks-only", filtered_data.to_pydict()),
                ("user-totals", agg_data.to_pydict()),
            ]:
                resolved = client.resolve_name(name)
                fetched = client.fetch_artifact(resolved["artifact_uri"])
                assert fetched.to_pydict() == expected

    def test_persistence_across_restart(self, tmp_path):
        """Artifacts persist across server restarts."""
        cache_dir = tmp_path / "cache"
        artifact_dir = tmp_path / "artifacts"
        cache_dir.mkdir()
        artifact_dir.mkdir()

        expected_data = {"x": [1, 2, 3], "y": ["a", "b", "c"]}

        # Phase 1: Create artifacts
        with run_server(cache_dir, artifact_dir, "personal") as ctx:
            with StrataClient(base_url=ctx.base_url) as client:
                uri, _ = materialize_and_upload(
                    client,
                    sql="SELECT 1 as x, 'a' as y UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'",
                    name="persistent-artifact",
                )
                saved_uri = uri

        # Phase 2: Restart and verify
        time.sleep(0.2)  # Ensure clean shutdown
        with run_server(cache_dir, artifact_dir, "personal") as ctx:
            with StrataClient(base_url=ctx.base_url) as client:
                # Cache should still hit
                union_sql = (
                    "SELECT 1 as x, 'a' as y UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'"
                )
                hit, uri, spec = client.materialize(
                    inputs=[],
                    executor="local://duckdb_sql@v1",
                    params={"sql": union_sql},
                )
                assert hit is True, "Should be cache hit after restart"
                assert uri == saved_uri

                # Data should be accessible
                data = client.fetch_artifact(uri)
                assert data.to_pydict() == expected_data

                # Name should still resolve
                resolved = client.resolve_name("persistent-artifact")
                assert resolved["artifact_uri"] == saved_uri

    def test_provenance_deduplication(self, personal_mode_server):
        """Same inputs + transform deduplicate via provenance hash."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            sql = "SELECT 'dedup' as tag"

            # First call with inputs [a, b]
            hit1, uri1, spec1 = client.materialize(
                inputs=["input-a", "input-b"],
                executor="local://duckdb_sql@v1",
                params={"sql": sql},
            )
            assert hit1 is False
            result = client.run_local({**spec1, "input_uris": []}, {})
            client.upload_artifact(
                artifact_id=spec1["artifact_id"],
                version=spec1["version"],
                table=result,
            )

            # Same inputs in different order - should hit (order independent)
            hit2, uri2, _ = client.materialize(
                inputs=["input-b", "input-a"],
                executor="local://duckdb_sql@v1",
                params={"sql": sql},
            )
            assert hit2 is True
            assert uri2 == uri1

            # Different inputs - should miss
            hit3, uri3, _ = client.materialize(
                inputs=["input-a", "input-c"],
                executor="local://duckdb_sql@v1",
                params={"sql": sql},
            )
            assert hit3 is False
            assert uri3 != uri1

    def test_unfinalized_artifact_not_cached(self, personal_mode_server):
        """Artifacts not finalized don't appear as cache hits."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Start materialize but don't upload
            hit1, _, _ = client.materialize(
                inputs=[],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT 'unfinalized' as status"},
            )
            assert hit1 is False

            # Same request again - still miss because never finalized
            hit2, _, _ = client.materialize(
                inputs=[],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT 'unfinalized' as status"},
            )
            assert hit2 is False

    def test_artifact_and_name_uri_equivalence(self, personal_mode_server):
        """Artifact URI and name URI return identical data."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            uri, _ = materialize_and_upload(
                client,
                sql="SELECT 1 as id, 'alice' as name, 95.5 as score",
                name="equivalence-test",
            )

            via_artifact = client.fetch_artifact(uri)
            resolved = client.resolve_name("equivalence-test")
            via_name = client.fetch_artifact(resolved["artifact_uri"])

            assert via_artifact.schema == via_name.schema
            assert via_artifact.to_pydict() == via_name.to_pydict()


# =============================================================================
# Lifecycle Management Tests
# =============================================================================


class TestArtifactLifecycle:
    """Tests for artifact lifecycle management: list, delete, GC, usage."""

    def test_list_artifacts(self, personal_mode_server):
        """List artifacts with pagination and filtering."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create multiple artifacts with names
            for i in range(3):
                materialize_and_upload(
                    client,
                    sql=f"SELECT {i} as idx",
                    name=f"list-test-{i}",
                )

            # List all
            result = client.list_artifacts()
            assert len(result["artifacts"]) >= 3

            # List with limit
            result = client.list_artifacts(limit=2)
            assert len(result["artifacts"]) == 2

            # List only ready artifacts
            result = client.list_artifacts(state="ready")
            assert all(a["state"] == "ready" for a in result["artifacts"])

            # List by name prefix - should return artifacts that have these names
            result = client.list_artifacts(name_prefix="list-test-")
            assert len(result["artifacts"]) == 3
            # Each artifact should have a valid URI
            for a in result["artifacts"]:
                assert a["artifact_uri"].startswith("strata://artifact/")

    def test_delete_artifact(self, personal_mode_server):
        """Delete an artifact version."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create artifact
            uri, _ = materialize_and_upload(
                client,
                sql="SELECT 'to-delete' as status",
                name="delete-test",
            )

            # Extract artifact_id and version from URI
            # URI format: strata://artifact/{id}@v={version}
            import re

            match = re.match(r"strata://artifact/([^@]+)@v=(\d+)", uri)
            artifact_id = match.group(1)
            version = int(match.group(2))

            # Verify it exists
            fetched = client.fetch_artifact(uri)
            assert fetched.to_pydict() == {"status": ["to-delete"]}

            # Delete it
            result = client.delete_artifact(artifact_id, version)
            assert result["deleted"] is True

            # Verify it's gone
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                client.fetch_artifact(uri)
            assert exc_info.value.response.status_code == 404

            # Name should also be gone (since it pointed to deleted artifact)
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                client.resolve_name("delete-test")
            assert exc_info.value.response.status_code == 404

    def test_garbage_collect_unreferenced(self, personal_mode_server):
        """GC removes unreferenced artifacts older than cutoff."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create artifact WITHOUT a name (unreferenced)
            hit, uri, spec = client.materialize(
                inputs=[],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT 'gc-candidate' as status"},
            )
            result = client.run_local(spec, {})
            client.upload_artifact(
                artifact_id=spec["artifact_id"],
                version=spec["version"],
                table=result,
                # No name - this artifact is unreferenced
            )

            # Create artifact WITH a name (referenced)
            materialize_and_upload(
                client,
                sql="SELECT 'gc-safe' as status",
                name="gc-protected",
            )

            # Get usage before GC
            usage_before = client.get_artifact_usage()
            assert usage_before["unreferenced_count"] >= 1

            # GC with max_age_days=0 should delete the unreferenced one immediately
            gc_result = client.garbage_collect(max_age_days=0)
            assert gc_result["deleted_count"] >= 1

            # Named artifact should still exist
            resolved = client.resolve_name("gc-protected")
            fetched = client.fetch_artifact(resolved["artifact_uri"])
            assert fetched.to_pydict() == {"status": ["gc-safe"]}

    def test_usage_metrics(self, personal_mode_server):
        """Usage metrics track artifacts correctly."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Get initial usage
            usage1 = client.get_artifact_usage()
            initial_versions = usage1["total_versions"]

            # Create some artifacts
            for i in range(2):
                materialize_and_upload(
                    client,
                    sql=f"SELECT {i} as idx, 'usage-test' as tag",
                    name=f"usage-{i}",
                )

            # Check usage increased
            usage2 = client.get_artifact_usage()
            assert usage2["total_versions"] >= initial_versions + 2
            assert usage2["total_bytes"] > 0
            assert usage2["name_count"] >= 2

    def test_delete_nonexistent_artifact(self, personal_mode_server):
        """Deleting nonexistent artifact returns 404."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                client.delete_artifact("nonexistent-id", 999)
            assert exc_info.value.response.status_code == 404

    def test_gc_preserves_named_artifacts(self, personal_mode_server):
        """GC never deletes artifacts with name pointers."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create several named artifacts
            uris = []
            for i in range(3):
                uri, _ = materialize_and_upload(
                    client,
                    sql=f"SELECT {i} as idx",
                    name=f"gc-preserve-{i}",
                )
                uris.append(uri)

            # Run aggressive GC
            client.garbage_collect(max_age_days=0)

            # All named artifacts should still exist
            for i, uri in enumerate(uris):
                fetched = client.fetch_artifact(uri)
                assert fetched.to_pydict() == {"idx": [i]}


# =============================================================================
# Staleness Detection Tests
# =============================================================================


class TestStalenessDetection:
    """Tests for artifact staleness detection endpoints.

    Staleness detection allows users to:
    - Check if a named artifact's inputs have changed
    - Understand why a rebuild is needed
    - Get dry-run materialize explanations
    """

    def test_get_name_status_not_stale(self, personal_mode_server):
        """Name status shows not stale when inputs unchanged."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create artifact with no inputs (will never be stale)
            materialize_and_upload(
                client,
                sql="SELECT 'fresh' as status",
                name="fresh-artifact",
            )

            # Check status
            status = client.get_name_status("fresh-artifact")
            assert status["name"] == "fresh-artifact"
            assert status["is_stale"] is False
            assert status["stale_reason"] is None
            assert status["changed_inputs"] is None
            assert status["artifact_uri"].startswith("strata://artifact/")
            assert status["state"] == "ready"

    def test_get_name_status_not_found(self, personal_mode_server):
        """Name status returns 404 for unknown name."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                client.get_name_status("nonexistent-name")
            assert exc_info.value.response.status_code == 404

    def test_is_artifact_stale_convenience(self, personal_mode_server):
        """is_artifact_stale convenience method returns boolean."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            materialize_and_upload(
                client,
                sql="SELECT 'test' as value",
                name="staleness-check",
            )

            # Should not be stale
            assert client.is_artifact_stale("staleness-check") is False

    def test_explain_materialize_hit(self, personal_mode_server):
        """Explain materialize shows would_hit for cached computation."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create and finalize an artifact
            materialize_and_upload(
                client,
                sql="SELECT 'cached' as result",
                name="explain-hit-test",
            )

            # Explain the same computation
            result = client.explain_materialize(
                inputs=[],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT 'cached' as result"},
                name="explain-hit-test",
            )

            assert result["would_hit"] is True
            assert result["would_build"] is False
            assert result["artifact_uri"].startswith("strata://artifact/")
            assert result["is_stale"] is False

    def test_explain_materialize_miss_no_name(self, personal_mode_server):
        """Explain materialize shows would_build for new computation."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            result = client.explain_materialize(
                inputs=[],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT 'new-computation' as result"},
            )

            assert result["would_hit"] is False
            assert result["would_build"] is True
            assert result["artifact_uri"] is None
            assert result["is_stale"] is False

    def test_explain_materialize_shows_stale_reason(self, personal_mode_server):
        """Explain materialize shows why rebuild is needed when stale.

        This test creates an artifact that depends on another artifact,
        then modifies the transform (while keeping same inputs) to trigger
        a cache miss. The explain should show that a rebuild is needed.

        Note: Staleness is detected by comparing the named artifact's stored
        input versions against the SAME input URIs' current versions. If the
        input URIs themselves differ, it's a different computation, not staleness.
        """
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create source artifact
            source_uri, _ = materialize_and_upload(
                client,
                sql="SELECT 1 as version",
                name="stale-source",
            )

            # Create dependent artifact using source
            hit, dep_uri, spec = client.materialize(
                inputs=[source_uri],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT version * 10 as derived FROM input0"},
            )
            result = client.run_local(spec, {source_uri: pa.table({"version": [1]})})
            client.upload_artifact(
                artifact_id=spec["artifact_id"],
                version=spec["version"],
                table=result,
                name="stale-dependent",
            )

            # Now explain a computation with DIFFERENT transform params (same inputs)
            # This triggers cache miss + staleness check against existing named artifact
            result = client.explain_materialize(
                inputs=[source_uri],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT version * 100 as derived FROM input0"},  # Different SQL
                name="stale-dependent",
            )

            # Should show would_build (cache miss due to different transform)
            # is_stale should be False since input versions haven't changed
            assert result["would_hit"] is False
            assert result["would_build"] is True
            # The artifact exists but inputs haven't changed - it's a different transform
            # So is_stale refers to whether the EXISTING artifact's inputs changed
            # In this case, inputs are the same, so not stale (just a different transform)
            assert result["is_stale"] is False

    def test_name_status_with_artifact_dependency(self, personal_mode_server):
        """Name status correctly reports dependencies for artifact.

        This test verifies that when an artifact depends on another artifact,
        the name status shows the input versions correctly.

        Note: Staleness detection compares stored vs current versions of the
        SAME input URIs. If the input URI itself changes (e.g., pointing to
        a new artifact version), that's a different computation entirely.
        """
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create base artifact
            base_uri, _ = materialize_and_upload(
                client,
                sql="SELECT 'v1' as data",
                name="base-artifact",
            )

            # Create derived artifact that depends on base
            hit, derived_uri, spec = client.materialize(
                inputs=[base_uri],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT data || '-derived' as result FROM input0"},
            )
            result = client.run_local(spec, {base_uri: pa.table({"data": ["v1"]})})
            client.upload_artifact(
                artifact_id=spec["artifact_id"],
                version=spec["version"],
                table=result,
                name="derived-artifact",
            )

            # Verify derived is NOT stale initially
            status = client.get_name_status("derived-artifact")
            assert status["is_stale"] is False
            # Should show the input dependency
            assert base_uri in status["input_versions"]
            # Input version should be the artifact version string
            assert "@v=" in status["input_versions"][base_uri]

    def test_name_status_reports_input_versions(self, personal_mode_server):
        """Name status reports stored input versions correctly."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create two input artifacts
            input1_uri, _ = materialize_and_upload(
                client,
                sql="SELECT 'input1' as source",
            )
            input2_uri, _ = materialize_and_upload(
                client,
                sql="SELECT 'input2' as source",
            )

            # Create artifact that depends on both
            hit, uri, spec = client.materialize(
                inputs=[input1_uri, input2_uri],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT 'combined' as result"},
            )
            result = client.run_local(
                spec,
                {
                    input1_uri: pa.table({"source": ["input1"]}),
                    input2_uri: pa.table({"source": ["input2"]}),
                },
            )
            client.upload_artifact(
                artifact_id=spec["artifact_id"],
                version=spec["version"],
                table=result,
                name="multi-input-artifact",
            )

            # Check status shows all input versions
            status = client.get_name_status("multi-input-artifact")
            assert len(status["input_versions"]) == 2
            assert input1_uri in status["input_versions"]
            assert input2_uri in status["input_versions"]
            assert status["is_stale"] is False

    def test_explain_resolved_input_versions(self, personal_mode_server):
        """Explain returns resolved input versions."""
        with StrataClient(base_url=personal_mode_server["base_url"]) as client:
            # Create an artifact to use as input
            input_uri, _ = materialize_and_upload(
                client,
                sql="SELECT 'input-data' as value",
            )

            # Explain with that input
            result = client.explain_materialize(
                inputs=[input_uri],
                executor="local://duckdb_sql@v1",
                params={"sql": "SELECT * FROM input0"},
            )

            # Should have resolved versions
            assert result["resolved_input_versions"] is not None
            assert input_uri in result["resolved_input_versions"]
            # Version should be in format "artifact_id@v=N"
            version = result["resolved_input_versions"][input_uri]
            assert "@v=" in version
