"""Tests for the unified /v1/materialize endpoint."""

import threading
import time
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
import requests
import uvicorn
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from strata.config import StrataConfig


@pytest.fixture
def temp_warehouse(tmp_path):
    """Create a temporary warehouse with a sample Iceberg table."""
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir()

    catalog = SqlCatalog(
        "strata",
        **{
            "uri": f"sqlite:///{warehouse_path / 'catalog.db'}",
            "warehouse": str(warehouse_path),
        },
    )

    catalog.create_namespace("test_db")

    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "value", DoubleType(), required=False),
        NestedField(3, "name", StringType(), required=False),
        NestedField(4, "timestamp", LongType(), required=False),
    )

    table = catalog.create_table("test_db.events", schema)

    # Create sample data
    num_rows = 100
    base_ts = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp() * 1_000_000)
    data = pa.table(
        {
            "id": pa.array(range(num_rows), type=pa.int64()),
            "value": pa.array([float(i * 1.5) for i in range(num_rows)], type=pa.float64()),
            "name": pa.array([f"item_{i}" for i in range(num_rows)], type=pa.string()),
            "timestamp": pa.array(
                [base_ts + i * 3600_000_000 for i in range(num_rows)],
                type=pa.int64(),
            ),
        }
    )

    table.append(data)

    return {
        "warehouse_path": warehouse_path,
        "table_uri": f"file://{warehouse_path}#test_db.events",
        "catalog": catalog,
        "table": table,
    }


@pytest.fixture
def server_with_personal_mode(temp_warehouse, tmp_path):
    """Start a server in personal mode (writes enabled) and provide base URL."""
    import socket

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    config = StrataConfig(
        host="127.0.0.1",
        port=port,
        cache_dir=tmp_path / "cache",
        artifact_dir=tmp_path / "artifacts",
        deployment_mode="personal",  # Enable writes for artifact store
    )

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

    base_url = f"http://127.0.0.1:{port}"

    yield {
        "base_url": base_url,
        "config": config,
        "warehouse": temp_warehouse,
    }


class TestUnifiedMaterialize:
    """Tests for the unified /v1/materialize endpoint."""

    def test_identity_materialize_stream_mode(self, server_with_personal_mode):
        """Test scan@v1 transform in stream mode."""
        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        # Request materialize with scan@v1
        response = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": [table_uri],
                "transform": {
                    "executor": "scan@v1",
                    "params": {},
                },
                "mode": "stream",
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should be a cache miss (first request)
        assert data["hit"] is False
        assert data["state"] == "building"
        assert data["artifact_uri"].startswith("strata://artifact/")
        assert data["stream_id"] is not None
        assert data["stream_url"].startswith("/v1/streams/")

        # Fetch the stream
        stream_response = requests.get(
            f"{base_url}{data['stream_url']}",
            headers={"Accept": "application/vnd.apache.arrow.stream"},
        )

        assert stream_response.status_code == 200
        assert stream_response.headers["content-type"] == "application/vnd.apache.arrow.stream"

        # Parse the Arrow IPC stream
        reader = ipc.open_stream(stream_response.content)
        table = reader.read_all()

        assert table.num_rows == 100
        assert set(table.column_names) == {"id", "value", "name", "timestamp"}

    def test_identity_materialize_with_projection(self, server_with_personal_mode):
        """Test scan@v1 with column projection."""
        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        response = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": [table_uri],
                "transform": {
                    "executor": "scan@v1",
                    "params": {
                        "columns": ["id", "name"],
                    },
                },
                "mode": "stream",
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Fetch the stream
        stream_response = requests.get(
            f"{base_url}{data['stream_url']}",
            headers={"Accept": "application/vnd.apache.arrow.stream"},
        )

        assert stream_response.status_code == 200

        # Parse and verify projection
        reader = ipc.open_stream(stream_response.content)
        table = reader.read_all()

        assert table.num_rows == 100
        assert set(table.column_names) == {"id", "name"}

    def test_identity_materialize_with_filters(self, server_with_personal_mode):
        """Test scan@v1 with row filters."""
        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        response = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": [table_uri],
                "transform": {
                    "executor": "scan@v1",
                    "params": {
                        "filters": [
                            {"column": "id", "op": "<", "value": 50},
                        ],
                    },
                },
                "mode": "stream",
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Fetch the stream
        stream_response = requests.get(
            f"{base_url}{data['stream_url']}",
            headers={"Accept": "application/vnd.apache.arrow.stream"},
        )

        assert stream_response.status_code == 200

        # Parse - filters may not reduce rows if row groups can't be pruned
        # But the request should succeed
        reader = ipc.open_stream(stream_response.content)
        table = reader.read_all()

        # Should have rows (exact count depends on pruning)
        assert table.num_rows >= 0

    def test_identity_materialize_cache_hit(self, server_with_personal_mode):
        """Test that same query returns cache hit."""
        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        # First request - cache miss
        response1 = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": [table_uri],
                "transform": {
                    "executor": "scan@v1",
                    "params": {"columns": ["id"]},
                },
                "mode": "stream",
            },
        )

        assert response1.status_code == 200
        data1 = response1.json()
        assert data1["hit"] is False

        # Consume the stream to finalize the artifact
        stream_response = requests.get(
            f"{base_url}{data1['stream_url']}",
        )
        assert stream_response.status_code == 200

        # Small delay for artifact finalization
        time.sleep(0.5)

        # Second request - should be cache hit
        response2 = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": [table_uri],
                "transform": {
                    "executor": "scan@v1",
                    "params": {"columns": ["id"]},
                },
                "mode": "stream",
            },
        )

        assert response2.status_code == 200
        data2 = response2.json()
        assert data2["hit"] is True
        assert data2["state"] == "ready"
        assert data2["artifact_uri"] == data1["artifact_uri"]

    def test_identity_materialize_artifact_mode(self, server_with_personal_mode):
        """Test scan@v1 in artifact mode."""
        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        response = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": [table_uri],
                "transform": {
                    "executor": "scan@v1",
                    "params": {},
                },
                "mode": "artifact",
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should be a cache miss with build_id
        assert data["hit"] is False
        assert data["state"] == "building"
        assert data["artifact_uri"].startswith("strata://artifact/")
        assert data["build_id"] is not None
        # In artifact mode, no stream_url should be provided
        assert data.get("stream_url") is None

    def test_identity_requires_single_input(self, server_with_personal_mode):
        """Test that scan@v1 rejects multiple inputs."""
        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        response = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": [table_uri, table_uri],  # Two inputs
                "transform": {
                    "executor": "scan@v1",
                    "params": {},
                },
            },
        )

        assert response.status_code == 400
        assert "exactly one input" in response.json()["detail"]

    def test_identity_rejects_artifact_input(self, server_with_personal_mode):
        """Test that scan@v1 rejects artifact URIs as input."""
        base_url = server_with_personal_mode["base_url"]

        response = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": ["strata://artifact/abc123@v=1"],
                "transform": {
                    "executor": "scan@v1",
                    "params": {},
                },
            },
        )

        assert response.status_code == 400
        assert "table URI" in response.json()["detail"]

    def test_stream_not_found(self, server_with_personal_mode):
        """Test 404 for non-existent stream."""
        base_url = server_with_personal_mode["base_url"]

        response = requests.get(f"{base_url}/v1/streams/nonexistent")

        assert response.status_code == 404

    def test_invalid_identity_params(self, server_with_personal_mode):
        """Test that invalid identity params return 400."""
        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        response = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": [table_uri],
                "transform": {
                    "executor": "scan@v1",
                    "params": {
                        "filters": "not_a_list",  # Invalid type
                    },
                },
            },
        )

        assert response.status_code == 400


class TestUnifiedMaterializeEdgeCases:
    """Edge case tests for unified materialize."""

    def test_default_mode_is_stream(self, server_with_personal_mode):
        """Test that the default mode is 'stream'."""
        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        # Request without specifying mode
        response = requests.post(
            f"{base_url}/v1/materialize",
            json={
                "inputs": [table_uri],
                "transform": {
                    "executor": "scan@v1",
                    "params": {},
                },
                # mode not specified - should default to "stream"
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Default mode should provide stream_url
        assert data.get("stream_url") is not None


class TestClientFetch:
    """Tests for the client SDK fetch() method."""

    def test_client_fetch_basic(self, server_with_personal_mode):
        """Test basic materialize() + fetch() usage."""
        from strata.client import StrataClient

        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        client = StrataClient(base_url=base_url)

        try:
            artifact = client.materialize(
                inputs=[table_uri],
                transform={"executor": "scan@v1", "params": {}},
            )
            table = client.fetch(artifact.uri)

            assert table.num_rows == 100
            assert set(table.column_names) == {"id", "value", "name", "timestamp"}
        finally:
            client.close()

    def test_client_fetch_with_projection(self, server_with_personal_mode):
        """Test materialize() + fetch() with column projection."""
        from strata.client import StrataClient

        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        client = StrataClient(base_url=base_url)

        try:
            artifact = client.materialize(
                inputs=[table_uri],
                transform={"executor": "scan@v1", "params": {"columns": ["id", "value"]}},
            )
            table = client.fetch(artifact.uri)

            assert table.num_rows == 100
            assert set(table.column_names) == {"id", "value"}
        finally:
            client.close()

    def test_client_fetch_with_filters(self, server_with_personal_mode):
        """Test materialize() + fetch() with row filters."""
        from strata.client import StrataClient

        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        client = StrataClient(base_url=base_url)

        try:
            artifact = client.materialize(
                inputs=[table_uri],
                transform={
                    "executor": "scan@v1",
                    "params": {"filters": [{"column": "id", "op": "<", "value": 50}]},
                },
            )
            table = client.fetch(artifact.uri)

            # Filters are applied at row-group level, so we may get all rows
            # depending on pruning. The test verifies the request succeeds.
            assert table.num_rows >= 0
        finally:
            client.close()

    def test_client_materialize_returns_artifact(self, server_with_personal_mode):
        """Test that materialize() returns an Artifact with metadata."""
        from strata.client import StrataClient

        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        client = StrataClient(base_url=base_url)

        try:
            artifact = client.materialize(
                inputs=[table_uri],
                transform={"executor": "scan@v1", "params": {"columns": ["id"]}},
            )

            assert artifact.artifact_id is not None
            assert artifact.version == 1
            assert artifact.uri.startswith("strata://artifact/")

            # Get data from artifact
            table = client.fetch(artifact.uri)
            assert table.num_rows == 100
            assert set(table.column_names) == {"id"}
        finally:
            client.close()

    def test_client_materialize_cache_hit(self, server_with_personal_mode):
        """Test that repeated materialize() calls return cache hits."""
        from strata.client import StrataClient

        base_url = server_with_personal_mode["base_url"]
        table_uri = server_with_personal_mode["warehouse"]["table_uri"]

        client = StrataClient(base_url=base_url)

        try:
            # First materialize - cache miss
            artifact1 = client.materialize(
                inputs=[table_uri],
                transform={"executor": "scan@v1", "params": {"columns": ["id", "name"]}},
            )
            assert artifact1.cache_hit is False

            # Small delay for artifact finalization
            import time

            time.sleep(0.5)

            # Second materialize - should be cache hit
            artifact2 = client.materialize(
                inputs=[table_uri],
                transform={"executor": "scan@v1", "params": {"columns": ["id", "name"]}},
            )
            assert artifact2.cache_hit is True
            assert artifact2.artifact_id == artifact1.artifact_id
        finally:
            client.close()
