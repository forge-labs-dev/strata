"""Tests for the PUT /v1/artifacts (put_json) endpoint.

These tests verify:
1. Direct JSON upload with provenance tracking
2. Cache hit on duplicate request
3. Lineage tracking via input_versions
4. Name assignment
5. JSON retrieval via get_json
"""

import socket
import threading
import time

import pytest
import requests
import uvicorn

from strata.client import StrataClient
from strata.config import StrataConfig


@pytest.fixture
def server_with_artifacts(tmp_path):
    """Start a server in personal mode for artifact testing."""
    # Reset global state
    from strata.artifact_store import reset_artifact_store

    reset_artifact_store()

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    config = StrataConfig(
        host="127.0.0.1",
        port=port,
        cache_dir=tmp_path / "cache",
        artifact_dir=tmp_path / "artifacts",
        deployment_mode="personal",
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
    time.sleep(1)

    base_url = f"http://127.0.0.1:{port}"
    yield {"base_url": base_url, "config": config}

    # Cleanup
    reset_artifact_store()


class TestPutJson:
    """Tests for PUT /v1/artifacts endpoint."""

    def test_put_json_basic(self, server_with_artifacts):
        """Test basic JSON upload."""
        base_url = server_with_artifacts["base_url"]

        response = requests.put(
            f"{base_url}/v1/artifacts",
            json={
                "inputs": [],
                "transform": {
                    "executor": "test@v1",
                    "params": {"step": "PROPOSE"},
                },
                "data": {"proposal": "test proposal", "claims": ["claim1", "claim2"]},
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["artifact_uri"].startswith("strata://artifact/")
        assert data["hit"] is False
        assert data["byte_size"] > 0

    def test_put_json_cache_hit(self, server_with_artifacts):
        """Test that duplicate request returns cache hit."""
        base_url = server_with_artifacts["base_url"]

        request_body = {
            "inputs": [],
            "transform": {
                "executor": "delibera@v1",
                "params": {
                    "operation": "WORK",
                    "step_id": "PROPOSE",
                    "code_hash": "sha256:abc123",
                },
            },
            "data": {"result": "computation output"},
        }

        # First request - should not be a hit
        response1 = requests.put(f"{base_url}/v1/artifacts", json=request_body)
        assert response1.status_code == 200
        data1 = response1.json()
        assert data1["hit"] is False

        # Second request - should be a cache hit
        response2 = requests.put(f"{base_url}/v1/artifacts", json=request_body)
        assert response2.status_code == 200
        data2 = response2.json()
        assert data2["hit"] is True
        assert data2["artifact_uri"] == data1["artifact_uri"]

    def test_put_json_with_name(self, server_with_artifacts):
        """Test JSON upload with name assignment."""
        base_url = server_with_artifacts["base_url"]

        response = requests.put(
            f"{base_url}/v1/artifacts",
            json={
                "inputs": [],
                "transform": {"executor": "test@v1", "params": {}},
                "data": {"value": 42},
                "name": "my_artifact",
            },
        )

        assert response.status_code == 200
        data = response.json()
        # Name should be set (even if resolve fails, the response should include it)
        assert data["name_uri"] == "strata://name/my_artifact"
        assert data["artifact_uri"].startswith("strata://artifact/")

    def test_put_json_with_inputs(self, server_with_artifacts):
        """Test JSON upload with input references for lineage."""
        base_url = server_with_artifacts["base_url"]

        # First create a parent artifact
        parent_response = requests.put(
            f"{base_url}/v1/artifacts",
            json={
                "inputs": [],
                "transform": {"executor": "parent@v1", "params": {}},
                "data": {"parent_data": "value"},
            },
        )
        assert parent_response.status_code == 200
        parent_uri = parent_response.json()["artifact_uri"]

        # Create child artifact with parent as input
        child_response = requests.put(
            f"{base_url}/v1/artifacts",
            json={
                "inputs": [parent_uri],
                "transform": {"executor": "child@v1", "params": {}},
                "data": {"child_data": "derived"},
            },
        )
        assert child_response.status_code == 200
        child_uri = child_response.json()["artifact_uri"]

        # Verify both artifacts are accessible
        assert parent_uri.startswith("strata://artifact/")
        assert child_uri.startswith("strata://artifact/")
        assert parent_uri != child_uri  # Different artifacts


class TestPutJsonClient:
    """Tests for client.put_json() method."""

    def test_client_put_json(self, server_with_artifacts):
        """Test client.put_json() method."""
        base_url = server_with_artifacts["base_url"]
        client = StrataClient(base_url=base_url)

        try:
            artifact = client.put_json(
                inputs=[],
                transform={
                    "executor": "delibera@v1",
                    "params": {
                        "operation": "WORK",
                        "step_id": "PLAN",
                        "role": "Planner",
                        "code_hash": "sha256:xyz789",
                    },
                },
                data={"branches": ["branch1", "branch2"]},
            )

            assert artifact.uri.startswith("strata://artifact/")
            assert artifact.cache_hit is False
        finally:
            client.close()

    def test_client_put_json_cache_hit(self, server_with_artifacts):
        """Test client.put_json() returns cache_hit=True on duplicate."""
        base_url = server_with_artifacts["base_url"]
        client = StrataClient(base_url=base_url)

        try:
            transform = {
                "executor": "delibera@v1",
                "params": {"step_id": "VALIDATE", "code_hash": "sha256:val123"},
            }
            data = {"validation_result": "passed"}

            # First call
            artifact1 = client.put_json(inputs=[], transform=transform, data=data)
            assert artifact1.cache_hit is False

            # Second call - should be cache hit
            artifact2 = client.put_json(inputs=[], transform=transform, data=data)
            assert artifact2.cache_hit is True
            assert artifact2.uri == artifact1.uri
        finally:
            client.close()

    def test_client_get_json(self, server_with_artifacts):
        """Test client.get_json() retrieves data correctly."""
        base_url = server_with_artifacts["base_url"]
        client = StrataClient(base_url=base_url)

        try:
            original_data = {
                "proposal": "Test proposal",
                "claims": [{"id": 1, "text": "claim1"}, {"id": 2, "text": "claim2"}],
                "metadata": {"version": 1},
            }

            artifact = client.put_json(
                inputs=[],
                transform={"executor": "test@v1", "params": {}},
                data=original_data,
            )

            # Retrieve the data
            retrieved_data = client.get_json(artifact.uri)
            assert retrieved_data == original_data
        finally:
            client.close()

    def test_client_put_json_with_name(self, server_with_artifacts):
        """Test client.put_json() with name assignment."""
        base_url = server_with_artifacts["base_url"]
        client = StrataClient(base_url=base_url)

        try:
            artifact = client.put_json(
                inputs=[],
                transform={"executor": "test@v1", "params": {}},
                data={"value": 123},
                name="test_named_artifact",
            )

            assert artifact.name == "test_named_artifact"
        finally:
            client.close()


class TestDeliberaIntegration:
    """Tests simulating Delibera-like usage patterns."""

    def test_delibera_step_persistence(self, server_with_artifacts):
        """Test Delibera-style step artifact persistence."""
        base_url = server_with_artifacts["base_url"]
        client = StrataClient(base_url=base_url)

        try:
            # 1. Persist protocol spec (one-time)
            protocol = client.put_json(
                inputs=[],
                transform={"executor": "protocol@v1", "params": {}},
                data={"version": "tree_v1", "steps": ["PLAN", "PROPOSE", "VALIDATE"]},
                name="protocol_tree_v1",
            )

            # 2. Persist constraints (per-run)
            constraints = client.put_json(
                inputs=[],
                transform={"executor": "constraints@v1", "params": {}},
                data={"risk_tolerance": 0.1, "scope": "narrow"},
                name="constraints_run_42",
            )

            # 3. Planner step (depends on protocol + constraints)
            planner_result = client.put_json(
                inputs=[protocol.uri, constraints.uri],
                transform={
                    "executor": "delibera@v1",
                    "params": {
                        "operation": "WORK",
                        "step_id": "PLAN",
                        "role": "Planner",
                        "code_hash": "sha256:planner_v1",
                    },
                },
                data={"branches": ["option_a", "option_b", "option_c"]},
            )

            # 4. Proposer step (depends on planner output)
            proposer_result = client.put_json(
                inputs=[planner_result.uri, constraints.uri],
                transform={
                    "executor": "delibera@v1",
                    "params": {
                        "operation": "WORK",
                        "step_id": "PROPOSE",
                        "role": "Proposer",
                        "code_hash": "sha256:proposer_v2",
                        "temperature": 0.1,
                    },
                },
                data={
                    "proposal": "Recommended approach...",
                    "claims": [
                        {"id": 1, "text": "Claim A", "evidence": []},
                        {"id": 2, "text": "Claim B", "evidence": []},
                    ],
                },
            )

            # Verify all artifacts exist and are readable
            assert client.get_json(protocol.uri)["version"] == "tree_v1"
            assert client.get_json(constraints.uri)["risk_tolerance"] == 0.1
            assert len(client.get_json(planner_result.uri)["branches"]) == 3
            assert "proposal" in client.get_json(proposer_result.uri)

            # Verify cache hits on replay
            replay_planner = client.put_json(
                inputs=[protocol.uri, constraints.uri],
                transform={
                    "executor": "delibera@v1",
                    "params": {
                        "operation": "WORK",
                        "step_id": "PLAN",
                        "role": "Planner",
                        "code_hash": "sha256:planner_v1",
                    },
                },
                data={"branches": ["option_a", "option_b", "option_c"]},
            )
            assert replay_planner.cache_hit is True
            assert replay_planner.uri == planner_result.uri

        finally:
            client.close()
