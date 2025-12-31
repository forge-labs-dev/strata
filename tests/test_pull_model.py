"""Tests for pull model endpoints (Stage 2).

Tests the complete pull model flow:
1. Create a build
2. Get manifest with signed URLs
3. Download inputs via signed URL
4. Upload output via signed URL
5. Finalize build
"""

from __future__ import annotations

import io
import tempfile
from pathlib import Path
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
from fastapi.testclient import TestClient

from strata.artifact_store import ArtifactStore, get_artifact_store, reset_artifact_store
from strata.config import StrataConfig
from strata.server import app
import strata.server as server_module
from strata.transforms.build_store import (
    BuildStore,
    get_build_store,
    reset_build_store,
)
from strata.transforms.signed_urls import reset_signing_secret, set_signing_secret


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def config(temp_dir):
    """Create a test config with server transforms enabled."""
    return StrataConfig(
        cache_dir=temp_dir / "cache",
        deployment_mode="service",
        transforms_config={"enabled": True},
        artifact_dir=temp_dir / "artifacts",
        signed_url_expiry_seconds=600.0,
    )


@pytest.fixture
def artifact_store(config):
    """Create an artifact store for testing."""
    reset_artifact_store()
    store = get_artifact_store(config.artifact_dir)
    yield store
    reset_artifact_store()


@pytest.fixture
def build_store(config):
    """Create a build store for testing."""
    reset_build_store()
    db_path = config.artifact_dir / "artifacts.sqlite"
    store = get_build_store(db_path)
    yield store
    reset_build_store()


@pytest.fixture
def client(config, artifact_store, build_store):
    """Create a test client with pull model enabled."""
    # Set signing secret for reproducible tests
    set_signing_secret(b"test-secret-key-12345678901234")

    # Create mock state
    mock_state = MagicMock()
    mock_state.config = config
    mock_state.planner = MagicMock()
    mock_state.fetcher = MagicMock()
    mock_state.scans = {}
    mock_state.metrics = MagicMock()

    # Patch _state on server module
    original_state = server_module._state
    server_module._state = mock_state

    yield TestClient(app)

    # Restore
    server_module._state = original_state
    reset_signing_secret()


def create_test_arrow_blob() -> bytes:
    """Create a small Arrow IPC stream for testing."""
    schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    data = [
        pa.array([1, 2, 3], type=pa.int64()),
        pa.array(["a", "b", "c"], type=pa.string()),
    ]
    batch = pa.RecordBatch.from_arrays(data, schema=schema)

    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, schema) as writer:
        writer.write_batch(batch)

    return sink.getvalue().to_pybytes()


def create_test_artifact(artifact_store, artifact_id: str, finalize: bool = True) -> int:
    """Helper to create an artifact for testing.

    Args:
        artifact_store: The artifact store
        artifact_id: Artifact ID to create
        finalize: Whether to finalize the artifact

    Returns:
        Version number
    """
    provenance_hash = f"test-hash-{artifact_id}"
    version = artifact_store.create_artifact(
        artifact_id=artifact_id,
        provenance_hash=provenance_hash,
    )

    if finalize:
        blob = create_test_arrow_blob()
        artifact_store.write_blob(artifact_id, version, blob)
        artifact_store.finalize_artifact(artifact_id, version, "test-schema", 3, len(blob))

    return version


class TestBuildManifestEndpoint:
    """Tests for GET /v1/builds/{build_id}/manifest."""

    def test_get_manifest_for_pending_build(self, client, build_store, artifact_store):
        """Can get manifest for a pending build."""
        # Create an input artifact first
        input_version = create_test_artifact(artifact_store, "input1", finalize=True)

        # Create output artifact placeholder
        output_version = create_test_artifact(artifact_store, "output1", finalize=False)

        # Create a build with input_uris
        build = build_store.create_build(
            build_id="build-001",
            artifact_id="output1",
            version=output_version,
            executor_ref="duckdb_sql@v1",
            input_uris=[f"strata://artifact/input1@v={input_version}"],
            params={"sql": "SELECT * FROM input"},
        )

        # Get manifest
        response = client.get("/v1/builds/build-001/manifest")
        assert response.status_code == 200

        data = response.json()
        assert data["build_id"] == "build-001"
        assert data["metadata"]["artifact_id"] == "output1"
        assert data["metadata"]["executor_ref"] == "duckdb_sql@v1"
        assert len(data["inputs"]) == 1
        assert data["inputs"][0]["artifact_id"] == "input1"
        assert data["inputs"][0]["version"] == 1
        assert "url" in data["inputs"][0]
        assert "signature=" in data["inputs"][0]["url"]
        assert data["output"]["max_bytes"] > 0
        assert "url" in data["output"]
        assert "finalize" in data["finalize_url"]

    def test_get_manifest_not_found(self, client):
        """Returns 404 for non-existent build."""
        response = client.get("/v1/builds/nonexistent/manifest")
        assert response.status_code == 404

    def test_get_manifest_completed_build_rejected(self, client, build_store, artifact_store):
        """Cannot get manifest for completed build."""
        version = create_test_artifact(artifact_store, "output2", finalize=False)
        build_store.create_build(
            build_id="build-002",
            artifact_id="output2",
            version=version,
            executor_ref="duckdb_sql@v1",
        )
        # Mark as running then complete
        build_store.start_build("build-002")
        build_store.complete_build("build-002")

        response = client.get("/v1/builds/build-002/manifest")
        assert response.status_code == 400
        assert "not in pending or running state" in response.json()["detail"]


class TestDownloadEndpoint:
    """Tests for GET /v1/artifacts/download."""

    def test_download_with_valid_signature(self, client, artifact_store):
        """Can download artifact with valid signed URL."""
        from strata.transforms.signed_urls import generate_download_url

        # Create artifact
        version = create_test_artifact(artifact_store, "dl-test", finalize=True)

        # Read the blob that was written
        blob = artifact_store.read_blob("dl-test", version)

        # Generate signed URL
        signed = generate_download_url(
            base_url="http://testserver",
            artifact_id="dl-test",
            version=version,
            build_id="build-123",
            expiry_seconds=300.0,
        )

        # Extract query params
        parsed = urlparse(signed.url)
        params = parse_qs(parsed.query)

        response = client.get(
            "/v1/artifacts/download",
            params={
                "artifact_id": params["artifact_id"][0],
                "version": params["version"][0],
                "build_id": params["build_id"][0],
                "expires_at": params["expires_at"][0],
                "signature": params["signature"][0],
            },
        )

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
        assert response.content == blob

    def test_download_expired_signature_rejected(self, client, artifact_store):
        """Expired signature is rejected."""
        from strata.transforms.signed_urls import generate_download_url

        version = create_test_artifact(artifact_store, "dl-test2", finalize=True)

        # Generate expired URL
        signed = generate_download_url(
            base_url="http://testserver",
            artifact_id="dl-test2",
            version=version,
            build_id="build-123",
            expiry_seconds=-1.0,  # Already expired
        )

        parsed = urlparse(signed.url)
        params = parse_qs(parsed.query)

        response = client.get(
            "/v1/artifacts/download",
            params={
                "artifact_id": params["artifact_id"][0],
                "version": params["version"][0],
                "build_id": params["build_id"][0],
                "expires_at": params["expires_at"][0],
                "signature": params["signature"][0],
            },
        )

        assert response.status_code == 403
        assert "Invalid or expired signature" in response.json()["detail"]

    def test_download_tampered_signature_rejected(self, client, artifact_store):
        """Tampered parameters are rejected."""
        from strata.transforms.signed_urls import generate_download_url

        version = create_test_artifact(artifact_store, "dl-test3", finalize=True)

        signed = generate_download_url(
            base_url="http://testserver",
            artifact_id="dl-test3",
            version=version,
            build_id="build-123",
            expiry_seconds=300.0,
        )

        parsed = urlparse(signed.url)
        params = parse_qs(parsed.query)

        # Tamper with artifact_id
        response = client.get(
            "/v1/artifacts/download",
            params={
                "artifact_id": "different-artifact",  # Tampered!
                "version": params["version"][0],
                "build_id": params["build_id"][0],
                "expires_at": params["expires_at"][0],
                "signature": params["signature"][0],
            },
        )

        assert response.status_code == 403


class TestUploadEndpoint:
    """Tests for POST /v1/artifacts/upload."""

    def test_upload_with_valid_signature(self, client, build_store, artifact_store):
        """Can upload artifact with valid signed URL."""
        from strata.transforms.signed_urls import generate_upload_url

        # Create build
        version = create_test_artifact(artifact_store, "up-output", finalize=False)
        build_store.create_build(
            build_id="up-build-001",
            artifact_id="up-output",
            version=version,
            executor_ref="test@v1",
        )

        # Generate signed upload URL
        blob = create_test_arrow_blob()
        signed = generate_upload_url(
            base_url="http://testserver",
            build_id="up-build-001",
            max_bytes=len(blob) + 1000,
            expiry_seconds=300.0,
        )

        parsed = urlparse(signed.url)
        params = parse_qs(parsed.query)

        response = client.post(
            "/v1/artifacts/upload",
            params={
                "build_id": params["build_id"][0],
                "max_bytes": params["max_bytes"][0],
                "expires_at": params["expires_at"][0],
                "signature": params["signature"][0],
            },
            content=blob,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "uploaded"
        assert data["byte_size"] == len(blob)

        # Verify blob was written
        stored_blob = artifact_store.read_blob("up-output", version)
        assert stored_blob == blob

    def test_upload_exceeds_max_bytes_rejected(self, client, build_store, artifact_store):
        """Upload exceeding max_bytes is rejected."""
        from strata.transforms.signed_urls import generate_upload_url

        version = create_test_artifact(artifact_store, "up-output2", finalize=False)
        build_store.create_build(
            build_id="up-build-002",
            artifact_id="up-output2",
            version=version,
            executor_ref="test@v1",
        )

        blob = create_test_arrow_blob()
        signed = generate_upload_url(
            base_url="http://testserver",
            build_id="up-build-002",
            max_bytes=10,  # Very small limit
            expiry_seconds=300.0,
        )

        parsed = urlparse(signed.url)
        params = parse_qs(parsed.query)

        response = client.post(
            "/v1/artifacts/upload",
            params={
                "build_id": params["build_id"][0],
                "max_bytes": params["max_bytes"][0],
                "expires_at": params["expires_at"][0],
                "signature": params["signature"][0],
            },
            content=blob,
        )

        assert response.status_code == 413
        assert "exceeds maximum size" in response.json()["detail"]

    def test_upload_expired_signature_rejected(self, client, build_store, artifact_store):
        """Expired upload signature is rejected."""
        from strata.transforms.signed_urls import generate_upload_url

        version = create_test_artifact(artifact_store, "up-output3", finalize=False)
        build_store.create_build(
            build_id="up-build-003",
            artifact_id="up-output3",
            version=version,
            executor_ref="test@v1",
        )

        signed = generate_upload_url(
            base_url="http://testserver",
            build_id="up-build-003",
            max_bytes=10000,
            expiry_seconds=-1.0,  # Expired
        )

        parsed = urlparse(signed.url)
        params = parse_qs(parsed.query)

        response = client.post(
            "/v1/artifacts/upload",
            params={
                "build_id": params["build_id"][0],
                "max_bytes": params["max_bytes"][0],
                "expires_at": params["expires_at"][0],
                "signature": params["signature"][0],
            },
            content=b"test data",
        )

        assert response.status_code == 403


class TestFinalizeEndpoint:
    """Tests for POST /v1/builds/{build_id}/finalize."""

    def test_finalize_after_upload(self, client, build_store, artifact_store):
        """Can finalize a build after uploading blob."""
        # Create artifact and build
        version = create_test_artifact(artifact_store, "fin-output", finalize=False)
        build_store.create_build(
            build_id="fin-build-001",
            artifact_id="fin-output",
            version=version,
            executor_ref="test@v1",
            name="my-result",
        )

        # Upload blob
        blob = create_test_arrow_blob()
        artifact_store.write_blob("fin-output", version, blob)

        # Finalize
        response = client.post("/v1/builds/fin-build-001/finalize")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "finalized"
        assert data["build_id"] == "fin-build-001"
        assert f"fin-output@v={version}" in data["artifact_uri"]
        assert data["name_uri"] == "strata://name/my-result"
        assert data["row_count"] == 3  # Our test blob has 3 rows

        # Verify build is complete
        build = build_store.get_build("fin-build-001")
        assert build.state == "ready"

        # Verify artifact is ready
        artifact = artifact_store.get_artifact("fin-output", version)
        assert artifact.state == "ready"

    def test_finalize_without_upload_rejected(self, client, build_store, artifact_store):
        """Cannot finalize without uploading blob first."""
        version = create_test_artifact(artifact_store, "fin-output2", finalize=False)
        build_store.create_build(
            build_id="fin-build-002",
            artifact_id="fin-output2",
            version=version,
            executor_ref="test@v1",
        )

        response = client.post("/v1/builds/fin-build-002/finalize")
        assert response.status_code == 400
        assert "Blob not uploaded" in response.json()["detail"]

    def test_finalize_already_complete_rejected(self, client, build_store, artifact_store):
        """Cannot finalize an already complete build."""
        version = create_test_artifact(artifact_store, "fin-output3", finalize=False)
        build_store.create_build(
            build_id="fin-build-003",
            artifact_id="fin-output3",
            version=version,
            executor_ref="test@v1",
        )

        # Complete the build
        build_store.start_build("fin-build-003")
        build_store.complete_build("fin-build-003")

        response = client.post("/v1/builds/fin-build-003/finalize")
        assert response.status_code == 400
        assert "not in pending or running state" in response.json()["detail"]

    def test_finalize_invalid_arrow_fails_build(self, client, build_store, artifact_store):
        """Finalizing with invalid Arrow data marks build as failed."""
        version = create_test_artifact(artifact_store, "fin-output4", finalize=False)
        build_store.create_build(
            build_id="fin-build-004",
            artifact_id="fin-output4",
            version=version,
            executor_ref="test@v1",
        )

        # Write invalid Arrow data
        artifact_store.write_blob("fin-output4", version, b"not valid arrow data")

        response = client.post("/v1/builds/fin-build-004/finalize")
        assert response.status_code == 400
        assert "Invalid Arrow IPC format" in response.json()["detail"]

        # Build should be marked as failed
        build = build_store.get_build("fin-build-004")
        assert build.state == "failed"
        assert build.error_code == "INVALID_ARROW_FORMAT"


class TestPullModelEndToEnd:
    """End-to-end test of the complete pull model flow."""

    def test_complete_pull_model_flow(self, client, build_store, artifact_store):
        """Test the complete pull model workflow."""
        # Step 1: Create input artifact
        input_version = create_test_artifact(artifact_store, "e2e-input", finalize=True)
        input_blob = artifact_store.read_blob("e2e-input", input_version)

        # Step 2: Create build with input_uris
        output_version = create_test_artifact(artifact_store, "e2e-output", finalize=False)
        build_store.create_build(
            build_id="e2e-build-001",
            artifact_id="e2e-output",
            version=output_version,
            executor_ref="test@v1",
            input_uris=[f"strata://artifact/e2e-input@v={input_version}"],
            params={"query": "SELECT * FROM input"},
            name="e2e-result",
        )

        # Step 3: Get manifest
        response = client.get("/v1/builds/e2e-build-001/manifest")
        assert response.status_code == 200
        manifest = response.json()

        # Step 4: Download input using signed URL from manifest
        input_url = manifest["inputs"][0]["url"]
        parsed = urlparse(input_url)
        params = parse_qs(parsed.query)
        response = client.get("/v1/artifacts/download", params={k: v[0] for k, v in params.items()})
        assert response.status_code == 200
        assert response.content == input_blob

        # Step 5: "Execute" transform (just use the same blob for testing)
        output_blob = create_test_arrow_blob()

        # Step 6: Upload output using signed URL from manifest
        output_url = manifest["output"]["url"]
        parsed = urlparse(output_url)
        params = parse_qs(parsed.query)
        response = client.post(
            "/v1/artifacts/upload",
            params={k: v[0] for k, v in params.items()},
            content=output_blob,
        )
        assert response.status_code == 200

        # Step 7: Finalize build
        response = client.post(manifest["finalize_url"].replace("http://testserver", ""))
        assert response.status_code == 200
        result = response.json()
        assert result["status"] == "finalized"
        assert result["name_uri"] == "strata://name/e2e-result"

        # Verify final state
        build = build_store.get_build("e2e-build-001")
        assert build.state == "ready"

        artifact = artifact_store.get_artifact("e2e-output", output_version)
        assert artifact.state == "ready"

        # Verify name pointer was set
        name_info = artifact_store.get_name("e2e-result")
        assert name_info is not None
        assert name_info.artifact_id == "e2e-output"
        assert name_info.version == output_version
