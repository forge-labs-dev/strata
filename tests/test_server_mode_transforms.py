"""Tests for server-mode transforms (async materialize + build polling)."""

import pytest
from fastapi.testclient import TestClient

from strata.artifact_store import get_artifact_store, reset_artifact_store
from strata.config import StrataConfig
from strata.transforms.build_qos import TenantQuotaExceededError
from strata.transforms.build_store import get_build_store, reset_build_store
from strata.transforms.registry import (
    TransformRegistry,
    reset_transform_registry,
    set_transform_registry,
)


@pytest.fixture
def server_mode_config(tmp_path):
    """Create config for server mode with transforms enabled."""
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()

    return StrataConfig(
        host="127.0.0.1",
        port=8765,
        deployment_mode="service",  # Server mode
        cache_dir=tmp_path / "cache",
        artifact_dir=artifact_dir,
        transforms_config={
            "enabled": True,
            "registry": [
                {
                    "ref": "duckdb_sql@v1",
                    "executor_url": "http://executor:8080/execute",
                    "timeout_seconds": 300,
                },
                {
                    "ref": "allowed_transform@*",
                    "executor_url": "http://allowed:8080/execute",
                },
            ],
        },
    )


@pytest.fixture
def personal_mode_config(tmp_path):
    """Create config for personal mode (client-side execution)."""
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir()

    return StrataConfig(
        host="127.0.0.1",
        port=8765,
        deployment_mode="personal",
        cache_dir=tmp_path / "cache",
        artifact_dir=artifact_dir,
    )


@pytest.fixture
def server_mode_app(server_mode_config):
    """Create test app with server mode config."""
    import strata.server as server_module
    from strata.server import app

    # Reset singletons
    reset_artifact_store()
    reset_transform_registry()
    reset_build_store()

    # Initialize transform registry
    transform_registry = TransformRegistry.from_config(server_mode_config.transforms_config)
    set_transform_registry(transform_registry)

    # Initialize artifact store
    get_artifact_store(server_mode_config.artifact_dir)

    # Initialize build store
    db_path = server_mode_config.artifact_dir / "artifacts.sqlite"
    get_build_store(db_path)

    # Set up server state with mock planner
    from unittest.mock import MagicMock

    mock_state = MagicMock()
    mock_state.config = server_mode_config
    mock_state.planner = MagicMock()
    mock_state.fetcher = MagicMock()
    mock_state.scans = {}
    mock_state.metrics = MagicMock()

    # Patch get_state
    original_state = server_module._state
    server_module._state = mock_state

    yield TestClient(app)

    # Restore
    server_module._state = original_state
    reset_artifact_store()
    reset_transform_registry()
    reset_build_store()


@pytest.fixture
def personal_mode_app(personal_mode_config):
    """Create test app with personal mode config."""
    import strata.server as server_module
    from strata.server import app

    # Reset singletons
    reset_artifact_store()
    reset_transform_registry()
    reset_build_store()

    # Initialize artifact store
    get_artifact_store(personal_mode_config.artifact_dir)

    # Set up server state with mock planner
    from unittest.mock import MagicMock

    mock_state = MagicMock()
    mock_state.config = personal_mode_config
    mock_state.planner = MagicMock()
    mock_state.fetcher = MagicMock()
    mock_state.scans = {}
    mock_state.metrics = MagicMock()

    # Patch get_state
    original_state = server_module._state
    server_module._state = mock_state

    yield TestClient(app)

    # Restore
    server_module._state = original_state
    reset_artifact_store()
    reset_transform_registry()
    reset_build_store()


class TestTransformValidation:
    """Tests for transform allowlist validation in server mode."""

    def test_allowed_transform_succeeds(self, server_mode_app):
        """Materialize with registered transform succeeds."""
        response = server_mode_app.post(
            "/v1/artifacts/materialize",
            json={
                "inputs": ["file:///fake/table"],
                "transform": {
                    "executor": "local://duckdb_sql@v1",
                    "params": {"sql": "SELECT * FROM input"},
                },
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["hit"] is False
        assert data["build_id"] is not None
        assert data["state"] == "pending"
        # In server mode, no build_spec (server executes)
        assert data["build_spec"] is None

    def test_unregistered_transform_rejected(self, server_mode_app):
        """Materialize with unregistered transform returns 403."""
        response = server_mode_app.post(
            "/v1/artifacts/materialize",
            json={
                "inputs": ["file:///fake/table"],
                "transform": {
                    "executor": "local://unknown_executor@v1",
                    "params": {},
                },
            },
        )

        assert response.status_code == 403
        data = response.json()
        assert data["detail"]["error"] == "transform_not_allowed"
        assert "unknown_executor" in data["detail"]["message"]

    def test_wildcard_version_matches(self, server_mode_app):
        """Wildcard version in registry matches any version."""
        response = server_mode_app.post(
            "/v1/artifacts/materialize",
            json={
                "inputs": ["file:///fake/table"],
                "transform": {
                    "executor": "local://allowed_transform@v99",
                    "params": {},
                },
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["build_id"] is not None

    def test_personal_mode_allows_any_transform(self, personal_mode_app):
        """Personal mode allows any transform (no validation)."""
        response = personal_mode_app.post(
            "/v1/artifacts/materialize",
            json={
                "inputs": ["file:///fake/table"],
                "transform": {
                    "executor": "local://any_executor@v1",
                    "params": {"sql": "SELECT 1"},
                },
            },
        )

        assert response.status_code == 200
        data = response.json()
        # In personal mode, build_spec is returned (client executes)
        assert data["build_spec"] is not None
        assert data["build_id"] is None

    @pytest.mark.asyncio
    async def test_server_mode_materialize_respects_quota_estimate(
        self, server_mode_config, monkeypatch
    ):
        """Quota checks should use a real output estimate, not zero."""
        from unittest.mock import MagicMock

        import strata.server as server_module
        from strata.server import materialize_artifact
        from strata.types import MaterializeRequest

        reset_artifact_store()
        reset_transform_registry()
        reset_build_store()

        transform_registry = TransformRegistry.from_config(server_mode_config.transforms_config)
        set_transform_registry(transform_registry)
        get_artifact_store(server_mode_config.artifact_dir)
        get_build_store(server_mode_config.artifact_dir / "artifacts.sqlite")

        mock_state = MagicMock()
        mock_state.config = server_mode_config
        mock_state.planner = MagicMock()
        mock_state.fetcher = MagicMock()
        mock_state.scans = {}
        mock_state.metrics = MagicMock()

        original_state = server_module._state
        server_module._state = mock_state
        captured: dict[str, object] = {}

        class FakeQoS:
            def classify_build(
                self,
                estimated_output_bytes=None,
                input_count=0,
                explicit_priority=None,
            ):
                captured["classified_estimated_bytes"] = estimated_output_bytes
                captured["classified_input_count"] = input_count
                return "interactive"

            async def check_quota(self, tenant_id, estimated_bytes):
                captured["quota_tenant_id"] = tenant_id
                captured["quota_estimated_bytes"] = estimated_bytes
                raise TenantQuotaExceededError(
                    tenant_id=tenant_id,
                    used_bytes=0,
                    limit_bytes=1,
                    reset_in_seconds=60.0,
                )

            async def acquire(self, tenant_id, priority):
                captured["acquired"] = (tenant_id, priority)
                raise AssertionError("quota rejection should happen before acquire")

        qos = FakeQoS()
        monkeypatch.setattr("strata.transforms.build_qos.get_build_qos", lambda: qos)

        try:
            response = await materialize_artifact(
                MaterializeRequest.model_validate(
                    {
                        "inputs": ["file:///fake/table"],
                        "transform": {
                            "executor": "local://duckdb_sql@v1",
                            "params": {"sql": "SELECT * FROM input"},
                        },
                    }
                )
            )

            assert response.status_code == 429
            assert response.body
            assert b"quota_exceeded" in response.body
            assert captured["quota_tenant_id"] == "__default__"
            assert (
                captured["quota_estimated_bytes"]
                == server_mode_config.build_runner_default_max_output
            )
        finally:
            server_module._state = original_state
            reset_artifact_store()
            reset_transform_registry()
            reset_build_store()


class TestAsyncBuildFlow:
    """Tests for async build flow in server mode."""

    def test_materialize_returns_build_id(self, server_mode_app):
        """Materialize in server mode returns build_id for polling."""
        response = server_mode_app.post(
            "/v1/artifacts/materialize",
            json={
                "inputs": ["file:///fake/table"],
                "transform": {
                    "executor": "duckdb_sql@v1",
                    "params": {"sql": "SELECT * FROM t"},
                },
            },
        )

        assert response.status_code == 200
        data = response.json()

        assert data["hit"] is False
        assert data["build_id"] is not None
        assert data["state"] == "pending"
        assert data["artifact_uri"].startswith("strata://artifact/")

    def test_poll_build_status(self, server_mode_app):
        """Can poll build status using build_id."""
        # Create a build
        create_resp = server_mode_app.post(
            "/v1/artifacts/materialize",
            json={
                "inputs": ["file:///fake/table"],
                "transform": {
                    "executor": "duckdb_sql@v1",
                    "params": {},
                },
            },
        )

        build_id = create_resp.json()["build_id"]

        # Poll status
        status_resp = server_mode_app.get(f"/v1/artifacts/builds/{build_id}")

        assert status_resp.status_code == 200
        data = status_resp.json()

        assert data["build_id"] == build_id
        assert data["state"] == "pending"
        assert data["executor_ref"] == "duckdb_sql@v1"
        assert data["created_at"] > 0

    def test_poll_nonexistent_build(self, server_mode_app):
        """Polling nonexistent build returns 404."""
        response = server_mode_app.get("/v1/artifacts/builds/nonexistent-id")

        assert response.status_code == 404

    def test_build_polling_disabled_in_personal_mode(self, personal_mode_app):
        """Build polling is disabled in personal mode."""
        response = personal_mode_app.get("/v1/artifacts/builds/some-id")

        assert response.status_code == 404
        assert "server mode" in response.json()["detail"]


class TestProvenanceDeduplication:
    """Tests for provenance-based deduplication in server mode."""

    def test_same_inputs_same_provenance(self, server_mode_app):
        """Same inputs + transform should have same provenance."""
        # First materialize
        resp1 = server_mode_app.post(
            "/v1/artifacts/materialize",
            json={
                "inputs": ["file:///fake/table"],
                "transform": {
                    "executor": "duckdb_sql@v1",
                    "params": {"sql": "SELECT * FROM t"},
                },
            },
        )

        artifact_uri = resp1.json()["artifact_uri"]

        # Simulate build completion by directly updating the artifact store
        from strata.artifact_store import get_artifact_store

        store = get_artifact_store()

        # Parse artifact_id and version from URI
        import re

        match = re.match(r"strata://artifact/([^@]+)@v=(\d+)", artifact_uri)
        artifact_id = match.group(1)
        version = int(match.group(2))

        # Write dummy blob and finalize
        store.write_blob(artifact_id, version, b"dummy data")
        store.finalize_artifact(artifact_id, version, "{}", 10, 10)

        # Second materialize with same inputs
        resp2 = server_mode_app.post(
            "/v1/artifacts/materialize",
            json={
                "inputs": ["file:///fake/table"],
                "transform": {
                    "executor": "duckdb_sql@v1",
                    "params": {"sql": "SELECT * FROM t"},
                },
            },
        )

        # Should be a cache hit
        data2 = resp2.json()
        assert data2["hit"] is True
        assert data2["artifact_uri"] == artifact_uri
        assert data2["state"] == "ready"


class TestServerModeConfig:
    """Tests for server-mode configuration."""

    def test_server_transforms_enabled_property(self):
        """server_transforms_enabled returns True with proper config."""
        config = StrataConfig(
            deployment_mode="service",
            transforms_config={"enabled": True},
        )
        assert config.server_transforms_enabled is True

    def test_server_transforms_disabled_by_default(self):
        """server_transforms_enabled is False by default."""
        config = StrataConfig(deployment_mode="service")
        assert config.server_transforms_enabled is False

    def test_server_transforms_disabled_in_personal_mode(self):
        """server_transforms_enabled is False in personal mode."""
        config = StrataConfig(
            deployment_mode="personal",
            transforms_config={"enabled": True},
        )
        assert config.server_transforms_enabled is False

    def test_transform_registry_from_config(self):
        """TransformRegistry.from_config parses properly."""
        config = {
            "enabled": True,
            "registry": [
                {
                    "ref": "duckdb_sql@v1",
                    "executor_url": "http://exec:8080",
                    "timeout_seconds": 600,
                    "max_output_bytes": 1024000,
                },
            ],
        }

        registry = TransformRegistry.from_config(config)

        assert registry.enabled is True
        assert len(registry.definitions) == 1

        defn = registry.definitions[0]
        assert defn.ref == "duckdb_sql@v1"
        assert defn.executor_url == "http://exec:8080"
        assert defn.timeout_seconds == 600
        assert defn.max_output_bytes == 1024000


class TestMixedModeScenarios:
    """Tests for mixed scenarios (e.g., server mode with auth)."""

    def test_materialize_without_transforms_enabled(self, tmp_path):
        """Materialize in service mode without transforms returns 403."""
        from unittest.mock import MagicMock

        import strata.server as server_module
        from strata.server import app

        # Reset singletons
        reset_artifact_store()
        reset_transform_registry()
        reset_build_store()

        # Config: service mode, no transforms
        config = StrataConfig(
            deployment_mode="service",
            cache_dir=tmp_path / "cache",
            artifact_dir=tmp_path / "artifacts",
            # transforms_config not enabled
        )
        (tmp_path / "artifacts").mkdir()

        mock_state = MagicMock()
        mock_state.config = config

        original_state = server_module._state
        server_module._state = mock_state

        client = TestClient(app)

        try:
            response = client.post(
                "/v1/artifacts/materialize",
                json={
                    "inputs": ["file:///fake/table"],
                    "transform": {"executor": "duckdb_sql@v1", "params": {}},
                },
            )

            assert response.status_code == 403
            assert response.json()["detail"]["error"] == "writes_disabled"
        finally:
            server_module._state = original_state
            reset_artifact_store()
            reset_transform_registry()
            reset_build_store()
