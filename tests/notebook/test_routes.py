"""Tests for notebook REST routes."""

import asyncio
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from strata.notebook.routes import router
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    write_cell,
)


@pytest.fixture(autouse=True)
def no_uv_sync(monkeypatch):
    """Skip real venv/pool creation — route tests only test HTTP routing."""
    monkeypatch.setattr("strata.notebook.session._uv_sync", lambda path, **kw: True)

    async def _fake_run_uv_command_streaming(*args, **kwargs):
        del args
        del kwargs
        return SimpleNamespace(success=True, error=None, operation_log=None)

    monkeypatch.setattr(
        "strata.notebook.session.run_uv_command_streaming",
        _fake_run_uv_command_streaming,
    )

    async def _noop_start(self):
        pass

    monkeypatch.setattr("strata.notebook.pool.WarmProcessPool.start", _noop_start)


# Create a test app with just the notebook router
def create_test_app():
    """Create a test FastAPI app with notebook router."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def service_mode_worker_state(monkeypatch):
    """Configure a fake server state with a service-mode worker registry."""

    def _configure(workers: list[dict] | None = None) -> None:
        monkeypatch.setattr(
            "strata.server._state",
            SimpleNamespace(
                config=SimpleNamespace(
                    deployment_mode="service",
                    transforms_config={
                        "notebook_workers": workers
                        or [
                            {
                                "name": "gpu-a100",
                                "backend": "executor",
                                "runtime_id": "cuda-12.4",
                                "config": {"url": "embedded://local"},
                            }
                        ]
                    },
                )
            ),
        )

    return _configure


@pytest.fixture
def deployment_mode_state(monkeypatch):
    """Configure a fake server state with only deployment-mode settings."""

    def _configure(mode: str) -> None:
        monkeypatch.setattr(
            "strata.server._state",
            SimpleNamespace(
                config=SimpleNamespace(
                    deployment_mode=mode,
                    transforms_config={},
                )
            ),
        )

    return _configure


def test_open_notebook():
    """Test POST /v1/notebooks/open endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create a test notebook
        notebook_dir = create_notebook(tmpdir_path, "Test Notebook")

        # Open it via API
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Test Notebook"
        assert "session_id" in data
        assert "id" in data
        assert data["default_parent_path"] == "/tmp/strata-notebooks"
        assert "environment" in data
        assert "python_version" in data["environment"]
        assert "requested_python_version" in data["environment"]
        assert "runtime_python_version" in data["environment"]
        assert "sync_state" in data["environment"]
        assert "declared_package_count" in data["environment"]
        assert "interpreter_source" in data["environment"]
        assert "last_sync_duration_ms" in data["environment"]
        assert "environment_job_history" in data
        assert "Server-Timing" in response.headers
        assert "session_open" in response.headers["Server-Timing"]


def test_open_notebook_reuses_existing_session_in_personal_mode(monkeypatch):
    """Opening the same path twice should reuse the live session in personal mode."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Reusable Notebook")
        monkeypatch.setattr(
            "strata.server._state",
            SimpleNamespace(
                config=SimpleNamespace(
                    deployment_mode="personal",
                    notebook_storage_dir=Path("/tmp/strata-notebooks"),
                    notebook_python_versions=["3.13"],
                    transforms_config={},
                )
            ),
        )

        first = client.post("/v1/notebooks/open", json={"path": str(notebook_dir)})
        second = client.post("/v1/notebooks/open", json={"path": str(notebook_dir)})

        assert first.status_code == 200
        assert second.status_code == 200
        assert first.json()["session_id"] == second.json()["session_id"]


def test_open_notebook_rehydrates_environment_job_history():
    """Opening a notebook should expose persisted recent environment jobs."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        notebook_dir = create_notebook(tmpdir_path, "Job History Notebook")
        history_path = notebook_dir / ".strata" / "environment_jobs.json"
        history_path.parent.mkdir(parents=True, exist_ok=True)
        history_path.write_text(
            json.dumps(
                [
                    {
                        "id": "job-789",
                        "action": "import",
                        "command": "uv sync",
                        "status": "completed",
                        "phase": "completed",
                        "started_at": 1234567890,
                        "finished_at": 1234567990,
                        "duration_ms": 100,
                        "stdout": "Resolved 4 packages\n",
                        "stderr": "",
                        "stdout_truncated": False,
                        "stderr_truncated": False,
                        "lockfile_changed": True,
                        "stale_cell_count": 1,
                        "stale_cell_ids": ["cell-1"],
                        "error": None,
                    }
                ]
            )
        )

        response = client.post("/v1/notebooks/open", json={"path": str(notebook_dir)})

        assert response.status_code == 200
        data = response.json()
        assert data["environment_job"]["action"] == "import"
        assert data["environment_job"]["status"] == "completed"
        assert len(data["environment_job_history"]) == 1
        assert data["environment_job_history"][0]["stale_cell_count"] == 1


def test_open_notebook_rehydrates_cached_status():
    """Opening an existing notebook should restore cached cell statuses."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        notebook_dir = create_notebook(tmpdir_path, "Rehydrate Test")
        add_cell_to_notebook(notebook_dir, "c1")
        write_cell(notebook_dir, "c1", "x = 1")
        add_cell_to_notebook(notebook_dir, "c2", after_cell_id="c1")
        write_cell(notebook_dir, "c2", "y = x + 1")

        from strata.notebook.executor import CellExecutor
        from strata.notebook.routes import get_session_manager

        session = get_session_manager().open_notebook(notebook_dir)

        async def _prime() -> None:
            executor = CellExecutor(session)
            assert (await executor.execute_cell("c1", "x = 1")).success

        asyncio.run(_prime())

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )

        assert response.status_code == 200
        cells = {cell["id"]: cell for cell in response.json()["cells"]}
        assert cells["c1"]["status"] == "ready"
        assert cells["c2"]["status"] == "idle"


def test_list_cells_includes_remote_execution_metadata(
    notebook_executor_server,
    notebook_build_server,
):
    """List-cells should retain remote execution metadata from the current session."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Remote Metadata Test")
        add_cell_to_notebook(notebook_dir, "cell-1")
        write_cell(notebook_dir, "cell-1", "x = 1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        from strata.notebook.executor import CellExecutor
        from strata.notebook.models import WorkerBackendType, WorkerSpec
        from strata.notebook.routes import get_session_manager

        notebook_build_server["config"].transforms_config["notebook_workers"] = [
            {
                "name": "gpu-http-signed",
                "backend": "executor",
                "runtime_id": "gpu-http-signed-a100",
                "config": {
                    "url": notebook_executor_server["execute_url"],
                    "transport": "signed",
                    "strata_url": notebook_build_server["base_url"],
                },
            }
        ]

        session = get_session_manager().get_session(session_id)
        assert session is not None
        session.notebook_state.workers = [
            WorkerSpec(
                name="gpu-http-signed",
                backend=WorkerBackendType.EXECUTOR,
                runtime_id="gpu-http-signed-a100",
                config={
                    "url": notebook_executor_server["execute_url"],
                    "transport": "signed",
                    "strata_url": notebook_build_server["base_url"],
                },
            )
        ]
        session.notebook_state.worker = "gpu-http-signed"
        cell = next(c for c in session.notebook_state.cells if c.id == "cell-1")
        cell.worker = "gpu-http-signed"

        async def _prime() -> None:
            executor = CellExecutor(session)
            assert (await executor.execute_cell("cell-1", "x = 1")).success

        asyncio.run(_prime())

        response = client.get(f"/v1/notebooks/{session_id}/cells")
        assert response.status_code == 200
        cell_payload = response.json()["cells"][0]
        assert cell_payload["execution_method"] == "executor"
        assert cell_payload["remote_worker"] == "gpu-http-signed"
        assert cell_payload["remote_transport"] == "signed"
        assert isinstance(cell_payload["remote_build_id"], str)
        assert cell_payload["remote_build_state"] == "ready"
        assert cell_payload["remote_error_code"] is None


def test_open_notebook_not_found():
    """Test opening a non-existent notebook."""
    client = TestClient(create_test_app())

    response = client.post(
        "/v1/notebooks/open",
        json={"path": "/nonexistent/notebook"}
    )

    assert response.status_code == 404


def test_create_notebook_endpoint():
    """Test POST /v1/notebooks/create endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        response = client.post(
            "/v1/notebooks/create",
            json={"parent_path": tmpdir, "name": "New Notebook"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "New Notebook"
        assert "session_id" in data
        assert data["default_parent_path"] == "/tmp/strata-notebooks"
        assert data["available_python_versions"]
        assert data["default_python_version"] == data["available_python_versions"][0]
        assert "python_selection_fixed" in data
        assert "environment" in data
        assert "lockfile_hash" in data["environment"]
        assert "requested_python_version" in data["environment"]
        assert "runtime_python_version" in data["environment"]
        assert "resolved_package_count" in data["environment"]
        assert "Server-Timing" in response.headers
        assert "create_notebook" in response.headers["Server-Timing"]


def test_create_notebook_endpoint_defers_initial_environment_sync(monkeypatch):
    """Fresh notebook creation should bootstrap the initial env as a background job."""
    client = TestClient(create_test_app())
    captured: dict[str, object] = {}

    def fake_create_notebook(
        parent_path,
        name,
        python_version=None,
        *,
        initialize_environment=True,
    ):
        captured["initialize_environment"] = initialize_environment
        captured["python_version"] = python_version
        return Path("/tmp/fake-notebook")

    class FakeSession:
        id = "session-123"
        path = Path("/tmp/fake-notebook")
        environment_job = None
        environment_sync_state = "pending"
        environment_sync_error = None
        environment_sync_notice = "Notebook environment is initializing."

        def serialize_notebook_state(self):
            return {
                "id": "notebook-123",
                "name": "Fast Notebook",
                "cells": [],
                "environment": {
                    "sync_state": self.environment_sync_state,
                },
                "environment_job": self.environment_job,
            }

        async def submit_environment_job(self, *, action: str, **_kwargs):
            captured["environment_job_action"] = action
            self.environment_job = {
                "id": "job-123",
                "action": action,
                "status": "running",
                "command": "uv sync",
            }
            return self.environment_job

    def fake_open_notebook(
        directory,
        *,
        skip_initial_venv_sync=False,
        defer_initial_venv_sync=False,
        timing=None,
    ):
        captured["directory"] = directory
        captured["skip_initial_venv_sync"] = skip_initial_venv_sync
        captured["defer_initial_venv_sync"] = defer_initial_venv_sync
        captured["timing"] = timing
        return FakeSession()

    monkeypatch.setattr("strata.notebook.routes.create_notebook", fake_create_notebook)
    monkeypatch.setattr("strata.notebook.routes._session_manager.open_notebook", fake_open_notebook)

    response = client.post(
        "/v1/notebooks/create",
        json={"parent_path": "/tmp/notebooks", "name": "Fast Notebook"},
    )

    assert response.status_code == 200
    data = response.json()
    assert captured["initialize_environment"] is False
    assert captured["skip_initial_venv_sync"] is False
    assert captured["defer_initial_venv_sync"] is True
    assert captured["environment_job_action"] == "sync"
    assert captured["timing"] is not None
    assert data["environment"]["sync_state"] == "pending"
    assert data["environment_job"]["action"] == "sync"
    assert data["environment_job"]["status"] == "running"


def test_create_notebook_endpoint_with_starter_cell():
    """Scratch-style create requests can return a starter empty cell."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        response = client.post(
            "/v1/notebooks/create",
            json={"parent_path": tmpdir, "name": "Scratch Notebook", "starter_cell": True},
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["cells"]) == 1
        assert data["cells"][0]["source"] == ""
        assert data["cells"][0]["language"] == "python"


def test_create_notebook_endpoint_rejects_unsupported_python_version(monkeypatch):
    """Notebook creation should validate requested Python versions against server config."""
    monkeypatch.setattr(
        "strata.server._state",
        SimpleNamespace(
            config=SimpleNamespace(
                deployment_mode="personal",
                notebook_storage_dir=Path("/tmp/strata-notebooks"),
                notebook_python_versions=["3.13"],
                transforms_config={},
            )
        ),
    )

    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        response = client.post(
            "/v1/notebooks/create",
            json={"parent_path": tmpdir, "name": "New Notebook", "python_version": "3.12"},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "Python 3.12 is not available for notebook creation"


def test_get_notebook_runtime_config_endpoint(monkeypatch):
    """The runtime config endpoint should expose the server default notebook path."""
    monkeypatch.setattr(
        "strata.server._state",
        SimpleNamespace(
            config=SimpleNamespace(
                deployment_mode="personal",
                notebook_storage_dir=Path("/srv/strata-notebooks"),
                notebook_python_versions=["3.12", "3.13"],
                transforms_config={},
            )
        ),
    )

    client = TestClient(create_test_app())
    response = client.get("/v1/notebooks/config")

    assert response.status_code == 200
    assert response.json() == {
        "deployment_mode": "personal",
        "default_parent_path": "/srv/strata-notebooks",
        "available_python_versions": ["3.12", "3.13"],
        "default_python_version": "3.12",
        "python_selection_fixed": False,
    }


def test_get_environment_status_endpoint():
    """Test GET /v1/notebooks/{id}/environment endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Environment Status Test")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.get(f"/v1/notebooks/{session_id}/environment")
        assert response.status_code == 200
        environment = response.json()["environment"]
        assert "python_version" in environment
        assert "requested_python_version" in environment
        assert "runtime_python_version" in environment
        assert "lockfile_hash" in environment
        assert "declared_package_count" in environment
        assert "resolved_package_count" in environment
        assert "sync_state" in environment
        assert "last_synced_at" in environment
        assert "interpreter_source" in environment
        assert "last_sync_duration_ms" in environment


def test_sync_environment_endpoint(monkeypatch):
    """Test POST /v1/notebooks/{id}/environment/sync endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Environment Sync Test")
        add_cell_to_notebook(notebook_dir, "cell-1")
        write_cell(notebook_dir, "cell-1", "x = 1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        from strata.notebook.models import CellStaleness, CellStatus
        from strata.notebook.routes import get_session_manager

        session = get_session_manager().get_session(session_id)
        assert session is not None

        async def _fake_sync_environment():
            session.environment_sync_state = "ready"
            session.environment_sync_error = None
            session.environment_sync_notice = "Using existing notebook venv."
            session.environment_last_synced_at = 1234567890
            session.environment_last_sync_duration_ms = 42
            session.environment_python_version = "3.13.2"
            session.environment_interpreter_source = "venv"
            return {"cell-1": CellStaleness(status=CellStatus.IDLE)}

        monkeypatch.setattr(session, "sync_environment", _fake_sync_environment)

        response = client.post(f"/v1/notebooks/{session_id}/environment/sync")
        assert response.status_code == 200
        data = response.json()
        assert data["environment"]["sync_state"] == "ready"
        assert "requested_python_version" in data["environment"]
        assert data["environment"]["runtime_python_version"] == "3.13.2"
        assert data["environment"]["python_version"] == "3.13.2"
        assert data["environment"]["sync_notice"] == "Using existing notebook venv."
        assert data["environment"]["last_sync_duration_ms"] == 42
        assert data["environment"]["interpreter_source"] == "venv"
        assert "dependencies" in data
        assert data["stale_cell_count"] == 1
        assert data["stale_cell_ids"] == ["cell-1"]
        assert "cells" in data


def test_submit_environment_job_endpoint(monkeypatch):
    """POST /environment/jobs should accept a background job and expose its snapshot."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Environment Job Test")
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        from strata.notebook.routes import get_session_manager
        from strata.notebook.session import EnvironmentJobSnapshot

        session = get_session_manager().get_session(session_id)
        assert session is not None

        async def _fake_submit_environment_job(
            *,
            action: str,
            package: str | None = None,
            requirements_text: str | None = None,
            environment_yaml_text: str | None = None,
        ):
            del requirements_text
            del environment_yaml_text
            job = EnvironmentJobSnapshot(
                id="job-123",
                action=action,
                package=package,
                command=f"uv {action} {package}".strip(),
                status="running",
                phase="uv_running",
                started_at=1234567890,
            )
            session.environment_job = job
            return job

        monkeypatch.setattr(session, "submit_environment_job", _fake_submit_environment_job)

        response = client.post(
            f"/v1/notebooks/{session_id}/environment/jobs",
            json={"action": "add", "package": "six"},
        )
        assert response.status_code == 202
        data = response.json()
        assert data["accepted"] is True
        assert data["environment_job"]["action"] == "add"
        assert data["environment_job"]["package"] == "six"
        assert data["environment_job"]["status"] == "running"


def test_submit_environment_import_job_endpoint(monkeypatch):
    """POST /environment/jobs should accept async requirements/environment imports."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Environment Import Job Test")
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        from strata.notebook.routes import get_session_manager
        from strata.notebook.session import EnvironmentJobSnapshot

        session = get_session_manager().get_session(session_id)
        assert session is not None

        captured: dict[str, str | None] = {}

        async def _fake_submit_environment_job(
            *,
            action: str,
            package: str | None = None,
            requirements_text: str | None = None,
            environment_yaml_text: str | None = None,
        ):
            captured["action"] = action
            captured["package"] = package
            captured["requirements_text"] = requirements_text
            captured["environment_yaml_text"] = environment_yaml_text
            job = EnvironmentJobSnapshot(
                id="job-456",
                action=action,
                package=package,
                command="uv sync",
                status="running",
                phase="preparing_import",
                started_at=1234567890,
            )
            session.environment_job = job
            return job

        monkeypatch.setattr(session, "submit_environment_job", _fake_submit_environment_job)

        response = client.post(
            f"/v1/notebooks/{session_id}/environment/jobs",
            json={"action": "import", "requirements": "pyarrow>=18.0.0\nsix==1.17.0\n"},
        )
        assert response.status_code == 202
        data = response.json()
        assert data["accepted"] is True
        assert data["environment_job"]["action"] == "import"
        assert data["environment_job"]["status"] == "running"
        assert captured == {
            "action": "import",
            "package": None,
            "requirements_text": "pyarrow>=18.0.0\nsix==1.17.0\n",
            "environment_yaml_text": None,
        }


def test_submit_environment_import_job_endpoint_rejects_invalid_payload():
    """Import jobs must provide exactly one import source and no package."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Environment Import Validation Test")
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.post(
            f"/v1/notebooks/{session_id}/environment/jobs",
            json={
                "action": "import",
                "requirements": "six==1.17.0\n",
                "environment_yaml": "dependencies: [six=1.17.0]\n",
            },
        )
        assert response.status_code == 400
        assert "exactly one" in response.json()["detail"]


def test_submit_environment_job_endpoint_conflict_when_execution_running():
    """Background environment jobs should be rejected while cells are running."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Environment Busy Test")
        add_cell_to_notebook(notebook_dir, "cell-1")
        write_cell(notebook_dir, "cell-1", "x = 1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        from strata.notebook.models import CellStatus
        from strata.notebook.routes import get_session_manager

        session = get_session_manager().get_session(session_id)
        assert session is not None
        session.notebook_state.cells[0].status = CellStatus.RUNNING

        response = client.post(
            f"/v1/notebooks/{session_id}/environment/jobs",
            json={"action": "sync"},
        )
        assert response.status_code == 409
        detail = response.json()["detail"]
        assert detail["code"] == "ENVIRONMENT_BUSY"


def test_list_sessions_personal_mode(deployment_mode_state):
    """Session listing should work in personal mode for reconnect UX."""
    deployment_mode_state("personal")
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Session Listing Test")
        open_response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        assert open_response.status_code == 200

        response = client.get("/v1/notebooks/sessions")
        assert response.status_code == 200
        sessions = response.json()["sessions"]
        matching = [
            session
            for session in sessions
            if session["session_id"] == open_response.json()["session_id"]
        ]
        assert len(matching) == 1
        assert matching[0]["name"] == "Session Listing Test"
        assert Path(matching[0]["path"]).resolve() == notebook_dir.resolve()


def test_get_session_personal_mode_includes_execution_metadata(deployment_mode_state):
    """Session reconnect should preserve the same serialized runtime metadata as open."""
    deployment_mode_state("personal")
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Session Metadata Test")
        add_cell_to_notebook(notebook_dir, "cell-1")
        write_cell(notebook_dir, "cell-1", "x = 1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        from strata.notebook.routes import get_session_manager

        session = get_session_manager().get_session(session_id)
        assert session is not None
        cell = next(c for c in session.notebook_state.cells if c.id == "cell-1")
        cell.execution_method = "executor"
        cell.remote_worker = "gpu-http-signed"
        cell.remote_transport = "signed"
        cell.remote_build_id = "build-123"
        cell.remote_build_state = "ready"
        cell.remote_error_code = None

        response = client.get(f"/v1/notebooks/sessions/{session_id}")
        assert response.status_code == 200
        assert "Server-Timing" in response.headers
        assert "lookup" in response.headers["Server-Timing"]
        cell_payload = response.json()["cells"][0]
        assert cell_payload["execution_method"] == "executor"
        assert cell_payload["remote_worker"] == "gpu-http-signed"
        assert cell_payload["remote_transport"] == "signed"
        assert cell_payload["remote_build_id"] == "build-123"
        assert cell_payload["remote_build_state"] == "ready"
        assert cell_payload["remote_error_code"] is None


def test_session_endpoints_blocked_in_service_mode(deployment_mode_state):
    """Session discovery/reconnect should not be exposed in service mode."""
    deployment_mode_state("service")
    client = TestClient(create_test_app())

    list_response = client.get("/v1/notebooks/sessions")
    assert list_response.status_code == 403
    assert "personal mode" in list_response.json()["detail"]

    get_response = client.get("/v1/notebooks/sessions/fake-session")
    assert get_response.status_code == 403
    assert "personal mode" in get_response.json()["detail"]


def test_list_cells():
    """Test GET /v1/notebooks/{id}/cells endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Cells Test")

        # Add cells
        cell1_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell1_id)
        write_cell(notebook_dir, cell1_id, "x = 1")

        # Open notebook to get session ID
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )
        session_id = response.json()["session_id"]

        # List cells
        response = client.get(f"/v1/notebooks/{session_id}/cells")
        assert response.status_code == 200
        data = response.json()
        assert len(data["cells"]) == 1
        assert data["cells"][0]["id"] == cell1_id
        assert data["cells"][0]["source"] == "x = 1"


def test_update_notebook_mounts():
    """Test PUT /v1/notebooks/{id}/mounts endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Mount Update Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.put(
            f"/v1/notebooks/{session_id}/mounts",
            json={
                "mounts": [
                    {
                        "name": "raw_data",
                        "uri": "s3://bucket/raw",
                        "mode": "ro",
                    }
                ]
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["mounts"][0]["name"] == "raw_data"
        assert data["cells"][0]["mounts"][0]["name"] == "raw_data"


def test_update_cell_mounts():
    """Test PUT /v1/notebooks/{id}/cells/{cell_id}/mounts endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Cell Mount Update Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.put(
            f"/v1/notebooks/{session_id}/cells/cell-1/mounts",
            json={
                "mounts": [
                    {
                        "name": "scratch",
                        "uri": "file:///tmp/scratch",
                        "mode": "rw",
                    }
                ]
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["mounts"][0]["name"] == "scratch"
        assert data["cell"]["mount_overrides"][0]["name"] == "scratch"
        assert data["cell"]["mounts"][0]["name"] == "scratch"


def test_update_notebook_worker():
    """Test PUT /v1/notebooks/{id}/worker endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Worker Update Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.put(
            f"/v1/notebooks/{session_id}/worker",
            json={"worker": "gpu-default"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["worker"] == "gpu-default"
        assert any(worker["name"] == "gpu-default" for worker in data["workers"])
        assert data["cells"][0]["worker"] == "gpu-default"


def test_update_cell_worker():
    """Test PUT /v1/notebooks/{id}/cells/{cell_id}/worker endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Cell Worker Update Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.put(
            f"/v1/notebooks/{session_id}/cells/cell-1/worker",
            json={"worker": "gpu-override"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["worker"] == "gpu-override"
        assert any(worker["name"] == "gpu-override" for worker in data["workers"])
        assert data["cell"]["worker"] == "gpu-override"
        assert data["cell"]["worker_override"] == "gpu-override"


def test_list_notebook_workers():
    """Test GET /v1/notebooks/{id}/workers endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Worker Catalog Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.get(f"/v1/notebooks/{session_id}/workers")
        assert response.status_code == 200
        data = response.json()
        assert any(worker["name"] == "local" for worker in data["workers"])
        assert data["definitions_editable"] is True
        assert isinstance(data["health_checked_at"], int)


def test_list_notebook_workers_refresh_bypasses_health_cache(monkeypatch):
    """Refreshing the worker list should bypass the short health cache."""
    import strata.notebook.routes as notebook_routes

    calls: list[bool] = []

    async def _fake_build_worker_catalog_with_health(notebook_state, *, force_refresh=False):
        calls.append(force_refresh)
        return [
            {
                "name": "local",
                "backend": "local",
                "runtime_id": None,
                "config": {},
                "source": "builtin",
                "health": "healthy",
                "allowed": True,
            }
        ]

    monkeypatch.setattr(
        notebook_routes,
        "build_worker_catalog_with_health",
        _fake_build_worker_catalog_with_health,
    )

    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Worker Refresh Test")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.get(f"/v1/notebooks/{session_id}/workers")
        assert response.status_code == 200
        assert response.json()["health_checked_at"] > 0

        response = client.get(f"/v1/notebooks/{session_id}/workers?refresh=true")
        assert response.status_code == 200
        assert response.json()["health_checked_at"] > 0

    assert calls == [False, True]


def test_list_notebook_workers_includes_health_history(monkeypatch):
    """Notebook worker catalog responses should include recent health probes."""
    import strata.notebook.routes as notebook_routes

    async def _fake_build_worker_catalog_with_health(notebook_state, *, force_refresh=False):
        del notebook_state, force_refresh
        return [
            {
                "name": "gpu-http",
                "backend": "executor",
                "runtime_id": None,
                "config": {"url": "https://executor.internal/v1/execute"},
                "source": "server",
                "health": "unavailable",
                "allowed": True,
                "enabled": True,
                "transport": "direct",
                "health_url": "https://executor.internal/health",
                "health_checked_at": 123,
                "last_error": "Health endpoint returned 503",
                "probe_count": 4,
                "healthy_probe_count": 1,
                "unavailable_probe_count": 2,
                "unknown_probe_count": 1,
                "consecutive_failures": 2,
                "last_healthy_at": 120,
                "last_unavailable_at": 123,
                "last_unknown_at": 118,
                "last_status_change_at": 123,
                "last_probe_duration_ms": 87,
                "health_history": [
                    {
                        "checked_at": 123,
                        "health": "unavailable",
                        "error": "Health endpoint returned 503",
                        "duration_ms": 87,
                    }
                ],
            }
        ]

    monkeypatch.setattr(
        notebook_routes,
        "build_worker_catalog_with_health",
        _fake_build_worker_catalog_with_health,
    )

    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Worker History Test")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.get(f"/v1/notebooks/{session_id}/workers")
        assert response.status_code == 200
        worker = response.json()["workers"][0]
        assert worker["name"] == "gpu-http"
        assert worker["health_history"] == [
            {
                "checked_at": 123,
                "health": "unavailable",
                "error": "Health endpoint returned 503",
                "duration_ms": 87,
            }
        ]
        assert worker["probe_count"] == 4
        assert worker["consecutive_failures"] == 2
        assert worker["last_healthy_at"] == 120
        assert worker["last_unavailable_at"] == 123
        assert worker["last_probe_duration_ms"] == 87


def test_list_notebook_workers_in_service_mode(service_mode_worker_state):
    """Service mode should expose a server-managed worker registry."""
    service_mode_worker_state()
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Service Worker Catalog Test")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.get(f"/v1/notebooks/{session_id}/workers")
        assert response.status_code == 200
        data = response.json()
        assert data["definitions_editable"] is False
        assert any(
            worker["name"] == "gpu-a100"
            and worker["source"] == "server"
            and worker["allowed"] is True
            for worker in data["workers"]
        )


def test_update_notebook_workers():
    """Test PUT /v1/notebooks/{id}/workers endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Worker Catalog Update Test")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.put(
            f"/v1/notebooks/{session_id}/workers",
            json={
                "workers": [
                    {
                        "name": "gpu-a100",
                        "backend": "executor",
                        "runtime_id": "cuda-12.4",
                        "config": {"url": "https://executor.internal/gpu-a100"},
                    }
                ]
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert data["configured_workers"][0]["name"] == "gpu-a100"
        assert data["configured_workers"][0]["backend"] == "executor"
        assert any(worker["name"] == "local" for worker in data["workers"])
        assert any(
            worker["name"] == "gpu-a100" and worker["health"] == "unavailable"
            for worker in data["workers"]
        )
        assert data["definitions_editable"] is True


def test_update_notebook_workers_forbidden_in_service_mode(service_mode_worker_state):
    """Notebook-scoped worker definitions should be disabled in service mode."""
    service_mode_worker_state()
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Service Worker Update Test")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.put(
            f"/v1/notebooks/{session_id}/workers",
            json={
                "workers": [
                    {
                        "name": "gpu-local",
                        "backend": "executor",
                        "config": {"url": "https://executor.internal/gpu-local"},
                    }
                ]
            },
        )
        assert response.status_code == 403
        assert "managed by the server" in response.json()["detail"]


def test_update_notebook_worker_requires_allowlisted_service_worker(
    service_mode_worker_state,
):
    """Service mode should reject worker names outside the server registry."""
    service_mode_worker_state()
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Service Worker Assignment Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        blocked = client.put(
            f"/v1/notebooks/{session_id}/worker",
            json={"worker": "gpu-shadow"},
        )
        assert blocked.status_code == 403
        assert "not allowed in service mode" in blocked.json()["detail"]

        allowed = client.put(
            f"/v1/notebooks/{session_id}/worker",
            json={"worker": "gpu-a100"},
        )
        assert allowed.status_code == 200
        payload = allowed.json()
        assert payload["worker"] == "gpu-a100"
        assert payload["definitions_editable"] is False


def test_update_notebook_worker_rejects_disabled_service_worker(
    service_mode_worker_state,
):
    """Service mode should reject server-managed workers that are disabled."""
    service_mode_worker_state(
        [
            {
                "name": "gpu-a100",
                "backend": "executor",
                "runtime_id": "cuda-12.4",
                "config": {"url": "embedded://local"},
                "enabled": False,
            }
        ]
    )
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Disabled Service Worker Assignment Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        blocked = client.put(
            f"/v1/notebooks/{session_id}/worker",
            json={"worker": "gpu-a100"},
        )
        assert blocked.status_code == 403
        assert "disabled by server policy" in blocked.json()["detail"]


def test_update_cell_worker_requires_allowlisted_service_worker(
    service_mode_worker_state,
):
    """Cell-level worker overrides should follow the same service-mode policy."""
    service_mode_worker_state()
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Service Cell Worker Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        blocked = client.put(
            f"/v1/notebooks/{session_id}/cells/cell-1/worker",
            json={"worker": "gpu-shadow"},
        )
        assert blocked.status_code == 403
        assert "not allowed in service mode" in blocked.json()["detail"]

        allowed = client.put(
            f"/v1/notebooks/{session_id}/cells/cell-1/worker",
            json={"worker": "gpu-a100"},
        )
        assert allowed.status_code == 200
        payload = allowed.json()
        assert payload["cell"]["worker"] == "gpu-a100"
        assert payload["definitions_editable"] is False


def test_update_notebook_workers_probes_executor_health(notebook_executor_server):
    """Configured notebook workers should surface healthy executor probes."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Worker Health Test")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        response = client.put(
            f"/v1/notebooks/{session_id}/workers",
            json={
                "workers": [
                    {
                        "name": "gpu-a100",
                        "backend": "executor",
                        "runtime_id": "cuda-12.4",
                        "config": {"url": notebook_executor_server["execute_url"]},
                    }
                ]
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert any(
            worker["name"] == "gpu-a100" and worker["health"] == "healthy"
            for worker in data["workers"]
        )


def test_update_notebook_timeout_and_env():
    """Test notebook-level timeout/env endpoints."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Runtime Update Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        timeout_response = client.put(
            f"/v1/notebooks/{session_id}/timeout",
            json={"timeout": 7.5},
        )
        assert timeout_response.status_code == 200
        assert timeout_response.json()["timeout"] == 7.5

        env_response = client.put(
            f"/v1/notebooks/{session_id}/env",
            json={"env": {"TOKEN": "secret"}},
        )
        assert env_response.status_code == 200
        data = env_response.json()
        assert data["env"] == {"TOKEN": "secret"}
        assert data["cells"][0]["env"] == {"TOKEN": "secret"}


def test_update_cell_timeout_and_env():
    """Test cell-level timeout/env endpoints."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Cell Runtime Update Test")
        add_cell_to_notebook(notebook_dir, "cell-1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)},
        )
        session_id = response.json()["session_id"]

        timeout_response = client.put(
            f"/v1/notebooks/{session_id}/cells/cell-1/timeout",
            json={"timeout": 2.0},
        )
        assert timeout_response.status_code == 200
        assert timeout_response.json()["timeout"] == 2.0

        env_response = client.put(
            f"/v1/notebooks/{session_id}/cells/cell-1/env",
            json={"env": {"TOKEN": "override"}},
        )
        assert env_response.status_code == 200
        data = env_response.json()
        assert data["env"] == {"TOKEN": "override"}
        assert data["cell"]["env"] == {"TOKEN": "override"}
        assert data["cell"]["env_overrides"] == {"TOKEN": "override"}


def test_update_cell_source():
    """Test PUT /v1/notebooks/{id}/cells/{cell_id} endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Update Test")

        # Add cell
        cell_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell_id)

        # Open notebook
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )
        session_id = response.json()["session_id"]

        # Update cell
        new_source = "x = 2 + 2"
        response = client.put(
            f"/v1/notebooks/{session_id}/cells/{cell_id}",
            json={"source": new_source}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["cell"]["source"] == new_source

        # Verify on disk
        cells_dir = notebook_dir / "cells"
        cell_file = cells_dir / f"{cell_id}.py"
        assert cell_file.read_text() == new_source


def test_add_cell():
    """Test POST /v1/notebooks/{id}/cells endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Add Cell Test")

        # Open notebook
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )
        session_id = response.json()["session_id"]

        # Add cell
        response = client.post(
            f"/v1/notebooks/{session_id}/cells",
            json={}
        )
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert data["source"] == ""


def test_delete_cell():
    """Test DELETE /v1/notebooks/{id}/cells/{cell_id} endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook with cell
        notebook_dir = create_notebook(tmpdir_path, "Delete Test")
        cell_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell_id)

        # Open notebook
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )
        session_id = response.json()["session_id"]

        # Delete cell
        response = client.delete(
            f"/v1/notebooks/{session_id}/cells/{cell_id}"
        )
        assert response.status_code == 200

        # Verify it's deleted
        response = client.get(f"/v1/notebooks/{session_id}/cells")
        assert len(response.json()["cells"]) == 0


def test_reorder_cells():
    """Test PUT /v1/notebooks/{id}/cells/reorder endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Reorder Test")

        # Add cells
        cell1_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell1_id)

        cell2_id = "cell-2"
        add_cell_to_notebook(notebook_dir, cell2_id)

        # Open notebook
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )
        session_id = response.json()["session_id"]

        # Reorder
        response = client.put(
            f"/v1/notebooks/{session_id}/cells/reorder",
            json={"cell_ids": [cell2_id, cell1_id]}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["cells"][0]["id"] == cell2_id
        assert data["cells"][1]["id"] == cell1_id


def test_rename_notebook():
    """Test PUT /v1/notebooks/{id}/name endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Original Name")

        # Open notebook
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )
        session_id = response.json()["session_id"]

        # Rename
        response = client.put(
            f"/v1/notebooks/{session_id}/name",
            json={"name": "New Name"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "New Name"


def test_execute_cell():
    """Test POST /v1/notebooks/{id}/cells/{cell_id}/execute endpoint."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Execute Test")

        # Add cell with simple code
        cell_id = "test-cell"
        add_cell_to_notebook(notebook_dir, cell_id)
        write_cell(notebook_dir, cell_id, "x = 1 + 1\ny = 'hello'")

        # Open notebook
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )
        session_id = response.json()["session_id"]

        # Execute cell
        response = client.post(
            f"/v1/notebooks/{session_id}/cells/{cell_id}/execute"
        )
        assert response.status_code == 200
        data = response.json()

        # Verify response structure
        assert data["cell_id"] == cell_id
        assert "outputs" in data
        assert "stdout" in data
        assert "stderr" in data
        assert "duration_ms" in data
        assert data["status"] == "ready", (
            f"Expected 'ready' but got '{data['status']}': {data.get('error')}"
        )
        assert "x" in data["outputs"], f"Missing x in outputs: {data}"
        assert "y" in data["outputs"], f"Missing y in outputs: {data}"


def test_execute_cell_updates_session_state_and_history():
    """REST execution should update backend cell state and profiling history."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        notebook_dir = create_notebook(tmpdir_path, "Execute Session State")
        cell_id = "test-cell"
        add_cell_to_notebook(notebook_dir, cell_id)
        write_cell(notebook_dir, cell_id, "x = 41 + 1")
        add_cell_to_notebook(notebook_dir, "consumer", after_cell_id=cell_id)
        write_cell(notebook_dir, "consumer", "y = x + 1")

        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )
        session_id = response.json()["session_id"]

        response = client.post(
            f"/v1/notebooks/{session_id}/cells/{cell_id}/execute"
        )
        assert response.status_code == 200
        assert response.json()["status"] == "ready"

        from strata.notebook.routes import get_session_manager

        session = get_session_manager().get_session(session_id)
        assert session is not None
        cell = next(c for c in session.notebook_state.cells if c.id == cell_id)
        consumer = next(c for c in session.notebook_state.cells if c.id == "consumer")
        assert cell.status == "ready"
        assert consumer.status == "idle"
        assert cell.cache_hit is False
        assert cell.artifact_uri is not None
        assert len(session.execution_history[cell_id]) == 1


def test_execute_cell_not_found():
    """Test executing a non-existent cell."""
    client = TestClient(create_test_app())

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Execute Test")

        # Open notebook
        response = client.post(
            "/v1/notebooks/open",
            json={"path": str(notebook_dir)}
        )
        session_id = response.json()["session_id"]

        # Try to execute non-existent cell
        response = client.post(
            f"/v1/notebooks/{session_id}/cells/nonexistent/execute"
        )
        assert response.status_code == 404
