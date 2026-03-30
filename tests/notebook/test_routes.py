"""Tests for notebook REST routes."""

import asyncio
import tempfile
from pathlib import Path

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

    async def _noop_start(self):
        pass

    monkeypatch.setattr("strata.notebook.pool.WarmProcessPool.start", _noop_start)


# Create a test app with just the notebook router
def create_test_app():
    """Create a test FastAPI app with notebook router."""
    app = FastAPI()
    app.include_router(router)
    return app


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
            worker["name"] == "gpu-a100" and worker["health"] == "unknown"
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
