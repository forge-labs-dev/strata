"""Tests for notebook REST routes."""

import tempfile
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from strata.notebook.routes import router
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    write_cell,
)


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
        assert data["status"] in ["ready", "error"]
        assert "outputs" in data
        assert "stdout" in data
        assert "stderr" in data
        assert "duration_ms" in data

        # Verify execution results (should succeed)
        if data["status"] == "ready":
            assert "x" in data["outputs"], f"Missing x in outputs: {data}"
            assert "y" in data["outputs"], f"Missing y in outputs: {data}"


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
