"""Tests for WebSocket notebook execution."""

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from strata.notebook.parser import parse_notebook
from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell


@pytest.fixture
def temp_notebook():
    """Create a temporary notebook with simple cells."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create notebook structure
        notebook_dir = create_notebook(tmpdir, "test_notebook")

        # Create simple cells
        cells_data = [
            ("root", "x = 1"),
            ("middle", "y = x + 1"),
            ("leaf", "z = y + 1"),
        ]

        for cell_id, source in cells_data:
            add_cell_to_notebook(notebook_dir, cell_id)
            write_cell(notebook_dir, cell_id, source)

        # Parse the notebook
        notebook_state = parse_notebook(notebook_dir)

        yield notebook_dir, notebook_state


@pytest.fixture
def app():
    """Create FastAPI test app with notebook routes."""
    from fastapi import FastAPI

    from strata.notebook.routes import router as notebook_router
    from strata.notebook.ws import router as notebook_ws_router

    app = FastAPI()
    app.include_router(notebook_router)
    app.include_router(notebook_ws_router)

    return app


@pytest.fixture
def client(app):
    """Create TestClient for the app."""
    return TestClient(app)


def test_notebook_sync(client, temp_notebook, app):
    """Test notebook_sync message returns full notebook state."""
    notebook_dir, notebook_state = temp_notebook

    # Register notebook with session manager
    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)

    # Connect WebSocket
    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        # Send notebook_sync
        websocket.send_json(
            {
                "type": "notebook_sync",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {},
            }
        )

        # Receive notebook_state
        response = websocket.receive_json()
        assert response["type"] == "notebook_state"
        assert "payload" in response
        state = response["payload"]
        assert "id" in state
        assert "cells" in state
        assert "dag" in state


def test_cell_execute_no_cascade(client, temp_notebook, app):
    """Test cell_execute on a root cell (no cascade needed)."""
    notebook_dir, notebook_state = temp_notebook

    # Register notebook
    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)

    # Connect WebSocket
    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        # Find root cell
        root_cell = next((c for c in session.notebook_state.cells if c.upstream_ids == []), None)
        if not root_cell:
            # If no root (no DAG), just pick first cell
            root_cell = session.notebook_state.cells[0]

        # Send cell_execute
        websocket.send_json(
            {
                "type": "cell_execute",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": root_cell.id},
            }
        )

        # Should receive cell_status(running)
        response = websocket.receive_json()
        assert response["type"] == "cell_status"
        assert response["payload"]["status"] == "running"

        # Should receive cell_output or cell_error
        response = websocket.receive_json()
        assert response["type"] in ["cell_output", "cell_error"]

        # Should receive cell_status(ready/error)
        response = websocket.receive_json()
        assert response["type"] == "cell_status"
        assert response["payload"]["status"] in ["ready", "error"]


def test_cell_source_update(client, temp_notebook, app):
    """Test cell_source_update triggers DAG recomputation."""
    notebook_dir, notebook_state = temp_notebook

    # Register notebook
    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)

    # Connect WebSocket
    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        # Send cell_source_update
        cell_id = session.notebook_state.cells[0].id
        websocket.send_json(
            {
                "type": "cell_source_update",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": cell_id, "source": "x = 2\ny = 3"},
            }
        )

        # Should receive dag_update
        response = websocket.receive_json()
        assert response["type"] == "dag_update"
        assert "edges" in response["payload"]
        assert "topological_order" in response["payload"]

        # May receive cell_status updates
        while True:
            try:
                response = websocket.receive_json(timeout=0.1)
            except Exception:
                break
            if response["type"] == "cell_status":
                # Cell status updates are expected
                assert "status" in response["payload"]


def test_cell_cancel(client, temp_notebook, app):
    """Test cell_cancel stops execution."""
    notebook_dir, notebook_state = temp_notebook

    # Register notebook
    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)

    # Connect WebSocket
    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        cell_id = session.notebook_state.cells[0].id

        # Send cell_cancel
        websocket.send_json(
            {
                "type": "cell_cancel",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": cell_id},
            }
        )

        # Should receive cell_status(idle)
        response = websocket.receive_json()
        assert response["type"] == "cell_status"
        assert response["payload"]["status"] == "idle"


def test_malformed_message(client, temp_notebook, app):
    """Test handling of malformed messages."""
    notebook_dir, notebook_state = temp_notebook

    # Register notebook
    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)

    # Connect WebSocket
    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        # Send message without cell_id
        websocket.send_json(
            {
                "type": "cell_execute",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {},  # Missing cell_id
            }
        )

        # Should receive error
        response = websocket.receive_json()
        assert response["type"] == "error"
        assert "error" in response["payload"]


def test_unknown_notebook(client, app):
    """Test connecting to non-existent notebook."""
    # Try to connect to non-existent notebook
    with pytest.raises(Exception):  # Should raise connection error
        with client.websocket_connect("/v1/notebooks/ws/nonexistent") as _websocket:
            pass
