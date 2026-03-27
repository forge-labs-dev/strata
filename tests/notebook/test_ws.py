"""Tests for WebSocket notebook execution."""

import asyncio
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from strata.notebook.parser import parse_notebook
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    write_cell,
)


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


def test_cell_execute_uses_warm_pool_when_available(
    client, temp_notebook, app, monkeypatch
):
    """Test the WebSocket path is wired to use the session warm pool."""
    notebook_dir, _ = temp_notebook

    from strata.notebook.executor import CellExecutor
    from strata.notebook.pool import PooledCellExecutor
    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)
    session.warm_pool = object()

    root_cell = next((c for c in session.notebook_state.cells if c.upstream_ids == []), None)
    if not root_cell:
        root_cell = session.notebook_state.cells[0]

    warm_calls = 0

    async def fake_execute_with_pool(pool, manifest_path, notebook_dir, timeout_seconds=30):
        nonlocal warm_calls
        warm_calls += 1
        return {
            "success": True,
            "variables": {
                "x": {
                    "content_type": "json/object",
                    "preview": 1,
                    "bytes": 1,
                    "file": "x.json",
                }
            },
            "stdout": "",
            "stderr": "",
            "mutation_warnings": [],
        }

    def fake_store_outputs(
        self,
        cell_id,
        output_dir,
        provenance_hash,
        input_hashes,
        *,
        source_hash="",
        env_hash="",
    ):
        return True

    monkeypatch.setattr(
        PooledCellExecutor,
        "execute_with_pool",
        staticmethod(fake_execute_with_pool),
    )
    monkeypatch.setattr(CellExecutor, "_store_outputs", fake_store_outputs)

    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        websocket.send_json(
            {
                "type": "cell_execute",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": root_cell.id},
            }
        )

        output_message = None
        final_status = None
        for _ in range(10):
            response = websocket.receive_json()
            if response["type"] in ["cell_output", "cell_error"]:
                output_message = response
            if (
                response["type"] == "cell_status"
                and response["payload"]["status"] in ["ready", "error"]
            ):
                final_status = response
                break

        assert output_message is not None
        assert final_status is not None
        assert output_message["type"] == "cell_output"
        assert output_message["payload"]["execution_method"] == "warm"
        assert warm_calls == 1


def test_cascade_prompt_is_sent_only_to_requesting_websocket(
    client, temp_notebook, app
):
    """A cascade prompt should not fan out to other clients on the notebook."""
    notebook_dir, _ = temp_notebook

    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)

    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as ws1:
        with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as ws2:
            ws1.send_json(
                {
                    "type": "cell_execute",
                    "seq": 1,
                    "ts": "2026-03-23T00:00:00Z",
                    "payload": {"cell_id": "middle"},
                }
            )

            response = ws1.receive_json()
            assert response["type"] == "cascade_prompt"
            assert response["payload"]["cell_id"] == "middle"

            with pytest.raises(Exception):
                ws2.receive_json(timeout=0.1)


def test_impact_preview_is_sent_only_to_requesting_websocket(
    client, temp_notebook, app
):
    """Impact preview responses should stay scoped to the requesting client."""
    notebook_dir, _ = temp_notebook

    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)

    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as ws1:
        with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as ws2:
            ws1.send_json(
                {
                    "type": "impact_preview_request",
                    "seq": 1,
                    "ts": "2026-03-23T00:00:00Z",
                    "payload": {"cell_id": "middle"},
                }
            )

            response = ws1.receive_json()
            assert response["type"] == "impact_preview"
            assert response["payload"]["target_cell_id"] == "middle"

            with pytest.raises(Exception):
                ws2.receive_json(timeout=0.1)


def test_inspect_repl_round_trip(client, temp_notebook, app):
    """Test the live inspect REPL path over WebSocket."""
    notebook_dir, _ = temp_notebook

    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)

    middle_cell = next((c for c in session.notebook_state.cells if "x" in c.references), None)
    assert middle_cell is not None

    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        websocket.send_json(
            {
                "type": "inspect_open",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": middle_cell.id},
            }
        )

        response = websocket.receive_json()
        assert response["type"] == "inspect_result"
        assert response["payload"]["action"] == "open"
        assert response["payload"]["ok"] is True

        websocket.send_json(
            {
                "type": "inspect_eval",
                "seq": 2,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": middle_cell.id, "expr": "x + 1"},
            }
        )

        response = websocket.receive_json()
        assert response["type"] == "inspect_result"
        assert response["payload"]["action"] == "eval"
        assert response["payload"]["ok"] is True
        assert response["payload"]["result"] == "2"
        assert response["payload"]["type"] == "int"

        websocket.send_json(
            {
                "type": "inspect_close",
                "seq": 3,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": middle_cell.id},
            }
        )

        response = websocket.receive_json()
        assert response["type"] == "inspect_result"
        assert response["payload"]["action"] == "close"
        assert response["payload"]["ok"] is True


def test_active_websocket_session_is_not_evicted(client, temp_notebook, app):
    """TTL eviction should skip sessions that still have connected sockets."""
    notebook_dir, _ = temp_notebook

    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)
    session.last_accessed = (
        time.time() - session_manager.SESSION_TTL_SECONDS - 60
    )

    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        session_manager._evict_stale()
        assert session.id in session_manager.list_sessions()

        websocket.send_json(
            {
                "type": "notebook_sync",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {},
            }
        )
        websocket.receive_json()
        assert session.last_accessed > time.time() - 5


def test_inspect_sessions_closed_when_last_websocket_disconnects(
    client, temp_notebook, app, monkeypatch
):
    """Disconnecting the last socket should close notebook inspect sessions."""
    notebook_dir, _ = temp_notebook

    from strata.notebook.inspect_repl import InspectManager
    from strata.notebook.routes import get_session_manager
    from strata.notebook.ws import _notebook_inspect_managers

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)

    close_all_calls = 0

    async def fake_open_session(self, cell_id, notebook_session):
        return SimpleNamespace(ready=True), "ready"

    async def fake_close_all(self):
        nonlocal close_all_calls
        close_all_calls += 1

    monkeypatch.setattr(InspectManager, "open_session", fake_open_session)
    monkeypatch.setattr(InspectManager, "close_all", fake_close_all)

    middle_cell = next((c for c in session.notebook_state.cells if "x" in c.references), None)
    assert middle_cell is not None

    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        websocket.send_json(
            {
                "type": "inspect_open",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": middle_cell.id},
            }
        )

        response = websocket.receive_json()
        assert response["type"] == "inspect_result"
        assert response["payload"]["ok"] is True

    assert close_all_calls == 1
    assert session.id not in _notebook_inspect_managers


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


def test_cell_cancel_interrupts_running_execution_on_same_websocket(
    client, temp_notebook, app, monkeypatch
):
    """A single WebSocket can cancel its own in-flight execution."""
    notebook_dir, _ = temp_notebook

    from strata.notebook.executor import CellExecutor
    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)
    cell_id = session.notebook_state.cells[0].id

    cancelled = threading.Event()

    async def fake_execute_cell(self, cell_id: str, source: str, timeout_seconds: float = 30):
        del self
        del cell_id
        del source
        del timeout_seconds
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(CellExecutor, "execute_cell", fake_execute_cell)

    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        websocket.send_json(
            {
                "type": "cell_execute",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": cell_id},
            }
        )

        response = websocket.receive_json()
        assert response["type"] == "cell_status"
        assert response["payload"]["cell_id"] == cell_id
        assert response["payload"]["status"] == "running"

        websocket.send_json(
            {
                "type": "cell_cancel",
                "seq": 2,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": cell_id},
            }
        )

        response = websocket.receive_json()
        assert response["type"] == "cell_status"
        assert response["payload"]["cell_id"] == cell_id
        assert response["payload"]["status"] == "idle"

    assert cancelled.is_set()


def test_stale_cell_cancel_does_not_clobber_ready_state(client, temp_notebook, app):
    """A late cancel should not rewrite a completed cell back to idle."""
    notebook_dir, _ = temp_notebook

    from strata.notebook.routes import get_session_manager

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)
    cell_id = session.notebook_state.cells[0].id

    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        websocket.send_json(
            {
                "type": "cell_execute",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": cell_id},
            }
        )

        while True:
            msg = websocket.receive_json()
            if (
                msg["type"] == "cell_status"
                and msg["payload"]["cell_id"] == cell_id
                and msg["payload"]["status"] in ["ready", "error"]
            ):
                assert msg["payload"]["status"] == "ready"
                break

        websocket.send_json(
            {
                "type": "cell_cancel",
                "seq": 2,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": cell_id},
            }
        )
        websocket.send_json(
            {
                "type": "notebook_sync",
                "seq": 3,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {},
            }
        )

        messages = []
        while True:
            response = websocket.receive_json()
            messages.append(response)
            if response["type"] == "notebook_state":
                break

        idle_messages = [
            msg
            for msg in messages
            if msg["type"] == "cell_status"
            and msg["payload"].get("cell_id") == cell_id
            and msg["payload"].get("status") == "idle"
        ]
        assert idle_messages == []

        state = messages[-1]["payload"]["cells"]
        cell = next(c for c in state if c["id"] == cell_id)
        assert cell["status"] == "ready"


def test_last_websocket_disconnect_cancels_running_execution(
    client, temp_notebook, app, monkeypatch
):
    """Closing the final socket should cancel the active notebook execution."""
    notebook_dir, _ = temp_notebook

    from strata.notebook.executor import CellExecutor
    from strata.notebook.routes import get_session_manager
    from strata.notebook.ws import _notebook_execution_state

    session_manager = get_session_manager()
    session = session_manager.open_notebook(notebook_dir)
    cell_id = session.notebook_state.cells[0].id

    cancelled = threading.Event()

    async def fake_execute_cell(self, cell_id: str, source: str, timeout_seconds: float = 30):
        del self
        del cell_id
        del source
        del timeout_seconds
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(CellExecutor, "execute_cell", fake_execute_cell)

    with client.websocket_connect(f"/v1/notebooks/ws/{session.id}") as websocket:
        websocket.send_json(
            {
                "type": "cell_execute",
                "seq": 1,
                "ts": "2026-03-23T00:00:00Z",
                "payload": {"cell_id": cell_id},
            }
        )

        response = websocket.receive_json()
        assert response["type"] == "cell_status"
        assert response["payload"]["status"] == "running"

    assert cancelled.wait(timeout=1)
    for _ in range(20):
        state = _notebook_execution_state.get(session.id)
        if state is None or state.get("execution_task") is None:
            break
        time.sleep(0.05)

    state = _notebook_execution_state.get(session.id)
    if state is not None:
        assert state["execution_task"] is None
        assert state["running_cell"] is None
        assert state["requested_cell"] is None


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
