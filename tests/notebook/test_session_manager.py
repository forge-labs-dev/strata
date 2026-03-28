"""Tests for notebook session manager lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path

from strata.notebook.session import SessionManager
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    rename_notebook,
    write_cell,
)


def test_close_session_without_running_loop_uses_nowait_pool_shutdown(
    monkeypatch, tmp_path: Path
):
    """Sync close_session should still trigger warm-pool cleanup."""
    manager = SessionManager()
    notebook_dir = create_notebook(tmp_path, "session_close")

    called: list[str] = []

    def _fake_shutdown_nowait(self):
        called.append("shutdown")

    monkeypatch.setattr(
        "strata.notebook.pool.WarmProcessPool.shutdown_nowait",
        _fake_shutdown_nowait,
    )

    session = manager.open_notebook(notebook_dir)
    manager.close_session(session.id)

    assert called == ["shutdown"]


def test_reload_preserves_ready_leaf_runtime_state(tmp_path: Path):
    """Metadata-only reloads should not drop an executed leaf back to idle."""
    notebook_dir = create_notebook(tmp_path, "reload_state")
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(notebook_dir, "c1", "x = 1")

    manager = SessionManager()
    session = manager.open_notebook(notebook_dir)

    from strata.notebook.executor import CellExecutor

    async def _prime() -> None:
        executor = CellExecutor(session)
        assert (await executor.execute_cell("c1", "x = 1")).success

    asyncio.run(_prime())
    session.mark_executed_ready("c1")

    rename_notebook(notebook_dir, "reload_state_renamed")
    session.reload()

    cell = next(c for c in session.notebook_state.cells if c.id == "c1")
    assert cell.status == "ready"
