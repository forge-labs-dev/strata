"""Tests for notebook session manager lifecycle."""

from __future__ import annotations

from pathlib import Path

from strata.notebook.session import SessionManager
from strata.notebook.writer import create_notebook


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
