"""Tests for notebook session manager lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path

from strata.notebook.models import (
    CellMeta,
    MountMode,
    MountSpec,
    NotebookToml,
    WorkerBackendType,
    WorkerSpec,
)
from strata.notebook.session import EnvironmentJobSnapshot, SessionManager
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    rename_notebook,
    write_cell,
    write_notebook_toml,
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


def test_reload_does_not_restore_ready_state_after_mount_change(tmp_path: Path):
    """Reload should not preserve ready state when cell mount provenance changed."""
    notebook_dir = create_notebook(tmp_path, "reload_mount_state")
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(notebook_dir, "c1", "x = raw_data.name")

    data_a = tmp_path / "data_a"
    data_b = tmp_path / "data_b"
    data_a.mkdir()
    data_b.mkdir()

    def _write_notebook_mount(uri: str) -> None:
        write_notebook_toml(
            notebook_dir,
            NotebookToml(
                notebook_id="reload_mount_state",
                name="reload_mount_state",
                cells=[CellMeta(id="c1", file="c1.py", order=0)],
                mounts=[
                    MountSpec(
                        name="raw_data",
                        uri=uri,
                        mode=MountMode.READ_ONLY,
                    )
                ],
            ),
        )

    _write_notebook_mount(f"file://{data_a}")

    manager = SessionManager()
    session = manager.open_notebook(notebook_dir)

    from strata.notebook.executor import CellExecutor

    async def _prime() -> None:
        executor = CellExecutor(session)
        assert (await executor.execute_cell("c1", "x = raw_data.name")).success

    asyncio.run(_prime())
    session.mark_executed_ready("c1")

    _write_notebook_mount(f"file://{data_b}")
    session.reload()

    cell = next(c for c in session.notebook_state.cells if c.id == "c1")
    assert cell.status == "idle"


def test_reload_does_not_restore_ready_state_after_env_change(tmp_path: Path):
    """Reload should not preserve ready state when runtime env provenance changed."""
    notebook_dir = create_notebook(tmp_path, "reload_env_state")
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(notebook_dir, "c1", "x = 1")

    write_notebook_toml(
        notebook_dir,
        NotebookToml(
            notebook_id="reload_env_state",
            name="reload_env_state",
            cells=[CellMeta(id="c1", file="c1.py", order=0)],
            env={"TOKEN": "a"},
        ),
    )

    manager = SessionManager()
    session = manager.open_notebook(notebook_dir)

    from strata.notebook.executor import CellExecutor

    async def _prime() -> None:
        executor = CellExecutor(session)
        assert (await executor.execute_cell("c1", "x = 1")).success

    asyncio.run(_prime())
    session.mark_executed_ready("c1")

    write_notebook_toml(
        notebook_dir,
        NotebookToml(
            notebook_id="reload_env_state",
            name="reload_env_state",
            cells=[CellMeta(id="c1", file="c1.py", order=0)],
            env={"TOKEN": "b"},
        ),
    )
    session.reload()

    cell = next(c for c in session.notebook_state.cells if c.id == "c1")
    assert cell.status == "idle"


def test_reload_does_not_restore_ready_state_after_worker_runtime_change(tmp_path: Path):
    """Reload should not preserve ready state when worker runtime identity changes."""
    notebook_dir = create_notebook(tmp_path, "reload_worker_state")
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(notebook_dir, "c1", "x = 1")

    write_notebook_toml(
        notebook_dir,
        NotebookToml(
            notebook_id="reload_worker_state",
            name="reload_worker_state",
            cells=[CellMeta(id="c1", file="c1.py", order=0)],
            worker="cpu-analytics",
            workers=[
                WorkerSpec(
                    name="cpu-analytics",
                    backend=WorkerBackendType.LOCAL,
                    runtime_id="py311-a",
                )
            ],
        ),
    )

    manager = SessionManager()
    session = manager.open_notebook(notebook_dir)

    from strata.notebook.executor import CellExecutor

    async def _prime() -> None:
        executor = CellExecutor(session)
        assert (await executor.execute_cell("c1", "x = 1")).success

    asyncio.run(_prime())
    session.mark_executed_ready("c1")

    write_notebook_toml(
        notebook_dir,
        NotebookToml(
            notebook_id="reload_worker_state",
            name="reload_worker_state",
            cells=[CellMeta(id="c1", file="c1.py", order=0)],
            worker="cpu-analytics",
            workers=[
                WorkerSpec(
                    name="cpu-analytics",
                    backend=WorkerBackendType.LOCAL,
                    runtime_id="py311-b",
                )
            ],
        ),
    )
    session.reload()

    cell = next(c for c in session.notebook_state.cells if c.id == "c1")
    assert cell.status == "idle"


def test_open_notebook_can_reuse_existing_session_by_path(tmp_path: Path):
    """Reopening the same path can reuse and refresh the existing session."""
    notebook_dir = create_notebook(tmp_path, "reuse_open")
    manager = SessionManager()

    session = manager.open_notebook(notebook_dir)
    original_id = session.id

    rename_notebook(notebook_dir, "reuse_open_renamed")

    reopened = manager.open_notebook(notebook_dir, reuse_existing=True)

    assert reopened is session
    assert reopened.id == original_id
    assert reopened.notebook_state.name == "reuse_open_renamed"


def test_open_notebook_reuse_existing_session_keeps_pending_environment(
    monkeypatch, tmp_path: Path
):
    """Reusing a live session should preserve pending env bootstrap instead of refreshing it."""
    notebook_dir = create_notebook(tmp_path, "reuse_pending")
    manager = SessionManager()

    session = manager.open_notebook(notebook_dir)
    session.environment_job = EnvironmentJobSnapshot(
        id="job-123",
        action="sync",
        command="uv sync",
        status="running",
        phase="uv_running",
        started_at=1,
    )
    session.mark_environment_pending()

    def _fail_refresh() -> None:
        raise AssertionError("refresh_environment_runtime should not be called")

    monkeypatch.setattr(session, "refresh_environment_runtime", _fail_refresh)

    reopened = manager.open_notebook(notebook_dir, reuse_existing=True)

    assert reopened is session
    assert reopened.environment_sync_state == "pending"
