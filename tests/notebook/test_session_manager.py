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

_MINIMAL_PNG_LITERAL = (
    "b\"\\x89PNG\\r\\n\\x1a\\n\\x00\\x00\\x00\\rIHDR\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01"
    "\\x08\\x04\\x00\\x00\\x00\\xb5\\x1c\\x0c\\x02\\x00\\x00\\x00\\x0bIDATx\\xdac\\xfc\\xff"
    "\\x1f\\x00\\x03\\x03\\x02\\x00\\xef\\x9b\\xe0M\\x00\\x00\\x00\\x00IEND\\xaeB`\\x82\""
)
_MARKDOWN_LITERAL = '"# Reopened\\n\\nRendered after refresh."'


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


def test_open_notebook_restores_persisted_display_output(tmp_path: Path):
    """A reopened notebook should restore persisted display output metadata."""
    notebook_dir = create_notebook(tmp_path, "restore_display")
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(
        notebook_dir,
        "c1",
        f"""
class Display:
    def _repr_png_(self):
        return {_MINIMAL_PNG_LITERAL}

Display()
""",
    )

    manager = SessionManager()
    session = manager.open_notebook(notebook_dir)

    from strata.notebook.executor import CellExecutor

    async def _prime() -> None:
        executor = CellExecutor(session)
        assert (await executor.execute_cell("c1", session.notebook_state.cells[0].source)).success

    asyncio.run(_prime())
    manager.close_session(session.id)

    reopened = SessionManager().open_notebook(notebook_dir)
    cell = next(c for c in reopened.notebook_state.cells if c.id == "c1")
    serialized = reopened.serialize_cell(cell)

    assert serialized["status"] == "ready"
    assert serialized["display_output"]["content_type"] == "image/png"
    assert serialized["display_output"]["artifact_uri"].startswith("strata://artifact/")
    assert serialized["display_output"]["inline_data_url"].startswith("data:image/png;base64,")


def test_open_notebook_restores_persisted_markdown_display_output(tmp_path: Path):
    """A reopened notebook should restore persisted markdown display output."""
    notebook_dir = create_notebook(tmp_path, "restore_markdown_display")
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(
        notebook_dir,
        "c1",
        f"""
class Display:
    def _repr_markdown_(self):
        return {_MARKDOWN_LITERAL}

Display()
""",
    )

    manager = SessionManager()
    session = manager.open_notebook(notebook_dir)

    from strata.notebook.executor import CellExecutor

    async def _prime() -> None:
        executor = CellExecutor(session)
        assert (await executor.execute_cell("c1", session.notebook_state.cells[0].source)).success

    asyncio.run(_prime())
    manager.close_session(session.id)

    reopened = SessionManager().open_notebook(notebook_dir)
    cell = next(c for c in reopened.notebook_state.cells if c.id == "c1")
    serialized = reopened.serialize_cell(cell)

    assert serialized["status"] == "ready"
    assert serialized["display_output"]["content_type"] == "text/markdown"
    assert serialized["display_output"]["artifact_uri"].startswith("strata://artifact/")
    assert serialized["display_output"]["markdown_text"] == "# Reopened\n\nRendered after refresh."
