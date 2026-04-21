"""Tests for notebook session manager lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import cast

from strata.notebook.models import (
    CellMeta,
    CellStatus,
    MountMode,
    MountSpec,
    NotebookToml,
    WorkerBackendType,
    WorkerSpec,
)
from strata.notebook.pool import WarmProcessPool
from strata.notebook.session import EnvironmentJobSnapshot, SessionManager
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    rename_notebook,
    write_cell,
    write_notebook_toml,
)

_MINIMAL_PNG_LITERAL = (
    'b"\\x89PNG\\r\\n\\x1a\\n\\x00\\x00\\x00\\rIHDR\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01'
    "\\x08\\x04\\x00\\x00\\x00\\xb5\\x1c\\x0c\\x02\\x00\\x00\\x00\\x0bIDATx\\xdac\\xfc\\xff"
    '\\x1f\\x00\\x03\\x03\\x02\\x00\\xef\\x9b\\xe0M\\x00\\x00\\x00\\x00IEND\\xaeB`\\x82"'
)
_MARKDOWN_LITERAL = '"# Reopened\\n\\nRendered after refresh."'


def test_close_session_without_running_loop_uses_nowait_pool_shutdown(monkeypatch, tmp_path: Path):
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


def test_close_session_tolerates_non_pool_warm_pool(tmp_path: Path):
    """Closing a session should tolerate test doubles without pool methods."""
    manager = SessionManager()
    notebook_dir = create_notebook(tmp_path, "session_close_tolerant")

    session = manager.open_notebook(notebook_dir)
    session.warm_pool = cast(WarmProcessPool, object())

    manager.close_session(session.id)

    assert session.id not in manager.list_sessions()


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


def _prime_env_reload_session(tmp_path: Path, source: str, notebook_env: dict[str, str]):
    """Set up a minimal notebook with one executed cell for env-reload tests."""
    notebook_dir = create_notebook(tmp_path, "reload_env_state")
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(notebook_dir, "c1", source)

    write_notebook_toml(
        notebook_dir,
        NotebookToml(
            notebook_id="reload_env_state",
            name="reload_env_state",
            cells=[CellMeta(id="c1", file="c1.py", order=0)],
            env=notebook_env,
        ),
    )

    manager = SessionManager()
    session = manager.open_notebook(notebook_dir)

    from strata.notebook.executor import CellExecutor

    async def _prime() -> None:
        executor = CellExecutor(session)
        assert (await executor.execute_cell("c1", source)).success

    asyncio.run(_prime())
    session.mark_executed_ready("c1")

    primed = next(c for c in session.notebook_state.cells if c.id == "c1")
    assert primed.status == CellStatus.READY
    return notebook_dir, session, primed


def test_reload_preserves_outputs_when_referenced_env_changes(tmp_path: Path):
    """A cell that reads ``APP_MODE`` should turn non-ready when the value
    changes, but its historical display outputs and artifact URIs must
    survive the reload so the UI can still render them."""
    source = "import os\nmode = os.environ['APP_MODE']\nmode"
    notebook_dir, session, primed = _prime_env_reload_session(tmp_path, source, {"APP_MODE": "a"})
    previous_display_outputs = [out.model_copy(deep=True) for out in primed.display_outputs]
    previous_artifact_uri = primed.artifact_uri
    previous_artifact_uris = dict(primed.artifact_uris)
    assert previous_display_outputs, "cell should have primed display outputs"

    write_notebook_toml(
        notebook_dir,
        NotebookToml(
            notebook_id="reload_env_state",
            name="reload_env_state",
            cells=[CellMeta(id="c1", file="c1.py", order=0)],
            env={"APP_MODE": "b"},
        ),
    )
    session.reload()

    cell = next(c for c in session.notebook_state.cells if c.id == "c1")
    assert cell.status != CellStatus.READY
    assert cell.display_outputs == previous_display_outputs
    assert cell.artifact_uri == previous_artifact_uri
    assert cell.artifact_uris == previous_artifact_uris


def test_reload_keeps_unrelated_cells_ready_after_env_change(tmp_path: Path):
    """Adding a new notebook-level env var must not invalidate a cell
    that neither references it nor declares it — this is the common case
    when the user saves an ambient API key for an LLM helper and every
    other cell previously went gray."""
    source = "x = 1\nx"
    notebook_dir, session, primed = _prime_env_reload_session(tmp_path, source, {"APP_MODE": "a"})
    previous_display_outputs = [out.model_copy(deep=True) for out in primed.display_outputs]

    write_notebook_toml(
        notebook_dir,
        NotebookToml(
            notebook_id="reload_env_state",
            name="reload_env_state",
            cells=[CellMeta(id="c1", file="c1.py", order=0)],
            env={"APP_MODE": "a", "OPENAI_API_KEY": "sk-new"},
        ),
    )
    session.reload()

    cell = next(c for c in session.notebook_state.cells if c.id == "c1")
    assert cell.status == CellStatus.READY
    assert cell.display_outputs == previous_display_outputs


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


def test_open_notebook_reuse_existing_session_does_not_reload_while_execution_active(
    monkeypatch, tmp_path: Path
):
    """Reusing a live session should not reload/refresh while execution is in flight."""
    notebook_dir = create_notebook(tmp_path, "reuse_running")
    manager = SessionManager()
    session = manager.open_notebook(notebook_dir)

    from strata.notebook import ws as notebook_ws

    async def _exercise() -> None:
        execution_state = notebook_ws._ensure_execution_state(session.id)
        blocker = asyncio.Event()
        task = asyncio.create_task(blocker.wait())
        execution_state["execution_task"] = task

        def _fail_reload() -> None:
            raise AssertionError("reload should not be called during active execution")

        def _fail_refresh() -> None:
            raise AssertionError(
                "refresh_environment_runtime should not be called during active execution"
            )

        monkeypatch.setattr(session, "reload", _fail_reload)
        monkeypatch.setattr(session, "refresh_environment_runtime", _fail_refresh)

        try:
            reopened = manager.open_notebook(notebook_dir, reuse_existing=True)
            assert reopened is session
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(_exercise())


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


def test_open_notebook_restores_explicit_display_side_effect_output(tmp_path: Path):
    """A reopened notebook should restore explicit display(...) side-effect output."""
    notebook_dir = create_notebook(tmp_path, "restore_display_side_effect")
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(
        notebook_dir,
        "c1",
        """
display(Markdown("# Side effect\\n\\nStill here."))
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
    assert serialized["display_output"]["markdown_text"] == "# Side effect\n\nStill here."


def test_open_notebook_restores_multiple_display_outputs_in_order(tmp_path: Path):
    """A reopened notebook should restore ordered display outputs plus the legacy last-item shim."""
    notebook_dir = create_notebook(tmp_path, "restore_multiple_displays")
    add_cell_to_notebook(notebook_dir, "c1")
    write_cell(
        notebook_dir,
        "c1",
        """
display(Markdown("# First"))
42
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
    assert len(serialized["display_outputs"]) == 2
    assert serialized["display_outputs"][0]["content_type"] == "text/markdown"
    assert serialized["display_outputs"][0]["markdown_text"] == "# First"
    assert serialized["display_outputs"][1]["content_type"] == "json/object"
    assert serialized["display_outputs"][1]["preview"] == 42
    assert serialized["display_output"]["content_type"] == "json/object"
    assert serialized["display_output"]["preview"] == 42


def test_serialize_cell_surfaces_module_cell_status(tmp_path: Path):
    """serialize_cell reports is_module_cell + module_exports so the UI
    can show the "module" pill and list exported symbols in a tooltip."""
    nb_dir = create_notebook(tmp_path, "module_flag", initialize_environment=False)
    add_cell_to_notebook(nb_dir, "mod")
    write_cell(
        nb_dir,
        "mod",
        "import math\n\nSTEP = 0.5\n\ndef scaled(x):\n    return x * STEP\n",
    )
    add_cell_to_notebook(nb_dir, "runtime", after_cell_id="mod")
    write_cell(nb_dir, "runtime", "y = scaled(2)\n")

    session = SessionManager().open_notebook(nb_dir)
    mod_cell = next(c for c in session.notebook_state.cells if c.id == "mod")
    runtime_cell = next(c for c in session.notebook_state.cells if c.id == "runtime")

    mod_payload = session.serialize_cell(mod_cell)
    assert mod_payload["is_module_cell"] is True
    exports = {entry["name"]: entry["kind"] for entry in mod_payload["module_exports"]}
    assert exports == {"STEP": "constant", "scaled": "function"}

    runtime_payload = session.serialize_cell(runtime_cell)
    assert runtime_payload["is_module_cell"] is False
    assert "module_exports" not in runtime_payload


def test_serialize_cell_does_not_flag_pure_data_cell_as_module(tmp_path: Path):
    """A cell that only defines a literal constant is still "pure" source
    but isn't a module cell — downstream consumers get the int through
    the data path, no synthetic module involved."""
    nb_dir = create_notebook(tmp_path, "pure_data", initialize_environment=False)
    add_cell_to_notebook(nb_dir, "c1")
    write_cell(nb_dir, "c1", "THRESHOLD = 42\n")

    session = SessionManager().open_notebook(nb_dir)
    cell = next(c for c in session.notebook_state.cells if c.id == "c1")
    payload = session.serialize_cell(cell)
    assert payload["is_module_cell"] is False
