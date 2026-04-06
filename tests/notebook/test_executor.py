"""Tests for the cell executor."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from strata.notebook.executor import CellExecutor
from strata.notebook.models import (
    MountMode,
    MountSpec,
    WorkerBackendType,
    WorkerSpec,
)
from strata.notebook.parser import parse_notebook
from strata.notebook.pool import WarmProcessPool
from strata.notebook.session import NotebookSession

_MINIMAL_PNG_LITERAL = (
    "b\"\\x89PNG\\r\\n\\x1a\\n\\x00\\x00\\x00\\rIHDR\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01"
    "\\x08\\x04\\x00\\x00\\x00\\xb5\\x1c\\x0c\\x02\\x00\\x00\\x00\\x0bIDATx\\xdac\\xfc\\xff"
    "\\x1f\\x00\\x03\\x03\\x02\\x00\\xef\\x9b\\xe0M\\x00\\x00\\x00\\x00IEND\\xaeB`\\x82\""
)
_MARKDOWN_LITERAL = '"# Title\\n\\nA **markdown** cell."'


@pytest.fixture
def sample_notebook(tmp_path):
    """Create a sample notebook for testing.

    Returns:
        NotebookSession for the test notebook
    """
    from strata.notebook.writer import add_cell_to_notebook, create_notebook

    # Create notebook
    notebook_dir = create_notebook(tmp_path, "Test Notebook")

    # Add a couple of cells
    add_cell_to_notebook(notebook_dir, "cell1", None)
    add_cell_to_notebook(notebook_dir, "cell2", "cell1")

    # Parse and create session
    notebook_state = parse_notebook(notebook_dir)
    session = NotebookSession(notebook_state, notebook_dir)

    return session


class TestCellExecutor:
    """Test basic cell execution."""

    @pytest.mark.asyncio
    async def test_execute_simple_assignment(self, sample_notebook):
        """Test executing a simple assignment."""
        executor = CellExecutor(sample_notebook)

        source = "x = 1 + 1"
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert result.cell_id == "cell1"
        assert result.error is None
        assert "x" in result.outputs
        assert result.outputs["x"]["content_type"] == "json/object"
        assert result.outputs["x"]["preview"] == 2

    @pytest.mark.asyncio
    async def test_execute_with_print(self, sample_notebook):
        """Test that print output is captured."""
        executor = CellExecutor(sample_notebook)

        source = 'print("Hello, world!")\ny = 42'
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "Hello, world!" in result.stdout
        assert "y" in result.outputs

    @pytest.mark.asyncio
    async def test_execute_with_error(self, sample_notebook):
        """Test executing a cell that raises an error."""
        executor = CellExecutor(sample_notebook)

        source = "z = 1 / 0"
        result = await executor.execute_cell("cell1", source)

        assert result.success is False
        assert result.error is not None
        assert "ZeroDivisionError" in result.error or "division" in result.error

    @pytest.mark.asyncio
    async def test_execute_dataframe(self, sample_notebook):
        """Test executing a cell that creates a dictionary (simulates DataFrame-like output)."""
        executor = CellExecutor(sample_notebook)

        # Use a dict instead of DataFrame since pandas may not be available in test venv
        source = 'df = {"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}'
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "df" in result.outputs
        assert result.outputs["df"]["content_type"] == "json/object"
        assert result.outputs["df"]["preview"] == {"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}

    @pytest.mark.asyncio
    async def test_execute_multiple_outputs(self, sample_notebook):
        """Test executing a cell that defines multiple variables."""
        executor = CellExecutor(sample_notebook)

        source = """
x = 10
y = "hello"
z = [1, 2, 3]
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert set(result.outputs.keys()) == {"x", "y", "z"}
        assert result.outputs["x"]["preview"] == 10
        assert result.outputs["y"]["preview"] == "hello"
        assert result.outputs["z"]["preview"] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_execute_dict_output(self, sample_notebook):
        """Test executing a cell that creates a dict."""
        executor = CellExecutor(sample_notebook)

        source = 'data = {"count": 42, "names": ["Alice", "Bob"]}'
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "data" in result.outputs
        assert result.outputs["data"]["content_type"] == "json/object"

    @pytest.mark.asyncio
    async def test_execute_png_display_output_is_cached_for_leaf_cells(self, sample_notebook):
        """Leaf display outputs should persist as display artifacts and cache-hit on rerun."""
        executor = CellExecutor(sample_notebook)
        source = f"""
class Display:
    def _repr_png_(self):
        return {_MINIMAL_PNG_LITERAL}

Display()
"""

        first = await executor.execute_cell("cell1", source)

        assert first.success is True
        assert first.cache_hit is False
        assert first.display_output is not None
        assert first.display_output["content_type"] == "image/png"
        assert first.display_output["artifact_uri"].startswith("strata://artifact/")
        assert first.outputs["_"]["content_type"] == "image/png"

        second = await executor.execute_cell("cell1", source)

        assert second.success is True
        assert second.cache_hit is True
        assert second.display_output is not None
        assert second.display_output["content_type"] == "image/png"
        assert second.display_output["artifact_uri"].startswith("strata://artifact/")

    @pytest.mark.asyncio
    async def test_execute_markdown_display_output_is_cached_for_leaf_cells(self, sample_notebook):
        """Markdown display outputs should persist as display artifacts and cache-hit on rerun."""
        executor = CellExecutor(sample_notebook)
        source = f"""
class Display:
    def _repr_markdown_(self):
        return {_MARKDOWN_LITERAL}

Display()
"""

        first = await executor.execute_cell("cell1", source)

        assert first.success is True
        assert first.cache_hit is False
        assert first.display_output is not None
        assert first.display_output["content_type"] == "text/markdown"
        assert first.display_output["artifact_uri"].startswith("strata://artifact/")
        assert first.display_output["markdown_text"] == "# Title\n\nA **markdown** cell."

        second = await executor.execute_cell("cell1", source)

        assert second.success is True
        assert second.cache_hit is True
        assert second.display_output is not None
        assert second.display_output["content_type"] == "text/markdown"
        assert second.display_output["artifact_uri"].startswith("strata://artifact/")
        assert second.display_output["markdown_text"] == "# Title\n\nA **markdown** cell."

    @pytest.mark.asyncio
    async def test_execute_display_side_effect_png_output_is_cached(self, sample_notebook):
        """display(...) side effects should flow through the primary display cache path."""
        executor = CellExecutor(sample_notebook)
        source = f"""
class Display:
    def _repr_png_(self):
        return {_MINIMAL_PNG_LITERAL}

display(Display())
"""

        first = await executor.execute_cell("cell1", source)

        assert first.success is True
        assert first.cache_hit is False
        assert first.display_output is not None
        assert first.display_output["content_type"] == "image/png"
        assert first.outputs["_"]["content_type"] == "image/png"

        second = await executor.execute_cell("cell1", source)

        assert second.success is True
        assert second.cache_hit is True
        assert second.display_output is not None
        assert second.display_output["content_type"] == "image/png"

    @pytest.mark.asyncio
    async def test_execute_display_side_effect_markdown_output_is_cached(self, sample_notebook):
        """display(Markdown(...)) should persist and cache-hit like explicit returned outputs."""
        executor = CellExecutor(sample_notebook)
        source = """
display(Markdown("# Side effect\\n\\nCaptured."))
"""

        first = await executor.execute_cell("cell1", source)

        assert first.success is True
        assert first.cache_hit is False
        assert first.display_output is not None
        assert first.display_output["content_type"] == "text/markdown"
        assert first.display_output["markdown_text"] == "# Side effect\n\nCaptured."

        second = await executor.execute_cell("cell1", source)

        assert second.success is True
        assert second.cache_hit is True
        assert second.display_output is not None
        assert second.display_output["content_type"] == "text/markdown"
        assert second.display_output["markdown_text"] == "# Side effect\n\nCaptured."

    @pytest.mark.asyncio
    async def test_execute_last_expression_overrides_earlier_display_side_effect(
        self, sample_notebook
    ):
        """With a single display slot, the last visible result should win."""
        executor = CellExecutor(sample_notebook)
        source = """
display(Markdown("# Earlier"))
42
"""

        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert result.display_output is not None
        assert result.display_output["content_type"] == "json/object"
        assert result.display_output["preview"] == 42

    @pytest.mark.asyncio
    async def test_execute_ignores_private_vars(self, sample_notebook):
        """Test that private variables (_name) are not included in outputs."""
        executor = CellExecutor(sample_notebook)

        source = """
public = 1
_private = 2
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "public" in result.outputs
        assert "_private" not in result.outputs

    @pytest.mark.asyncio
    async def test_execute_empty_cell(self, sample_notebook):
        """Test executing a cell with no outputs."""
        executor = CellExecutor(sample_notebook)

        source = "# Just a comment"
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert len(result.outputs) == 0

    @pytest.mark.asyncio
    async def test_execute_with_import(self, sample_notebook):
        """Test executing a cell with imports."""
        executor = CellExecutor(sample_notebook)

        source = """
import math
result = math.pi
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "result" in result.outputs
        # Pi should be serialized as JSON number
        assert abs(result.outputs["result"]["preview"] - 3.14159) < 0.01

    @pytest.mark.asyncio
    async def test_execute_with_stderr(self, sample_notebook):
        """Test that stderr is captured."""
        executor = CellExecutor(sample_notebook)

        source = """
import sys
print("error message", file=sys.stderr)
x = 1
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "error message" in result.stderr

    @pytest.mark.asyncio
    async def test_execute_surfaces_mount_resolution_error(self, sample_notebook):
        """Mount resolution failures should be surfaced, not silently ignored."""
        executor = CellExecutor(sample_notebook)
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.mounts = [
            MountSpec(
                name="raw_data",
                uri="file:///definitely/not/a/real/path",
                mode=MountMode.READ_ONLY,
            )
        ]

        result = await executor.execute_cell("cell1", "x = raw_data")

        assert result.success is False
        assert result.error is not None
        assert "Local mount 'raw_data' path does not exist" in result.error

    @pytest.mark.asyncio
    async def test_execute_fails_when_rw_sync_back_fails(
        self,
        sample_notebook,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ):
        """RW mount sync-back failures should mark the execution failed."""
        executor = CellExecutor(sample_notebook)
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.mounts = [
            MountSpec(
                name="scratch",
                uri=f"file://{tmp_path / 'scratch'}",
                mode=MountMode.READ_WRITE,
            )
        ]

        async def _boom(*args, **kwargs):
            raise RuntimeError("sync failed")

        monkeypatch.setattr(executor._mount_resolver, "sync_back", _boom)

        result = await executor.execute_cell(
            "cell1",
            '(scratch / "result.txt").write_text("ok")',
        )

        assert result.success is False
        assert result.error is not None
        assert "failed to sync read-write mounts: sync failed" in result.error

    @pytest.mark.asyncio
    async def test_execute_applies_env_annotations(self, sample_notebook):
        """@env annotations should be visible inside cell execution."""
        executor = CellExecutor(sample_notebook)

        source = """
# @env NOTEBOOK_TOKEN=secret-value
import os
token = os.getenv("NOTEBOOK_TOKEN")
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert result.outputs["token"]["preview"] == "secret-value"

    @pytest.mark.asyncio
    async def test_execute_applies_persisted_env_defaults(self, sample_notebook):
        """Persisted env defaults should be visible inside cell execution."""
        executor = CellExecutor(sample_notebook)
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        sample_notebook.notebook_state.env = {"NOTEBOOK_TOKEN": "saved-default"}
        cell.env = {"NOTEBOOK_TOKEN": "saved-default"}

        source = """
import os
token = os.getenv("NOTEBOOK_TOKEN")
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert result.outputs["token"]["preview"] == "saved-default"

    @pytest.mark.asyncio
    async def test_execute_uses_timeout_annotation(
        self,
        sample_notebook,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """@timeout annotations should override the executor timeout."""
        executor = CellExecutor(sample_notebook)
        observed: list[float] = []

        async def _fake_run_harness(
            manifest_path: Path,
            venv_python: Path,
            timeout_seconds: float,
        ) -> dict[str, object]:
            del manifest_path, venv_python
            observed.append(timeout_seconds)
            return {
                "success": True,
                "variables": {},
                "stdout": "",
                "stderr": "",
                "mutation_warnings": [],
            }

        monkeypatch.setattr(executor, "_run_harness", _fake_run_harness)

        result = await executor.execute_cell(
            "cell1",
            "# @timeout 1.5\nx = 1",
            timeout_seconds=30,
        )

        assert result.success is True
        assert observed == [1.5]

    @pytest.mark.asyncio
    async def test_execute_uses_persisted_timeout_override(
        self,
        sample_notebook,
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Persisted timeout defaults should affect execution timeout."""
        executor = CellExecutor(sample_notebook)
        observed: list[float] = []
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        sample_notebook.notebook_state.timeout = 6.0
        cell.timeout = 2.5
        cell.timeout_override = 2.5

        async def _fake_run_harness(
            manifest_path: Path,
            venv_python: Path,
            timeout_seconds: float,
        ) -> dict[str, object]:
            del manifest_path, venv_python
            observed.append(timeout_seconds)
            return {
                "success": True,
                "variables": {},
                "stdout": "",
                "stderr": "",
                "mutation_warnings": [],
            }

        monkeypatch.setattr(executor, "_run_harness", _fake_run_harness)

        result = await executor.execute_cell("cell1", "x = 1", timeout_seconds=30)

        assert result.success is True
        assert observed == [2.5]

    @pytest.mark.asyncio
    async def test_execute_rejects_unimplemented_worker_annotation(
        self,
        sample_notebook,
    ):
        """@worker should fail fast until worker routing is implemented."""
        executor = CellExecutor(sample_notebook)

        result = await executor.execute_cell(
            "cell1",
            "# @worker gpu-a100\nx = 1",
        )

        assert result.success is False
        assert result.error == "Execution failed: worker 'gpu-a100' is not implemented yet"

    @pytest.mark.asyncio
    async def test_execute_allows_registered_local_worker_annotation(
        self,
        sample_notebook,
    ):
        """Named local workers should execute through the existing local path."""
        executor = CellExecutor(sample_notebook)
        sample_notebook.notebook_state.workers = [
            WorkerSpec(
                name="cpu-analytics",
                backend=WorkerBackendType.LOCAL,
                runtime_id="python-analytics",
            )
        ]

        result = await executor.execute_cell(
            "cell1",
            "# @worker cpu-analytics\nx = 1",
        )

        assert result.success is True
        assert result.error is None
        assert "x" in result.outputs

    @pytest.mark.asyncio
    async def test_execute_rejects_unimplemented_notebook_worker(
        self,
        sample_notebook,
    ):
        """Persisted notebook worker defaults should affect execution routing."""
        executor = CellExecutor(sample_notebook)
        sample_notebook.notebook_state.worker = "gpu-default"
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.worker = "gpu-default"

        result = await executor.execute_cell("cell1", "x = 1")

        assert result.success is False
        assert result.error == "Execution failed: worker 'gpu-default' is not implemented yet"

    @pytest.mark.asyncio
    async def test_execute_rejects_unimplemented_cell_worker_override(
        self,
        sample_notebook,
    ):
        """Persisted cell worker overrides should take precedence over notebook defaults."""
        executor = CellExecutor(sample_notebook)
        sample_notebook.notebook_state.worker = "gpu-default"
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.worker = "gpu-override"
        cell.worker_override = "gpu-override"

        result = await executor.execute_cell("cell1", "x = 1")

        assert result.success is False
        assert result.error == "Execution failed: worker 'gpu-override' is not implemented yet"

    @pytest.mark.asyncio
    async def test_execute_rejects_disallowed_service_mode_worker(
        self,
        sample_notebook,
        monkeypatch,
    ):
        """Service mode should reject notebook-local workers outside the server registry."""
        monkeypatch.setattr(
            "strata.server._state",
            SimpleNamespace(
                config=SimpleNamespace(
                    deployment_mode="service",
                    transforms_config={
                        "notebook_workers": [
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
        sample_notebook.notebook_state.workers = [
            WorkerSpec(
                name="gpu-shadow",
                backend=WorkerBackendType.EXECUTOR,
                runtime_id="shadow-runtime",
                config={"url": "embedded://local"},
            )
        ]
        sample_notebook.notebook_state.worker = "gpu-shadow"
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.worker = "gpu-shadow"

        result = await CellExecutor(sample_notebook).execute_cell("cell1", "x = 1")

        assert result.success is False
        assert result.error == (
            "Execution failed: Worker 'gpu-shadow' is not allowed in service mode. "
            "Choose a server-managed worker."
        )

    @pytest.mark.asyncio
    async def test_execute_rejects_disabled_service_mode_worker(
        self,
        sample_notebook,
        monkeypatch,
    ):
        """Service mode should reject disabled server-managed workers."""
        monkeypatch.setattr(
            "strata.server._state",
            SimpleNamespace(
                config=SimpleNamespace(
                    deployment_mode="service",
                    transforms_config={
                        "notebook_workers": [
                            {
                                "name": "gpu-a100",
                                "backend": "executor",
                                "runtime_id": "cuda-12.4",
                                "config": {"url": "embedded://local"},
                                "enabled": False,
                            }
                        ]
                    },
                )
            ),
        )
        sample_notebook.notebook_state.worker = "gpu-a100"
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.worker = "gpu-a100"

        result = await CellExecutor(sample_notebook).execute_cell("cell1", "x = 1")

        assert result.success is False
        assert result.error == (
            "Execution failed: Worker 'gpu-a100' is disabled by server policy. "
            "Choose an enabled server-managed worker."
        )

    @pytest.mark.asyncio
    async def test_execute_allows_service_mode_server_worker(
        self,
        sample_notebook,
        monkeypatch,
    ):
        """Service mode should resolve executor workers from the server registry."""
        monkeypatch.setattr(
            "strata.server._state",
            SimpleNamespace(
                config=SimpleNamespace(
                    deployment_mode="service",
                    transforms_config={
                        "notebook_workers": [
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
        sample_notebook.notebook_state.worker = "gpu-a100"
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.worker = "gpu-a100"

        result = await CellExecutor(sample_notebook).execute_cell("cell1", "x = 1")

        assert result.success is True
        assert result.execution_method == "executor"

    @pytest.mark.asyncio
    async def test_worker_runtime_identity_invalidates_cache(
        self,
        sample_notebook,
    ):
        """Changing a local worker runtime identity should invalidate cache."""
        sample_notebook.notebook_state.workers = [
            WorkerSpec(
                name="cpu-analytics",
                backend=WorkerBackendType.LOCAL,
                runtime_id="py311-a",
            )
        ]
        sample_notebook.notebook_state.worker = "cpu-analytics"
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.worker = "cpu-analytics"
        cell2.worker = "cpu-analytics"
        cell1.source = "x = 1"
        cell2.source = "y = x + 1"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        executor = CellExecutor(sample_notebook)

        first = await executor.execute_cell("cell1", "x = 1")
        second = await executor.execute_cell("cell1", "x = 1")

        assert first.success is True
        assert first.cache_hit is False
        assert second.success is True
        assert second.cache_hit is True

        sample_notebook.notebook_state.workers = [
            WorkerSpec(
                name="cpu-analytics",
                backend=WorkerBackendType.LOCAL,
                runtime_id="py311-b",
            )
        ]

        third = await executor.execute_cell("cell1", "x = 1")

        assert third.success is True
        assert third.cache_hit is False

    @pytest.mark.asyncio
    async def test_execute_supports_embedded_executor_worker(
        self,
        sample_notebook,
    ):
        """Supported executor workers should use the bundle-based executor path."""
        sample_notebook.notebook_state.workers = [
            WorkerSpec(
                name="gpu-embedded",
                backend=WorkerBackendType.EXECUTOR,
                runtime_id="gpu-a100",
                config={"url": "embedded://local"},
            )
        ]
        sample_notebook.notebook_state.worker = "gpu-embedded"
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.worker = "gpu-embedded"
        cell2.worker = "gpu-embedded"
        cell1.source = "x = 1"
        cell2.source = "y = x + 1"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        executor = CellExecutor(sample_notebook)
        first = await executor.execute_cell("cell1", "x = 1")
        second = await executor.execute_cell("cell1", "x = 1")

        assert first.success is True
        assert first.execution_method == "executor"
        assert first.cache_hit is False
        assert first.remote_worker == "gpu-embedded"
        assert first.remote_transport == "embedded"
        assert first.remote_build_id is None
        assert "x" in first.outputs

        assert second.success is True
        assert second.cache_hit is True
        assert second.remote_worker == "gpu-embedded"
        assert second.remote_transport == "embedded"

    @pytest.mark.asyncio
    async def test_execute_supports_http_executor_worker(
        self,
        sample_notebook,
        notebook_executor_server,
    ):
        """HTTP executor workers should execute through the remote bundle transport."""
        sample_notebook.notebook_state.workers = [
            WorkerSpec(
                name="gpu-http",
                backend=WorkerBackendType.EXECUTOR,
                runtime_id="gpu-http-a100",
                config={"url": notebook_executor_server["execute_url"]},
            )
        ]
        sample_notebook.notebook_state.worker = "gpu-http"
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.worker = "gpu-http"
        cell2.worker = "gpu-http"
        cell1.source = "x = 1"
        cell2.source = "y = x + 1"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        executor = CellExecutor(sample_notebook)
        first = await executor.execute_cell("cell1", "x = 1")
        second = await executor.execute_cell("cell1", "x = 1")

        assert first.success is True
        assert first.execution_method == "executor"
        assert first.cache_hit is False
        assert "x" in first.outputs

        assert second.success is True
        assert second.cache_hit is True

    @pytest.mark.asyncio
    async def test_execute_supports_http_executor_worker_with_class_instances(
        self,
        tmp_path,
        notebook_executor_server,
    ):
        """HTTP executor workers should preserve exported class instances across cells."""
        from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

        notebook_dir = create_notebook(tmp_path, "http_class_instances")
        add_cell_to_notebook(notebook_dir, "cell1", None)
        add_cell_to_notebook(notebook_dir, "cell2", "cell1")
        add_cell_to_notebook(notebook_dir, "cell3", "cell2")

        write_cell(
            notebook_dir,
            "cell1",
            """
class Person:
    name = "John"
    age = 20

    def __str__(self):
        return f"{self.name}:{self.age}"
""".strip(),
        )
        write_cell(notebook_dir, "cell2", "p = Person()")
        write_cell(notebook_dir, "cell3", "rendered = str(p)")

        session = NotebookSession(parse_notebook(notebook_dir), notebook_dir)
        session.notebook_state.workers = [
            WorkerSpec(
                name="gpu-http",
                backend=WorkerBackendType.EXECUTOR,
                runtime_id="gpu-http-a100",
                config={"url": notebook_executor_server["execute_url"]},
            )
        ]
        session.notebook_state.worker = "gpu-http"
        for cell in session.notebook_state.cells:
            cell.worker = "gpu-http"

        executor = CellExecutor(session)
        first = await executor.execute_cell("cell1", session.notebook_state.cells[0].source)
        second = await executor.execute_cell("cell2", session.notebook_state.cells[1].source)
        third = await executor.execute_cell("cell3", session.notebook_state.cells[2].source)

        assert first.success is True
        assert first.execution_method == "executor"
        assert second.success is True
        assert second.execution_method == "executor"
        assert second.outputs["p"]["content_type"] == "module/cell-instance"
        assert third.success is True
        assert third.execution_method == "executor"
        assert third.outputs["rendered"]["preview"] == "John:20"

    @pytest.mark.asyncio
    async def test_execute_supports_signed_http_executor_worker(
        self,
        sample_notebook,
        notebook_executor_server,
        notebook_build_server,
    ):
        """HTTP executor workers can opt into the build + signed-URL transport."""
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
        sample_notebook.notebook_state.workers = [
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
        sample_notebook.notebook_state.worker = "gpu-http-signed"
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.worker = "gpu-http-signed"
        cell2.worker = "gpu-http-signed"
        cell1.source = "x = 1"
        cell2.source = "y = x + 1"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        executor = CellExecutor(sample_notebook)
        first = await executor.execute_cell("cell1", "x = 1")
        second = await executor.execute_cell("cell1", "x = 1")

        assert first.success is True
        assert first.execution_method == "executor"
        assert first.cache_hit is False
        assert first.remote_worker == "gpu-http-signed"
        assert first.remote_transport == "signed"
        assert first.remote_build_id is not None
        assert first.remote_build_state == "ready"
        assert first.remote_error_code is None
        assert first.outputs["x"]["preview"] == 1

        assert second.success is True
        assert second.cache_hit is True
        assert second.remote_build_id is None
        assert second.remote_build_state == "ready"
        assert second.remote_error_code is None

    @pytest.mark.asyncio
    async def test_execute_supports_signed_http_executor_worker_with_class_instances(
        self,
        tmp_path,
        notebook_executor_server,
        notebook_build_server,
    ):
        """Signed executor workers should preserve exported class instances across cells."""
        from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

        notebook_dir = create_notebook(tmp_path, "signed_http_class_instances")
        add_cell_to_notebook(notebook_dir, "cell1", None)
        add_cell_to_notebook(notebook_dir, "cell2", "cell1")
        add_cell_to_notebook(notebook_dir, "cell3", "cell2")

        write_cell(
            notebook_dir,
            "cell1",
            """
class Person:
    name = "John"
    age = 20

    def __str__(self):
        return f"{self.name}:{self.age}"
""".strip(),
        )
        write_cell(notebook_dir, "cell2", "p = Person()")
        write_cell(notebook_dir, "cell3", "rendered = str(p)")

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

        session = NotebookSession(parse_notebook(notebook_dir), notebook_dir)
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
        for cell in session.notebook_state.cells:
            cell.worker = "gpu-http-signed"

        executor = CellExecutor(session)
        first = await executor.execute_cell("cell1", session.notebook_state.cells[0].source)
        second = await executor.execute_cell("cell2", session.notebook_state.cells[1].source)
        third = await executor.execute_cell("cell3", session.notebook_state.cells[2].source)

        assert first.success is True
        assert first.execution_method == "executor"
        assert first.remote_transport == "signed"
        assert first.remote_build_state == "ready"

        assert second.success is True
        assert second.execution_method == "executor"
        assert second.remote_transport == "signed"
        assert second.remote_build_state == "ready"
        assert second.outputs["p"]["content_type"] == "module/cell-instance"

        assert third.success is True
        assert third.execution_method == "executor"
        assert third.remote_transport == "signed"
        assert third.remote_build_state == "ready"
        assert third.outputs["rendered"]["preview"] == "John:20"

    @pytest.mark.asyncio
    async def test_execute_supports_signed_http_executor_worker_in_personal_mode(
        self,
        sample_notebook,
        notebook_executor_server,
        notebook_personal_server,
    ):
        """Signed notebook transport should work in personal mode without server transforms."""
        sample_notebook.notebook_state.workers = [
            WorkerSpec(
                name="gpu-http-signed",
                backend=WorkerBackendType.EXECUTOR,
                runtime_id="gpu-http-signed-a100",
                config={
                    "url": notebook_executor_server["execute_url"],
                    "transport": "signed",
                    "strata_url": notebook_personal_server["base_url"],
                },
            )
        ]
        sample_notebook.notebook_state.worker = "gpu-http-signed"
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell1.worker = "gpu-http-signed"
        cell1.source = "x = 1"
        sample_notebook.re_analyze_cell("cell1")

        result = await CellExecutor(sample_notebook).execute_cell("cell1", "x = 1")

        assert result.success is True
        assert result.execution_method == "executor"
        assert result.remote_worker == "gpu-http-signed"
        assert result.remote_transport == "signed"
        assert result.remote_build_id is not None
        assert result.remote_build_state == "ready"
        assert result.remote_error_code is None
        assert result.outputs["x"]["preview"] == 1

    @pytest.mark.asyncio
    async def test_execute_signed_http_executor_marks_build_failed_on_transport_error(
        self,
        sample_notebook,
        notebook_executor_server,
        notebook_build_server,
    ):
        """Signed transport failures should leave no pending/building notebook builds."""
        notebook_build_server["config"].transforms_config["notebook_workers"] = [
            {
                "name": "gpu-http-signed",
                "backend": "executor",
                "runtime_id": "gpu-http-signed-a100",
                "config": {
                    "url": notebook_executor_server["execute_url"],
                    "transport": "signed",
                    "strata_url": "http://127.0.0.1:9",
                },
            }
        ]
        sample_notebook.notebook_state.worker = "gpu-http-signed"
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.worker = "gpu-http-signed"

        result = await CellExecutor(sample_notebook).execute_cell("cell1", "x = 1")

        assert result.success is False
        assert "Remote executor 'gpu-http-signed' returned 502:" in str(result.error)
        assert "Notebook bundle transfer failed:" in str(result.error)
        assert result.remote_worker == "gpu-http-signed"
        assert result.remote_transport == "signed"
        assert result.remote_build_id is not None
        assert result.remote_build_state == "failed"
        assert result.remote_error_code == "EXECUTOR_HTTP_ERROR"

        stats = notebook_build_server["build_store"].get_stats()
        assert stats["failed"] == 1
        assert stats["pending"] == 0
        assert stats["building"] == 0

    @pytest.mark.asyncio
    async def test_cancelled_signed_http_executor_marks_build_failed(
        self,
        sample_notebook,
        notebook_executor_server,
        notebook_build_server,
        monkeypatch,
    ):
        """Cancelling signed transport execution should fail the in-flight build."""
        started = threading.Event()

        async def _slow_run_harness(
            harness_path: Path,
            manifest_path: Path,
            timeout_seconds: float,
        ) -> dict[str, object]:
            del harness_path, manifest_path, timeout_seconds
            started.set()
            await asyncio.sleep(0.5)
            return {
                "success": True,
                "variables": {
                    "x": {
                        "content_type": "json/object",
                        "file": "x.json",
                        "preview": 1,
                    }
                },
                "stdout": "",
                "stderr": "",
                "mutation_warnings": [],
            }

        monkeypatch.setattr(
            "strata.notebook.remote_executor._run_harness",
            _slow_run_harness,
        )

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
        sample_notebook.notebook_state.worker = "gpu-http-signed"
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.worker = "gpu-http-signed"

        task = asyncio.create_task(
            CellExecutor(sample_notebook).execute_cell("cell1", "x = 1")
        )
        assert await asyncio.to_thread(started.wait, 2.0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        stats = notebook_build_server["build_store"].get_stats()
        assert stats["failed"] == 1
        assert stats["pending"] == 0
        assert stats["building"] == 0

    @pytest.mark.asyncio
    async def test_execute_rejects_file_mounts_for_http_executor_worker(
        self,
        sample_notebook,
        notebook_executor_server,
        tmp_path: Path,
    ):
        """HTTP executor workers should reject notebook-declared file mounts."""
        sample_notebook.notebook_state.workers = [
            WorkerSpec(
                name="gpu-http",
                backend=WorkerBackendType.EXECUTOR,
                runtime_id="gpu-http-a100",
                config={"url": notebook_executor_server["execute_url"]},
            )
        ]
        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.worker = "gpu-http"
        cell.mounts = [
            MountSpec(
                name="raw_data",
                uri=f"file://{tmp_path}",
                mode=MountMode.READ_ONLY,
            )
        ]

        result = await CellExecutor(sample_notebook).execute_cell("cell1", "x = 1")

        assert result.success is False
        assert result.error == (
            "Execution failed: Remote executor workers do not support file:// mounts: "
            "'raw_data'"
        )

    @pytest.mark.asyncio
    async def test_execution_duration(self, sample_notebook):
        """Test that execution duration is measured."""
        executor = CellExecutor(sample_notebook)

        source = "x = 42"
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert result.duration_ms > 0

    @pytest.mark.asyncio
    async def test_execute_function_definition(self, sample_notebook):
        """Test executing a cell that defines a function."""
        executor = CellExecutor(sample_notebook)

        source = """
def add(a, b):
    return a + b

result = add(2, 3)
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        # Function definition is not captured, but result is
        assert "add" in result.outputs
        assert "result" in result.outputs
        assert result.outputs["result"]["preview"] == 5

    @pytest.mark.asyncio
    async def test_execute_supports_cross_cell_function_definition(self, sample_notebook):
        """Top-level functions should be reusable across cells via module export."""
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.source = """
import math

def area(r):
    return math.pi * r * r
"""
        cell2.source = "result = round(area(2), 5)"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        executor = CellExecutor(sample_notebook)
        first = await executor.execute_cell("cell1", cell1.source)
        second = await executor.execute_cell("cell2", cell2.source)

        assert first.success is True
        assert first.outputs["area"]["content_type"] == "module/cell"
        assert second.success is True
        assert second.outputs["result"]["preview"] == 12.56637

    @pytest.mark.asyncio
    async def test_execute_supports_cross_cell_class_definition(self, sample_notebook):
        """Top-level classes should be reusable across cells via module export."""
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.source = """
class Box:
    def __init__(self, x):
        self.x = x
"""
        cell2.source = "value = Box(3).x"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        executor = CellExecutor(sample_notebook)
        first = await executor.execute_cell("cell1", cell1.source)
        second = await executor.execute_cell("cell2", cell2.source)

        assert first.success is True
        assert first.outputs["Box"]["content_type"] == "module/cell"
        assert second.success is True
        assert second.outputs["value"]["preview"] == 3

    @pytest.mark.asyncio
    async def test_execute_supports_exported_class_instances_across_cells(self, tmp_path):
        """Instances of exported notebook classes should round-trip to downstream cells."""
        from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

        notebook_dir = create_notebook(tmp_path, "class_instances")
        add_cell_to_notebook(notebook_dir, "cell1", None)
        add_cell_to_notebook(notebook_dir, "cell2", "cell1")
        add_cell_to_notebook(notebook_dir, "cell3", "cell2")

        write_cell(
            notebook_dir,
            "cell1",
            """
class Person:
    name = "John"
    age = 20

    def __str__(self):
        return f"{self.name}:{self.age}"
""".strip(),
        )
        write_cell(notebook_dir, "cell2", "p = Person()")
        write_cell(notebook_dir, "cell3", "rendered = str(p)")

        session = NotebookSession(parse_notebook(notebook_dir), notebook_dir)
        executor = CellExecutor(session)

        first = await executor.execute_cell("cell1", session.notebook_state.cells[0].source)
        second = await executor.execute_cell("cell2", session.notebook_state.cells[1].source)
        third = await executor.execute_cell("cell3", session.notebook_state.cells[2].source)

        assert first.success is True
        assert first.outputs["Person"]["content_type"] == "module/cell"
        assert second.success is True
        assert second.outputs["p"]["content_type"] == "module/cell-instance"
        assert third.success is True
        assert third.outputs["rendered"]["preview"] == "John:20"

    @pytest.mark.asyncio
    async def test_execute_supports_slot_based_class_instances_across_cells(self, tmp_path):
        """Slot-based exported instances should round-trip across notebook cells."""
        from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

        notebook_dir = create_notebook(tmp_path, "slot_instances")
        add_cell_to_notebook(notebook_dir, "cell1", None)
        add_cell_to_notebook(notebook_dir, "cell2", "cell1")
        add_cell_to_notebook(notebook_dir, "cell3", "cell2")

        write_cell(
            notebook_dir,
            "cell1",
            """
class Person:
    __slots__ = ("name", "age")

    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __str__(self):
        return f"{self.name}:{self.age}"
""".strip(),
        )
        write_cell(notebook_dir, "cell2", 'p = Person("Ada", 10)')
        write_cell(notebook_dir, "cell3", "rendered = str(p)")

        session = NotebookSession(parse_notebook(notebook_dir), notebook_dir)
        executor = CellExecutor(session)

        first = await executor.execute_cell("cell1", session.notebook_state.cells[0].source)
        second = await executor.execute_cell("cell2", session.notebook_state.cells[1].source)
        third = await executor.execute_cell("cell3", session.notebook_state.cells[2].source)

        assert first.success is True
        assert second.success is True
        assert second.outputs["p"]["content_type"] == "module/cell-instance"
        assert third.success is True
        assert third.outputs["rendered"]["preview"] == "Ada:10"

    @pytest.mark.asyncio
    async def test_execute_rejects_cross_cell_export_with_top_level_runtime_state(
        self, sample_notebook
    ):
        """Mixed runtime statements should fail with an explicit exportability error."""
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.source = """
x = 1

def add(y):
    return x + y
"""
        cell2.source = "result = add(2)"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        result = await CellExecutor(sample_notebook).execute_cell("cell1", cell1.source)

        assert result.success is False
        assert result.error is not None
        assert "cannot be shared across cells yet" in result.error
        assert "top-level runtime state" in result.error

    @pytest.mark.asyncio
    async def test_execute_rejects_cross_cell_export_with_top_level_lambda(
        self, sample_notebook
    ):
        """Top-level lambdas should fail with a targeted exportability error."""
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.source = "add = lambda y: y + 1"
        cell2.source = "result = add(2)"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        result = await CellExecutor(sample_notebook).execute_cell("cell1", cell1.source)

        assert result.success is False
        assert result.error is not None
        assert "cannot be shared across cells yet" in result.error
        assert "top-level lambdas are not shareable across cells" in result.error

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.warm_pool
    async def test_execute_uses_warm_pool_when_available(self, sample_notebook):
        """Test executor uses a live warm worker when one is available."""
        sample_notebook.ensure_venv_synced()
        pool = WarmProcessPool(
            sample_notebook.path,
            pool_size=1,
            python_executable=sample_notebook.venv_python or Path("python"),
        )
        await pool.start()

        try:
            executor = CellExecutor(sample_notebook, pool)
            result = await executor.execute_cell("cell1", "x = 1 + 1")

            assert result.success is True
            assert result.execution_method == "warm"
            assert result.outputs["x"]["preview"] == 2
        finally:
            await pool.drain()

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.warm_pool
    async def test_warm_execution_applies_env_annotations(self, sample_notebook):
        """Warm workers should honor @env overrides the same way cold execution does."""
        sample_notebook.ensure_venv_synced()
        pool = WarmProcessPool(
            sample_notebook.path,
            pool_size=1,
            python_executable=sample_notebook.venv_python or Path("python"),
        )
        await pool.start()

        try:
            executor = CellExecutor(sample_notebook, pool)
            result = await executor.execute_cell(
                "cell1",
                """
# @env NOTEBOOK_TOKEN=warm-secret
import os
token = os.getenv("NOTEBOOK_TOKEN")
""",
            )

            assert result.success is True
            assert result.execution_method == "warm"
            assert result.outputs["token"]["preview"] == "warm-secret"
        finally:
            await pool.drain()

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.warm_pool
    async def test_warm_execution_injects_mount_paths(self, sample_notebook, tmp_path: Path):
        """Warm workers should inject prepared mount paths the same way cold execution does."""
        mount_dir = tmp_path / "mounted-data"
        mount_dir.mkdir()
        (mount_dir / "data.txt").write_text("hello mount", encoding="utf-8")

        cell = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell.mounts = [
            MountSpec(
                name="raw_data",
                uri=f"file://{mount_dir}",
                mode=MountMode.READ_ONLY,
            )
        ]

        sample_notebook.ensure_venv_synced()
        pool = WarmProcessPool(
            sample_notebook.path,
            pool_size=1,
            python_executable=sample_notebook.venv_python or Path("python"),
        )
        await pool.start()

        try:
            executor = CellExecutor(sample_notebook, pool)
            result = await executor.execute_cell(
                "cell1",
                'text = (raw_data / "data.txt").read_text()',
            )

            assert result.success is True
            assert result.execution_method == "warm"
            assert result.outputs["text"]["preview"] == "hello mount"
        finally:
            await pool.drain()

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.warm_pool
    async def test_warm_execution_supports_cross_cell_function_exports(self, sample_notebook):
        """Warm workers should receive synthetic module exports as upstream inputs."""
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.source = """
def add(a, b):
    return a + b
"""
        cell2.source = "result = add(2, 3)"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        cold_executor = CellExecutor(sample_notebook)
        first = await cold_executor.execute_cell("cell1", cell1.source)
        assert first.success is True
        assert first.outputs["add"]["content_type"] == "module/cell"

        sample_notebook.ensure_venv_synced()
        pool = WarmProcessPool(
            sample_notebook.path,
            pool_size=1,
            python_executable=sample_notebook.venv_python or Path("python"),
        )
        await pool.start()

        try:
            executor = CellExecutor(sample_notebook, pool)
            second = await executor.execute_cell("cell2", cell2.source)

            assert second.success is True
            assert second.execution_method == "warm"
            assert second.outputs["result"]["preview"] == 5
        finally:
            await pool.drain()

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.warm_pool
    async def test_warm_execution_supports_exported_class_instances(self, tmp_path):
        """Warm workers should deserialize instances of exported notebook classes."""
        from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

        notebook_dir = create_notebook(tmp_path, "warm_class_instances")
        add_cell_to_notebook(notebook_dir, "cell1", None)
        add_cell_to_notebook(notebook_dir, "cell2", "cell1")
        add_cell_to_notebook(notebook_dir, "cell3", "cell2")

        write_cell(
            notebook_dir,
            "cell1",
            """
class Person:
    name = "John"
    age = 20

    def __str__(self):
        return f"{self.name}:{self.age}"
""".strip(),
        )
        write_cell(notebook_dir, "cell2", "p = Person()")
        write_cell(notebook_dir, "cell3", "rendered = str(p)")

        session = NotebookSession(parse_notebook(notebook_dir), notebook_dir)
        cold_executor = CellExecutor(session)

        first = await cold_executor.execute_cell("cell1", session.notebook_state.cells[0].source)
        second = await cold_executor.execute_cell("cell2", session.notebook_state.cells[1].source)
        assert first.success is True
        assert second.success is True
        assert second.outputs["p"]["content_type"] == "module/cell-instance"

        session.ensure_venv_synced()
        pool = WarmProcessPool(
            session.path,
            pool_size=1,
            python_executable=session.venv_python or Path("python"),
        )
        await pool.start()

        try:
            executor = CellExecutor(session, pool)
            third = await executor.execute_cell("cell3", session.notebook_state.cells[2].source)

            assert third.success is True
            assert third.execution_method == "warm"
            assert third.outputs["rendered"]["preview"] == "John:20"
        finally:
            await pool.drain()

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.warm_pool
    async def test_warm_execution_reports_same_mutation_warnings_as_cold(
        self, sample_notebook
    ):
        """Warm workers should preserve mutation warnings emitted by cold execution."""
        cell1 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell1")
        cell2 = next(c for c in sample_notebook.notebook_state.cells if c.id == "cell2")
        cell1.source = "x = 1"
        cell2.source = "del x\ny = 2"
        sample_notebook.re_analyze_cell("cell1")
        sample_notebook.re_analyze_cell("cell2")

        cold_result = await CellExecutor(sample_notebook).execute_cell(
            "cell2", cell2.source
        )
        assert cold_result.success is True
        assert cold_result.mutation_warnings
        assert cold_result.mutation_warnings[0]["var_name"] == "x"

        sample_notebook.ensure_venv_synced()
        pool = WarmProcessPool(
            sample_notebook.path,
            pool_size=1,
            python_executable=sample_notebook.venv_python or Path("python"),
        )
        await pool.start()

        try:
            warm_result = await CellExecutor(sample_notebook, pool).execute_cell(
                "cell2", cell2.source
            )
            assert warm_result.success is True
            assert warm_result.execution_method == "warm"
            assert warm_result.mutation_warnings == cold_result.mutation_warnings
        finally:
            await pool.drain()
