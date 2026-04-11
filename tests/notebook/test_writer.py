"""Tests for notebook writer."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

# Python 3.10 compatibility
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

from strata.notebook import writer as writer_module
from strata.notebook.models import (
    CellMeta,
    MountMode,
    MountSpec,
    NotebookToml,
    WorkerBackendType,
    WorkerSpec,
)
from strata.notebook.parser import parse_notebook
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    remove_cell_from_notebook,
    rename_notebook,
    reorder_cells,
    update_cell_env,
    update_cell_timeout,
    update_cell_worker,
    update_environment_metadata,
    update_notebook_env,
    update_notebook_timeout,
    update_notebook_worker,
    update_notebook_workers,
    write_cell,
    write_notebook_toml,
)


def test_create_notebook():
    """Test creating a new notebook."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "New Notebook")

        # Verify structure
        assert notebook_dir.exists()
        assert (notebook_dir / "notebook.toml").exists()
        assert (notebook_dir / "pyproject.toml").exists()
        assert (notebook_dir / "cells").exists()
        assert (notebook_dir / "cells").is_dir()


def test_update_environment_metadata_reads_pyvenv_cfg_without_subprocess(
    monkeypatch: pytest.MonkeyPatch,
):
    """Refreshing environment metadata should reuse pyvenv.cfg when available."""
    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Metadata Probe Test")

        def fail_subprocess(*args, **kwargs):
            raise AssertionError("venv python probe should not spawn a subprocess")

        monkeypatch.setattr(
            writer_module,
            "read_venv_runtime_python_version",
            lambda *_args, **_kwargs: "3.13.3",
        )
        monkeypatch.setattr(writer_module.subprocess, "run", fail_subprocess)

        update_environment_metadata(notebook_dir)

        with open(notebook_dir / "notebook.toml", "rb") as f:
            data = tomllib.load(f)

        environment = data.get("environment", {})
        assert environment.get("runtime_python_version") == "3.13.3"


def test_write_cell():
    """Test writing cell source."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Cell Write Test")

        # Add a cell
        cell_id = "test-cell"
        add_cell_to_notebook(notebook_dir, cell_id)

        # Write source
        source = "x = 1 + 1\ny = x * 2"
        write_cell(notebook_dir, cell_id, source)

        # Verify file was written
        cells_dir = notebook_dir / "cells"
        cell_file = cells_dir / f"{cell_id}.py"
        assert cell_file.exists()
        assert cell_file.read_text() == source


def test_write_cell_not_found():
    """Test writing to a non-existent cell."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Cell Write Test")

        # Try to write to non-existent cell
        with pytest.raises(ValueError, match="Cell .* not found"):
            write_cell(notebook_dir, "nonexistent", "code")


def test_add_cell():
    """Test adding cells."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Add Cell Test")

        # Add first cell
        cell1_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell1_id)

        # Add second cell
        cell2_id = "cell-2"
        add_cell_to_notebook(notebook_dir, cell2_id)

        # Parse and verify
        notebook_state = parse_notebook(notebook_dir)
        assert len(notebook_state.cells) == 2
        assert notebook_state.cells[0].id == cell1_id
        assert notebook_state.cells[1].id == cell2_id


def test_add_cell_after():
    """Test adding cell after a specific cell."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Add Cell After Test")

        # Add cells
        cell1_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell1_id)

        cell2_id = "cell-2"
        add_cell_to_notebook(notebook_dir, cell2_id)

        # Add cell after cell1
        cell1_5_id = "cell-1.5"
        add_cell_to_notebook(notebook_dir, cell1_5_id, after_cell_id=cell1_id)

        # Parse and verify order
        notebook_state = parse_notebook(notebook_dir)
        assert len(notebook_state.cells) == 3
        cell_ids = [c.id for c in notebook_state.cells]
        assert cell_ids.index(cell1_id) < cell_ids.index(cell1_5_id)
        assert cell_ids.index(cell1_5_id) < cell_ids.index(cell2_id)


def test_remove_cell():
    """Test removing cells."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Remove Cell Test")

        # Add cells
        cell1_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell1_id)

        cell2_id = "cell-2"
        add_cell_to_notebook(notebook_dir, cell2_id)

        # Remove first cell
        remove_cell_from_notebook(notebook_dir, cell1_id)

        # Verify
        notebook_state = parse_notebook(notebook_dir)
        assert len(notebook_state.cells) == 1
        assert notebook_state.cells[0].id == cell2_id


def test_remove_cell_not_found():
    """Test removing a non-existent cell."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Remove Cell Test")

        # Try to remove non-existent cell
        with pytest.raises(ValueError, match="Cell .* not found"):
            remove_cell_from_notebook(notebook_dir, "nonexistent")


def test_reorder_cells():
    """Test reordering cells."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Reorder Test")

        # Add cells
        cell1_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell1_id)

        cell2_id = "cell-2"
        add_cell_to_notebook(notebook_dir, cell2_id)

        cell3_id = "cell-3"
        add_cell_to_notebook(notebook_dir, cell3_id)

        # Reorder to [2, 3, 1]
        reorder_cells(notebook_dir, [cell2_id, cell3_id, cell1_id])

        # Verify
        notebook_state = parse_notebook(notebook_dir)
        cell_ids = [c.id for c in notebook_state.cells]
        assert cell_ids == [cell2_id, cell3_id, cell1_id]


def test_rename_notebook():
    """Test renaming a notebook."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Original Name")

        # Rename
        rename_notebook(notebook_dir, "New Name")

        # Verify
        notebook_state = parse_notebook(notebook_dir)
        assert notebook_state.name == "New Name"


def test_write_notebook_toml():
    """Test writing notebook.toml."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "TOML Test")

        # Create a NotebookToml
        now = datetime.now(tz=UTC)
        notebook_toml = NotebookToml(
            notebook_id="custom-id",
            name="Custom Notebook",
            created_at=now,
            updated_at=now,
            worker="gpu-default",
            timeout=9.5,
            env={"API_ROOT": "https://example.test"},
            ai={"model": "gpt-4o", "base_url": "https://api.openai.com/v1"},
            mounts=[
                MountSpec(name="raw_data", uri="s3://bucket/dataset", mode=MountMode.READ_ONLY),
            ],
            cells=[
                CellMeta(
                    id="c1",
                    file="cell1.py",
                    language="python",
                    order=0,
                    worker="gpu-worker",
                    timeout=2.0,
                    env={"TOKEN": "cell-secret"},
                    mounts=[
                        MountSpec(
                            name="scratch",
                            uri="file:///tmp/scratch",
                            mode=MountMode.READ_WRITE,
                        )
                    ],
                ),
                CellMeta(id="c2", file="cell2.py", language="python", order=1),
            ],
        )

        # Write it
        write_notebook_toml(notebook_dir, notebook_toml)

        # Verify by reading it back
        notebook_state = parse_notebook(notebook_dir)
        assert notebook_state.id == "custom-id"
        assert notebook_state.name == "Custom Notebook"
        assert notebook_state.worker == "gpu-default"
        assert notebook_state.timeout == 9.5
        assert notebook_state.env == {"API_ROOT": "https://example.test"}
        assert len(notebook_state.cells) == 2
        assert notebook_state.cells[0].worker == "gpu-worker"
        assert notebook_state.cells[0].worker_override == "gpu-worker"
        assert notebook_state.cells[0].timeout == 2.0
        assert notebook_state.cells[0].timeout_override == 2.0
        assert notebook_state.cells[0].env == {
            "API_ROOT": "https://example.test",
            "TOKEN": "cell-secret",
        }
        assert notebook_state.cells[0].env_overrides == {"TOKEN": "cell-secret"}
        assert notebook_state.cells[1].worker == "gpu-default"
        assert notebook_state.cells[1].timeout == 9.5
        assert len(notebook_state.cells[0].mounts) == 2
        assert {mount.name for mount in notebook_state.cells[0].mounts} == {
            "raw_data",
            "scratch",
        }
        assert len(notebook_state.cells[1].mounts) == 1
        assert notebook_state.cells[1].mounts[0].name == "raw_data"

        with open(notebook_dir / "notebook.toml", "rb") as f:
            data = tomllib.load(f)
        assert data["ai"] == {
            "model": "gpt-4o",
            "base_url": "https://api.openai.com/v1",
        }


def test_update_notebook_worker():
    """Test persisting notebook-level worker configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Worker Notebook")
        update_notebook_worker(notebook_dir, "gpu-default")

        notebook_state = parse_notebook(notebook_dir)
        assert notebook_state.worker == "gpu-default"


def test_update_notebook_workers():
    """Test persisting notebook-scoped worker definitions."""
    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Worker Catalog Notebook")
        update_notebook_workers(
            notebook_dir,
            [
                WorkerSpec(name="local", backend=WorkerBackendType.LOCAL),
                WorkerSpec(
                    name="gpu-a100",
                    backend=WorkerBackendType.EXECUTOR,
                    runtime_id="cuda-12.4",
                    config={"url": "https://executor.internal/gpu-a100"},
                ),
            ],
        )

        notebook_state = parse_notebook(notebook_dir)
        assert [worker.name for worker in notebook_state.workers] == [
            "local",
            "gpu-a100",
        ]
        assert notebook_state.workers[1].backend == WorkerBackendType.EXECUTOR
        assert notebook_state.workers[1].runtime_id == "cuda-12.4"
        assert notebook_state.workers[1].config == {"url": "https://executor.internal/gpu-a100"}


def test_update_notebook_timeout_and_env():
    """Test persisting notebook-level timeout/env configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Notebook Runtime")
        update_notebook_timeout(notebook_dir, 7.5)
        update_notebook_env(notebook_dir, {"TOKEN": "secret"})

        notebook_state = parse_notebook(notebook_dir)
        assert notebook_state.timeout == 7.5
        assert notebook_state.env == {"TOKEN": "secret"}


def test_update_notebook_env_preserves_ai_config():
    """Notebook runtime edits should not strip [ai] configuration."""
    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Notebook AI Runtime")

        with open(notebook_dir / "notebook.toml", "a", encoding="utf-8") as f:
            f.write('\n[ai]\nmodel = "gpt-4o"\nbase_url = "https://api.openai.com/v1"\n')

        update_notebook_env(notebook_dir, {"TOKEN": "secret"})

        with open(notebook_dir / "notebook.toml", "rb") as f:
            data = tomllib.load(f)

        assert data["env"] == {"TOKEN": "secret"}
        assert data["ai"] == {
            "model": "gpt-4o",
            "base_url": "https://api.openai.com/v1",
        }


def test_update_cell_worker():
    """Test persisting cell-level worker overrides."""
    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Cell Worker Notebook")
        add_cell_to_notebook(notebook_dir, "cell-1")

        update_notebook_worker(notebook_dir, "gpu-default")
        update_cell_worker(notebook_dir, "cell-1", "gpu-override")

        notebook_state = parse_notebook(notebook_dir)
        assert notebook_state.cells[0].worker == "gpu-override"
        assert notebook_state.cells[0].worker_override == "gpu-override"


def test_update_cell_timeout_and_env():
    """Test persisting cell-level timeout/env overrides."""
    with tempfile.TemporaryDirectory() as tmpdir:
        notebook_dir = create_notebook(Path(tmpdir), "Cell Runtime Notebook")
        add_cell_to_notebook(notebook_dir, "cell-1")

        update_notebook_timeout(notebook_dir, 7.5)
        update_notebook_env(notebook_dir, {"TOKEN": "base"})
        update_cell_timeout(notebook_dir, "cell-1", 2.0)
        update_cell_env(notebook_dir, "cell-1", {"TOKEN": "override"})

        notebook_state = parse_notebook(notebook_dir)
        assert notebook_state.cells[0].timeout == 2.0
        assert notebook_state.cells[0].timeout_override == 2.0
        assert notebook_state.cells[0].env == {"TOKEN": "override"}
        assert notebook_state.cells[0].env_overrides == {"TOKEN": "override"}


def test_create_notebook_preserves_existing_id():
    """Re-creating at the same path must keep the original notebook_id."""
    with tempfile.TemporaryDirectory() as tmpdir:
        nb_dir = create_notebook(Path(tmpdir), "Stable ID")
        add_cell_to_notebook(nb_dir, "c1")
        write_cell(nb_dir, "c1", "x = 1")

        original = parse_notebook(nb_dir)
        original_id = original.id
        assert len(original.cells) == 1

        # Re-create at the same path (simulates boot() calling create again)
        nb_dir_2 = create_notebook(Path(tmpdir), "Stable ID")
        assert nb_dir_2 == nb_dir

        reopened = parse_notebook(nb_dir)
        assert reopened.id == original_id, "create_notebook must preserve the existing notebook_id"
        assert len(reopened.cells) == 1
        assert reopened.cells[0].id == "c1"
