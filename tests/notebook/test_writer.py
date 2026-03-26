"""Tests for notebook writer."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from strata.notebook.models import CellMeta, NotebookToml
from strata.notebook.parser import parse_notebook
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    remove_cell_from_notebook,
    rename_notebook,
    reorder_cells,
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
            cells=[
                CellMeta(id="c1", file="cell1.py", language="python", order=0),
                CellMeta(id="c2", file="cell2.py", language="python", order=1),
            ],
        )

        # Write it
        write_notebook_toml(notebook_dir, notebook_toml)

        # Verify by reading it back
        notebook_state = parse_notebook(notebook_dir)
        assert notebook_state.id == "custom-id"
        assert notebook_state.name == "Custom Notebook"
        assert len(notebook_state.cells) == 2
