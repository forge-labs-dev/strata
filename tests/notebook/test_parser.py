"""Tests for notebook parser."""

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from strata.notebook.models import CellMeta, NotebookToml
from strata.notebook.parser import parse_notebook
from strata.notebook.writer import create_notebook, write_cell, write_notebook_toml


def test_parse_empty_notebook():
    """Test parsing a notebook with no cells."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create a notebook
        notebook_dir = create_notebook(tmpdir_path, "Test Notebook")

        # Parse it
        notebook_state = parse_notebook(notebook_dir)

        assert notebook_state.id == notebook_state.id
        assert notebook_state.name == "Test Notebook"
        assert notebook_state.cells == []
        assert notebook_state.path == notebook_dir


def test_parse_notebook_with_cells():
    """Test parsing a notebook with multiple cells."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create a notebook
        notebook_dir = create_notebook(tmpdir_path, "Multi-cell Notebook")

        # Add cells
        from strata.notebook.writer import add_cell_to_notebook

        cell1_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell1_id)

        cell2_id = "cell-2"
        add_cell_to_notebook(notebook_dir, cell2_id, after_cell_id=cell1_id)

        # Write source for cells
        write_cell(notebook_dir, cell1_id, "x = 1 + 1")
        write_cell(notebook_dir, cell2_id, "y = x * 2")

        # Parse it
        notebook_state = parse_notebook(notebook_dir)

        assert notebook_state.name == "Multi-cell Notebook"
        assert len(notebook_state.cells) == 2
        assert notebook_state.cells[0].id == cell1_id
        assert notebook_state.cells[0].source == "x = 1 + 1"
        assert notebook_state.cells[1].id == cell2_id
        assert notebook_state.cells[1].source == "y = x * 2"


def test_parse_notebook_missing_cells_directory():
    """Test parsing a notebook with missing cell files (graceful degradation)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook directory structure manually
        notebook_dir = tmpdir_path / "test_notebook"
        notebook_dir.mkdir()
        cells_dir = notebook_dir / "cells"
        cells_dir.mkdir()

        # Create notebook.toml with cell reference
        now = datetime.now(tz=UTC)
        notebook_toml = NotebookToml(
            notebook_id="test-123",
            name="Missing Cell Notebook",
            created_at=now,
            updated_at=now,
            cells=[
                CellMeta(id="cell-1", file="missing.py", language="python", order=0)
            ],
        )
        write_notebook_toml(notebook_dir, notebook_toml)

        # Note: we don't create the actual cell file

        # Parse it - should gracefully handle missing file
        notebook_state = parse_notebook(notebook_dir)

        assert notebook_state.name == "Missing Cell Notebook"
        assert len(notebook_state.cells) == 1
        assert notebook_state.cells[0].id == "cell-1"
        assert notebook_state.cells[0].source == ""  # Empty source for missing file


def test_parse_notebook_not_found():
    """Test parsing a non-existent notebook."""
    with pytest.raises(FileNotFoundError):
        parse_notebook(Path("/nonexistent/notebook"))


def test_parse_and_reload_after_edit():
    """Test round-trip: parse, edit, reload."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Create notebook
        notebook_dir = create_notebook(tmpdir_path, "Editable Notebook")

        from strata.notebook.writer import add_cell_to_notebook

        cell_id = "cell-1"
        add_cell_to_notebook(notebook_dir, cell_id)
        write_cell(notebook_dir, cell_id, "original code")

        # Parse it
        notebook_state1 = parse_notebook(notebook_dir)
        assert notebook_state1.cells[0].source == "original code"

        # Edit cell
        write_cell(notebook_dir, cell_id, "modified code")

        # Reload
        notebook_state2 = parse_notebook(notebook_dir)
        assert notebook_state2.cells[0].source == "modified code"
