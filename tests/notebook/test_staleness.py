"""Tests for staleness detection."""


import pytest

from strata.notebook.models import CellState, NotebookState
from strata.notebook.session import NotebookSession


@pytest.fixture
def three_cell_notebook(tmp_path):
    """Create a 3-cell notebook with dependencies."""
    notebook_dir = tmp_path / "notebook"
    notebook_dir.mkdir()

    # Create cells directory
    cells_dir = notebook_dir / "cells"
    cells_dir.mkdir()

    # Create cell files
    (cells_dir / "load.py").write_text("df = [1, 2, 3]")
    (cells_dir / "clean.py").write_text("cleaned = [x for x in df]")
    (cells_dir / "explore.py").write_text("print(cleaned)")

    # Create pyproject.toml
    (notebook_dir / "pyproject.toml").write_text("[project]\nname = 'test'\n")

    # Create NotebookState
    notebook_state = NotebookState(
        id="test_nb",
        name="Test",
        cells=[
            CellState(
                id="load",
                source="df = [1, 2, 3]",
                language="python",
                order=0,
            ),
            CellState(
                id="clean",
                source="cleaned = [x for x in df]",
                language="python",
                order=1,
            ),
            CellState(
                id="explore",
                source="print(cleaned)",
                language="python",
                order=2,
            ),
        ],
    )

    return notebook_dir, notebook_state


def test_fresh_notebook_all_idle(three_cell_notebook):
    """Fresh notebook → all cells should be idle."""
    notebook_dir, notebook_state = three_cell_notebook

    session = NotebookSession(notebook_state, notebook_dir)
    staleness = session.compute_staleness()

    # All cells should be idle initially (no cached artifacts)
    for cell_id, status in staleness.items():
        assert status.status == "idle"


def test_staleness_reasons_empty_for_idle(three_cell_notebook):
    """Idle cells should have no staleness reasons."""
    notebook_dir, notebook_state = three_cell_notebook

    session = NotebookSession(notebook_state, notebook_dir)
    staleness = session.compute_staleness()

    # All cells should have empty reasons
    for cell_id, status in staleness.items():
        assert len(status.reasons) == 0


def test_staleness_computation_multiple_calls(three_cell_notebook):
    """Multiple staleness computations should be consistent."""
    notebook_dir, notebook_state = three_cell_notebook

    session = NotebookSession(notebook_state, notebook_dir)

    # Compute staleness multiple times
    s1 = session.compute_staleness()
    s2 = session.compute_staleness()

    # Should be identical
    for cell_id in s1:
        assert s1[cell_id].status == s2[cell_id].status
        assert s1[cell_id].reasons == s2[cell_id].reasons
