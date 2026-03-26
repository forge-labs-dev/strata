"""Parse notebook directory and load notebook.toml + cell sources."""

from __future__ import annotations

import tomllib
from datetime import UTC, datetime
from pathlib import Path

from strata.notebook.models import CellMeta, CellState, NotebookState, NotebookToml


def parse_notebook(directory: Path) -> NotebookState:
    """Parse notebook directory, load notebook.toml and cell files.

    Args:
        directory: Path to notebook directory

    Returns:
        NotebookState with all cells loaded

    Raises:
        FileNotFoundError: If notebook.toml is missing
    """
    directory = Path(directory)
    notebook_toml_path = directory / "notebook.toml"

    if not notebook_toml_path.exists():
        raise FileNotFoundError(f"notebook.toml not found at {notebook_toml_path}")

    # Read notebook.toml
    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    # Parse into NotebookToml
    # Get created_at and updated_at, defaulting to now if not present
    created_at = toml_data.get("created_at")
    if created_at is None:
        created_at = datetime.now(tz=UTC)

    updated_at = toml_data.get("updated_at")
    if updated_at is None:
        updated_at = datetime.now(tz=UTC)

    notebook_toml = NotebookToml(
        notebook_id=toml_data.get("notebook_id", ""),
        name=toml_data.get("name", "Untitled Notebook"),
        created_at=created_at,
        updated_at=updated_at,
        cells=[
            CellMeta(**cell_meta)
            for cell_meta in toml_data.get("cells", [])
        ],
        artifacts=toml_data.get("artifacts", {}),
        environment=toml_data.get("environment", {}),
        cache=toml_data.get("cache", {}),
    )

    # Load cell sources
    cells_dir = directory / "cells"
    cell_states: list[CellState] = []

    for cell_meta in notebook_toml.cells:
        cell_file = cells_dir / cell_meta.file
        source = ""

        if cell_file.exists():
            with open(cell_file, encoding="utf-8") as f:
                source = f.read()

        cell_states.append(
            CellState(
                id=cell_meta.id,
                source=source,
                language=cell_meta.language,
                order=cell_meta.order,
            )
        )

    # Sort by order
    cell_states.sort(key=lambda c: c.order)

    return NotebookState(
        id=notebook_toml.notebook_id,
        name=notebook_toml.name,
        cells=cell_states,
        path=directory,
        created_at=notebook_toml.created_at,
        updated_at=notebook_toml.updated_at,
    )
