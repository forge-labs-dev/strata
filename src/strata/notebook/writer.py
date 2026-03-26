"""Write notebook changes back to disk (notebook.toml and cell files)."""

from __future__ import annotations

import logging
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import tomli_w

from strata.notebook.models import NotebookToml

# Python 3.10 compatibility
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

if TYPE_CHECKING:
    pass


def write_cell(notebook_dir: Path, cell_id: str, source: str) -> None:
    """Write cell source to disk.

    Args:
        notebook_dir: Path to notebook directory
        cell_id: Cell ID
        source: Cell source code

    Raises:
        ValueError: If cell not found in notebook.toml
    """
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    # Read current notebook.toml
    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    # Find cell metadata
    cells_data = toml_data.get("cells", [])
    cell_meta = None
    for cell in cells_data:
        if cell.get("id") == cell_id:
            cell_meta = cell
            break

    if cell_meta is None:
        raise ValueError(f"Cell {cell_id} not found in notebook.toml")

    # Write cell file
    cells_dir = notebook_dir / "cells"
    cells_dir.mkdir(exist_ok=True)
    cell_file = cells_dir / cell_meta["file"]
    cell_file.parent.mkdir(parents=True, exist_ok=True)

    with open(cell_file, "w", encoding="utf-8") as f:
        f.write(source)


def write_notebook_toml(notebook_dir: Path, toml: NotebookToml) -> None:
    """Write notebook.toml to disk.

    Args:
        notebook_dir: Path to notebook directory
        toml: NotebookToml model to write
    """
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    # Convert to dict for TOML serialization
    toml_data = {
        "notebook_id": toml.notebook_id,
        "name": toml.name,
        "created_at": (
            toml.created_at.isoformat()
            if isinstance(toml.created_at, datetime)
            else toml.created_at
        ),
        "updated_at": (
            toml.updated_at.isoformat()
            if isinstance(toml.updated_at, datetime)
            else toml.updated_at
        ),
        "cells": [
            {
                "id": cell.id,
                "file": cell.file,
                "language": cell.language,
                "order": cell.order,
            }
            for cell in toml.cells
        ],
        "artifacts": toml.artifacts,
        "environment": toml.environment,
        "cache": toml.cache,
    }

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def create_notebook(parent_dir: Path, name: str) -> Path:
    """Create a new notebook directory with notebook.toml and pyproject.toml.

    Args:
        parent_dir: Parent directory for the notebook
        name: Notebook name (used for folder and notebook name)

    Returns:
        Path to created notebook directory
    """
    parent_dir = Path(parent_dir)
    parent_dir.mkdir(parents=True, exist_ok=True)

    # Create notebook directory (slugify the name)
    notebook_dir = parent_dir / name.lower().replace(" ", "_")
    notebook_dir.mkdir(exist_ok=True)

    # Create cells subdirectory
    cells_dir = notebook_dir / "cells"
    cells_dir.mkdir(exist_ok=True)

    # Generate notebook ID
    notebook_id = str(uuid.uuid4())

    # Create notebook.toml
    now = datetime.now(tz=UTC)
    notebook_toml = NotebookToml(
        notebook_id=notebook_id,
        name=name,
        created_at=now,
        updated_at=now,
        cells=[],
    )
    write_notebook_toml(notebook_dir, notebook_toml)

    # Create pyproject.toml (minimal)
    pyproject_content = f'''[project]
name = "{name.lower().replace(" ", "-")}"
version = "0.1.0"
description = ""
requires-python = ">=3.12"

[tool.uv]
'''

    with open(notebook_dir / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write(pyproject_content)

    # Run uv sync to create venv + uv.lock (best-effort)
    _uv_sync(notebook_dir)

    # Populate environment section with lockfile hash + python version
    _update_environment_metadata(notebook_dir)

    return notebook_dir


_logger = logging.getLogger(__name__)


def _uv_sync(notebook_dir: Path, *, timeout: int = 60) -> bool:
    """Run ``uv sync`` in *notebook_dir*.

    Returns True on success, False on failure (logged, never raised).
    """
    try:
        subprocess.run(
            ["uv", "sync"],
            cwd=str(notebook_dir),
            timeout=timeout,
            capture_output=True,
            check=True,
        )
        _logger.debug("uv sync succeeded in %s", notebook_dir)
        return True
    except FileNotFoundError:
        _logger.warning("uv not found on PATH — skipping venv creation")
    except subprocess.TimeoutExpired:
        _logger.warning("uv sync timed out after %ds in %s", timeout, notebook_dir)
    except subprocess.CalledProcessError as exc:
        _logger.warning(
            "uv sync failed in %s: %s",
            notebook_dir,
            exc.stderr.decode(errors="replace") if exc.stderr else "(no stderr)",
        )
    return False


def _update_environment_metadata(notebook_dir: Path) -> None:
    """Update the ``[environment]`` section in ``notebook.toml``.

    Records lockfile_hash and python_version so that clients can detect
    environment changes without recomputing hashes themselves.
    """
    import sys

    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    from strata.notebook.env import compute_lockfile_hash

    toml_path = notebook_dir / "notebook.toml"
    if not toml_path.exists():
        return

    try:
        with open(toml_path, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        _logger.debug("Cannot parse notebook.toml for env metadata update")
        return

    data["environment"] = {
        "lockfile_hash": compute_lockfile_hash(notebook_dir),
        "python_version": (
            f"{sys.version_info.major}.{sys.version_info.minor}"
            f".{sys.version_info.micro}"
        ),
    }

    with open(toml_path, "wb") as f:
        tomli_w.dump(data, f)


def update_environment_metadata(notebook_dir: Path) -> None:
    """Public API: refresh ``[environment]`` in ``notebook.toml``.

    Called after ``uv add`` / ``uv remove`` to persist the new lockfile hash.
    """
    _update_environment_metadata(notebook_dir)


def add_cell_to_notebook(
    notebook_dir: Path, cell_id: str, after_cell_id: str | None = None
) -> None:
    """Add a new cell to the notebook.

    Args:
        notebook_dir: Path to notebook directory
        cell_id: New cell ID
        after_cell_id: Cell ID to add after (None = at end)
    """
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    # Calculate order
    cells_data = toml_data.get("cells", [])
    if after_cell_id:
        idx = next((i for i, c in enumerate(cells_data) if c.get("id") == after_cell_id), None)
        if idx is not None:
            order = cells_data[idx].get("order", 0) + 0.5
        else:
            order = len(cells_data)
    else:
        order = len(cells_data)

    # Create cell file
    cell_filename = f"{cell_id}.py"
    cells_dir = notebook_dir / "cells"
    cells_dir.mkdir(exist_ok=True)

    with open(cells_dir / cell_filename, "w", encoding="utf-8") as f:
        f.write("")

    # Add to cells list
    cells_data.append(
        {
            "id": cell_id,
            "file": cell_filename,
            "language": "python",
            "order": order,
        }
    )

    # Re-sort cells by order
    cells_data.sort(key=lambda c: c.get("order", 0))

    toml_data["cells"] = cells_data
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def remove_cell_from_notebook(notebook_dir: Path, cell_id: str) -> None:
    """Remove a cell from the notebook.

    Args:
        notebook_dir: Path to notebook directory
        cell_id: Cell ID to remove

    Raises:
        ValueError: If cell not found
    """
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    cells_data = toml_data.get("cells", [])
    cell_meta = None
    cell_idx = None

    for i, cell in enumerate(cells_data):
        if cell.get("id") == cell_id:
            cell_meta = cell
            cell_idx = i
            break

    if cell_meta is None:
        raise ValueError(f"Cell {cell_id} not found")

    # Remove cell file
    cells_dir = notebook_dir / "cells"
    cell_file = cells_dir / cell_meta["file"]
    if cell_file.exists():
        cell_file.unlink()

    # Remove from cells list
    cells_data.pop(cell_idx)
    toml_data["cells"] = cells_data
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def reorder_cells(notebook_dir: Path, cell_ids: list[str]) -> None:
    """Reorder cells in the notebook.

    Args:
        notebook_dir: Path to notebook directory
        cell_ids: Ordered list of cell IDs
    """
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    cells_data = toml_data.get("cells", [])

    # Create mapping of cell_id to metadata
    cell_map = {cell.get("id"): cell for cell in cells_data}

    # Reorder and update order field
    new_cells = []
    for i, cell_id in enumerate(cell_ids):
        if cell_id in cell_map:
            cell = cell_map[cell_id]
            cell["order"] = i
            new_cells.append(cell)

    toml_data["cells"] = new_cells
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def rename_notebook(notebook_dir: Path, new_name: str) -> None:
    """Rename the notebook.

    Args:
        notebook_dir: Path to notebook directory
        new_name: New notebook name
    """
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    toml_data["name"] = new_name
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)
