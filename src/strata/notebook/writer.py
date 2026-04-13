"""Write notebook changes back to disk (notebook.toml and cell files)."""

from __future__ import annotations

import logging
import shutil
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import tomli_w

from strata.notebook.models import CellMeta, MountSpec, NotebookToml, WorkerSpec
from strata.notebook.python_versions import (
    current_python_minor,
    format_requires_python,
    normalize_python_minor,
    read_requested_python_minor,
    read_venv_runtime_python_version,
)

# Python 3.10 compatibility
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

if TYPE_CHECKING:
    pass


def _serialize_mounts(mounts: list[MountSpec]) -> list[dict[str, str]]:
    """Convert mount specs into TOML-friendly dicts."""
    return [
        {
            "name": mount.name,
            "uri": mount.uri,
            "mode": mount.mode.value,
            **({"pin": mount.pin} if mount.pin is not None else {}),
        }
        for mount in mounts
    ]


_SENSITIVE_KEY_PATTERNS = ("KEY", "SECRET", "TOKEN", "PASSWORD", "CREDENTIAL")


def _is_sensitive_env_key(key: str) -> bool:
    """Return True if the env var name looks like a secret."""
    upper = key.upper()
    return any(pattern in upper for pattern in _SENSITIVE_KEY_PATTERNS)


def _serialize_env(env: dict[str, str]) -> dict[str, str]:
    """Convert env vars into a TOML-friendly dict.

    Values for sensitive-looking keys (API keys, tokens, passwords) are
    stripped so they never reach disk. The key names are preserved so
    the notebook remembers *which* vars are configured — the user
    re-enters values via the Runtime panel on next open.
    """
    return {
        key: ("" if _is_sensitive_env_key(key) else value) for key, value in sorted(env.items())
    }


def _serialize_workers(workers: list[WorkerSpec]) -> list[dict[str, object]]:
    """Convert worker specs into TOML-friendly dicts."""
    return [
        {
            "name": worker.name,
            "backend": worker.backend.value,
            **({"runtime_id": worker.runtime_id} if worker.runtime_id else {}),
            **({"config": worker.config} if worker.config else {}),
        }
        for worker in workers
    ]


def _sanitize_display_output_for_toml(
    display_output: dict[str, object] | None,
) -> dict[str, object] | None:
    """Strip transient fields before persisting cell display metadata."""
    if display_output is None:
        return None

    persisted = dict(display_output)
    persisted.pop("inline_data_url", None)
    persisted.pop("file", None)
    persisted.pop("markdown_text", None)
    return {key: value for key, value in persisted.items() if value is not None}


def _sanitize_display_outputs_for_toml(
    display_outputs: list[dict[str, object]] | None,
) -> list[dict[str, object]]:
    """Strip transient fields from a display output list before persistence."""
    if not display_outputs:
        return []

    persisted_outputs: list[dict[str, object]] = []
    for display_output in display_outputs:
        persisted = _sanitize_display_output_for_toml(display_output)
        if persisted:
            persisted_outputs.append(persisted)
    return persisted_outputs


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
                **({"worker": cell.worker} if cell.worker is not None else {}),
                **({"timeout": cell.timeout} if cell.timeout is not None else {}),
                **({"env": _serialize_env(cell.env)} if cell.env else {}),
                "mounts": _serialize_mounts(cell.mounts),
            }
            for cell in toml.cells
        ],
        **({"worker": toml.worker} if toml.worker is not None else {}),
        **({"timeout": toml.timeout} if toml.timeout is not None else {}),
        **({"env": _serialize_env(toml.env)} if toml.env else {}),
        "workers": _serialize_workers(toml.workers),
        "mounts": _serialize_mounts(toml.mounts),
        **({"ai": toml.ai} if toml.ai else {}),
        "artifacts": toml.artifacts,
        "environment": toml.environment,
        "cache": toml.cache,
    }

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def create_notebook(
    parent_dir: Path,
    name: str,
    python_version: str | None = None,
    *,
    initialize_environment: bool = True,
) -> Path:
    """Create a new notebook directory with notebook.toml and pyproject.toml.

    Args:
        parent_dir: Parent directory for the notebook
        name: Notebook name (used for folder and notebook name)
        python_version: Requested notebook Python major.minor version
        initialize_environment: Whether to create the notebook venv immediately

    Returns:
        Path to created notebook directory
    """
    parent_dir = Path(parent_dir)
    parent_dir.mkdir(parents=True, exist_ok=True)
    requested_python_version = (
        normalize_python_minor(python_version)
        if python_version is not None
        else current_python_minor()
    )

    # Validate notebook name
    if "/" in name or "\\" in name or ".." in name or "\0" in name:
        raise ValueError("Notebook name contains invalid characters")

    # Create notebook directory (slugify the name)
    notebook_dir = parent_dir / name.lower().replace(" ", "_")
    notebook_dir.mkdir(exist_ok=True)

    # Create cells subdirectory
    cells_dir = notebook_dir / "cells"
    cells_dir.mkdir(exist_ok=True)

    # Preserve existing notebook ID if notebook.toml already exists.
    # Overwriting the ID would orphan all artifacts keyed to the old ID.
    existing_toml_path = notebook_dir / "notebook.toml"
    existing_notebook_id = None
    existing_created_at = None
    existing_cells: list[CellMeta] = []
    if existing_toml_path.exists():
        try:
            import tomllib

            with open(existing_toml_path, "rb") as f:
                raw = tomllib.load(f)
            existing_notebook_id = raw.get("notebook_id")
            raw_created = raw.get("created_at")
            if isinstance(raw_created, datetime):
                existing_created_at = raw_created
            for c in raw.get("cells", []):
                if isinstance(c, dict) and "id" in c and "file" in c:
                    existing_cells.append(
                        CellMeta(
                            id=c["id"],
                            file=c["file"],
                            language=c.get("language", "python"),
                            order=c.get("order", 0),
                        )
                    )
        except Exception:
            pass

    notebook_id = existing_notebook_id or str(uuid.uuid4())

    # Create notebook.toml
    now = datetime.now(tz=UTC)
    notebook_toml = NotebookToml(
        notebook_id=notebook_id,
        name=name,
        created_at=existing_created_at or now,
        updated_at=now,
        cells=existing_cells,
    )
    write_notebook_toml(notebook_dir, notebook_toml)

    # Create pyproject.toml (minimal)
    pyproject_content = f'''[project]
name = "{name.lower().replace(" ", "-")}"
version = "0.1.0"
description = ""
requires-python = "{format_requires_python(requested_python_version)}"
dependencies = [
    "pyarrow>=18.0.0",
]

[tool.uv]
'''

    with open(notebook_dir / "pyproject.toml", "w", encoding="utf-8") as f:
        f.write(pyproject_content)

    if initialize_environment:
        # Run uv sync to create venv + uv.lock (best-effort)
        _uv_sync(notebook_dir, python_version=requested_python_version)

        # Populate environment section with lockfile hash + python version
        _update_environment_metadata(notebook_dir)

    return notebook_dir


_logger = logging.getLogger(__name__)


def _uv_sync(notebook_dir: Path, *, timeout: int = 60, python_version: str | None = None) -> bool:
    """Run ``uv sync`` in *notebook_dir*.

    Returns True on success, False on failure (logged, never raised).
    """
    command = ["uv", "sync"]
    if python_version is not None:
        command.extend(["--python", normalize_python_minor(python_version)])
    try:
        subprocess.run(
            command,
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
    from strata.notebook.dependencies import list_dependencies
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

    requested_python_version = read_requested_python_minor(notebook_dir) or ""
    runtime_python_version = ""
    venv_python = notebook_dir / ".venv" / "bin" / "python"
    if venv_python.exists():
        runtime_python_version = read_venv_runtime_python_version(venv_python) or ""
        if not runtime_python_version:
            try:
                result = subprocess.run(
                    [
                        str(venv_python),
                        "-c",
                        (
                            "import sys; "
                            "print("
                            "f'{sys.version_info.major}."
                            "{sys.version_info.minor}."
                            "{sys.version_info.micro}'"
                            ")"
                        ),
                    ],
                    cwd=str(notebook_dir),
                    capture_output=True,
                    check=True,
                    text=True,
                    timeout=10,
                )
                runtime_python_version = result.stdout.strip()
            except Exception:
                _logger.debug("Failed to probe notebook venv python version", exc_info=True)

    resolved_package_count = 0
    lock_path = notebook_dir / "uv.lock"
    if lock_path.exists():
        try:
            with open(lock_path, "rb") as f:
                lock_data = tomllib.load(f)
            packages = lock_data.get("package", [])
            resolved_package_count = len(packages) if isinstance(packages, list) else 0
        except Exception:
            _logger.debug("Failed to parse uv.lock for env metadata", exc_info=True)

    declared_package_count = len(list_dependencies(notebook_dir))
    data["environment"] = {
        "requested_python_version": requested_python_version,
        "runtime_python_version": runtime_python_version,
        "lockfile_hash": compute_lockfile_hash(notebook_dir),
        "python_version": runtime_python_version,
        "package_count": declared_package_count,
        "declared_package_count": declared_package_count,
        "resolved_package_count": resolved_package_count,
        "has_lockfile": lock_path.exists(),
        "last_synced_at": int(time.time() * 1000),
    }

    with open(toml_path, "wb") as f:
        tomli_w.dump(data, f)


def update_environment_metadata(notebook_dir: Path) -> None:
    """Public API: refresh ``[environment]`` in ``notebook.toml``.

    Called after ``uv add`` / ``uv remove`` to persist the new lockfile hash.
    """
    _update_environment_metadata(notebook_dir)


def add_cell_to_notebook(
    notebook_dir: Path,
    cell_id: str,
    after_cell_id: str | None = None,
    language: str = "python",
) -> None:
    """Add a new cell to the notebook.

    Args:
        notebook_dir: Path to notebook directory
        cell_id: New cell ID
        after_cell_id: Cell ID to add after (None = at end)
        language: Cell language ("python" or "prompt")
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
            "language": language,
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
    normalized_name = new_name.strip()

    if not normalized_name:
        raise ValueError("Notebook name cannot be empty")
    if (
        "/" in normalized_name
        or "\\" in normalized_name
        or ".." in normalized_name
        or "\0" in normalized_name
    ):
        raise ValueError("Notebook name contains invalid characters")

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    toml_data["name"] = normalized_name
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def delete_notebook_directory(notebook_dir: Path) -> None:
    """Delete a notebook directory and all notebook-owned runtime state."""
    notebook_dir = Path(notebook_dir).resolve()
    notebook_toml_path = notebook_dir / "notebook.toml"

    if notebook_dir.is_symlink():
        raise ValueError("Refusing to delete a symlinked notebook directory")
    if not notebook_dir.exists():
        raise FileNotFoundError(f"Notebook directory not found: {notebook_dir}")
    if not notebook_dir.is_dir():
        raise ValueError(f"Notebook path is not a directory: {notebook_dir}")
    if not notebook_toml_path.is_file():
        raise ValueError(f"Notebook directory missing notebook.toml: {notebook_dir}")

    shutil.rmtree(notebook_dir)


def update_notebook_mounts(notebook_dir: Path, mounts: list[MountSpec]) -> None:
    """Persist notebook-level mount defaults."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    toml_data["mounts"] = _serialize_mounts(mounts)
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def update_notebook_worker(notebook_dir: Path, worker: str | None) -> None:
    """Persist the notebook-level default worker."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    if worker is None:
        toml_data.pop("worker", None)
    else:
        toml_data["worker"] = worker
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def update_notebook_workers(notebook_dir: Path, workers: list[WorkerSpec]) -> None:
    """Persist notebook-scoped worker definitions."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    toml_data["workers"] = _serialize_workers(workers)
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def update_notebook_timeout(notebook_dir: Path, timeout: float | None) -> None:
    """Persist the notebook-level default timeout."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    if timeout is None:
        toml_data.pop("timeout", None)
    else:
        toml_data["timeout"] = timeout
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def update_notebook_env(notebook_dir: Path, env: dict[str, str]) -> None:
    """Persist notebook-level default environment variables."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    if env:
        toml_data["env"] = _serialize_env(env)
    else:
        toml_data.pop("env", None)
    toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()

    with open(notebook_toml_path, "wb") as f:
        tomli_w.dump(toml_data, f)


def update_cell_mounts(
    notebook_dir: Path,
    cell_id: str,
    mounts: list[MountSpec],
) -> None:
    """Persist cell-level mount overrides."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    cells_data = toml_data.get("cells", [])
    for cell in cells_data:
        if cell.get("id") == cell_id:
            cell["mounts"] = _serialize_mounts(mounts)
            toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()
            with open(notebook_toml_path, "wb") as out:
                tomli_w.dump(toml_data, out)
            return

    raise ValueError(f"Cell {cell_id} not found")


def update_cell_worker(
    notebook_dir: Path,
    cell_id: str,
    worker: str | None,
) -> None:
    """Persist a cell-level worker override."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    cells_data = toml_data.get("cells", [])
    for cell in cells_data:
        if cell.get("id") == cell_id:
            if worker is None:
                cell.pop("worker", None)
            else:
                cell["worker"] = worker
            toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()
            with open(notebook_toml_path, "wb") as out:
                tomli_w.dump(toml_data, out)
            return

    raise ValueError(f"Cell {cell_id} not found")


def update_cell_timeout(
    notebook_dir: Path,
    cell_id: str,
    timeout: float | None,
) -> None:
    """Persist a cell-level timeout override."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    cells_data = toml_data.get("cells", [])
    for cell in cells_data:
        if cell.get("id") == cell_id:
            if timeout is None:
                cell.pop("timeout", None)
            else:
                cell["timeout"] = timeout
            toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()
            with open(notebook_toml_path, "wb") as out:
                tomli_w.dump(toml_data, out)
            return

    raise ValueError(f"Cell {cell_id} not found")


def update_cell_env(
    notebook_dir: Path,
    cell_id: str,
    env: dict[str, str],
) -> None:
    """Persist a cell-level environment override map."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    cells_data = toml_data.get("cells", [])
    for cell in cells_data:
        if cell.get("id") == cell_id:
            if env:
                cell["env"] = _serialize_env(env)
            else:
                cell.pop("env", None)
            toml_data["updated_at"] = datetime.now(tz=UTC).isoformat()
            with open(notebook_toml_path, "wb") as out:
                tomli_w.dump(toml_data, out)
            return

    raise ValueError(f"Cell {cell_id} not found")


def update_cell_display_outputs(
    notebook_dir: Path,
    cell_id: str,
    display_outputs: list[dict[str, object]] | None,
) -> None:
    """Persist or clear ordered display output metadata for a cell."""
    notebook_dir = Path(notebook_dir)
    notebook_toml_path = notebook_dir / "notebook.toml"

    with open(notebook_toml_path, "rb") as f:
        toml_data = tomllib.load(f)

    artifacts_data = toml_data.get("artifacts", {})
    if not isinstance(artifacts_data, dict):
        artifacts_data = {}

    raw_cell_artifacts = artifacts_data.get(cell_id, {})
    cell_artifacts = dict(raw_cell_artifacts) if isinstance(raw_cell_artifacts, dict) else {}
    persisted_displays = _sanitize_display_outputs_for_toml(display_outputs)

    if persisted_displays:
        cell_artifacts["display_outputs"] = persisted_displays
        cell_artifacts["display"] = persisted_displays[-1]
    else:
        cell_artifacts.pop("display_outputs", None)
        cell_artifacts.pop("display", None)

    if cell_artifacts:
        artifacts_data[cell_id] = cell_artifacts
    else:
        artifacts_data.pop(cell_id, None)

    toml_data["artifacts"] = artifacts_data

    with open(notebook_toml_path, "wb") as out:
        tomli_w.dump(toml_data, out)


def update_cell_console_output(
    notebook_dir: Path,
    cell_id: str,
    stdout: str,
    stderr: str,
) -> None:
    """Persist stdout/stderr for a cell so they survive notebook reopens.

    Written to ``.strata/console/{cell_id}.json`` — separate from
    notebook.toml to keep configuration and runtime state apart.
    Truncated to 10 KB per stream.
    """
    max_len = 10_000
    console_dir = Path(notebook_dir) / ".strata" / "console"
    console_dir.mkdir(parents=True, exist_ok=True)

    console_file = console_dir / f"{cell_id}.json"
    if stdout or stderr:
        import json

        with open(console_file, "w", encoding="utf-8") as f:
            json.dump(
                {"stdout": stdout[:max_len], "stderr": stderr[:max_len]},
                f,
            )
    elif console_file.exists():
        console_file.unlink()


def load_cell_console_output(notebook_dir: Path, cell_id: str) -> tuple[str, str]:
    """Load persisted stdout/stderr for a cell.

    Returns ``(stdout, stderr)`` — empty strings if nothing is persisted.
    """
    console_file = Path(notebook_dir) / ".strata" / "console" / f"{cell_id}.json"
    if not console_file.exists():
        return "", ""
    try:
        import json

        with open(console_file, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("stdout", ""), data.get("stderr", "")
    except Exception:
        return "", ""


def update_cell_display_output(
    notebook_dir: Path,
    cell_id: str,
    display_output: dict[str, object] | None,
) -> None:
    """Backward-compatible wrapper for persisting a single display output."""
    update_cell_display_outputs(
        notebook_dir,
        cell_id,
        [display_output] if display_output is not None else None,
    )
