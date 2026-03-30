"""FastAPI router for notebook endpoints."""

from __future__ import annotations

import io
import json
import logging
import re
import uuid
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from strata.notebook.dependencies import (
    list_dependencies,
)
from strata.notebook.executor import CellExecutor
from strata.notebook.models import CellStatus, MountSpec, WorkerSpec
from strata.notebook.session import SessionManager
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    remove_cell_from_notebook,
    rename_notebook,
    reorder_cells,
    update_cell_env,
    update_cell_mounts,
    update_cell_timeout,
    update_cell_worker,
    update_notebook_env,
    update_notebook_mounts,
    update_notebook_timeout,
    update_notebook_worker,
    update_notebook_workers,
    write_cell,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Global session manager (shared with WebSocket handler)
_session_manager = SessionManager()

router = APIRouter(prefix="/v1/notebooks", tags=["notebooks"])


def get_session_manager() -> SessionManager:
    """Export session manager for WebSocket handler."""
    return _session_manager


def validate_package_name(package: str) -> str:
    """Validate and sanitize a package specifier.

    Rejects shell metacharacters. Used by both REST and WS handlers.
    """
    if len(package) > 200:
        raise ValueError("Package specifier too long")
    if any(c in package for c in ';&|`$(){}!<>"\'\n\r\t'):
        raise ValueError("Package specifier contains invalid characters")
    return package.strip()


def _validate_notebook_path(user_path: str, label: str = "path") -> Path:
    """Validate that a user-supplied path is safe (no path traversal)."""
    path = Path(user_path)
    if ".." in path.parts:
        raise HTTPException(
            status_code=400, detail=f"Invalid {label}: path traversal not allowed"
        )
    return path.resolve()


def _safe_filename(name: str) -> str:
    """Sanitize a string for use in Content-Disposition."""
    safe = re.sub(r'[^\w\s.-]', '', name)
    safe = re.sub(r'\s+', '_', safe).strip('_') or 'notebook'
    return safe


def validate_env_vars(env: dict[str, str]) -> dict[str, str]:
    """Validate notebook env var keys and values."""
    validated: dict[str, str] = {}
    for key, value in env.items():
        normalized_key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", normalized_key):
            raise ValueError(f"Invalid env var name: {key}")
        if any(c in value for c in "\0\r\n"):
            raise ValueError(f"Invalid env var value for {key}")
        validated[normalized_key] = value
    return validated


# ============================================================================
# Request/Response Models
# ============================================================================


class OpenNotebookRequest(BaseModel):
    """Request to open a notebook."""

    path: str = "..."


class CreateNotebookRequest(BaseModel):
    """Request to create a new notebook."""

    parent_path: str
    name: str


class UpdateCellSourceRequest(BaseModel):
    """Request to update cell source."""

    source: str = Field(..., max_length=1_000_000)  # 1MB limit


class AddCellRequest(BaseModel):
    """Request to add a new cell."""

    after_cell_id: str | None = None


class MountConfigRequest(BaseModel):
    """Request to replace a mount list."""

    mounts: list[MountSpec] = Field(default_factory=list)


class WorkerConfigRequest(BaseModel):
    """Request to replace a worker setting."""

    worker: str | None = Field(default=None, max_length=200)

    @field_validator("worker")
    @classmethod
    def normalize_worker(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class WorkersConfigRequest(BaseModel):
    """Request to replace notebook-scoped worker definitions."""

    workers: list[WorkerSpec] = Field(default_factory=list)


class TimeoutConfigRequest(BaseModel):
    """Request to replace a timeout setting."""

    timeout: float | None = Field(default=None, gt=0, le=86_400)


class EnvConfigRequest(BaseModel):
    """Request to replace env vars."""

    env: dict[str, str] = Field(default_factory=dict)

    @field_validator("env")
    @classmethod
    def validate_env_field(cls, value: dict[str, str]) -> dict[str, str]:
        return validate_env_vars(value)


class ReorderCellsRequest(BaseModel):
    """Request to reorder cells."""

    cell_ids: list[str]


class RenameNotebookRequest(BaseModel):
    """Request to rename notebook."""

    name: str


class AddDependencyRequest(BaseModel):
    """Request to add a dependency."""

    package: str = Field(..., max_length=200)  # e.g. "requests" or "pandas>=2.0"

    @field_validator("package")
    @classmethod
    def validate_package_field(cls, v: str) -> str:
        return validate_package_name(v)


class RemoveDependencyRequest(BaseModel):
    """Request to remove a dependency."""

    package: str = Field(..., max_length=200)

    @field_validator("package")
    @classmethod
    def validate_package_field(cls, v: str) -> str:
        return validate_package_name(v)


# ============================================================================
# Endpoints
# ============================================================================


@router.post("/open")
async def open_notebook(req: OpenNotebookRequest) -> dict:
    """Open a notebook directory.

    Args:
        req: OpenNotebookRequest with path

    Returns:
        Notebook state, session ID, and DAG as JSON
    """
    notebook_path = _validate_notebook_path(req.path, "notebook path")
    if not notebook_path.exists():
        raise HTTPException(status_code=404, detail="Notebook directory not found")

    try:
        # Create session (parses notebook and triggers DAG analysis)
        session = _session_manager.open_notebook(notebook_path)

        # Return notebook state with session ID and DAG
        data = session.serialize_notebook_state()
        data["session_id"] = session.id
        data["dag"] = _format_dag(session)
        return data
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/create")
async def create_new_notebook(req: CreateNotebookRequest) -> dict:
    """Create a new notebook.

    Args:
        req: CreateNotebookRequest with parent_path and name

    Returns:
        Notebook state as JSON
    """
    try:
        parent_path = _validate_notebook_path(req.parent_path, "parent path")
        notebook_dir = create_notebook(parent_path, req.name)
        session = _session_manager.open_notebook(notebook_dir)

        data = session.serialize_notebook_state()
        data["session_id"] = session.id
        return data
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/cells/reorder")
async def reorder_notebook_cells(
    notebook_id: str, req: ReorderCellsRequest
) -> dict:
    """Reorder cells in the notebook."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        reorder_cells(session.path, req.cell_ids)
        session.reload()
        return {
            "notebook_id": session.notebook_state.id,
            "cells": session.serialize_cells(),
        }
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{notebook_id}/cells")
async def list_cells(notebook_id: str) -> dict:
    """List cells in a notebook.

    Args:
        notebook_id: Notebook/session ID

    Returns:
        List of cells with source
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    return {
        "notebook_id": session.notebook_state.id,
        "cells": session.serialize_cells(),
    }


@router.put("/{notebook_id}/cells/{cell_id}")
async def update_cell_source(
    notebook_id: str, cell_id: str, req: UpdateCellSourceRequest
) -> dict:
    """Update cell source code.

    Args:
        notebook_id: Notebook/session ID
        cell_id: Cell ID
        req: UpdateCellSourceRequest with source

    Returns:
        Updated cell state and DAG
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        # Write to disk
        write_cell(session.path, cell_id, req.source)

        # Update source in session
        cell_in_session = next(
            (c for c in session.notebook_state.cells if c.id == cell_id), None
        )
        if cell_in_session:
            cell_in_session.source = req.source

        # Re-analyze just this cell and rebuild DAG
        session.re_analyze_cell(cell_id)

        # Recompute staleness so cell statuses reflect the edit.
        # Without this, cells keep their old "ready" status and the
        # cascade planner won't trigger when the user runs a
        # downstream cell.
        session.compute_staleness()

        # Find and return the updated cell with DAG info
        cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
        if not cell:
            raise HTTPException(status_code=404, detail="Cell not found")

        # Return cell and updated DAG — include all cells so the
        # frontend can sync staleness/status changes.
        return {
            "cell": session.serialize_cell(cell),
            "dag": _format_dag(session),
            "cells": session.serialize_cells(),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/mounts")
async def update_notebook_mounts_endpoint(
    notebook_id: str,
    req: MountConfigRequest,
) -> dict:
    """Replace notebook-level mount defaults."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        update_notebook_mounts(session.path, req.mounts)
        session.reload()
        return {
            "mounts": [mount.model_dump() for mount in session.notebook_state.mounts],
            "cells": session.serialize_cells(),
        }
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/cells/{cell_id}/mounts")
async def update_cell_mounts_endpoint(
    notebook_id: str,
    cell_id: str,
    req: MountConfigRequest,
) -> dict:
    """Replace cell-level mount overrides."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        update_cell_mounts(session.path, cell_id, req.mounts)
        session.reload()
        cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
        if cell is None:
            raise HTTPException(status_code=404, detail="Cell not found")
        return {
            "cell": session.serialize_cell(cell),
            "mounts": [mount.model_dump() for mount in cell.mount_overrides],
            "cells": session.serialize_cells(),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{notebook_id}/workers")
async def list_notebook_workers(notebook_id: str) -> dict:
    """List the worker catalog visible to a notebook."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    return {"workers": session.serialize_worker_catalog()}


@router.put("/{notebook_id}/workers")
async def update_notebook_workers_endpoint(
    notebook_id: str,
    req: WorkersConfigRequest,
) -> dict:
    """Replace notebook-scoped worker definitions."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        update_notebook_workers(session.path, req.workers)
        session.reload()
        return {
            "configured_workers": [
                worker.model_dump() for worker in session.notebook_state.workers
            ],
            "workers": session.serialize_worker_catalog(),
        }
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/worker")
async def update_notebook_worker_endpoint(
    notebook_id: str,
    req: WorkerConfigRequest,
) -> dict:
    """Replace the notebook-level default worker."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        update_notebook_worker(session.path, req.worker)
        session.reload()
        return {
            "worker": session.notebook_state.worker,
            "workers": session.serialize_worker_catalog(),
            "cells": session.serialize_cells(),
        }
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/timeout")
async def update_notebook_timeout_endpoint(
    notebook_id: str,
    req: TimeoutConfigRequest,
) -> dict:
    """Replace the notebook-level default timeout."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        update_notebook_timeout(session.path, req.timeout)
        session.reload()
        return {
            "timeout": session.notebook_state.timeout,
            "cells": session.serialize_cells(),
        }
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/env")
async def update_notebook_env_endpoint(
    notebook_id: str,
    req: EnvConfigRequest,
) -> dict:
    """Replace the notebook-level default env vars."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        update_notebook_env(session.path, req.env)
        session.reload()
        return {
            "env": session.notebook_state.env,
            "cells": session.serialize_cells(),
        }
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/cells/{cell_id}/worker")
async def update_cell_worker_endpoint(
    notebook_id: str,
    cell_id: str,
    req: WorkerConfigRequest,
) -> dict:
    """Replace a cell-level worker override."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        update_cell_worker(session.path, cell_id, req.worker)
        session.reload()
        cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
        if cell is None:
            raise HTTPException(status_code=404, detail="Cell not found")
        return {
            "cell": session.serialize_cell(cell),
            "worker": cell.worker_override,
            "workers": session.serialize_worker_catalog(),
            "cells": session.serialize_cells(),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/cells/{cell_id}/timeout")
async def update_cell_timeout_endpoint(
    notebook_id: str,
    cell_id: str,
    req: TimeoutConfigRequest,
) -> dict:
    """Replace a cell-level timeout override."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        update_cell_timeout(session.path, cell_id, req.timeout)
        session.reload()
        cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
        if cell is None:
            raise HTTPException(status_code=404, detail="Cell not found")
        return {
            "cell": session.serialize_cell(cell),
            "timeout": cell.timeout_override,
            "cells": session.serialize_cells(),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/cells/{cell_id}/env")
async def update_cell_env_endpoint(
    notebook_id: str,
    cell_id: str,
    req: EnvConfigRequest,
) -> dict:
    """Replace a cell-level env override map."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        update_cell_env(session.path, cell_id, req.env)
        session.reload()
        cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
        if cell is None:
            raise HTTPException(status_code=404, detail="Cell not found")
        return {
            "cell": session.serialize_cell(cell),
            "env": cell.env_overrides,
            "cells": session.serialize_cells(),
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/{notebook_id}/cells")
async def add_cell(notebook_id: str, req: AddCellRequest) -> dict:
    """Add a new cell to the notebook.

    Args:
        notebook_id: Notebook/session ID
        req: AddCellRequest with optional after_cell_id

    Returns:
        New cell state
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        # Generate cell ID
        cell_id = str(uuid.uuid4())[:8]

        # Add to notebook
        add_cell_to_notebook(session.path, cell_id, req.after_cell_id)

        # Reload notebook state
        session.reload()

        # Find and return the new cell
        cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
        if not cell:
            raise HTTPException(status_code=500, detail="Failed to create cell")

        return session.serialize_cell(cell)
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/{notebook_id}/cells/{cell_id}")
async def delete_cell(notebook_id: str, cell_id: str) -> dict:
    """Delete a cell from the notebook.

    Args:
        notebook_id: Notebook/session ID
        cell_id: Cell ID to delete

    Returns:
        Success message
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        # Check if cell exists before deleting
        if not any(c.id == cell_id for c in session.notebook_state.cells):
            raise HTTPException(status_code=404, detail="Cell not found")

        # Remove from notebook
        remove_cell_from_notebook(session.path, cell_id)

        # Reload notebook state
        session.reload()

        return {"message": "Cell deleted", "cell_id": cell_id}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/{notebook_id}/name")
async def rename_notebook_endpoint(
    notebook_id: str, req: RenameNotebookRequest
) -> dict:
    """Rename the notebook.

    Args:
        notebook_id: Notebook/session ID
        req: RenameNotebookRequest with name

    Returns:
        Updated notebook state
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        rename_notebook(session.path, req.name)

        # Reload notebook state
        session.reload()

        return {
            "notebook_id": session.notebook_state.id,
            "name": session.notebook_state.name,
        }
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{notebook_id}/dag")
async def get_notebook_dag(notebook_id: str) -> dict:
    """Get the DAG for a notebook.

    Args:
        notebook_id: Notebook/session ID

    Returns:
        DAG edges, topological order, leaves, roots, and per-cell metadata
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    return _format_dag(session)


@router.get("/{notebook_id}/dependencies")
async def get_dependencies(notebook_id: str) -> dict:
    """List current dependencies for a notebook.

    Returns:
        List of dependencies from pyproject.toml
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    deps = list_dependencies(session.path)
    return {
        "dependencies": [
            {"name": d.name, "version": d.version, "specifier": d.specifier}
            for d in deps
        ],
    }


@router.post("/{notebook_id}/dependencies")
async def add_notebook_dependency(
    notebook_id: str, req: AddDependencyRequest
) -> dict:
    """Add a dependency to the notebook.

    Runs ``uv add``, updates pyproject.toml + uv.lock, syncs venv.
    If the lockfile changes, the session's venv_python is re-synced.
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    outcome = await session.mutate_dependency(req.package, action="add")
    result = outcome.result

    if not result.success:
        raise HTTPException(status_code=400, detail=result.error or "Failed to add dependency")

    return {
        "success": True,
        "package": result.package,
        "lockfile_changed": result.lockfile_changed,
        "dependencies": [
            {"name": d.name, "version": d.version, "specifier": d.specifier}
            for d in result.dependencies
        ],
        "cells": session.serialize_cells(),
    }


@router.delete("/{notebook_id}/dependencies/{package_name}")
async def remove_notebook_dependency(
    notebook_id: str, package_name: str
) -> dict:
    """Remove a dependency from the notebook.

    Runs ``uv remove``, updates pyproject.toml + uv.lock, syncs venv.
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        package_name = validate_package_name(package_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    outcome = await session.mutate_dependency(package_name, action="remove")
    result = outcome.result

    if not result.success:
        raise HTTPException(status_code=400, detail=result.error or "Failed to remove dependency")

    return {
        "success": True,
        "package": result.package,
        "lockfile_changed": result.lockfile_changed,
        "dependencies": [
            {"name": d.name, "version": d.version, "specifier": d.specifier}
            for d in result.dependencies
        ],
        "cells": session.serialize_cells(),
    }


def _format_dag(session) -> dict:
    """Format the DAG for API response.

    Args:
        session: NotebookSession

    Returns:
        DAG data as dict
    """
    if not session.dag:
        return {
            "edges": [],
            "topological_order": [],
            "leaves": [],
            "roots": [],
            "variable_producer": {},
        }

    return {
        "edges": [
            {
                "from_cell_id": edge.from_cell_id,
                "to_cell_id": edge.to_cell_id,
                "variable": edge.variable,
            }
            for edge in session.dag.edges
        ],
        "topological_order": session.dag.topological_order,
        "leaves": list(session.dag.leaves),
        "roots": list(session.dag.roots),
        "variable_producer": session.dag.variable_producer,
    }


@router.post("/{notebook_id}/cells/{cell_id}/execute")
async def execute_cell(notebook_id: str, cell_id: str) -> dict:
    """Execute a cell and return results.

    Args:
        notebook_id: Notebook/session ID
        cell_id: Cell ID to execute

    Returns:
        Execution result with outputs and stdout/stderr
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    # Find the cell
    cell = next(
        (c for c in session.notebook_state.cells if c.id == cell_id), None
    )
    if not cell:
        raise HTTPException(status_code=404, detail="Cell not found")

    try:
        # Execute the cell
        cell.status = CellStatus.RUNNING
        executor = CellExecutor(session, session.warm_pool)
        result = await executor.execute_cell(cell_id, cell.source)
        session.record_execution(cell_id, result.duration_ms, result.cache_hit)
        if result.success:
            session.compute_staleness()
            session.mark_executed_ready(cell_id)
        else:
            cell.status = CellStatus.ERROR

        return result.to_dict()
    except Exception:
        cell.status = CellStatus.ERROR
        logger.exception("Cell execution failed")
        raise HTTPException(
            status_code=500, detail="Execution failed"
        )


@router.get("/{notebook_id}/export")
async def export_notebook(notebook_id: str) -> StreamingResponse:
    """Export a reproducible notebook bundle as a zip archive.

    The zip contains:
    - notebook.toml
    - pyproject.toml
    - uv.lock (if present)
    - cells/*.py
    - provenance.json (DAG + per-cell provenance hashes)

    This is everything needed to reproduce the notebook environment
    and computations on another machine.
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    nb_dir = session.path
    buf = io.BytesIO()

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # notebook.toml
        toml_path = nb_dir / "notebook.toml"
        if toml_path.exists():
            zf.write(toml_path, "notebook.toml")

        # pyproject.toml
        pyproject_path = nb_dir / "pyproject.toml"
        if pyproject_path.exists():
            zf.write(pyproject_path, "pyproject.toml")

        # uv.lock
        lock_path = nb_dir / "uv.lock"
        if lock_path.exists():
            zf.write(lock_path, "uv.lock")

        # Cell source files
        cells_dir = nb_dir / "cells"
        if cells_dir.is_dir():
            for cell_file in sorted(cells_dir.glob("*.py")):
                zf.write(cell_file, f"cells/{cell_file.name}")

        # provenance.json — DAG + per-cell hashes for reproducibility
        from strata.notebook.env import compute_lockfile_hash
        from strata.notebook.provenance import compute_source_hash

        provenance: dict = {
            "notebook_id": session.notebook_state.id,
            "lockfile_hash": compute_lockfile_hash(nb_dir),
            "dag": _format_dag(session),
            "cells": {},
        }
        for cell in session.notebook_state.cells:
            provenance["cells"][cell.id] = {
                "source_hash": compute_source_hash(cell.source),
                "defines": cell.defines,
                "references": cell.references,
                "status": cell.status,
                "artifact_uri": cell.artifact_uri,
            }
        zf.writestr("provenance.json", json.dumps(provenance, indent=2))

    buf.seek(0)
    filename = f"{_safe_filename(session.notebook_state.name or 'notebook')}.zip"
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
