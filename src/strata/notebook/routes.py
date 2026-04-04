"""FastAPI router for notebook endpoints."""

from __future__ import annotations

import io
import json
import logging
import re
import time
import uuid
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator

from strata.notebook.dependencies import (
    export_requirements_text,
    list_dependencies,
    list_resolved_dependencies,
    preview_environment_yaml_text,
    preview_requirements_text,
)
from strata.notebook.executor import CellExecutor
from strata.notebook.models import CellStatus, MountSpec, WorkerSpec
from strata.notebook.python_versions import current_python_minor, normalize_python_minor
from strata.notebook.session import SessionManager
from strata.notebook.workers import (
    build_worker_catalog_with_health,
    notebook_worker_definitions_editable,
    validate_worker_assignment,
)
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
    from strata.notebook.session import NotebookSession

logger = logging.getLogger(__name__)

# Global session manager (shared with WebSocket handler)
_session_manager = SessionManager()

router = APIRouter(prefix="/v1/notebooks", tags=["notebooks"])


def get_session_manager() -> SessionManager:
    """Export session manager for WebSocket handler."""
    return _session_manager


def _require_personal_mode_session_api() -> None:
    """Restrict session discovery/reconnect APIs to personal mode.

    These endpoints expose in-memory session IDs and notebook filesystem paths.
    They are intended as a local UX helper for page refresh/reconnect, not as a
    multi-user service-mode surface.
    """
    try:
        from strata.server import get_state

        state = get_state()
    except RuntimeError:
        # Route unit tests may use the notebook router without a full server state.
        return

    if state.config.deployment_mode != "personal":
        raise HTTPException(
            status_code=403,
            detail="Notebook session APIs are only available in personal mode",
        )


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


async def _serialize_worker_catalog(
    session: NotebookSession,
    *,
    force_refresh: bool = False,
) -> dict:
    return {
        "workers": await build_worker_catalog_with_health(
            session.notebook_state,
            force_refresh=force_refresh,
        ),
        "definitions_editable": notebook_worker_definitions_editable(
            session.notebook_state
        ),
        "health_checked_at": int(time.time() * 1000),
    }


def _serialize_environment_change(session: NotebookSession, staleness_map: dict) -> dict:
    """Summarize environment change impact for sidebar UX."""
    stale_cell_ids = [
        cell_id
        for cell_id, staleness in staleness_map.items()
        if staleness.status != CellStatus.READY
    ]
    return {
        "stale_cell_count": len(stale_cell_ids),
        "stale_cell_ids": stale_cell_ids,
        "warm_pool_reset": session.warm_pool is not None,
    }


def _serialize_notebook_runtime_config() -> dict:
    """Serialize frontend-relevant notebook runtime defaults."""
    deployment_mode = "service"
    default_parent_path = Path("/tmp/strata-notebooks")
    available_python_versions = [current_python_minor()]

    try:
        from strata.server import get_state

        state = get_state()
        deployment_mode = getattr(state.config, "deployment_mode", deployment_mode)
        configured_path = getattr(state.config, "notebook_storage_dir", None)
        if configured_path is not None:
            default_parent_path = Path(configured_path)
        configured_versions = getattr(state.config, "notebook_python_versions", None)
        if isinstance(configured_versions, list) and configured_versions:
            available_python_versions = [
                normalize_python_minor(str(version)) for version in configured_versions
            ]
    except RuntimeError:
        pass

    return {
        "deployment_mode": deployment_mode,
        "default_parent_path": str(default_parent_path),
        "available_python_versions": available_python_versions,
        "default_python_version": available_python_versions[0],
        "python_selection_fixed": len(available_python_versions) <= 1,
    }


def _serialize_dependency_info_list(dependencies: list) -> list[dict]:
    """Serialize dependency metadata for API responses."""
    return [
        {"name": dep.name, "version": dep.version, "specifier": dep.specifier}
        for dep in dependencies
    ]


def _serialize_environment_payload(session: NotebookSession) -> dict:
    """Serialize the current environment plus direct and resolved dependencies."""
    return {
        "environment": session.serialize_environment_state(),
        "environment_job": session.serialize_environment_job_state(),
        "environment_job_history": session.serialize_environment_job_history(),
        "dependencies": _serialize_dependency_info_list(list_dependencies(session.path)),
        "resolved_dependencies": _serialize_dependency_info_list(
            list_resolved_dependencies(session.path)
        ),
    }


def _serialize_import_preview(result) -> dict:
    """Serialize an import preview response."""
    return {
        "preview_dependencies": _serialize_dependency_info_list(result.dependencies),
        "normalized_requirements": result.normalized_requirements,
        "imported_count": result.imported_count,
        "warnings": list(result.warnings),
        "additions": _serialize_dependency_info_list(result.additions),
        "removals": _serialize_dependency_info_list(result.removals),
        "unchanged": _serialize_dependency_info_list(result.unchanged),
    }


def _serialize_environment_operation_log(raw: object | None) -> dict | None:
    """Serialize structured uv command details for the UI."""
    if raw is None:
        return None

    return {
        "command": getattr(raw, "command", ""),
        "duration_ms": getattr(raw, "duration_ms", None),
        "stdout": getattr(raw, "stdout", ""),
        "stderr": getattr(raw, "stderr", ""),
        "stdout_truncated": getattr(raw, "stdout_truncated", False),
        "stderr_truncated": getattr(raw, "stderr_truncated", False),
    }


def _serialize_result_operation_log(result: object) -> dict:
    """Serialize operation log details from a dependency/import result."""
    operation_log = _serialize_environment_operation_log(
        getattr(result, "operation_log", None)
    )
    if operation_log is None:
        return {}
    return {"operation_log": operation_log}


def _serialize_operation_error_detail(message: str, result: object) -> dict:
    """Build a structured HTTP error detail payload with optional command logs."""
    detail: dict[str, object] = {"message": message}
    operation_log = _serialize_environment_operation_log(
        getattr(result, "operation_log", None)
    )
    if operation_log is not None:
        detail["operation_log"] = operation_log
    return detail


def _raise_environment_busy(session: NotebookSession, message: str) -> None:
    """Raise a structured 409 conflict for competing env/runtime operations."""
    raise HTTPException(
        status_code=409,
        detail={
            "message": message,
            "code": "ENVIRONMENT_BUSY",
            "environment_job": session.serialize_environment_job_state(),
        },
    )


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
    python_version: str | None = Field(default=None, max_length=16)

    @field_validator("python_version")
    @classmethod
    def validate_python_version_field(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return normalize_python_minor(value)


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


class EnvironmentJobRequest(BaseModel):
    """Request to submit a background environment job."""

    action: str = Field(..., max_length=32)
    package: str | None = Field(default=None, max_length=200)
    requirements: str | None = Field(default=None, max_length=500_000)
    environment_yaml: str | None = Field(default=None, max_length=500_000)

    @field_validator("action")
    @classmethod
    def validate_action_field(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"add", "remove", "sync", "import"}:
            raise ValueError("Unsupported environment job action")
        return normalized

    @field_validator("package")
    @classmethod
    def validate_package_field(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_package_name(value)


class ImportRequirementsRequest(BaseModel):
    """Request to import direct dependencies from requirements text."""

    requirements: str = Field(..., max_length=500_000)


class ImportEnvironmentYamlRequest(BaseModel):
    """Request to import dependencies from ``environment.yaml`` text."""

    environment_yaml: str = Field(..., max_length=500_000)


class PreviewRequirementsRequest(BaseModel):
    """Request to preview direct dependency import from requirements text."""

    requirements: str = Field(..., max_length=500_000)


class PreviewEnvironmentYamlRequest(BaseModel):
    """Request to preview dependency import from ``environment.yaml`` text."""

    environment_yaml: str = Field(..., max_length=500_000)


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
        data["path"] = str(session.path)
        data["dag"] = _format_dag(session)
        data.update(_serialize_notebook_runtime_config())
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
        runtime_config = _serialize_notebook_runtime_config()
        selected_python_version = req.python_version or runtime_config["default_python_version"]
        allowed_python_versions = runtime_config["available_python_versions"]
        if selected_python_version not in allowed_python_versions:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Python {selected_python_version} is not available for notebook creation"
                ),
            )

        notebook_dir = create_notebook(
            parent_path,
            req.name,
            python_version=selected_python_version,
        )
        session = _session_manager.open_notebook(notebook_dir)

        data = session.serialize_notebook_state()
        data["session_id"] = session.id
        data["path"] = str(session.path)
        data.update(runtime_config)
        return data
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{notebook_id}/environment")
async def get_environment_status(notebook_id: str) -> dict:
    """Get the live notebook environment status."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    return _serialize_environment_payload(session)


@router.get("/config")
async def get_notebook_runtime_config() -> dict:
    """Return frontend runtime defaults for notebook creation/open flows."""
    return _serialize_notebook_runtime_config()


@router.post("/{notebook_id}/environment/sync")
async def sync_environment(notebook_id: str) -> dict:
    """Re-sync the notebook environment and invalidate stale runtimes."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        session._begin_synchronous_environment_mutation("environment sync")
    except RuntimeError as exc:
        _raise_environment_busy(session, str(exc))

    try:
        old_hash = session.serialize_environment_state()["lockfile_hash"]
        staleness_map = await session.sync_environment()
        new_hash = session.serialize_environment_state()["lockfile_hash"]
    finally:
        session._end_synchronous_environment_mutation()

    return {
        **_serialize_environment_payload(session),
        "lockfile_changed": old_hash != new_hash,
        **_serialize_environment_change(session, staleness_map),
        "operation_log": {
            "command": "uv sync",
            "duration_ms": session.environment_last_sync_duration_ms,
            "stdout": "",
            "stderr": session.environment_sync_error or "",
            "stdout_truncated": False,
            "stderr_truncated": False,
        },
        "cells": session.serialize_cells(),
    }


@router.get("/{notebook_id}/environment/jobs/current")
async def get_current_environment_job(notebook_id: str) -> dict:
    """Return the currently active background environment job, if any."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")
    return {
        **_serialize_environment_payload(session),
        "cells": session.serialize_cells(),
    }


@router.post("/{notebook_id}/environment/jobs")
async def submit_environment_job(notebook_id: str, req: EnvironmentJobRequest) -> JSONResponse:
    """Submit a background notebook environment job."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    if req.action in {"add", "remove"} and not req.package:
        raise HTTPException(
            status_code=400,
            detail=f"Package is required for {req.action} environment jobs",
        )
    if req.action == "sync" and (
        req.package is not None
        or req.requirements is not None
        or req.environment_yaml is not None
    ):
        raise HTTPException(
            status_code=400,
            detail="Sync environment jobs do not accept package or import content",
        )
    if req.action in {"add", "remove"} and (
        req.requirements is not None or req.environment_yaml is not None
    ):
        raise HTTPException(
            status_code=400,
            detail=f"{req.action.title()} environment jobs do not accept import content",
        )
    if req.action == "import":
        provided_inputs = sum(
            value is not None for value in (req.requirements, req.environment_yaml)
        )
        if provided_inputs != 1 or req.package is not None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Import environment jobs require exactly one of requirements or "
                    "environment_yaml and do not accept a package"
                ),
            )

    try:
        await session.submit_environment_job(
            action=req.action,
            package=req.package,
            requirements_text=req.requirements,
            environment_yaml_text=req.environment_yaml,
        )
    except RuntimeError as exc:
        _raise_environment_busy(session, str(exc))

    return JSONResponse(
        status_code=202,
        content={
            "accepted": True,
            **_serialize_environment_payload(session),
            "cells": session.serialize_cells(),
        },
    )


@router.get("/{notebook_id}/environment/requirements.txt")
async def export_environment_requirements(notebook_id: str) -> PlainTextResponse:
    """Export direct notebook dependencies as ``requirements.txt`` text."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    filename = f"{_safe_filename(session.notebook_state.name)}-requirements.txt"
    return PlainTextResponse(
        export_requirements_text(session.path),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/{notebook_id}/environment/requirements.txt")
async def import_environment_requirements(
    notebook_id: str, req: ImportRequirementsRequest
) -> dict:
    """Replace direct notebook dependencies from ``requirements.txt`` text."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        session._begin_synchronous_environment_mutation("requirements import")
    except RuntimeError as exc:
        _raise_environment_busy(session, str(exc))

    try:
        try:
            outcome = await session.import_requirements(req.requirements)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    finally:
        session._end_synchronous_environment_mutation()

    result = outcome.result
    if not result.success:
        raise HTTPException(
            status_code=400,
            detail=_serialize_operation_error_detail(
                result.error or "Failed to import requirements.txt",
                result,
            ),
        )

    return {
        "success": True,
        "imported_count": result.imported_count,
        "lockfile_changed": result.lockfile_changed,
        **_serialize_result_operation_log(result),
        **_serialize_environment_payload(session),
        **_serialize_environment_change(session, outcome.staleness_map),
        "cells": session.serialize_cells(),
    }


@router.post("/{notebook_id}/environment/requirements.txt/preview")
async def preview_environment_requirements(
    notebook_id: str, req: PreviewRequirementsRequest
) -> dict:
    """Preview replacing direct notebook dependencies from ``requirements.txt`` text."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        result = preview_requirements_text(session.path, req.requirements)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {
        **_serialize_import_preview(result),
        **_serialize_environment_payload(session),
    }


@router.post("/{notebook_id}/environment/environment.yaml")
async def import_environment_yaml(
    notebook_id: str, req: ImportEnvironmentYamlRequest
) -> dict:
    """Best-effort import of Conda-style ``environment.yaml`` text."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        session._begin_synchronous_environment_mutation("environment.yaml import")
    except RuntimeError as exc:
        _raise_environment_busy(session, str(exc))

    try:
        try:
            outcome = await session.import_environment_yaml(req.environment_yaml)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
    finally:
        session._end_synchronous_environment_mutation()

    result = outcome.result
    if not result.success:
        raise HTTPException(
            status_code=400,
            detail=_serialize_operation_error_detail(
                result.error or "Failed to import environment.yaml",
                result,
            ),
        )

    return {
        "success": True,
        "imported_count": result.imported_count,
        "warnings": result.warnings,
        "lockfile_changed": result.lockfile_changed,
        **_serialize_result_operation_log(result),
        **_serialize_environment_payload(session),
        **_serialize_environment_change(session, outcome.staleness_map),
        "cells": session.serialize_cells(),
    }


@router.post("/{notebook_id}/environment/environment.yaml/preview")
async def preview_environment_yaml(
    notebook_id: str, req: PreviewEnvironmentYamlRequest
) -> dict:
    """Preview best-effort import of Conda-style ``environment.yaml`` text."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    try:
        result = preview_environment_yaml_text(session.path, req.environment_yaml)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {
        **_serialize_import_preview(result),
        **_serialize_environment_payload(session),
    }


@router.get("/sessions")
async def list_sessions() -> dict:
    """List all active notebook sessions.

    Returns:
        Dictionary with a ``sessions`` array, each entry containing
        session_id, notebook name, filesystem path, and timestamps.
    """
    _require_personal_mode_session_api()
    sessions = []
    for sid in _session_manager.list_sessions():
        session = _session_manager.get_session(sid)
        if session is None:
            continue
        sessions.append(
            {
                "session_id": session.id,
                "name": session.notebook_state.name,
                "path": str(session.path),
                "notebook_id": session.notebook_state.id,
                "created_at": session.notebook_state.created_at
                if hasattr(session.notebook_state, "created_at")
                else None,
                "updated_at": session.notebook_state.updated_at
                if hasattr(session.notebook_state, "updated_at")
                else None,
            }
        )
    return {"sessions": sessions}


@router.get("/sessions/{session_id}")
async def get_session(session_id: str) -> dict:
    """Get full state for an existing session.

    This allows the frontend to reconnect to a session after a page
    refresh without re-opening the notebook from disk.

    Args:
        session_id: UUID session identifier

    Returns:
        Notebook state, session ID, and DAG as JSON (same shape as
        the ``open`` endpoint response).
    """
    _require_personal_mode_session_api()
    session = _session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    data = session.serialize_notebook_state()
    data["session_id"] = session.id
    data["path"] = str(session.path)
    data["dag"] = _format_dag(session)
    data.update(_serialize_notebook_runtime_config())
    return data


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
async def list_notebook_workers(notebook_id: str, refresh: bool = False) -> dict:
    """List the worker catalog visible to a notebook."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    return await _serialize_worker_catalog(session, force_refresh=refresh)


@router.put("/{notebook_id}/workers")
async def update_notebook_workers_endpoint(
    notebook_id: str,
    req: WorkersConfigRequest,
) -> dict:
    """Replace notebook-scoped worker definitions."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")
    if not notebook_worker_definitions_editable(session.notebook_state):
        raise HTTPException(
            status_code=403,
            detail="Notebook worker definitions are managed by the server in service mode",
        )

    try:
        update_notebook_workers(session.path, req.workers)
        session.reload()
        return {
            "configured_workers": [
                worker.model_dump() for worker in session.notebook_state.workers
            ],
            **await _serialize_worker_catalog(session),
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
    policy_error = validate_worker_assignment(session.notebook_state, req.worker)
    if policy_error is not None:
        raise HTTPException(status_code=403, detail=policy_error)

    try:
        update_notebook_worker(session.path, req.worker)
        session.reload()
        return {
            "worker": session.notebook_state.worker,
            **await _serialize_worker_catalog(session),
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
    policy_error = validate_worker_assignment(session.notebook_state, req.worker)
    if policy_error is not None:
        raise HTTPException(status_code=403, detail=policy_error)

    try:
        update_cell_worker(session.path, cell_id, req.worker)
        session.reload()
        cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
        if cell is None:
            raise HTTPException(status_code=404, detail="Cell not found")
        return {
            "cell": session.serialize_cell(cell),
            "worker": cell.worker_override,
            **await _serialize_worker_catalog(session),
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

    return _serialize_environment_payload(session)


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

    try:
        session._begin_synchronous_environment_mutation(f"add {req.package}")
    except RuntimeError as exc:
        _raise_environment_busy(session, str(exc))

    try:
        outcome = await session.mutate_dependency(req.package, action="add")
    finally:
        session._end_synchronous_environment_mutation()
    result = outcome.result

    if not result.success:
        raise HTTPException(
            status_code=400,
            detail=_serialize_operation_error_detail(
                result.error or "Failed to add dependency",
                result,
            ),
        )

    return {
        "success": True,
        "package": result.package,
        "lockfile_changed": result.lockfile_changed,
        **_serialize_result_operation_log(result),
        **_serialize_environment_payload(session),
        **_serialize_environment_change(session, outcome.staleness_map),
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

    try:
        session._begin_synchronous_environment_mutation(f"remove {package_name}")
    except RuntimeError as exc:
        _raise_environment_busy(session, str(exc))

    try:
        outcome = await session.mutate_dependency(package_name, action="remove")
    finally:
        session._end_synchronous_environment_mutation()
    result = outcome.result

    if not result.success:
        raise HTTPException(
            status_code=400,
            detail=_serialize_operation_error_detail(
                result.error or "Failed to remove dependency",
                result,
            ),
        )

    return {
        "success": True,
        "package": result.package,
        "lockfile_changed": result.lockfile_changed,
        **_serialize_result_operation_log(result),
        **_serialize_environment_payload(session),
        **_serialize_environment_change(session, outcome.staleness_map),
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

    environment_block_reason = session.environment_execution_block_message()
    if environment_block_reason:
        raise HTTPException(
            status_code=409,
            detail={
                "message": environment_block_reason,
                "code": "ENVIRONMENT_BUSY",
                "environment_job": session.serialize_environment_job_state(),
            },
        )

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
        session.apply_execution_result_metadata(cell_id, result)
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
