"""FastAPI router for notebook endpoints."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import re
import time
import tomllib
import uuid
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
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
from strata.notebook.timing import NotebookTimingRecorder
from strata.notebook.workers import (
    build_worker_catalog_with_health,
    notebook_worker_definitions_editable,
    validate_worker_assignment,
)
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    delete_notebook_directory,
    remove_cell_from_notebook,
    rename_notebook,
    reorder_cells,
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


def _reuse_open_session_by_path() -> bool:
    """Enable path-based session reuse only in personal mode."""
    try:
        from strata.server import get_state

        state = get_state()
    except RuntimeError:
        return True

    return state.config.deployment_mode == "personal"


def _get_notebook_storage_root() -> Path | None:
    """Return the configured notebook storage root when server state is available."""
    try:
        from strata.server import get_state

        state = get_state()
    except RuntimeError:
        return None

    configured_path = getattr(state.config, "notebook_storage_dir", None)
    if configured_path is None:
        return None
    return Path(configured_path).resolve()


def _require_personal_mode_notebook_delete() -> None:
    """Restrict destructive notebook deletion to personal mode for now."""
    try:
        from strata.server import get_state

        state = get_state()
    except RuntimeError:
        return

    if state.config.deployment_mode != "personal":
        raise HTTPException(
            status_code=403,
            detail="Notebook deletion is only available in personal mode",
        )


def _timed_json_response(
    data: dict,
    *,
    timing: NotebookTimingRecorder,
    route_name: str,
    log_context: str,
) -> JSONResponse:
    timings_ms = timing.as_dict()
    logger.info(
        "%s timing %s",
        route_name,
        {
            "context": log_context,
            "timings_ms": {name: round(duration, 1) for name, duration in timings_ms.items()},
        },
    )
    return JSONResponse(
        content=jsonable_encoder(data),
        headers={"Server-Timing": timing.server_timing_header()},
    )


def validate_package_name(package: str) -> str:
    """Validate and sanitize a package specifier.

    Rejects shell metacharacters. Used by both REST and WS handlers.
    """
    if len(package) > 200:
        raise ValueError("Package specifier too long")
    if any(c in package for c in ";&|`$(){}!<>\"'\n\r\t"):
        raise ValueError("Package specifier contains invalid characters")
    return package.strip()


def _validate_notebook_path(user_path: str, label: str = "path") -> Path:
    """Validate that a notebook path is safe and confined to the storage root."""
    path = Path(user_path)
    if ".." in path.parts:
        raise HTTPException(status_code=400, detail=f"Invalid {label}: path traversal not allowed")

    root = _get_notebook_storage_root()
    resolved = (
        (root / path).resolve() if root is not None and not path.is_absolute() else path.resolve()
    )

    if root is not None and resolved != root and root not in resolved.parents:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid {label}: must be inside configured notebook storage",
        )

    return resolved


def _safe_filename(name: str) -> str:
    """Sanitize a string for use in Content-Disposition."""
    safe = re.sub(r"[^\w\s.-]", "", name)
    safe = re.sub(r"\s+", "_", safe).strip("_") or "notebook"
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
        "definitions_editable": notebook_worker_definitions_editable(session.notebook_state),
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
    operation_log = _serialize_environment_operation_log(getattr(result, "operation_log", None))
    if operation_log is None:
        return {}
    return {"operation_log": operation_log}


def _serialize_operation_error_detail(message: str, result: object) -> dict:
    """Build a structured HTTP error detail payload with optional command logs."""
    detail: dict[str, object] = {"message": message}
    operation_log = _serialize_environment_operation_log(getattr(result, "operation_log", None))
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
    starter_cell: bool = False

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
    language: str = "python"


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

    name: str = Field(..., min_length=1, max_length=255)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Notebook name cannot be empty")
        return normalized


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
async def open_notebook(req: OpenNotebookRequest) -> JSONResponse:
    """Open a notebook directory.

    Args:
        req: OpenNotebookRequest with path

    Returns:
        Notebook state, session ID, and DAG as JSON
    """
    timing = NotebookTimingRecorder()

    try:
        with timing.phase("validate"):
            notebook_path = _validate_notebook_path(req.path, "notebook path")
            if not notebook_path.exists():
                raise HTTPException(status_code=404, detail="Notebook directory not found")

        # Create session (parses notebook and triggers DAG analysis)
        with timing.phase("session_open"):
            session = _session_manager.open_notebook(
                notebook_path,
                reuse_existing=_reuse_open_session_by_path(),
                timing=timing,
            )

        # Return notebook state with session ID and DAG
        with timing.phase("serialize"):
            data = session.serialize_notebook_state()
            data["session_id"] = session.id
            data["path"] = str(session.path)
            data["dag"] = _format_dag(session)
            data.update(_serialize_notebook_runtime_config())
        return _timed_json_response(
            data,
            timing=timing,
            route_name="notebook_open",
            log_context=str(notebook_path),
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/create")
async def create_new_notebook(req: CreateNotebookRequest) -> JSONResponse:
    """Create a new notebook.

    Args:
        req: CreateNotebookRequest with parent_path and name

    Returns:
        Notebook state as JSON
    """
    timing = NotebookTimingRecorder()
    try:
        with timing.phase("validate"):
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

        # Check if a notebook already exists at this path
        expected_dir = parent_path / req.name.lower().replace(" ", "_")
        if (expected_dir / "notebook.toml").exists():
            raise HTTPException(
                status_code=409,
                detail=f"A notebook already exists at {expected_dir}. Use Open to open it.",
            )

        with timing.phase("create_notebook"):
            notebook_dir = create_notebook(
                parent_path,
                req.name,
                python_version=selected_python_version,
                initialize_environment=False,
            )
        if req.starter_cell:
            with timing.phase("create_starter_cell"):
                add_cell_to_notebook(notebook_dir, str(uuid.uuid4()))
        with timing.phase("session_open"):
            session = _session_manager.open_notebook(
                notebook_dir,
                defer_initial_venv_sync=True,
                timing=timing,
            )
        with timing.phase("environment_job_submit"):
            try:
                await session.submit_environment_job(action="sync")
            except Exception as exc:
                logger.exception(
                    "Failed to start initial environment bootstrap for %s",
                    notebook_dir,
                )
                session.environment_sync_state = "failed"
                session.environment_sync_error = (
                    f"Failed to start notebook environment initialization: {exc}"
                )
                session.environment_sync_notice = None

        with timing.phase("serialize"):
            data = session.serialize_notebook_state()
            data["session_id"] = session.id
            data["path"] = str(session.path)
            data.update(runtime_config)
        return _timed_json_response(
            data,
            timing=timing,
            route_name="notebook_create",
            log_context=str(notebook_dir),
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.exception("Internal server error")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/{notebook_id}")
async def delete_notebook(notebook_id: str) -> dict:
    """Delete a notebook directory and all notebook-owned runtime state."""
    _require_personal_mode_notebook_delete()

    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    if session.has_active_environment_mutation():
        _raise_environment_busy(
            session,
            "Notebook deletion is blocked while an environment update is in progress.",
        )

    if session._has_active_execution():
        raise HTTPException(
            status_code=409,
            detail="Notebook deletion is blocked while notebook execution is running.",
        )

    notebook_path = session.path.resolve()
    notebook_name = session.notebook_state.name

    _session_manager.close_session(session.id)

    try:
        delete_notebook_directory(notebook_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.exception("Failed to delete notebook %s", notebook_path)
        raise HTTPException(status_code=500, detail="Failed to delete notebook")

    return {
        "deleted": True,
        "session_id": notebook_id,
        "name": notebook_name,
        "path": str(notebook_path),
    }


# Names to skip while recursing into notebook_storage_dir. Large dirs
# (node_modules, .venv) are noise; hidden dirs (starting with .) are
# skipped wholesale except for the notebook's own .strata directory
# which is handled by the ignore below rather than a name match.
_DISCOVER_SKIP_DIRS = frozenset(
    {
        "node_modules",
        "__pycache__",
        ".git",
        ".venv",
        "venv",
        "dist",
        "build",
        "target",
        ".strata",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".ipynb_checkpoints",
    }
)


def _read_notebook_metadata(notebook_toml_path: Path) -> dict[str, Any] | None:
    """Cheaply read a notebook.toml's summary fields (name, id, updated_at).

    Returns None if the file is unreadable; intentionally does not parse
    cells — discovery should stay fast even on a directory with hundreds
    of notebooks.
    """
    try:
        raw = tomllib.loads(notebook_toml_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    name = raw.get("name")
    notebook_id = raw.get("notebook_id")
    updated_at = raw.get("updated_at")
    return {
        "name": str(name) if isinstance(name, str) and name.strip() else None,
        "notebook_id": str(notebook_id) if isinstance(notebook_id, str) else None,
        "updated_at": str(updated_at) if updated_at is not None else None,
    }


def _discover_notebooks(
    root: Path, *, max_depth: int = 4, max_results: int = 500
) -> list[dict[str, Any]]:
    """Walk ``root`` looking for directories containing ``notebook.toml``.

    Stops descending into any matched directory (notebooks don't nest) or
    any name in ``_DISCOVER_SKIP_DIRS``. Bounded by ``max_depth`` and
    ``max_results`` so a misconfigured storage root can't stall the
    server scanning a huge tree.
    """
    results: list[dict[str, Any]] = []
    if not root.exists() or not root.is_dir():
        return results

    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack and len(results) < max_results:
        current, depth = stack.pop()
        try:
            entries = list(current.iterdir())
        except (PermissionError, OSError):
            continue

        notebook_toml = current / "notebook.toml"
        if notebook_toml.is_file():
            metadata = _read_notebook_metadata(notebook_toml)
            if metadata is not None:
                results.append({"path": str(current.resolve()), **metadata})
            # Don't descend into a notebook directory — nested notebooks
            # aren't a supported layout and would create duplicate hits.
            continue

        if depth >= max_depth:
            continue
        for entry in entries:
            if not entry.is_dir():
                continue
            name = entry.name
            if name.startswith(".") or name in _DISCOVER_SKIP_DIRS:
                continue
            stack.append((entry, depth + 1))

    # Newest first when updated_at is present; fall back to path sort so
    # ordering is stable when timestamps are missing or equal.
    def sort_key(entry: dict[str, Any]) -> tuple[int, str]:
        ts = entry.get("updated_at") or ""
        return (0 if ts else 1, ts or entry["path"])

    results.sort(key=sort_key, reverse=True)
    return results


@router.get("/discover")
async def discover_notebooks() -> dict:
    """List notebook directories found under the configured storage root.

    Used by the "Open existing" UI so users pick from a list instead of
    typing a filesystem path. Returns ``{"root", "notebooks"}`` where
    ``root`` is the scan root (for display) and ``notebooks`` is a
    ``[{path, name, notebook_id, updated_at}]`` list sorted newest first.
    """
    root = _get_notebook_storage_root()
    if root is None:
        return {"root": None, "notebooks": []}
    return {"root": str(root), "notebooks": _discover_notebooks(root)}


class DeleteNotebookByPathRequest(BaseModel):
    """Request for path-based notebook deletion (no session required)."""

    path: str = Field(..., description="Filesystem path of the notebook directory to delete")


@router.post("/delete-by-path")
async def delete_notebook_by_path(req: DeleteNotebookByPathRequest) -> dict:
    """Delete a notebook directory identified by path.

    Unlike ``DELETE /{notebook_id}`` this does not require the notebook to
    be open in a session — it's for deleting notebooks from the home page
    list. If a session happens to be open against the same directory it
    is closed first so the subsequent ``rmtree`` is safe.
    """
    _require_personal_mode_notebook_delete()

    notebook_path = _validate_notebook_path(req.path, "notebook path")
    if not (notebook_path / "notebook.toml").is_file():
        raise HTTPException(
            status_code=404,
            detail=f"No notebook found at {notebook_path}",
        )

    # If a session happens to be open for this path, close it first.
    # _find_session_by_path takes care of path canonicalization.
    existing = _session_manager._find_session_by_path(notebook_path)
    if existing is not None:
        if existing.has_active_environment_mutation():
            _raise_environment_busy(
                existing,
                "Notebook deletion is blocked while an environment update is in progress.",
            )
        if existing._has_active_execution():
            raise HTTPException(
                status_code=409,
                detail="Notebook deletion is blocked while notebook execution is running.",
            )
        _session_manager.close_session(existing.id)

    try:
        delete_notebook_directory(notebook_path.resolve())
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.exception("Failed to delete notebook by path %s", notebook_path)
        raise HTTPException(status_code=500, detail="Failed to delete notebook")

    return {"deleted": True, "path": str(notebook_path)}


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
        req.package is not None or req.requirements is not None or req.environment_yaml is not None
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
async def import_environment_requirements(notebook_id: str, req: ImportRequirementsRequest) -> dict:
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
async def import_environment_yaml(notebook_id: str, req: ImportEnvironmentYamlRequest) -> dict:
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
async def preview_environment_yaml(notebook_id: str, req: PreviewEnvironmentYamlRequest) -> dict:
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
async def get_session(session_id: str) -> JSONResponse:
    """Get full state for an existing session.

    This allows the frontend to reconnect to a session after a page
    refresh without re-opening the notebook from disk.

    Args:
        session_id: UUID session identifier

    Returns:
        Notebook state, session ID, and DAG as JSON (same shape as
        the ``open`` endpoint response).
    """
    timing = NotebookTimingRecorder()
    _require_personal_mode_session_api()
    with timing.phase("lookup"):
        session = _session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")

    with timing.phase("serialize"):
        data = session.serialize_notebook_state()
        data["session_id"] = session.id
        data["path"] = str(session.path)
        data["dag"] = _format_dag(session)
        data.update(_serialize_notebook_runtime_config())
    return _timed_json_response(
        data,
        timing=timing,
        route_name="notebook_get_session",
        log_context=session_id,
    )


@router.put("/{notebook_id}/cells/reorder")
async def reorder_notebook_cells(notebook_id: str, req: ReorderCellsRequest) -> dict:
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
async def update_cell_source(notebook_id: str, cell_id: str, req: UpdateCellSourceRequest) -> dict:
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
        cell_in_session = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
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
        # The disk writer strips sensitive values (API keys, tokens) so
        # they don't leak into git. Restore the full values in the
        # in-memory session so the LLM config and Runtime panel work
        # for the duration of this session.
        session.notebook_state.env.update(req.env)
        return {
            "env": session.notebook_state.env,
            "cells": session.serialize_cells(),
        }
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
        add_cell_to_notebook(
            session.path,
            cell_id,
            req.after_cell_id,
            language=req.language,
        )

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
async def rename_notebook_endpoint(notebook_id: str, req: RenameNotebookRequest) -> dict:
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
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
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
async def add_notebook_dependency(notebook_id: str, req: AddDependencyRequest) -> dict:
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
async def remove_notebook_dependency(notebook_id: str, package_name: str) -> dict:
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
    cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
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
        raise HTTPException(status_code=500, detail="Execution failed")


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


# ============================================================================
# LLM Assistant
# ============================================================================


class LlmChatTurn(BaseModel):
    """One prior chat turn passed back to the LLM for context."""

    role: Literal["user", "assistant"]
    content: str = Field(..., max_length=20_000)


class LlmCompleteRequest(BaseModel):
    """Request for LLM assistant completion."""

    message: str = Field(..., max_length=10_000, description="User message")
    cell_id: str | None = Field(default=None, description="Target cell ID for context")
    history: list[LlmChatTurn] = Field(
        default_factory=list,
        description="Prior chat turns in this session (capped to most recent)",
        max_length=20,
    )


def _read_notebook_ai_config(session) -> dict | None:
    """Read [ai] section from notebook.toml if present."""
    notebook_toml = session.path / "notebook.toml"
    if not notebook_toml.exists():
        return None
    try:
        import tomllib

        with open(notebook_toml, "rb") as f:
            data = tomllib.load(f)
        ai_section = data.get("ai")
        return ai_section if isinstance(ai_section, dict) else None
    except Exception:
        return None


def _get_llm_config(session):
    """Resolve LLM config for a notebook session."""
    from strata.notebook.llm import resolve_llm_config

    notebook_ai = _read_notebook_ai_config(session)

    server_config = None
    try:
        from strata.server import get_state

        server_config = get_state().config
    except RuntimeError:
        pass

    # Notebook-level env vars (set via Runtime panel)
    notebook_env = getattr(session.notebook_state, "env", None) or {}

    return resolve_llm_config(notebook_ai, server_config, notebook_env)


@router.get("/{notebook_id}/ai/status")
async def llm_status(notebook_id: str) -> dict:
    """Check if the LLM assistant is configured and available."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    from strata.notebook.llm import infer_provider_name

    config = _get_llm_config(session)
    if config is None:
        return {"available": False, "model": None, "provider": None}

    return {
        "available": True,
        "model": config.model,
        "provider": infer_provider_name(config.base_url),
    }


@router.get("/{notebook_id}/ai/models")
async def llm_models(notebook_id: str) -> dict:
    """Fetch available models from the configured LLM provider.

    Queries the provider's ``/models`` endpoint (OpenAI-compatible) and
    returns a list of model IDs sorted alphabetically.
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    config = _get_llm_config(session)
    if config is None:
        return {"models": [], "current": None}

    from strata.notebook.llm import infer_provider_name

    models: list[str] = []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{config.base_url.rstrip('/')}/models",
                headers={"Authorization": f"Bearer {config.api_key}"},
            )
            if resp.status_code == 200:
                data = resp.json()
                raw = data.get("data") or data.get("models") or []
                for item in raw:
                    model_id = item.get("id") if isinstance(item, dict) else str(item)
                    if model_id:
                        models.append(model_id)
                models.sort()
    except Exception:
        logger.debug("Failed to fetch models from %s", config.base_url, exc_info=True)

    return {
        "models": models,
        "current": config.model,
        "provider": infer_provider_name(config.base_url),
    }


class UpdateAiModelRequest(BaseModel):
    """Request to update the notebook's default LLM model."""

    model: str = Field(..., max_length=200, description="Model ID")


@router.put("/{notebook_id}/ai/model")
async def update_ai_model(notebook_id: str, req: UpdateAiModelRequest) -> dict:
    """Set the notebook's default LLM model.

    Writes to the ``[ai]`` section of notebook.toml.
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    from strata.notebook.writer import update_notebook_ai_model

    update_notebook_ai_model(session.path, req.model)
    session.reload()

    config = _get_llm_config(session)
    return {
        "model": config.model if config else req.model,
    }


def _prepare_chat_request(session, req: LlmCompleteRequest):
    """Resolve config + build messages for a chat request.

    Returns ``(config, messages)``. Raises ``HTTPException`` if LLM is not
    configured.
    """
    from strata.notebook.llm import build_messages, build_notebook_context

    config = _get_llm_config(session)
    if config is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "LLM assistant not configured for this notebook. "
                "Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or STRATA_AI_API_KEY "
                "in the Runtime panel, or add an [ai] section to notebook.toml."
            ),
        )

    notebook_context = build_notebook_context(session, max_tokens=config.max_context_tokens // 4)

    cell_source = None
    if req.cell_id:
        cell = next(
            (c for c in session.notebook_state.cells if c.id == req.cell_id),
            None,
        )
        if cell:
            cell_source = cell.source

    history = [{"role": t.role, "content": t.content} for t in req.history]
    messages = build_messages(
        req.message,
        notebook_context,
        history=history,
        cell_source=cell_source,
    )
    return config, messages


@router.post("/{notebook_id}/ai/complete")
async def llm_complete(notebook_id: str, req: LlmCompleteRequest) -> dict:
    """Run a (blocking) LLM chat completion. Prefer ``/ai/stream`` for UI."""
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    from strata.notebook.llm import chat_completion

    config, messages = _prepare_chat_request(session, req)

    try:
        result = await chat_completion(config, messages)
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if status == 429:
            raise HTTPException(status_code=429, detail="LLM provider rate limited")
        raise HTTPException(status_code=502, detail=f"LLM provider error: {status}")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="LLM provider timed out")
    except Exception:
        logger.exception("LLM completion failed")
        raise HTTPException(status_code=502, detail="LLM completion failed")

    return {
        "content": result.content,
        "model": result.model,
        "tokens": {
            "input": result.input_tokens,
            "output": result.output_tokens,
        },
    }


@router.post("/{notebook_id}/ai/stream")
async def llm_stream(notebook_id: str, req: LlmCompleteRequest):
    """Stream an LLM chat completion as Server-Sent Events.

    Event types:
    - ``delta``: ``{"text": "..."}`` — incremental content chunk
    - ``done``: ``{"model": "...", "tokens": {"input": N, "output": N}}``
    - ``error``: ``{"message": "..."}``
    """
    from fastapi.responses import StreamingResponse

    from strata.notebook.llm import chat_completion_stream

    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    config, messages = _prepare_chat_request(session, req)

    async def event_stream():
        def _sse(event: str, payload: dict) -> str:
            return f"event: {event}\ndata: {json.dumps(payload)}\n\n"

        try:
            async for chunk in chat_completion_stream(config, messages):
                if chunk["type"] == "delta":
                    yield _sse("delta", {"text": chunk["text"]})
                elif chunk["type"] == "done":
                    yield _sse(
                        "done",
                        {
                            "model": chunk.get("model"),
                            "tokens": {
                                "input": chunk.get("input_tokens", 0),
                                "output": chunk.get("output_tokens", 0),
                            },
                        },
                    )
        except httpx.HTTPStatusError as e:
            logger.warning("LLM stream HTTP error: %s", e.response.status_code)
            yield _sse("error", {"message": f"LLM provider error: {e.response.status_code}"})
        except httpx.TimeoutException:
            yield _sse("error", {"message": "LLM provider timed out"})
        except Exception as e:
            logger.exception("LLM stream failed")
            yield _sse("error", {"message": f"LLM stream failed: {e}"})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ============================================================================
# LLM Agent
# ============================================================================


class AgentRequest(BaseModel):
    """Request to start an agent loop."""

    message: str = Field(..., max_length=10_000, description="User instruction")


# Per-notebook agent cancellation events
_agent_cancel_events: dict[str, asyncio.Event] = {}


@router.post("/{notebook_id}/ai/agent")
async def run_agent(notebook_id: str, req: AgentRequest) -> dict:
    """Run an LLM agent loop with tool use and observe-retry.

    Progress is streamed via WebSocket ``agent_progress`` messages.
    Only one agent can run per notebook at a time.
    """
    session = _session_manager.get_session(notebook_id)
    if not session:
        raise HTTPException(status_code=404, detail="Notebook not found")

    from strata.notebook.llm import run_agent_loop

    config = _get_llm_config(session)
    if config is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "LLM not configured for this notebook. "
                "Set ANTHROPIC_API_KEY, OPENAI_API_KEY, or STRATA_AI_API_KEY "
                "in the Runtime panel, or add an [ai] section to notebook.toml."
            ),
        )

    # Only one agent per notebook
    if notebook_id in _agent_cancel_events:
        raise HTTPException(
            status_code=409,
            detail="An agent is already running for this notebook.",
        )

    cancel_event = asyncio.Event()
    _agent_cancel_events[notebook_id] = cancel_event

    job_id = str(uuid.uuid4())[:8]

    async def _progress(event_type: str, detail: str) -> None:
        from strata.notebook.ws import _broadcast_message

        try:
            await _broadcast_message(
                notebook_id,
                {
                    "type": "agent_progress",
                    "seq": 0,
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "payload": {"event": event_type, "detail": detail, "job_id": job_id},
                },
            )
        except Exception:
            pass

    async def _run_agent_task() -> None:
        from strata.notebook.ws import _broadcast_message, broadcast_notebook_sync

        try:
            result = await run_agent_loop(
                config,
                session,
                req.message,
                notebook_id=notebook_id,
                max_iterations=10,
                cancel_event=cancel_event,
                progress_callback=_progress,
            )

            # Broadcast completion via WS
            await _broadcast_message(
                notebook_id,
                {
                    "type": "agent_done",
                    "seq": 0,
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "payload": {
                        "job_id": job_id,
                        "content": result.content,
                        "model": result.model,
                        "tokens": {
                            "input": result.total_input_tokens,
                            "output": result.total_output_tokens,
                        },
                        "iterations": result.iterations,
                        "tool_calls": [
                            {
                                "tool": tc.tool_name,
                                "args": tc.arguments,
                                "result": tc.result,
                                "duration_ms": int(tc.duration_ms),
                            }
                            for tc in result.tool_calls
                        ],
                        "cancelled": result.cancelled,
                        "error": result.error,
                    },
                },
            )
        except Exception as e:
            logger.exception("Agent loop failed")
            try:
                await _broadcast_message(
                    notebook_id,
                    {
                        "type": "agent_done",
                        "seq": 0,
                        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "payload": {
                            "job_id": job_id,
                            "content": "",
                            "error": str(e),
                            "cancelled": False,
                        },
                    },
                )
            except Exception:
                pass
        finally:
            _agent_cancel_events.pop(notebook_id, None)
            try:
                await broadcast_notebook_sync(notebook_id, session)
            except Exception:
                pass

    asyncio.create_task(_run_agent_task())

    return {"job_id": job_id, "status": "started"}


def cancel_agent(notebook_id: str) -> bool:
    """Cancel a running agent loop. Called from WebSocket handler."""
    event = _agent_cancel_events.get(notebook_id)
    if event is not None:
        event.set()
        return True
    return False
