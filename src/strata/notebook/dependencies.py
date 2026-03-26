"""Dependency management for notebooks.

Wraps ``uv add`` / ``uv remove`` to manage Python packages in a
notebook's virtual environment.  After every mutation the lockfile
is re-synced so that ``uv.lock`` and ``.venv/`` stay consistent.
"""

from __future__ import annotations

import logging
import subprocess
import threading
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Serialize concurrent uv add/remove per notebook directory.
# Without this, two concurrent ``uv add`` calls for the same notebook
# can corrupt pyproject.toml / uv.lock.
_locks: dict[str, threading.Lock] = {}
_locks_lock = threading.Lock()


def _get_notebook_lock(notebook_dir: Path) -> threading.Lock:
    """Get or create a per-notebook lock for uv operations."""
    key = str(notebook_dir.resolve())
    with _locks_lock:
        if key not in _locks:
            _locks[key] = threading.Lock()
        return _locks[key]


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass
class DependencyInfo:
    """One installed dependency."""

    name: str
    version: str | None = None
    specifier: str | None = None  # e.g. ">=1.0,<2"


@dataclass
class DependencyChangeResult:
    """Result of adding or removing a dependency."""

    success: bool
    package: str
    action: str  # "add" | "remove"
    error: str | None = None
    lockfile_changed: bool = False
    dependencies: list[DependencyInfo] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------


def list_dependencies(notebook_dir: Path) -> list[DependencyInfo]:
    """List current project dependencies from pyproject.toml.

    Parses the ``[project] dependencies`` array.  Does **not** shell out.
    """
    # Python 3.10 compat
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    pyproject_path = notebook_dir / "pyproject.toml"
    if not pyproject_path.exists():
        return []

    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)

    deps_list: list[str] = data.get("project", {}).get("dependencies", [])
    results: list[DependencyInfo] = []
    for dep_str in deps_list:
        # Simple parse: "requests>=2.0" → name="requests", specifier=">=2.0"
        # PEP 508 is complex; we handle the common subset.
        name = dep_str
        specifier = None
        for op in (">=", "<=", "!=", "==", "~=", ">", "<"):
            if op in dep_str:
                idx = dep_str.index(op)
                name = dep_str[:idx].strip()
                specifier = dep_str[idx:].strip()
                break
        # Handle extras: "pkg[extra]"
        if "[" in name:
            name = name[: name.index("[")]
        results.append(DependencyInfo(name=name.strip(), specifier=specifier))

    return results


def add_dependency(
    notebook_dir: Path,
    package: str,
    *,
    timeout: int = 120,
) -> DependencyChangeResult:
    """Add a Python package to the notebook.

    Runs ``uv add <package>`` which updates pyproject.toml, resolves
    dependencies, writes uv.lock, and syncs .venv.

    Args:
        notebook_dir: Path to notebook directory
        package: Package specifier (e.g. ``"requests"`` or ``"pandas>=2.0"``)
        timeout: Subprocess timeout in seconds

    Returns:
        DependencyChangeResult with success status
    """
    lock = _get_notebook_lock(notebook_dir)
    with lock:
        return _add_dependency_locked(notebook_dir, package, timeout=timeout)


def _add_dependency_locked(
    notebook_dir: Path, package: str, *, timeout: int = 120
) -> DependencyChangeResult:
    old_lockfile_hash = _lockfile_hash(notebook_dir)

    try:
        proc = subprocess.run(
            ["uv", "add", package],
            cwd=str(notebook_dir),
            timeout=timeout,
            capture_output=True,
            check=True,
        )
        logger.info("uv add %s succeeded in %s", package, notebook_dir)
    except FileNotFoundError:
        return DependencyChangeResult(
            success=False,
            package=package,
            action="add",
            error="uv not found on PATH",
        )
    except subprocess.TimeoutExpired:
        return DependencyChangeResult(
            success=False,
            package=package,
            action="add",
            error=f"uv add timed out after {timeout}s",
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode(errors="replace") if exc.stderr else ""
        return DependencyChangeResult(
            success=False,
            package=package,
            action="add",
            error=f"uv add failed: {stderr}",
        )

    new_lockfile_hash = _lockfile_hash(notebook_dir)
    return DependencyChangeResult(
        success=True,
        package=package,
        action="add",
        lockfile_changed=old_lockfile_hash != new_lockfile_hash,
        dependencies=list_dependencies(notebook_dir),
    )


def remove_dependency(
    notebook_dir: Path,
    package: str,
    *,
    timeout: int = 120,
) -> DependencyChangeResult:
    """Remove a Python package from the notebook.

    Runs ``uv remove <package>`` which updates pyproject.toml,
    re-resolves, writes uv.lock, and syncs .venv.

    Args:
        notebook_dir: Path to notebook directory
        package: Package name to remove
        timeout: Subprocess timeout in seconds

    Returns:
        DependencyChangeResult with success status
    """
    lock = _get_notebook_lock(notebook_dir)
    with lock:
        return _remove_dependency_locked(notebook_dir, package, timeout=timeout)


def _remove_dependency_locked(
    notebook_dir: Path, package: str, *, timeout: int = 120
) -> DependencyChangeResult:
    old_lockfile_hash = _lockfile_hash(notebook_dir)

    try:
        proc = subprocess.run(
            ["uv", "remove", package],
            cwd=str(notebook_dir),
            timeout=timeout,
            capture_output=True,
            check=True,
        )
        logger.info("uv remove %s succeeded in %s", package, notebook_dir)
    except FileNotFoundError:
        return DependencyChangeResult(
            success=False,
            package=package,
            action="remove",
            error="uv not found on PATH",
        )
    except subprocess.TimeoutExpired:
        return DependencyChangeResult(
            success=False,
            package=package,
            action="remove",
            error=f"uv remove timed out after {timeout}s",
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode(errors="replace") if exc.stderr else ""
        return DependencyChangeResult(
            success=False,
            package=package,
            action="remove",
            error=f"uv remove failed: {stderr}",
        )

    new_lockfile_hash = _lockfile_hash(notebook_dir)
    return DependencyChangeResult(
        success=True,
        package=package,
        action="remove",
        lockfile_changed=old_lockfile_hash != new_lockfile_hash,
        dependencies=list_dependencies(notebook_dir),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _lockfile_hash(notebook_dir: Path) -> str:
    """Compute hash of uv.lock for change detection."""
    from strata.notebook.env import compute_lockfile_hash

    return compute_lockfile_hash(notebook_dir)
