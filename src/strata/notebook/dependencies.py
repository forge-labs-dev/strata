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

import tomli_w

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


@dataclass
class RequirementsImportResult:
    """Result of importing notebook dependencies from requirements text."""

    success: bool
    error: str | None = None
    lockfile_changed: bool = False
    dependencies: list[DependencyInfo] = field(default_factory=list)
    imported_count: int = 0
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------


def list_dependencies(notebook_dir: Path) -> list[DependencyInfo]:
    """List current project dependencies from pyproject.toml.

    Parses the ``[project] dependencies`` array.  Does **not** shell out.
    """
    deps_list = _read_project_dependency_strings(notebook_dir)
    results: list[DependencyInfo] = []
    for dep_str in deps_list:
        name, specifier = _split_requirement(dep_str)
        results.append(DependencyInfo(name=name.strip(), specifier=specifier))

    return results


def export_requirements_text(notebook_dir: Path) -> str:
    """Export direct notebook dependencies as ``requirements.txt`` text."""
    deps_list = _read_project_dependency_strings(notebook_dir)
    if not deps_list:
        return ""
    return "\n".join(deps_list) + "\n"


def import_requirements_text(
    notebook_dir: Path,
    requirements_text: str,
    *,
    timeout: int = 180,
) -> RequirementsImportResult:
    """Replace direct notebook dependencies from ``requirements.txt`` text."""
    normalized_requirements = parse_requirements_text(requirements_text)
    # Python 3.10 compat
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    pyproject_path = notebook_dir / "pyproject.toml"
    if not pyproject_path.exists():
        return RequirementsImportResult(
            success=False,
            error="pyproject.toml not found",
        )

    old_lockfile_hash = _lockfile_hash(notebook_dir)
    old_pyproject = pyproject_path.read_bytes()
    lockfile_path = notebook_dir / "uv.lock"
    old_lockfile = lockfile_path.read_bytes() if lockfile_path.exists() else None

    lock = _get_notebook_lock(notebook_dir)
    with lock:
        try:
            with open(pyproject_path, "rb") as f:
                data = tomllib.load(f)
        except Exception as exc:
            return RequirementsImportResult(
                success=False,
                error=f"Failed to parse pyproject.toml: {exc}",
            )

        project = data.setdefault("project", {})
        if not isinstance(project, dict):
            return RequirementsImportResult(
                success=False,
                error="pyproject.toml project section is invalid",
            )
        project["dependencies"] = normalized_requirements

        try:
            with open(pyproject_path, "wb") as f:
                tomli_w.dump(data, f)
        except Exception as exc:
            return RequirementsImportResult(
                success=False,
                error=f"Failed to write pyproject.toml: {exc}",
            )

        try:
            subprocess.run(
                ["uv", "sync"],
                cwd=str(notebook_dir),
                timeout=timeout,
                capture_output=True,
                check=True,
            )
            logger.info(
                "Imported %s requirements into %s",
                len(normalized_requirements),
                notebook_dir,
            )
        except FileNotFoundError:
            _restore_dependency_files(pyproject_path, old_pyproject, lockfile_path, old_lockfile)
            return RequirementsImportResult(
                success=False,
                error="uv not found on PATH",
            )
        except subprocess.TimeoutExpired:
            _restore_dependency_files(pyproject_path, old_pyproject, lockfile_path, old_lockfile)
            return RequirementsImportResult(
                success=False,
                error=f"uv sync timed out after {timeout}s",
            )
        except subprocess.CalledProcessError as exc:
            _restore_dependency_files(pyproject_path, old_pyproject, lockfile_path, old_lockfile)
            stderr = exc.stderr.decode(errors="replace") if exc.stderr else ""
            return RequirementsImportResult(
                success=False,
                error=f"uv sync failed: {stderr}",
            )

    new_lockfile_hash = _lockfile_hash(notebook_dir)
    return RequirementsImportResult(
        success=True,
        lockfile_changed=old_lockfile_hash != new_lockfile_hash,
        dependencies=list_dependencies(notebook_dir),
        imported_count=len(normalized_requirements),
    )


def import_environment_yaml_text(
    notebook_dir: Path,
    environment_yaml_text: str,
    *,
    timeout: int = 180,
) -> RequirementsImportResult:
    """Best-effort import of Conda-style ``environment.yaml`` into notebook deps."""
    requirements, warnings = parse_environment_yaml_text(environment_yaml_text)
    result = import_requirements_text(
        notebook_dir,
        "\n".join(requirements),
        timeout=timeout,
    )
    result.warnings = warnings
    return result


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
        subprocess.run(
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
        subprocess.run(
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


def parse_requirements_text(requirements_text: str) -> list[str]:
    """Parse a small supported subset of ``requirements.txt`` syntax."""
    requirements: list[str] = []
    seen_names: set[str] = set()

    for raw_line in requirements_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-"):
            raise ValueError(
                "Unsupported requirements entry. Use plain package specifiers only."
            )
        if " #" in line:
            line = line.split(" #", 1)[0].strip()

        validated = _validate_requirement_specifier(line)
        requirement_name, _ = _split_requirement(validated)
        if requirement_name in seen_names:
            raise ValueError(f"Duplicate requirement: {requirement_name}")
        seen_names.add(requirement_name)
        requirements.append(validated)

    return requirements


def parse_environment_yaml_text(environment_yaml_text: str) -> tuple[list[str], list[str]]:
    """Translate a subset of Conda ``environment.yaml`` into pip requirements."""
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise ValueError("PyYAML is required to import environment.yaml") from exc

    try:
        data = yaml.safe_load(environment_yaml_text) or {}
    except Exception as exc:
        raise ValueError(f"Failed to parse environment.yaml: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("environment.yaml must contain a mapping at the top level")

    dependencies = data.get("dependencies", [])
    if not isinstance(dependencies, list):
        raise ValueError("environment.yaml dependencies must be a list")

    warnings: list[str] = []
    requirements: list[str] = []
    seen_names: set[str] = set()

    channels = data.get("channels")
    if isinstance(channels, list) and channels:
        warnings.append(
            "Ignored conda channels from environment.yaml; notebook "
            "environments use pip/uv resolution."
        )

    def add_requirement(requirement: str) -> None:
        validated = _validate_requirement_specifier(requirement)
        requirement_name, _ = _split_requirement(validated)
        if requirement_name in seen_names:
            raise ValueError(f"Duplicate requirement: {requirement_name}")
        seen_names.add(requirement_name)
        requirements.append(validated)

    for entry in dependencies:
        if isinstance(entry, str):
            translated, entry_warning = _translate_conda_dependency(entry)
            if entry_warning:
                warnings.append(entry_warning)
            if translated:
                add_requirement(translated)
            continue

        if isinstance(entry, dict):
            pip_entries = entry.get("pip")
            if isinstance(pip_entries, list):
                for pip_entry in pip_entries:
                    if not isinstance(pip_entry, str):
                        warnings.append(
                            "Ignored non-string pip dependency entry in environment.yaml."
                        )
                        continue
                    add_requirement(pip_entry.strip())
                continue

            warnings.append(
                "Ignored unsupported mapping entry in environment.yaml dependencies."
            )
            continue

        warnings.append("Ignored unsupported dependency entry in environment.yaml.")

    return requirements, warnings


def _read_project_dependency_strings(notebook_dir: Path) -> list[str]:
    """Read raw dependency strings from ``pyproject.toml``."""
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
    return [str(dep) for dep in deps_list]


def _split_requirement(dep_str: str) -> tuple[str, str | None]:
    """Split a requirement into package name and version specifier."""
    name = dep_str
    specifier = None
    for op in (">=", "<=", "!=", "==", "~=", ">", "<"):
        if op in dep_str:
            idx = dep_str.index(op)
            name = dep_str[:idx].strip()
            specifier = dep_str[idx:].strip()
            break
    if "[" in name:
        name = name[: name.index("[")]
    return name.strip(), specifier


def _validate_requirement_specifier(requirement: str) -> str:
    """Validate a supported requirement line."""
    normalized = requirement.strip()
    if not normalized:
        raise ValueError("Requirement cannot be empty")
    if len(normalized) > 200:
        raise ValueError("Requirement specifier too long")
    if any(c in normalized for c in ';&|`$(){}"\'\n\r\t'):
        raise ValueError("Requirement specifier contains invalid characters")
    return normalized


def _translate_conda_dependency(dependency: str) -> tuple[str | None, str | None]:
    """Best-effort conversion from a Conda dependency string to a pip requirement."""
    normalized = dependency.strip()
    if not normalized:
        return None, None

    warning: str | None = None
    if "::" in normalized:
        _, normalized = normalized.split("::", 1)
        warning = (
            "Ignored conda channel prefixes in environment.yaml; using package names only."
        )

    lowered = normalized.lower()
    if lowered == "pip":
        return None, "Ignored explicit pip bootstrap entry from environment.yaml."
    if lowered.startswith("python"):
        return (
            None,
            "Ignored python version pin from environment.yaml; notebook "
            "Python is managed separately.",
        )

    if (
        "==" not in normalized
        and "!=" not in normalized
        and ">=" not in normalized
        and "<=" not in normalized
        and "~=" not in normalized
        and "=" in normalized
    ):
        pieces = normalized.split("=")
        if len(pieces) == 2 and pieces[0] and pieces[1]:
            normalized = f"{pieces[0]}=={pieces[1]}"
        else:
            return (
                None,
                f"Ignored unsupported conda dependency entry: {dependency}",
            )

    return normalized, warning


def _restore_dependency_files(
    pyproject_path: Path,
    old_pyproject: bytes,
    lockfile_path: Path,
    old_lockfile: bytes | None,
) -> None:
    """Restore dependency files after a failed import attempt."""
    pyproject_path.write_bytes(old_pyproject)
    if old_lockfile is None:
        if lockfile_path.exists():
            lockfile_path.unlink()
    else:
        lockfile_path.write_bytes(old_lockfile)
