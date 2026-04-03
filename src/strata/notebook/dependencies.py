"""Dependency management for notebooks.

Wraps ``uv add`` / ``uv remove`` to manage Python packages in a
notebook's virtual environment.  After every mutation the lockfile
is re-synced so that ``uv.lock`` and ``.venv/`` stay consistent.
"""

from __future__ import annotations

import logging
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import tomli_w

logger = logging.getLogger(__name__)
_MAX_OPERATION_LOG_CHARS = 12_000

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
class EnvironmentOperationLog:
    """Structured command details for environment/package operations."""

    command: str
    duration_ms: int | None = None
    stdout: str = ""
    stderr: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False


@dataclass
class DependencyChangeResult:
    """Result of adding or removing a dependency."""

    success: bool
    package: str
    action: str  # "add" | "remove"
    error: str | None = None
    lockfile_changed: bool = False
    dependencies: list[DependencyInfo] = field(default_factory=list)
    operation_log: EnvironmentOperationLog | None = None


@dataclass
class RequirementsImportResult:
    """Result of importing notebook dependencies from requirements text."""

    success: bool
    error: str | None = None
    lockfile_changed: bool = False
    dependencies: list[DependencyInfo] = field(default_factory=list)
    imported_count: int = 0
    warnings: list[str] = field(default_factory=list)
    operation_log: EnvironmentOperationLog | None = None


@dataclass
class RequirementsPreviewResult:
    """Preview of importing notebook dependencies from external text."""

    dependencies: list[DependencyInfo] = field(default_factory=list)
    normalized_requirements: list[str] = field(default_factory=list)
    imported_count: int = 0
    warnings: list[str] = field(default_factory=list)
    additions: list[DependencyInfo] = field(default_factory=list)
    removals: list[DependencyInfo] = field(default_factory=list)
    unchanged: list[DependencyInfo] = field(default_factory=list)


@dataclass
class _UvCommandResult:
    """Internal subprocess result wrapper for uv commands."""

    success: bool
    error: str | None
    operation_log: EnvironmentOperationLog


def _normalize_output_text(value: str | bytes | None) -> str:
    """Normalize subprocess output into a safe UI string."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode(errors="replace")
    return value.strip()


def _trim_output_for_ui(value: str | bytes | None) -> tuple[str, bool]:
    """Trim command output so REST payloads stay bounded."""
    text = _normalize_output_text(value)
    if len(text) <= _MAX_OPERATION_LOG_CHARS:
        return text, False
    return text[:_MAX_OPERATION_LOG_CHARS], True


def _format_command_for_ui(command: list[str]) -> str:
    """Render a subprocess command for UI/debugging."""
    return " ".join(shlex.quote(part) for part in command)


def _run_uv_command(
    notebook_dir: Path,
    args: list[str],
    *,
    timeout: int,
    display_name: str,
) -> _UvCommandResult:
    """Run a uv command and capture bounded UI logs."""
    command = ["uv", *args]
    started = time.perf_counter()
    formatted_command = _format_command_for_ui(command)

    try:
        completed = subprocess.run(
            command,
            cwd=str(notebook_dir),
            timeout=timeout,
            capture_output=True,
            check=True,
            text=True,
        )
        stdout, stdout_truncated = _trim_output_for_ui(completed.stdout)
        stderr, stderr_truncated = _trim_output_for_ui(completed.stderr)
        return _UvCommandResult(
            success=True,
            error=None,
            operation_log=EnvironmentOperationLog(
                command=formatted_command,
                duration_ms=int((time.perf_counter() - started) * 1000),
                stdout=stdout,
                stderr=stderr,
                stdout_truncated=stdout_truncated,
                stderr_truncated=stderr_truncated,
            ),
        )
    except FileNotFoundError:
        return _UvCommandResult(
            success=False,
            error="uv not found on PATH",
            operation_log=EnvironmentOperationLog(
                command=formatted_command,
                duration_ms=int((time.perf_counter() - started) * 1000),
            ),
        )
    except subprocess.TimeoutExpired as exc:
        stdout, stdout_truncated = _trim_output_for_ui(exc.stdout)
        stderr, stderr_truncated = _trim_output_for_ui(exc.stderr)
        return _UvCommandResult(
            success=False,
            error=f"{display_name} timed out after {timeout}s",
            operation_log=EnvironmentOperationLog(
                command=formatted_command,
                duration_ms=int((time.perf_counter() - started) * 1000),
                stdout=stdout,
                stderr=stderr,
                stdout_truncated=stdout_truncated,
                stderr_truncated=stderr_truncated,
            ),
        )
    except subprocess.CalledProcessError as exc:
        stdout, stdout_truncated = _trim_output_for_ui(exc.stdout)
        stderr, stderr_truncated = _trim_output_for_ui(exc.stderr)
        error_detail = stderr or stdout or f"{display_name} exited with status {exc.returncode}"
        return _UvCommandResult(
            success=False,
            error=f"{display_name} failed: {error_detail}",
            operation_log=EnvironmentOperationLog(
                command=formatted_command,
                duration_ms=int((time.perf_counter() - started) * 1000),
                stdout=stdout,
                stderr=stderr,
                stdout_truncated=stdout_truncated,
                stderr_truncated=stderr_truncated,
            ),
        )


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


def list_resolved_dependencies(notebook_dir: Path) -> list[DependencyInfo]:
    """List resolved packages from ``uv.lock`` when present."""
    # Python 3.10 compat
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore

    lockfile_path = notebook_dir / "uv.lock"
    if not lockfile_path.exists():
        return []

    try:
        with open(lockfile_path, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        logger.debug("Failed to parse uv.lock in %s", notebook_dir, exc_info=True)
        return []

    packages = data.get("package", [])
    if not isinstance(packages, list):
        return []

    resolved: list[DependencyInfo] = []
    for package in packages:
        if not isinstance(package, dict):
            continue
        name = package.get("name")
        version = package.get("version")
        if not isinstance(name, str):
            continue
        resolved.append(
            DependencyInfo(
                name=name,
                version=str(version) if version is not None else None,
                specifier=None,
            )
        )

    resolved.sort(key=lambda dep: dep.name.lower())
    return resolved


def export_requirements_text(notebook_dir: Path) -> str:
    """Export direct notebook dependencies as ``requirements.txt`` text."""
    deps_list = _read_project_dependency_strings(notebook_dir)
    if not deps_list:
        return ""
    return "\n".join(deps_list) + "\n"


def preview_requirements_text(
    notebook_dir: Path,
    requirements_text: str,
) -> RequirementsPreviewResult:
    """Preview replacing direct notebook dependencies from requirements text."""
    normalized_requirements = parse_requirements_text(requirements_text)
    preview_dependencies = _dependency_info_from_requirement_strings(normalized_requirements)
    additions, removals, unchanged = _diff_dependency_sets(
        list_dependencies(notebook_dir),
        preview_dependencies,
    )
    return RequirementsPreviewResult(
        dependencies=preview_dependencies,
        normalized_requirements=normalized_requirements,
        imported_count=len(preview_dependencies),
        additions=additions,
        removals=removals,
        unchanged=unchanged,
    )


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

        command_result = _run_uv_command(
            notebook_dir,
            ["sync"],
            timeout=timeout,
            display_name="uv sync",
        )
        if command_result.success:
            logger.info(
                "Imported %s requirements into %s",
                len(normalized_requirements),
                notebook_dir,
            )
        else:
            _restore_dependency_files(pyproject_path, old_pyproject, lockfile_path, old_lockfile)
            return RequirementsImportResult(
                success=False,
                error=command_result.error,
                operation_log=command_result.operation_log,
            )

    new_lockfile_hash = _lockfile_hash(notebook_dir)
    return RequirementsImportResult(
        success=True,
        lockfile_changed=old_lockfile_hash != new_lockfile_hash,
        dependencies=list_dependencies(notebook_dir),
        imported_count=len(normalized_requirements),
        operation_log=command_result.operation_log,
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


def preview_environment_yaml_text(
    notebook_dir: Path,
    environment_yaml_text: str,
) -> RequirementsPreviewResult:
    """Preview best-effort import of Conda-style ``environment.yaml`` text."""
    normalized_requirements, warnings = parse_environment_yaml_text(environment_yaml_text)
    preview_dependencies = _dependency_info_from_requirement_strings(normalized_requirements)
    additions, removals, unchanged = _diff_dependency_sets(
        list_dependencies(notebook_dir),
        preview_dependencies,
    )
    return RequirementsPreviewResult(
        dependencies=preview_dependencies,
        normalized_requirements=normalized_requirements,
        imported_count=len(preview_dependencies),
        warnings=warnings,
        additions=additions,
        removals=removals,
        unchanged=unchanged,
    )


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

    command_result = _run_uv_command(
        notebook_dir,
        ["add", package],
        timeout=timeout,
        display_name="uv add",
    )
    if command_result.success:
        logger.info("uv add %s succeeded in %s", package, notebook_dir)
    else:
        return DependencyChangeResult(
            success=False,
            package=package,
            action="add",
            error=command_result.error,
            operation_log=command_result.operation_log,
        )

    new_lockfile_hash = _lockfile_hash(notebook_dir)
    return DependencyChangeResult(
        success=True,
        package=package,
        action="add",
        lockfile_changed=old_lockfile_hash != new_lockfile_hash,
        dependencies=list_dependencies(notebook_dir),
        operation_log=command_result.operation_log,
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

    command_result = _run_uv_command(
        notebook_dir,
        ["remove", package],
        timeout=timeout,
        display_name="uv remove",
    )
    if command_result.success:
        logger.info("uv remove %s succeeded in %s", package, notebook_dir)
    else:
        return DependencyChangeResult(
            success=False,
            package=package,
            action="remove",
            error=command_result.error,
            operation_log=command_result.operation_log,
        )

    new_lockfile_hash = _lockfile_hash(notebook_dir)
    return DependencyChangeResult(
        success=True,
        package=package,
        action="remove",
        lockfile_changed=old_lockfile_hash != new_lockfile_hash,
        dependencies=list_dependencies(notebook_dir),
        operation_log=command_result.operation_log,
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


def _dependency_info_from_requirement_strings(
    requirements: list[str],
) -> list[DependencyInfo]:
    """Convert normalized requirement strings to dependency metadata."""
    results: list[DependencyInfo] = []
    for requirement in requirements:
        name, specifier = _split_requirement(requirement)
        results.append(DependencyInfo(name=name.strip(), specifier=specifier))
    return results


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


def _diff_dependency_sets(
    current: list[DependencyInfo],
    target: list[DependencyInfo],
) -> tuple[list[DependencyInfo], list[DependencyInfo], list[DependencyInfo]]:
    """Diff dependency sets by package name and specifier."""
    current_map = {dep.name: dep for dep in current}
    target_map = {dep.name: dep for dep in target}

    additions: list[DependencyInfo] = []
    removals: list[DependencyInfo] = []
    unchanged: list[DependencyInfo] = []

    for name, target_dep in target_map.items():
        current_dep = current_map.get(name)
        if current_dep is None:
            additions.append(target_dep)
        elif current_dep.specifier == target_dep.specifier:
            unchanged.append(target_dep)
        else:
            additions.append(target_dep)
            removals.append(current_dep)

    for name, current_dep in current_map.items():
        if name not in target_map:
            removals.append(current_dep)

    additions.sort(key=lambda dep: dep.name.lower())
    removals.sort(key=lambda dep: dep.name.lower())
    unchanged.sort(key=lambda dep: dep.name.lower())
    return additions, removals, unchanged


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
