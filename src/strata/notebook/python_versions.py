"""Helpers for notebook Python-version selection and persistence."""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Python 3.10 compatibility
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore


_MINOR_VERSION_RE = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)$")
_PATCH_VERSION_RE = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")


def current_python_minor() -> str:
    """Return the current interpreter's major.minor version."""
    return f"{sys.version_info.major}.{sys.version_info.minor}"


def normalize_python_minor(version: str) -> str:
    """Normalize and validate a Python major.minor string."""
    normalized = version.strip()
    match = _MINOR_VERSION_RE.fullmatch(normalized)
    if match is None:
        raise ValueError("Python version must use major.minor format like '3.12' or '3.13'")
    return f"{int(match.group('major'))}.{int(match.group('minor'))}"


def format_requires_python(version: str) -> str:
    """Return a project-level requires-python spec for one Python minor line."""
    normalized = normalize_python_minor(version)
    major_str, minor_str = normalized.split(".", 1)
    major = int(major_str)
    minor = int(minor_str)
    return f">={major}.{minor},<{major}.{minor + 1}"


def infer_requested_python_minor(requires_python: str | None) -> str | None:
    """Best-effort extract of a requested Python major.minor from requires-python."""
    if not requires_python:
        return None

    normalized = requires_python.strip()

    for pattern in (
        r"^==\s*(\d+\.\d+)\.\*$",
        r"^>=\s*(\d+\.\d+)\s*,\s*<\s*\d+\.\d+$",
        r"^>=\s*(\d+\.\d+)$",
        r"^(\d+\.\d+)$",
    ):
        match = re.match(pattern, normalized)
        if match is not None:
            return normalize_python_minor(match.group(1))

    return None


def read_requested_python_minor(notebook_dir: Path) -> str | None:
    """Read the requested Python minor version from a notebook pyproject."""
    pyproject_path = Path(notebook_dir) / "pyproject.toml"
    if not pyproject_path.exists():
        return None

    try:
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)
    except Exception:
        return None

    project = data.get("project")
    if not isinstance(project, dict):
        return None

    requires_python = project.get("requires-python")
    if not isinstance(requires_python, str):
        return None

    return infer_requested_python_minor(requires_python)


def read_venv_runtime_python_version(python_executable: Path) -> str | None:
    """Read ``major.minor.micro`` from a venv ``pyvenv.cfg`` when available."""
    python_path = Path(python_executable)
    config_path = python_path.parent.parent / "pyvenv.cfg"
    if not config_path.exists():
        return None

    try:
        for raw_line in config_path.read_text(encoding="utf-8").splitlines():
            key, separator, value = raw_line.partition("=")
            if separator != "=" or key.strip() != "version_info":
                continue
            version = value.strip()
            if _PATCH_VERSION_RE.fullmatch(version):
                return version
            return None
    except Exception:
        return None

    return None
