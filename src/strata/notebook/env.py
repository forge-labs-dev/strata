"""Environment hashing for notebook dependencies.

For now, we compute the hash of the entire uv.lock file.
Runtime-only filtering (excluding dev deps) is a future optimization.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping
from pathlib import Path

logger = logging.getLogger(__name__)


def compute_lockfile_hash(notebook_dir: Path) -> str:
    """Compute SHA-256 hash of uv.lock (lockfile dependencies).

    For v1 simplification: hash the entire uv.lock file.
    Filtering to runtime-only deps is a future optimization.

    If uv.lock doesn't exist (e.g., notebook has no dependencies),
    return a sentinel hash (empty string hash).

    Args:
        notebook_dir: Path to notebook directory

    Returns:
        SHA-256 hex digest of lockfile contents, or empty hash if not found
    """
    lockfile_path = notebook_dir / "uv.lock"

    if not lockfile_path.exists():
        # No lockfile — return sentinel hash (hash of empty string)
        # This allows notebooks without lockfiles to still work
        return hashlib.sha256(b"").hexdigest()

    try:
        with open(lockfile_path, "rb") as f:
            lockfile_content = f.read()
        return hashlib.sha256(lockfile_content).hexdigest()
    except Exception as e:
        # If we can't read the lockfile, log and return sentinel
        logger.warning("Could not read uv.lock: %s", e)
        return hashlib.sha256(b"").hexdigest()


def compute_execution_env_hash(
    notebook_dir: Path,
    runtime_env: Mapping[str, str] | None = None,
    runtime_identity: str | None = None,
) -> str:
    """Compute the effective execution environment hash for a cell.

    This combines the notebook lockfile hash with any persisted or annotated
    runtime environment variables that should participate in provenance.

    If ``runtime_env`` and ``runtime_identity`` are empty, this is identical
    to ``compute_lockfile_hash``.
    """
    lockfile_hash = compute_lockfile_hash(notebook_dir)
    if not runtime_env and not runtime_identity:
        return lockfile_hash

    runtime_env = runtime_env or {}
    hasher = hashlib.sha256()
    hasher.update(lockfile_hash.encode("utf-8"))
    if runtime_identity:
        hasher.update(b"\0runtime=")
        hasher.update(runtime_identity.encode("utf-8"))
    for key, value in sorted(runtime_env.items()):
        hasher.update(b"\0")
        hasher.update(key.encode("utf-8"))
        hasher.update(b"=")
        hasher.update(value.encode("utf-8"))
    return hasher.hexdigest()
