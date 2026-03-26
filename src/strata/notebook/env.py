"""Environment hashing for notebook dependencies.

For now, we compute the hash of the entire uv.lock file.
Runtime-only filtering (excluding dev deps) is a future optimization.
"""

from __future__ import annotations

import hashlib
import logging
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
