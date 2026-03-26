"""Provenance hashing for notebook cells.

Provenance hashing enables cache deduplication by computing a deterministic
hash of:
1. The sorted input artifact hashes (from upstream cells)
2. The cell source code (normalized)
3. The runtime environment hash (lockfile)

This ensures identical computations always produce the same hash and can
be cached.
"""

from __future__ import annotations

import hashlib


def compute_source_hash(source: str) -> str:
    """Compute SHA-256 hash of cell source code.

    Source normalization: we use the source as-is (no stripping).
    Whitespace changes should invalidate cache — they may affect
    semantics (e.g., indentation in control flow).

    Args:
        source: Cell source code

    Returns:
        SHA-256 hex digest
    """
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def compute_provenance_hash(
    input_hashes: list[str],
    source_hash: str,
    env_hash: str,
) -> str:
    """Compute the provenance hash for a cell execution.

    The hash uniquely identifies a computation based on:
    1. Content hashes of all input artifacts (sorted for determinism)
    2. The cell source code hash
    3. The runtime environment hash (lockfile)

    Args:
        input_hashes: Hashes of upstream artifacts this cell consumes.
                     Will be sorted for deterministic ordering.
        source_hash: SHA-256 of cell source code
        env_hash: SHA-256 of runtime lockfile dependencies

    Returns:
        SHA-256 hex digest of the combined provenance
    """
    # Sort input hashes for deterministic ordering
    sorted_inputs = sorted(input_hashes)

    # Combine all components
    hasher = hashlib.sha256()

    # Add sorted input hashes
    for h in sorted_inputs:
        hasher.update(h.encode("utf-8"))
        hasher.update(b"\x00")  # Separator

    # Add source hash
    hasher.update(source_hash.encode("utf-8"))
    hasher.update(b"\x00")

    # Add environment hash
    hasher.update(env_hash.encode("utf-8"))

    return hasher.hexdigest()
