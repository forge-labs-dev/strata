"""Local executors for embedded transform execution.

This module provides internal functions for running transforms locally
(within the server process or embedded executor). These functions are
not part of the public API - users should call client.materialize() instead.

Internal usage:
    from strata.executors import _run_local

    # Used by embedded executor in runner.py
    result = _run_local(build_spec, input_tables)

Supported executors:
- scan@v1: Read from Iceberg tables (server-only, cannot run locally)
- duckdb_sql@v1: Execute DuckDB SQL queries

To add new executors, create a Transform subclass and register it:
    @register_transform("my_transform@v1")
    class MyTransform(Transform):
        ...
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

# Import transforms to register them
import strata.transforms  # noqa: F401 - registers transforms
from strata.transforms.base import _run_transform, get_transform, list_transforms

if TYPE_CHECKING:
    import pyarrow as pa


def _run_local(
    build_spec: dict[str, Any],
    input_tables: dict[str, pa.Table],
) -> pa.Table:
    """Execute a transform locally based on the build spec (internal use only).

    This is an internal function used by the server's embedded executor.
    Users should call client.materialize() instead.

    Args:
        build_spec: BuildSpec containing:
            - executor: Executor URI (e.g., "duckdb_sql@v1", "local://duckdb_sql@v1")
            - params: Executor-specific parameters
            - input_uris: List of input URIs (used for ordering)
        input_tables: Mapping of input URI -> Arrow Table

    Returns:
        Result Arrow Table

    Raises:
        ValueError: If executor is not supported or inputs are missing
    """
    executor = build_spec.get("executor", "")
    params = build_spec.get("params", {})
    input_uris = build_spec.get("input_uris", [])

    # Build inputs list in order
    inputs: list[pa.Table] = []
    for uri in input_uris:
        table = input_tables.get(uri)
        if table is None:
            raise ValueError(f"Missing input table for URI: {uri}")
        inputs.append(table)

    # Run via transform registry
    return _run_transform(executor, inputs, params)


# Backward compatibility alias (deprecated)
run_local = _run_local


# Re-export for convenience
__all__ = [
    "_run_local",  # Internal: for embedded executor use
    "run_local",  # Deprecated: kept for backward compatibility
    "get_transform",
    "list_transforms",
]
