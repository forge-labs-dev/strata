"""Local executors for client-side transform execution.

This module provides executor implementations for running transforms locally
in the client process. The server never executes transforms - it only returns
BuildSpecs that clients use to execute locally.

Supported executors:
- local://duckdb_sql@v1: Execute DuckDB SQL queries

Example usage:
    from strata.executors import run_local

    result = run_local(build_spec, input_tables)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa


def run_local(
    build_spec: dict,
    input_tables: dict[str, pa.Table],
) -> pa.Table:
    """Execute a transform locally based on the build spec.

    This is the main entry point for client-side execution. It dispatches
    to the appropriate executor based on the executor URI in the build spec.

    Args:
        build_spec: BuildSpec from materialize() response containing:
            - executor: Executor URI (e.g., "local://duckdb_sql@v1")
            - params: Executor-specific parameters
            - input_uris: List of input URIs
        input_tables: Mapping of input URI -> Arrow Table

    Returns:
        Result Arrow Table

    Raises:
        ValueError: If executor is not supported
    """
    executor = build_spec.get("executor", "")

    # Accept both "local://duckdb_sql@v1" and "duckdb_sql@v1" formats
    if executor.startswith("local://duckdb_sql") or executor.startswith("duckdb_sql"):
        return _run_duckdb_sql(build_spec, input_tables)
    else:
        raise ValueError(f"Unsupported executor: {executor}")


def _run_duckdb_sql(
    build_spec: dict,
    input_tables: dict[str, pa.Table],
) -> pa.Table:
    """Execute a DuckDB SQL transform locally.

    Args:
        build_spec: BuildSpec with params containing SQL query
        input_tables: Mapping of input URI -> Arrow Table

    Returns:
        Result Arrow Table

    Raises:
        ImportError: If DuckDB is not installed
        ValueError: If SQL is missing or input table is not provided
    """
    try:
        import duckdb
    except ImportError:
        raise ImportError(
            "DuckDB is required for local execution. Install it with: pip install duckdb"
        )

    # Get SQL query from params
    sql = build_spec.get("params", {}).get("sql")
    if not sql:
        raise ValueError("DuckDB executor requires 'sql' in params")

    # Create DuckDB connection
    conn = duckdb.connect(":memory:")

    # Register input tables with sanitized names
    input_uris = build_spec.get("input_uris", [])
    for i, uri in enumerate(input_uris):
        table = input_tables.get(uri)
        if table is None:
            raise ValueError(f"Missing input table for URI: {uri}")
        # Use generic names: input0, input1, etc.
        conn.register(f"input{i}", table)

    # Execute query
    result = conn.execute(sql).fetch_arrow_table()
    return result
