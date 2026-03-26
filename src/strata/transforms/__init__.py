"""Transform registry and execution.

This module provides:
- Transform base class for defining transforms
- Built-in transforms (scan@v1, duckdb_sql@v1)
- Server-mode transform registry and build runner
"""

# Core transform abstraction
from strata.transforms.base import (
    Transform,
    _run_transform,
    get_transform,
    list_transforms,
    register_transform,
    run_transform,  # Deprecated, use _run_transform internally
)

# Server-mode infrastructure
from strata.transforms.build_store import (
    BuildState,
    BuildStore,
    get_build_store,
)

# Built-in transforms (auto-register on import)
from strata.transforms.duckdb_sql import (
    DuckDBSQLParams,
    DuckDBSQLTransform,
    build_duckdb_sql_transform,
)
from strata.transforms.registry import (
    TransformDefinition,
    TransformRegistry,
    get_transform_registry,
)
from strata.transforms.runner import (
    BuildRunner,
    RunnerConfig,
    get_build_runner,
    set_build_runner,
)
from strata.transforms.scan import (
    ScanParams,
    ScanTransform,
    build_scan_transform,
)

__all__ = [
    # Core transform abstraction
    "Transform",
    "_run_transform",  # Internal: for server/embedded executor use
    "get_transform",
    "list_transforms",
    "register_transform",
    "run_transform",  # Deprecated: kept for backward compatibility
    # Built-in transforms
    "DuckDBSQLParams",
    "DuckDBSQLTransform",
    "ScanParams",
    "ScanTransform",
    "build_duckdb_sql_transform",
    "build_scan_transform",
    # Server-mode infrastructure
    "BuildRunner",
    "BuildState",
    "BuildStore",
    "RunnerConfig",
    "TransformDefinition",
    "TransformRegistry",
    "get_build_runner",
    "get_build_store",
    "get_transform_registry",
    "set_build_runner",
]
