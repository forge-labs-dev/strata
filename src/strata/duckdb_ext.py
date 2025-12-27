"""DuckDB integration for Strata.

DEPRECATED: This module has moved to strata.integration.duckdb.
This re-export is provided for backwards compatibility.
"""

from strata.integration.duckdb import (
    StrataScanner,
    StrataTableParams,
    register_strata_scan,
    strata_query,
)

__all__ = [
    "StrataScanner",
    "StrataTableParams",
    "register_strata_scan",
    "strata_query",
]
