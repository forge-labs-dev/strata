"""Polars integration for Strata.

DEPRECATED: This module has moved to strata.integration.polars.
This re-export is provided for backwards compatibility.
"""

from strata.integration.polars import (
    StrataPolarsScanner,
    scan_to_lazy,
    scan_to_polars,
)

__all__ = [
    "StrataPolarsScanner",
    "scan_to_lazy",
    "scan_to_polars",
]
