"""Strata: Snapshot-aware serving layer for Iceberg tables."""

from strata.client import StrataClient
from strata.config import StrataConfig
from strata.duckdb_ext import register_strata_scan
from strata.types import CacheKey, ReadPlan, ScanRequest, Task

__version__ = "0.1.0"

__all__ = [
    "CacheKey",
    "ReadPlan",
    "ScanRequest",
    "StrataClient",
    "StrataConfig",
    "Task",
    "register_strata_scan",
]
