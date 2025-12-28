"""Strata: Snapshot-aware serving layer for Iceberg tables."""

from strata.client import AsyncStrataClient, RetryConfig, StrataClient
from strata.config import StrataConfig
from strata.integration.duckdb import register_strata_scan
from strata.types import CacheKey, ReadPlan, ScanRequest, Task

__version__ = "0.1.0"

__all__ = [
    "AsyncStrataClient",
    "CacheKey",
    "ReadPlan",
    "RetryConfig",
    "ScanRequest",
    "StrataClient",
    "StrataConfig",
    "Task",
    "register_strata_scan",
]
