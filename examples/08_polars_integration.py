#!/usr/bin/env python3
"""
Example 8: Polars Integration

This example shows how to use Strata with Polars for DataFrame operations.
Polars is Arrow-native, so the integration is zero-copy.

What you'll learn:
    - How to use the Polars extension module
    - Zero-copy Arrow interop
    - Using LazyFrames for deferred execution
"""

import polars as pl

from strata.client import gt
from strata.polars_ext import StrataPolarsScanner, scan_to_lazy, scan_to_polars

table_uri = "file:///path/to/warehouse#my_db.events"

# Method 1: Simple one-shot scan
df = scan_to_polars(
    table_uri,
    columns=["id", "value", "category", "timestamp"],
    filters=[gt("value", 10.0)],
)

print("=== DataFrame from Strata ===")
print(df.head())

# Method 2: LazyFrame for deferred operations
lf = scan_to_lazy(
    table_uri,
    columns=["id", "value", "category"],
)

result = (
    lf.filter(pl.col("value") > 50)
    .group_by("category")
    .agg([
        pl.count().alias("count"),
        pl.col("value").mean().alias("avg_value"),
    ])
    .sort("count", descending=True)
    .head(10)
    .collect()
)

print("\n=== Aggregation Result ===")
print(result)

# Method 3: Reusable scanner for multiple tables
with StrataPolarsScanner() as scanner:
    events = scanner.scan(
        table_uri,
        columns=["id", "category", "value"],
    )

    # Polars operations
    summary = (
        events.group_by("category")
        .agg([
            pl.count().alias("count"),
            pl.col("value").sum().alias("total_value"),
        ])
    )
    print("\n=== Category Summary ===")
    print(summary.head())
