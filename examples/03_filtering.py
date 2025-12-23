#!/usr/bin/env python3
"""
Example 3: Filtering with Predicates

This example shows how to use filters to reduce data at the source.
Filters enable two-tier pruning that can dramatically reduce I/O:

1. **Iceberg file-level pruning**: Uses manifest statistics (per-file min/max)
   to skip entire Parquet files before reading any file metadata.

2. **Parquet row-group pruning**: Uses row group statistics (per-chunk min/max)
   to skip row groups within files that don't match the filter.

What you'll learn:
    - How to use comparison filters (gt, lt, eq, etc.)
    - How to combine multiple filters
    - How filters enable two-tier pruning
"""

from strata.client import StrataClient, eq, ge, gt, le, lt

client = StrataClient(base_url="http://127.0.0.1:8765")
table_uri = "file:///path/to/warehouse#my_db.events"

# Filter with greater-than
# Two-tier pruning:
#   1. Iceberg skips files where max(timestamp) < 1704067200000000
#   2. Parquet skips row groups where max(timestamp) < 1704067200000000
batches = list(
    client.scan(
        table_uri,
        filters=[gt("timestamp", 1704067200000000)],  # After 2024-01-01
    )
)

# Filter with less-than
batches = list(client.scan(table_uri, filters=[lt("value", 100.0)]))

# Filter with equality
batches = list(client.scan(table_uri, filters=[eq("category", "electronics")]))

# Combine multiple filters (AND logic)
# All conditions must be true for a row to be included
batches = list(
    client.scan(
        table_uri,
        columns=["id", "value", "category"],
        filters=[ge("value", 10.0), le("value", 100.0), eq("category", "electronics")],
    )
)

# Check how many row groups were pruned
# (visible in server metrics)
metrics = client.metrics()
print(f"Row groups pruned: {metrics.get('row_groups_pruned', 0)}")

client.close()
