#!/usr/bin/env python3
"""
Example 3: Filtering with Predicates

This example shows how to use filters to reduce data at the source.
Filters enable row-group pruning, which can dramatically reduce I/O.

What you'll learn:
    - How to use comparison filters (gt, lt, eq, etc.)
    - How to combine multiple filters
    - How filters enable row-group pruning
"""

from strata.client import StrataClient, eq, gt, gte, lt, lte

client = StrataClient(base_url="http://127.0.0.1:8765")
table_uri = "file:///path/to/warehouse#my_db.events"

# Filter with greater-than
# This prunes row groups where max(timestamp) < 1704067200000000
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
        filters=[gte("value", 10.0), lte("value", 100.0), eq("category", "electronics")],
    )
)

# Check how many row groups were pruned
# (visible in server metrics)
metrics = client.metrics()
print(f"Row groups pruned: {metrics.get('row_groups_pruned', 0)}")

client.close()
