#!/usr/bin/env python3
"""
Example 4: Time Travel (Snapshot Queries)

This example shows how to query historical snapshots of a table.
Iceberg maintains a history of table snapshots, and Strata can read any of them.

What you'll learn:
    - How to query a specific snapshot by ID
    - When to use time travel
"""

from strata.client import StrataClient

client = StrataClient(base_url="http://127.0.0.1:8765")
table_uri = "file:///path/to/warehouse#my_db.events"

# Query the current (latest) snapshot
# snapshot_id=None means "use the latest"
current_batches = list(client.scan(table_uri))
print(f"Current snapshot: {sum(b.num_rows for b in current_batches)} rows")

# Query a specific historical snapshot
# You can get snapshot IDs from Iceberg table metadata
historical_snapshot_id = 1234567890123456789

historical_batches = list(client.scan(table_uri, snapshot_id=historical_snapshot_id))
print(f"Historical snapshot: {sum(b.num_rows for b in historical_batches)} rows")

# Use case: Compare current vs historical data
# This is useful for:
# - Auditing changes
# - Reproducing ML training data
# - Debugging data issues

client.close()
