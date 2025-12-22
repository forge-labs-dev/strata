#!/usr/bin/env python3
"""
Example 1: Basic Strata Usage

This example shows the simplest way to use Strata to query an Iceberg table.

Prerequisites:
    1. Start the Strata server: strata-server
    2. Have an Iceberg table accessible via file:// URI

What you'll learn:
    - How to connect to a Strata server
    - How to scan a table and get Arrow batches
    - How to convert results to pandas
"""

from strata.client import StrataClient

# Connect to Strata server
client = StrataClient(base_url="http://127.0.0.1:8765")

# Define your table URI
# Format: file://<warehouse_path>#<namespace>.<table>
table_uri = "file:///path/to/warehouse#my_db.my_table"

# Scan the table - returns an iterator of Arrow RecordBatches
batches = client.scan(table_uri)

# Option 1: Process batches one at a time (memory efficient)
for batch in batches:
    print(f"Got batch with {batch.num_rows} rows")
    print(f"Columns: {batch.schema.names}")

# Option 2: Collect all batches and convert to pandas
batches = list(client.scan(table_uri))
if batches:
    import pyarrow as pa

    table = pa.Table.from_batches(batches)
    df = table.to_pandas()
    print(df.head())

# Always close the client when done
client.close()
