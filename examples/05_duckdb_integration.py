#!/usr/bin/env python3
"""
Example 5: DuckDB Integration

This example shows how to use Strata with DuckDB for SQL queries.
Strata provides a custom DuckDB scanner that fetches data efficiently.

What you'll learn:
    - How to register Strata as a DuckDB table function
    - How to run SQL queries against Iceberg tables via Strata
"""

import duckdb

from strata.duckdb_ext import register_strata

# Create a DuckDB connection
conn = duckdb.connect()

# Register the Strata scanner
# This adds a 'strata_scan' table function to DuckDB
register_strata(conn, base_url="http://127.0.0.1:8765")

# Define your table URI
table_uri = "file:///path/to/warehouse#my_db.events"

# Query using SQL
result = conn.execute(f"""
    SELECT
        category,
        COUNT(*) as count,
        AVG(value) as avg_value
    FROM strata_scan('{table_uri}')
    GROUP BY category
    ORDER BY count DESC
    LIMIT 10
""").fetchdf()

print(result)

# You can also use column projection in the scan
# This pushes projection down to Strata
result = conn.execute(f"""
    SELECT id, value
    FROM strata_scan('{table_uri}', columns=['id', 'value'])
    WHERE value > 50
""").fetchdf()

print(result)

conn.close()
