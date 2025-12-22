# Strata Examples

Learn how to use Strata through these examples, ordered from basic to advanced.

## Getting Started

Before running examples, start the Strata server:

```bash
strata-server
```

## Examples

| File | Description |
|------|-------------|
| [01_basic_usage.py](01_basic_usage.py) | Connect to Strata and scan a table |
| [02_column_projection.py](02_column_projection.py) | Select specific columns to reduce data transfer |
| [03_filtering.py](03_filtering.py) | Use predicates for row-group pruning |
| [04_time_travel.py](04_time_travel.py) | Query historical snapshots |
| [05_duckdb_integration.py](05_duckdb_integration.py) | Run SQL queries with DuckDB |
| [06_cache_management.py](06_cache_management.py) | Monitor and manage the cache |
| [07_error_handling.py](07_error_handling.py) | Handle common errors |

## Demo Scripts

| File | Description |
|------|-------------|
| [setup_demo.py](setup_demo.py) | Create a demo Iceberg table for testing |
| [hello_world.py](hello_world.py) | Benchmark cold/warm/restart performance |

## Quick Start

```python
from strata.client import StrataClient, gt

client = StrataClient(base_url="http://127.0.0.1:8765")

# Scan with projection and filter
batches = list(client.scan(
    "file:///warehouse#db.events",
    columns=["id", "value", "timestamp"],
    filters=[gt("timestamp", 1704067200000000)]
))

# Convert to pandas
import pyarrow as pa
df = pa.Table.from_batches(batches).to_pandas()
print(df.head())

client.close()
```
