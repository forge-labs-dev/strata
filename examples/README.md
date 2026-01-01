# Strata Examples

Learn how to use Strata through these examples, ordered from basic to advanced.

## Getting Started

Before running examples, start the Strata server:

```bash
uv run python -m strata
```

Or with the CLI:

```bash
strata-server
```

## Examples

### Core Usage

| File | Description |
|------|-------------|
| [01_basic_usage.py](01_basic_usage.py) | Connect to Strata and scan a table |
| [02_column_projection.py](02_column_projection.py) | Select specific columns to reduce data transfer |
| [03_filtering.py](03_filtering.py) | Use predicates for row-group pruning |
| [04_time_travel.py](04_time_travel.py) | Query historical snapshots |

### Integrations

| File | Description |
|------|-------------|
| [05_duckdb_integration.py](05_duckdb_integration.py) | Run SQL queries with DuckDB |
| [08_polars_integration.py](08_polars_integration.py) | Use Polars DataFrames with zero-copy Arrow |
| [09_s3_storage.py](09_s3_storage.py) | Connect to Iceberg tables in S3 |

### Advanced Features

| File | Description |
|------|-------------|
| [06_cache_management.py](06_cache_management.py) | Monitor and manage the cache |
| [07_error_handling.py](07_error_handling.py) | Handle common errors gracefully |
| [10_artifacts.py](10_artifacts.py) | Materialize, chain, and track transform artifacts |
| [11_async_client.py](11_async_client.py) | Non-blocking async operations for high throughput |

### Demo Scripts

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

## Async Quick Start

```python
import asyncio
from strata.client import AsyncStrataClient, gt

async def main():
    async with AsyncStrataClient() as client:
        table = await client.scan_to_table(
            "file:///warehouse#db.events",
            columns=["id", "value"],
            filters=[gt("value", 100.0)],
        )
        print(f"Got {table.num_rows} rows")

asyncio.run(main())
```

## Artifact Workflow

```python
from strata.client import StrataClient

# Materialize a transform result as an artifact
with StrataClient(base_url="http://127.0.0.1:8765") as client:
    artifact = client.materialize(
        inputs=["file:///warehouse#db.events"],
        transform={
            "ref": "duckdb_sql@v1",
            "params": {"sql": "SELECT category, COUNT(*) FROM input0 GROUP BY 1"}
        },
        name="daily_summary",
    )
    print(f"Artifact URI: {artifact.uri}")
    print(f"Cache hit: {artifact.cache_hit}")
    print(artifact.to_pandas())
```
