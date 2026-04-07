# Core API Quickstart

Strata Core is the programmatic materialization and artifact layer. Use this if you want the `materialize()` API, artifact caching, lineage, and executor integration.

## The primitive

```
materialize(inputs, transform) → artifact
```

This gives you:

- Immutable, versioned artifacts
- Provenance-based deduplication (same inputs + transform = cache hit)
- Explicit lineage
- Safe reuse across runs and processes

## 1. Start the server

```bash
uv run strata-server
```

## 2. Run the demo

```bash
uv run python examples/hello_world.py
```

This creates a demo Iceberg table and exercises the cold → warm → restart cache path.

## 3. Materialize a result

```python
from strata.client import StrataClient

client = StrataClient()

artifact = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={
        "executor": "scan@v1",
        "params": {
            "columns": ["id", "value"],
            "filters": [{"column": "value", "op": ">", "value": 100}],
        },
    },
)

print(f"URI: {artifact.uri}")
print(f"Cache hit: {artifact.cache_hit}")
```

## 4. Fetch the result

```python
table = client.fetch(artifact.uri)
df = table.to_pandas()
```

## 5. Integration with data libraries

=== "Pandas"

    ```python
    from strata.integration.pandas import fetch_to_pandas
    df = fetch_to_pandas("file:///warehouse#db.events")
    ```

=== "Polars"

    ```python
    from strata.integration.polars import fetch_to_polars
    df = fetch_to_polars("file:///warehouse#db.events")
    ```

=== "DuckDB"

    ```python
    from strata.integration.duckdb import StrataScanner
    with StrataScanner() as scanner:
        scanner.register("events", "file:///warehouse#db.events")
        result = scanner.query("SELECT * FROM events WHERE id > 100")
    ```

## Core behaviors

- Same inputs + transform → existing artifact, no recomputation
- Artifacts are immutable and versioned
- Names are mutable pointers to specific artifact versions
- Provenance hash is derived from pinned inputs and transform identity

## What's next

- [Configuration reference](../reference/configuration.md) — all environment variables
- [REST API reference](../reference/rest-api.md) — the full API surface
