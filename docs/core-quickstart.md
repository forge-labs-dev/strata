# Strata Core Quick Start

Strata Core is the lower-level materialization and artifact layer. Use this path
if you want the programmatic API, artifact caching, lineage, and executor
integration.

At its core, Strata provides one primitive:

```text
materialize(inputs, transform) -> artifact
```

That primitive gives you:

- immutable, versioned artifacts
- provenance-based deduplication
- explicit lineage
- safe reuse across runs and processes

## 1. Install

Strata currently targets Python 3.12+ and needs Rust to build the extension.

```bash
uv sync
```

## 2. Start the server

```bash
strata-server
```

## 3. Run the demo

In another terminal:

```bash
uv run python examples/hello_world.py
```

That creates a demo Iceberg table and exercises the cold -> warm -> restart
cache path.

## 4. Materialize a result

```python
from strata import StrataClient

client = StrataClient()

artifact = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={
        "executor": "duckdb_sql@v1",
        "params": {"sql": "SELECT category, COUNT(*) AS cnt FROM input0 GROUP BY 1"},
    },
    name="category_counts",
)

print(artifact.uri)
print(artifact.cache_hit)
```

## 5. Fetch the result

```python
table = client.fetch(artifact.uri)
df = table.to_pandas()
```

## 6. Materialize from an Iceberg table

```python
artifact = client.materialize(
    inputs=["file:///warehouse#db.events"],
    transform={
        "executor": "scan@v1",
        "params": {
            "columns": ["id", "timestamp", "value"],
            "filters": [{"column": "value", "op": ">", "value": 100}],
        },
    },
)

table = client.fetch(artifact.uri)
```

## Core Behaviors

- same inputs + transform -> existing artifact, no recomputation
- artifacts are immutable and versioned
- names are mutable pointers to specific artifact versions
- provenance hash is derived from pinned inputs and transform identity

## Where Next

- root overview and architecture: [README.md](../README.md)
- unified materialize API design:
  [docs/design/unified-materialize-api.md](design/unified-materialize-api.md)
- examples: [examples](../examples)
