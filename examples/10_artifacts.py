#!/usr/bin/env python3
"""
Example 10: Artifact Workflow

This example shows how to use Strata's artifact system for caching
transform results. Artifacts are immutable, versioned outputs that
can depend on source tables or other artifacts.

What you'll learn:
    - How to materialize artifacts from source tables
    - How to chain artifacts (use one artifact as input to another)
    - How to track artifact lineage
    - How to query artifact staleness
"""

import httpx

# Strata server URL
BASE_URL = "http://127.0.0.1:8765"

# Source table URI
source_uri = "file:///path/to/warehouse#analytics.events"


def create_client() -> httpx.Client:
    """Create an HTTP client for Strata API calls."""
    return httpx.Client(base_url=BASE_URL)


# =============================================================================
# Example 1: Materialize an artifact from a source table
# =============================================================================

with create_client() as client:
    # Materialize an artifact - Strata runs the DuckDB SQL transform
    resp = client.post(
        "/v1/artifacts/materialize",
        json={
            "artifact_id": "daily_summary",
            "inputs": [source_uri],
            "transform": {
                "ref": "duckdb_sql@v1",
                "params": {
                    "sql": """
                        SELECT
                            category,
                            COUNT(*) as count,
                            SUM(value) as total_value
                        FROM input0
                        GROUP BY category
                    """
                },
            },
        },
    )
    result = resp.json()
    print(f"Build ID: {result['build_id']}")
    print(f"Status: {result['status']}")
    print(f"Artifact: {result['artifact_id']}@v{result['version']}")

    # Get artifact info
    artifact_id = result["artifact_id"]
    version = result["version"]

    info = client.get(f"/v1/artifacts/{artifact_id}/v/{version}").json()
    print("\nArtifact Info:")
    print(f"  Size: {info['size_bytes']:,} bytes")
    print(f"  Rows: {info.get('row_count', 'N/A')}")
    print(f"  Created: {info['created_at']}")


# =============================================================================
# Example 2: Chain artifacts (use output as input to another artifact)
# =============================================================================

with create_client() as client:
    # First artifact: filter and aggregate
    resp1 = client.post(
        "/v1/artifacts/materialize",
        json={
            "artifact_id": "filtered_events",
            "inputs": [source_uri],
            "transform": {
                "ref": "duckdb_sql@v1",
                "params": {
                    "sql": "SELECT * FROM input0 WHERE value > 100",
                },
            },
        },
    )
    result1 = resp1.json()
    filtered_uri = f"artifact://filtered_events/v/{result1['version']}"

    # Second artifact: uses first artifact as input
    resp2 = client.post(
        "/v1/artifacts/materialize",
        json={
            "artifact_id": "category_stats",
            "inputs": [filtered_uri],  # Reference the first artifact
            "transform": {
                "ref": "duckdb_sql@v1",
                "params": {
                    "sql": """
                        SELECT category, AVG(value) as avg_value
                        FROM input0
                        GROUP BY category
                    """,
                },
            },
        },
    )
    result2 = resp2.json()
    print(f"\nChained artifact: {result2['artifact_id']}@v{result2['version']}")


# =============================================================================
# Example 3: Query artifact lineage
# =============================================================================

with create_client() as client:
    # Get upstream lineage (what inputs led to this artifact)
    resp = client.get(
        "/v1/artifacts/category_stats/v/1/lineage",
        params={"direction": "upstream", "max_depth": 5},
    )
    lineage = resp.json()

    print("\nUpstream Lineage:")
    for node in lineage["nodes"]:
        print(f"  {node['artifact_id']}@v{node['version']}")
        for input_ref in node.get("inputs", []):
            print(f"    <- {input_ref}")


# =============================================================================
# Example 4: Check if artifacts are stale
# =============================================================================

with create_client() as client:
    # Get staleness status for a named artifact
    resp = client.get("/v1/artifacts/names/daily_summary/status")
    status = resp.json()

    print("\nStaleness Check for 'daily_summary':")
    print(f"  Is stale: {status['is_stale']}")
    print(f"  Current version: {status['current_version']}")
    if status["is_stale"]:
        print(f"  Stale reason: {status.get('stale_reason', 'Input changed')}")


# =============================================================================
# Example 5: Explain what would happen without running
# =============================================================================

with create_client() as client:
    # Dry-run to see if materialization is needed
    resp = client.post(
        "/v1/artifacts/explain-materialize",
        json={
            "artifact_id": "daily_summary",
            "inputs": [source_uri],
            "transform": {
                "ref": "duckdb_sql@v1",
                "params": {"sql": "SELECT * FROM input0"},
            },
        },
    )
    explanation = resp.json()

    print("\nExplain Materialize:")
    print(f"  Would reuse cache: {explanation['cache_hit']}")
    print(f"  Provenance hash: {explanation['provenance_hash'][:16]}...")
    if explanation["cache_hit"]:
        print(f"  Existing version: {explanation['existing_version']}")
    else:
        print("  Would create new version")


# =============================================================================
# Example 6: Download artifact data
# =============================================================================

with create_client() as client:
    # Get artifact data as Arrow IPC stream
    resp = client.get("/v1/artifacts/daily_summary/v/1/data")

    if resp.status_code == 200:
        # Parse Arrow data
        import io

        import pyarrow.ipc as ipc

        reader = ipc.open_stream(io.BytesIO(resp.content))
        table = reader.read_all()
        print(f"\nArtifact data: {table.num_rows} rows, {table.num_columns} columns")
        print(table.to_pandas().head())
    else:
        print(f"Error: {resp.json()}")
