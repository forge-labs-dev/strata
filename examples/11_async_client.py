#!/usr/bin/env python3
"""
Example 11: Async Client

This example shows how to use Strata's async client for non-blocking
operations. The async client is ideal for:
- Web applications (FastAPI, aiohttp, etc.)
- Concurrent data fetching
- High-throughput data pipelines

What you'll learn:
    - How to use AsyncStrataClient
    - Concurrent scanning of multiple tables
    - Async context managers
"""

import asyncio

from strata.client import AsyncStrataClient, gt


async def basic_async_scan():
    """Basic async scan example."""
    async with AsyncStrataClient(base_url="http://127.0.0.1:8765") as client:
        # Check server health
        health = await client.health()
        print(f"Server status: {health['status']}")

        # Scan a table asynchronously
        table_uri = "file:///path/to/warehouse#analytics.events"

        # Stream batches as they arrive
        async for batch in client.scan(table_uri, columns=["id", "value"]):
            print(f"Received batch with {batch.num_rows} rows")

        # Or collect to Arrow table
        table = await client.scan_to_table(
            table_uri,
            columns=["id", "value", "category"],
            filters=[gt("value", 100.0)],
        )
        print(f"\nTotal rows: {table.num_rows}")


async def concurrent_scans():
    """Fetch multiple tables concurrently."""
    async with AsyncStrataClient() as client:
        # Define tables to scan
        tables = [
            ("file:///warehouse#db.events", ["id", "value"]),
            ("file:///warehouse#db.users", ["id", "name"]),
            ("file:///warehouse#db.products", ["id", "price"]),
        ]

        # Create scan tasks
        async def scan_table(uri: str, columns: list[str]):
            return await client.scan_to_table(uri, columns=columns)

        # Run concurrently
        results = await asyncio.gather(
            *[scan_table(uri, cols) for uri, cols in tables],
            return_exceptions=True,
        )

        for (uri, _), result in zip(tables, results):
            if isinstance(result, Exception):
                print(f"{uri}: Error - {result}")
            else:
                print(f"{uri}: {result.num_rows} rows")


async def async_with_timeout():
    """Async scan with custom timeout."""
    async with AsyncStrataClient(timeout=30.0) as client:
        table_uri = "file:///warehouse#db.large_table"

        try:
            table = await asyncio.wait_for(
                client.scan_to_table(table_uri),
                timeout=10.0,  # 10 second timeout
            )
            print(f"Got {table.num_rows} rows")
        except TimeoutError:
            print("Scan timed out - try adding filters")


async def stream_to_processor():
    """Stream batches to an async processor."""
    async with AsyncStrataClient() as client:
        table_uri = "file:///warehouse#db.events"

        # Process batches as they arrive
        total_value = 0.0
        row_count = 0

        async for batch in client.scan(table_uri, columns=["value"]):
            # Process each batch without collecting all in memory
            import pyarrow.compute as pc

            total_value += pc.sum(batch.column("value")).as_py() or 0.0
            row_count += batch.num_rows

        print(f"Processed {row_count} rows")
        print(f"Total value: {total_value:,.2f}")


async def main():
    """Run all examples."""
    print("=== Basic Async Scan ===")
    await basic_async_scan()

    print("\n=== Concurrent Scans ===")
    await concurrent_scans()

    print("\n=== Async with Timeout ===")
    await async_with_timeout()

    print("\n=== Stream to Processor ===")
    await stream_to_processor()


if __name__ == "__main__":
    asyncio.run(main())
