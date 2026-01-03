"""Polars integration for Strata.

Provides helpers to convert Strata fetches to Polars DataFrames.
Polars is Arrow-native, so conversions are typically zero-copy when
Arrow types are supported (may copy for dictionary encoding, large
strings, or extension types).

Important: Polars filter operations (e.g., df.filter(...)) are applied
*after* data is fetched from Strata. To get Strata-side pruning, pass
filters to the fetch functions. For example:

    # Strata-side pruning (fast, reduces data transfer):
    df = scan_to_polars(uri, filters=[gt("value", 100)])

    # Polars-side filtering (after full fetch):
    df = scan_to_polars(uri).filter(pl.col("value") > 100)

For best performance, use Strata filters for coarse pruning and Polars
filters for fine-grained predicates.
"""

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pyarrow as pa

from strata.client import StrataClient
from strata.config import StrataConfig
from strata.types import Filter

if TYPE_CHECKING:
    import polars as pl


def scan_to_polars(
    table_uri: str,
    snapshot_id: int | None = None,
    columns: list[str] | None = None,
    filters: list[Filter] | None = None,
    config: StrataConfig | None = None,
    base_url: str | None = None,
) -> "pl.DataFrame":
    """Fetch an Iceberg table via Strata and return a Polars DataFrame.

    This is the simplest way to get Iceberg data into Polars.
    Arrow-native; typically zero-copy when types are supported.

    Args:
        table_uri: Iceberg table URI (e.g., "file:///warehouse#db.table")
        snapshot_id: Specific snapshot to read (None for latest)
        columns: Columns to project (None for all)
        filters: Filters for row-group pruning
        config: Strata configuration
        base_url: Override server URL (default: http://127.0.0.1:8765)

    Returns:
        Polars DataFrame with the fetch results

    Example:
        from strata.integration.polars import scan_to_polars
        from strata.client import gt

        df = scan_to_polars(
            "file:///warehouse#db.events",
            columns=["id", "value", "timestamp"],
            filters=[gt("value", 100.0)],
        )
        print(df.head())
    """
    import polars as pl

    client = StrataClient(config=config, base_url=base_url)

    try:
        arrow_table = client.fetch(
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
        )
        # Arrow-native; typically zero-copy when types are supported
        return pl.from_arrow(arrow_table)
    finally:
        client.close()


def scan_to_lazy(
    table_uri: str,
    snapshot_id: int | None = None,
    columns: list[str] | None = None,
    filters: list[Filter] | None = None,
    config: StrataConfig | None = None,
    base_url: str | None = None,
) -> "pl.LazyFrame":
    """Scan an Iceberg table via Strata and return a Polars LazyFrame.

    NOTE: Data is fetched eagerly from Strata, then wrapped in a LazyFrame
    for downstream lazy transforms. This is NOT true lazy evaluation from
    storage—use this when you want Polars' lazy API for chaining operations
    after the fetch is complete.

    For true streaming, use StrataPolarsScanner.scan_batches() which yields
    Arrow RecordBatches as they arrive.

    Args:
        table_uri: Iceberg table URI
        snapshot_id: Specific snapshot to read
        columns: Columns to project
        filters: Filters for row-group pruning
        config: Strata configuration
        base_url: Override server URL

    Returns:
        Polars LazyFrame wrapping eagerly-fetched data

    Example:
        from strata.integration.polars import scan_to_lazy

        # Data is fetched immediately, but downstream ops are lazy
        lf = scan_to_lazy("file:///warehouse#db.events")
        result = (
            lf
            .filter(pl.col("value") > 100)
            .group_by("category")
            .agg(pl.col("value").mean())
            .collect()  # Only this triggers Polars computation
        )
    """
    df = scan_to_polars(
        table_uri=table_uri,
        snapshot_id=snapshot_id,
        columns=columns,
        filters=filters,
        config=config,
        base_url=base_url,
    )
    return df.lazy()


class StrataPolarsScanner:
    """A reusable scanner for Polars integration.

    Maintains a connection to the Strata server for multiple fetches.

    Example:
        from strata.integration.polars import StrataPolarsScanner

        with StrataPolarsScanner() as scanner:
            events = scanner.scan("file:///warehouse#db.events")
            users = scanner.scan("file:///warehouse#db.users")

            # Join in Polars
            result = events.join(users, on="user_id")
    """

    def __init__(
        self,
        config: StrataConfig | None = None,
        base_url: str | None = None,
    ) -> None:
        self.client = StrataClient(config=config, base_url=base_url)

    def __enter__(self) -> "StrataPolarsScanner":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def close(self) -> None:
        """Close the client connection."""
        self.client.close()

    def scan(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> "pl.DataFrame":
        """Fetch a table and return a Polars DataFrame."""
        import polars as pl

        arrow_table = self.client.fetch(
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
        )
        return pl.from_arrow(arrow_table)

    def scan_lazy(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> "pl.LazyFrame":
        """Fetch a table and return a Polars LazyFrame.

        NOTE: Data is fetched eagerly, then wrapped in LazyFrame.
        For streaming, use scan_batches().
        """
        return self.scan(
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
        ).lazy()

    def scan_batches(
        self,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Fetch a table and yield Arrow RecordBatches.

        Uses the unified materialize API and returns all batches from the
        Arrow IPC stream.

        Args:
            table_uri: Iceberg table URI
            snapshot_id: Specific snapshot to read
            columns: Columns to project
            filters: Filters for row-group pruning

        Yields:
            pyarrow.RecordBatch objects from the fetched data

        Example:
            with StrataPolarsScanner() as scanner:
                for batch in scanner.scan_batches("file:///warehouse#db.events"):
                    # Process each batch
                    df = pl.from_arrow(batch)
                    process(df)
        """
        # Use fetch and iterate over the table's batches
        arrow_table = self.client.fetch(
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
        )
        yield from arrow_table.to_batches()
