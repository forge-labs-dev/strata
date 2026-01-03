"""Pandas integration for Strata.

Provides helpers to convert Strata fetches to pandas DataFrames.
Uses Arrow as the intermediate format, then converts to pandas.

Note on memory: Unlike Polars, Arrow → pandas conversion typically
involves a copy because pandas uses its own memory layout. This is
expected behavior and acceptable for most use cases.

Important: pandas filter operations (e.g., df[df["value"] > 100]) are
applied *after* data is fetched from Strata. To get Strata-side pruning,
pass filters to the fetch functions. For example:

    # Strata-side pruning (fast, reduces data transfer):
    df = scan_to_pandas(uri, filters=[gt("value", 100)])

    # pandas-side filtering (after full fetch):
    df = scan_to_pandas(uri)
    df = df[df["value"] > 100]

For best performance, use Strata filters for coarse pruning and pandas
filters for fine-grained predicates.
"""

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pyarrow as pa

from strata.client import StrataClient
from strata.config import StrataConfig
from strata.types import Filter

if TYPE_CHECKING:
    import pandas as pd


def scan_to_pandas(
    table_uri: str,
    snapshot_id: int | None = None,
    columns: list[str] | None = None,
    filters: list[Filter] | None = None,
    config: StrataConfig | None = None,
    base_url: str | None = None,
) -> "pd.DataFrame":
    """Fetch an Iceberg table via Strata and return a pandas DataFrame.

    This is the simplest way to get Iceberg data into pandas.
    Converts via Arrow (may copy data due to pandas memory layout).

    Args:
        table_uri: Iceberg table URI (e.g., "file:///warehouse#db.table")
        snapshot_id: Specific snapshot to read (None for latest)
        columns: Columns to project (None for all)
        filters: Filters for row-group pruning
        config: Strata configuration
        base_url: Override server URL (default: http://127.0.0.1:8765)

    Returns:
        pandas DataFrame with the fetch results

    Example:
        from strata.integration.pandas import scan_to_pandas
        from strata.client import gt

        df = scan_to_pandas(
            "file:///warehouse#db.events",
            columns=["id", "value", "timestamp"],
            filters=[gt("value", 100.0)],
        )
        print(df.head())
    """
    client = StrataClient(config=config, base_url=base_url)

    try:
        arrow_table = client.fetch(
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
        )
        # Convert Arrow table to pandas (may copy due to memory layout)
        return arrow_table.to_pandas()
    finally:
        client.close()


class StrataPandasScanner:
    """A reusable scanner for pandas integration.

    Maintains a connection to the Strata server for multiple fetches.

    Example:
        from strata.integration.pandas import StrataPandasScanner

        with StrataPandasScanner() as scanner:
            events = scanner.scan("file:///warehouse#db.events")
            users = scanner.scan("file:///warehouse#db.users")

            # Merge in pandas
            result = events.merge(users, on="user_id")
    """

    def __init__(
        self,
        config: StrataConfig | None = None,
        base_url: str | None = None,
    ) -> None:
        self.client = StrataClient(config=config, base_url=base_url)

    def __enter__(self) -> "StrataPandasScanner":
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
    ) -> "pd.DataFrame":
        """Fetch a table and return a pandas DataFrame."""
        arrow_table = self.client.fetch(
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
        )
        return arrow_table.to_pandas()

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
            with StrataPandasScanner() as scanner:
                for batch in scanner.scan_batches("file:///warehouse#db.events"):
                    # Process each batch
                    df = batch.to_pandas()
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
