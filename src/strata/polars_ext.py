"""Polars integration for Strata.

Provides helpers to convert Strata scans to Polars DataFrames.
Polars is Arrow-native, so all conversions are zero-copy.
"""

from typing import TYPE_CHECKING

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
    """Scan an Iceberg table via Strata and return a Polars DataFrame.

    This is the simplest way to get Iceberg data into Polars.
    The conversion is zero-copy since Polars is Arrow-native.

    Args:
        table_uri: Iceberg table URI (e.g., "file:///warehouse#db.table")
        snapshot_id: Specific snapshot to read (None for latest)
        columns: Columns to project (None for all)
        filters: Filters for row-group pruning
        config: Strata configuration
        base_url: Override server URL (default: http://127.0.0.1:8765)

    Returns:
        Polars DataFrame with the scan results

    Example:
        from strata.polars_ext import scan_to_polars
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
        arrow_table = client.scan_to_table(
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
        )
        # Zero-copy conversion to Polars
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

    Returns a LazyFrame for deferred execution. The data is fetched
    immediately but Polars operations are lazy.

    Args:
        table_uri: Iceberg table URI
        snapshot_id: Specific snapshot to read
        columns: Columns to project
        filters: Filters for row-group pruning
        config: Strata configuration
        base_url: Override server URL

    Returns:
        Polars LazyFrame for deferred operations

    Example:
        from strata.polars_ext import scan_to_lazy

        lf = scan_to_lazy("file:///warehouse#db.events")
        result = (
            lf
            .filter(pl.col("value") > 100)
            .group_by("category")
            .agg(pl.col("value").mean())
            .collect()
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

    Maintains a connection to the Strata server for multiple scans.

    Example:
        from strata.polars_ext import StrataPolarsScanner

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
        """Scan a table and return a Polars DataFrame."""
        import polars as pl

        arrow_table = self.client.scan_to_table(
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
        """Scan a table and return a Polars LazyFrame."""
        return self.scan(
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
        ).lazy()
