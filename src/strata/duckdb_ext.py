"""DuckDB integration for Strata.

Provides helpers to register Strata scans as DuckDB table functions.
"""

from typing import Any

import duckdb
import pyarrow as pa

from strata.client import StrataClient
from strata.config import StrataConfig
from strata.types import Filter


def register_strata_scan(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    table_uri: str,
    snapshot_id: int | None = None,
    columns: list[str] | None = None,
    filters: list[Filter] | None = None,
    config: StrataConfig | None = None,
    base_url: str | None = None,
) -> None:
    """Register a Strata scan as a DuckDB view.

    This fetches data from a Strata server and registers it as a view
    that DuckDB can query.

    Args:
        conn: DuckDB connection
        name: Name for the registered view
        table_uri: Iceberg table URI
        snapshot_id: Specific snapshot to read
        columns: Columns to project
        filters: Filters for pruning
        config: Strata configuration
        base_url: Override server URL

    Example:
        import duckdb
        from strata import register_strata_scan

        conn = duckdb.connect()
        register_strata_scan(
            conn,
            "my_table",
            "file:///warehouse#db.events",
            columns=["id", "timestamp", "value"],
        )
        result = conn.execute("SELECT * FROM my_table WHERE value > 100").fetchall()
    """
    client = StrataClient(config=config, base_url=base_url)

    try:
        # Fetch data as Arrow Table
        arrow_table = client.scan_to_table(
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
        )

        # Register as a view in DuckDB
        conn.register(name, arrow_table)

    finally:
        client.close()


def strata_query(
    sql: str,
    tables: dict[str, dict[str, Any]],
    config: StrataConfig | None = None,
    base_url: str | None = None,
) -> pa.Table:
    """Execute a SQL query over Strata tables.

    This is a convenience function that registers multiple Strata scans
    and executes a query over them.

    Args:
        sql: SQL query to execute
        tables: Dict mapping view names to scan parameters.
            Each value should be a dict with keys: table_uri, snapshot_id,
            columns, filters (all optional except table_uri)
        config: Strata configuration
        base_url: Override server URL

    Returns:
        Arrow Table with query results

    Example:
        result = strata_query(
            "SELECT e.id, e.value FROM events e WHERE e.value > 100",
            tables={
                "events": {
                    "table_uri": "file:///warehouse#db.events",
                    "columns": ["id", "value"],
                }
            }
        )
    """
    conn = duckdb.connect()

    try:
        for name, params in tables.items():
            register_strata_scan(
                conn=conn,
                name=name,
                table_uri=params["table_uri"],
                snapshot_id=params.get("snapshot_id"),
                columns=params.get("columns"),
                filters=params.get("filters"),
                config=config,
                base_url=base_url,
            )

        result = conn.execute(sql).fetch_arrow_table()
        return result

    finally:
        conn.close()


class StrataScanner:
    """A reusable scanner for DuckDB integration.

    Maintains a connection to the Strata server and allows
    incremental registration of tables.

    Example:
        scanner = StrataScanner()
        scanner.register("events", "file:///warehouse#db.events")
        scanner.register("users", "file:///warehouse#db.users")

        result = scanner.query('''
            SELECT e.*, u.name
            FROM events e
            JOIN users u ON e.user_id = u.id
        ''')
    """

    def __init__(
        self,
        config: StrataConfig | None = None,
        base_url: str | None = None,
    ) -> None:
        self.config = config
        self.base_url = base_url
        self.conn = duckdb.connect()
        self._registered: set[str] = set()

    def __enter__(self) -> "StrataScanner":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def close(self) -> None:
        """Close resources."""
        self.conn.close()

    def register(
        self,
        name: str,
        table_uri: str,
        snapshot_id: int | None = None,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
    ) -> "StrataScanner":
        """Register a Strata table for querying.

        Returns self for method chaining.
        """
        register_strata_scan(
            conn=self.conn,
            name=name,
            table_uri=table_uri,
            snapshot_id=snapshot_id,
            columns=columns,
            filters=filters,
            config=self.config,
            base_url=self.base_url,
        )
        self._registered.add(name)
        return self

    def query(self, sql: str) -> pa.Table:
        """Execute a SQL query and return Arrow Table."""
        return self.conn.execute(sql).fetch_arrow_table()

    def query_df(self, sql: str):
        """Execute a SQL query and return a pandas DataFrame."""
        return self.conn.execute(sql).fetchdf()
