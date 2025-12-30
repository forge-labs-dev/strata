"""Iceberg snapshot resolution using pyiceberg."""

from pathlib import Path
from typing import Protocol

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table

from strata.config import StrataConfig


class CatalogProvider(Protocol):
    """Protocol for catalog providers to allow future extensibility."""

    def load_table(self, table_uri: str) -> Table: ...

    def get_snapshot_id(self, table: Table, snapshot_id: int | None) -> int: ...


class PyIcebergCatalog:
    """Default catalog provider using pyiceberg."""

    def __init__(self, config: StrataConfig) -> None:
        self.config = config
        self._catalogs: dict[str, Catalog] = {}

    def _get_default_catalog_uri(self, warehouse_path: str | None = None) -> str:
        """Get the default catalog URI based on warehouse path.

        If catalog_properties has a URI configured (e.g., PostgreSQL), use that.
        Otherwise fall back to SQLite based on warehouse path.
        """
        # Use configured URI if provided (supports PostgreSQL, MySQL, etc.)
        if "uri" in self.config.catalog_properties:
            return self.config.catalog_properties["uri"]

        # Fall back to SQLite based on warehouse path
        if warehouse_path and warehouse_path.startswith("s3://"):
            return f"sqlite:///{self.config.metadata_db}"
        elif warehouse_path:
            return f"sqlite:///{Path(warehouse_path) / 'catalog.db'}"
        else:
            return "sqlite:///:memory:"

    def _get_catalog(self, warehouse_path: str | None = None) -> Catalog:
        """Get or create a catalog instance.

        The catalog backend is determined by catalog_properties.uri:
        - If set (e.g., postgresql://...), uses that for all warehouses
        - Otherwise, falls back to SQLite (per-warehouse or in-memory)
        """
        # For S3 warehouses
        if warehouse_path and warehouse_path.startswith("s3://"):
            cache_key = warehouse_path
            if cache_key not in self._catalogs:
                # S3 catalog properties
                s3_props = {}
                if self.config.s3_region:
                    s3_props["s3.region"] = self.config.s3_region
                if self.config.s3_access_key:
                    s3_props["s3.access-key-id"] = self.config.s3_access_key
                if self.config.s3_secret_key:
                    s3_props["s3.secret-access-key"] = self.config.s3_secret_key
                if self.config.s3_endpoint_url:
                    s3_props["s3.endpoint"] = self.config.s3_endpoint_url

                catalog_uri = self._get_default_catalog_uri(warehouse_path)
                self._catalogs[cache_key] = SqlCatalog(
                    "strata",
                    **{
                        "uri": catalog_uri,
                        "warehouse": warehouse_path,
                        **s3_props,
                        **self.config.catalog_properties,
                    },
                )
            return self._catalogs[cache_key]

        # For local filesystem tables
        if warehouse_path:
            cache_key = warehouse_path
            if cache_key not in self._catalogs:
                catalog_uri = self._get_default_catalog_uri(warehouse_path)
                self._catalogs[cache_key] = SqlCatalog(
                    "strata",
                    **{
                        "uri": catalog_uri,
                        "warehouse": warehouse_path,
                        **self.config.catalog_properties,
                    },
                )
            return self._catalogs[cache_key]

        # Use configured catalog properties (default catalog)
        cache_key = "default"
        if cache_key not in self._catalogs:
            if self.config.catalog_properties:
                self._catalogs[cache_key] = load_catalog(
                    self.config.catalog_name, **self.config.catalog_properties
                )
            else:
                # Fallback to in-memory SQL catalog
                self._catalogs[cache_key] = SqlCatalog(
                    self.config.catalog_name,
                    **{
                        "uri": "sqlite:///:memory:",
                        "warehouse": str(self.config.cache_dir / "warehouse"),
                    },
                )
        return self._catalogs[cache_key]

    @staticmethod
    def parse_table_uri(table_uri: str) -> tuple[str | None, str]:
        """Parse a table URI into (warehouse_path, table_id).

        Args:
            table_uri: Table identifier in one of these formats:
                - file:///path/to/warehouse#namespace.table
                - /path/to/warehouse#namespace.table
                - s3://bucket/path/to/warehouse#namespace.table
                - namespace.table (uses default catalog)

        Returns:
            Tuple of (warehouse_path or None, table_id)
        """
        if "#" in table_uri:
            path_part, table_id = table_uri.rsplit("#", 1)
            # Preserve s3:// prefix, strip file:// prefix
            if path_part.startswith("s3://"):
                warehouse_path = path_part  # Keep s3:// prefix
            else:
                warehouse_path = path_part.replace("file://", "")
            return warehouse_path, table_id
        else:
            return None, table_uri

    def load_table(self, table_uri: str) -> Table:
        """Load an Iceberg table from URI.

        Supports:
        - file:///path/to/warehouse#namespace.table
        - /path/to/warehouse#namespace.table
        - namespace.table (uses default catalog)
        """
        warehouse_path, table_id = self.parse_table_uri(table_uri)
        catalog = self._get_catalog(warehouse_path)
        return catalog.load_table(table_id)

    def get_snapshot_id(self, table: Table, snapshot_id: int | None) -> int:
        """Get the snapshot ID to use (current if None)."""
        if snapshot_id is not None:
            # Verify the snapshot exists
            snapshot = table.snapshot_by_id(snapshot_id)
            if snapshot is None:
                raise ValueError(f"Snapshot {snapshot_id} not found in table")
            return snapshot_id

        current = table.current_snapshot()
        if current is None:
            raise ValueError("Table has no snapshots")
        return current.snapshot_id

    def create_table_if_not_exists(
        self,
        warehouse_path: str,
        namespace: str,
        table_name: str,
        schema,
    ) -> Table:
        """Create a table if it doesn't exist (for demos)."""
        catalog = self._get_catalog(warehouse_path)

        # Create namespace if needed
        try:
            catalog.create_namespace(namespace)
        except Exception:
            pass  # Namespace might already exist

        table_id = f"{namespace}.{table_name}"
        try:
            return catalog.load_table(table_id)
        except Exception:
            return catalog.create_table(table_id, schema)
