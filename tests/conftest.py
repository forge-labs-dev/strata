"""Shared pytest fixtures for Strata tests."""

import socket
import threading
import time
from datetime import UTC, datetime

import pyarrow as pa
import pytest
import uvicorn
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
)

from strata.client import StrataClient
from strata.config import StrataConfig


@pytest.fixture
def temp_warehouse(tmp_path):
    """Create a temporary warehouse with a sample Iceberg table."""
    warehouse_path = tmp_path / "warehouse"
    warehouse_path.mkdir()

    # Create a SQL catalog - use "strata" to match PyIcebergCatalog
    catalog = SqlCatalog(
        "strata",
        **{
            "uri": f"sqlite:///{warehouse_path / 'catalog.db'}",
            "warehouse": str(warehouse_path),
        },
    )

    # Create namespace
    catalog.create_namespace("test_db")

    # Define schema - use optional fields to match PyArrow defaults
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "value", DoubleType(), required=False),
        NestedField(3, "name", StringType(), required=False),
        NestedField(4, "timestamp", LongType(), required=False),  # Epoch micros
    )

    # Create table
    table = catalog.create_table("test_db.events", schema)

    # Create sample data with multiple row groups
    num_rows = 500
    base_ts = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp() * 1_000_000)
    data = pa.table(
        {
            "id": pa.array(range(num_rows), type=pa.int64()),
            "value": pa.array([float(i * 1.5) for i in range(num_rows)], type=pa.float64()),
            "name": pa.array([f"item_{i}" for i in range(num_rows)], type=pa.string()),
            "timestamp": pa.array(
                [base_ts + i * 3600_000_000 for i in range(num_rows)],  # micros
                type=pa.int64(),
            ),
        }
    )

    # Append data to table
    table.append(data)

    return {
        "warehouse_path": warehouse_path,
        "table_uri": f"file://{warehouse_path}#test_db.events",
        "catalog": catalog,
        "table": table,
    }


@pytest.fixture
def strata_config(tmp_path):
    """Create a test configuration."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    return StrataConfig(cache_dir=cache_dir)


@pytest.fixture
def server_with_client(temp_warehouse, tmp_path):
    """Start a server and provide a client.

    Yields a dict with:
        - client: StrataClient connected to the running server
        - config: StrataConfig used by the server
        - warehouse: temp_warehouse dict with table_uri, catalog, etc.
    """
    import strata.server as server_module
    from strata.server import ServerState, app

    # Find a free port
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    config = StrataConfig(
        host="127.0.0.1",
        port=port,
        cache_dir=tmp_path / "cache",
    )

    # Initialize state manually for testing
    server_module._state = ServerState(config)

    server_thread = threading.Thread(
        target=uvicorn.run,
        kwargs={
            "app": app,
            "host": config.host,
            "port": config.port,
            "log_level": "error",
        },
        daemon=True,
    )
    server_thread.start()

    # Wait for server to start
    time.sleep(1)

    client = StrataClient(base_url=f"http://127.0.0.1:{port}")

    yield {
        "client": client,
        "config": config,
        "warehouse": temp_warehouse,
    }

    client.close()
