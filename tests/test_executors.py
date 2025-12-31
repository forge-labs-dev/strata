"""Unit tests for local executors.

These tests verify client-side execution without requiring a server.
"""

import pyarrow as pa
import pytest

from strata.executors import _run_duckdb_sql, run_local


class TestRunLocal:
    """Tests for the run_local dispatcher."""

    def test_duckdb_executor_dispatch(self):
        """DuckDB executor is correctly dispatched."""
        build_spec = {
            "executor": "local://duckdb_sql@v1",
            "params": {"sql": "SELECT 1 as x"},
            "input_uris": [],
        }
        result = run_local(build_spec, {})
        assert result.num_rows == 1
        assert result.column_names == ["x"]

    def test_duckdb_executor_without_version(self):
        """DuckDB executor works without version suffix."""
        build_spec = {
            "executor": "local://duckdb_sql",
            "params": {"sql": "SELECT 42 as answer"},
            "input_uris": [],
        }
        result = run_local(build_spec, {})
        assert result.to_pydict() == {"answer": [42]}

    def test_unsupported_executor_raises(self):
        """Unknown executor raises ValueError."""
        build_spec = {
            "executor": "local://unknown_executor@v1",
            "params": {},
            "input_uris": [],
        }
        with pytest.raises(ValueError) as exc_info:
            run_local(build_spec, {})
        assert "Unsupported executor" in str(exc_info.value)
        assert "unknown_executor" in str(exc_info.value)

    def test_empty_executor_raises(self):
        """Empty executor raises ValueError."""
        build_spec = {
            "executor": "",
            "params": {},
            "input_uris": [],
        }
        with pytest.raises(ValueError) as exc_info:
            run_local(build_spec, {})
        assert "Unsupported executor" in str(exc_info.value)


class TestDuckDBExecutor:
    """Tests for DuckDB SQL executor."""

    def test_simple_query(self):
        """Execute a simple query without inputs."""
        build_spec = {
            "executor": "local://duckdb_sql@v1",
            "params": {"sql": "SELECT 1 as a, 'hello' as b"},
            "input_uris": [],
        }
        result = _run_duckdb_sql(build_spec, {})
        assert result.num_rows == 1
        assert result.to_pydict() == {"a": [1], "b": ["hello"]}

    def test_single_input_table(self):
        """Execute query with one input table."""
        input_table = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})
        build_spec = {
            "executor": "local://duckdb_sql@v1",
            "params": {"sql": "SELECT sum(value) as total FROM input0"},
            "input_uris": ["file:///warehouse#db.events"],
        }
        result = _run_duckdb_sql(build_spec, {"file:///warehouse#db.events": input_table})
        assert result.to_pydict() == {"total": [60]}

    def test_multiple_input_tables(self):
        """Execute query joining multiple input tables."""
        users = pa.table({"user_id": [1, 2], "name": ["Alice", "Bob"]})
        orders = pa.table({"user_id": [1, 1, 2], "amount": [100, 200, 150]})

        build_spec = {
            "executor": "local://duckdb_sql@v1",
            "params": {
                "sql": """
                SELECT u.name, sum(o.amount) as total
                FROM input0 u
                JOIN input1 o ON u.user_id = o.user_id
                GROUP BY u.name
                ORDER BY u.name
                """
            },
            "input_uris": ["strata://artifact/users@v=1", "strata://artifact/orders@v=1"],
        }
        result = _run_duckdb_sql(
            build_spec,
            {
                "strata://artifact/users@v=1": users,
                "strata://artifact/orders@v=1": orders,
            },
        )
        assert result.to_pydict() == {"name": ["Alice", "Bob"], "total": [300, 150]}

    def test_missing_sql_raises(self):
        """Missing SQL parameter raises ValueError."""
        build_spec = {
            "executor": "local://duckdb_sql@v1",
            "params": {},
            "input_uris": [],
        }
        with pytest.raises(ValueError) as exc_info:
            _run_duckdb_sql(build_spec, {})
        assert "requires 'sql' in params" in str(exc_info.value)

    def test_empty_sql_raises(self):
        """Empty SQL parameter raises ValueError."""
        build_spec = {
            "executor": "local://duckdb_sql@v1",
            "params": {"sql": ""},
            "input_uris": [],
        }
        with pytest.raises(ValueError) as exc_info:
            _run_duckdb_sql(build_spec, {})
        assert "requires 'sql' in params" in str(exc_info.value)

    def test_missing_input_table_raises(self):
        """Missing input table raises ValueError."""
        build_spec = {
            "executor": "local://duckdb_sql@v1",
            "params": {"sql": "SELECT * FROM input0"},
            "input_uris": ["file:///warehouse#db.events"],
        }
        with pytest.raises(ValueError) as exc_info:
            _run_duckdb_sql(build_spec, {})
        assert "Missing input table" in str(exc_info.value)
        assert "file:///warehouse#db.events" in str(exc_info.value)

    def test_filter_and_aggregate(self):
        """Test filtering and aggregation."""
        events = pa.table(
            {
                "event_type": ["click", "view", "click", "purchase", "view"],
                "count": [1, 2, 3, 4, 5],
            }
        )
        build_spec = {
            "executor": "local://duckdb_sql@v1",
            "params": {
                "sql": """
                SELECT event_type, sum(count) as total
                FROM input0
                WHERE event_type != 'purchase'
                GROUP BY event_type
                ORDER BY event_type
                """
            },
            "input_uris": ["events"],
        }
        result = _run_duckdb_sql(build_spec, {"events": events})
        assert result.to_pydict() == {
            "event_type": ["click", "view"],
            "total": [4, 7],
        }

    def test_preserves_arrow_types(self):
        """DuckDB preserves Arrow types correctly."""
        input_table = pa.table(
            {
                "int_col": pa.array([1, 2, 3], type=pa.int64()),
                "float_col": pa.array([1.5, 2.5, 3.5], type=pa.float64()),
                "str_col": pa.array(["a", "b", "c"], type=pa.string()),
            }
        )
        build_spec = {
            "executor": "local://duckdb_sql@v1",
            "params": {"sql": "SELECT * FROM input0"},
            "input_uris": ["input"],
        }
        result = _run_duckdb_sql(build_spec, {"input": input_table})
        assert result.num_rows == 3
        assert "int_col" in result.column_names
        assert "float_col" in result.column_names
        assert "str_col" in result.column_names
