"""Tests for the cell executor."""

from __future__ import annotations

import pytest

from strata.notebook.executor import CellExecutor
from strata.notebook.parser import parse_notebook
from strata.notebook.session import NotebookSession


@pytest.fixture
def sample_notebook(tmp_path):
    """Create a sample notebook for testing.

    Returns:
        NotebookSession for the test notebook
    """
    from strata.notebook.writer import add_cell_to_notebook, create_notebook

    # Create notebook
    notebook_dir = create_notebook(tmp_path, "Test Notebook")

    # Add a couple of cells
    add_cell_to_notebook(notebook_dir, "cell1", None)
    add_cell_to_notebook(notebook_dir, "cell2", "cell1")

    # Parse and create session
    notebook_state = parse_notebook(notebook_dir)
    session = NotebookSession(notebook_state, notebook_dir)

    return session


class TestCellExecutor:
    """Test basic cell execution."""

    @pytest.mark.asyncio
    async def test_execute_simple_assignment(self, sample_notebook):
        """Test executing a simple assignment."""
        executor = CellExecutor(sample_notebook)

        source = "x = 1 + 1"
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert result.cell_id == "cell1"
        assert result.error is None
        assert "x" in result.outputs
        assert result.outputs["x"]["content_type"] == "json/object"
        assert result.outputs["x"]["preview"] == 2

    @pytest.mark.asyncio
    async def test_execute_with_print(self, sample_notebook):
        """Test that print output is captured."""
        executor = CellExecutor(sample_notebook)

        source = 'print("Hello, world!")\ny = 42'
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "Hello, world!" in result.stdout
        assert "y" in result.outputs

    @pytest.mark.asyncio
    async def test_execute_with_error(self, sample_notebook):
        """Test executing a cell that raises an error."""
        executor = CellExecutor(sample_notebook)

        source = "z = 1 / 0"
        result = await executor.execute_cell("cell1", source)

        assert result.success is False
        assert result.error is not None
        assert "ZeroDivisionError" in result.error or "division" in result.error

    @pytest.mark.asyncio
    async def test_execute_dataframe(self, sample_notebook):
        """Test executing a cell that creates a dictionary (simulates DataFrame-like output)."""
        executor = CellExecutor(sample_notebook)

        # Use a dict instead of DataFrame since pandas may not be available in test venv
        source = 'df = {"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}'
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "df" in result.outputs
        assert result.outputs["df"]["content_type"] == "json/object"
        assert result.outputs["df"]["preview"] == {"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]}

    @pytest.mark.asyncio
    async def test_execute_multiple_outputs(self, sample_notebook):
        """Test executing a cell that defines multiple variables."""
        executor = CellExecutor(sample_notebook)

        source = """
x = 10
y = "hello"
z = [1, 2, 3]
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert set(result.outputs.keys()) == {"x", "y", "z"}
        assert result.outputs["x"]["preview"] == 10
        assert result.outputs["y"]["preview"] == "hello"
        assert result.outputs["z"]["preview"] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_execute_dict_output(self, sample_notebook):
        """Test executing a cell that creates a dict."""
        executor = CellExecutor(sample_notebook)

        source = 'data = {"count": 42, "names": ["Alice", "Bob"]}'
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "data" in result.outputs
        assert result.outputs["data"]["content_type"] == "json/object"

    @pytest.mark.asyncio
    async def test_execute_ignores_private_vars(self, sample_notebook):
        """Test that private variables (_name) are not included in outputs."""
        executor = CellExecutor(sample_notebook)

        source = """
public = 1
_private = 2
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "public" in result.outputs
        assert "_private" not in result.outputs

    @pytest.mark.asyncio
    async def test_execute_empty_cell(self, sample_notebook):
        """Test executing a cell with no outputs."""
        executor = CellExecutor(sample_notebook)

        source = "# Just a comment"
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert len(result.outputs) == 0

    @pytest.mark.asyncio
    async def test_execute_with_import(self, sample_notebook):
        """Test executing a cell with imports."""
        executor = CellExecutor(sample_notebook)

        source = """
import math
result = math.pi
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "result" in result.outputs
        # Pi should be serialized as JSON number
        assert abs(result.outputs["result"]["preview"] - 3.14159) < 0.01

    @pytest.mark.asyncio
    async def test_execute_with_stderr(self, sample_notebook):
        """Test that stderr is captured."""
        executor = CellExecutor(sample_notebook)

        source = """
import sys
print("error message", file=sys.stderr)
x = 1
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert "error message" in result.stderr

    @pytest.mark.asyncio
    async def test_execution_duration(self, sample_notebook):
        """Test that execution duration is measured."""
        executor = CellExecutor(sample_notebook)

        source = "x = 42"
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        assert result.duration_ms > 0

    @pytest.mark.asyncio
    async def test_execute_function_definition(self, sample_notebook):
        """Test executing a cell that defines a function."""
        executor = CellExecutor(sample_notebook)

        source = """
def add(a, b):
    return a + b

result = add(2, 3)
"""
        result = await executor.execute_cell("cell1", source)

        assert result.success is True
        # Function definition is not captured, but result is
        assert "add" in result.outputs
        assert "result" in result.outputs
        assert result.outputs["result"]["preview"] == 5
