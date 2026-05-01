"""Tests for the markdown cell language.

Markdown cells are pure prose: no Python execution, no DAG edges, no
provenance chain. These tests pin the contract so a future regression
can't accidentally drag them through the executor or analyzer pipelines.
"""

from __future__ import annotations

import asyncio
import time

from strata.notebook.executor import CellExecutor
from strata.notebook.parser import parse_notebook
from strata.notebook.session import NotebookSession
from strata.notebook.writer import (
    add_cell_to_notebook,
    create_notebook,
    write_cell,
)


class TestMarkdownCellPersistence:
    def test_add_markdown_cell_creates_md_file(self, tmp_path):
        """A markdown cell should land in a ``.md`` file, not ``.py``."""
        nb_dir = create_notebook(tmp_path, "md_persist")
        add_cell_to_notebook(nb_dir, "abc12345", language="markdown")

        cells_dir = nb_dir / "cells"
        assert (cells_dir / "abc12345.md").exists()
        assert not (cells_dir / "abc12345.py").exists()

    def test_python_cell_still_uses_py_file(self, tmp_path):
        """Don't accidentally regress the existing Python cell file naming."""
        nb_dir = create_notebook(tmp_path, "py_persist")
        add_cell_to_notebook(nb_dir, "py_cell1", language="python")
        assert (nb_dir / "cells" / "py_cell1.py").exists()

    def test_parse_round_trip_preserves_markdown_source(self, tmp_path):
        """Source written to a .md file round-trips through parse."""
        nb_dir = create_notebook(tmp_path, "md_roundtrip")
        add_cell_to_notebook(nb_dir, "doc01234", language="markdown")
        write_cell(nb_dir, "doc01234", "# Hello\n\nMarkdown body.")

        state = parse_notebook(nb_dir)
        cell = next(c for c in state.cells if c.id == "doc01234")
        assert cell.language == "markdown"
        assert cell.source == "# Hello\n\nMarkdown body."


class TestMarkdownCellAnalysis:
    def test_markdown_has_no_defines_or_references(self, tmp_path):
        """Markdown cells must not produce DAG edges — they're prose."""
        nb_dir = create_notebook(tmp_path, "md_dag")
        # Source that *would* parse as Python with defines+references —
        # if the analyzer mistakenly treats it as Python it would record
        # ``x`` as a define.
        add_cell_to_notebook(nb_dir, "p_cell", language="python")
        write_cell(nb_dir, "p_cell", "x = 1")

        add_cell_to_notebook(nb_dir, "md_cell", language="markdown")
        write_cell(nb_dir, "md_cell", "# Doc\n\nx = 1 here is just prose.")

        session = NotebookSession(parse_notebook(nb_dir), nb_dir)
        md_cell = next(c for c in session.notebook_state.cells if c.id == "md_cell")
        assert md_cell.defines == []
        assert md_cell.references == []
        # Also: the python cell's ``x`` should still be visible in the DAG
        # producer map — the markdown cell shouldn't shadow it.
        assert session.dag is not None
        assert session.dag.variable_producer.get("x") == "p_cell"


class TestMarkdownCellExecution:
    def test_executor_short_circuits_with_no_output(self, tmp_path):
        """``execute_cell`` returns success with no display outputs.

        The frontend already renders the markdown source in-place via the
        cell's preview view; emitting it as a display output would
        duplicate the same content in the output panel below.
        """
        nb_dir = create_notebook(tmp_path, "md_exec")
        add_cell_to_notebook(nb_dir, "md_cell", language="markdown")
        body = "# Hi\n\nThis is **bold**."
        write_cell(nb_dir, "md_cell", body)

        session = NotebookSession(parse_notebook(nb_dir), nb_dir)
        executor = CellExecutor(session, session.warm_pool)

        start = time.monotonic()
        result = asyncio.run(executor.execute_cell("md_cell", body))
        elapsed = time.monotonic() - start

        assert result.success is True
        # The fast path should be much faster than spawning a uv subprocess.
        # 500ms is generous; in practice this returns in single-digit ms.
        assert elapsed < 0.5
        assert result.display_outputs == []
        assert result.display_output is None
        assert result.cache_hit is True


class TestMarkdownCellStaleness:
    def test_markdown_cells_are_always_ready(self, tmp_path):
        """Markdown cells should never appear stale — they have no inputs."""
        nb_dir = create_notebook(tmp_path, "md_stale")
        add_cell_to_notebook(nb_dir, "md_cell", language="markdown")
        write_cell(nb_dir, "md_cell", "# Doc")

        session = NotebookSession(parse_notebook(nb_dir), nb_dir)
        staleness = session.compute_staleness()
        from strata.notebook.models import CellStatus

        assert staleness["md_cell"].status == CellStatus.READY
