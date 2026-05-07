"""End-to-end tests for the SQL cell executor (sqlite, real DB)."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest

# Skip the whole suite if optional ADBC packages are missing — the
# tests need adbc-driver-sqlite to actually open a connection.
adbc_sqlite = pytest.importorskip("adbc_driver_sqlite")


# --- fixtures -------------------------------------------------------------


def _seed_sqlite(path: Path) -> None:
    """Create a SQLite file with one table the cells can query."""
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
        conn.executemany(
            "INSERT INTO events (id, name, value) VALUES (?, ?, ?)",
            [(1, "alpha", 10), (2, "beta", 20), (3, "gamma", 30)],
        )
        conn.commit()


def _build_notebook_with_sql_cell(
    tmp_path: Path,
    *,
    db_path: Path,
    cell_id: str = "c1",
    cell_source: str,
) -> Path:
    """Materialize a notebook directory with one [connections.db] +
    one SQL cell. Returns the notebook directory."""
    from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

    nb_dir = create_notebook(tmp_path, "sql_e2e")
    add_cell_to_notebook(nb_dir, cell_id, language="sql")
    write_cell(nb_dir, cell_id, cell_source)

    # Inject [connections.db]. The current writer rewrites the whole
    # toml on serialize, so the safest path is to read the existing
    # toml as text, append the connection block, and write back.
    toml_path = nb_dir / "notebook.toml"
    text = toml_path.read_text()
    text += f'\n[connections.db]\ndriver = "sqlite"\npath = "{db_path}"\n'
    toml_path.write_text(text)
    return nb_dir


def _make_session(nb_dir: Path) -> Any:
    """Parse the notebook and build a session (DAG + analyzer pass)."""
    from strata.notebook.parser import parse_notebook
    from strata.notebook.session import NotebookSession

    return NotebookSession(parse_notebook(nb_dir), nb_dir)


def _run(coro: Any) -> Any:
    """Run an async coroutine to completion."""
    return asyncio.get_event_loop().run_until_complete(coro)


# --- end-to-end execution -------------------------------------------------


@pytest.mark.asyncio
async def test_sql_cell_executes_and_returns_arrow_table(tmp_path):
    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)

    nb_dir = _build_notebook_with_sql_cell(
        tmp_path,
        db_path=db_path,
        cell_source=(
            "# @sql connection=db\n"
            "# @cache forever\n"
            "SELECT id, name, value FROM events ORDER BY id\n"
        ),
    )
    session = _make_session(nb_dir)

    from strata.notebook.sql.cell_executor import execute_sql_cell

    result = await execute_sql_cell(session, "c1", _read_cell(nb_dir, "c1"))

    assert result["success"], result.get("error")
    assert result["cache_hit"] is False
    assert result["execution_method"] == "sql"
    assert result["artifact_uri"]

    # Verify the stored artifact actually contains the rows.
    table = _load_artifact_as_arrow(session, result["artifact_uri"])
    assert table.num_rows == 3
    assert set(table.schema.names) == {"id", "name", "value"}
    rows = table.to_pylist()
    assert {r["name"] for r in rows} == {"alpha", "beta", "gamma"}


@pytest.mark.asyncio
async def test_sql_cell_cache_hit_on_unchanged_inputs(tmp_path):
    """``# @cache forever`` skips the freshness probe; running twice
    in a row must return the same artifact with cache_hit=True."""
    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)
    nb_dir = _build_notebook_with_sql_cell(
        tmp_path,
        db_path=db_path,
        cell_source=("# @sql connection=db\n# @cache forever\nSELECT * FROM events\n"),
    )
    session = _make_session(nb_dir)

    from strata.notebook.sql.cell_executor import execute_sql_cell

    src = _read_cell(nb_dir, "c1")
    first = await execute_sql_cell(session, "c1", src)
    second = await execute_sql_cell(session, "c1", src)

    assert first["success"] and second["success"]
    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    assert second["execution_method"] == "cached"
    assert first["artifact_uri"] == second["artifact_uri"]


@pytest.mark.asyncio
async def test_sql_cell_fingerprint_invalidates_on_schema_change(tmp_path):
    """Fingerprint policy folds SQLite's ``PRAGMA schema_version``
    into the hash. After an external DDL change, the second run
    must re-execute rather than serve the stale artifact.

    Note: ``PRAGMA data_version`` *also* feeds the freshness token,
    but it's only meaningful when the probe connection stays open
    across the write — a fresh-open / read / close cycle always
    starts at 1 cross-process. SQLite's design treats data_version
    as "this connection's view of the file's write counter," not
    "the file's absolute write counter." The schema_version value
    in contrast does refresh on each fresh open, so DDL changes
    invalidate cleanly. DML invalidation in a multi-process SQLite
    setup is a known limitation; users with that workload should
    pin ``# @cache session`` or accept the coarser granularity."""
    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)
    nb_dir = _build_notebook_with_sql_cell(
        tmp_path,
        db_path=db_path,
        cell_source=(
            "# @sql connection=db\n"
            "# fingerprint is the default; spelling it out for clarity\n"
            "# @cache fingerprint\n"
            "SELECT * FROM events\n"
        ),
    )
    session = _make_session(nb_dir)

    from strata.notebook.sql.cell_executor import execute_sql_cell

    src = _read_cell(nb_dir, "c1")
    first = await execute_sql_cell(session, "c1", src)
    assert first["success"], first.get("error")
    assert first["cache_hit"] is False

    # Mutate the schema from outside Strata.
    with sqlite3.connect(db_path) as conn:
        conn.execute("ALTER TABLE events ADD COLUMN extra TEXT")
        conn.commit()

    second = await execute_sql_cell(session, "c1", src)
    assert second["success"], second.get("error")
    assert second["cache_hit"] is False, "fingerprint should have invalidated after the ALTER TABLE"

    # The new column should appear in the re-executed result's schema.
    table = _load_artifact_as_arrow(session, second["artifact_uri"])
    assert "extra" in table.schema.names


@pytest.mark.asyncio
async def test_sql_cell_read_only_enforces_no_writes(tmp_path):
    """A SQL cell that tries to INSERT must fail and leave the DB
    untouched. The executor opens the connection in enforced
    read-only mode (URI ``mode=ro`` plus ``PRAGMA query_only=ON``);
    that is the security boundary, not SQL-text keyword filtering.

    Note: ADBC's SQLite driver collapses SQLite's "attempt to write
    a readonly database" message into a generic ``InternalError``
    at the Python surface (the descriptive OperationalError fires
    during statement finalization in ``__del__`` and is only
    logged). The cell-level contract Strata pins is "this fails
    and the DB is untouched"; pinning the exact message text would
    couple us to ADBC internals."""
    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)
    nb_dir = _build_notebook_with_sql_cell(
        tmp_path,
        db_path=db_path,
        cell_source=(
            "# @sql connection=db\n"
            "# @cache forever\n"
            "INSERT INTO events (id, name, value) VALUES (99, 'hack', 1)\n"
        ),
    )
    session = _make_session(nb_dir)

    from strata.notebook.sql.cell_executor import execute_sql_cell

    result = await execute_sql_cell(session, "c1", _read_cell(nb_dir, "c1"))
    assert result["success"] is False
    assert result["error"], "expected a non-empty error message"

    # Security boundary: the underlying DB row count is unchanged.
    with sqlite3.connect(db_path) as conn:
        (count,) = conn.execute("SELECT COUNT(*) FROM events").fetchone()
        assert count == 3


@pytest.mark.asyncio
async def test_sql_cell_bind_param_from_upstream_python_cell(tmp_path):
    """Cross-language wiring: a Python cell defines a variable, a
    SQL cell binds against it via ``:name``. This is the integration
    test that exercises every slice 6–9 piece together."""
    from strata.notebook.parser import parse_notebook
    from strata.notebook.session import NotebookSession
    from strata.notebook.writer import (
        add_cell_to_notebook,
        create_notebook,
        write_cell,
    )

    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)
    nb_dir = create_notebook(tmp_path, "cross_lang")
    add_cell_to_notebook(nb_dir, "py", language="python")
    write_cell(nb_dir, "py", "min_value = 15\n")
    add_cell_to_notebook(nb_dir, "sql", after_cell_id="py", language="sql")
    write_cell(
        nb_dir,
        "sql",
        (
            "# @sql connection=db\n"
            "# @cache forever\n"
            "SELECT id, name, value FROM events WHERE value > :min_value ORDER BY id\n"
        ),
    )
    toml_path = nb_dir / "notebook.toml"
    toml_path.write_text(
        toml_path.read_text()
        + "\n[connections.db]\n"
        + 'driver = "sqlite"\n'
        + f'path = "{db_path}"\n'
    )
    session = NotebookSession(parse_notebook(nb_dir), nb_dir)

    # Run the Python cell first so the upstream artifact exists.
    from strata.notebook.executor import CellExecutor

    executor = CellExecutor(session)
    py_src = (nb_dir / "cells" / "py.py").read_text()
    py_result = await executor.execute_cell("py", py_src)
    assert py_result.success, py_result.error

    # Now run the SQL cell. It should resolve :min_value to 15 from
    # the upstream artifact and return rows with value > 15.
    from strata.notebook.sql.cell_executor import execute_sql_cell

    sql_src = (nb_dir / "cells" / "sql.py").read_text()
    result = await execute_sql_cell(session, "sql", sql_src)
    assert result["success"], result.get("error")

    table = _load_artifact_as_arrow(session, result["artifact_uri"])
    rows = table.to_pylist()
    assert len(rows) == 2
    assert {r["name"] for r in rows} == {"beta", "gamma"}


@pytest.mark.asyncio
async def test_sql_cell_missing_connection_yields_clear_error(tmp_path):
    nb_dir = _build_notebook_with_sql_cell(
        tmp_path,
        db_path=tmp_path / "anywhere.db",
        cell_source=("# @sql connection=missing_one\nSELECT 1\n"),
    )
    session = _make_session(nb_dir)

    from strata.notebook.sql.cell_executor import execute_sql_cell

    result = await execute_sql_cell(session, "c1", _read_cell(nb_dir, "c1"))
    assert result["success"] is False
    assert "missing_one" in (result["error"] or "")


@pytest.mark.asyncio
async def test_sql_cell_executor_dispatched_via_main_executor(tmp_path):
    """The ``CellExecutor`` dispatches ``language='sql'`` to the SQL
    path. This is the wiring point that makes the slice 8 helpers
    no longer dead code in production — see the prior Codex review."""
    from strata.notebook.executor import CellExecutor

    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)
    nb_dir = _build_notebook_with_sql_cell(
        tmp_path,
        db_path=db_path,
        cell_source=(
            "# @sql connection=db\n# @cache forever\nSELECT id, name FROM events ORDER BY id\n"
        ),
    )
    session = _make_session(nb_dir)
    executor = CellExecutor(session)

    src = _read_cell(nb_dir, "c1")
    result = await executor.execute_cell("c1", src)
    assert result.success, result.error
    assert result.execution_method == "sql"
    assert result.artifact_uri is not None


# --- helpers --------------------------------------------------------------


def _read_cell(nb_dir: Path, cell_id: str) -> str:
    return (nb_dir / "cells" / f"{cell_id}.py").read_text()


def _load_artifact_as_arrow(session: Any, uri: str) -> Any:
    """Pull the artifact bytes back and decode to a pyarrow Table."""
    import pyarrow as pa

    # ``strata://artifact/<id>@v=<n>``
    body = uri.removeprefix("strata://artifact/")
    art_id, version = body.rsplit("@v=", 1)
    artifact_mgr = session.get_artifact_manager()
    blob = artifact_mgr.load_artifact_data(art_id, int(version))
    return pa.ipc.open_stream(blob).read_all()
