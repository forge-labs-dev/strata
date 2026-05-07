"""End-to-end scenarios for SQL cells (slice 10).

Slice 6–9 covered the units (bind coercion, hash resolution, cache
policy, executor wiring) and a few headline e2e cases (basic execute,
cache hit, schema-change invalidation, read-only enforcement, cross-
language Python→SQL bind). This file pins the remaining design-doc
scenarios that benefit from real DB execution:

- **Injection rejection at the SQL boundary.** The bind unit test
  proves we accept adversarial strings unchanged; this e2e test
  proves the database is *not* mutated by a string that would
  otherwise be a DROP TABLE.
- **Cache identity tracks upstream values.** Same SQL, different
  bind value → re-execution; same bind value → cache hit.
- **Snapshot policy fails fast on non-snapshot drivers.** SQLite
  can't expose a durable snapshot ID; ``# @cache snapshot`` must
  surface a clear error before the executor opens the connection.
- **NULL binds.** ``None`` upstream → SQL NULL bind → expected
  IS NULL semantics.
- **Empty result set.** A query returning 0 rows still produces a
  valid ``arrow/ipc`` artifact with the right schema.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

# Skip the suite if optional ADBC packages are missing.
adbc_sqlite = pytest.importorskip("adbc_driver_sqlite")


# --- shared fixtures ------------------------------------------------------


def _seed_sqlite(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
        conn.executemany(
            "INSERT INTO events (id, name, value) VALUES (?, ?, ?)",
            [(1, "alpha", 10), (2, "beta", 20), (3, "gamma", 30)],
        )
        conn.commit()


def _build_notebook(
    tmp_path: Path,
    *,
    db_path: Path,
    cells: list[tuple[str, str, str]],
) -> Path:
    """Create a notebook with the given cells (id, language, source).

    Connections appended manually so we don't depend on the writer's
    [connections.<name>] serialization shape.
    """
    from strata.notebook.writer import (
        add_cell_to_notebook,
        create_notebook,
        write_cell,
    )

    nb_dir = create_notebook(tmp_path, "sql_e2e_scenarios")
    after: str | None = None
    for cell_id, language, source in cells:
        add_cell_to_notebook(nb_dir, cell_id, after_cell_id=after, language=language)
        write_cell(nb_dir, cell_id, source)
        after = cell_id

    toml = nb_dir / "notebook.toml"
    toml.write_text(
        toml.read_text() + "\n[connections.db]\n" + 'driver = "sqlite"\n' + f'path = "{db_path}"\n'
    )
    return nb_dir


def _session(nb_dir: Path) -> Any:
    from strata.notebook.parser import parse_notebook
    from strata.notebook.session import NotebookSession

    return NotebookSession(parse_notebook(nb_dir), nb_dir)


def _read(nb_dir: Path, cell_id: str) -> str:
    return (nb_dir / "cells" / f"{cell_id}.py").read_text()


def _load_arrow(session: Any, uri: str) -> Any:
    import pyarrow as pa

    body = uri.removeprefix("strata://artifact/")
    art_id, version = body.rsplit("@v=", 1)
    blob = session.get_artifact_manager().load_artifact_data(art_id, int(version))
    return pa.ipc.open_stream(blob).read_all()


# --- 1. Injection rejection (e2e) ----------------------------------------


@pytest.mark.asyncio
async def test_sql_injection_via_bind_does_not_alter_database(tmp_path):
    """The bind unit test pins "the adversarial string is accepted
    as data." This test pins the actual security property: after
    feeding an injection-shaped string through a bind parameter,
    the underlying database is byte-identical. ADBC's
    parameter-binding API is the security boundary, not any text
    filter."""
    from strata.notebook.executor import CellExecutor

    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)
    nb_dir = _build_notebook(
        tmp_path,
        db_path=db_path,
        cells=[
            ("py", "python", 'needle = "\'; DROP TABLE events; --"\n'),
            (
                "sql",
                "sql",
                "# @sql connection=db\n"
                "# @cache forever\n"
                "SELECT id FROM events WHERE name = :needle\n",
            ),
        ],
    )
    session = _session(nb_dir)
    executor = CellExecutor(session)

    py_result = await executor.execute_cell("py", _read(nb_dir, "py"))
    assert py_result.success, py_result.error

    sql_result = await executor.execute_cell("sql", _read(nb_dir, "sql"))
    assert sql_result.success, sql_result.error

    # No row matches the literal needle → empty result is fine.
    table = _load_arrow(session, sql_result.artifact_uri)
    assert table.num_rows == 0

    # Critical: the events table is intact.
    with sqlite3.connect(db_path) as conn:
        (count,) = conn.execute("SELECT COUNT(*) FROM events").fetchone()
        assert count == 3
        names = sorted(r[0] for r in conn.execute("SELECT name FROM events"))
        assert names == ["alpha", "beta", "gamma"]


# --- 2. Cache identity tracks upstream values ----------------------------


@pytest.mark.asyncio
async def test_sql_cache_invalidates_on_upstream_bind_value_change(tmp_path):
    """upstream_input_hashes feeds the SQL provenance hash. Same SQL,
    different upstream value → cache must miss. Without the upstream
    hash in provenance, two cells with different bind values could
    silently share a cached artifact — wrong rows."""
    from strata.notebook.executor import CellExecutor

    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)
    nb_dir = _build_notebook(
        tmp_path,
        db_path=db_path,
        cells=[
            ("py", "python", "min_value = 15\n"),
            (
                "sql",
                "sql",
                "# @sql connection=db\n"
                "# @cache forever\n"
                "SELECT id, name FROM events WHERE value > :min_value ORDER BY id\n",
            ),
        ],
    )
    session = _session(nb_dir)
    executor = CellExecutor(session)

    # First: min_value=15 → 2 rows.
    await executor.execute_cell("py", _read(nb_dir, "py"))
    first = await executor.execute_cell("sql", _read(nb_dir, "sql"))
    assert first.success
    assert first.cache_hit is False
    first_table = _load_arrow(session, first.artifact_uri)
    assert first_table.num_rows == 2

    # Edit the upstream cell so min_value=25 — expect re-execution
    # and 1 row. Mutate both the on-disk file and the in-memory
    # cell.source field so re_analyze_cell + execute see the new
    # source (the route normally does this through
    # update_cell_source, but tests don't have that surface).
    py_cell = next(c for c in session.notebook_state.cells if c.id == "py")
    new_py_src = "min_value = 25\n"
    (nb_dir / "cells" / "py.py").write_text(new_py_src)
    py_cell.source = new_py_src
    session.re_analyze_cell("py")
    await executor.execute_cell("py", new_py_src)

    second = await executor.execute_cell("sql", _read(nb_dir, "sql"))
    assert second.success
    assert second.cache_hit is False, (
        "SQL cell must re-execute when an upstream bind variable changes"
    )
    second_table = _load_arrow(session, second.artifact_uri)
    assert second_table.num_rows == 1

    # Third run with the same min_value=25 → cache hit on the SQL cell.
    third = await executor.execute_cell("sql", _read(nb_dir, "sql"))
    assert third.success
    assert third.cache_hit is True


# --- 3. Snapshot policy on non-snapshot driver ---------------------------


@pytest.mark.asyncio
async def test_sql_snapshot_policy_errors_on_sqlite(tmp_path):
    """SQLite has no durable snapshot identity (capabilities.
    supports_snapshot=False). ``# @cache snapshot`` must fail at
    resolve_cache_policy before the executor opens the connection,
    surfacing a clear diagnostic."""
    from strata.notebook.sql.cell_executor import execute_sql_cell

    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)
    nb_dir = _build_notebook(
        tmp_path,
        db_path=db_path,
        cells=[
            (
                "c1",
                "sql",
                "# @sql connection=db\n# @cache snapshot\nSELECT * FROM events\n",
            )
        ],
    )
    session = _session(nb_dir)

    result = await execute_sql_cell(session, "c1", _read(nb_dir, "c1"))
    assert result["success"] is False
    err = (result.get("error") or "").lower()
    assert "snapshot" in err, f"unexpected error: {result['error']!r}"


# --- 4. NULL bind values --------------------------------------------------


@pytest.mark.asyncio
async def test_sql_null_bind_param_via_none_upstream(tmp_path):
    """A ``None`` upstream value binds as SQL NULL. SQL's three-
    valued logic means ``= NULL`` is never true, so the query must
    use ``IS NULL`` to match the existing NULL row. This test pins
    that None flows through the bind layer correctly."""
    from strata.notebook.executor import CellExecutor

    db_path = tmp_path / "events.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
        conn.executemany(
            "INSERT INTO events (id, name, value) VALUES (?, ?, ?)",
            [(1, "alpha", None), (2, "beta", 20)],
        )
        conn.commit()

    nb_dir = _build_notebook(
        tmp_path,
        db_path=db_path,
        cells=[
            ("py", "python", "sentinel = None\n"),
            # ``:sentinel`` is bound but unused for matching — the
            # query selects rows with NULL value. The point is that
            # binding ``None`` doesn't crash the executor.
            (
                "sql",
                "sql",
                "# @sql connection=db\n"
                "# @cache forever\n"
                "SELECT id FROM events WHERE value IS NULL OR value = :sentinel\n",
            ),
        ],
    )
    session = _session(nb_dir)
    executor = CellExecutor(session)

    await executor.execute_cell("py", _read(nb_dir, "py"))
    result = await executor.execute_cell("sql", _read(nb_dir, "sql"))
    assert result.success, result.error
    table = _load_arrow(session, result.artifact_uri)
    rows = table.to_pylist()
    assert rows == [{"id": 1}]


# --- 5. Empty result set --------------------------------------------------


@pytest.mark.asyncio
async def test_sql_empty_result_set_produces_valid_artifact(tmp_path):
    """A SQL query returning zero rows must still produce a valid
    ``arrow/ipc`` artifact with the correct schema. Edge case: a
    naive Arrow IPC writer can produce a stream that fails to
    decode if no batches are written."""
    from strata.notebook.executor import CellExecutor

    db_path = tmp_path / "events.db"
    _seed_sqlite(db_path)
    nb_dir = _build_notebook(
        tmp_path,
        db_path=db_path,
        cells=[
            (
                "c1",
                "sql",
                "# @sql connection=db\n"
                "# @cache forever\n"
                "SELECT id, name FROM events WHERE value < 0\n",
            )
        ],
    )
    session = _session(nb_dir)
    executor = CellExecutor(session)

    result = await executor.execute_cell("c1", _read(nb_dir, "c1"))
    assert result.success, result.error
    table = _load_arrow(session, result.artifact_uri)
    assert table.num_rows == 0
    # Schema preserved even with no rows — downstream cells can
    # still inspect column names / types.
    assert set(table.schema.names) == {"id", "name"}
