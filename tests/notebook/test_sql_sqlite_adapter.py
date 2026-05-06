"""Tests for the SQLite DriverAdapter.

Includes a real-DB integration block at the bottom — SQLite is local
and free, so we don't need testcontainers to exercise the full open →
probe → write → reprobe cycle.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

import pytest

from strata.notebook.models import ConnectionSpec
from strata.notebook.sql import FreshnessToken, QualifiedTable, SchemaFingerprint
from strata.notebook.sql.drivers.sqlite import SqliteAdapter

# --- mock-conn helper -------------------------------------------------------


class _FakeCursor:
    """Scripted cursor: maps query-substring → result.

    Each ``execute`` records its SQL + params; ``fetchone`` and
    ``fetchall`` return the most recently matched script entry.
    """

    def __init__(self, scripts: list[tuple[str, object]]):
        self._scripts = scripts
        self._last_match: object = None
        self.executions: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.executions.append((sql, tuple(params)))
        for needle, value in self._scripts:
            if needle in sql:
                self._last_match = value
                return
        self._last_match = None

    def fetchone(self):
        last = self._last_match
        if isinstance(last, list):
            return last.pop(0) if last else None
        return last

    def fetchall(self):
        last = self._last_match
        if isinstance(last, list):
            return last
        return [last] if last is not None else []


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    @contextmanager
    def cursor(self):
        yield self._cursor


# --- capability flags -------------------------------------------------------


def test_capabilities_match_design_doc():
    a = SqliteAdapter()
    assert a.name == "sqlite"
    assert a.sqlglot_dialect == "sqlite"
    # data_version / schema_version are DB-wide, not per-table.
    assert a.capabilities.per_table_freshness is False
    assert a.capabilities.supports_snapshot is False
    # Pragmas don't have transaction-frozen semantics — same
    # connection can probe and query.
    assert a.capabilities.needs_separate_probe_conn is False


# --- canonicalize_connection_id -------------------------------------------


def test_connection_id_canonicalizes_relative_paths(tmp_path, monkeypatch):
    """Two specs that resolve to the same absolute path produce the
    same id, regardless of how the path was written. macOS's
    /var → /private/var symlink dance is the reason we use the
    fixture-supplied tmp_path and resolve both sides through abspath
    on the same root."""
    a = SqliteAdapter()
    nested = tmp_path / "nested"
    nested.mkdir()
    monkeypatch.chdir(tmp_path)

    absolute = ConnectionSpec(
        name="db",
        driver="sqlite",
        path=os.path.abspath(str(nested / "events.sqlite")),
    )
    relative = ConnectionSpec(
        name="db",
        driver="sqlite",
        path="nested/events.sqlite",
    )
    assert a.canonicalize_connection_id(absolute) == a.canonicalize_connection_id(relative)


def test_connection_id_distinguishes_paths():
    a = SqliteAdapter()
    a_spec = ConnectionSpec(name="db", driver="sqlite", path="/tmp/a.sqlite")
    b_spec = ConnectionSpec(name="db", driver="sqlite", path="/tmp/b.sqlite")
    assert a.canonicalize_connection_id(a_spec) != a.canonicalize_connection_id(b_spec)


def test_connection_id_strips_read_only_query_params():
    """``mode=ro`` and ``immutable=1`` change *how* we open, not which
    objects we see — must not perturb the cache key."""
    a = SqliteAdapter()
    base = ConnectionSpec(name="db", driver="sqlite", uri="file:/tmp/db.sqlite")
    with_ro = ConnectionSpec(name="db", driver="sqlite", uri="file:/tmp/db.sqlite?mode=ro")
    with_immutable = ConnectionSpec(
        name="db", driver="sqlite", uri="file:/tmp/db.sqlite?immutable=1"
    )
    cid = a.canonicalize_connection_id(base)
    assert a.canonicalize_connection_id(with_ro) == cid
    assert a.canonicalize_connection_id(with_immutable) == cid


def test_connection_id_memory_distinct_from_file():
    a = SqliteAdapter()
    mem = ConnectionSpec(name="db", driver="sqlite", path=":memory:")
    file = ConnectionSpec(name="db", driver="sqlite", path="/tmp/mem.sqlite")
    assert a.canonicalize_connection_id(mem) != a.canonicalize_connection_id(file)


# --- open() URI building --------------------------------------------------


def test_open_with_read_only_appends_mode_ro_to_path():
    captured: dict[str, str] = {}

    def fake_connect(uri):
        captured["uri"] = uri
        return _FakeConn(_FakeCursor(scripts=[]))

    a = SqliteAdapter(connect_fn=fake_connect)
    spec = ConnectionSpec(name="db", driver="sqlite", path="/tmp/events.sqlite")
    a.open(spec, read_only=True)
    assert captured["uri"] == "file:/tmp/events.sqlite?mode=ro"


def test_open_without_read_only_uses_bare_path():
    captured: dict[str, str] = {}

    def fake_connect(uri):
        captured["uri"] = uri
        return _FakeConn(_FakeCursor(scripts=[]))

    a = SqliteAdapter(connect_fn=fake_connect)
    spec = ConnectionSpec(name="db", driver="sqlite", path="/tmp/events.sqlite")
    a.open(spec, read_only=False)
    assert captured["uri"] == "/tmp/events.sqlite"


def test_open_memory_db_passes_through_unchanged():
    """``:memory:`` databases can't be read-only — there's nothing to
    restrict — so we hand the literal back even when read_only=True."""
    captured: dict[str, str] = {}

    def fake_connect(uri):
        captured["uri"] = uri
        return _FakeConn(_FakeCursor(scripts=[]))

    a = SqliteAdapter(connect_fn=fake_connect)
    spec = ConnectionSpec(name="db", driver="sqlite", path=":memory:")
    a.open(spec, read_only=True)
    assert captured["uri"] == ":memory:"


def test_open_existing_uri_appends_mode_ro_when_missing():
    captured: dict[str, str] = {}

    def fake_connect(uri):
        captured["uri"] = uri
        return _FakeConn(_FakeCursor(scripts=[]))

    a = SqliteAdapter(connect_fn=fake_connect)
    spec = ConnectionSpec(name="db", driver="sqlite", uri="file:/tmp/events.sqlite")
    a.open(spec, read_only=True)
    assert "mode=ro" in captured["uri"]


def test_open_existing_uri_with_mode_does_not_double_append():
    captured: dict[str, str] = {}

    def fake_connect(uri):
        captured["uri"] = uri
        return _FakeConn(_FakeCursor(scripts=[]))

    a = SqliteAdapter(connect_fn=fake_connect)
    spec = ConnectionSpec(
        name="db",
        driver="sqlite",
        uri="file:/tmp/events.sqlite?mode=rwc",
    )
    a.open(spec, read_only=True)
    # User wrote mode=rwc; we don't override it, just pass through.
    assert captured["uri"] == "file:/tmp/events.sqlite?mode=rwc"


def test_open_raises_when_neither_path_nor_uri_set():
    a = SqliteAdapter(connect_fn=lambda uri: None)
    spec = ConnectionSpec(name="db", driver="sqlite")
    with pytest.raises(RuntimeError, match="path"):
        a.open(spec, read_only=True)


# --- probe_freshness (mocked) --------------------------------------------


def test_probe_freshness_combines_data_and_schema_version():
    a = SqliteAdapter()
    cursor = _FakeCursor(
        scripts=[
            ("data_version", [(123,)]),
            ("schema_version", [(7,)]),
        ]
    )
    conn = _FakeConn(cursor)
    token = a.probe_freshness(conn, [QualifiedTable(None, None, "users")])
    assert isinstance(token, FreshnessToken)
    assert not token.is_session_only
    assert token.value != b""


def test_probe_freshness_token_changes_on_data_version():
    a = SqliteAdapter()

    def make_token(data_v: int, schema_v: int) -> bytes:
        cursor = _FakeCursor(
            scripts=[
                ("data_version", [(data_v,)]),
                ("schema_version", [(schema_v,)]),
            ]
        )
        return a.probe_freshness(_FakeConn(cursor), [QualifiedTable(None, None, "t")]).value

    base = make_token(1, 1)
    assert make_token(1, 1) == base
    assert make_token(2, 1) != base  # data_version moved → DML happened
    assert make_token(1, 2) != base  # schema_version moved → DDL happened


def test_probe_freshness_ignores_table_list_db_wide():
    """Capability flag says per_table_freshness=False — the token is
    DB-wide, so different table sets must produce the same token."""
    a = SqliteAdapter()

    def make_token(tables):
        cursor = _FakeCursor(
            scripts=[
                ("data_version", [(99,)]),
                ("schema_version", [(5,)]),
            ]
        )
        return a.probe_freshness(_FakeConn(cursor), tables).value

    a_only = make_token([QualifiedTable(None, None, "users")])
    a_and_b = make_token(
        [QualifiedTable(None, None, "users"), QualifiedTable(None, None, "orders")]
    )
    assert a_only == a_and_b


# --- probe_schema (mocked) ----------------------------------------------


def test_probe_schema_token_reflects_columns():
    a = SqliteAdapter()

    def make_token(rows):
        # PRAGMA table_info row shape: (cid via ordering, name, type,
        # notnull, dflt, pk). Our SCHEMA_QUERY projects just
        # (name, type, notnull, dflt_value, pk).
        cursor = _FakeCursor(scripts=[("pragma_table_info", list(rows))])
        return a.probe_schema(_FakeConn(cursor), [QualifiedTable(None, None, "users")]).value

    base = make_token([("id", "INTEGER", 1, None, 1), ("name", "TEXT", 0, None, 0)])
    assert make_token([("id", "INTEGER", 1, None, 1), ("name", "TEXT", 0, None, 0)]) == base
    # ADD COLUMN
    assert (
        make_token(
            [
                ("id", "INTEGER", 1, None, 1),
                ("name", "TEXT", 0, None, 0),
                ("age", "INTEGER", 0, None, 0),
            ]
        )
        != base
    )
    # Type change
    assert make_token([("id", "BIGINT", 1, None, 1), ("name", "TEXT", 0, None, 0)]) != base
    # Nullability flip
    assert make_token([("id", "INTEGER", 1, None, 1), ("name", "TEXT", 1, None, 0)]) != base


def test_probe_schema_empty_tables_returns_empty_token():
    a = SqliteAdapter()
    token = a.probe_schema(_FakeConn(_FakeCursor(scripts=[])), [])
    assert isinstance(token, SchemaFingerprint)
    assert token.value == b""


def test_probe_schema_uses_pragma_table_info():
    a = SqliteAdapter()
    cursor = _FakeCursor(scripts=[("pragma_table_info", [])])
    conn = _FakeConn(cursor)
    a.probe_schema(conn, [QualifiedTable(None, None, "events")])
    sql, params = cursor.executions[0]
    assert "pragma_table_info" in sql
    assert params == ("events",)


# --- registry integration -------------------------------------------------


def test_sqlite_adapter_is_auto_registered():
    from strata.notebook.sql import get_adapter, known_drivers

    assert "sqlite" in known_drivers()
    assert get_adapter("sqlite").name == "sqlite"


# --- real-DB integration --------------------------------------------------
# These tests use ADBC against a real SQLite file. Skipped if the
# package isn't installed, otherwise execute the full open → probe →
# write → reprobe cycle to catch protocol-level mistakes the mock
# tests miss.


def _adbc_sqlite_available() -> bool:
    try:
        import adbc_driver_sqlite.dbapi  # noqa: F401

        return True
    except ImportError:
        return False


pytestmark_real_db = pytest.mark.skipif(
    not _adbc_sqlite_available(), reason="adbc-driver-sqlite not installed"
)


@pytestmark_real_db
def test_real_open_read_only_rejects_write(tmp_path):
    """Read-only enforcement must be the engine's job, not text
    filtering. A DML statement issued through a read-only connection
    has to error at the SQLite engine — though the error may surface
    on cursor close rather than on execute, depending on whether the
    failed statement is reported synchronously."""
    db_path = tmp_path / "rw.sqlite"
    seed = sqlite3.connect(db_path)
    seed.execute("CREATE TABLE t (x INTEGER)")
    seed.execute("INSERT INTO t VALUES (1)")
    seed.commit()
    seed.close()

    a = SqliteAdapter()
    spec = ConnectionSpec(name="db", driver="sqlite", path=str(db_path))
    conn = a.open(spec, read_only=True)
    try:
        # Wrap the whole cursor lifecycle, not just .execute(): ADBC
        # SQLite surfaces the read-only failure during statement
        # finalize/close, not at execute time.
        with pytest.raises(Exception, match="readonly"):
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO t VALUES (2)")
    finally:
        conn.close()


@pytestmark_real_db
def test_real_schema_version_changes_on_ddl(tmp_path):
    """``schema_version`` is the more reliable cross-implementation
    signal — Python's stdlib ``sqlite3`` and ADBC's bundled SQLite
    are different builds, so ``data_version``'s "writes by another
    connection" semantics aren't guaranteed across them. Schema
    changes always bump ``schema_version`` and must move the
    freshness token."""
    db_path = tmp_path / "ddl.sqlite"
    seed = sqlite3.connect(db_path)
    seed.execute("CREATE TABLE t (x INTEGER)")
    seed.commit()
    seed.close()

    a = SqliteAdapter()
    spec = ConnectionSpec(name="db", driver="sqlite", path=str(db_path))

    probe = a.open(spec, read_only=True)
    try:
        before = a.probe_freshness(probe, [QualifiedTable(None, None, "t")]).value
    finally:
        probe.close()

    altering = sqlite3.connect(db_path)
    altering.execute("ALTER TABLE t ADD COLUMN y TEXT")
    altering.commit()
    altering.close()

    probe2 = a.open(spec, read_only=True)
    try:
        after = a.probe_freshness(probe2, [QualifiedTable(None, None, "t")]).value
    finally:
        probe2.close()

    assert before != after, "schema_version should advance after DDL"


@pytestmark_real_db
def test_real_schema_fingerprint_changes_on_add_column(tmp_path):
    """A schema-only edit (no row changes) must move the schema
    fingerprint even when freshness sees the new ``schema_version``
    too — both probes must catch this."""
    db_path = tmp_path / "schema.sqlite"
    seed = sqlite3.connect(db_path)
    seed.execute("CREATE TABLE t (x INTEGER)")
    seed.commit()
    seed.close()

    a = SqliteAdapter()
    spec = ConnectionSpec(name="db", driver="sqlite", path=str(db_path))

    probe = a.open(spec, read_only=True)
    try:
        before = a.probe_schema(probe, [QualifiedTable(None, None, "t")]).value
    finally:
        probe.close()

    altering = sqlite3.connect(db_path)
    altering.execute("ALTER TABLE t ADD COLUMN y TEXT")
    altering.commit()
    altering.close()

    probe2 = a.open(spec, read_only=True)
    try:
        after = a.probe_schema(probe2, [QualifiedTable(None, None, "t")]).value
    finally:
        probe2.close()

    assert before != after
