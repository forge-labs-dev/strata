"""Tests for the Snowflake DriverAdapter — contract, identity hash,
and probe shape with mocked ADBC connections. Real-Snowflake
integration tests are out of scope locally (would burn cloud-services
credits and need a live account)."""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from strata.notebook.models import ConnectionSpec
from strata.notebook.sql import FreshnessToken, QualifiedTable, SchemaFingerprint
from strata.notebook.sql.drivers.snowflake import SnowflakeAdapter

# --- mock-conn helper -------------------------------------------------------


class _FakeCursor:
    """Minimal DBAPI cursor that returns scripted rows.

    ``scripts`` is a list of (query_substring, result) pairs; the
    next ``fetchone`` / ``fetchall`` call uses the result whose
    query substring matches the most recently executed SQL. The
    cursor records every (sql, params) pair so tests can assert
    what the adapter actually issued.
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
        if last is None:
            return None
        return last

    def fetchall(self):
        last = self._last_match
        if isinstance(last, list):
            return last
        return [last] if last is not None else []

    def close(self):
        pass


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        self.commits = 0

    @contextmanager
    def cursor(self):
        yield self._cursor

    def commit(self):
        self.commits += 1

    def close(self):
        pass


# --- capabilities + dialect ------------------------------------------------


def test_capabilities_match_design():
    a = SnowflakeAdapter()
    assert a.name == "snowflake"
    assert a.sqlglot_dialect == "snowflake"
    assert a.capabilities.per_table_freshness is True
    assert a.capabilities.supports_snapshot is False
    # INFORMATION_SCHEMA isn't frozen inside a transaction the
    # way Postgres's pg_stat_* views are.
    assert a.capabilities.needs_separate_probe_conn is False


# --- canonicalize_connection_id -------------------------------------------


def test_connection_id_includes_all_identity_shaping_fields():
    """account, user, role, warehouse, database, schema all change
    what objects the connection sees — distinct ids."""
    a = SnowflakeAdapter()
    base = ConnectionSpec(
        name="x",
        driver="snowflake",
        account="ACME-PROD",
        user="reader",
        role="ANALYTICS_RO",
        warehouse="WH_SMALL",
        database="EVENTS",
        schema="public",
    )
    cid = a.canonicalize_connection_id(base)
    for field, new_value in [
        ("account", "ACME-DEV"),
        ("user", "writer"),
        ("role", "ANALYTICS_RW"),
        ("warehouse", "WH_LARGE"),
        ("database", "METRICS"),
        ("schema", "staging"),
    ]:
        variant = base.model_copy(update={field: new_value})
        assert a.canonicalize_connection_id(variant) != cid, (
            f"changing {field} should change the connection id"
        )


def test_connection_id_excludes_password():
    """Password is a secret — never identity-shaping."""
    a = SnowflakeAdapter()
    base = ConnectionSpec(
        name="x",
        driver="snowflake",
        account="ACME",
        user="reader",
        auth={"user": "reader", "password": "${SF_PASS}"},
    )
    with_different_pw = ConnectionSpec(
        name="x",
        driver="snowflake",
        account="ACME",
        user="reader",
        auth={"user": "reader", "password": "${OTHER_PASS}"},
    )
    assert a.canonicalize_connection_id(base) == a.canonicalize_connection_id(with_different_pw)


def test_connection_id_resolves_auth_user_var(monkeypatch):
    """Two specs that point at the same effective user via different
    expression paths (one literal, one ${VAR}) produce the same id
    once the env var is resolved — so cache identity is stable
    across user-typed and env-derived configs."""
    monkeypatch.setenv("SF_USER", "reader")
    a = SnowflakeAdapter()
    via_literal = ConnectionSpec(
        name="x",
        driver="snowflake",
        account="ACME",
        user="reader",
    )
    via_var = ConnectionSpec(
        name="x",
        driver="snowflake",
        account="ACME",
        auth={"user": "${SF_USER}"},
    )
    assert a.canonicalize_connection_id(via_literal) == a.canonicalize_connection_id(via_var)


# --- open() session-shaping --------------------------------------------------


def test_open_applies_role_warehouse_database_schema_in_order():
    """Each declared identity-shaping field issues its own ``USE …``
    statement before the connection is handed back. Order matches
    the spec field order in the adapter so a missing role doesn't
    leave the warehouse / db / schema unconfigured."""
    cur = _FakeCursor([])
    a = SnowflakeAdapter(connect_fn=lambda _uri: _FakeConn(cur))

    spec = ConnectionSpec(
        name="x",
        driver="snowflake",
        account="ACME",
        role="ANALYTICS_RO",
        warehouse="WH_SMALL",
        database="EVENTS",
        schema="PUBLIC",
    )
    a.open(spec, read_only=True)

    issued = [sql for sql, _ in cur.executions]
    assert issued == [
        'USE ROLE "ANALYTICS_RO"',
        'USE WAREHOUSE "WH_SMALL"',
        'USE DATABASE "EVENTS"',
        'USE SCHEMA "PUBLIC"',
    ]


def test_open_skips_unset_fields():
    """Only fields present on the spec issue ``USE …``. A spec
    with just account + role doesn't try to ``USE WAREHOUSE`` of
    an empty string."""
    cur = _FakeCursor([])
    a = SnowflakeAdapter(connect_fn=lambda _uri: _FakeConn(cur))
    a.open(
        ConnectionSpec(name="x", driver="snowflake", account="ACME", role="ANALYTICS_RO"),
        read_only=True,
    )
    assert [sql for sql, _ in cur.executions] == ['USE ROLE "ANALYTICS_RO"']


def test_open_rejects_invalid_identifier():
    """``USE`` doesn't accept bind parameters; we splice the
    identifier in. A pathological value (semicolons, quotes) must
    be rejected before any SQL hits the connection."""
    cur = _FakeCursor([])
    a = SnowflakeAdapter(connect_fn=lambda _uri: _FakeConn(cur))
    with pytest.raises(RuntimeError, match="Snowflake identifier"):
        a.open(
            ConnectionSpec(
                name="x",
                driver="snowflake",
                account="ACME",
                role="evil; DROP TABLE secret",
            ),
            read_only=True,
        )
    # No SQL was issued before the validation error.
    assert cur.executions == []


# --- _build_uri --------------------------------------------------------------


def test_build_uri_from_components():
    a = SnowflakeAdapter()
    spec = ConnectionSpec(
        name="x",
        driver="snowflake",
        account="ACME-PROD",
        database="EVENTS",
        schema="PUBLIC",
        warehouse="WH_SMALL",
        role="ANALYTICS_RO",
        auth={"user": "reader", "password": "p@ss/w0rd"},
    )
    uri = a._build_uri(spec)
    assert uri.startswith("snowflake://reader:p%40ss%2Fw0rd@ACME-PROD/EVENTS/PUBLIC")
    assert "warehouse=WH_SMALL" in uri
    assert "role=ANALYTICS_RO" in uri


def test_build_uri_passes_through_explicit_uri():
    """When a user has hand-rolled a Snowflake URI (private-key auth,
    custom params) we don't second-guess it."""
    a = SnowflakeAdapter()
    spec = ConnectionSpec(
        name="x",
        driver="snowflake",
        uri="snowflake://reader@ACME/EVENTS?authenticator=externalbrowser",
    )
    assert a._build_uri(spec) == ("snowflake://reader@ACME/EVENTS?authenticator=externalbrowser")


def test_build_uri_requires_account_or_uri():
    a = SnowflakeAdapter()
    with pytest.raises(RuntimeError, match="account"):
        a._build_uri(ConnectionSpec(name="x", driver="snowflake"))


# --- probe_freshness --------------------------------------------------------


def test_probe_freshness_groups_by_database():
    """One INFORMATION_SCHEMA query per touched database (Snowflake
    scopes INFORMATION_SCHEMA per database). Tables grouped by
    catalog yield one round-trip per group."""
    cur = _FakeCursor(
        [
            ("INFORMATION_SCHEMA.TABLES", ("PUBLIC", "events", "2026-05-01 12:00:00")),
        ]
    )
    a = SnowflakeAdapter()
    tables = [
        QualifiedTable(catalog="EVENTS", schema="PUBLIC", name="events"),
        QualifiedTable(catalog="METRICS", schema="PUBLIC", name="counters"),
    ]
    token = a.probe_freshness(_FakeConn(cur), tables)
    assert isinstance(token, FreshnessToken)
    assert token.value  # non-empty hash

    # The adapter issued one INFORMATION_SCHEMA query per database.
    db_queries = [sql for sql, _ in cur.executions if "INFORMATION_SCHEMA" in sql]
    assert len(db_queries) == 2
    # Each query targets the right database (spliced as
    # "<DB>".INFORMATION_SCHEMA…).
    assert any('"EVENTS".INFORMATION_SCHEMA' in q for q in db_queries)
    assert any('"METRICS".INFORMATION_SCHEMA' in q for q in db_queries)


def test_probe_freshness_uses_current_database_for_unqualified_tables():
    """A table without a catalog falls back to CURRENT_DATABASE()."""
    cur = _FakeCursor(
        [
            ("CURRENT_DATABASE", ("MAIN_DB",)),
            ("INFORMATION_SCHEMA.TABLES", ("PUBLIC", "events", "ts")),
        ]
    )
    a = SnowflakeAdapter()
    tables = [QualifiedTable(catalog=None, schema="PUBLIC", name="events")]
    a.probe_freshness(_FakeConn(cur), tables)

    queries = [sql for sql, _ in cur.executions]
    # First we resolve the current database, then use it.
    assert "CURRENT_DATABASE" in queries[0]
    assert any('"MAIN_DB".INFORMATION_SCHEMA.TABLES' in q for q in queries)


def test_probe_freshness_token_changes_on_last_altered():
    """Same table, different LAST_ALTERED → different token. This
    is the core fingerprint property — without it, cache
    invalidation is broken."""
    a = SnowflakeAdapter()
    tables = [QualifiedTable(catalog="EVENTS", schema="PUBLIC", name="orders")]

    cur1 = _FakeCursor([("INFORMATION_SCHEMA.TABLES", ("PUBLIC", "orders", "2026-05-01 12:00:00"))])
    token1 = a.probe_freshness(_FakeConn(cur1), tables)

    cur2 = _FakeCursor([("INFORMATION_SCHEMA.TABLES", ("PUBLIC", "orders", "2026-05-01 12:01:00"))])
    token2 = a.probe_freshness(_FakeConn(cur2), tables)
    assert token1.value != token2.value


def test_probe_freshness_missing_table_distinct_from_present():
    """A table that resolves vs one that doesn't must produce
    different tokens — otherwise a permission lapse silently
    masquerades as 'unchanged.'"""
    a = SnowflakeAdapter()
    tables = [QualifiedTable(catalog="EVENTS", schema="PUBLIC", name="orders")]

    cur_present = _FakeCursor([("INFORMATION_SCHEMA.TABLES", ("PUBLIC", "orders", "2026-05-01"))])
    cur_missing = _FakeCursor([("INFORMATION_SCHEMA.TABLES", None)])
    a_present = a.probe_freshness(_FakeConn(cur_present), tables)
    a_missing = a.probe_freshness(_FakeConn(cur_missing), tables)
    assert a_present.value != a_missing.value


def test_probe_freshness_empty_tables():
    """No tables touched → empty token, no queries issued."""
    cur = _FakeCursor([])
    a = SnowflakeAdapter()
    token = a.probe_freshness(_FakeConn(cur), [])
    assert token.value == b""
    assert cur.executions == []


# --- probe_schema -----------------------------------------------------------


def test_probe_schema_walks_columns():
    """Schema fingerprint reads INFORMATION_SCHEMA.COLUMNS per
    touched table, grouped by database. Catches metadata-only
    changes that LAST_ALTERED would also catch — belt-and-
    suspenders."""
    cur = _FakeCursor(
        [
            (
                "INFORMATION_SCHEMA.COLUMNS",
                [
                    ("id", "NUMBER", "NO"),
                    ("label", "TEXT", "YES"),
                ],
            ),
        ]
    )
    a = SnowflakeAdapter()
    tables = [QualifiedTable(catalog="EVENTS", schema="PUBLIC", name="orders")]
    fp = a.probe_schema(_FakeConn(cur), tables)
    assert isinstance(fp, SchemaFingerprint)
    assert fp.value


def test_probe_schema_fingerprint_changes_on_column_set():
    """Add a column → different fingerprint, even if LAST_ALTERED
    didn't move."""
    a = SnowflakeAdapter()
    tables = [QualifiedTable(catalog="EVENTS", schema="PUBLIC", name="orders")]

    cur1 = _FakeCursor([("INFORMATION_SCHEMA.COLUMNS", [("id", "NUMBER", "NO")])])
    cur2 = _FakeCursor(
        [
            (
                "INFORMATION_SCHEMA.COLUMNS",
                [("id", "NUMBER", "NO"), ("label", "TEXT", "YES")],
            )
        ]
    )
    fp1 = a.probe_schema(_FakeConn(cur1), tables)
    fp2 = a.probe_schema(_FakeConn(cur2), tables)
    assert fp1.value != fp2.value


# --- list_schema ------------------------------------------------------------


def test_list_schema_uses_current_database():
    """Schema discovery scopes to the connection's current
    database. The route returns columns grouped by ordinal_position
    so users see the natural ordering."""
    cur = _FakeCursor(
        [
            ("CURRENT_DATABASE", ("EVENTS",)),
            (
                "INFORMATION_SCHEMA.TABLES",
                [
                    ("EVENTS", "PUBLIC", "orders", "id", "NUMBER", "NO"),
                    ("EVENTS", "PUBLIC", "orders", "amount", "NUMBER", "YES"),
                    ("EVENTS", "PUBLIC", "products", "sku", "TEXT", "NO"),
                ],
            ),
        ]
    )
    a = SnowflakeAdapter()
    schema = a.list_schema(_FakeConn(cur))
    by_name = {t.name: t for t in schema}
    assert set(by_name) == {"orders", "products"}

    orders = by_name["orders"]
    assert [c.name for c in orders.columns] == ["id", "amount"]
    id_col = orders.columns[0]
    assert id_col.type == "NUMBER"
    assert id_col.nullable is False
    amount_col = orders.columns[1]
    assert amount_col.nullable is True


def test_list_schema_empty_when_no_database():
    """If CURRENT_DATABASE() is null (rare but possible — no
    default DB on the role), enumeration silently returns []
    rather than running an unscoped INFORMATION_SCHEMA query."""
    cur = _FakeCursor([("CURRENT_DATABASE", (None,))])
    a = SnowflakeAdapter()
    assert a.list_schema(_FakeConn(cur)) == []
