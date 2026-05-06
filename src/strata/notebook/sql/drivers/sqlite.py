"""SQLite driver adapter.

Backed by ``adbc-driver-sqlite``. The freshness probe combines
``PRAGMA data_version`` (database-wide write counter) with
``PRAGMA schema_version`` (DDL counter); both are DB-wide rather than
per-table because SQLite doesn't expose per-table change tracking.
The schema fingerprint is still per-table — column structure lives
in ``pragma_table_info``.

Read-only enforcement uses SQLite's URI ``mode=ro`` so the connection
is opened against an immutable DB handle; any ``INSERT`` / ``UPDATE``
/ ``DELETE`` is rejected at the engine before reaching disk.

See ``docs/internal/design-sql-cells.md`` for the SQLite-specific
gotcha list, especially: ``data_version`` only increments on writes
from *other* connections (Phase 1 SQL cells are read-only, so
self-writes that would defeat the probe aren't possible from cells).
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable
from typing import Any

from strata.notebook.sql.adapter import (
    AdapterCapabilities,
    FreshnessToken,
    QualifiedTable,
    SchemaFingerprint,
    hash_connection_identity,
)
from strata.notebook.sql.registry import register_adapter

_CAPABILITIES = AdapterCapabilities(
    # SQLite has no per-table change counter — ``data_version`` is
    # database-wide, so any write to any table flips the freshness
    # token. Acceptable for the common "one DB per notebook" case;
    # documented as a known limitation for shared-DB setups.
    per_table_freshness=False,
    supports_snapshot=False,
    # Pragmas don't have transaction-frozen semantics like Postgres
    # ``pg_stat_*``; the same connection can probe and query.
    needs_separate_probe_conn=False,
)

# ``pragma_table_info`` is the table-valued form of ``PRAGMA table_info(...)``;
# it accepts bind parameters and projects standard columns. Using the
# function form here means we don't have to splice the table name into
# the SQL text and re-derive identifier quoting.
_SCHEMA_QUERY = """
SELECT name, type, "notnull", dflt_value, pk
FROM pragma_table_info(?)
ORDER BY cid
"""


class SqliteAdapter:
    """ADBC-backed driver adapter for SQLite."""

    name = "sqlite"
    sqlglot_dialect = "sqlite"
    capabilities = _CAPABILITIES

    def __init__(
        self,
        *,
        connect_fn: Callable[[str], Any] | None = None,
    ) -> None:
        # Test seam: pass a fake connect callable to bypass the real
        # ADBC import in unit tests.
        self._connect_fn = connect_fn

    # --- identity ---------------------------------------------------------

    def canonicalize_connection_id(self, spec: Any) -> str:
        """Hash the absolute DB path (or URI / ``:memory:`` literal).

        Identity-shaping for SQLite is just "which file." Two specs
        that resolve to the same absolute path produce the same id;
        relative-path differences canonicalize away. ``:memory:``
        connections produce a stable id that's distinct from any
        on-disk path.
        """
        return hash_connection_identity(self.name, self._extract_identity(spec))

    def _extract_identity(self, spec: Any) -> dict[str, Any]:
        identity: dict[str, Any] = {}
        path = getattr(spec, "path", None)
        uri = getattr(spec, "uri", None)
        if uri:
            identity["uri"] = self._canonicalize_uri(uri)
        elif path:
            if path == ":memory:":
                identity["path"] = ":memory:"
            else:
                identity["path"] = os.path.abspath(path)
        return identity

    def _canonicalize_uri(self, uri: str) -> str:
        """Strip read-only query parameters from URI for identity hashing.

        ``mode=ro`` and ``immutable=1`` change *how* we open, not which
        objects we see — they shouldn't perturb the cache key. The
        path portion of a ``file:`` URI is canonicalized to absolute.
        """
        if uri == ":memory:":
            return uri
        if not uri.startswith("file:"):
            # Non-URI form; treat as a path.
            return os.path.abspath(uri)
        # Split file:<path>?<query>
        rest = uri[len("file:") :]
        if "?" in rest:
            path_part, _ = rest.split("?", 1)
        else:
            path_part = rest
        # Drop leading "//" if URI uses authority form.
        if path_part.startswith("//"):
            path_part = path_part[2:]
            # Drop any host portion after "//".
            if "/" in path_part:
                path_part = "/" + path_part.split("/", 1)[1]
        if not path_part:
            return uri
        return f"file:{os.path.abspath(path_part)}"

    # --- connection lifecycle --------------------------------------------

    def open(self, spec: Any, *, read_only: bool) -> Any:
        """Open an ADBC SQLite connection.

        ``read_only=True`` builds a ``file:<path>?mode=ro`` URI so
        SQLite enforces read-only at the file-handle level. Attempts
        to issue DML through the connection error at the engine; this
        is the security boundary, not SQL-text keyword filtering.
        """
        uri = self._build_uri(spec, read_only=read_only)
        return self._invoke_connect(uri)

    def _build_uri(self, spec: Any, *, read_only: bool) -> str:
        path = getattr(spec, "path", None)
        existing_uri = getattr(spec, "uri", None)

        if existing_uri:
            if existing_uri == ":memory:":
                return existing_uri
            if read_only and "mode=" not in existing_uri:
                sep = "&" if "?" in existing_uri else "?"
                return f"{existing_uri}{sep}mode=ro"
            return existing_uri

        if not path:
            raise RuntimeError("SQLite connection requires either ``path`` or ``uri`` to be set")

        if path == ":memory:":
            # In-memory DBs can't be read-only — there's nothing to
            # restrict. Just hand the literal back.
            return ":memory:"

        abspath = os.path.abspath(path)
        if read_only:
            return f"file:{abspath}?mode=ro"
        return abspath

    def _invoke_connect(self, uri: str) -> Any:
        if self._connect_fn is not None:
            return self._connect_fn(uri)
        try:
            from adbc_driver_sqlite import dbapi as adbc_sqlite
        except ImportError as exc:
            raise RuntimeError(
                "adbc-driver-sqlite is not installed; install with "
                "`uv pip install 'strata[sql-sqlite]'`"
            ) from exc
        return adbc_sqlite.connect(uri)

    # --- probes -----------------------------------------------------------

    def probe_freshness(
        self,
        probe_conn: Any,
        tables: list[QualifiedTable],
    ) -> FreshnessToken:
        """DB-wide freshness via ``PRAGMA data_version`` + ``schema_version``.

        ``tables`` is intentionally ignored — SQLite doesn't expose
        per-table change counters, so every cell against this
        connection sees the same token. A write to any table (from
        another connection or process) increments ``data_version``;
        any DDL increments ``schema_version``.

        Caveat: ``data_version`` does NOT increment for writes on the
        connection it's queried on. Phase 1 SQL cells are read-only
        and the probe runs on its own usage path, so this gotcha
        doesn't bite us in practice — but the limitation is real and
        documented.
        """
        h = hashlib.sha256()
        with probe_conn.cursor() as cursor:
            cursor.execute("PRAGMA data_version")
            data_row = cursor.fetchone()
            cursor.execute("PRAGMA schema_version")
            schema_row = cursor.fetchone()

        if data_row is None or schema_row is None:
            # Pragmas always return a row on a healthy SQLite handle;
            # missing here means the connection is broken. Surface as
            # a session-only token rather than crashing the executor.
            return FreshnessToken(value=b"sqlite-pragma-missing", is_session_only=True)

        h.update(b"data_version:")
        h.update(str(data_row[0]).encode())
        h.update(b":schema_version:")
        h.update(str(schema_row[0]).encode())
        return FreshnessToken(value=h.digest())

    def probe_schema(
        self,
        probe_conn: Any,
        tables: list[QualifiedTable],
    ) -> SchemaFingerprint:
        """Per-table schema fingerprint via ``pragma_table_info``.

        SQLite's freshness probe is DB-wide, but column structure is
        per-table — and a schema-only change (rename column, ADD
        COLUMN) on a table that no other connection has written to
        wouldn't move ``data_version``. The schema probe is what
        catches that.
        """
        if not tables:
            return SchemaFingerprint(value=b"")

        h = hashlib.sha256()
        with probe_conn.cursor() as cursor:
            for table in sorted(tables, key=lambda t: t.render()):
                # SQLite's notion of "schema" is attached-database
                # name (default ``main``). For Phase 1 we ignore the
                # attached-DB layer and look up by bare table name —
                # ``pragma_table_info`` resolves through the connection's
                # standard search order.
                cursor.execute(_SCHEMA_QUERY, (table.name,))
                rows = cursor.fetchall() or []

                h.update(table.render().encode())
                h.update(b"\x00")
                for col_name, col_type, notnull, _dflt, _pk in rows:
                    h.update(str(col_name).encode())
                    h.update(b":")
                    h.update(str(col_type).encode())
                    h.update(b":")
                    h.update(str(notnull).encode())
                    h.update(b"\x00")
                h.update(b"\x00")

        return SchemaFingerprint(value=h.digest())


_ADAPTER = SqliteAdapter()


def register() -> None:
    """Register this adapter in the global SQL driver registry."""
    register_adapter(_ADAPTER)


# Auto-register on first import.
register()
