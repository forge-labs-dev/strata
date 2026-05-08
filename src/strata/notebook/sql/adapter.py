"""DriverAdapter protocol and supporting types for SQL cells.

The protocol is intentionally Strata-shaped, not a generic SQL
abstraction: each method exists because the executor or cache layer
needs it. Per-driver implementations bind ADBC drivers to Strata's
provenance and read-only-execution semantics.

See ``docs/internal/design-sql-cells.md`` for the broader rationale.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class AdapterCapabilities:
    """Capability flags for a ``DriverAdapter``.

    The cache policy resolver consults these to decide whether a
    ``# @cache snapshot`` request can be honored, whether a
    ``fingerprint`` policy yields a real per-table token or has to fall
    back to session-scoped, and whether the executor needs to open a
    second probe connection alongside the query connection.
    """

    per_table_freshness: bool
    """True when ``probe_freshness`` returns a per-table token; False
    when only a database-wide token is available (SQLite) or no token
    at all (DuckDB native)."""

    supports_snapshot: bool
    """True when ``probe_freshness`` can return a durable snapshot
    identity (Iceberg ``snapshot_id``, BigQuery time-travel target)."""

    needs_separate_probe_conn: bool
    """True when the freshness probe must run on a connection separate
    from the query connection. Postgres requires this because
    ``pg_stat_*`` views are frozen inside an open transaction."""


@dataclass(frozen=True)
class QualifiedTable:
    """Fully qualified table reference.

    ``catalog`` and ``schema`` may be None for backends that don't
    expose those layers (SQLite has neither; an unqualified Postgres
    table name resolves to the connection's ``search_path`` first
    schema after qualify-time resolution).
    """

    catalog: str | None
    schema: str | None
    name: str

    def render(self) -> str:
        """Render as a dotted string for diagnostics and probe queries."""
        parts = [p for p in (self.catalog, self.schema, self.name) if p]
        return ".".join(parts)


@dataclass(frozen=True)
class FreshnessToken:
    """Opaque equality token reflecting database state for touched tables.

    Two tokens compare equal iff the touched tables produce the same
    query result. The token's bytes are not interpreted by the cache
    layer — only compared.

    ``is_session_only`` is True when the adapter couldn't derive a real
    fingerprint (e.g. DuckDB native, MySQL ``UPDATE_TIME=NULL`` after
    server restart) and substituted a session-unique salt. The executor
    surfaces this as a diagnostic so users know cache reuse is
    session-scoped only.

    ``is_snapshot`` is True when the token is a durable, reproducibly
    queryable snapshot ID (Iceberg ``snapshot_id`` is the canonical
    example). The ``# @cache snapshot`` policy requires this.
    """

    value: bytes
    is_session_only: bool = False
    is_snapshot: bool = False


@dataclass(frozen=True)
class ColumnInfo:
    """One column of a table, as the driver reports it.

    ``type`` is whatever the catalog's column-type column returns —
    SQL-text form (``INTEGER``, ``VARCHAR(64)``, ``timestamp with
    time zone``). Strata doesn't normalize across drivers because
    the user typed SQL for a specific dialect; surfacing the
    driver's own type label is the most honest thing.
    """

    name: str
    type: str
    nullable: bool | None = None


@dataclass(frozen=True)
class TableSchema:
    """A table's identity plus its columns.

    Used by the schema-discovery surface (``DriverAdapter.list_schema``)
    so the UI can show users the tables and columns available on a
    connection without them having to hand-write probe queries.
    """

    catalog: str | None
    schema: str | None
    name: str
    columns: tuple[ColumnInfo, ...] = ()

    def render(self) -> str:
        parts = [p for p in (self.catalog, self.schema, self.name) if p]
        return ".".join(parts)


@dataclass(frozen=True)
class SchemaFingerprint:
    """Opaque equality token reflecting touched-table column structure.

    Catches schema evolution that doesn't move the freshness token:
    metadata-only ADD COLUMN, type changes, column rename. Folded into
    the provenance hash so a cached Arrow Table whose schema would
    differ from a re-run is correctly invalidated.
    """

    value: bytes


class DriverAdapter(Protocol):
    """Per-driver glue between Strata's SQL pipeline and ADBC.

    Implementations live in ``strata.notebook.sql.drivers.*`` and are
    registered via ``register_adapter`` at import time. The executor
    looks up the adapter for the cell's connection driver via
    ``get_adapter(connection.driver)``.
    """

    name: str
    """Driver identifier, matched against ``ConnectionSpec.driver``."""

    sqlglot_dialect: str
    """Dialect name passed to ``sqlglot.parse(..., dialect=...)``."""

    capabilities: AdapterCapabilities

    def canonicalize_connection_id(self, spec: Any, *, read_only: bool = True) -> str:
        """Return a stable hash of the connection's identity-shaping config.

        Includes everything that changes object visibility (host, port,
        default database, role, warehouse, search-path-like options)
        and excludes secrets (auth credentials) and runtime-tunables
        that don't change visibility (e.g. ``application_name``,
        ``connect_timeout``).

        Two connections with the same ``connection_id`` are guaranteed
        to see the same set of objects from the same effective
        principal. Two connections that differ in identity-shaping
        config produce different ``connection_id`` so cache entries
        can't bleed between them.

        ``read_only`` lets adapters that route reads vs writes through
        different principals (Snowflake's ``write_role``, BigQuery's
        ``write_credentials_path``) include only the fields that
        actually shape *this* cell's identity. Without the param, a
        change to the write principal would invalidate read-cell
        caches even though read execution never touches it.
        Defaults to ``True`` because read cells are more common; the
        cell executor passes ``False`` for ``# @sql write=true``.
        """
        ...

    def open(self, spec: Any, *, read_only: bool) -> Any:
        """Open an ADBC connection in the requested mode.

        ``read_only=True`` means the executor wants the connection in
        an enforceable read-only mode (Postgres ``READ ONLY``
        transaction, SQLite immutable connection). Adapters that can't
        satisfy this raise ``RuntimeError`` — the executor turns the
        error into a user-visible diagnostic instead of falling back to
        keyword-based DML rejection.

        Returns whatever connection handle the driver provides; the
        executor treats it as opaque.
        """
        ...

    def probe_freshness(
        self,
        probe_conn: Any,
        tables: list[QualifiedTable],
    ) -> FreshnessToken:
        """Return a freshness token for the given tables.

        Called once per cell execution as part of the cache key check.
        Must be cheap (single metadata round-trip per touched
        database). Returning a token with ``is_session_only=True``
        signals the executor that the adapter couldn't derive a real
        fingerprint for some or all of the requested tables.
        """
        ...

    def probe_schema(
        self,
        probe_conn: Any,
        tables: list[QualifiedTable],
    ) -> SchemaFingerprint:
        """Return a schema fingerprint for the given tables.

        Catches metadata-only schema changes (ADD COLUMN, type changes)
        that the freshness token would miss. Cheap; one metadata read
        per touched table.
        """
        ...

    def list_schema(self, conn: Any) -> list[TableSchema]:
        """Enumerate the tables (and columns) visible on this connection.

        Used by the UI's schema-discovery sidebar so users can see
        what's available before writing SQL. Should be idempotent
        and side-effect free; the executor calls it on a read-only
        connection. Each ``TableSchema`` carries the table's
        catalog/schema/name plus a tuple of ``ColumnInfo`` (column
        name + driver-reported type).

        Adapters that can't enumerate (e.g. a driver behind a
        catalog the user lacks rights to) raise — the route
        surfaces the error verbatim so the user sees what went
        wrong.
        """
        ...


def hash_connection_identity(
    driver: str,
    identity: dict[str, Any],
) -> str:
    """Default canonicalization helper for adapters.

    Hashes ``{"driver": <driver>, **identity}`` as sorted JSON. The
    caller (the adapter) is responsible for choosing which keys belong
    in ``identity`` — i.e. which fields are identity-shaping for that
    driver — and for excluding secrets.

    Adapters can call this directly from
    ``canonicalize_connection_id``; nothing requires them to.
    """
    payload = {"driver": driver, **identity}
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(encoded.encode()).hexdigest()
