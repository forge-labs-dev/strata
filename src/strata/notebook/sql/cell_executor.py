"""SQL cell executor — coordinator for the language=='sql' path.

Mirrors the structure of ``prompt_executor.execute_prompt_cell``: a
single entry point ``execute_sql_cell`` that ``CellExecutor`` calls
when it dispatches a SQL cell. The function:

1. Parses annotations and resolves the connection / adapter.
2. Runs the analyzer with the adapter's dialect.
3. Loads upstream variables and resolves bind parameters.
4. Resolves the ``# @cache`` policy and runs probes if required.
5. Computes the SQL provenance hash via the helpers in
   ``strata.notebook.sql.provenance``.
6. Checks the artifact store for a cache hit; on hit, returns the
   cached Arrow Table.
7. On miss, opens an enforced read-only ADBC connection, executes
   the query with positional binds, and stores the result as an
   ``arrow/ipc`` artifact.

Failure modes (missing connection, unknown driver, parse error,
read-only-enforcement failure, cache-policy error, bind error) all
surface as a populated ``error`` field in the result dict — same
shape the prompt executor uses — so the caller can render them in
the cell's output panel without special handling.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import time
from typing import TYPE_CHECKING, Any

from strata.notebook.annotations import parse_annotations
from strata.notebook.sql.analyzer import analyze_sql_cell, rewrite_named_to_positional
from strata.notebook.sql.bind import BindError, resolve_bind_params
from strata.notebook.sql.provenance import (
    CachePolicyError,
    compute_sql_provenance_hash,
    normalize_query,
    resolve_cache_policy,
)
from strata.notebook.sql.registry import get_adapter

if TYPE_CHECKING:
    from strata.notebook.models import ConnectionSpec
    from strata.notebook.session import NotebookSession
    from strata.notebook.sql.adapter import DriverAdapter

logger = logging.getLogger(__name__)


async def execute_sql_cell(
    session: NotebookSession,
    cell_id: str,
    source: str,
    *,
    use_cache: bool = True,
) -> dict[str, Any]:
    """Execute a SQL cell. Return shape mirrors ``execute_prompt_cell``.

    Returns a dict with ``success``, ``outputs``, ``stdout``,
    ``stderr``, ``error``, ``cache_hit``, ``duration_ms``,
    ``execution_method``, ``artifact_uri``. The notebook executor
    lifts these into a ``CellExecutionResult``.
    """
    start_time = time.time()

    # ---- annotations + connection resolution ----------------------
    annotations = parse_annotations(source)
    if annotations.sql is None or not annotations.sql.connection:
        return _error_result(
            "SQL cell is missing `# @sql connection=<name>`.",
            start_time,
        )
    spec = _find_connection(session, annotations.sql.connection)
    if spec is None:
        return _error_result(
            f"unknown connection {annotations.sql.connection!r}; "
            "declare it under [connections.<name>] in notebook.toml",
            start_time,
        )

    try:
        adapter = get_adapter(spec.driver)
    except KeyError as exc:
        return _error_result(str(exc), start_time)

    # ---- analyze ---------------------------------------------------
    analysis = analyze_sql_cell(source, dialect=adapter.sqlglot_dialect)
    if analysis.parse_error:
        return _error_result(f"SQL parse error: {analysis.parse_error}", start_time)
    if not analysis.sql_body:
        return _error_result("SQL cell body is empty.", start_time)

    # ---- bind params -----------------------------------------------
    namespace, upstream_input_hashes = _load_upstream_variables(
        session, cell_id, analysis.references
    )
    try:
        params = resolve_bind_params(analysis.placeholder_positions, namespace)
    except BindError as exc:
        return _error_result(str(exc), start_time)

    # ---- cache policy ----------------------------------------------
    try:
        policy = resolve_cache_policy(
            analysis.cache_policy,
            capabilities=adapter.capabilities,
            session_id=session.id,
        )
    except CachePolicyError as exc:
        return _error_result(str(exc), start_time)

    # ---- probes (optional) -----------------------------------------
    freshness = None
    schema_fp = None
    if policy.freshness_required or policy.schema_required:
        try:
            freshness, schema_fp = _run_probes(adapter, spec, analysis.tables, policy)
        except Exception as exc:  # noqa: BLE001
            return _error_result(f"probe failed: {exc}", start_time)
        if policy.snapshot_required and (freshness is None or not freshness.is_snapshot):
            return _error_result(
                "@cache snapshot requires a durable snapshot ID; "
                "the freshness probe returned an equality token instead",
                start_time,
            )

    # ---- provenance hash -------------------------------------------
    query_normalized = normalize_query(analysis.sql_body, adapter.sqlglot_dialect)
    connection_id = adapter.canonicalize_connection_id(spec)
    provenance_hash = compute_sql_provenance_hash(
        query_normalized=query_normalized,
        bind_params=params,
        connection_id=connection_id,
        upstream_input_hashes=upstream_input_hashes,
        cache_salt=policy.salt,
        freshness_token=freshness,
        schema_fingerprint=schema_fp,
    )
    output_name = analysis.name
    var_provenance = hashlib.sha256(f"{provenance_hash}:{output_name}".encode()).hexdigest()

    # ---- cache check -----------------------------------------------
    artifact_mgr = session.get_artifact_manager()
    notebook_id = session.notebook_state.id
    canonical_id = f"nb_{notebook_id}_cell_{cell_id}_var_{output_name}"

    if use_cache:
        cached = artifact_mgr.find_cached(var_provenance)
        if cached is not None:
            canonical = artifact_mgr.artifact_store.get_latest_version(canonical_id)
            if canonical is not None and canonical.provenance_hash == var_provenance:
                return _cache_hit_result(
                    artifact_mgr,
                    canonical,
                    output_name,
                    start_time,
                )

    # ---- execute query ---------------------------------------------
    try:
        table = _execute_query(adapter, spec, analysis, params)
    except Exception as exc:  # noqa: BLE001
        return _error_result(
            f"SQL execution failed: {_exception_message(exc)}",
            start_time,
        )

    blob = _serialize_arrow_ipc(table)
    artifact = artifact_mgr.store_cell_output(
        cell_id=cell_id,
        variable_name=output_name,
        blob_data=blob,
        content_type="arrow/ipc",
        provenance_hash=var_provenance,
        source_hash=provenance_hash,  # cell-level provenance for staleness
    )
    uri = f"strata://artifact/{artifact.id}@v={artifact.version}"

    duration_ms = (time.time() - start_time) * 1000
    display_output = _table_display(table)
    return {
        "success": True,
        "outputs": {
            output_name: {
                "content_type": "arrow/ipc",
                "bytes": len(blob),
                "preview": display_output["preview"],
            }
        },
        "display_outputs": [display_output],
        "display_output": display_output,
        "stdout": "",
        "stderr": "",
        "error": None,
        "cache_hit": False,
        "duration_ms": int(duration_ms),
        "execution_method": "sql",
        "artifact_uri": uri,
        "mutation_warnings": [],
    }


# --- helpers --------------------------------------------------------------


def _find_connection(session: NotebookSession, name: str) -> ConnectionSpec | None:
    for spec in session.notebook_state.connections:
        if spec.name == name:
            return spec
    return None


def _load_upstream_variables(
    session: NotebookSession,
    cell_id: str,
    references: list[str],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Load upstream variable values + per-variable artifact hashes.

    Returns (namespace, upstream_input_hashes). The hashes feed into
    the provenance hash so a change in any referenced variable's
    artifact invalidates this cell's cache.
    """
    namespace: dict[str, Any] = {}
    hashes: dict[str, str] = {}
    cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
    if cell is None:
        return namespace, hashes

    artifact_mgr = session.get_artifact_manager()
    notebook_id = session.notebook_state.id
    references_set = set(references)

    for upstream_id in cell.upstream_ids:
        upstream_cell = next(
            (c for c in session.notebook_state.cells if c.id == upstream_id),
            None,
        )
        if upstream_cell is None:
            continue
        for var_name in upstream_cell.defines:
            if var_name not in references_set:
                continue
            canonical_id = f"nb_{notebook_id}_cell_{upstream_id}_var_{var_name}"
            artifact = artifact_mgr.artifact_store.get_latest_version(canonical_id)
            if artifact is None:
                continue
            hashes[var_name] = artifact.provenance_hash
            blob = artifact_mgr.load_artifact_data(canonical_id, artifact.version)
            content_type = _content_type_of(artifact)
            namespace[var_name] = _deserialize_blob(blob, content_type)

    return namespace, hashes


def _content_type_of(artifact: Any) -> str:
    spec = getattr(artifact, "transform_spec", None)
    if not spec:
        return "json/object"
    try:
        parsed = json.loads(spec)
        return parsed.get("params", {}).get("content_type", "json/object")
    except (ValueError, KeyError):
        return "json/object"


def _deserialize_blob(blob: bytes, content_type: str) -> Any:
    """Pull just enough out of an artifact for SQL bind use.

    SQL binds want primitive values (int / str / bytes / etc.). The
    full ``serializer.deserialize`` round-trip handles every shape
    the notebook supports — for SQL we only need the scalar / Arrow
    paths, which simplifies the imports.
    """
    if content_type == "json/object":
        try:
            return json.loads(blob)
        except (ValueError, TypeError):
            return None
    if content_type == "arrow/ipc":
        # An upstream Arrow table can't be a SQL bind value; the
        # bind layer rejects it with a clear type error. We still
        # return a useful representation so the namespace is
        # populated and the user gets the right BindError.
        try:
            import pyarrow as pa

            reader = pa.ipc.open_stream(blob)
            return reader.read_all()
        except Exception:  # noqa: BLE001
            return blob
    if content_type == "pickle/object":
        import pickle

        try:
            return pickle.loads(blob)  # noqa: S301
        except Exception:  # noqa: BLE001
            return None
    return blob


def _run_probes(
    adapter: DriverAdapter,
    spec: ConnectionSpec,
    tables: list[Any],
    policy: Any,
) -> tuple[Any, Any]:
    """Run freshness + schema probes per the resolved policy.

    ``needs_separate_probe_conn=True`` (Postgres) gets its own
    connection because per-transaction stats are frozen inside the
    query connection's open transaction. Other adapters share the
    probe connection across both calls.
    """
    freshness = None
    schema_fp = None
    probe_conn = adapter.open(spec, read_only=True)
    try:
        if policy.freshness_required:
            freshness = adapter.probe_freshness(probe_conn, tables)
        if policy.schema_required:
            schema_fp = adapter.probe_schema(probe_conn, tables)
    finally:
        _safely_close(probe_conn)
    return freshness, schema_fp


def _execute_query(
    adapter: DriverAdapter,
    spec: ConnectionSpec,
    analysis: Any,
    params: tuple[Any, ...],
) -> Any:
    """Open a read-only connection, run the rewritten query, fetch Arrow."""
    rewritten = rewrite_named_to_positional(analysis.sql_body, adapter.sqlglot_dialect)
    conn = adapter.open(spec, read_only=True)
    try:
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(rewritten, parameters=params)
            else:
                cursor.execute(rewritten)
            return cursor.fetch_arrow_table()
        finally:
            _safely_close(cursor)
    finally:
        _safely_close(conn)


def _exception_message(exc: BaseException) -> str:
    """Walk the exception chain so ADBC's wrapped errors stay visible.

    ADBC's Python driver often surfaces a generic ``InternalError``
    or ``OperationalError`` whose top-level message is
    ``"INTERNAL: (unknown error)"`` while the actually-useful
    "Failed to finalize statement: attempt to write a readonly
    database" lives in a chained exception. Without walking the
    chain, the user sees a useless message.
    """
    parts: list[str] = []
    seen: set[str] = set()
    cur: BaseException | None = exc
    while cur is not None:
        msg = str(cur).strip()
        if msg and msg not in seen:
            seen.add(msg)
            parts.append(msg)
        cur = cur.__cause__ or cur.__context__
    return " | ".join(parts) or type(exc).__name__


def _safely_close(handle: Any) -> None:
    if handle is None:
        return
    try:
        handle.close()
    except Exception:  # noqa: BLE001
        logger.exception("error closing handle")


def _serialize_arrow_ipc(table: Any) -> bytes:
    import pyarrow as pa

    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _table_display(table: Any) -> dict[str, Any]:
    """Build a small markdown preview for the cell's display panel."""
    rows = table.num_rows
    cols = table.num_columns
    sample = min(rows, 5)
    head = table.slice(0, sample).to_pylist() if sample else []
    column_names = list(table.schema.names)

    preview_lines = [f"{rows} rows × {cols} cols"]
    if column_names:
        preview_lines.append("| " + " | ".join(column_names) + " |")
        preview_lines.append("| " + " | ".join("---" for _ in column_names) + " |")
        for row in head:
            preview_lines.append(
                "| " + " | ".join(str(row.get(c, "")) for c in column_names) + " |"
            )
        if rows > sample:
            preview_lines.append(f"… {rows - sample} more rows")
    preview = "\n".join(preview_lines)
    return {
        "content_type": "text/markdown",
        "preview": preview,
        "markdown_text": preview,
    }


def _cache_hit_result(
    artifact_mgr: Any,
    canonical: Any,
    output_name: str,
    start_time: float,
) -> dict[str, Any]:
    blob = artifact_mgr.load_artifact_data(canonical.id, canonical.version)
    import pyarrow as pa

    table = pa.ipc.open_stream(blob).read_all()
    duration_ms = (time.time() - start_time) * 1000
    display_output = _table_display(table)
    uri = f"strata://artifact/{canonical.id}@v={canonical.version}"
    return {
        "success": True,
        "outputs": {
            output_name: {
                "content_type": "arrow/ipc",
                "bytes": len(blob),
                "preview": display_output["preview"],
            }
        },
        "display_outputs": [display_output],
        "display_output": display_output,
        "stdout": "",
        "stderr": "",
        "error": None,
        "cache_hit": True,
        "duration_ms": int(duration_ms),
        "execution_method": "cached",
        "artifact_uri": uri,
        "mutation_warnings": [],
    }


def _error_result(message: str, start_time: float) -> dict[str, Any]:
    duration_ms = (time.time() - start_time) * 1000
    return {
        "success": False,
        "outputs": {},
        "display_outputs": [],
        "display_output": None,
        "stdout": "",
        "stderr": "",
        "error": message,
        "cache_hit": False,
        "duration_ms": int(duration_ms),
        "execution_method": "sql",
        "artifact_uri": None,
        "mutation_warnings": [],
    }
