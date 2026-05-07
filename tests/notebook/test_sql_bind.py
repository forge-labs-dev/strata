"""Tests for SQL bind-parameter coercion and resolution."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from uuid import UUID

import pytest

from strata.notebook.sql.bind import (
    BindError,
    coerce_bind_value,
    resolve_bind_params,
)

# --- accepted types -------------------------------------------------------


def test_coerce_none_passes_through():
    assert coerce_bind_value("x", None) is None


def test_coerce_bool_preserves_type_identity():
    """``True`` must come back as ``bool``, not as ``int``. Order in
    the accept list (bool before int) is irrelevant under ``type() is``
    semantics — what matters is that we don't auto-cast. Provenance
    hashing in slice 8 will treat ``bool`` and ``int`` as distinct,
    so widening here would silently change cache identity."""
    out = coerce_bind_value("flag", True)
    assert out is True
    assert type(out) is bool


def test_coerce_int_passes_through():
    assert coerce_bind_value("n", 42) == 42
    assert coerce_bind_value("n", -7) == -7
    assert coerce_bind_value("n", 0) == 0


def test_coerce_float_passes_through():
    assert coerce_bind_value("x", 3.14) == 3.14
    assert coerce_bind_value("x", -1.5) == -1.5
    assert coerce_bind_value("x", 0.0) == 0.0


def test_coerce_str_passes_through():
    assert coerce_bind_value("s", "") == ""
    assert coerce_bind_value("s", "hello") == "hello"


def test_coerce_str_with_sql_injection_attempt_is_accepted_unchanged():
    """The whole point: adversarial strings round-trip as plain bind
    values. ADBC's parameter binding handles escaping at the wire
    layer, so we accept any string and document that we do *not*
    string-substitute. If this ever fails, our injection-defense
    story is broken."""
    payload = "'; DROP TABLE users; --"
    assert coerce_bind_value("name", payload) == payload


def test_coerce_bytes_passes_through():
    assert coerce_bind_value("b", b"") == b""
    assert coerce_bind_value("b", b"\x00\x01\x02") == b"\x00\x01\x02"


def test_coerce_bytearray_is_normalized_to_bytes():
    """``bytearray`` is mutable; ADBC doesn't universally accept it
    and a downstream hash needs an immutable byte-sequence anyway."""
    out = coerce_bind_value("b", bytearray(b"data"))
    assert out == b"data"
    assert type(out) is bytes


def test_coerce_decimal_passes_through():
    d = Decimal("123.456")
    assert coerce_bind_value("amt", d) == d
    assert type(coerce_bind_value("amt", d)) is Decimal


def test_coerce_uuid_passes_through():
    u = UUID("12345678-1234-5678-1234-567812345678")
    assert coerce_bind_value("id", u) == u
    assert type(coerce_bind_value("id", u)) is UUID


def test_coerce_datetime_naive_passes_through():
    naive = dt.datetime(2026, 5, 6, 12, 30, 45)
    assert coerce_bind_value("ts", naive) == naive


def test_coerce_datetime_aware_passes_through():
    aware = dt.datetime(2026, 5, 6, 12, 30, 45, tzinfo=dt.UTC)
    assert coerce_bind_value("ts", aware) == aware
    assert coerce_bind_value("ts", aware).tzinfo is dt.UTC


def test_coerce_date_passes_through():
    d = dt.date(2026, 5, 6)
    assert coerce_bind_value("d", d) == d
    assert type(coerce_bind_value("d", d)) is dt.date


def test_coerce_time_passes_through():
    t = dt.time(12, 30, 45)
    assert coerce_bind_value("t", t) == t


# --- rejected types -------------------------------------------------------


def test_coerce_rejects_list():
    with pytest.raises(BindError, match=r":xs.*list"):
        coerce_bind_value("xs", [1, 2, 3])


def test_coerce_rejects_tuple():
    with pytest.raises(BindError, match=r":xs.*tuple"):
        coerce_bind_value("xs", (1, 2, 3))


def test_coerce_rejects_dict():
    with pytest.raises(BindError, match=r":m.*dict"):
        coerce_bind_value("m", {"a": 1})


def test_coerce_rejects_set():
    with pytest.raises(BindError, match=r":s.*set"):
        coerce_bind_value("s", {1, 2, 3})


def test_coerce_rejects_complex():
    """``complex`` isn't generally bindable in SQL — and silently
    coercing to float would lose the imaginary component."""
    with pytest.raises(BindError, match=r":x.*complex"):
        coerce_bind_value("x", complex(1, 2))


def test_coerce_rejects_arbitrary_object():
    class Custom:
        pass

    with pytest.raises(BindError, match=r":obj.*Custom"):
        coerce_bind_value("obj", Custom())


def test_coerce_rejects_callable():
    """Catches the easy mistake of writing ``:foo`` when there's a
    function ``foo`` in scope."""

    def fn():
        return 42

    with pytest.raises(BindError, match=r":fn"):
        coerce_bind_value("fn", fn)


def test_coerce_error_lists_accepted_types():
    """The error message must enumerate the accepted types so users
    can fix the upstream cell without consulting docs."""
    with pytest.raises(BindError) as exc:
        coerce_bind_value("x", [1, 2])
    msg = str(exc.value)
    for tp in ("None", "bool", "int", "float", "str", "bytes", "Decimal", "UUID"):
        assert tp in msg, f"{tp} missing from error: {msg}"


def test_coerce_rejects_int_subclass():
    """``isinstance``-based acceptance would silently widen a
    user-defined ``MyInt`` (or numpy.int64) into ``int``. Strict
    ``type() is`` semantics rejects them so the user converts
    explicitly — which makes overflow/nullability assumptions
    visible in the cell."""

    class MyInt(int):
        pass

    with pytest.raises(BindError, match=r":n.*MyInt"):
        coerce_bind_value("n", MyInt(5))


def test_coerce_rejects_bytearray_subclass():
    """Codex review fix: the ``bytearray`` shortcut used ``isinstance``
    which silently widened user-subclassed ``bytearray`` types
    against the rest of the contract. ``type() is`` semantics now
    rejects subclasses everywhere, including the bytearray path."""

    class TaggedBytes(bytearray):
        pass

    with pytest.raises(BindError, match=r":b.*TaggedBytes"):
        coerce_bind_value("b", TaggedBytes(b"data"))


def test_coerce_rejects_datetime_subclass():
    """Same strictness rationale for ``datetime``: a
    ``pandas.Timestamp`` is a ``datetime`` subclass with different
    nullability semantics (``NaT``). User converts via
    ``.to_pydatetime()``; we don't paper over it."""

    class FancyDateTime(dt.datetime):
        pass

    fdt = FancyDateTime(2026, 5, 6)
    with pytest.raises(BindError, match=r":ts.*FancyDateTime"):
        coerce_bind_value("ts", fdt)


def test_coerce_rejects_numpy_scalars_when_available():
    """numpy.int64 / numpy.float64 are not Python ints/floats and
    have surprising overflow behavior at SQL boundaries. Reject so
    the user converts via ``int(x)`` / ``float(x)`` and the intent
    is visible."""
    np = pytest.importorskip("numpy")

    with pytest.raises(BindError, match=r":n.*int64"):
        coerce_bind_value("n", np.int64(42))
    with pytest.raises(BindError, match=r":x.*float64"):
        coerce_bind_value("x", np.float64(3.14))


# --- resolve_bind_params --------------------------------------------------


def test_resolve_empty_placeholders_returns_empty_tuple():
    assert resolve_bind_params([], {}) == ()


def test_resolve_single_placeholder():
    out = resolve_bind_params(["user_id"], {"user_id": 42})
    assert out == (42,)


def test_resolve_preserves_placeholder_order():
    out = resolve_bind_params(
        ["second", "first", "third"],
        {"first": 1, "second": 2, "third": 3},
    )
    assert out == (2, 1, 3)


def test_resolve_ignores_unreferenced_namespace_entries():
    """The namespace can carry every upstream variable; we only pull
    the placeholders the SQL body actually references."""
    out = resolve_bind_params(
        ["wanted"],
        {"wanted": "ok", "ignored": object()},
    )
    assert out == ("ok",)


def test_resolve_missing_name_raises_bind_error():
    with pytest.raises(BindError, match=r":missing.*not found"):
        resolve_bind_params(["missing"], {"present": 1})


def test_resolve_unsupported_type_raises_bind_error():
    """Resolution coerces every value through ``coerce_bind_value``,
    so the type-rejection error surfaces here too."""
    with pytest.raises(BindError, match=r":xs.*list"):
        resolve_bind_params(["xs"], {"xs": [1, 2]})


def test_resolve_short_circuits_on_first_failure():
    """``resolve_bind_params`` doesn't accumulate diagnostics — the
    analyzer's validation pass surfaces every missing reference up
    front, and at execute time we fail fast."""
    namespace = {"good": 1}
    with pytest.raises(BindError, match=r":bad.*not found"):
        # ``later_bad`` would also fail (unsupported type), but we
        # never reach it.
        resolve_bind_params(["good", "bad", "later_bad"], namespace)


def test_resolve_handles_duplicate_placeholder_names():
    """The analyzer dedupes ``references`` for the DAG, but exposes a
    parallel ``placeholder_positions`` list with duplicates kept —
    that's what the executor passes here when the SQL body repeats
    ``:foo`` (e.g. ``SELECT :foo + :foo``). The integration test
    below pipes the analyzer through to confirm the contract holds
    end-to-end; this test pins the local behavior."""
    out = resolve_bind_params(["foo", "bar", "foo"], {"foo": 1, "bar": 2})
    assert out == (1, 2, 1)


def test_resolve_consumes_analyzer_placeholder_positions_for_duplicates():
    """Codex review fix: ``resolve_bind_params``'s duplicate
    semantics were unreachable — the analyzer's ``references`` is
    deduplicated. The fix added ``SqlAnalysis.placeholder_positions``
    that preserves duplicates; this test exercises the wire-up so
    the duplicate behavior is actually reachable from real callers."""
    from strata.notebook.sql.analyzer import analyze_sql_cell

    analysis = analyze_sql_cell(
        "# @sql connection=db\nSELECT :foo + :foo AS doubled, :bar AS single",
    )
    # DAG view is deduped.
    assert analysis.references == ["foo", "bar"]
    # Executor view keeps every occurrence in source order.
    assert analysis.placeholder_positions == ["foo", "foo", "bar"]

    out = resolve_bind_params(
        analysis.placeholder_positions,
        {"foo": 7, "bar": 99},
    )
    assert out == (7, 7, 99)


def test_resolve_passes_none_through_for_null_binds():
    """A ``None`` upstream variable binds as SQL NULL — must not be
    confused with "missing name"."""
    out = resolve_bind_params(["maybe"], {"maybe": None})
    assert out == (None,)


def test_resolve_returns_tuple_not_list():
    """The executor downstream expects a tuple (immutable, hashable).
    A list would still work for ADBC.execute, but we want the
    immutability for safety when this value is captured by the
    provenance hash in slice 8."""
    out = resolve_bind_params(["x"], {"x": 1})
    assert isinstance(out, tuple)
