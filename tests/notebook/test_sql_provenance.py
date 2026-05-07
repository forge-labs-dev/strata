"""Tests for SQL provenance hashing and ``# @cache`` policy resolution."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from uuid import UUID

import pytest

from strata.notebook.annotations import CachePolicy
from strata.notebook.sql.adapter import (
    AdapterCapabilities,
    FreshnessToken,
    SchemaFingerprint,
)
from strata.notebook.sql.provenance import (
    CachePolicyError,
    ResolvedCachePolicy,
    compute_sql_provenance_hash,
    normalize_query,
    resolve_cache_policy,
    serialize_bind_params,
)

# --- shared fixtures ------------------------------------------------------


_FULL_CAPS = AdapterCapabilities(
    per_table_freshness=True,
    supports_snapshot=True,
    needs_separate_probe_conn=False,
)

_NO_SNAPSHOT_CAPS = AdapterCapabilities(
    per_table_freshness=True,
    supports_snapshot=False,
    needs_separate_probe_conn=True,
)


def _hash_inputs(**overrides):
    """Default kwargs for compute_sql_provenance_hash; tests override per-case."""
    base = dict(
        query_normalized="SELECT 1",
        bind_params=(),
        connection_id="conn-id-aaaa",
        upstream_input_hashes={},
        source_hash="source-hash-aaaa",
        cache_salt=b"strata.cache.fingerprint",
        freshness_token=None,
        schema_fingerprint=None,
    )
    base.update(overrides)
    return base


# --- resolve_cache_policy -------------------------------------------------


def test_resolve_fingerprint_default():
    """Default policy (no ``# @cache``) is fingerprint — probes
    required, snapshot not."""
    out = resolve_cache_policy(
        CachePolicy(kind="fingerprint"),
        capabilities=_FULL_CAPS,
        session_id="s1",
    )
    assert out.kind == "fingerprint"
    assert out.freshness_required is True
    assert out.schema_required is True
    assert out.snapshot_required is False
    assert out.salt == b"strata.cache.fingerprint"


def test_resolve_forever_skips_probes():
    """``forever`` is the user asserting "this is reference data";
    no DB-side state factors into the hash, so no probe is needed."""
    out = resolve_cache_policy(
        CachePolicy(kind="forever"),
        capabilities=_FULL_CAPS,
        session_id="s1",
    )
    assert out.freshness_required is False
    assert out.schema_required is False
    assert out.snapshot_required is False
    assert out.salt == b"strata.cache.forever"


def test_resolve_session_includes_session_id():
    """Different sessions → different salt → different hash. Same
    session_id twice → same salt."""
    a = resolve_cache_policy(
        CachePolicy(kind="session"), capabilities=_FULL_CAPS, session_id="alpha"
    )
    b = resolve_cache_policy(
        CachePolicy(kind="session"), capabilities=_FULL_CAPS, session_id="beta"
    )
    a2 = resolve_cache_policy(
        CachePolicy(kind="session"), capabilities=_FULL_CAPS, session_id="alpha"
    )
    assert a.salt != b.salt
    assert a.salt == a2.salt
    assert a.freshness_required is False


def test_resolve_ttl_buckets_clock_into_windows():
    """Two ``now`` values in the same TTL bucket → same salt; values
    in different buckets → different salt. Bucket boundary for
    ttl=300 is at multiples of 300 — 900..1199 fall in bucket 3,
    1200 starts bucket 4."""
    p = CachePolicy(kind="ttl", ttl_seconds=300)
    same1 = resolve_cache_policy(p, capabilities=_FULL_CAPS, session_id="s", now=900.0)
    same2 = resolve_cache_policy(p, capabilities=_FULL_CAPS, session_id="s", now=1199.0)
    diff = resolve_cache_policy(p, capabilities=_FULL_CAPS, session_id="s", now=1200.0)
    assert same1.salt == same2.salt
    assert same1.salt != diff.salt
    assert same1.freshness_required is False


def test_resolve_ttl_zero_or_negative_raises():
    """Belt-and-suspenders: the parser already rejects these, but
    a directly-constructed policy shouldn't slip through and
    silently produce a divide-or-bucket-collapse hash."""
    with pytest.raises(CachePolicyError, match="positive"):
        resolve_cache_policy(
            CachePolicy(kind="ttl", ttl_seconds=0),
            capabilities=_FULL_CAPS,
            session_id="s",
        )
    with pytest.raises(CachePolicyError, match="positive"):
        resolve_cache_policy(
            CachePolicy(kind="ttl", ttl_seconds=-1),
            capabilities=_FULL_CAPS,
            session_id="s",
        )
    with pytest.raises(CachePolicyError, match="positive"):
        resolve_cache_policy(
            CachePolicy(kind="ttl", ttl_seconds=None),
            capabilities=_FULL_CAPS,
            session_id="s",
        )


def test_resolve_snapshot_requires_capability():
    """``# @cache snapshot`` against a driver that can't return a
    durable snapshot ID is a static error — fail before the executor
    burns a probe."""
    with pytest.raises(CachePolicyError, match="snapshot"):
        resolve_cache_policy(
            CachePolicy(kind="snapshot"),
            capabilities=_NO_SNAPSHOT_CAPS,
            session_id="s",
        )


def test_resolve_snapshot_with_capable_driver():
    out = resolve_cache_policy(
        CachePolicy(kind="snapshot"),
        capabilities=_FULL_CAPS,
        session_id="s",
    )
    assert out.freshness_required is True
    assert out.schema_required is True
    assert out.snapshot_required is True


def test_resolve_unknown_kind_raises():
    with pytest.raises(CachePolicyError, match="unknown"):
        resolve_cache_policy(
            CachePolicy(kind="bogus"),
            capabilities=_FULL_CAPS,
            session_id="s",
        )


def test_resolve_returns_frozen_dataclass():
    """Frozen so callers can't mutate the result and accidentally
    flip ``freshness_required`` after the fact."""
    out = resolve_cache_policy(
        CachePolicy(kind="fingerprint"),
        capabilities=_FULL_CAPS,
        session_id="s",
    )
    assert isinstance(out, ResolvedCachePolicy)
    with pytest.raises((AttributeError, Exception)):
        out.kind = "session"  # type: ignore[misc]


# --- normalize_query ------------------------------------------------------


def test_normalize_is_whitespace_insensitive():
    a = normalize_query("SELECT 1", dialect="postgres")
    b = normalize_query("SELECT  1", dialect="postgres")
    c = normalize_query("SELECT\n1", dialect="postgres")
    assert a == b == c


def test_normalize_is_keyword_case_insensitive():
    a = normalize_query("SELECT 1 FROM t", dialect="postgres")
    b = normalize_query("select 1 from t", dialect="postgres")
    assert a == b


def test_normalize_strips_comments():
    """Inline / block comments don't change semantics and shouldn't
    invalidate the cache."""
    a = normalize_query("SELECT 1 -- doc\nFROM t /* note */", dialect="postgres")
    b = normalize_query("SELECT 1 FROM t", dialect="postgres")
    assert a == b


def test_normalize_handles_multi_statement():
    """``sqlglot.parse`` returns a list; we join with ``;\\n`` so a
    multi-statement body still gets a deterministic canonical form."""
    out = normalize_query("SELECT 1; SELECT 2", dialect="postgres")
    assert "SELECT" in out
    # Two statements → at least one ``;`` separator in the output.
    assert ";" in out


def test_normalize_returns_stripped_input_on_parse_failure():
    """Parse failures still need a stable string for unit tests
    that lift the hash without a real adapter."""
    out = normalize_query("SELECT * FROM", dialect="postgres")
    assert out == "SELECT * FROM"


def test_normalize_empty_input_returns_empty():
    assert normalize_query("", dialect="postgres") == ""
    assert normalize_query("   \n  ", dialect="postgres") == ""


# --- serialize_bind_params -----------------------------------------------


def test_serialize_tags_each_value_with_its_concrete_type():
    """Type tags catch the bool-vs-int gotcha: ``True`` and ``1``
    serialize to the same JSON without a tag, but they're different
    inputs to a SQL parameter binding."""
    out = serialize_bind_params([True, 1])
    assert out == [["bool", True], ["int", 1]]


def test_serialize_floats_use_repr_for_cross_platform_stability():
    """``json.dumps(0.1)`` is stable on CPython but ``repr(x)`` is
    the canonical round-trippable form — denormals and NaN survive
    ``repr`` even when JSON would lose them."""
    out = serialize_bind_params([0.1, 1.5])
    assert out == [["float", "0.1"], ["float", "1.5"]]


def test_serialize_bytes_is_base64():
    """JSON can't carry raw bytes; base64 keeps the bytes intact
    through the JSON encode."""
    out = serialize_bind_params([b"\x00\x01\x02"])
    assert out == [["bytes", "AAEC"]]


def test_serialize_decimal_preserves_precision():
    """Decimal('1.10') ≠ Decimal('1.1') — different scale, same
    numeric value. ``str()`` keeps the precision."""
    out = serialize_bind_params([Decimal("1.10"), Decimal("1.1")])
    assert out[0][1] == "1.10"
    assert out[1][1] == "1.1"
    assert out[0] != out[1]


def test_serialize_uuid_canonical_string():
    u = UUID("12345678-1234-5678-1234-567812345678")
    out = serialize_bind_params([u])
    assert out == [["uuid", "12345678-1234-5678-1234-567812345678"]]


def test_serialize_naive_vs_aware_datetime_distinct():
    """Adding a tz changes wall-clock semantics, so the hash must
    differ between naive and UTC-aware datetimes that look "the
    same"."""
    naive = dt.datetime(2026, 5, 6, 12, 0, 0)
    aware = dt.datetime(2026, 5, 6, 12, 0, 0, tzinfo=dt.UTC)
    out = serialize_bind_params([naive, aware])
    assert out[0] != out[1]


def test_serialize_date_and_time_separate_tags():
    out = serialize_bind_params([dt.date(2026, 5, 6), dt.time(12, 30)])
    assert out[0][0] == "date"
    assert out[1][0] == "time"


def test_serialize_none_is_tagged():
    out = serialize_bind_params([None])
    assert out == [["none", None]]


def test_serialize_unsupported_type_raises():
    """``coerce_bind_value`` is the gate; if a caller bypasses it
    we fail loudly rather than silently producing an unstable
    hash via ``str()``."""
    with pytest.raises(ValueError, match="cannot serialize"):
        serialize_bind_params([[1, 2, 3]])


# --- compute_sql_provenance_hash -----------------------------------------


def test_hash_is_deterministic():
    """Same inputs twice → same hex digest. The contract every
    cache lookup depends on."""
    a = compute_sql_provenance_hash(**_hash_inputs())
    b = compute_sql_provenance_hash(**_hash_inputs())
    assert a == b
    # SHA-256 → 64 hex chars.
    assert len(a) == 64


def test_hash_responds_to_query_change():
    a = compute_sql_provenance_hash(**_hash_inputs(query_normalized="SELECT 1"))
    b = compute_sql_provenance_hash(**_hash_inputs(query_normalized="SELECT 2"))
    assert a != b


def test_hash_responds_to_bind_param_change():
    a = compute_sql_provenance_hash(**_hash_inputs(bind_params=(1,)))
    b = compute_sql_provenance_hash(**_hash_inputs(bind_params=(2,)))
    assert a != b


def test_hash_distinguishes_bool_and_int_binds():
    """The whole reason ``serialize_bind_params`` type-tags values:
    ``True`` and ``1`` are equal in Python but different SQL inputs."""
    a = compute_sql_provenance_hash(**_hash_inputs(bind_params=(True,)))
    b = compute_sql_provenance_hash(**_hash_inputs(bind_params=(1,)))
    assert a != b


def test_hash_responds_to_bind_param_order():
    a = compute_sql_provenance_hash(**_hash_inputs(bind_params=(1, 2)))
    b = compute_sql_provenance_hash(**_hash_inputs(bind_params=(2, 1)))
    assert a != b


def test_hash_responds_to_connection_id_change():
    a = compute_sql_provenance_hash(**_hash_inputs(connection_id="conn-a"))
    b = compute_sql_provenance_hash(**_hash_inputs(connection_id="conn-b"))
    assert a != b


def test_hash_upstream_inputs_dict_is_order_insensitive():
    """The upstream-inputs dict is sorted before hashing so caller
    ordering doesn't churn cache identity."""
    a = compute_sql_provenance_hash(**_hash_inputs(upstream_input_hashes={"a": "h1", "b": "h2"}))
    b = compute_sql_provenance_hash(**_hash_inputs(upstream_input_hashes={"b": "h2", "a": "h1"}))
    assert a == b


def test_hash_upstream_inputs_change_invalidates():
    a = compute_sql_provenance_hash(**_hash_inputs(upstream_input_hashes={"x": "v1"}))
    b = compute_sql_provenance_hash(**_hash_inputs(upstream_input_hashes={"x": "v2"}))
    assert a != b


def test_hash_responds_to_source_hash_change():
    """Source edits that aren't in the SQL body itself (e.g.
    AST-normalized whitespace edits to surrounding annotations)
    are caught by the source_hash slot — distinct from
    query_normalized which only sees the SQL body."""
    a = compute_sql_provenance_hash(**_hash_inputs(source_hash="src-a"))
    b = compute_sql_provenance_hash(**_hash_inputs(source_hash="src-b"))
    assert a != b


def test_hash_responds_to_cache_salt_change():
    a = compute_sql_provenance_hash(**_hash_inputs(cache_salt=b"salt-a"))
    b = compute_sql_provenance_hash(**_hash_inputs(cache_salt=b"salt-b"))
    assert a != b


def test_hash_freshness_token_change_invalidates():
    a = compute_sql_provenance_hash(**_hash_inputs(freshness_token=FreshnessToken(value=b"\x01")))
    b = compute_sql_provenance_hash(**_hash_inputs(freshness_token=FreshnessToken(value=b"\x02")))
    assert a != b


def test_hash_no_freshness_vs_zero_byte_freshness_distinct():
    """``None`` (policy didn't probe) and ``b""`` (policy probed,
    backend returned zero-byte token) are semantically different
    — must produce different hashes."""
    a = compute_sql_provenance_hash(**_hash_inputs(freshness_token=None))
    b = compute_sql_provenance_hash(**_hash_inputs(freshness_token=FreshnessToken(value=b"")))
    assert a != b


def test_hash_session_only_flag_invalidates():
    """A session-only token *was* in the hash — its session-only
    nature is part of the cell's identity, not just a UI hint."""
    plain = FreshnessToken(value=b"\x42")
    session_only = FreshnessToken(value=b"\x42", is_session_only=True)
    a = compute_sql_provenance_hash(**_hash_inputs(freshness_token=plain))
    b = compute_sql_provenance_hash(**_hash_inputs(freshness_token=session_only))
    assert a != b


def test_hash_snapshot_flag_invalidates():
    plain = FreshnessToken(value=b"\x99")
    snapshot = FreshnessToken(value=b"\x99", is_snapshot=True)
    a = compute_sql_provenance_hash(**_hash_inputs(freshness_token=plain))
    b = compute_sql_provenance_hash(**_hash_inputs(freshness_token=snapshot))
    assert a != b


def test_hash_schema_fingerprint_change_invalidates():
    a = compute_sql_provenance_hash(
        **_hash_inputs(schema_fingerprint=SchemaFingerprint(value=b"\x01"))
    )
    b = compute_sql_provenance_hash(
        **_hash_inputs(schema_fingerprint=SchemaFingerprint(value=b"\x02"))
    )
    assert a != b


def test_hash_no_schema_vs_zero_byte_schema_distinct():
    a = compute_sql_provenance_hash(**_hash_inputs(schema_fingerprint=None))
    b = compute_sql_provenance_hash(**_hash_inputs(schema_fingerprint=SchemaFingerprint(value=b"")))
    assert a != b


# --- end-to-end policy → hash ---------------------------------------------


def test_fingerprint_policy_with_probe_token_in_hash():
    """End-to-end exercise: the fingerprint policy yields the
    expected salt and the executor folds in the probe results."""
    policy = resolve_cache_policy(
        CachePolicy(kind="fingerprint"),
        capabilities=_FULL_CAPS,
        session_id="s1",
    )
    a = compute_sql_provenance_hash(
        **_hash_inputs(
            cache_salt=policy.salt,
            freshness_token=FreshnessToken(value=b"v1"),
            schema_fingerprint=SchemaFingerprint(value=b"sch1"),
        )
    )
    b = compute_sql_provenance_hash(
        **_hash_inputs(
            cache_salt=policy.salt,
            freshness_token=FreshnessToken(value=b"v2"),  # data changed
            schema_fingerprint=SchemaFingerprint(value=b"sch1"),
        )
    )
    assert a != b


def test_forever_policy_unaffected_by_freshness_token():
    """``forever`` doesn't fold a freshness token (the executor
    skips the probe), so two cells with the same other inputs
    produce the same hash regardless of DB-side state."""
    policy = resolve_cache_policy(
        CachePolicy(kind="forever"),
        capabilities=_FULL_CAPS,
        session_id="s1",
    )
    a = compute_sql_provenance_hash(**_hash_inputs(cache_salt=policy.salt, freshness_token=None))
    b = compute_sql_provenance_hash(**_hash_inputs(cache_salt=policy.salt, freshness_token=None))
    assert a == b


def test_session_policy_invalidates_across_sessions():
    a_pol = resolve_cache_policy(
        CachePolicy(kind="session"), capabilities=_FULL_CAPS, session_id="alpha"
    )
    b_pol = resolve_cache_policy(
        CachePolicy(kind="session"), capabilities=_FULL_CAPS, session_id="beta"
    )
    a = compute_sql_provenance_hash(**_hash_inputs(cache_salt=a_pol.salt))
    b = compute_sql_provenance_hash(**_hash_inputs(cache_salt=b_pol.salt))
    assert a != b
