"""Tests for the dialect-aware ``:name`` → positional rewriter."""

from __future__ import annotations

from strata.notebook.sql.analyzer import rewrite_named_to_positional


def test_rewrite_postgres_uses_dollar_numeric():
    sql = "SELECT * FROM t WHERE id = :user_id AND tenant = :tenant"
    out = rewrite_named_to_positional(sql, dialect="postgres")
    assert out == "SELECT * FROM t WHERE id = $1 AND tenant = $2"


def test_rewrite_sqlite_uses_qmark():
    sql = "SELECT * FROM t WHERE id = :user_id AND tenant = :tenant"
    out = rewrite_named_to_positional(sql, dialect="sqlite")
    assert out == "SELECT * FROM t WHERE id = ? AND tenant = ?"


def test_rewrite_unknown_dialect_falls_back_to_qmark():
    """ADBC drivers default to qmark; unknown dialects should match
    that default rather than guess at numeric form."""
    sql = "SELECT :a, :b"
    assert rewrite_named_to_positional(sql, dialect=None) == "SELECT ?, ?"
    assert rewrite_named_to_positional(sql, dialect="duckdb") == "SELECT ?, ?"


def test_rewrite_duplicates_emit_one_position_per_occurrence():
    """Each ``:foo`` occurrence becomes a fresh positional bind so
    the executor's bind tuple lines up position-for-position."""
    sql = "SELECT :foo + :foo + :bar"
    pg = rewrite_named_to_positional(sql, dialect="postgres")
    assert pg == "SELECT $1 + $2 + $3"
    lite = rewrite_named_to_positional(sql, dialect="sqlite")
    assert lite == "SELECT ? + ? + ?"


def test_rewrite_does_not_touch_strings_or_comments():
    """Crucial: ``:foo`` inside ``'literal :foo'`` or ``-- :foo``
    must not be rewritten. The bind layer would otherwise see a
    bogus parameter and the user's query would silently change
    semantics."""
    sql = "SELECT 'hello :foo', :real -- :ignored\nFROM t"
    out = rewrite_named_to_positional(sql, dialect="postgres")
    # Original string and comment unchanged; only :real becomes $1.
    assert "'hello :foo'" in out
    assert "-- :ignored" in out
    assert ":real" not in out
    assert "$1" in out


def test_rewrite_does_not_touch_postgres_dollar_quotes():
    """Codex-flagged territory: ``$$ ... $$`` and ``$tag$ ... $tag$``
    are dollar-quoted strings, not placeholders. Their bodies must
    survive untouched."""
    sql = "SELECT $$:foo$$, $body$:bar$body$, :real FROM t"
    out = rewrite_named_to_positional(sql, dialect="postgres")
    assert "$$:foo$$" in out
    assert "$body$:bar$body$" in out
    assert ":real" not in out


def test_rewrite_skips_postgres_cast_operator():
    """``::int`` is the cast operator; the leading ``:`` is part of
    a larger token and shouldn't trigger a placeholder rewrite."""
    sql = "SELECT id::int, value::text, :user_id FROM t"
    out = rewrite_named_to_positional(sql, dialect="postgres")
    assert "id::int" in out
    assert "value::text" in out
    assert "$1" in out


def test_rewrite_no_placeholders_returns_input_unchanged():
    sql = "SELECT 1 FROM t"
    assert rewrite_named_to_positional(sql, dialect="postgres") == sql
    assert rewrite_named_to_positional(sql, dialect="sqlite") == sql


def test_rewrite_preserves_whitespace_and_punctuation():
    """The rewriter is byte-exact outside placeholder positions —
    important so query-plan caches at the backend stay warm across
    Strata runs."""
    sql = "SELECT\n  :a,\n  :b\nFROM t\nWHERE x = :a"
    out = rewrite_named_to_positional(sql, dialect="postgres")
    assert out == "SELECT\n  $1,\n  $2\nFROM t\nWHERE x = $3"
