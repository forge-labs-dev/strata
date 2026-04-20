"""Tests for prompt-cell execution helpers.

Full end-to-end execution is covered by the LLM integration suite;
these tests focus on the pure-function pieces that drive caching and
provider-aware request shaping.
"""

from __future__ import annotations

from strata.notebook.prompt_executor import compute_prompt_provenance_hash

_BASE_ARGS = {
    "rendered": "Summarize this dataset",
    "model": "gpt-5.4",
    "temperature": 0.0,
    "system_prompt": None,
    "output_type": "json",
}


def test_provenance_hash_is_stable():
    first = compute_prompt_provenance_hash(**_BASE_ARGS, output_schema=None)
    second = compute_prompt_provenance_hash(**_BASE_ARGS, output_schema=None)
    assert first == second


def test_schema_change_invalidates_cache():
    """Editing @output_schema must change the provenance hash so the
    executor doesn't hand back an answer shaped like the old schema."""
    a = compute_prompt_provenance_hash(
        **_BASE_ARGS,
        output_schema={"type": "object", "properties": {"score": {"type": "number"}}},
    )
    b = compute_prompt_provenance_hash(
        **_BASE_ARGS,
        output_schema={"type": "object", "properties": {"label": {"type": "string"}}},
    )
    assert a != b


def test_schema_key_order_does_not_affect_hash():
    """Two dicts with the same contents but different insertion order
    must hash the same — the fingerprint uses sorted keys."""
    a = compute_prompt_provenance_hash(
        **_BASE_ARGS,
        output_schema={
            "type": "object",
            "required": ["x"],
            "properties": {"x": {"type": "integer"}},
        },
    )
    b = compute_prompt_provenance_hash(
        **_BASE_ARGS,
        output_schema={
            "properties": {"x": {"type": "integer"}},
            "required": ["x"],
            "type": "object",
        },
    )
    assert a == b


def test_adding_schema_changes_hash():
    """A prior-run without a schema must not hit the cache for a
    subsequent run that adds one."""
    without = compute_prompt_provenance_hash(**_BASE_ARGS, output_schema=None)
    with_schema = compute_prompt_provenance_hash(
        **_BASE_ARGS,
        output_schema={"type": "object"},
    )
    assert without != with_schema
