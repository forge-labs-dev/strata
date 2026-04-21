"""Tests for prompt-cell execution helpers.

Full end-to-end execution is covered by the LLM integration suite;
these tests focus on the pure-function pieces that drive caching,
provider-aware request shaping, and the validate-and-retry loop.
"""

from __future__ import annotations

from unittest import mock

import pytest

from strata.notebook.prompt_executor import (
    _format_retry_prompt,
    _validation_errors,
    compute_prompt_provenance_hash,
)

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


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


_SCHEMA = {
    "type": "object",
    "properties": {
        "sentiment": {"type": "string", "enum": ["positive", "negative"]},
        "score": {"type": "number"},
    },
    "required": ["sentiment", "score"],
    "additionalProperties": False,
}


class TestValidationErrors:
    def test_valid_payload_returns_empty(self):
        content = '{"sentiment": "positive", "score": 0.8}'
        assert _validation_errors(content, _SCHEMA) == []

    def test_invalid_json_returns_single_parse_error(self):
        errors = _validation_errors("not json", _SCHEMA)
        assert len(errors) == 1
        assert "not valid JSON" in errors[0]

    def test_schema_violations_are_path_addressed(self):
        content = '{"sentiment": "ecstatic", "score": "high"}'
        errors = _validation_errors(content, _SCHEMA)
        # We expect one error per violation, each beginning with a
        # JSON Pointer path so downstream feedback can reference the
        # exact location the model got wrong.
        joined = "\n".join(errors)
        assert "/sentiment" in joined
        assert "/score" in joined

    def test_missing_required_property_reported_at_root(self):
        errors = _validation_errors('{"sentiment": "positive"}', _SCHEMA)
        assert any("(root)" in e and "score" in e for e in errors)


class TestRetryPromptFormat:
    def test_contains_each_error_bullet(self):
        prompt = _format_retry_prompt(["/a: nope", "/b: also nope"])
        assert "- /a: nope" in prompt
        assert "- /b: also nope" in prompt
        assert "Return a corrected JSON object" in prompt


# ---------------------------------------------------------------------------
# Retry loop integration (through execute_prompt_cell)
# ---------------------------------------------------------------------------


def _prompt_session(tmp_path, source: str, *, cell_id: str = "p1"):
    """Spin up a minimal session with one prompt cell."""
    from strata.notebook.parser import parse_notebook
    from strata.notebook.session import NotebookSession
    from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

    nb_dir = create_notebook(tmp_path, "validate_test")
    add_cell_to_notebook(nb_dir, cell_id, language="prompt")
    write_cell(nb_dir, cell_id, source)
    return NotebookSession(parse_notebook(nb_dir), nb_dir)


def _fake_llm_returning(*responses: str):
    """Return an async fake that yields each response in turn."""
    from strata.notebook.llm import LlmCompletionResult

    it = iter(responses)
    calls: list[dict] = []

    async def fake(config, messages, *, temperature=None, output_type=None, output_schema=None):
        content = next(it)
        calls.append({"messages": [dict(m) for m in messages], "content": content})
        return LlmCompletionResult(
            content=content,
            model="test-model",
            input_tokens=5,
            output_tokens=3,
        )

    return fake, calls


_TINY_SCHEMA = (
    '{"type": "object", "properties": {"n": {"type": "integer"}},'
    ' "required": ["n"], "additionalProperties": false}'
)


@pytest.mark.asyncio
async def test_first_try_passes_no_retries_no_feedback(tmp_path):
    """Clean first response → zero retries, zero follow-up messages."""
    from strata.notebook.llm import LlmConfig
    from strata.notebook.prompt_executor import execute_prompt_cell

    session = _prompt_session(tmp_path, f"# @output_schema {_TINY_SCHEMA}\nGive me a number.")
    fake, calls = _fake_llm_returning('{"n": 7}')
    cfg = LlmConfig(base_url="https://api.openai.com/v1", api_key="sk", model="m")

    with mock.patch("strata.notebook.prompt_executor.chat_completion", fake):
        result = await execute_prompt_cell(
            session, "p1", session.notebook_state.cells[0].source, cfg
        )

    assert result["success"] is True
    assert result["validation_retries"] == 0
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_retries_on_schema_violation_then_succeeds(tmp_path):
    """Bad JSON shape on attempt 1, valid on attempt 2 → retries=1 and the
    retry message carries both the prior response and the validator error."""
    from strata.notebook.llm import LlmConfig
    from strata.notebook.prompt_executor import execute_prompt_cell

    session = _prompt_session(tmp_path, f"# @output_schema {_TINY_SCHEMA}\nGive me a number.")
    fake, calls = _fake_llm_returning(
        '{"n": "seven"}',  # wrong type
        '{"n": 7}',  # correct
    )
    cfg = LlmConfig(base_url="https://api.openai.com/v1", api_key="sk", model="m")

    with mock.patch("strata.notebook.prompt_executor.chat_completion", fake):
        result = await execute_prompt_cell(
            session, "p1", session.notebook_state.cells[0].source, cfg
        )

    assert result["success"] is True
    assert result["validation_retries"] == 1
    assert len(calls) == 2

    # Second call's messages must carry the previous response + the
    # validator feedback so the model can correct itself.
    retry_messages = calls[1]["messages"]
    assert any(
        m["role"] == "assistant" and m["content"] == '{"n": "seven"}' for m in retry_messages
    )
    assert any(
        m["role"] == "user"
        and "did not validate against the required schema" in m["content"]
        and "/n" in m["content"]
        for m in retry_messages
    )


@pytest.mark.asyncio
async def test_exhausted_retries_surface_error(tmp_path):
    """Every attempt fails → error result, final validator message is
    surfaced so the user can see what went wrong."""
    from strata.notebook.llm import LlmConfig
    from strata.notebook.prompt_executor import execute_prompt_cell

    src = f"# @output_schema {_TINY_SCHEMA}\n# @validate_retries 2\nGive me a number."
    session = _prompt_session(tmp_path, src)
    fake, calls = _fake_llm_returning('{"n": "a"}', '{"n": "b"}')
    cfg = LlmConfig(base_url="https://api.openai.com/v1", api_key="sk", model="m")

    with mock.patch("strata.notebook.prompt_executor.chat_completion", fake):
        result = await execute_prompt_cell(
            session, "p1", session.notebook_state.cells[0].source, cfg
        )

    assert result["success"] is False
    assert "Response failed schema validation" in result["error"]
    # Two attempts, matching @validate_retries 2
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_no_schema_means_no_retries(tmp_path):
    """Without a schema the loop should make exactly one call even if the
    model returned garbage — there's nothing to validate against."""
    from strata.notebook.llm import LlmConfig
    from strata.notebook.prompt_executor import execute_prompt_cell

    session = _prompt_session(tmp_path, "Give me anything.")
    fake, calls = _fake_llm_returning("not even json")
    cfg = LlmConfig(base_url="https://api.openai.com/v1", api_key="sk", model="m")

    with mock.patch("strata.notebook.prompt_executor.chat_completion", fake):
        result = await execute_prompt_cell(
            session, "p1", session.notebook_state.cells[0].source, cfg
        )

    assert result["success"] is True
    assert result["validation_retries"] == 0
    assert len(calls) == 1
