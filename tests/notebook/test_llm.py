"""Tests for LLM assistant integration."""

from __future__ import annotations

import os
from unittest.mock import patch

from strata.notebook.llm import (
    build_messages,
    build_notebook_context,
    estimate_tokens,
    infer_provider_name,
    resolve_llm_config,
)


class _FakeServerConfig:
    """Minimal server config stub for layering tests."""

    def __init__(self, **kwargs):
        self.ai_api_key = kwargs.get("ai_api_key")
        self.ai_base_url = kwargs.get("ai_base_url")
        self.ai_model = kwargs.get("ai_model")
        self.ai_max_context_tokens = kwargs.get("ai_max_context_tokens")
        self.ai_max_output_tokens = kwargs.get("ai_max_output_tokens")
        self.ai_timeout_seconds = kwargs.get("ai_timeout_seconds")


class TestResolveLlmConfig:
    """Tests for LLM config resolution.

    Process env vars must NOT be consulted by ``resolve_llm_config`` — only
    explicit server config, notebook env vars (Runtime panel), and the
    notebook.toml [ai] section. All tests run with os.environ cleared to
    make accidental regressions obvious.
    """

    def test_returns_none_when_no_key(self):
        """No key anywhere → None."""
        with patch.dict(os.environ, {}, clear=True):
            assert resolve_llm_config() is None

    def test_process_env_is_ignored(self):
        """Shell-exported keys must NOT leak into notebooks."""
        with patch.dict(
            os.environ,
            {
                "ANTHROPIC_API_KEY": "sk-shell",
                "OPENAI_API_KEY": "sk-shell-openai",
                "STRATA_AI_API_KEY": "sk-shell-generic",
            },
            clear=True,
        ):
            # No notebook, no server config — process env must not rescue this.
            assert resolve_llm_config() is None

    def test_notebook_env_anthropic(self):
        """Notebook env ANTHROPIC_API_KEY → Anthropic defaults."""
        with patch.dict(os.environ, {}, clear=True):
            config = resolve_llm_config(notebook_env={"ANTHROPIC_API_KEY": "sk-ant-nb"})
            assert config is not None
            assert config.api_key == "sk-ant-nb"
            assert "anthropic" in config.base_url
            assert "claude" in config.model

    def test_notebook_env_openai(self):
        """Notebook env OPENAI_API_KEY → OpenAI defaults."""
        with patch.dict(os.environ, {}, clear=True):
            config = resolve_llm_config(notebook_env={"OPENAI_API_KEY": "sk-test"})
            assert config is not None
            assert config.api_key == "sk-test"
            assert "openai" in config.base_url

    def test_notebook_env_generic_strata_key(self):
        """STRATA_AI_API_KEY in notebook env works as a generic fallback."""
        with patch.dict(os.environ, {}, clear=True):
            config = resolve_llm_config(notebook_env={"STRATA_AI_API_KEY": "sk-generic"})
            assert config is not None
            assert config.api_key == "sk-generic"

    def test_notebook_toml_overrides_server_and_env(self):
        """notebook.toml [ai] beats notebook env and server config."""
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-shell"}, clear=True):
            config = resolve_llm_config(
                notebook_config={
                    "api_key": "sk-from-toml",
                    "base_url": "http://localhost:11434/v1",
                    "model": "llama3",
                },
                notebook_env={"OPENAI_API_KEY": "sk-runtime"},
                server_config=_FakeServerConfig(ai_api_key="sk-server"),
            )
            assert config is not None
            assert config.api_key == "sk-from-toml"
            assert config.base_url == "http://localhost:11434/v1"
            assert config.model == "llama3"

    def test_notebook_env_overrides_server_config(self):
        """Notebook env (Runtime panel) takes priority over server-wide defaults."""
        with patch.dict(os.environ, {}, clear=True):
            config = resolve_llm_config(
                notebook_env={"ANTHROPIC_API_KEY": "sk-notebook"},
                server_config=_FakeServerConfig(ai_api_key="sk-server"),
            )
            assert config is not None
            assert config.api_key == "sk-notebook"
            assert "anthropic" in config.base_url

    def test_server_config_layer(self):
        """Server config is the lowest-priority source of defaults."""
        with patch.dict(os.environ, {}, clear=True):
            config = resolve_llm_config(
                server_config=_FakeServerConfig(
                    ai_api_key="sk-server",
                    ai_base_url="https://custom.api.com/v1",
                    ai_model="custom-model",
                    ai_max_context_tokens=50_000,
                    ai_max_output_tokens=2048,
                    ai_timeout_seconds=30.0,
                )
            )
            assert config is not None
            assert config.api_key == "sk-server"
            assert config.base_url == "https://custom.api.com/v1"
            assert config.model == "custom-model"
            assert config.max_output_tokens == 2048


class TestInferProviderName:
    """Tests for provider name inference."""

    def test_anthropic(self):
        assert infer_provider_name("https://api.anthropic.com/v1") == "anthropic"

    def test_openai(self):
        assert infer_provider_name("https://api.openai.com/v1") == "openai"

    def test_google(self):
        assert (
            infer_provider_name("https://generativelanguage.googleapis.com/v1beta/openai")
            == "google"
        )

    def test_local(self):
        assert infer_provider_name("http://localhost:11434/v1") == "local"

    def test_custom(self):
        assert infer_provider_name("https://my-company.com/llm/v1") == "custom"


class TestEstimateTokens:
    """Tests for token estimation."""

    def test_basic(self):
        assert estimate_tokens("hello world") > 0

    def test_empty(self):
        assert estimate_tokens("") == 1

    def test_proportional(self):
        short = estimate_tokens("hello")
        long = estimate_tokens("hello " * 100)
        assert long > short


class TestBuildMessages:
    """Tests for message building."""

    def test_basic_chat(self):
        messages = build_messages("What is pandas?", "ctx")
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert "ctx" in messages[0]["content"]
        assert messages[1]["role"] == "user"
        assert messages[1]["content"] == "What is pandas?"

    def test_with_cell_source(self):
        messages = build_messages(
            "Why does this fail?",
            "ctx",
            cell_source="x = 1/0",
        )
        assert len(messages) == 2
        assert "x = 1/0" in messages[1]["content"]
        assert "Why does this fail?" in messages[1]["content"]

    def test_with_history(self):
        history = [
            {"role": "user", "content": "What is pandas?"},
            {"role": "assistant", "content": "A data analysis library."},
        ]
        messages = build_messages("Give an example.", "ctx", history=history)
        assert len(messages) == 4
        assert messages[0]["role"] == "system"
        assert messages[1]["content"] == "What is pandas?"
        assert messages[2]["content"] == "A data analysis library."
        assert messages[3]["content"] == "Give an example."

    def test_history_filters_invalid_roles(self):
        history = [
            {"role": "user", "content": "ok"},
            {"role": "system", "content": "should be dropped"},
            {"role": "assistant", "content": ""},
        ]
        messages = build_messages("hi", "ctx", history=history)
        # system (index 0) + 1 valid history turn + current user = 3
        assert len(messages) == 3
        assert messages[1]["content"] == "ok"


class TestBuildNotebookContext:
    """Tests for notebook context building."""

    def test_builds_context_from_session(self, tmp_path):
        from strata.notebook.parser import parse_notebook
        from strata.notebook.session import NotebookSession
        from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

        nb_dir = create_notebook(tmp_path, "ctx_test")
        add_cell_to_notebook(nb_dir, "c1")
        write_cell(nb_dir, "c1", "x = 1")
        add_cell_to_notebook(nb_dir, "c2", "c1")
        write_cell(nb_dir, "c2", "y = x + 1")

        session = NotebookSession(parse_notebook(nb_dir), nb_dir)
        context = build_notebook_context(session)

        assert "x = 1" in context
        assert "y = x + 1" in context

    def test_truncates_long_context(self, tmp_path):
        from strata.notebook.parser import parse_notebook
        from strata.notebook.session import NotebookSession
        from strata.notebook.writer import add_cell_to_notebook, create_notebook, write_cell

        nb_dir = create_notebook(tmp_path, "trunc_test")
        add_cell_to_notebook(nb_dir, "c1")
        write_cell(nb_dir, "c1", "x = 1\n" * 10000)

        session = NotebookSession(parse_notebook(nb_dir), nb_dir)
        context = build_notebook_context(session, max_tokens=100)

        assert len(context) < 500
        assert "truncated" in context
