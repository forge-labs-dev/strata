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


class TestResolveLlmConfig:
    """Tests for LLM config resolution."""

    def test_returns_none_when_no_key(self):
        """No API key → None."""
        with patch.dict(os.environ, {}, clear=True):
            assert resolve_llm_config() is None

    def test_anthropic_key_sets_defaults(self):
        """ANTHROPIC_API_KEY → Anthropic base URL and Claude model."""
        with patch.dict(
            os.environ,
            {"ANTHROPIC_API_KEY": "sk-ant-test"},
            clear=True,
        ):
            config = resolve_llm_config()
            assert config is not None
            assert config.api_key == "sk-ant-test"
            assert "anthropic" in config.base_url
            assert "claude" in config.model

    def test_openai_key_sets_defaults(self):
        """OPENAI_API_KEY → OpenAI base URL and GPT model."""
        with patch.dict(
            os.environ,
            {"OPENAI_API_KEY": "sk-test"},
            clear=True,
        ):
            config = resolve_llm_config()
            assert config is not None
            assert config.api_key == "sk-test"
            assert "openai" in config.base_url

    def test_strata_ai_key_as_fallback(self):
        """STRATA_AI_API_KEY works as a generic fallback."""
        with patch.dict(
            os.environ,
            {"STRATA_AI_API_KEY": "sk-generic"},
            clear=True,
        ):
            config = resolve_llm_config()
            assert config is not None
            assert config.api_key == "sk-generic"

    def test_notebook_config_overrides_env(self):
        """notebook.toml [ai] section overrides env vars."""
        with patch.dict(
            os.environ,
            {"ANTHROPIC_API_KEY": "sk-ant-test"},
            clear=True,
        ):
            config = resolve_llm_config(
                notebook_config={
                    "base_url": "http://localhost:11434/v1",
                    "model": "llama3",
                }
            )
            assert config is not None
            assert config.base_url == "http://localhost:11434/v1"
            assert config.model == "llama3"
            # API key still from env since notebook didn't override it
            assert config.api_key == "sk-ant-test"

    def test_notebook_env_provides_api_key(self):
        """Notebook-level env vars (Runtime panel) provide API keys."""
        with patch.dict(os.environ, {}, clear=True):
            config = resolve_llm_config(
                notebook_env={"OPENAI_API_KEY": "sk-from-runtime-panel"}
            )
            assert config is not None
            assert config.api_key == "sk-from-runtime-panel"
            assert "openai" in config.base_url

    def test_notebook_env_overrides_process_env(self):
        """Notebook env takes priority over process env for API keys."""
        with patch.dict(
            os.environ,
            {"OPENAI_API_KEY": "sk-process"},
            clear=True,
        ):
            config = resolve_llm_config(
                notebook_env={"ANTHROPIC_API_KEY": "sk-notebook-runtime"}
            )
            assert config is not None
            assert config.api_key == "sk-notebook-runtime"
            assert "anthropic" in config.base_url

    def test_server_config_layer(self):
        """Server config provides middle-priority defaults."""

        class FakeServerConfig:
            ai_api_key = "sk-server"
            ai_base_url = "https://custom.api.com/v1"
            ai_model = "custom-model"
            ai_max_context_tokens = 50_000
            ai_max_output_tokens = 2048
            ai_timeout_seconds = 30.0

        with patch.dict(os.environ, {}, clear=True):
            config = resolve_llm_config(server_config=FakeServerConfig())
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
            infer_provider_name(
                "https://generativelanguage.googleapis.com/v1beta/openai"
            )
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

    def test_generate_action(self):
        messages = build_messages("generate", "Write a function", "ctx")
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"
        assert "Write a function" in messages[1]["content"]

    def test_explain_with_cell_error(self):
        messages = build_messages(
            "explain",
            "Why does this fail?",
            "ctx",
            cell_source="x = 1/0",
            cell_error="ZeroDivisionError: division by zero",
        )
        assert len(messages) == 2
        assert "x = 1/0" in messages[1]["content"]
        assert "ZeroDivisionError" in messages[1]["content"]

    def test_describe_with_cell_source(self):
        messages = build_messages(
            "describe",
            "What does this do?",
            "ctx",
            cell_source="df = pd.read_csv('data.csv')",
        )
        assert "pd.read_csv" in messages[1]["content"]

    def test_chat_action(self):
        messages = build_messages("chat", "What is pandas?", "ctx")
        assert len(messages) == 2


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
