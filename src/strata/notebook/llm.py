"""LLM assistant integration for Strata notebooks.

Uses the OpenAI-compatible chat completions API (no SDK dependencies).
Works with Anthropic, OpenAI, Google, Mistral, Ollama, vLLM, and any
provider that implements the ``/v1/chat/completions`` endpoint.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import httpx

if TYPE_CHECKING:
    from strata.notebook.session import NotebookSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_PROVIDER_DEFAULTS: dict[str, tuple[str, str]] = {
    "ANTHROPIC_API_KEY": ("https://api.anthropic.com/v1", "claude-sonnet-4-20250514"),
    "OPENAI_API_KEY": ("https://api.openai.com/v1", "gpt-4o"),
    "GEMINI_API_KEY": (
        "https://generativelanguage.googleapis.com/v1beta/openai",
        "gemini-2.0-flash",
    ),
    "MISTRAL_API_KEY": ("https://api.mistral.ai/v1", "mistral-large-latest"),
}

ActionType = Literal["generate", "explain", "describe", "chat"]


@dataclass(frozen=True)
class LlmConfig:
    """Resolved LLM provider configuration."""

    base_url: str
    api_key: str
    model: str
    max_context_tokens: int = 100_000
    max_output_tokens: int = 4096
    timeout_seconds: float = 60.0


@dataclass
class LlmCompletionResult:
    """Result from a chat completion request."""

    content: str
    model: str
    input_tokens: int
    output_tokens: int


# ---------------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------------


def resolve_llm_config(
    notebook_config: dict[str, Any] | None = None,
    server_config: Any | None = None,
    notebook_env: dict[str, str] | None = None,
) -> LlmConfig | None:
    """Merge notebook [ai] config, server config, and env vars.

    Resolution order (highest priority wins):
    1. notebook.toml ``[ai]`` section
    2. Server config (``STRATA_AI_*`` env vars via StrataConfig)
    3. Notebook-level env vars (set via the Runtime panel)
    4. Provider-specific process env vars (``ANTHROPIC_API_KEY``, etc.)

    Returns ``None`` if no API key can be found.
    """
    base_url: str | None = None
    api_key: str | None = None
    model: str | None = None
    max_context_tokens = 100_000
    max_output_tokens = 4096
    timeout_seconds = 60.0

    # Layer 1: provider-specific process env var fallbacks
    for env_var, (default_url, default_model) in _PROVIDER_DEFAULTS.items():
        key = os.environ.get(env_var)
        if key:
            api_key = key
            base_url = default_url
            model = default_model
            break

    # Layer 1b: notebook-level env vars (from Runtime panel)
    if notebook_env:
        for env_var, (default_url, default_model) in _PROVIDER_DEFAULTS.items():
            key = notebook_env.get(env_var)
            if key:
                api_key = key
                base_url = default_url
                model = default_model
                break

    # Layer 2: server config (STRATA_AI_* env vars)
    if server_config is not None:
        if getattr(server_config, "ai_api_key", None):
            api_key = server_config.ai_api_key
        if getattr(server_config, "ai_base_url", None):
            base_url = server_config.ai_base_url
        if getattr(server_config, "ai_model", None):
            model = server_config.ai_model
        if getattr(server_config, "ai_max_context_tokens", None):
            max_context_tokens = server_config.ai_max_context_tokens
        if getattr(server_config, "ai_max_output_tokens", None):
            max_output_tokens = server_config.ai_max_output_tokens
        if getattr(server_config, "ai_timeout_seconds", None):
            timeout_seconds = server_config.ai_timeout_seconds

    # Layer 3: notebook.toml [ai] section (highest priority)
    if notebook_config:
        if notebook_config.get("api_key"):
            api_key = notebook_config["api_key"]
        if notebook_config.get("base_url"):
            base_url = notebook_config["base_url"]
        if notebook_config.get("model"):
            model = notebook_config["model"]
        if notebook_config.get("max_context_tokens"):
            max_context_tokens = int(notebook_config["max_context_tokens"])
        if notebook_config.get("max_output_tokens"):
            max_output_tokens = int(notebook_config["max_output_tokens"])
        if notebook_config.get("timeout_seconds"):
            timeout_seconds = float(notebook_config["timeout_seconds"])

    # Also check the generic STRATA_AI_API_KEY (process env + notebook env)
    if not api_key:
        api_key = (
            (notebook_env or {}).get("STRATA_AI_API_KEY")
            or os.environ.get("STRATA_AI_API_KEY")
        )

    if not api_key:
        return None

    return LlmConfig(
        base_url=base_url or "https://api.openai.com/v1",
        api_key=api_key,
        model=model or "gpt-4o",
        max_context_tokens=max_context_tokens,
        max_output_tokens=max_output_tokens,
        timeout_seconds=timeout_seconds,
    )


def infer_provider_name(base_url: str) -> str:
    """Infer a human-readable provider name from the base URL."""
    url = base_url.lower()
    if "anthropic" in url:
        return "anthropic"
    if "googleapis" in url or "generativelanguage" in url:
        return "google"
    if "mistral" in url:
        return "mistral"
    if "openai" in url:
        return "openai"
    if "localhost" in url or "127.0.0.1" in url:
        return "local"
    return "custom"


# ---------------------------------------------------------------------------
# Chat completion
# ---------------------------------------------------------------------------


async def chat_completion(
    config: LlmConfig,
    messages: list[dict[str, str]],
) -> LlmCompletionResult:
    """Send a chat completion request via the OpenAI-compatible API."""
    async with httpx.AsyncClient(timeout=config.timeout_seconds) as client:
        resp = await client.post(
            f"{config.base_url.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {config.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": config.model,
                "messages": messages,
                "max_tokens": config.max_output_tokens,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    choice = data["choices"][0]
    usage = data.get("usage", {})

    return LlmCompletionResult(
        content=choice["message"]["content"],
        model=data.get("model", config.model),
        input_tokens=usage.get("prompt_tokens", 0),
        output_tokens=usage.get("completion_tokens", 0),
    )


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for English text."""
    return max(1, len(text) // 4)


# ---------------------------------------------------------------------------
# Notebook context
# ---------------------------------------------------------------------------


def build_notebook_context(
    session: NotebookSession,
    max_tokens: int = 8000,
) -> str:
    """Build a context string from the current notebook state.

    Includes cell sources, variable definitions, and installed packages.
    Truncates to stay within the token budget.
    """
    parts: list[str] = []

    # Installed packages
    try:
        pyproject = session.path / "pyproject.toml"
        if pyproject.exists():
            import tomllib

            with open(pyproject, "rb") as f:
                data = tomllib.load(f)
            deps = data.get("project", {}).get("dependencies", [])
            if deps:
                parts.append(f"Installed packages: {', '.join(deps)}")
    except Exception:
        pass

    # Cells in order
    cells = sorted(session.notebook_state.cells, key=lambda c: c.order)
    for cell in cells:
        header = f"[Cell {cell.id}]"
        if cell.defines:
            header += f" defines: {', '.join(cell.defines)}"
        if cell.references:
            header += f" uses: {', '.join(cell.references)}"
        parts.append(f"{header}\n{cell.source}")

    context = "\n\n".join(parts)

    # Truncate to budget
    max_chars = max_tokens * 4
    if len(context) > max_chars:
        context = context[:max_chars] + "\n... (truncated)"

    return context


# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

_SYSTEM_GENERATE = """\
You are a Python coding assistant for a data notebook. Write clean, \
concise Python code in fenced code blocks. No explanations outside \
code blocks unless the user asks.

When the task involves multiple logical steps (e.g. load data, \
transform, visualize), split into SEPARATE fenced code blocks — one \
per step. Each block becomes its own notebook cell. Start each block \
with a brief # comment describing its purpose. Variables defined in \
earlier blocks are automatically available in later ones.

The user's notebook has these cells and variables:

{context}"""

_SYSTEM_EXPLAIN = """\
You are explaining a Python error in a data notebook. Be concise. \
Explain what went wrong and suggest a fix. If you suggest code, \
put it in a fenced code block.

Notebook context:

{context}"""

_SYSTEM_DESCRIBE = """\
You are describing what a Python notebook cell does. Be concise (2-3 sentences). \
Mention the key variables produced and any transformations applied.

Notebook context:

{context}"""

_SYSTEM_CHAT = """\
You are a helpful assistant for a Python data notebook. Answer questions \
about the code, data, and analysis. When suggesting code, use fenced code blocks.

Notebook context:

{context}"""


def build_messages(
    action: ActionType,
    user_message: str,
    notebook_context: str,
    cell_source: str | None = None,
    cell_error: str | None = None,
) -> list[dict[str, str]]:
    """Build the chat messages list for a given action type."""
    system_templates = {
        "generate": _SYSTEM_GENERATE,
        "explain": _SYSTEM_EXPLAIN,
        "describe": _SYSTEM_DESCRIBE,
        "chat": _SYSTEM_CHAT,
    }

    system = system_templates[action].format(context=notebook_context)
    messages: list[dict[str, str]] = [{"role": "system", "content": system}]

    # Add cell context for explain/describe
    if action == "explain" and cell_source:
        user_content = f"Cell code:\n```python\n{cell_source}\n```\n\n"
        if cell_error:
            user_content += f"Error:\n```\n{cell_error}\n```\n\n"
        user_content += user_message
        messages.append({"role": "user", "content": user_content})
    elif action == "describe" and cell_source:
        messages.append(
            {"role": "user", "content": f"```python\n{cell_source}\n```\n\n{user_message}"}
        )
    else:
        messages.append({"role": "user", "content": user_message})

    return messages
