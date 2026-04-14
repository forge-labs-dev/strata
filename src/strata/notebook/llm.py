"""LLM assistant integration for Strata notebooks.

Uses the OpenAI-compatible chat completions API (no SDK dependencies).
Works with Anthropic, OpenAI, Google, Mistral, Ollama, vLLM, and any
provider that implements the ``/v1/chat/completions`` endpoint.
"""

from __future__ import annotations

import ast
import asyncio
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import httpx

if TYPE_CHECKING:
    from strata.notebook.session import NotebookSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_PROVIDER_DEFAULTS: dict[str, tuple[str, str]] = {
    "ANTHROPIC_API_KEY": ("https://api.anthropic.com/v1", "claude-sonnet-4-6"),
    "OPENAI_API_KEY": ("https://api.openai.com/v1", "gpt-5.4"),
    "GEMINI_API_KEY": (
        "https://generativelanguage.googleapis.com/v1beta/openai",
        "gemini-3-flash",
    ),
    "MISTRAL_API_KEY": ("https://api.mistral.ai/v1", "mistral-large-latest"),
}

ActionType = Literal["chat"]


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
    """Merge notebook [ai] config, server config, and notebook env vars.

    Resolution order (highest priority wins):
    1. notebook.toml ``[ai]`` section
    2. Notebook-level env vars (set via the Runtime panel)
    3. Server config (``STRATA_AI_*`` env vars read at server startup)

    Process-level environment variables are **not** consulted, so a key
    accidentally exported in the shell that started the server does not
    leak into every notebook. An admin deploying a shared server can still
    provide a default via the explicit ``STRATA_AI_*`` server config.

    Returns ``None`` if no API key can be found.
    """
    base_url: str | None = None
    api_key: str | None = None
    model: str | None = None
    max_context_tokens = 100_000
    max_output_tokens = 4096
    timeout_seconds = 60.0

    # Layer 1 (lowest): server config (explicit STRATA_AI_* at startup)
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

    # Layer 2: notebook-level env vars (from Runtime panel).
    # Setting a provider-specific key here picks up that provider's
    # default base_url and model unless the notebook.toml overrides them.
    if notebook_env:
        for env_var, (default_url, default_model) in _PROVIDER_DEFAULTS.items():
            key = notebook_env.get(env_var)
            if key:
                api_key = key
                base_url = default_url
                model = default_model
                break
        # Generic key (no implicit provider selection)
        if not api_key and notebook_env.get("STRATA_AI_API_KEY"):
            api_key = notebook_env["STRATA_AI_API_KEY"]

    # Layer 3 (highest): notebook.toml [ai] section
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

    if not api_key:
        return None

    return LlmConfig(
        base_url=base_url or "https://api.openai.com/v1",
        api_key=api_key,
        model=model or "gpt-5.4",
        max_context_tokens=max_context_tokens,
        max_output_tokens=max_output_tokens,
        timeout_seconds=timeout_seconds,
    )


def _max_output_tokens_param(base_url: str) -> str:
    """Return the correct max-output-tokens field name for this provider.

    OpenAI's gpt-5 / o-series / gpt-4o reject ``max_tokens`` with
    "unsupported_parameter" and require ``max_completion_tokens``. Other
    OpenAI-compatible providers (Anthropic, Google, Mistral, local
    servers) still accept ``max_tokens``, so we only switch for openai.
    """
    if "openai" in base_url.lower():
        return "max_completion_tokens"
    return "max_tokens"


def _raise_for_llm_status(resp: httpx.Response, model: str) -> None:
    """Like ``resp.raise_for_status()`` but surface the provider's error body.

    The default httpx error only includes URL + status; when a provider
    returns 400 with "model not found" or "invalid parameter", the
    message is in the body — which users need to debug.
    """
    if resp.is_success:
        return
    body = resp.text[:1000] if resp.text else "(empty body)"
    raise RuntimeError(f"LLM provider returned HTTP {resp.status_code} for model {model!r}: {body}")


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
    *,
    temperature: float | None = None,
) -> LlmCompletionResult:
    """Send a chat completion request via the OpenAI-compatible API."""
    body: dict[str, Any] = {
        "model": config.model,
        "messages": messages,
        _max_output_tokens_param(config.base_url): config.max_output_tokens,
    }
    if temperature is not None:
        body["temperature"] = temperature

    async with httpx.AsyncClient(timeout=config.timeout_seconds) as client:
        resp = await client.post(
            f"{config.base_url.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {config.api_key}",
                "Content-Type": "application/json",
            },
            json=body,
        )
        _raise_for_llm_status(resp, config.model)
        data = resp.json()

    choice = data["choices"][0]
    usage = data.get("usage", {})

    return LlmCompletionResult(
        content=choice["message"]["content"],
        model=data.get("model", config.model),
        input_tokens=usage.get("prompt_tokens", 0),
        output_tokens=usage.get("completion_tokens", 0),
    )


async def chat_completion_stream(
    config: LlmConfig,
    messages: list[dict[str, str]],
):
    """Stream a chat completion as text deltas.

    Yields dicts of the form ``{"type": "delta", "text": str}`` for content
    chunks and a final ``{"type": "done", "model": str, "input_tokens": int,
    "output_tokens": int}`` event when the stream ends. The OpenAI-compatible
    API returns ``data: ...`` SSE lines; we parse them and pull
    ``choices[0].delta.content``.
    """
    model = config.model
    input_tokens = 0
    output_tokens = 0

    async with httpx.AsyncClient(timeout=config.timeout_seconds) as client:
        async with client.stream(
            "POST",
            f"{config.base_url.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {config.api_key}",
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
            },
            json={
                "model": config.model,
                "messages": messages,
                _max_output_tokens_param(config.base_url): config.max_output_tokens,
                "stream": True,
                "stream_options": {"include_usage": True},
            },
        ) as resp:
            if not resp.is_success:
                body = (await resp.aread()).decode("utf-8", errors="replace")[:1000]
                raise RuntimeError(
                    f"LLM provider returned HTTP {resp.status_code} "
                    f"for model {config.model!r}: {body}"
                )
            async for line in resp.aiter_lines():
                if not line or not line.startswith("data:"):
                    continue
                payload = line[5:].strip()
                if payload == "[DONE]":
                    break
                try:
                    chunk = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if chunk.get("model"):
                    model = chunk["model"]
                usage = chunk.get("usage")
                if isinstance(usage, dict):
                    input_tokens = usage.get("prompt_tokens", input_tokens)
                    output_tokens = usage.get("completion_tokens", output_tokens)
                choices = chunk.get("choices") or []
                if not choices:
                    continue
                delta = choices[0].get("delta") or {}
                text = delta.get("content")
                if text:
                    yield {"type": "delta", "text": text}

    yield {
        "type": "done",
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
    }


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for English text."""
    return max(1, len(text) // 4)


def variable_to_text(value: Any, max_tokens: int = 2000) -> str:
    """Convert a Python value to a text representation for prompt injection.

    Applies type-specific formatting with a per-variable token budget.
    """
    max_chars = max_tokens * 4

    try:
        import pandas as pd

        if isinstance(value, pd.DataFrame):
            text = value.head(20).to_markdown(index=False)
            if len(text) > max_chars:
                text = value.describe().to_markdown()
            if len(text) > max_chars:
                text = text[:max_chars] + "\n... (truncated)"
            return text
        if isinstance(value, pd.Series):
            return str(value.head(20))
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            header = f"ndarray shape={value.shape} dtype={value.dtype}"
            preview = str(value.flat[:10])
            text = f"{header}\n{preview}"
            return text[:max_chars]
    except ImportError:
        pass

    if isinstance(value, dict):
        import json as _json

        text = _json.dumps(value, indent=2, default=str)
    elif isinstance(value, (list, tuple)):
        import json as _json

        text = _json.dumps(value, indent=2, default=str)
    else:
        text = str(value)

    if len(text) > max_chars:
        text = text[:max_chars] + "\n... (truncated)"
    return text


def render_prompt_template(
    template: str,
    variables: dict[str, Any],
    max_tokens_per_var: int = 2000,
) -> str:
    """Render a prompt template by replacing ``{{ var }}`` with text values."""
    import re

    def _replace(match: re.Match) -> str:
        expr = match.group(1).strip()
        try:
            value = _resolve_prompt_expression(expr, variables)
        except Exception:
            return match.group(0)
        else:
            return variable_to_text(value, max_tokens=max_tokens_per_var)

    return re.sub(r"\{\{\s*([^}]+)\s*\}\}", _replace, template)


def _resolve_prompt_expression(expr: str, variables: dict[str, Any]) -> Any:
    """Resolve a prompt template expression without executing arbitrary code."""
    parsed = ast.parse(expr, mode="eval")
    return _evaluate_prompt_node(parsed.body, variables)


def _evaluate_prompt_node(node: ast.AST, variables: dict[str, Any]) -> Any:
    """Evaluate a restricted AST node for prompt templating."""
    if isinstance(node, ast.Name):
        if node.id not in variables:
            raise KeyError(node.id)
        return variables[node.id]

    if isinstance(node, ast.Attribute):
        value = _evaluate_prompt_node(node.value, variables)
        if node.attr.startswith("_"):
            raise ValueError("Private attributes are not allowed")
        resolved = getattr(value, node.attr)
        if callable(resolved):
            raise ValueError("Callable attributes must be explicitly allowed")
        return resolved

    if isinstance(node, ast.Call):
        if node.args or node.keywords:
            raise ValueError("Prompt template calls do not accept arguments")
        if not isinstance(node.func, ast.Attribute):
            raise ValueError("Only attribute method calls are allowed")
        obj = _evaluate_prompt_node(node.func.value, variables)
        method_name = node.func.attr
        if method_name.startswith("_"):
            raise ValueError("Private methods are not allowed")
        method = getattr(obj, method_name)
        if not _is_safe_prompt_method(obj, method_name, method):
            raise ValueError(f"Unsafe prompt method: {method_name}")
        return method()

    raise ValueError("Unsupported prompt expression")


def _is_safe_prompt_method(obj: Any, method_name: str, method: Any) -> bool:
    """Allow a very small set of known-safe zero-arg prompt helpers."""
    if not callable(method):
        return False

    try:
        import pandas as pd

        if isinstance(obj, (pd.DataFrame, pd.Series)):
            return method_name in {"describe", "head", "tail"}
    except ImportError:
        pass

    return False


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

_SYSTEM_CHAT = """\
You are a helpful assistant for a Python data notebook. Answer questions \
about the code, data, and analysis. When suggesting code, use fenced code blocks.

Notebook context:

{context}"""


def build_messages(
    user_message: str,
    notebook_context: str,
    history: list[dict[str, str]] | None = None,
    cell_source: str | None = None,
) -> list[dict[str, str]]:
    """Build the chat messages list.

    Order: system prompt → prior turns (``history``) → current user message.
    ``cell_source``, if given, is prepended to the current user message as
    optional context.
    """
    system = _SYSTEM_CHAT.format(context=notebook_context)
    messages: list[dict[str, str]] = [{"role": "system", "content": system}]

    if history:
        for turn in history:
            role = turn.get("role")
            content = turn.get("content", "")
            if role in ("user", "assistant") and isinstance(content, str) and content:
                messages.append({"role": role, "content": content})

    if cell_source:
        user_content = f"Selected cell:\n```python\n{cell_source}\n```\n\n{user_message}"
    else:
        user_content = user_message

    messages.append({"role": "user", "content": user_content})
    return messages


# ---------------------------------------------------------------------------
# Agent loop — tool-use with observe-and-retry
# ---------------------------------------------------------------------------

AGENT_TOOLS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_notebook_state",
            "description": (
                "Get current notebook state: all cells with source code, "
                "defined variables, execution status, and dependency graph."
            ),
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "create_cell",
            "description": (
                "Create a new Python or prompt cell. Returns the cell ID "
                "and the variables it defines."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "Source code for the cell",
                    },
                    "language": {
                        "type": "string",
                        "enum": ["python", "prompt"],
                        "default": "python",
                    },
                    "after_variable": {
                        "type": "string",
                        "description": (
                            "Insert after the cell that defines this variable. "
                            "Omit to append at end."
                        ),
                    },
                },
                "required": ["source"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "edit_cell",
            "description": (
                "Replace the source code of an existing cell, identified "
                "by the variable it defines."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "variable_name": {
                        "type": "string",
                        "description": "Variable defined by the target cell",
                    },
                    "new_source": {
                        "type": "string",
                        "description": "New source code",
                    },
                },
                "required": ["variable_name", "new_source"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delete_cell",
            "description": ("Delete an existing cell, identified by the variable it defines."),
            "parameters": {
                "type": "object",
                "properties": {
                    "variable_name": {
                        "type": "string",
                        "description": "Variable defined by the target cell",
                    },
                },
                "required": ["variable_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_cell",
            "description": (
                "Execute a cell and return its output, stdout, stderr, "
                "and any errors. Automatically cascades upstream cells."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "variable_name": {
                        "type": "string",
                        "description": "Variable defined by the target cell",
                    },
                },
                "required": ["variable_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_package",
            "description": "Install a Python package into the notebook environment.",
            "parameters": {
                "type": "object",
                "properties": {
                    "package_spec": {
                        "type": "string",
                        "description": "Package spec (e.g. 'pandas>=2.0')",
                    },
                },
                "required": ["package_spec"],
            },
        },
    },
]

_SYSTEM_AGENT = """\
You are an expert Python notebook assistant with access to tools. \
You can create cells, edit cells, delete cells, run cells, and install packages.

RULES:
- Reference cells by the VARIABLE NAME they define, not by ID.
- After creating or editing a cell, RUN IT to verify it works.
- If a cell errors, read the error, fix the code, and retry.
- If a ModuleNotFoundError occurs, use add_package to install it, then retry.
- Keep cells focused — one logical step per cell.
- When done, respond with a brief summary of what you did.

Current notebook state:

{context}"""


@dataclass
class AgentToolCall:
    """Record of one tool call in the agent loop."""

    tool_name: str
    arguments: dict[str, Any]
    result: str
    duration_ms: float


@dataclass
class AgentLoopResult:
    """Final result of an agent loop run."""

    content: str
    model: str
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    iterations: int = 0
    tool_calls: list[AgentToolCall] = field(default_factory=list)
    cancelled: bool = False
    error: str | None = None


async def agent_chat_completion(
    config: LlmConfig,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Chat completion with tool support. Returns raw choice + usage."""
    body: dict[str, Any] = {
        "model": config.model,
        "messages": messages,
        _max_output_tokens_param(config.base_url): config.max_output_tokens,
    }
    if tools:
        body["tools"] = tools

    async with httpx.AsyncClient(timeout=config.timeout_seconds) as client:
        resp = await client.post(
            f"{config.base_url.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {config.api_key}",
                "Content-Type": "application/json",
            },
            json=body,
        )
        _raise_for_llm_status(resp, config.model)
        data = resp.json()

    choice = data["choices"][0]
    usage = data.get("usage", {})
    return {
        "message": choice["message"],
        "finish_reason": choice.get("finish_reason"),
        "input_tokens": usage.get("prompt_tokens", 0),
        "output_tokens": usage.get("completion_tokens", 0),
        "model": data.get("model", config.model),
    }


def resolve_variable_to_cell_id(
    session: NotebookSession,
    variable_name: str,
) -> str | None:
    """Resolve a variable name to the cell ID that defines it."""
    if session.dag and variable_name in session.dag.variable_producer:
        return session.dag.variable_producer[variable_name]
    for cell in session.notebook_state.cells:
        if variable_name in cell.defines:
            return cell.id
    return None


async def execute_tool(
    session: NotebookSession,
    tool_name: str,
    arguments: dict[str, Any],
    notebook_id: str | None = None,
) -> str:
    """Execute a tool call and return a text result for the LLM."""
    from strata.notebook.executor import CellExecutor
    from strata.notebook.writer import (
        add_cell_to_notebook,
        remove_cell_from_notebook,
        write_cell,
    )

    async def _sync_frontend() -> None:
        """Broadcast updated notebook state to all connected frontends."""
        if notebook_id:
            try:
                from strata.notebook.ws import broadcast_notebook_sync

                await broadcast_notebook_sync(notebook_id, session)
            except Exception:
                pass

    try:
        if tool_name == "get_notebook_state":
            return build_notebook_context(session, max_tokens=4000)

        elif tool_name == "create_cell":
            source = arguments.get("source", "")
            language = arguments.get("language", "python")
            after_var = arguments.get("after_variable")
            after_id = None
            if after_var:
                after_id = resolve_variable_to_cell_id(session, after_var)

            cell_id = str(uuid.uuid4())[:8]
            add_cell_to_notebook(session.path, cell_id, after_id, language=language)
            if source:
                write_cell(session.path, cell_id, source)
            session.reload()
            await _sync_frontend()

            cell = next(
                (c for c in session.notebook_state.cells if c.id == cell_id),
                None,
            )
            defines = cell.defines if cell else []
            return f"Created cell {cell_id} (defines: {', '.join(defines) or 'none'})"

        elif tool_name == "edit_cell":
            var_name = arguments.get("variable_name", "")
            new_source = arguments.get("new_source", "")
            cell_id = resolve_variable_to_cell_id(session, var_name)
            if not cell_id:
                valid = [v for c in session.notebook_state.cells for v in c.defines]
                return f"Error: No cell defines '{var_name}'. Valid variables: {valid}"
            write_cell(session.path, cell_id, new_source)
            session.reload()
            await _sync_frontend()
            return f"Edited cell {cell_id} (was defining: {var_name})"

        elif tool_name == "delete_cell":
            var_name = arguments.get("variable_name", "")
            cell_id = resolve_variable_to_cell_id(session, var_name)
            if not cell_id:
                valid = [v for c in session.notebook_state.cells for v in c.defines]
                return f"Error: No cell defines '{var_name}'. Valid variables: {valid}"
            remove_cell_from_notebook(session.path, cell_id)
            session.reload()
            await _sync_frontend()
            return f"Deleted cell {cell_id} (defined: {var_name})"

        elif tool_name == "run_cell":
            var_name = arguments.get("variable_name", "")
            cell_id = resolve_variable_to_cell_id(session, var_name)
            if not cell_id:
                valid = [v for c in session.notebook_state.cells for v in c.defines]
                return f"Error: No cell defines '{var_name}'. Valid variables: {valid}"
            cell = next(
                (c for c in session.notebook_state.cells if c.id == cell_id),
                None,
            )
            if not cell:
                return f"Error: Cell {cell_id} not found in session state"

            # Use WS-aware execution if notebook_id is available
            if notebook_id:
                from strata.notebook.ws import execute_cell_for_agent

                try:
                    result = await execute_cell_for_agent(
                        notebook_id, session, cell_id, cell.source
                    )
                except RuntimeError as e:
                    return f"Error: {e}"
            else:
                executor = CellExecutor(session, session.warm_pool)
                result = await executor.execute_cell(cell_id, cell.source)

            parts = []
            if result.success:
                parts.append("Execution succeeded.")
                if result.cache_hit:
                    parts.append("(cache hit)")
            else:
                parts.append("Execution FAILED.")
            if result.error:
                parts.append(f"Error: {result.error}")
            if result.stdout:
                stdout = result.stdout[:2000]
                parts.append(f"Stdout:\n{stdout}")
            if result.stderr:
                stderr = result.stderr[:1000]
                parts.append(f"Stderr:\n{stderr}")
            if result.outputs:
                for name, meta in result.outputs.items():
                    preview = meta.get("preview", "")
                    parts.append(f"Output '{name}': {str(preview)[:500]}")
            await _sync_frontend()
            return "\n".join(parts)

        elif tool_name == "add_package":
            spec = arguments.get("package_spec", "")
            if not spec:
                return "Error: package_spec is required"
            try:
                job = await session.submit_environment_job(action="add", package=spec)
                await session.wait_for_environment_job()
                await _sync_frontend()
                history = session.serialize_environment_job_history()
                completed = next((entry for entry in history if entry.get("id") == job.id), None)
                if completed and completed.get("status") == "completed":
                    return f"Installed {spec} successfully."
                error = (
                    completed.get("error")
                    if completed is not None
                    else "Environment job did not finish cleanly"
                )
                return f"Failed to install {spec}: {error}"
            except Exception as e:
                return f"Error installing {spec}: {e}"

        else:
            return f"Error: Unknown tool '{tool_name}'"

    except Exception as e:
        return f"Error in {tool_name}: {type(e).__name__}: {e}"


async def run_agent_loop(
    config: LlmConfig,
    session: NotebookSession,
    user_message: str,
    *,
    notebook_id: str | None = None,
    max_iterations: int = 10,
    cancel_event: asyncio.Event | None = None,
    progress_callback: Callable[[str, str], Awaitable[None]] | None = None,
) -> AgentLoopResult:
    """Run an agent loop with tool use and observe-retry.

    The LLM calls tools, observes results, and retries until it
    produces a text response or hits the iteration limit.
    """
    context = build_notebook_context(session, max_tokens=4000)
    system = _SYSTEM_AGENT.format(context=context)

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_message},
    ]

    result = AgentLoopResult(content="", model=config.model)

    if progress_callback:
        await progress_callback("started", f"Agent started: {user_message[:100]}")

    for iteration in range(max_iterations):
        # Check cancellation
        if cancel_event and cancel_event.is_set():
            result.cancelled = True
            result.content = "Agent cancelled by user."
            if progress_callback:
                await progress_callback("cancelled", "Agent cancelled")
            return result

        result.iterations = iteration + 1

        if progress_callback:
            await progress_callback("iteration", f"Iteration {iteration + 1}/{max_iterations}")

        # Call LLM
        try:
            response = await agent_chat_completion(config, messages, tools=AGENT_TOOLS)
        except Exception as e:
            result.error = f"LLM call failed: {e}"
            if progress_callback:
                await progress_callback("error", result.error)
            return result

        result.total_input_tokens += response["input_tokens"]
        result.total_output_tokens += response["output_tokens"]
        result.model = response["model"]

        message = response["message"]
        tool_calls = message.get("tool_calls")

        # If no tool calls, the LLM is done — return text
        if not tool_calls:
            result.content = message.get("content", "") or ""
            if progress_callback:
                await progress_callback("done", result.content[:200])
            return result

        # Process tool calls
        messages.append(message)

        for tc in tool_calls:
            fn = tc["function"]
            tool_name = fn["name"]
            try:
                args = json.loads(fn["arguments"])
            except (json.JSONDecodeError, TypeError):
                args = {}

            if progress_callback:
                args_summary = ", ".join(f"{k}={str(v)[:50]}" for k, v in args.items())
                await progress_callback("tool_call", f"{tool_name}({args_summary})")

            # Execute tool
            start = time.time()
            tool_result = await execute_tool(session, tool_name, args, notebook_id=notebook_id)
            duration_ms = (time.time() - start) * 1000

            result.tool_calls.append(
                AgentToolCall(
                    tool_name=tool_name,
                    arguments=args,
                    result=tool_result[:500],
                    duration_ms=duration_ms,
                )
            )

            if progress_callback:
                await progress_callback(
                    "tool_result",
                    f"{tool_name} → {tool_result[:200]}",
                )

            # Add tool result to messages
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": tool_result,
                }
            )

    # Hit iteration limit — force a final response
    messages.append(
        {
            "role": "user",
            "content": (
                "You have reached the maximum number of tool calls. "
                "Please respond with a summary of what you accomplished."
            ),
        }
    )
    try:
        final = await agent_chat_completion(config, messages, tools=None)
        result.total_input_tokens += final["input_tokens"]
        result.total_output_tokens += final["output_tokens"]
        result.content = final["message"].get("content", "") or ""
    except Exception as e:
        result.error = f"Final LLM call failed: {e}"

    if progress_callback:
        await progress_callback("done", result.content[:200])

    return result
