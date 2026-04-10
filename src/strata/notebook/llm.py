"""LLM assistant integration for Strata notebooks.

Uses the OpenAI-compatible chat completions API (no SDK dependencies).
Works with Anthropic, OpenAI, Google, Mistral, Ollama, vLLM, and any
provider that implements the ``/v1/chat/completions`` endpoint.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
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
    "ANTHROPIC_API_KEY": ("https://api.anthropic.com/v1", "claude-sonnet-4-20250514"),
    "OPENAI_API_KEY": ("https://api.openai.com/v1", "gpt-4o"),
    "GEMINI_API_KEY": (
        "https://generativelanguage.googleapis.com/v1beta/openai",
        "gemini-2.0-flash",
    ),
    "MISTRAL_API_KEY": ("https://api.mistral.ai/v1", "mistral-large-latest"),
}

ActionType = Literal["generate", "explain", "describe", "chat", "plan"]


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
        api_key = (notebook_env or {}).get("STRATA_AI_API_KEY") or os.environ.get(
            "STRATA_AI_API_KEY"
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
        root_var = expr.split(".")[0].split("(")[0]
        if root_var in variables:
            value = variables[root_var]
            # If the expression has attribute access, try to evaluate it
            if expr != root_var:
                try:
                    value = eval(expr, {"__builtins__": {}}, variables)  # noqa: S307
                except Exception:
                    pass
            return variable_to_text(value, max_tokens=max_tokens_per_var)
        return match.group(0)  # Leave unreplaced if variable not found

    return re.sub(r"\{\{\s*([^}]+)\s*\}\}", _replace, template)


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
    system_templates: dict[str, str] = {
        "generate": _SYSTEM_GENERATE,
        "explain": _SYSTEM_EXPLAIN,
        "describe": _SYSTEM_DESCRIBE,
        "chat": _SYSTEM_CHAT,
        "plan": _SYSTEM_PLAN,
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


# ---------------------------------------------------------------------------
# Plan prompt and parser
# ---------------------------------------------------------------------------

_SYSTEM_PLAN = """\
You are a notebook assistant that proposes structured changes. Return ONLY \
a JSON object with this exact schema — no markdown, no explanation outside \
the JSON:

{{
  "summary": "Brief description of what these changes do",
  "changes": [
    {{"type": "add_package", "package": "pandas>=2.0"}},
    {{"type": "add_cell", "language": "python",
      "source": "import pandas as pd\\ndf = pd.read_csv('data.csv')",
      "name": "Load data"}},
    {{"type": "add_cell", "language": "python",
      "source": "df.describe()", "name": "Explore"}},
    {{"type": "set_env", "key": "API_KEY", "value": "placeholder"}},
    {{"type": "add_cell", "language": "prompt",
      "source": "# @name summary\\nSummarize {{{{ df }}}}",
      "name": "AI summary"}}
  ]
}}

Valid change types: add_cell, add_package, set_env.

For add_cell, set language to "python" or "prompt". Use "name" for a \
human-readable label. Order matters — cells will be added sequentially. \
When the task involves multiple steps, split into separate cells.

Do NOT propose delete or modify operations — those are done by the user \
directly in the notebook UI. Focus on generating new content.

The user's notebook has these cells and variables:

{{context}}"""


@dataclass
class ProposedChange:
    """One proposed change to the notebook."""

    type: str  # add_cell, add_package, set_env
    source: str | None = None
    language: str | None = None
    name: str | None = None
    package: str | None = None
    key: str | None = None
    value: str | None = None


@dataclass
class ChangePlan:
    """A structured set of proposed changes from the LLM."""

    summary: str
    changes: list[ProposedChange]
    raw_content: str = ""


def parse_change_plan(content: str) -> ChangePlan | None:
    """Parse an LLM response as a structured change plan.

    Tries JSON directly, then extracts from a code block.
    Returns None if parsing fails.
    """
    import re

    raw = content.strip()

    # Try direct JSON parse
    parsed = _try_parse_json(raw)

    # Try extracting from markdown code block
    if parsed is None:
        m = re.search(r"```(?:json)?\s*\n([\s\S]*?)\n```", raw)
        if m:
            parsed = _try_parse_json(m.group(1).strip())

    if parsed is None or not isinstance(parsed, dict):
        return None

    summary = parsed.get("summary", "")
    raw_changes = parsed.get("changes", [])
    if not isinstance(raw_changes, list):
        return None

    changes: list[ProposedChange] = []
    for item in raw_changes:
        if not isinstance(item, dict) or "type" not in item:
            continue
        changes.append(
            ProposedChange(
                type=item["type"],
                source=item.get("source"),
                language=item.get("language"),
                name=item.get("name"),
                package=item.get("package"),
                key=item.get("key"),
                value=item.get("value"),
            )
        )

    if not changes:
        return None

    return ChangePlan(summary=summary, changes=changes, raw_content=content)


def _try_parse_json(text: str) -> dict | None:
    """Attempt to parse JSON, returning None on failure."""
    try:
        result = json.loads(text)
        return result if isinstance(result, dict) else None
    except (json.JSONDecodeError, ValueError):
        return None


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
        "max_tokens": config.max_output_tokens,
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
        resp.raise_for_status()
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
                outcome = await session.mutate_dependency(spec, action="add")
                await _sync_frontend()
                if outcome.result.success:
                    return f"Installed {spec} successfully."
                return f"Failed to install {spec}: {outcome.result.error}"
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
