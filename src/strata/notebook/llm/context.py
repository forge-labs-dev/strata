"""Notebook context builder + chat-mode message assembly.

``build_notebook_context`` returns a compact text rendering of the
notebook (cells, defines/uses, installed packages) for injection into
LLM prompts. ``build_messages`` is the chat-mode message constructor;
the agent loop builds its own messages and does not use this helper.
"""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from strata.notebook.session import NotebookSession


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
