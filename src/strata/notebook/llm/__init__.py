"""LLM assistant integration for Strata notebooks.

Submodule layout:
* ``config``      — ``LlmConfig``, ``resolve_llm_config``, provider helpers
* ``structured``  — ``response_format_for`` and Anthropic tool-use builders
* ``client``      — ``chat_completion`` and ``chat_completion_stream``
* ``prompts``     — ``variable_to_text``, ``render_prompt_template``
* ``context``     — ``build_notebook_context``, ``build_messages``
* ``agent``       — the redesigned ``run_agent_loop`` with tool use,
                    streaming, per-notebook memory, and approval gates

The package re-exports the historical public surface so callers that
previously imported from ``strata.notebook.llm`` (e.g. ``LlmConfig``,
``chat_completion``, ``run_agent_loop``) keep working unchanged.
"""

from strata.notebook.llm.agent import (
    AGENT_TOOLS,
    DESTRUCTIVE_TOOLS,
    READ_ONLY_TOOLS,
    AgentLoopResult,
    AgentToolCall,
    append_history,
    execute_tool,
    get_history,
    make_approval_callback,
    reset_history,
    resolve_approval,
    resolve_variable_to_cell_id,
    run_agent_loop,
)
from strata.notebook.llm.client import (
    chat_completion,
    chat_completion_stream,
)
from strata.notebook.llm.config import (
    ActionType,
    LlmCompletionResult,
    LlmConfig,
    infer_provider_name,
    resolve_llm_config,
)
from strata.notebook.llm.context import (
    build_messages,
    build_notebook_context,
)
from strata.notebook.llm.prompts import (
    estimate_tokens,
    render_prompt_template,
    variable_to_text,
)
from strata.notebook.llm.structured import (
    build_anthropic_tool_use_body,
    parse_anthropic_tool_use_response,
    response_format_for,
)

__all__ = [
    # config
    "ActionType",
    "LlmConfig",
    "LlmCompletionResult",
    "infer_provider_name",
    "resolve_llm_config",
    # structured
    "build_anthropic_tool_use_body",
    "parse_anthropic_tool_use_response",
    "response_format_for",
    # client
    "chat_completion",
    "chat_completion_stream",
    # prompts
    "estimate_tokens",
    "render_prompt_template",
    "variable_to_text",
    # context
    "build_messages",
    "build_notebook_context",
    # agent
    "AGENT_TOOLS",
    "AgentLoopResult",
    "AgentToolCall",
    "DESTRUCTIVE_TOOLS",
    "READ_ONLY_TOOLS",
    "append_history",
    "execute_tool",
    "get_history",
    "make_approval_callback",
    "reset_history",
    "resolve_approval",
    "resolve_variable_to_cell_id",
    "run_agent_loop",
]
