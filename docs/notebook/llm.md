# LLM Integration

Strata Notebook has two ways to use LLMs: **prompt cells** (declarative, part of the DAG) and the **AI assistant** (conversational, in a sidebar panel). Both use the same provider configuration and support any OpenAI-compatible API.

---

## Configuration

Set an API key in the **Runtime panel** under Environment Variables. The key determines which provider is used:

| Environment Variable | Provider | Default Model |
|---------------------|----------|--------------|
| `ANTHROPIC_API_KEY` | Anthropic | claude-sonnet-4-20250514 |
| `OPENAI_API_KEY` | OpenAI | gpt-4o |
| `GEMINI_API_KEY` | Google | gemini-2.0-flash |
| `MISTRAL_API_KEY` | Mistral | mistral-large-latest |
| `STRATA_AI_API_KEY` | Custom (requires `[ai]` config) | — |

**Resolution order** (highest priority wins):

1. `notebook.toml` `[ai]` section — per-notebook override
2. Runtime panel env vars — set in the UI
3. Server config (`STRATA_AI_*` env vars) — admin default

!!! note "Process environment is not consulted"
    A shell-exported `OPENAI_API_KEY` does **not** leak into notebooks. This is intentional — each notebook must explicitly opt in to an LLM provider. See the [Annotations](annotations.md) page for how env vars flow.

### Custom Provider Configuration

For self-hosted models (Ollama, vLLM) or custom endpoints, add an `[ai]` section to `notebook.toml`:

```toml
[ai]
base_url = "http://localhost:11434/v1"
model = "llama3"
```

The API key still comes from the Runtime panel env vars. The `[ai]` section only overrides the endpoint URL and model name.

### Supported Providers

Any service that implements the OpenAI `/v1/chat/completions` endpoint works, including:

- OpenAI (GPT-4o, GPT-4, GPT-3.5)
- Anthropic (Claude, via their OpenAI-compatible endpoint)
- Google (Gemini, via their OpenAI-compatible endpoint)
- Mistral (Mistral Large, Codestral)
- Ollama (local models)
- vLLM, TGI, LiteLLM (self-hosted)

---

## Prompt Cells

Prompt cells are notebook cells with `language="prompt"`. They render a text template, call the LLM, and store the response as an artifact — just like Python cells, they participate in the DAG and cache by provenance.

### Basic Syntax

A prompt cell is plain text with `{{ variable }}` template injection:

```
# @name summary
Summarize this dataset:

{{ df }}

Return 3 key findings as a numbered list.
```

The `{{ df }}` placeholder is replaced with a text representation of the upstream variable `df` before sending to the LLM. The response is stored as an artifact named `summary` (from the `@name` annotation).

### Template Variables

Variables are injected using `{{ expression }}` syntax. The expression is resolved against upstream cell outputs:

| Upstream Type | Text Representation |
|--------------|-------------------|
| pandas DataFrame | Markdown table (first 20 rows) |
| pandas Series | String representation (first 20 values) |
| numpy ndarray | Shape + dtype + first 10 elements |
| dict / list | JSON (indented, truncated) |
| str / int / float | Direct string conversion |

Each variable has a token budget (default: 2,000 tokens). If the text representation exceeds the budget, it's truncated with a `... (truncated)` marker.

**Attribute access** is supported for safe, read-only operations:

```
{{ df.describe() }}     # OK — pandas describe() is allowed
{{ df.head() }}         # OK — pandas head() is allowed
{{ obj.value }}         # OK — attribute access (non-callable)
{{ obj.mutate() }}      # BLOCKED — unknown method, left as-is
```

Only a small set of known-safe methods are allowed (`describe`, `head`, `tail` on pandas DataFrames/Series). Arbitrary method calls are blocked to prevent side effects in template rendering.

### Prompt Cell Annotations

| Annotation | Description | Default |
|-----------|------------|---------|
| `@name` | Output variable name (must be a Python identifier) | `result` |
| `@model` | Override the LLM model | From provider config |
| `@temperature` | Sampling temperature (0.0 = deterministic) | `0.0` |
| `@max_tokens` | Maximum response tokens | `4096` |
| `@system` | System prompt prepended to the request | None |

Example with all annotations:

```
# @name classification
# @model gpt-4o
# @temperature 0.0
# @max_tokens 1000
# @system You are a data scientist. Return only valid JSON.
Classify each paper by topic:

{{ sampled_papers }}

Return a JSON object mapping paper ID to topic.
```

### Caching

Prompt cells cache like any other cell. The cache key includes:

- The rendered template text (after variable injection)
- The model name
- The temperature
- The system prompt
- The output type

If you re-run a prompt cell with the same inputs and the same model config, the cached response is returned instantly — no LLM call is made.

---

## AI Assistant

The AI assistant is a sidebar panel (toggle with the **AI Assistant** button) that provides conversational access to an LLM. It operates outside the DAG — it doesn't create artifacts or participate in caching.

### Chat Mode (Enter)

Type a message and press Enter. The assistant streams a response with full conversation context:

- **Conversation memory**: prior turns are sent back to the LLM so follow-up questions work ("give an example of that", "now do it for column X")
- **Notebook context**: the current notebook state (cell sources, variable definitions, packages) is included in every request as a system prompt
- **Cell context**: optionally select a cell from the dropdown to focus the conversation on that cell's code and errors
- **Code insertion**: assistant responses with fenced code blocks show an "Insert Cell" button to add the code as a new notebook cell

The conversation resets when you click "Clear" or reload the page. History is session-only (not persisted to disk).

### Agent Mode (Shift+Enter)

Type an instruction and press Shift+Enter. The agent autonomously takes actions on the notebook:

**Available tools:**

| Tool | Description |
|------|------------|
| `get_notebook_state` | Read all cells, variables, and execution status |
| `create_cell` | Add a new Python or prompt cell |
| `edit_cell` | Modify an existing cell's source |
| `delete_cell` | Remove a cell |
| `run_cell` | Execute a cell and observe the result |
| `add_package` | Install a Python package via uv |

The agent runs as a background task with a 10-iteration limit. Progress events appear in the panel as they happen. You can cancel a running agent with the Cancel button.

**Example agent instructions:**

- "Add a cell that loads the iris dataset and prints its shape"
- "Install pandas and create a simple data analysis"
- "Fix the error in cell c3 and run it again"

The agent works best for additive tasks (creating new cells, installing packages). For complex refactoring, use Chat mode to discuss the approach first.
