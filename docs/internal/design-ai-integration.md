# AI Integration Design

## Status (April 2026)

This document describes how AI/LLM capabilities integrate into Strata Notebook.
The design follows Strata's layering model: orchestration decides what to run,
executors decide how to compute, and Strata decides whether the result already
exists and persists it.

## Why This Matters for Strata

LLM calls share every property that motivates Strata's existence:

- **Expensive** -- API calls cost real money and take seconds to minutes
- **Iterative** -- prompt engineering is evaluate-then-refine
- **Branching** -- try the same data with different models or temperatures
- **Failure-prone** -- rate limits, timeouts, model outages

The insight is that `materialize(inputs, transform) -> artifact` already handles
all of this. An LLM call with the same prompt, same model, and same input data
should return the cached result -- not bill you again.

## Design Decisions

### 1. No SDK dependencies — OpenAI-compatible API only

All major providers (Anthropic, OpenAI, Google, Mistral) and local servers
(Ollama, vLLM, LMStudio, llama.cpp) expose the OpenAI chat completions format.
One `httpx` call covers all of them. No `anthropic`, `openai`, or `litellm`
dependency needed.

```python
POST {base_url}/v1/chat/completions
{
  "model": "claude-sonnet-4-20250514",
  "messages": [{"role": "user", "content": "..."}],
  "temperature": 0.0,
  "max_tokens": 4096
}
```

### 2. Token-aware variable injection

Variables injected into prompts via `{{ var }}` are converted to text with size
limits to prevent context window blowouts and runaway costs. Users see estimated
token count before execution.

### 3. Phase 1 is the AI assistant panel, not prompt cells

Highest-impact, lowest-effort: a chat panel that generates Python cells. No new
execution model needed. Prompt cells come in Phase 2.

## Provider Configuration

In `notebook.toml`:

```toml
[ai]
base_url = "https://api.anthropic.com"  # or any OpenAI-compatible endpoint
model = "claude-sonnet-4-20250514"
temperature = 0.0
max_tokens = 4096
```

Or a local model:

```toml
[ai]
base_url = "http://localhost:11434/v1"  # Ollama
model = "llama3"
```

API key from environment: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or
`STRATA_AI_API_KEY` (checked in that order).

### Known provider base URLs

| Provider | Base URL | Env var |
|----------|----------|---------|
| Anthropic | `https://api.anthropic.com` | `ANTHROPIC_API_KEY` |
| OpenAI | `https://api.openai.com` | `OPENAI_API_KEY` |
| Google AI | `https://generativelanguage.googleapis.com/v1beta/openai` | `GEMINI_API_KEY` |
| Mistral | `https://api.mistral.ai` | `MISTRAL_API_KEY` |
| Ollama | `http://localhost:11434/v1` | (none) |
| vLLM | `http://localhost:8000/v1` | (none) |
| LMStudio | `http://localhost:1234/v1` | (none) |

## Variable Injection and Token Management

### Default text representations

| Python type | Default representation | If too large |
|---|---|---|
| DataFrame | `.head(20).to_markdown()` | `.describe().to_markdown()` |
| dict / list | `json.dumps(v, indent=2)` | Truncated with `... (N items)` |
| str | Raw text | First N chars + `... (truncated)` |
| int / float / bool / None | `str(v)` | Always fits |
| ndarray | Shape + dtype + first 5 rows | Shape + dtype only |

### Token estimation

Approximate: `len(text) / 4`. No tokenizer dependency. Good enough for budget
display and truncation decisions. Shown in the UI before execution.

### Per-variable token budget

Each `{{ var }}` gets a default budget of 2000 tokens (~8K chars). Configurable:

```
{{ df }}                  → default representation (head + truncate)
{{ df.describe() }}       → user controls what's injected (Python expression)
{{ df | tokens(4000) }}   → explicit per-variable token budget
```

The `{{ expr }}` syntax evaluates arbitrary Python expressions against the
upstream namespace. `{{ df.describe() }}` just works.

### Provenance hashing

The **rendered text** (after truncation) participates in the provenance hash,
not the raw variable. Same DataFrame truncated the same way = same hash = cache
hit. This is correct because the LLM sees the rendered text.

```
provenance = sha256(
    rendered_prompt_text
    + model_id
    + str(temperature)
    + system_prompt_text
)
```

## Implementation Phases

### Phase 1: AI Assistant Panel

A chat sidebar where users can:
- "Write a cell that joins df1 and df2 on user_id" → generates Python code
- Select error → "Explain this" → explanation + fix suggestion
- "Describe this data" → generates EDA code
- "Suggest a visualization" → generates matplotlib/seaborn code

The generated code becomes a normal Python cell. No new execution model.

**Backend:**

- `src/strata/notebook/ai.py` — provider abstraction (one function), prompt
  templates for generate/explain/describe actions
- `src/strata/notebook/routes.py` — `POST /v1/notebooks/{id}/ai/complete`
  endpoint proxying to the configured provider
- `src/strata/config.py` — AI config fields (`ai_base_url`, `ai_model`,
  `ai_api_key`)

**Frontend:**

- `frontend/src/components/AiPanel.vue` — chat UI with action buttons
- `frontend/src/composables/useStrata.ts` — `aiComplete()` API call
- `frontend/src/stores/notebook.ts` — AI actions (generate cell, explain error)

**Context sent to LLM:**

The assistant receives notebook context so it can write relevant code:
- Cell source code for all cells (truncated)
- Variable defines/references from DAG
- Current cell's error traceback (for explain action)
- Installed packages list

**Estimated effort:** 1-2 days.

### Phase 2: Prompt Cells

New `language="prompt"` cell type where source is a prompt template.

**Backend:**

- `src/strata/notebook/prompt_analyzer.py` — extract `{{ var }}` references
  for DAG building, estimate token usage per variable
- `src/strata/notebook/prompt_executor.py` — resolve upstream artifacts to text,
  render template, call LLM, parse response, store as artifact
- Variable-to-text conversion with per-variable token budgets
- Provenance hashing: `sha256(rendered_prompt + model + temperature + system)`

**Frontend:**

- `frontend/src/components/PromptCellEditor.vue` — prompt editor with:
  - Model selector dropdown
  - Temperature slider
  - Token budget display (estimated input tokens, per-variable breakdown)
  - Output type selector (text/json/code)
- Syntax highlighting for `{{ var }}` references

**DAG integration:** `{{ var }}` references create the same DAG edges as Python
variable references. An AI cell using `{{ df }}` depends on the cell defining `df`.

**Output types:**

- `text` (default) — raw string, stored as `json/object`
- `json` — parsed JSON, stored as `json/object`, downstream gets dict
- `code` — Python code string, can be executed by downstream Python cells

**Estimated effort:** 2-3 days.

### Phase 3: Token Tracking and Cost Visibility

- Record per-execution: `input_tokens`, `output_tokens`, `model`, `cost_usd`
- Store in artifact `transform_spec.params`
- Profiling panel shows: tokens used, cost, cost saved by cache hits
- Notebook-level token budget with warning when approaching limit

**Estimated effort:** 1 day.

### Phase 4: Server-Mode LLM Executor (Future)

- Register `llm@v1` as a transform executor in the server registry
- Centralized API key management (server holds keys, not notebooks)
- Per-tenant token budget QoS
- Audit log for all LLM calls

## System Prompt Management

- **Notebook-level:** `[ai] system_prompt = "..."` in `notebook.toml`
- **Cell-level override:** `# @system_prompt You are a data analyst.`
- Default system prompt provides notebook context (variables, packages, DAG)

## Open Questions (Resolved)

1. **Jinja2 vs simpler syntax?** → Simple `{{ expr }}` with Python eval. No
   Jinja2. Expressions are evaluated against the upstream namespace.

2. **System prompt?** → Notebook-level default + per-cell override via
   annotation.

3. **Streaming?** → No streaming for v1. Wait for completion, store artifact.
   Streaming complicates provenance (can't hash partial output). Add in v2.

4. **Multimodal?** → Defer to v2. Text-only for v1.

5. **Tool use?** → Defer. Creates DAG cycles. Needs separate design.

6. **SDK dependencies?** → None. OpenAI-compatible HTTP API via `httpx`. Works
   with all providers out of the box.

7. **Variable size?** → Per-variable token budget (default 2000). Token
   estimation via `len(text) / 4`. UI shows budget before execution.
