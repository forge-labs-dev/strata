# AI Integration Design

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
all of this.  An LLM call with the same prompt, same model, and same input data
should return the cached result -- not bill you again.

## Integration Tiers

### Tier 1: AI Cell Type

A cell where `source` is a prompt template instead of Python code.  Upstream
variables are injected as context.  The LLM response becomes the output artifact.

```
# Cell source (prompt template)
Summarize the following dataset in 3 bullet points:

{{ df.describe() }}
```

**Provenance hashing**:
```
sha256(prompt_template_hash + model_config_hash + sorted(input_artifact_hashes))
```

Same prompt + same model + same inputs = cache hit.  Change the prompt or the
upstream data, and it re-executes.

**Model configuration** lives in `notebook.toml` or cell annotations:
```toml
[ai]
default_model = "claude-sonnet-4-20250514"
default_temperature = 0.0
```

```python
# @model claude-sonnet-4-20250514
# @temperature 0.0
# @max_tokens 4096
Summarize {{ df }} by category.
```

**Output types**:
- `text` -- raw string (default)
- `json` -- parsed JSON object, stored as `json/object` artifact
- `code` -- generated Python code (can feed into downstream Python cells)
- `arrow` -- structured extraction into a table

The output type is either inferred from the prompt or set via annotation:
```python
# @output json
Extract entities from {{ text }} as a JSON array.
```

**Variable injection** uses Jinja2-style `{{ var }}` syntax.  Before sending
the prompt, the executor:
1. Resolves each referenced variable from upstream artifacts
2. Converts to a text representation (DataFrame -> `.describe()` or `.head()`,
   scalar -> `str()`, dict -> JSON)
3. Renders the template
4. Sends to the model API

**DAG integration**: The analyzer treats `{{ var }}` references the same as
Python variable references for DAG building.  An AI cell that uses `{{ df }}`
depends on the cell that defines `df`.

### Tier 2: LLM Transform Executor

Register LLM providers as transform executors in the server-mode registry:

```toml
[[transforms]]
ref = "llm@v1"
executor_url = "http://localhost:9000/v1/execute"
timeout_seconds = 120
max_output_bytes = 10_485_760
```

The executor receives input artifacts via the standard executor protocol
(push or pull model) and returns structured output.  This enables:

- Server-side LLM execution with centralized API key management
- Build QoS admission control (token budget instead of byte budget)
- Multi-tenant isolation (each tenant's LLM calls are tracked separately)

**Executor protocol** (extends `notebook-cell-v1`):

```
POST /v1/execute
Content-Type: application/json

{
  "model": "claude-sonnet-4-20250514",
  "prompt_template": "Summarize {{ input0 }}",
  "parameters": {
    "temperature": 0.0,
    "max_tokens": 4096
  },
  "output_type": "json",
  "inputs": {
    "input0": {"artifact_uri": "strata://artifact/abc@v=1", "content_type": "arrow/ipc"}
  }
}
```

### Tier 3: AI-Assisted Authoring

Copilot-style integration in the notebook UI:

- **Cell generation** -- natural language to Python cell
- **Error explanation** -- parse traceback, suggest fix
- **Data exploration** -- "describe this dataframe", "suggest a visualization"
- **Cell refactoring** -- "split this cell into two", "add error handling"

This is a frontend/UX feature that calls an LLM API directly from the UI layer
or via a lightweight backend endpoint.  It does not participate in the compute
graph -- the generated code becomes a normal Python cell.

## Architecture

### AI Cell Execution Flow

```
1. User writes prompt template in AI cell
2. Analyzer extracts {{ var }} references -> DAG edges
3. Cascade planner ensures upstream cells are ready
4. Executor resolves upstream artifacts -> text representations
5. Render prompt template with resolved variables
6. Compute provenance hash (template + model + inputs)
7. Check artifact cache -> return on hit
8. Call LLM API (via configured provider)
9. Parse response according to output_type
10. Store result as artifact (text, json, or arrow)
11. Downstream cells can consume the output as a normal variable
```

### Provider Abstraction

```python
class LLMProvider(Protocol):
    async def complete(
        self,
        model: str,
        messages: list[dict],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> str: ...
```

Built-in providers:
- `anthropic` -- Claude models via the Anthropic API
- `openai` -- GPT models via the OpenAI API
- `http` -- Generic OpenAI-compatible endpoint (vLLM, Ollama, etc.)

Provider configuration in `notebook.toml`:
```toml
[ai.provider]
type = "anthropic"
# API key from environment: ANTHROPIC_API_KEY
```

### Token Budget and Cost Tracking

Each AI cell execution records:
- `input_tokens` -- tokens sent (prompt + context)
- `output_tokens` -- tokens received
- `model` -- model identifier
- `cost_estimate_usd` -- estimated cost based on model pricing

These are stored in the artifact's `transform_spec.params` and surfaced in the
profiling panel.  Cache hits show the cost that was *avoided*.

## Provenance and Caching Details

### What Participates in the Hash

| Component | In provenance hash? | Rationale |
|---|---|---|
| Prompt template text | Yes | Different prompt = different result |
| Model identifier | Yes | Different model = different result |
| Temperature | Yes | Different temperature = potentially different result |
| max_tokens | No | Affects truncation, not content identity |
| Input artifact hashes | Yes | Different data = different result |
| API key | No | Authentication, not computation |
| Provider endpoint | No | Same model at different endpoints = same result |

### Temperature = 0 Special Case

When `temperature=0`, the LLM output is (approximately) deterministic.  The
cache hit rate should be high.  When `temperature > 0`, the same inputs can
produce different outputs.  Two options:

1. **Cache anyway** (default) -- first execution wins, subsequent identical
   requests return the cached result.  This is correct for iterative workflows
   where you want stability.
2. **`refresh=True`** -- force re-execution.  Uses the existing refresh
   mechanism that generates a unique provenance hash.

### Structured Output Caching

When `output_type=json`, the parsed JSON object is stored as a `json/object`
artifact.  Downstream Python cells receive it as a dict:

```python
# AI cell (output_type=json)
# @output json
Extract the top 5 entities from {{ text }}.

# Python cell (downstream)
for entity in ai_result:
    print(entity["name"], entity["score"])
```

When `output_type=arrow`, the executor parses the LLM response into a PyArrow
table (e.g., from CSV or JSON array output) and stores it as `arrow/ipc`.
Downstream cells receive a pandas DataFrame.

## Implementation Plan

### Phase 1: AI Cell Type (MVP)

- [ ] New `language="prompt"` cell type in `notebook.toml`
- [ ] `PromptAnalyzer` -- extract `{{ var }}` references for DAG
- [ ] `PromptRenderer` -- resolve upstream artifacts and render template
- [ ] `LLMExecutor` -- call provider API, parse response, store artifact
- [ ] Provenance hashing with model config
- [ ] Frontend: prompt cell editor with model/temperature controls
- [ ] Provider: Anthropic (Claude) as first implementation

### Phase 2: Multi-Provider and Structured Output

- [ ] OpenAI provider
- [ ] Generic HTTP provider (vLLM, Ollama)
- [ ] `output_type=json` with schema validation
- [ ] `output_type=arrow` with CSV/JSON-to-table parsing
- [ ] Token usage tracking and cost display in profiling panel

### Phase 3: Server-Mode LLM Executor

- [ ] Register `llm@v1` as a transform executor
- [ ] Token budget QoS (per-tenant daily token limits)
- [ ] Centralized API key management (server holds keys, not notebooks)
- [ ] Audit log for LLM calls (model, tokens, cost, principal)

### Phase 4: AI-Assisted Authoring

- [ ] `/ai generate` -- natural language to Python cell
- [ ] `/ai explain` -- error explanation with fix suggestion
- [ ] `/ai describe` -- dataset summary and visualization suggestions
- [ ] Context-aware completions (knows upstream variables and types)

## Open Questions

1. **Jinja2 vs simpler syntax?** Jinja2 is powerful but adds a dependency and
   complexity (loops, conditionals in prompts).  A simpler `{{ var }}` regex
   replacement may be sufficient for v1.

2. **System prompt management?** Should there be a notebook-level system prompt
   that applies to all AI cells?  Or per-cell only?

3. **Streaming?** LLM responses stream token-by-token.  Should the notebook UI
   show streaming output, or wait for completion?  Streaming is better UX but
   complicates artifact storage (can't hash until complete).

4. **Image/multimodal inputs?** Claude and GPT-4V support image inputs.  Should
   AI cells accept image artifacts from upstream cells?  This affects the
   variable-to-text rendering pipeline.

5. **Tool use / function calling?** Should AI cells support tool use, where the
   LLM can call back into Python cells?  This creates cycles in the DAG and
   needs careful design.
