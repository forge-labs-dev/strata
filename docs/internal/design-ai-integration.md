# AI Integration Design

## Status (April 2026)

This area is **partially implemented**.

Shipped now:

- prompt cells (`language="prompt"`) with provenance-aware caching
- notebook-scoped LLM configuration resolution
- assistant chat and streaming responses
- assistant agent mode with notebook-editing tools
- environment-job-aware `add_package` tool behavior

Still on the roadmap:

- cost/token visibility in the notebook UI
- richer provider/model management UX
- broader multimodal support
- more capable tool use beyond the current notebook-editing surface
- stronger hosted/service-mode operational controls

See [design-status.md](design-status.md) for the consolidated shipped vs roadmap
view.

## Why AI Fits Strata

LLM calls share the same properties that motivated Strata's execution model:

- they are expensive
- they are iterative
- they depend on explicit inputs and configuration
- they benefit from deterministic reuse when the effective request is unchanged

The important architectural point is unchanged:

```text
materialize(inputs, transform) -> artifact
```

Prompt cells fit this directly. If the rendered prompt text, provider/model
configuration, and execution parameters are unchanged, the result should be a
cache hit rather than another billable API call.

## Current Product Surface

### 1. Prompt Cells

Prompt cells are first-class notebook cells whose source is a prompt template.

Current semantics:

- upstream references inside `{{ ... }}` create notebook dependencies
- the rendered prompt text participates in provenance
- annotations such as `@name`, `@model`, `@temperature`, `@max_tokens`, and
  `@system` affect execution and provenance
- the prompt response is stored as a notebook artifact and reused on cache hit

### 2. AI Assistant

The assistant is a sidebar surface outside the DAG.

Current capabilities:

- blocking completion endpoint
- streaming chat endpoint
- agent mode with iterative notebook tool use
- notebook context injection so responses are aware of cells, variables, and
  current state

The assistant is intentionally not the same thing as prompt-cell execution:

- assistant conversations are session-oriented
- prompt cells are notebook artifacts with provenance and cache semantics

## Configuration Resolution

LLM configuration resolves from three layers, highest priority last:

1. server config (`STRATA_AI_*`)
2. notebook runtime env vars (for example `OPENAI_API_KEY`)
3. notebook `[ai]` config in `notebook.toml`

Current `[ai]` support includes:

- `base_url`
- `model`
- `api_key`
- `max_context_tokens`
- `max_output_tokens`
- `timeout_seconds`

Recommended practice:

- keep API keys in notebook runtime env vars or server config
- use `[ai]` primarily for model/endpoint overrides and notebook-specific tuning

## Prompt Template Rules

Template rendering is intentionally constrained.

Supported today:

- direct variable references: `{{ df }}`
- attribute access: `{{ obj.value }}`
- a small allowlist of safe pandas methods:
  - `describe()`
  - `head()`
  - `tail()`

Explicit non-goals in the current implementation:

- arbitrary Python evaluation during template rendering
- side-effecting method calls
- Jinja-style filter syntax such as `{{ df | tokens(4000) }}`

If a template expression is unsupported, it is not executed as arbitrary Python.

## Provider Model

The implementation intentionally uses the OpenAI-compatible chat-completions
shape so the same code path can target:

- OpenAI
- Anthropic-compatible endpoints
- Google Gemini's OpenAI-compatible endpoint
- Mistral
- Ollama
- vLLM / TGI / LiteLLM-style deployments

That keeps the runtime dependency surface small and shifts provider choice into
configuration rather than SDK branching.

## Provenance and Caching

Prompt-cell provenance depends on the effective request, not the raw notebook
source alone.

Important inputs include:

- rendered prompt text
- selected model
- temperature
- system prompt
- output name / artifact identity

The cache contract is:

- same effective request -> cache hit
- changed upstream data or changed prompt/model config -> recompute

## Assistant / Agent Boundaries

The assistant and agent are intentionally bounded by notebook execution rules.

Current guardrails:

- package installs go through notebook environment jobs
- environment mutation and execution exclusion still applies
- agent actions operate through notebook APIs rather than direct hidden state

This is important because the assistant should not become a second, weaker
execution plane that bypasses notebook semantics.

## Remaining Roadmap

### Near-term

- notebook-visible token and cost accounting
- better provider/model discovery UX
- more precise documentation of hosted/service-mode AI behavior

### Later

- multimodal prompt inputs and outputs
- broader assistant tool surface
- hosted/service-mode authorization and policy controls for AI features
- possible future LLM transform alignment with the broader Strata execution plane
