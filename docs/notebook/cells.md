# Cell Types

Strata Notebook has three cell kinds:

| Kind       | What it runs                              | Created by                                                        |
| ---------- | ----------------------------------------- | ----------------------------------------------------------------- |
| **Python** | Python source in the notebook's venv      | The default — any new cell                                        |
| **Prompt** | A text template sent to an LLM            | The "Add Prompt Cell" button in the UI                            |
| **Loop**   | A Python cell executed N times in a row   | Add a Python cell, then put a `# @loop` annotation at the top     |

All three participate in the DAG, cache by provenance hash, and can be routed to remote workers. Pick the kind that matches the shape of the computation — this page walks through each.

See [Concepts](concepts.md) for the execution model; see [Cell Annotations](annotations.md) for the full per-annotation reference.

---

## Python Cells

The default. A Python cell is just Python source — assignments at module scope become the cell's outputs, and free variables become inputs pulled from upstream cells.

### Writing a Python cell

```python
import pandas as pd

sales = pd.read_parquet("https://example.com/sales.parquet")
by_region = sales.groupby("region")["total"].sum()
```

This cell *defines* `sales` and `by_region`. A downstream cell that references either name will automatically depend on this one.

```python
# downstream cell — reads by_region from upstream
top_region = by_region.idxmax()
print(f"Top region: {top_region}")
```

### Variable flow and the DAG

Strata analyzes each cell's AST to extract:

- **Defines** — top-level assignments (`x = 1`, `df = pd.read_csv(...)`)
- **References** — free variables used but not defined locally

The DAG builder links references back to the **last** cell that defined each name (shadowing is handled by order). Edges flow producer → consumer. When you edit an upstream cell, every downstream cell that depends on it becomes stale automatically.

Only variables that a downstream cell actually references get stored as artifacts. Intermediate scratch variables stay in the subprocess and are discarded when the cell finishes.

### Module cells

A cell that contains only "pure" top-level statements is classified as a **module cell**. Its definitions get serialized as a synthetic Python module that downstream cells import transparently — so you can write a helper once and call it from anywhere in the notebook.

```python
# module cell
import numpy as np

STEP_SIZE = 0.5
CLASS_NAMES = ["cat", "dog", "fish"]

def himmelblau(x, y):
    return (x * x + y - 11) ** 2 + (x + y * y - 7) ** 2

class Config:
    lr = 1e-3
    batch = 32
```

Downstream cells can then reference `himmelblau(x, y)`, `STEP_SIZE`, `CLASS_NAMES`, or `Config.lr` directly.

#### What counts as "pure"

The classification is based on the cell's AST. Allowed at the top level:

- Module docstring
- `import X` / `from X import Y` (but not `from X import *`)
- `def` / `async def`
- `class`
- Assignments whose right-hand side is a **literal constant** — numbers, strings, bools, `None`, and nested tuples/lists/sets/dicts of literals. Negations of literals (`-1`, `~0`) count.

Anything else taints the whole cell:

- Assignments with a non-literal right-hand side (function calls, attribute access, arithmetic, name references): `x = compute()`, `PI = math.pi`, `X = y + 1`
- Augmented assignments: `x += 1`
- Expression statements: `print("hi")`, a bare trailing expression
- Control flow: `for`, `while`, `if`, `with`, `try`, `match`
- Bare annotations without a value: `x: int`
- `from … import *`

#### The failure mode (and how to avoid it)

Why does "pure" matter? Defs and classes can't be pickled reliably across the subprocess boundary, so they round-trip via **source reconstitution** — Strata saves the cell source, re-executes it on the other side, and hands the downstream cell the resulting module attribute. That only works if the source has no side effects, which is what the "pure" rule enforces.

If a cell mixes defs with non-literal runtime logic and a downstream cell tries to use one of those defs, execution fails with a clear error:

> This cell defines reusable code used downstream (`scaled`), but it cannot be shared across cells yet: top-level runtime state (assignments like `x = ...`) is not shareable across cells.

You'll also see a `module_export_blocked` annotation diagnostic on the cell before you even run it — pre-flight warning, not just a runtime surprise.

The fix is always the same: **split the cell**. Put the pure definitions in one cell (the module cell), and the runtime logic in a separate Python cell that references them:

```python
# cell A — module cell (pure)
STEP_SIZE = 0.5

def scale(x):
    return x * STEP_SIZE
```

```python
# cell B — runtime (can call scale and read STEP_SIZE)
result = scale(10) + STEP_SIZE
```

Plain-data cells (no defs or classes, just values) don't need module export at all — `THRESHOLD = 42` in its own cell serializes as a regular int and flows through the normal artifact path.

### Mutation warnings

If a cell mutates a value it received from an upstream cell (e.g. `df.drop(columns=[...], inplace=True)`), Strata raises a **mutation warning** — the upstream artifact was supposed to be immutable, and subsequent cells that reuse the cached artifact will see the mutated version.

The fix is to copy before mutating:

```python
df = upstream_df.copy()    # make a private copy
df.drop(columns=[...], inplace=True)
```

Warnings surface as a pill on the cell and a structured entry in the execution log.

### Python-cell annotations

| Annotation         | What it does                                         |
| ------------------ | ---------------------------------------------------- |
| `# @name X`        | Display name for the DAG view                        |
| `# @worker X`      | Route execution to a named remote worker             |
| `# @timeout 60`    | Override execution timeout (seconds, default 30)     |
| `# @env KEY=value` | Set an env var for this cell only                    |
| `# @mount …`       | Attach a filesystem mount (see [Annotations][a])     |
| `# @loop …`        | Turn the cell into a [loop cell](#loop-cells)        |

See [Cell Annotations][a] for the full reference.

[a]: annotations.md

---

## Prompt Cells

A prompt cell is a text template that gets rendered with upstream variable values, sent to an LLM, and the response stored as an artifact. Prompt cells participate in the DAG and cache by provenance exactly like Python cells — same inputs + same template + same model config = cache hit, no LLM call.

Create a prompt cell with the **"Add Prompt Cell"** button in the UI — the same toolbar that adds a Python cell. You never need to touch `notebook.toml` directly; editing the cell's source, wiring it into the DAG, and persisting the result all happen through the UI.

### Basic syntax

```
# @name summary
Summarize this dataset and return the top 3 findings as a numbered list:

{{ df }}
```

- `{{ df }}` is replaced with a text representation of the upstream variable `df` before sending to the LLM.
- The LLM's response is stored as an artifact named `summary` (from `# @name`).
- Downstream cells can read `summary` like any other upstream variable.

### Template syntax

Variables are injected with `{{ expression }}`. The expression is resolved against upstream cell outputs and converted to text using type-specific rules:

| Upstream type     | Text representation                         |
| ----------------- | ------------------------------------------- |
| pandas DataFrame  | Markdown table (first 20 rows)              |
| pandas Series     | String representation (first 20 values)     |
| numpy ndarray     | Shape + dtype + first 10 elements           |
| dict / list       | JSON, indented                              |
| str / int / float | Direct string conversion                    |

Each variable has a 2,000-token budget per template render. Oversized values are truncated with a `... (truncated)` marker.

**Attribute access** is supported for safe read-only operations:

```
{{ df.describe() }}     # OK — pandas describe() is allow-listed
{{ df.head() }}         # OK
{{ obj.attr }}          # OK — attribute access (non-callable)
{{ obj.mutate() }}      # blocked — unknown method, left as-is in the template
```

Only a small set of methods is permitted (`describe`, `head`, `tail` on pandas objects). Arbitrary method calls are blocked to keep template rendering side-effect-free.

### Prompt-cell annotations

| Annotation               | What it does                                                               | Default               |
| ------------------------ | -------------------------------------------------------------------------- | --------------------- |
| `# @name <identifier>`   | Output variable name; must be a Python identifier                          | `result`              |
| `# @model <model_id>`    | Override the notebook-level LLM model                                      | From provider config  |
| `# @temperature <float>` | Sampling temperature (0.0 = deterministic; see [Caching](#caching) below)  | `0.0`                 |
| `# @max_tokens <int>`    | Response token ceiling                                                     | `4096`                |
| `# @system <text>`       | System prompt prepended to the request                                     | None                  |
| `# @output json\|text`   | Coerce the response to JSON (or keep as free-form text)                    | `text`                |
| `# @output_schema {…}`   | Inline JSON Schema pinning the response shape                              | None                  |
| `# @validate_retries N`  | Total attempts for the validate-and-retry loop (1 initial + N−1 retries)   | `3`                   |

Example using several at once:

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

### Schema-constrained output

`# @output_schema {...}` pins the shape of the LLM response to an inline JSON Schema. Strata picks the best provider-native path:

| Provider                                          | Enforcement                                                                                    |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **OpenAI**                                        | Native `response_format: {type: "json_schema"}`. `additionalProperties: false` is auto-injected at every `object` node; strict mode is used when the user's `required` list covers every property (otherwise relaxed to `strict: false`). |
| **Anthropic**                                     | Native `/v1/messages` with tool-use: the schema is sent as a tool's `input_schema` and `tool_choice` is forced to that tool. The returned `tool_use.input` is extracted verbatim. |
| **Gemini / Mistral / Ollama / vLLM**              | Fallback to `response_format: {type: "json_object"}` — valid JSON guaranteed, shape not enforced server-side. Client-side validation (see below) fills the gap. |

Setting `@output_schema` implies `@output json`; you don't need both.

Example — triage each review into a structured record:

````
# @name triage
# @output_schema {"type":"object","properties":{"items":{"type":"array","items":{"type":"object","properties":{"sentiment":{"type":"string","enum":["positive","negative","neutral"]},"priority":{"type":"string","enum":["low","medium","high"]},"tags":{"type":"array","items":{"type":"string"}}},"required":["sentiment","priority","tags"]}}},"required":["items"]}
Triage these customer reviews:

{{ reviews }}

For each review return sentiment, priority, and 1–3 short tags.
````

A downstream cell can then destructure without regex-wrangling:

```python
import pandas as pd
df = pd.DataFrame(triage["items"])
print(df["priority"].value_counts())
```

### Validate-and-retry

When `@output_schema` is set, Strata runs a **validate-and-retry loop** after every LLM call:

1. Parse the response as JSON and run it through `jsonschema`.
2. On success → store the artifact and return.
3. On failure → append the bad response as an `assistant` turn, feed the validator's path-addressed errors back as a `user` turn, and retry.
4. On retry exhaustion → surface a cell error with the last validator messages.

The default is 3 total attempts (1 initial + 2 retries). Override with `# @validate_retries N`. Cumulative input/output tokens across all attempts are recorded on the artifact so cost accounting is accurate. The retry count is surfaced on the cell result (`validation_retries`) — the UI shows "validated after N retries" when non-zero.

Retries are mostly invisible on OpenAI-strict and Anthropic-native paths because the provider enforces the schema at decode time. They earn their keep on the `json_object` fallback path (Gemini, Mistral, Ollama) where the provider only guarantees *syntactic* JSON.

### Caching

A prompt cell's provenance hash mixes together:

- The rendered template text (after `{{ var }}` injection)
- Model name
- Temperature
- System prompt
- Output type (`json` / `text`)
- Output schema fingerprint (when set)

Editing any of these invalidates the cache. In particular, tweaking `@output_schema` on a cached cell forces a fresh call — exactly what you want when iterating on the response shape.

!!! tip "Keep temperature at 0.0 for prompt cells"
    With `temperature=0.0` the model is deterministic: same inputs → same output, and cache behavior is intuitive. Bumping temperature makes the first response "sticky" in the cache — future runs return the stored stochastic sample rather than re-sampling.

See [LLM Integration](llm.md) for provider configuration and the conversational AI assistant.

---

## Loop Cells

A loop cell is a regular Python cell with a `# @loop` annotation. The body runs N times, with a **carry variable** threaded between iterations. Each iteration's state is stored as its own artifact, so you can inspect any intermediate step.

Use loop cells for iterative refinement (hill climbing, MCMC, training loops with checkpoints), simulations, and anything where you'd want to pause and inspect intermediate states — or fork a new run from a promising one.

### Minimal example

Two cells: a seed and a loop.

```python
# seed cell — initial carry state
state = {"x": 0.0, "best_score": float("inf"), "iter": 0}
```

```python
# loop cell
# @loop max_iter=40 carry=state
# @loop_until state["best_score"] < 1e-3
import random

# Each iteration: read `state`, compute the next step, rebind `state`.
candidate = state["x"] + random.uniform(-0.1, 0.1)
score = candidate ** 2   # some objective
if score < state["best_score"]:
    state = {**state, "x": candidate, "best_score": score, "iter": state["iter"] + 1}
else:
    state = {**state, "iter": state["iter"] + 1}
```

After execution, `state` holds the final iteration's value and every intermediate iteration is queryable.

### Required directives

| Directive                | What it does                                                      |
| ------------------------ | ----------------------------------------------------------------- |
| `# @loop max_iter=N`     | Hard cap on iterations. Required — the safety bound on the loop.  |
| `# @loop carry=VAR`      | The variable threaded between iterations. Required. Must be re-bound by the cell body each iteration, and seeded by an upstream cell on iteration 0. |

These can be on the same line: `# @loop max_iter=40 carry=state`.

### Optional directives

| Directive                         | What it does                                                                          |
| --------------------------------- | ------------------------------------------------------------------------------------- |
| `# @loop_until <expr>`            | Early termination when `<expr>` is truthy (evaluated against the current `state`)     |
| `# @loop start_from=<cell>@iter=k` | Seed iteration 0 from a specific prior iteration's artifact — used for forking runs   |

### Per-iteration artifacts

Every iteration's carry value becomes its own artifact with an `@iter=k` suffix:

```
strata://artifact/nb_..._cell_<loop_id>_var_state@v=1@iter=0
strata://artifact/nb_..._cell_<loop_id>_var_state@v=1@iter=1
...
```

The inspect panel shows an iteration picker so you can scrub through the intermediate states. The **final** iteration's artifact is also the cell's canonical output (no `@iter` suffix) — downstream cells read it via the normal DAG path.

### Forking a loop

Intermediate iterations are first-class artifacts, so you can branch a new
run from any step of an old one without re-running the expensive prefix.

**Scenario.** You ran a hill-climbing search for 50 iterations. Glancing at
the inspect panel, iteration 17 looked like it was about to find a better
local optimum before the sampler drifted away. You want to explore what
happens if you push harder from that exact state with a different step size.

1. Open the loop cell's **Inspect** panel, scrub to iteration 17, copy its
   artifact URI. It'll look like
   `strata://artifact/nb_..._cell_hill_climb_var_state@v=1@iter=17`.
2. Add a new loop cell below. Reference the original cell's ID (not the full
   URI) in `start_from`:

    ```python
    # new loop cell — continues from iteration 17 of the previous run
    # @loop max_iter=20 carry=state start_from=hill_climb@iter=17
    state["step_size"] *= 0.5  # smaller steps from here on
    state = sample_and_score(state)
    ```

3. Run the new cell. It reads iteration 17's carry value as its seed, runs up
   to 20 more iterations under the modified strategy, and stores those
   iterations as its own artifact chain — the original run stays untouched.

You now have two parallel forks materialized in the artifact store. Either
one can be forked further, and the inspect panel shows both chains.

This is the escape hatch for "that intermediate state looked promising, let
me explore from there" — the thing that's hard to do in a plain for-loop
once you've thrown away the intermediates.

### When not to use a loop cell

- Tight `for` loops over short collections — a regular Python cell with a `for` loop is simpler and the extra per-iteration artifact overhead isn't worth it.
- Loops where intermediate state is genuinely disposable — store only the final answer in a regular Python cell.
- Anything that needs to branch out into multiple parallel runs — loop cells are sequential by design. Use separate cells, or model the fan-out in Python.

Reach for loop cells when **being able to inspect or fork from iteration k matters**. That's the feature you're paying for.

---

## Choosing between kinds

| Reach for a…  | When you want…                                                                     |
| ------------- | ---------------------------------------------------------------------------------- |
| Python cell   | Ordinary computation. Default.                                                     |
| Prompt cell   | An LLM response as a first-class, cached, DAG-participating artifact.              |
| Loop cell     | Iterative refinement where pausing or forking from an intermediate state matters.  |

Mixing is encouraged — a typical LLM-assisted pipeline is Python cells for data prep → prompt cell for extraction → Python cells for aggregation.
