# Strata Notebook — Display Outputs Design

## Overview

Strata notebooks currently have a narrow visible-output model:

- console output (`stdout` / `stderr`)
- scalar / JSON output
- tabular output (`arrow/ipc`)

That is enough for data inspection, but not for common notebook workflows like:

- returning a matplotlib figure
- returning a PIL image
- rendering markdown explanations inline

This document defines a richer **display output** model for notebook cells.

The key design decision is:

- **DAG artifacts** and **visible display outputs** are related, but they are not
  the same thing.

Today the runtime mostly infers visible output from the special `_` variable during
live execution. That is too narrow for images and too implicit for long-term growth.
The next implementation should introduce display outputs as a first-class concept.

---

## Goals

- Support inline display of plots and images.
- Preserve display outputs across:
  - cache hits
  - notebook reopen
  - notebook refresh / websocket reconnect
- Keep the DAG and artifact model correct:
  - downstream-consumed variables remain normal artifacts
  - display-only outputs do not accidentally become DAG dependencies
- Create a path to future display types, especially markdown.
- Keep the first implementation small enough to ship quickly.

## Non-Goals

- Full IPython/Jupyter display protocol compatibility in v1
- Arbitrary unsanitized HTML rendering
- Multiple independent display outputs per cell in the first phase
- `plt.show()` / rich display hook capture in the first phase
- A generic frontend plugin system for renderers

---

## Current State (April 2026)

What exists today:

- the harness executes a cell and captures:
  - named outputs
  - `_` when the last statement is a bare expression
  - console output
- the serializer supports:
  - `arrow/ipc`
  - `json/object`
  - module export formats
  - `pickle/object`
- the frontend renders:
  - tables
  - scalars / JSON
  - errors
  - console output

Important limitations:

- `image/png` exists in frontend types but is not actually emitted or rendered
- visible output is reconstructed from `outputs["_"]` instead of a dedicated display model
- display output is not a first-class persisted concept
- `plt.show()` does nothing useful for display because only the returned value is captured

---

## Design Principles

1. **Separate computation artifacts from visible display**

   DAG artifacts exist for dependency tracking, caching, and downstream execution.
   Display outputs exist for what the user sees in the notebook.

2. **One primary display output first**

   The initial design should support one primary visible display result per cell.
   Multiple display outputs can come later.

3. **Persist display output like a real notebook result**

   If a cell showed an image before refresh, it should still show that image after
   refresh or reopen when the result is still current.

4. **Prefer explicit content types**

   Rendering should key off an explicit display content type, not implicit guessing
   from JSON shape or file extension.

5. **Treat markdown as a display type, not as generic HTML**

   Markdown is useful and likely desirable. Raw HTML is a separate and riskier
   capability and should not be implied by markdown support.

---

## Proposed Model

### Two Result Planes

Each successful cell execution can produce two related but distinct result planes:

1. **Exported outputs**
   - variable artifacts for DAG/dataflow
   - used by downstream cells
   - persisted in the artifact store as they are today

2. **Primary display output**
   - the visible result shown in the cell output area
   - may be derived from `_` (last expression result)
   - may or may not correspond to a downstream-consumed variable
   - persisted separately from DAG artifacts

This avoids conflating “what users see” with “what downstream cells consume.”

### Cell Runtime State

Each cell should carry a dedicated display payload in runtime state:

```json
{
  "display": {
    "content_type": "image/png",
    "artifact_uri": "strata://artifact/...",
    "bytes": 18342,
    "width": 800,
    "height": 600,
    "inline_data_url": "data:image/png;base64,..."
  }
}
```

The exact fields can vary by content type, but the important point is that
`display` is explicit and separate from `outputs`.

### Persistence

The display payload should survive:

- cache-hit restoration
- notebook reopen
- websocket `notebook_sync`

That means display output must not live only inside the transient `cell_output`
websocket event. It needs a notebook/session representation similar to other
runtime-derived cell metadata.

---

## Content Types

### Phase 1

The first implementation should add:

- `image/png`

Supported source objects for `image/png`:

- `matplotlib.figure.Figure`
- `PIL.Image.Image`
- objects exposing `_repr_png_()`

### Planned Next Types

- `text/markdown`
- possibly `image/svg+xml`

### Types Not In Scope Yet

- `text/html`
- arbitrary embedded JavaScript
- arbitrary raw MIME bundle support

---

## Execution Semantics

### Phase 1 Rule

The primary display output comes from the cell’s last-expression result (`_`),
not from display side effects.

That means this should work:

```python
import matplotlib.pyplot as plt

fig, ax = plt.subplots()
ax.plot([1, 2, 3], [1, 4, 9])
fig
```

This should **not** be a phase-1 requirement:

```python
import matplotlib.pyplot as plt

plt.plot([1, 2, 3], [1, 4, 9])
plt.show()
```

Why this constraint is correct for v1:

- it fits the existing harness model
- it avoids implementing a Jupyter-style display hook system immediately
- it is enough to support intentional plot/image return values

### Future Display Capture

A later phase can capture rich display side effects:

- `plt.show()`
- `display(obj)`
- multiple visible outputs in order

That should be a separate extension once the primary display model exists.

---

## Serializer Design

### Phase 1 Extension

Extend the notebook serializer to detect and serialize image-like values before
falling back to pickle.

Recommended order:

1. `_repr_png_()`
2. matplotlib figure
3. PIL image
4. existing JSON / module / pickle logic

Serializer output for images should include:

- `content_type: "image/png"`
- `file`
- `bytes`
- optional metadata such as width / height

### Artifact Storage

Display images should be stored as normal artifacts so they can be:

- cached
- reloaded after reopen
- reused on cache hit

But they should be referenced from the cell’s display payload, not only from the
consumed-variable artifact map.

---

## Frontend Rendering Model

The frontend should stop inferring the visible result solely from `outputs["_"]`.

Instead:

- prefer `cell.display` when present
- keep a temporary compatibility path that can still interpret `_` during rollout

### Image Rendering

For `image/png`, the cell output area should:

- render the image inline
- constrain width to the cell content width
- preserve aspect ratio
- allow the user to open/save the source image later if we add that affordance

### Markdown Rendering

Markdown should be designed now even if implementation comes later.

Recommended design:

- content type: `text/markdown`
- rendered as sanitized HTML in the frontend
- no raw HTML pass-through by default

Important rule:

- markdown support should not imply full HTML support

That means the markdown pipeline should sanitize aggressively and either strip or
escape raw HTML blocks rather than execute them.

---

## Markdown Considerations

Markdown is useful for:

- narrative explanation cells
- model summary commentary
- inline experiment notes
- rich text next to plots and tables

But it is also where notebook systems often blur the boundary between content and
code execution. Strata should keep that boundary clear.

### Recommended Markdown Scope

When implemented, markdown display should support:

- headings
- emphasis
- lists
- links
- code blocks
- tables
- blockquotes
- inline images only if explicitly allowed later

### Security Posture

Markdown rendering should:

- sanitize generated HTML
- disallow script execution
- disallow inline event handlers
- treat raw HTML conservatively

Markdown should remain **display-only**. It should not become a route for
injecting arbitrary executable browser content into the notebook UI.

---

## API and WebSocket Shape

### Current Problem

Live execution currently sends:

- `outputs`
- `stdout`
- `stderr`

and the frontend reconstructs the visible output from `outputs["_"]`.

### Proposed Shape

Successful cell execution payloads should carry:

```json
{
  "cell_id": "cell_123",
  "outputs": { "...": "..." },
  "display": { "...": "..." },
  "stdout": "...",
  "stderr": "..."
}
```

Notebook/session serialization should do the same so that:

- `cell_output`
- `notebook_state`
- route open / reopen

all use the same display model.

---

## Rollout Plan

### Phase 1: Primary Display Output + Images

- add a first-class `display` payload
- persist display metadata in session/runtime state
- support `image/png`
- render returned matplotlib figures and PIL images
- keep `_`-based compatibility during rollout

### Phase 2: Markdown

- add `text/markdown`
- frontend markdown renderer with sanitization
- persisted markdown display payload

### Phase 3: Rich Display Capture

- support `plt.show()`
- support explicit display hooks
- support multiple visible outputs per cell

---

## Test Plan

### Backend

- serializer detects matplotlib figure and PIL image
- serializer emits `image/png` metadata
- executor persists display artifact for returned figure
- cache-hit/open paths restore display payload

### Frontend

- store parses display payload
- cell renderer shows inline image
- markdown renderer is covered when markdown ships

### End-to-End

- execute a notebook cell returning a matplotlib figure
- verify image renders
- refresh / reopen notebook
- verify image still renders

---

## Recommendation

Implement **Phase 1** next, but do it on top of the explicit `display` model.

That gives Strata:

- plots/images that users actually expect
- a coherent persistence story
- a clean path to markdown
- no commitment yet to full Jupyter display semantics
