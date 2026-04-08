# Notebook Quickstart

Strata Notebook is an interactive notebook with content-addressed caching, automatic dependency tracking, and cascade execution.

## 1. Start the Server

=== "Docker"

    ```bash
    docker compose up -d --build
    ```

=== "From source"

    ```bash
    uv run strata-server
    ```

Open [http://localhost:8765](http://localhost:8765).

## 2. Create a Notebook

Click **New Notebook** on the landing page. Choose a name and parent directory.

Each notebook gets its own Python environment (managed by `uv`), so packages installed in one notebook don't affect others.

## 3. Write and Run Cells

Add a cell and type:

```python
x = 1
x + 1
```

Press ++shift+enter++ to run. The result appears below the cell.

## 4. Multi-Cell Dependencies

Add a second cell:

```python
y = x * 10
print(f"y = {y}")
```

Strata automatically detects that this cell references `x` from the first cell. The DAG in the sidebar shows the dependency arrow.

!!! info "Cascade execution"
    If you change the first cell and re-run the second, Strata detects the staleness and offers to cascade-execute both cells in the correct order.

## 5. Rich Display Outputs

### Markdown

The `Markdown` helper is injected into every cell's namespace:

```python
Markdown("# Hello\n\n- item one\n- item two")
```

### Matplotlib

```python
import matplotlib.pyplot as plt

plt.plot([1, 2, 3], [1, 4, 9])
plt.show()
```

!!! tip "Install matplotlib first"
    Open the **Environment** panel in the sidebar and add `matplotlib` before running plot cells.

### Multiple Outputs

A cell can produce multiple visible outputs:

```python
display(Markdown("## Summary"))
42
```

### DataFrames

```python
import pandas as pd

df = pd.DataFrame({
    "name": ["Alice", "Bob", "Carol"],
    "score": [95, 87, 92],
})
df
```

DataFrames render as scrollable tables with column headers and row counts.

## 6. Manage Packages

Open the **Environment** panel in the sidebar to:

- Install and remove packages
- Import from `requirements.txt`
- Export dependencies
- Sync or rebuild the environment

See [Environment Management](../notebook/environment.md) for details.

## 7. Caching

When you re-run a cell with the same inputs and source, Strata returns the cached result instantly. The cell shows a **⚡ cached** badge with timing.

Change the source or any upstream cell, and the cache is automatically invalidated.

## 8. Try an Example

Open one of the bundled example notebooks to see a real workflow:

```
examples/iris_classification/     # ML pipeline with scikit-learn
examples/pandas_basics/           # DataFrame operations
examples/titanic_ml/              # End-to-end ML with multiple models
```

Use the **Open Existing** button on the landing page and paste the path.

## Cell Operations

| Action | How |
|--------|-----|
| Run cell | ++shift+enter++ or ▶ button |
| Add cell | **+** button in gutter or header |
| Delete cell | **×** button in gutter |
| Duplicate cell | **⎘** button in gutter |
| Move cell | **▲** / **▼** buttons in gutter |
| Keyboard help | Press ++question++ |

## What's Next

- [Concepts](../notebook/concepts.md) — how the DAG, caching, and cascade work
- [Environment](../notebook/environment.md) — package management and Python versions
- [Keyboard Shortcuts](../notebook/keyboard.md) — all available shortcuts
- [Docker deployment](../deployment/docker.md) — run in a container
