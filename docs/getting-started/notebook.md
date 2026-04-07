# Notebook Quickstart

Strata Notebook is an interactive notebook with content-addressed caching, automatic dependency tracking, and cascade execution.

## 1. Start the server

```bash
uv run strata-server
```

Open [http://localhost:8765](http://localhost:8765).

!!! tip "Docker alternative"
    ```bash
    docker compose up -d --build
    ```

## 2. Create a notebook

Click **New Notebook** on the landing page. Choose a name and parent directory.

Each notebook gets its own Python environment (managed by `uv`), so packages installed in one notebook don't affect others.

## 3. Write and run cells

Add a cell and type:

```python
x = 1
x + 1
```

Press **Shift+Enter** to run. The result appears below the cell.

## 4. Multi-cell dependencies

Add a second cell:

```python
y = x * 10
print(f"y = {y}")
```

Strata automatically detects that this cell references `x` from the first cell. The DAG in the sidebar shows the dependency.

If you change the first cell and re-run, Strata knows the second cell is **stale** and will offer to cascade-execute both.

## 5. Rich display outputs

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

### Multiple outputs

A cell can produce multiple visible outputs:

```python
display(Markdown("## Summary"))
42
```

## 6. Manage packages

Open the **Environment** panel in the sidebar to:

- Install and remove packages
- Import from `requirements.txt`
- Export dependencies
- Sync or rebuild the environment

## 7. Caching

When you re-run a cell with the same inputs and source, Strata returns the cached result instantly. The cell shows a **⚡ cached** badge with timing.

Change the source or any upstream cell, and the cache is automatically invalidated.

## Cell operations

| Action | How |
|--------|-----|
| Run cell | Shift+Enter or ▶ button |
| Add cell | **+** button in gutter or header |
| Delete cell | **×** button in gutter |
| Duplicate cell | **⎘** button in gutter |
| Move cell | **▲** / **▼** buttons in gutter |
| Keyboard help | Press **?** |

## What's next

- [Concepts](../notebook/concepts.md) — how the DAG, caching, and cascade work
- [Environment](../notebook/environment.md) — package management and Python versions
- [Docker deployment](../deployment/docker.md) — run in a container
