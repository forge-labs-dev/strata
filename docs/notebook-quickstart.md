# Strata Notebook Quick Start

Strata Notebook is the interactive notebook product built on top of the Strata
runtime and artifact model.

The recommended starting point is **personal mode** on your local machine.

## 1. Start the notebook server

```bash
STRATA_DEPLOYMENT_MODE=personal uv run strata-server
```

Then open:

```text
http://127.0.0.1:8765/#/
```

## 2. Create a notebook

- click `New Notebook`
- choose a notebook name and parent path
- choose a Python version if the deployment offers more than one

## 3. Run a simple cell

```python
x = 1
x + 1
```

## 4. Try rich display outputs

Markdown is injected into the notebook runtime and is available without an
import.

```python
display(Markdown("# First"))
42
```

```python
text = """
# Report

- item one
- item two
"""

Markdown(text)
```

```python
import matplotlib.pyplot as plt

plt.plot([1, 2, 3], [1, 4, 9])
plt.show()
Markdown("## done")
```

## 5. Manage the notebook environment

The notebook sidebar currently supports:

- install and remove packages with `uv`
- `requirements.txt` import/export
- `environment.yaml` best-effort import
- explicit environment sync / rebuild
- environment job progress and recent operation history

## Current Notebook Scope

The notebook runtime currently supports:

- create / open / rename / delete
- notebook-local Python environment management via `uv`
- create-time Python version selection
- PNG image display
- markdown display
- `display(...)` side effects
- `plt.show()` / `Figure.show()`
- ordered multiple visible outputs per cell

## Known Notebook Limitations

- no collaborative live notebook editing
- no full Jupyter MIME bundle compatibility
- no raw HTML notebook output support
- markdown is sanitized and display-only
- service-mode session discovery is intentionally restricted
- some richer display types such as SVG are still planned work

## Deployment Notes

- hosted personal-mode example:
  [docs/fly-notebook-smoke-checklist.md](fly-notebook-smoke-checklist.md)
- shared-backend service mode:
  [docs/service-mode-deployment.md](service-mode-deployment.md)

## Related Design Docs

- consolidated status view: [docs/design-status.md](design-status.md)
- notebook core design: [docs/design-notebook.md](design-notebook.md)
- notebook environments:
  [docs/design-notebook-environments.md](design-notebook-environments.md)
- display outputs:
  [docs/design-notebook-display-outputs.md](design-notebook-display-outputs.md)
