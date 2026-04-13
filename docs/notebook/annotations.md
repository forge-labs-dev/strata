# Cell Annotations & Environment

Annotations are metadata directives written in the leading comment block of a cell. They control display names, execution routing, timeouts, environment variables, and filesystem mounts — without separate configuration UI. The cell source is the single source of truth.

```python
# @name Train Classifier
# @worker my-gpu
# @timeout 120
# @env CUDA_VISIBLE_DEVICES=0
x = embeddings
y = labels
classifier.fit(x, y)
```

Annotations are parsed from the **first contiguous block** of `#`-prefixed lines. Once a non-comment, non-blank line is encountered, parsing stops. Each annotation is one line in the format `# @key value`.

---

## @name

Set a human-readable display name for the cell. Shown in the DAG view and as a badge in the cell editor.

```python
# @name Load arXiv Papers
import pandas as pd
papers = pd.read_parquet("https://...")
```

For **Python cells**, any non-empty string is accepted — spaces, parentheses, and special characters are fine.

For **prompt cells**, `@name` also sets the output variable name and must be a valid Python identifier:

```
# @name research_themes
Given these paper counts: {{ category_stats }}
Identify 3 research themes.
```

If no `@name` is set, the DAG view falls back to showing the cell's defined variable names, then the cell ID.

---

## @worker

Route the cell's execution to a named worker instead of the local machine.

```python
# @worker df-cluster
category_stats = ctx.sql("SELECT topic, COUNT(*) FROM papers GROUP BY topic").to_pandas()
```

Workers are HTTP endpoints that implement the Strata executor protocol. You define them in `notebook.toml`:

```toml
[[workers]]
name = "df-cluster"
backend = "executor"
runtime_id = "df-cluster"

[workers.config]
url = "https://my-datafusion-worker.fly.dev/v1/execute"
transport = "http"
```

During execution, the UI shows a pulsing "dispatching → df-cluster" badge on the cell. After completion, the worker name appears in the cell's metadata.

Workers can be anything that speaks HTTP: a GPU box on RunPod, a DataFusion cluster on Fly, a beefy EC2 instance, or a local process on a different port. The built-in `remote_executor.py` provides a reference implementation:

```bash
python -m strata.notebook.remote_executor --port 9000
```

If no `@worker` is set, the cell runs locally in the notebook's Python environment.

---

## @timeout

Override the execution timeout for a single cell, in seconds. The default is 30 seconds.

```python
# @timeout 300
# @worker my-gpu
embeddings = model.encode(abstracts, batch_size=256)
```

Useful for cells that download data, train models, or call slow external APIs. The timeout applies to the full execution including any remote worker round-trip.

---

## @env

Set an environment variable for this cell only, overriding the notebook-level value.

```python
# @env CUDA_VISIBLE_DEVICES=0
# @env OMP_NUM_THREADS=4
import torch
model = torch.nn.Linear(384, 10).cuda()
```

Format: `# @env KEY=value`. Multiple `@env` lines are supported. The variable is available in `os.environ` during cell execution.

---

## @mount

Attach a filesystem mount to the cell. Mounts provide read or read-write access to external storage (S3, local paths) during execution.

```python
# @mount raw_data s3://my-bucket/dataset ro
# @mount scratch file:///tmp/work rw
df = pd.read_parquet("/mnt/raw_data/events.parquet")
```

Format: `# @mount <name> <uri> [ro|rw]`. Defaults to `ro` (read-only) if the mode is omitted. The mount name must be a valid Python identifier.

---

## Precedence Rules

When the same setting is configured at multiple levels, the most specific wins:

| Setting | Annotation | Cell config (notebook.toml) | Notebook default |
|---------|-----------|---------------------------|-----------------|
| **Worker** | `# @worker X` | `cell.worker` field | `notebook.worker` field |
| **Timeout** | `# @timeout N` | `cell.timeout` field | 30 seconds |
| **Env vars** | `# @env K=V` | `cell.env` overrides | `notebook.env` defaults |
| **Mounts** | `# @mount ...` | `cell.mounts` overrides | `notebook.mounts` defaults |

Annotations always take priority. This lets you override per-cell behavior without editing `notebook.toml`.

---

## Notebook-Level Environment (Runtime Panel)

Notebook-wide environment variables are set via the **Runtime panel** in the sidebar. These apply to all cells unless overridden by a cell-level `@env` annotation.

Common use cases:

- **API keys**: `OPENAI_API_KEY`, `ANTHROPIC_API_KEY` (for prompt cells and AI assistant)
- **Database URLs**: `DATABASE_URL`, `REDIS_URL`
- **Feature flags**: `DEBUG=true`, `LOG_LEVEL=info`

!!! note "Sensitive values are not persisted to disk"
    Environment variables with names containing KEY, SECRET, TOKEN, PASSWORD, or CREDENTIAL are stripped from `notebook.toml` when saving. The key names are preserved so you know which vars are configured, but you'll need to re-enter values after reopening the notebook.

Notebook env vars are stored in the `[env]` section of `notebook.toml`:

```toml
[env]
DATABASE_URL = "postgres://localhost/mydb"
OPENAI_API_KEY = ""  # value stripped for security
```
