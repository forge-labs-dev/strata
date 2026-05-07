# SQL Orders Report

A small five-cell notebook that demonstrates Strata's SQL cell support
against a local SQLite warehouse. The cells span three languages
(Python → SQL → Python) and exercise both `# @cache fingerprint` (data-
sensitive) and `# @cache forever` (reference-data) policies.

## What it shows

- **Connection config in `notebook.toml`.** The `[connections.warehouse]`
  block declares a SQLite driver with a relative path. SQL cells
  reference it by name via `# @sql connection=warehouse`.
- **Bind parameters from upstream Python.** `top_orders` resolves
  `:min_amount` from the `threshold` cell's variable. No string
  substitution happens — the value flows through ADBC's parameter
  binding API, so adversarial strings round-trip as data.
- **Two cache policies side by side.** `top_orders` uses `fingerprint`
  (folds SQLite's `PRAGMA data_version` + `schema_version` into the
  hash) so a DDL change to the warehouse re-executes it. `category_summary`
  uses `forever` because the user's asserting the catalog is reference
  data; only edits to the SQL body itself invalidate it.
- **Read-only enforcement.** SQL cells open the connection with
  `mode=ro` plus `PRAGMA query_only = ON`. Any cell that tried
  `INSERT`/`UPDATE`/`DELETE` would error before mutating the DB.
- **Cross-language pipeline.** The two SQL results flow back into the
  `report` Python cell as pandas DataFrames (the Arrow IPC artifacts
  decode through the standard notebook serializer).

## Cells

| Cell | Language | What it does |
|---|---|---|
| `setup` | Python | Seeds `analytics.db` with five products and ten orders. Run this once. |
| `threshold` | Python | Defines `min_amount = 50`. Edit and rerun to vary the threshold. |
| `top_orders` | SQL | `WHERE amount > :min_amount`, joined to the product catalog, top 5. |
| `category_summary` | SQL | Revenue by category with `# @cache forever`. |
| `report` | Python | Stitches the two SQL outputs into a markdown report. |

## Running

From the project root:

```bash
uv run strata-server --host 127.0.0.1 --port 8765
```

Then open `examples/sql_orders_report` from the Strata home page. Run
`setup` first to create the SQLite file, then run the other cells in
order (or hit "Run all").

## Try this

- **Change the threshold.** Edit `threshold` to `min_amount = 100`.
  `top_orders` re-executes (different bind value → different
  provenance hash). Run `top_orders` again without changing
  anything — it cache-hits, because the canonical artifact's
  provenance still matches the current threshold.
- **Mutate the schema.** From a separate shell:

  ```bash
  sqlite3 analytics.db 'ALTER TABLE orders ADD COLUMN region TEXT'
  ```

  Re-run `top_orders`. SQLite's `PRAGMA schema_version` bumps, the
  freshness token changes, and the cell re-executes with a new schema.
- **Try a write from a SQL cell.** Add a cell with `INSERT INTO orders
  VALUES (...)`. The executor returns an error and the database row
  count is unchanged — read-only mode is the security boundary.
