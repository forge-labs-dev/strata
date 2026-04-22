# Secret Manager Integration

Strata can pull environment variables from an external secret manager so API keys, database URLs, and other sensitive config don't have to be re-entered every time a notebook is reopened. Values fetched from the manager flow into the same `notebook.env` map the Runtime panel uses, so cells read them with plain `os.environ` — no notebook code changes.

## Supported providers

| Provider    | Status    | Auth                                       |
| ----------- | --------- | ------------------------------------------ |
| Infisical   | Supported | Service token via `INFISICAL_TOKEN` env var |

Adding a new provider is a one-file drop-in behind the `SecretProvider` protocol in `strata.notebook.secrets`. Vault / AWS Secrets Manager / Doppler can plug in without changing the session or routing code.

## Setting up Infisical

1. **Mint a service token** in your Infisical project with read access to the secrets you want Strata to see. Restrict it to a specific environment (`dev` / `staging` / `prod`) and path if possible.

2. **Export the token** in the shell that launches Strata:

    ```bash
    export INFISICAL_TOKEN="st.xxxx..."
    uv run uvicorn strata.server:app --host 0.0.0.0 --port 8765
    ```

    The token only ever lives in the process environment — never in `notebook.toml`, never in `.strata/`, never in any commit.

3. **Configure the notebook** by adding a `[secrets]` block to `notebook.toml`. The Runtime-panel UI will edit this for you; manual form is:

    ```toml
    [secrets]
    provider = "infisical"
    project_id = "your-project-id"
    environment = "dev"       # optional, defaults to "dev"
    path = "/"                # optional, defaults to "/"
    # base_url = "https://your-self-hosted.infisical.com"  # self-hosted only
    ```

    All four fields are non-sensitive routing info and safe to commit.

4. **Open the notebook.** On session open Strata calls Infisical, pulls the secrets at `project_id / environment / path`, and merges them into the notebook's env. A **"Secret manager"** block appears in the Runtime panel with a **Refresh** button and the provider name.

## How values flow

```
notebook.toml [env]  ──►  memory  ◄──  Runtime panel edits (session-only)
                            ▲
                            │ (merged at session open + on refresh)
                   Infisical (project_id, env, path)
```

- On **session open**, Strata pulls all secrets at the configured path and merges them into `notebook.env` where the key isn't already present (or where the existing value is a blanked sensitive placeholder from disk).
- On **Refresh** (button in the Runtime panel), Strata re-fetches without reopening. New/rotated values take effect for the next cell run.
- Values typed **manually in the Runtime panel** override anything from the manager for the current session. Useful for one-off overrides; remove the value to fall back to the manager's version.

Each env row in the Runtime panel shows a green source badge (`INFISICAL`) next to its name when the value came from the manager. Rows without a badge are manual overrides or local-only vars.

## Rotation

Rotate the secret in Infisical, then hit the **Refresh** button. Cells that run after the refresh see the new value immediately (the executor reads the cell's `env` each run). Cells with cached artifacts under the *old* value stay cached — rotating a secret doesn't invalidate history. If you need to re-execute with the new value, edit or force-run the downstream cell.

## Fetch errors

When a fetch fails — bad token, network error, wrong project — the notebook still opens. The error surfaces in the Runtime panel's Secret manager block:

> Infisical rejected the token (401). Check INFISICAL_TOKEN scope / expiry.

Fix the cause (rotate the token, check `project_id`, etc.), then hit Refresh.

## Security notes

- Secret **values** never ship to the frontend in cleartext from the env endpoints — values are only visible in the notebook venv's `os.environ`. The UI shows the key names + source, not the values.
- Secret values are **not written to disk**. `[env]` blocks on disk blank sensitive keys (`KEY`, `SECRET`, `TOKEN`, `PASSWORD`, `CREDENTIAL` name patterns) before persisting; secrets fetched at open time are in-memory only.
- If a cell **prints** an env var, its value is captured in the cell's console output and persisted in `.strata/console/` alongside stdout/stderr. Don't `print(os.environ)` in production notebooks.
- `INFISICAL_TOKEN` itself lives in the process environment, set by whoever launches the server. Distribute it the same way you'd distribute any deploy secret (systemd unit, k8s secret, `.envrc` with direnv-allow, etc.) — **not** in a committed file.

## Limits

This MVP is a read-only integration: Strata **reads** secrets from Infisical, it doesn't write to or rotate them. For updates, use the Infisical dashboard or CLI, then hit Refresh in the Runtime panel.
