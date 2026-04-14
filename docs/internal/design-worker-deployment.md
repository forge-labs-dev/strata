# Worker Deployment — Design Notes

**Status:** Draft, not implemented. Current shipping story is ad-hoc template files under `examples/arxiv_classifier/` (Modal + Fly). Revisit after alpha.

## Problem

Today users deploy Strata workers by copying template files:

- `examples/arxiv_classifier/modal_gpu_worker.py` — Modal GPU worker (~82 lines, mostly boilerplate: Rust toolchain, pip install, ASGI wrap)
- `examples/arxiv_classifier/df-cluster/Dockerfile` + `fly.toml` — Fly CPU worker

Pain for users:

- Must copy files into their own repo and maintain them.
- Must pin a Strata git SHA and bump it on every upgrade.
- Must know to install Rust, maturin, etc.
- Deps list, GPU class, app name, scaledown — all hardcoded; no clean override knob.

Pain for us:

- Two sibling templates living outside the package drift from code.
- Every `main` commit invalidates any example pin we document.
- No CI validation that the templates still deploy cleanly.

## Goals

1. **Easy for users** — one command or short config, no copied boilerplate, no pin-bumping on every Strata upgrade.
2. **Easy for us** — entry points live inside the package, versioned with the code, testable in CI.
3. **Works for both PyPI and git-installed users.**
4. **Preserves escape hatches** — advanced users can still drop to raw Modal/Fly configs when needed.

## Proposed surface

### Modal — direct module invocation

```
modal deploy -m strata.deploy.modal
```

Modal's CLI supports `-m module.name`. No file for the user to copy. Config via:

- Env vars: `STRATA_WORKER_APP`, `STRATA_WORKER_GPU`, `STRATA_WORKER_DEPS`, `STRATA_WORKER_SCALEDOWN`, ...
- Optional `strata.worker.toml` in cwd (pydantic-settings auto-loads).
- Escape hatch: 5-line user stub that imports `strata.deploy.modal.build_app()` and customizes before exposing `app`.

### Fly / Docker — scaffolder

```
strata deploy fly init [--preset cpu-datafusion] [--app NAME] [--region sjc]
strata deploy fly deploy   # thin flyctl wrapper; optional
```

`init` writes a minimal `fly.toml` plus a two-line `Dockerfile`:

```dockerfile
FROM ghcr.io/forge-labs-dev/strata-worker:<pinned>
RUN pip install --no-cache-dir datafusion pandas pyarrow
```

No Rust toolchain in the user's Dockerfile.

### Prebuilt base image — the keystone

Publish `ghcr.io/forge-labs-dev/strata-worker:{version,latest,sha}` from CI on every tag/commit. Rust toolchain and compiled Strata live here. Both Modal and Fly entry points consume this image. The pin tracks `strata.__version__` automatically via `importlib.metadata`, so users never bump it manually.

This is the single change that removes the "bump pin on every commit" pain. Without it, any entry point still has the same problem.

## File layout

New package `src/strata/deploy/`:

- `__init__.py` — exports `WorkerConfig`, `build_modal_app`, `base_image_ref()`.
- `config.py` — `WorkerConfig` (pydantic-settings): `app_name`, `gpu`, `cpu`, `memory`, `scaledown_window`, `max_containers`, `workload_deps: list[str]`, `preset`, `secrets`, `region`, `base_image`.
- `presets.py` — named dep bundles: `gpu-torch`, `cpu-datafusion`, `cpu-minimal`. Free-form `workload_deps` extends the preset.
- `modal.py` — loads `WorkerConfig`, builds `app` and `worker` ASGI function at import time. Target of `modal deploy -m`.
- `version.py` — resolves base-image tag: `importlib.metadata.version("strata")` for PyPI; falls back to `git rev-parse` or `STRATA_DEPLOY_REF` for editable/git installs.
- `fly/__init__.py` — scaffolder logic.
- `fly/templates/Dockerfile` and `fly.toml` — package-data templates, rendered with `WorkerConfig` + version tag.
- `cli.py` — registers `strata deploy` subcommands, hooked into `src/strata/cli.py`.

Plus:

- `docker/worker/Dockerfile` — base image build.
- `.github/workflows/publish-worker-image.yml` — CI publish on tag/commit.

## Migration path

1. Ship base image + `strata.deploy` in release N; keep existing templates with a header comment pointing at the new path.
2. Document migration in `docs/notebook/workers.md`.
3. One release later, shrink `examples/arxiv_classifier/modal_gpu_worker.py` to a 3-line override demo; delete the Fly template files in favor of `strata deploy fly init` output.
4. Keep `create_notebook_executor_app()` untouched — stable integration point.

## Open questions (decide before implementing)

1. **Registry** — ghcr.io/forge-labs-dev, Docker Hub, or ECR?
2. **Image variants** — single CPU image (Modal layers CUDA on top), or publish `strata-worker:{cpu,cuda12}` variants? Cold-start size vs. matrix complexity.
3. **Presets vs. free-form deps** — worth maintaining named presets, or just document recipe snippets and accept `workload_deps` only?
4. **Fly orchestration depth** — stop at scaffolding, or wrap `flyctl` for app-create / secrets set / deploy? Wrapping adds a runtime dependency on `flyctl` we can't vendor.
5. **Git-install pin behavior** — when user is on a dirty editable install, should `strata deploy` refuse, warn, or fall back to `:latest`? Refusing is safest but annoying for alpha users.
6. **Config source precedence** — env > `strata.worker.toml` > `notebook.toml [deploy.modal]` section > CLI flags? A `[deploy]` section inside `notebook.toml` means one config file per project but couples deploy to notebook semantics.
7. **CI canary** — nightly job that actually `modal deploy`s + `flyctl deploy`s and hits `/health`? Otherwise regressions will ship silently.
8. **Third platforms** (RunPod, Beam, Fargate, Lambda) — design `strata.deploy.<platform>` as an open plugin surface now, or defer?

## Tentative defaults (if we need to pick fast)

- Registry: ghcr.io/forge-labs-dev
- One CPU base image; CUDA via Modal's image layers
- Presets: yes, keep the concept
- Fly: scaffolding only, no flyctl wrapping
- Dirty git install: warn and use `:latest`
- Config: standalone `strata.worker.toml`
- CI: manual for alpha
- Third platforms: defer

## Non-goals (for this design)

- Full SaaS / control-plane deployment. This is about users deploying their own workers, not about us running them.
- Replacing the `strata-worker` CLI — that stays the stable integration point.
- Supporting arbitrary orchestrators (k8s, Nomad) — design doesn't preclude them but isn't tuned for them.
