# Fly Notebook Smoke Checklist

Use this checklist against the hosted personal-mode notebook at:

- `https://strata-notebook.fly.dev/#/`

The current Fly deployment stores notebook state under the mounted volume at:

- `/home/strata/.strata/notebooks`

This smoke pass is meant to answer three questions quickly:

1. Can I create and reopen notebooks on the hosted app?
2. Does execution still behave correctly after recent notebook/environment changes?
3. Do notebook files survive the normal Fly machine lifecycle?

## Quick Pass

1. Open the app and create a notebook from the home page.
   Expected:
   - the default parent directory is `/home/strata/.strata/notebooks`
   - the notebook opens directly without a reconnect error

2. Add two cells:

```python
# c1
x = 1
```

```python
# c2
y = x + 1
y
```

3. Run `c2`.
   Expected:
   - cascade runs `c1` and `c2`
   - `c2` finishes `ready`

4. Re-run unchanged `c1`.
   Expected:
   - `c2` stays green/ready
   - `Why stale?` does not appear

5. Edit `c1` to `x = 2` and run `c1`.
   Expected:
   - `c2` becomes stale/idle because of upstream change

## Environment Pass

1. Open the `Environment` panel.
   Expected:
   - status shows the live notebook environment
   - declared and resolved package counts are visible

2. Add a small package, for example `six`.
   Expected:
   - the panel shows a running operation for `uv add six`
   - when it completes, the operation card shows duration and any stdout/stderr
   - the direct dependency list includes `six`

3. Export `requirements.txt`.
   Expected:
   - exported text includes `pyarrow` and `six`

4. Rebuild the environment with `Rebuild .venv`.
   Expected:
   - the panel shows a running `uv sync` operation
   - the last sync timestamp and duration update

## Persistence Pass

1. Note the notebook path from the home page or recent-notebooks list.

2. Let the Fly machine recycle through your normal deploy/restart/suspend-resume workflow.
   Expected:
   - the notebook directory still exists because it is stored on the mounted volume

3. Reopen the notebook by path.
   Expected:
   - notebook contents are still present
   - package metadata still reflects the last successful environment state

## Failure Triage

If notebook creation or reopen fails:

- verify the parent path shown in the UI is `/home/strata/.strata/notebooks`
- check whether the failure is path-related or session-reconnect-related

If package install is slow or fails:

- open the `Environment` panel and inspect the last operation card
- capture:
  - command
  - duration
  - stdout
  - stderr

If notebook data looks missing after a restart:

- verify the deployment still mounts `/home/strata/.strata`
- verify the notebook path is under `/home/strata/.strata/notebooks`, not `/tmp`
