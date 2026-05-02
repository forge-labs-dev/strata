# Circle Packing — n = 26 (LLM-driven code evolution)

## Problem

Place **26 circles** inside the unit square `[0, 1] × [0, 1]` to maximize
the **sum of radii**. Constraints:

- every circle must be fully inside the unit square
  (`r ≤ x ≤ 1 − r` and `r ≤ y ≤ 1 − r`)
- no two circles may overlap
  (distance between centers `≥ r₁ + r₂`)
- infeasible solutions score **0**

**Metric:** `sum(r for (x, y, r) in circles)` (higher is better).

**Target:** `≥ 2.636` — close to the best known result on Packomania.

## Approach: two-level evolution

The LLM doesn't pick coordinates. It writes a Python function

```python
def propose(rng: numpy.random.Generator) -> list[tuple[float, float, float]]:
    ...  # uses scipy / numpy freely; returns 26 (x, y, r) triples
```

Each generation:

1. We show the LLM the **top strategies so far** (verbatim source) plus
   **recent failures** (with structured error messages — overlaps,
   out-of-bounds, exec errors).
2. The LLM rewrites `propose`. We `safe_exec` it in the notebook's
   subprocess, score the output, and either replace the worst
   population member or stash the result as a "recent failure" the
   next generation can learn from.

Stops on `best_score >= 2.636` or `max_iter = 80`.

## Setup

The notebook expects `ANTHROPIC_API_KEY` in the runtime env (set it via
the Runtime panel before running `evolve`). Model is `claude-opus-4-7`.
