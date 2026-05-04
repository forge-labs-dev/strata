# heilbronn_17

The same architecture comparison as `circle_packing_26`, applied to a
problem the LLM cannot have memorized: the **Heilbronn triangle
problem for n=17**.

## Why this experiment

`circle_packing_26` ended in a tie at the published SOTA (~2.6358) —
both architectures hit it, and Opus has plausibly seen the answer in
training. The tie was uninformative as a thesis test: we can't tell
whether the architectures were searching or recalling.

The Heilbronn triangle problem for n=17 is open. No proven optimum
exists; published bounds are around 0.018-0.020 and have been
incrementally tightened by hand constructions, computer search, and
recently AlphaEvolve. The exact best-known value for n=17 isn't a
single canonical number — small improvements are real progress, and
the LLM has no specific number to retrieve.

If the architectures still tie here, that's stronger evidence that
the GP scaffolding really isn't load-bearing once the mutator can
reason. If they diverge, that's the signal we couldn't extract from
n=26.

## Problem

Place 17 distinct points inside the unit square `[0, 1] × [0, 1]`
to maximize the *minimum* triangle area over all `C(17, 3) = 680`
triples of points. Higher is better. The LLM writes
`def propose(rng) -> list[(x, y)]`; the harness validates the
output, computes the min triangle area, and identifies the
**bottleneck triple** — the three points whose triangle is the
smallest. The bottleneck is the natural target for the next attempt.

## Two notebooks, same problem, same budget

Same comparison setup as `circle_packing_26`: 200,000 input+output
tokens, `claude-opus-4-7`, 30s per-candidate timeout, shared harness.
Only the architecture differs:

- `baseline_gp/` — population of 8, top-3 verbatim sources shown
  each iteration, recent failures fed back as feedback. Each
  population entry shows its bottleneck triple.
- `agent/` — single agent, full memory of every attempt with its
  insight + bottleneck triple, one tool (`score_candidate`).

## Seed strategies

- `RANDOM_UNIFORM` — 17 i.i.d. uniform points; baseline (~0.0001-0.0003)
- `HALTON_JITTERED` — Halton (2,3) sequence + tiny jitter (~0.0002)
- `SLSQP_MULTISTART` — multi-start max-min formulation; the strong
  seed (~0.0067)

## Running

```bash
STRATA_DEPLOYMENT_MODE=personal \
  STRATA_NOTEBOOK_STORAGE_DIR=/Users/fangchenli/Workspace/strata \
  uv run python -m strata
```

Open both notebooks in the UI:

- *Heilbronn 17 — GP baseline*
- *Heilbronn 17 — Agent variant*

Set `ANTHROPIC_API_KEY` in each. Run order:

- GP: `harness` → `seed` → `evolve` → `report`
- Agent: `harness` → `run_agent` → `report`

Both `report` cells print final score, total tokens, and a
score-over-time plot.

## What we expect to learn

The seed baseline is ~0.0067; published bounds are ~0.018-0.020.
That's a 3× gap of pure search, with no answer to retrieve.

- If both architectures converge near the published bound: still a
  weak result for the thesis (both are good search systems, but
  this problem is tractable enough for either).
- If the agent reaches higher than GP at equal budget: evidence
  that directed mutation + memory is doing real work.
- If GP reaches higher: counter-evidence — population diversity is
  load-bearing for harder search.
- If both stall well below 0.018: the budget is the bottleneck, not
  the architecture. Try a budget extension.
