# Circle Packing — n = 26 (agent variant)

## Problem

Place **26 circles** inside the unit square `[0, 1] × [0, 1]` to maximize
the **sum of radii**. Constraints:

- every circle must be fully inside the unit square
  (`r ≤ x ≤ 1 − r` and `r ≤ y ≤ 1 − r`)
- no two circles may overlap
  (distance between centers `≥ r₁ + r₂`)
- infeasible solutions score **0**

**Metric:** `sum(r for (x, y, r) in circles)` (higher is better).

**Target:** `≥ 2.636`.

## Approach: single agent, full memory, no GP scaffolding

This notebook tests whether the GP machinery used by published systems
(populations, selection pressure, islands, MAP-Elites, novelty filters,
crossover, UCB-bandit mutator selection) is actually load-bearing for
LLM-driven code evolution. The hypothesis: it isn't. Once the mutator
can reason, the variation distribution is already directed; selection
becomes book-keeping.

The setup:

- One `claude-opus-4-7` agent runs in a loop. Each round it sees the
  full memory of past attempts (every score-and-source pair, plus the
  one-line insights it wrote at the time) and is asked to propose new
  strategies. No population. No fitness-based replacement.
- The agent's only tool is `score_candidate(source, insight)`. The
  harness compiles, runs, and scores the source in a 30s subprocess
  and returns a structured result. Every call is auto-recorded in
  memory.
- The agent decides when to stop a round (`end_turn`); the outer loop
  starts a fresh round if there's budget left. Within a round, the
  agent can call the tool up to `TOOL_CALL_CAP` times (configurable).
- Stops on `best_score ≥ 2.636` or when total tokens (input + output,
  cumulative across all rounds) hit `TOKEN_BUDGET`.

The companion `baseline_gp/` notebook runs the GP-shaped version at
the same token budget. Reading both `report` cells side by side is
the comparison.

## Setup

The cell needs `ANTHROPIC_API_KEY` in the runtime env (set it from the
Runtime panel).
