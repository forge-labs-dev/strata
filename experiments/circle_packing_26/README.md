# circle_packing_26

A controlled comparison of two architectures for LLM-driven code
evolution on the n=26 circle packing problem.

## Thesis

Every published LLM-driven code-evolution system (AlphaEvolve, FunSearch,
ShinkaEvolve, CodeEvolve, ThetaEvolve) assumes that genetic-programming
scaffolding — populations, selection pressure, islands, MAP-Elites,
novelty filters, UCB-bandit mutators, crossover operators — is
load-bearing. **Hypothesis: it is not.** A single general-purpose agent
equipped with a small library of well-designed skills and a memory of
what has been tried can match or exceed these systems at equal token
budget, because an LLM mutation is already a directed proposal
conditioned on context, not a sample from a noise distribution.

## Two notebooks, same problem, same budget

| | `baseline_gp/` | `agent/` |
| --- | --- | --- |
| Architecture | FunSearch-style: population, fitness, top-K elite | Single agent, full memory, no population |
| LLM call structure | One `messages.create` per generation, no tools | Multi-step tool use per round, agent decides what to score |
| State tracked | top-8 `population` + last-3 `recent_failures` | full `memory` of every attempt |
| Selection | sort-by-score, evict worst | none — agent reasons over full history |
| Stop condition | `best_score >= 2.636` OR `total_tokens >= 200_000` | same |
| Token budget | 200,000 (input + output, cumulative) | 200,000 (input + output, cumulative) |

Both notebooks share:

- `harness.py` — `CandidateResult`, `_validate_circles`,
  `run_candidate(source, rng_seed, timeout)` with structured failure
  modes (`syntax_error`, `exec_error`, `timeout`, `infeasible`, etc.)
  and a feasibility-fraction soft score.
- The same problem statement, same target (≥ 2.636), same model
  (`claude-opus-4-7`), same per-candidate timeout (30s).

## Running

```bash
STRATA_DEPLOYMENT_MODE=personal \
  STRATA_NOTEBOOK_STORAGE_DIR=/Users/fangchenli/Workspace/strata \
  uv run python -m strata
```

Open both notebooks in the UI:

- *Circle Packing 26 — GP baseline*
- *Circle Packing 26 — Agent variant*

Set `ANTHROPIC_API_KEY` in each notebook's Runtime panel. Then in each:

1. Run `harness` (and, for GP, also `seed`)
2. Run the main loop cell (`evolve` for GP, `run_agent` for agent)
3. Run `report`

Both `report` cells print `Final best score`, `Total tokens`, and a
plot of best-score over time. That's the comparison.

## Limits of this experiment

- The n=26 problem is in AlphaEvolve's paper; the LLM may have seen
  the exact answer in training. A genuinely novel benchmark would be
  more compelling.
- Two runs is two data points. To make the thesis claim with any
  confidence we'd want ≥ 5 seeds per architecture and a t-test.
- The agent variant uses a single tool (`score_candidate`). Adding
  `inspect_violations`, `recall_attempts(filter)`, or `sketch_layout`
  would be in the spirit of the thesis but adds confounders. Keep
  the skill library small for the first comparison.
- `safe_exec` runs LLM-generated code in the notebook's subprocess
  with a `signal.SIGALRM` budget. Fine for personal research; not
  acceptable for shared infrastructure.

## Notes on the comparison

- **Tokens, not iterations.** The GP baseline does ~80-200 short
  generations; the agent does ~10-20 deep rounds. They aren't
  iso-iteration — they're iso-token. That's the only fair axis.
- **The GP baseline is honest, not strawman.** It includes
  per-iteration error feedback (the LLM sees recent failures with
  structured violation messages), which already lifts it above
  textbook FunSearch. If the agent variant ties at equal budget, the
  more interesting result is "feedback alone explains most of the
  gain"; if it wins, the thesis stands.
