# @name Evolve — one LLM-driven generation
# @loop max_iter=200 carry=state
# @loop_until state["best_score"] >= 2.636 or state["total_tokens"] + state.get("last_iter_tokens", 7000) >= 200000
# @timeout 120
#
# One iteration of the outer loop:
#   1. Build a prompt showing the LLM the top-K population members
#      (verbatim source) plus the most recent failures and their
#      structured error messages.
#   2. Ask claude-opus-4-7 to write a new ``propose`` function.
#   3. Run it through ``run_candidate`` (compile, exec, score in
#      30s budget). The result is either ``status="ok"`` (joins the
#      population if it beats the worst member) or one of the failure
#      statuses (joins ``recent_failures`` so the next iteration's
#      prompt shows what went wrong).
#
# ``state`` is a plain dict carried between iterations by Strata's
# loop machinery; everything mutates ``state`` and assigns it back at
# the bottom of the cell.

from __future__ import annotations

import os
import re

import anthropic

POPULATION_CAP = 8
TOP_K_IN_PROMPT = 3
N_RECENT_FAILURES_IN_PROMPT = 3
MODEL = "claude-opus-4-7"
PER_CANDIDATE_TIMEOUT = 30.0


SYSTEM_PROMPT = """You are evolving a Python function for the n=26 circle
packing problem. The function signature is:

    def propose(rng: numpy.random.Generator) -> list[tuple[float, float, float]]

It must return EXACTLY 26 (x, y, r) circles inside the unit square
[0, 1] x [0, 1], with no overlap (distance between centers >= sum of
radii). Maximize the sum of radii. The known SOTA is around 2.6358.

You may use numpy and scipy freely. The function runs with a 30s
timeout. Be deterministic given the rng — don't call random or
np.random outside of the rng you're given.

When the user prompt flags a stagnation alert, your top strategies
have all converged to one local optimum. Switch to a structurally
different topology (hex-derived arrangements, asymmetric rows,
mixed-radius patterns) before refining further. For global search
inside a single optimization, ``scipy.optimize.basin_hopping`` or
``scipy.optimize.dual_annealing`` can escape minima that SLSQP cannot.

Output ONLY a Python code block containing the `propose` function and
any helpers it needs. No prose, no commentary outside the code."""


def _format_population_section(population: list[dict], top_k: int) -> str:
    sorted_pop = sorted(population, key=lambda d: d["score"], reverse=True)[:top_k]
    if not sorted_pop:
        return "(no successful strategies yet)"
    parts = []
    for i, d in enumerate(sorted_pop):
        parts.append(
            f"### Strategy #{i + 1} — score {d['score']:.4f}\n```python\n{d['source']}\n```"
        )
    return "\n\n".join(parts)


def _detect_plateau(population: list[dict], rel_threshold: float = 0.01) -> dict | None:
    """If the top-3 of the population span less than *rel_threshold*
    fraction of the best score, the GP loop is polishing one local
    optimum. Returns a small descriptor for the prompt; ``None`` when
    we're still exploring or have too few feasible attempts.
    """
    if len(population) < 3:
        return None
    sorted_pop = sorted(population, key=lambda d: d["score"], reverse=True)
    top, third = sorted_pop[0]["score"], sorted_pop[2]["score"]
    if top <= 0:
        return None
    spread = top - third
    if spread / top >= rel_threshold:
        return None
    return {"top": top, "third": third, "spread": spread}


def _format_failures_section(failures: list[dict], k: int) -> str:
    recent = failures[-k:]
    if not recent:
        return "(no recent failures)"
    parts = []
    for i, d in enumerate(recent):
        parts.append(
            f"### Failure #{i + 1} — status={d['status']}\n"
            f"error: {d.get('error') or '(no detail)'}\n"
            f"```python\n{d['source']}\n```"
        )
    return "\n\n".join(parts)


def _build_user_prompt(state: dict) -> str:
    pop_block = _format_population_section(state["population"], TOP_K_IN_PROMPT)
    fail_block = _format_failures_section(state["recent_failures"], N_RECENT_FAILURES_IN_PROMPT)
    best = state["best_score"]
    plateau = _detect_plateau(state["population"])
    plateau_note = ""
    if plateau is not None:
        plateau_note = (
            f"\n**Stagnation alert**: the top-3 strategies span only "
            f"{plateau['spread']:.4f} (best {plateau['top']:.4f}, "
            f"3rd {plateau['third']:.4f}). The population has converged "
            f"on one local optimum. Refining variants of the same layout "
            f"will not escape it. Try a structurally different topology "
            f"or use basin_hopping / dual_annealing for global search.\n"
        )
    return (
        f"Best score so far: {best:.4f} (target: 2.636).\n"
        f"Iteration: {state['iter'] + 1} / 200.\n"
        f"Tokens used: {state['total_tokens']:,} / 200,000.\n"
        f"{plateau_note}\n"
        "## Top strategies\n\n"
        f"{pop_block}\n\n"
        "## Recent failures (learn from these)\n\n"
        f"{fail_block}\n\n"
        "## Task\n\n"
        "Write a NEW `propose(rng)` that improves on the top strategies. "
        "If a recent failure was close to feasible, you may try to fix "
        "its specific violations. Output ONLY the code block."
    )


def _extract_code(content: str) -> str:
    match = re.search(r"```(?:python)?\s*\n(.*?)```", content, re.DOTALL)
    if match:
        return match.group(1).strip()
    # Fallback: assume the whole content is code (LLM may have ignored
    # the formatting instruction).
    return content.strip()


def _call_llm(state: dict) -> tuple[str, int]:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    message = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _build_user_prompt(state)}],
    )
    used = message.usage.input_tokens + message.usage.output_tokens
    return message.content[0].text, used


# --- one iteration --------------------------------------------------------

next_iter = state["iter"] + 1
raw_response, tokens_used = _call_llm(state)
candidate_source = _extract_code(raw_response)

result = run_candidate(
    candidate_source,
    rng_seed=next_iter,
    timeout_seconds=PER_CANDIDATE_TIMEOUT,
)
result_d = result_to_dict(result)

total_tokens = state["total_tokens"] + tokens_used
print(
    f"iter {next_iter}: status={result.status}, score={result.score:.4f}, "
    f"soft={result.soft_score:.4f}, tokens={tokens_used} (cum {total_tokens})"
)
if result.error:
    print(f"  error: {result.error[:160]}")

# Population update — keep top-N by score.
new_population = list(state["population"])
new_failures = list(state["recent_failures"])

if result.status == "ok":
    new_population.append(result_d)
    new_population.sort(key=lambda d: d["score"], reverse=True)
    new_population = new_population[:POPULATION_CAP]
else:
    new_failures.append(result_d)
    # Keep only the most recent failures so the prompt stays concise.
    new_failures = new_failures[-N_RECENT_FAILURES_IN_PROMPT:]

new_best = max((d["score"] for d in new_population), default=state["best_score"])

state = {
    "iter": next_iter,
    "population": new_population,
    "recent_failures": new_failures,
    "best_score": new_best,
    "total_tokens": total_tokens,
    # Used by the @loop_until predicate to decide whether the next
    # iteration would push us past the budget. Predicting next-call
    # cost from the last call's cost is a good proxy because GP iters
    # are independent (no growing conversation history).
    "last_iter_tokens": tokens_used,
    "history": state["history"]
    + [
        {
            "iter": next_iter,
            "best_score": new_best,
            "n_evaluated": state["history"][-1]["n_evaluated"] + 1,
            "n_ok": state["history"][-1]["n_ok"] + (1 if result.status == "ok" else 0),
            "this_status": result.status,
            "this_score": result.score,
            "tokens_this_iter": tokens_used,
            "total_tokens": total_tokens,
        }
    ],
}
