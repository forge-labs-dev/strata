# @name Evolve — one LLM-driven generation
# @loop max_iter=200 carry=state
# @loop_until state["total_tokens"] + state.get("last_iter_tokens", 7000) >= 200000
# @timeout 600
#
# Note on max_tokens: this cell was originally 2048, then bumped to
# 8192 to fix mid-response truncation. 8192 caused per-iteration wall
# times in the 5-10 minute range as Opus elaborated to the cap, and
# iter 2 of the second GP run timed out at 600s. Settled on 4096 plus
# a more robust ``_extract_code`` that handles truncated fences —
# halves worst-case generation time, and the parser recovers from
# the common truncation patterns when 4096 isn't enough.
#
# One iteration of the outer loop:
#   1. Build a prompt showing the LLM the top-K population members
#      (verbatim source, score, and the bottleneck triple — the three
#      points whose triangle currently has the smallest area).
#   2. Ask claude-opus-4-7 to write a new ``propose`` function.
#   3. Run it through ``run_candidate`` (compile, exec, score in 30s
#      budget). Successes join the population if they beat the worst
#      member; failures (syntax / exec / timeout / bad output) join
#      ``recent_failures`` so the next iteration can learn from them.
#
# Heilbronn has no early-success target — the optimum for n=17 is
# unsolved, so we only stop on the 200k token budget.

from __future__ import annotations

import os
import re

import anthropic

POPULATION_CAP = 8
TOP_K_IN_PROMPT = 3
N_RECENT_FAILURES_IN_PROMPT = 3
MODEL = "claude-opus-4-7"
PER_CANDIDATE_TIMEOUT = 30.0


SYSTEM_PROMPT = """You are evolving a Python function for the n=17
Heilbronn triangle problem. The function signature is:

    def propose(rng: numpy.random.Generator) -> list[tuple[float, float]]

It must return EXACTLY 17 distinct (x, y) points inside the unit square
[0, 1] x [0, 1]. The score is the MINIMUM triangle area over all
C(17, 3) = 680 triples of points; we maximize that minimum.

The Heilbronn problem for n=17 is open — no proven optimum exists.
Published bounds are around 0.018-0.020. Any score above the seed
strategies (~0.005-0.007) is a real improvement; reaching 0.015+
would be competitive with the literature.

You may use numpy and scipy freely. The function runs with a 30s
timeout. Be deterministic given the rng — don't call random or
np.random outside of the rng you're given.

Each population entry shows you the BOTTLENECK TRIPLE — the three
points forming the smallest triangle. That triangle is what's
limiting the score; moving one of its three points typically gains
more than refining points that are not bottlenecks.

When the user prompt flags a stagnation alert, your top strategies
have all converged on one local optimum. Switch to a structurally
different topology (rotate the layout, perturb edges, try a
boundary-heavy vs interior-heavy arrangement) before refining
further. ``scipy.optimize.basin_hopping`` and ``dual_annealing`` can
escape minima that SLSQP cannot.

Output ONLY a Python code block containing the `propose` function
and any helpers it needs. No prose, no commentary outside the code."""


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


def _format_population_section(population: list[dict], top_k: int) -> str:
    sorted_pop = sorted(population, key=lambda d: d["score"], reverse=True)[:top_k]
    if not sorted_pop:
        return "(no successful strategies yet)"
    parts = []
    for i, d in enumerate(sorted_pop):
        bottleneck = ""
        if d.get("bottleneck") is not None and d.get("points"):
            bi, bj, bk = d["bottleneck"]
            p_i = d["points"][bi]
            p_j = d["points"][bj]
            p_k = d["points"][bk]
            bottleneck = (
                f"\nbottleneck triangle: points[{bi}]=({p_i[0]:.4f}, {p_i[1]:.4f}), "
                f"points[{bj}]=({p_j[0]:.4f}, {p_j[1]:.4f}), "
                f"points[{bk}]=({p_k[0]:.4f}, {p_k[1]:.4f})"
            )
        parts.append(
            f"### Strategy #{i + 1} — score {d['score']:.6f}{bottleneck}\n"
            f"```python\n{d['source']}\n```"
        )
    return "\n\n".join(parts)


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
            f"{plateau['spread']:.6f} (best {plateau['top']:.6f}, "
            f"3rd {plateau['third']:.6f}). The population has converged "
            f"on one local optimum. Refining variants of the same layout "
            f"will not escape it. Try a structurally different topology "
            f"or use basin_hopping / dual_annealing for global search.\n"
        )
    return (
        f"Best score so far: {best:.6f} (Heilbronn n=17, no known optimum).\n"
        f"Iteration: {state['iter'] + 1} / 200.\n"
        f"Tokens used: {state['total_tokens']:,} / 200,000.\n"
        f"{plateau_note}\n"
        "## Top strategies\n\n"
        f"{pop_block}\n\n"
        "## Recent failures (learn from these)\n\n"
        f"{fail_block}\n\n"
        "## Task\n\n"
        "Write a NEW `propose(rng)` that improves on the top strategies. "
        "Focus on the bottleneck triple — moving one of those three "
        "points usually gains more than tweaking others. "
        "Output ONLY the code block."
    )


def _extract_code(content: str) -> str:
    """Pull a Python source block out of an LLM response.

    The LLM is asked to emit a single ``` ```python ... ``` `` fenced
    block. We try four strategies in order:

    1. Complete fenced block — the happy path. Use the regex that
       requires both an opening and closing fence.
    2. Truncated fence (no closing) — happens when the response is
       cut off by ``max_tokens``. Strip the opening fence and use
       everything after it.
    3. No fence at all — the LLM ignored the formatting instruction.
       Find the first occurrence of ``def propose`` or ``import``
       and use everything from there.
    4. Last resort — return the raw content. ``compile`` will likely
       fail and the harness records the failure with a syntax error.
    """
    match = re.search(r"```(?:python)?\s*\n(.*?)```", content, re.DOTALL)
    if match:
        return match.group(1).strip()
    open_match = re.search(r"```(?:python)?\s*\n", content)
    if open_match:
        return content[open_match.end() :].strip()
    for marker in ("def propose", "import "):
        idx = content.find(marker)
        if idx >= 0:
            return content[idx:].strip()
    return content.strip()


def _call_llm(state: dict) -> tuple[str, int]:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    # 4096 strikes the balance: 2048 truncated nearly every Heilbronn
    # response, 8192 caused per-iter wall times in the 5-10 minute
    # range and timeouts. ``_extract_code`` handles the occasional
    # truncation gracefully so 4096 is comfortable.
    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
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
    f"iter {next_iter}: status={result.status}, score={result.score:.6f}, "
    f"tokens={tokens_used} (cum {total_tokens})"
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
    new_failures = new_failures[-N_RECENT_FAILURES_IN_PROMPT:]

new_best = max((d["score"] for d in new_population), default=state["best_score"])

state = {
    "iter": next_iter,
    "population": new_population,
    "recent_failures": new_failures,
    "best_score": new_best,
    "total_tokens": total_tokens,
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
