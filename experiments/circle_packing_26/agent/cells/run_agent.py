# @name Run agent — single-agent loop with full memory
# @timeout 3600
#
# Thesis test: at equal token budget, a single LLM agent equipped with
# a small skill library and full memory of past attempts matches or
# exceeds the GP baseline. No population, no fitness-based replacement,
# no crossover, no islands. Just a reasoner with tools.
#
# Outer loop: keep starting fresh agent rounds until the token budget
# is exhausted or the target score is hit. Each round is a Claude
# request that may make up to ``TOOL_CALL_CAP`` tool calls before
# yielding back. Memory persists across rounds and is rendered into
# every round's user prompt — this is the "full memory" condition.

from __future__ import annotations

import json
import os

import anthropic

# ---- Configuration -------------------------------------------------------
MODEL = "claude-opus-4-7"
TOKEN_BUDGET = 200_000  # input + output tokens across the whole run
TOOL_CALL_CAP = 12  # max tool calls per agent round
PER_CANDIDATE_TIMEOUT = 30.0
TARGET = 2.636
MAX_TOKENS_PER_RESPONSE = 2048


SYSTEM_PROMPT = """You are working on the n=26 circle packing problem.
Your goal is to maximize the sum of radii of 26 non-overlapping circles
inside the unit square [0, 1] x [0, 1]. The known SOTA is around 2.6358.

You design a Python function with the signature

    def propose(rng: numpy.random.Generator) -> list[tuple[float, float, float]]

that returns 26 (x, y, r) circles. You may use numpy and scipy freely.

You have one tool: ``score_candidate(source, insight)``. It compiles
and runs your code in a 30s sandboxed subprocess and returns a
structured result (score, status, error message, the produced
circles). Every call is recorded in your memory automatically and
visible in future rounds.

Strategy notes:
- Each round, look at memory and decide what to try next. Don't repeat
  strategies that have already been tried unless you're patching them
  to fix a specific failure.
- The hard score is sum_radii when feasible, 0 otherwise. The soft
  score (radii * fraction_feasible) is a proxy for "how close to
  feasible" a broken candidate was — useful when iterating on a
  near-miss.
- ``insight`` is a one-line note about WHY this strategy is worth
  trying. Future rounds will see it. Be honest — if you're testing a
  hypothesis, say so. Memory is a record of reasoning, not just code.
- When you've tried enough strategies in this round, end your turn.
  The outer loop will start a fresh round if the budget allows.

When your top attempts cluster in a tight band (< 1% spread) the user
prompt will flag it as a stagnation alert. When that happens, try a
structurally different topology (hex-derived, asymmetric rows,
mixed-radius patterns) before refining further. For global search
inside a single optimization, ``scipy.optimize.basin_hopping`` or
``scipy.optimize.dual_annealing`` can escape minima that SLSQP cannot.
"""


TOOLS = [
    {
        "name": "score_candidate",
        "description": (
            "Run a candidate ``propose`` function in a sandboxed "
            "subprocess and return its score and status. Auto-records to "
            "memory; visible to all future rounds. Use this to try a "
            "strategy and see how it performs."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": (
                        "Full Python source defining ``def propose(rng)``. "
                        "May include helper functions and imports. Must "
                        "return a list of exactly 26 (x, y, r) tuples."
                    ),
                },
                "insight": {
                    "type": "string",
                    "description": (
                        "One-line note (<=120 chars) describing the "
                        "key idea behind this strategy. Stored in memory "
                        "so future rounds can see your reasoning."
                    ),
                },
            },
            "required": ["source", "insight"],
        },
    },
]


# ---- Plateau detection + budget bookkeeping -----------------------------


def _detect_plateau(memory: list[dict], rel_threshold: float = 0.01) -> dict | None:
    """If the top-3 successful attempts span less than *rel_threshold*
    fraction of the best score, the agent is polishing one local
    optimum. Return a small descriptor for the prompt; ``None`` when
    we're still exploring or have too few data points.
    """
    ok = [m for m in memory if m["status"] == "ok"]
    if len(ok) < 3:
        return None
    sorted_ok = sorted(ok, key=lambda m: m["score"], reverse=True)
    top, third = sorted_ok[0]["score"], sorted_ok[2]["score"]
    if top <= 0:
        return None
    spread = top - third
    if spread / top >= rel_threshold:
        return None
    return {"top": top, "third": third, "spread": spread}


def _next_call_fits(total_tokens: int, last_input_tokens: int) -> bool:
    """Conservative budget check: assume the next API call costs
    ``last_input_tokens`` (or 1024 if we have no observation) plus the
    worst-case ``MAX_TOKENS_PER_RESPONSE``. Skip the call when that
    would exceed ``TOKEN_BUDGET``.

    The first call has ``last_input_tokens == 0`` so the guard is
    cheap; subsequent calls auto-tune from observed input sizes,
    which grow as memory accumulates. Inputs in this experiment are
    monotone-increasing, so using the most recent observation is
    safe enough — it slightly under-reserves but never over-reserves.
    """
    estimated_input = max(last_input_tokens, 1024)
    return total_tokens + estimated_input + MAX_TOKENS_PER_RESPONSE <= TOKEN_BUDGET


# ---- Memory rendering ----------------------------------------------------


def _format_memory(memory: list[dict]) -> str:
    if not memory:
        return "(memory empty — this is your first round)"
    sorted_idx = sorted(
        range(len(memory)),
        key=lambda i: memory[i]["score"],
        reverse=True,
    )
    parts = []
    for rank, i in enumerate(sorted_idx):
        m = memory[i]
        parts.append(
            f"### Attempt #{i + 1} — score {m['score']:.4f} ({m['status']})\n"
            f"insight: {m.get('insight') or '(no insight given)'}\n"
            + (f"error: {m['error'][:200]}\n" if m.get("error") else "")
            + f"```python\n{m['source']}\n```"
        )
    return "\n\n".join(parts)


def _build_user_prompt(memory: list[dict], total_tokens: int, best_score: float) -> str:
    n_total = len(memory)
    n_ok = sum(1 for m in memory if m["status"] == "ok")
    plateau = _detect_plateau(memory)
    plateau_note = ""
    if plateau is not None:
        plateau_note = (
            f"\n**Stagnation alert**: your top-3 attempts span only "
            f"{plateau['spread']:.4f} (best {plateau['top']:.4f}, "
            f"3rd {plateau['third']:.4f}). You are polishing a local "
            f"optimum. Refining variants of the same layout will not "
            f"escape it. Try a structurally different topology — "
            f"hex-derived, asymmetric rows, mixed radii — or use "
            f"basin_hopping / dual_annealing for global search.\n"
        )
    return (
        f"Memory ({n_total} attempts, {n_ok} feasible, best score "
        f"{best_score:.4f}):\n\n"
        f"{_format_memory(memory)}\n\n"
        f"Tokens used: {total_tokens:,} / {TOKEN_BUDGET:,}.\n"
        f"Target: {TARGET}."
        f"{plateau_note}\n"
        "Propose at least one new strategy and score it before ending "
        f"your turn. You may iterate within this round — call score_candidate "
        f"up to {TOOL_CALL_CAP} times, observe the result of each, and "
        "refine. End the turn when you're done."
    )


# ---- Tool execution ------------------------------------------------------


def _run_tool(name: str, args: dict, memory: list[dict]) -> dict:
    """Execute a tool call and return a JSON-serializable result.

    The agent's memory is updated as a side effect: ``score_candidate``
    appends the full candidate record (source, score, status, error,
    insight) so future rounds see it.
    """
    if name != "score_candidate":
        return {"error": f"unknown tool: {name}"}

    source = args.get("source", "")
    insight = args.get("insight", "")

    result = run_candidate(
        source,
        rng_seed=len(memory),
        timeout_seconds=PER_CANDIDATE_TIMEOUT,
    )
    record = result_to_dict(result)
    record["insight"] = insight
    memory.append(record)

    # Return a compact view back to the agent — full source is what the
    # agent just sent us, no need to echo it.
    return {
        "status": result.status,
        "score": result.score,
        "soft_score": result.soft_score,
        "error": result.error,
        "n_circles": len(result.circles) if result.circles else 0,
    }


# ---- Outer loop ----------------------------------------------------------


def _run_agent() -> dict:
    """Run the agent until the token budget is exhausted or the target hit.

    Returns the final state dict (memory, total_tokens, rounds,
    best_score, history). Wrapped in a function so the slicer doesn't
    flag the loop counters as runtime divergences.
    """
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    memory: list[dict] = []
    total_tokens = 0
    round_idx = 0
    last_input_tokens = 0
    history: list[dict] = []

    while True:
        best_score = max((m["score"] for m in memory), default=0.0)
        if best_score >= TARGET:
            print(f"target hit at score {best_score:.4f} — stopping")
            break
        if not _next_call_fits(total_tokens, last_input_tokens):
            print(
                f"\nbudget headroom too low (used {total_tokens:,} / "
                f"{TOKEN_BUDGET:,}, last_input ~{last_input_tokens:,}); "
                "stopping before next call"
            )
            break

        round_idx += 1
        print(f"\n--- round {round_idx} (best={best_score:.4f}, tokens={total_tokens:,}) ---")

        messages = [
            {"role": "user", "content": _build_user_prompt(memory, total_tokens, best_score)},
        ]

        for step in range(TOOL_CALL_CAP):
            if not _next_call_fits(total_tokens, last_input_tokens):
                # Outer loop will print and break on the next iteration.
                break
            # Force a tool call on the first step of each round so the
            # agent can't end its turn without trying anything (the
            # earlier run had 17/22 rounds spent on text-only thinking
            # with no progress). After step 0, ``auto`` lets the agent
            # observe its tool result, refine, or end naturally.
            tool_choice = {"type": "any"} if step == 0 else {"type": "auto"}
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS_PER_RESPONSE,
                system=SYSTEM_PROMPT,
                messages=messages,
                tools=TOOLS,
                tool_choice=tool_choice,
            )
            last_input_tokens = response.usage.input_tokens
            used = response.usage.input_tokens + response.usage.output_tokens
            total_tokens += used

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                break

            if response.stop_reason != "tool_use":
                print(f"  unexpected stop_reason={response.stop_reason}; ending round")
                break

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                out = _run_tool(block.name, block.input, memory)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(out, default=str),
                    }
                )
                print(
                    f"  step {step + 1}: {block.name} -> "
                    f"status={out.get('status')}, score={out.get('score', 0):.4f}, "
                    f"tokens this turn={used} (cum {total_tokens:,})"
                )
            messages.append({"role": "user", "content": tool_results})

        history.append(
            {
                "round": round_idx,
                "tokens_after": total_tokens,
                "memory_len": len(memory),
                "best_score": max((m["score"] for m in memory), default=0.0),
            }
        )

    return {
        "memory": memory,
        "total_tokens": total_tokens,
        "rounds": round_idx,
        "best_score": max((m["score"] for m in memory), default=0.0),
        "history": history,
    }


state = _run_agent()

print(
    f"\n=== done: rounds={state['rounds']}, attempts={len(state['memory'])}, "
    f"tokens={state['total_tokens']:,}, best={state['best_score']:.4f} ==="
)
