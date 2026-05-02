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
TEMPERATURE = 0.7
TOKEN_BUDGET = 200_000  # input + output tokens across the whole run
TOOL_CALL_CAP = 12       # max tool calls per agent round
PER_CANDIDATE_TIMEOUT = 30.0
TARGET = 2.636
MAX_TOKENS_PER_RESPONSE = 2048


SYSTEM_PROMPT = """You are working on the n=26 circle packing problem.
Your goal is to maximize the sum of radii of 26 non-overlapping circles
inside the unit square [0, 1] x [0, 1]. The known best is around 2.636.

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
    return (
        f"Memory ({n_total} attempts, {n_ok} feasible, best score "
        f"{best_score:.4f}):\n\n"
        f"{_format_memory(memory)}\n\n"
        f"Tokens used: {total_tokens:,} / {TOKEN_BUDGET:,}.\n"
        f"Target: {TARGET}.\n\n"
        "Propose and score new strategies. Use score_candidate as many "
        "times as you need (cap 12 per round). End your turn when "
        "you're done with this round."
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
    history: list[dict] = []

    while total_tokens < TOKEN_BUDGET:
        best_score = max((m["score"] for m in memory), default=0.0)
        if best_score >= TARGET:
            print(f"target hit at score {best_score:.4f} — stopping")
            break

        round_idx += 1
        print(
            f"\n--- round {round_idx} (best={best_score:.4f}, "
            f"tokens={total_tokens:,}) ---"
        )

        messages = [
            {"role": "user", "content": _build_user_prompt(memory, total_tokens, best_score)},
        ]

        for step in range(TOOL_CALL_CAP):
            response = client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS_PER_RESPONSE,
                temperature=TEMPERATURE,
                system=SYSTEM_PROMPT,
                messages=messages,
                tools=TOOLS,
            )
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

            if total_tokens >= TOKEN_BUDGET:
                break

        history.append(
            {
                "round": round_idx,
                "tokens_after": total_tokens,
                "memory_len": len(memory),
                "best_score": max((m["score"] for m in memory), default=0.0),
            }
        )

        if total_tokens >= TOKEN_BUDGET:
            print(f"\nbudget exhausted at {total_tokens:,} tokens")
            break

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
