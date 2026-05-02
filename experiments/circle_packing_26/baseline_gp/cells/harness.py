# @name Harness — scoring, validation, safe_exec
#
# Pure module cell: every helper is exported to downstream cells. The
# evolution loop calls ``run_candidate(source, rng_seed)`` and gets back
# a structured ``CandidateResult`` it can either keep (population) or
# show as a failure to the next LLM call.

from __future__ import annotations

import math
import signal
import traceback
from dataclasses import dataclass, field
from typing import Literal

N_CIRCLES = 26
TARGET_SCORE = 2.636

# Numerical slack for "is the circle inside the square / non-overlapping?"
# Tighter than the score gap between strategies, looser than scipy's
# convergence tolerance.
EPS = 1e-9

CandidateStatus = Literal[
    "ok",
    "syntax_error",
    "exec_error",
    "missing_propose",
    "propose_error",
    "timeout",
    "bad_output",
    "infeasible",
]


@dataclass
class CandidateResult:
    """One evaluated candidate strategy.

    ``status == "ok"`` means the strategy returned 26 valid, non-overlapping
    circles inside the unit square; ``score`` is the sum of radii.

    Anything else has ``score = 0``. The ``error`` field is a short,
    structured, human-readable string the LLM can act on next generation.
    ``soft_score`` always carries some signal, even when infeasible:
    ``sum(r) × feasibility_fraction``.
    """

    source: str
    status: CandidateStatus
    score: float = 0.0
    soft_score: float = 0.0
    circles: list[tuple[float, float, float]] | None = None
    error: str | None = None
    history: dict = field(default_factory=dict)


# --- Validation -----------------------------------------------------------


def _validate_circles(
    circles: list[tuple[float, float, float]],
) -> tuple[bool, list[str], float]:
    """Check the constraints. Returns ``(is_feasible, violations, frac_ok)``.

    ``violations`` is a list of short human-readable messages (capped to
    the first few so the LLM context doesn't explode). ``frac_ok`` is
    the fraction of circles with no violation — the soft signal.
    """
    n = len(circles)
    bad: set[int] = set()
    msgs: list[str] = []

    for i, c in enumerate(circles):
        if not (isinstance(c, (list, tuple)) and len(c) == 3):
            msgs.append(f"circles[{i}] is not a (x, y, r) triple")
            bad.add(i)
            continue
        try:
            x, y, r = float(c[0]), float(c[1]), float(c[2])
        except (TypeError, ValueError):
            msgs.append(f"circles[{i}] has non-numeric values")
            bad.add(i)
            continue
        if r <= 0:
            msgs.append(f"circles[{i}] has non-positive radius r={r:.4f}")
            bad.add(i)
            continue
        if x < r - EPS or x > 1 - r + EPS or y < r - EPS or y > 1 - r + EPS:
            over_x = max(0.0, r - x, (x + r) - 1)
            over_y = max(0.0, r - y, (y + r) - 1)
            msgs.append(
                f"circles[{i}] extends outside unit square "
                f"(x={x:.4f}, y={y:.4f}, r={r:.4f}; "
                f"over_x={over_x:.4f}, over_y={over_y:.4f})"
            )
            bad.add(i)

    # Pairwise overlaps. We collect at most a handful of representative
    # cases so the LLM sees concrete pairs without us blowing the
    # context window when a candidate is wildly broken.
    overlap_msgs: list[str] = []
    for i in range(n):
        ci = circles[i]
        if not (isinstance(ci, (list, tuple)) and len(ci) == 3):
            continue
        for j in range(i + 1, n):
            cj = circles[j]
            if not (isinstance(cj, (list, tuple)) and len(cj) == 3):
                continue
            try:
                xi, yi, ri = float(ci[0]), float(ci[1]), float(ci[2])
                xj, yj, rj = float(cj[0]), float(cj[1]), float(cj[2])
            except (TypeError, ValueError):
                continue
            d = math.hypot(xi - xj, yi - yj)
            overlap = (ri + rj) - d
            if overlap > EPS:
                bad.add(i)
                bad.add(j)
                if len(overlap_msgs) < 5:
                    overlap_msgs.append(
                        f"circles[{i}] and circles[{j}] overlap by {overlap:.4f} "
                        f"(centers {d:.4f} apart, sum_radii {ri + rj:.4f})"
                    )

    if overlap_msgs:
        msgs.append("; ".join(overlap_msgs))

    frac_ok = (n - len(bad)) / n if n else 0.0
    return (not bad, msgs, frac_ok)


def _score_circles(circles) -> float:
    return float(sum(c[2] for c in circles))


# --- safe_exec ------------------------------------------------------------


class _CandidateTimeout(Exception):
    """Raised inside the SIGALRM handler when a candidate runs over budget."""


def _alarm_handler(signum, frame):
    raise _CandidateTimeout()


def _short_tb(limit: int = 6) -> str:
    """Return the last *limit* lines of the traceback — enough for the LLM
    to spot the line/error without flooding context."""
    tb = traceback.format_exc().strip().splitlines()
    return "\n".join(tb[-limit:])


def run_candidate(
    source: str,
    rng_seed: int = 0,
    timeout_seconds: float = 30.0,
) -> CandidateResult:
    """Compile, exec, run, and score a candidate strategy.

    Uses ``signal.setitimer`` for a soft wall-clock budget on
    ``propose()``. Pure-Python loops and most scipy.optimize calls
    return to the interpreter often enough that SIGALRM fires
    promptly; opaque C extensions can in principle ignore the signal
    until they yield. If we hit a hang in practice we'll switch to a
    real subprocess.
    """
    import numpy as np

    ns: dict = {}
    try:
        compiled = compile(source, "<llm-candidate>", "exec")
    except SyntaxError as exc:
        return CandidateResult(
            source=source,
            status="syntax_error",
            error=f"{exc.msg} at line {exc.lineno}",
        )

    try:
        exec(compiled, ns)  # noqa: S102
    except Exception:
        return CandidateResult(source=source, status="exec_error", error=_short_tb())

    propose = ns.get("propose")
    if not callable(propose):
        return CandidateResult(
            source=source,
            status="missing_propose",
            error="no callable named `propose` in module",
        )

    rng = np.random.default_rng(rng_seed)

    previous = signal.signal(signal.SIGALRM, _alarm_handler)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        circles_raw = propose(rng)
    except _CandidateTimeout:
        return CandidateResult(
            source=source,
            status="timeout",
            error=f"propose() exceeded {timeout_seconds:.0f}s wall time",
        )
    except Exception:
        return CandidateResult(source=source, status="propose_error", error=_short_tb())
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)

    try:
        circles = [(float(c[0]), float(c[1]), float(c[2])) for c in circles_raw]
    except Exception:
        return CandidateResult(
            source=source,
            status="bad_output",
            error="propose() did not return an iterable of 3-tuples",
        )

    if len(circles) != N_CIRCLES:
        return CandidateResult(
            source=source,
            status="bad_output",
            circles=circles,
            error=f"expected {N_CIRCLES} circles, got {len(circles)}",
        )

    feasible, violations, frac_ok = _validate_circles(circles)
    radii_sum = _score_circles(circles)
    soft = radii_sum * frac_ok

    if not feasible:
        return CandidateResult(
            source=source,
            status="infeasible",
            score=0.0,
            soft_score=soft,
            circles=circles,
            error="; ".join(violations[:3]) if violations else "constraint violation",
        )

    return CandidateResult(
        source=source,
        status="ok",
        score=radii_sum,
        soft_score=soft,
        circles=circles,
    )


# --- Pretty-printers used by the evolve cell ------------------------------


def format_summary(result: CandidateResult, *, show_source: bool = False) -> str:
    """Compact human-readable summary used inside the LLM prompt."""
    lines = [f"status={result.status}, score={result.score:.4f}, soft={result.soft_score:.4f}"]
    if result.error:
        lines.append(f"error: {result.error}")
    if show_source:
        lines.append("source:")
        lines.append(result.source.strip())
    return "\n".join(lines)


def result_to_dict(r: CandidateResult) -> dict:
    """Project a ``CandidateResult`` to a plain dict for the carry state.

    The carry value is written to disk as an Arrow / JSON artifact, so we
    can't ride a synthetic-module dataclass through it. Plain dicts
    serialize cleanly through the regular artifact path.
    """
    return {
        "source": r.source,
        "status": r.status,
        "score": r.score,
        "soft_score": r.soft_score,
        "circles": r.circles,
        "error": r.error,
    }


def format_dict_summary(d: dict, *, show_source: bool = False) -> str:
    """Same as ``format_summary`` but operates on a population dict."""
    lines = [f"status={d['status']}, score={d['score']:.4f}, soft={d['soft_score']:.4f}"]
    if d.get("error"):
        lines.append(f"error: {d['error']}")
    if show_source:
        lines.append("source:")
        lines.append(d["source"].strip())
    return "\n".join(lines)
