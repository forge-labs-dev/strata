# @name Harness — scoring, validation, safe_exec
#
# Pure module cell exporting helpers used by seed / evolve / report.
# The Heilbronn problem differs from circle packing in two ways:
#
#   1. Every distinct point set inside the unit square is "feasible"
#      — the only failure modes are bounds violations and duplicates,
#      both of which we treat as bad_output.
#   2. The score is the *minimum* triangle area over all C(n, 3)
#      triples, so improvement is bottleneck-driven: moving one point
#      to break the smallest triangle is what gains points. The soft
#      score is the same as the hard score (no feasibility fraction
#      to project against).

from __future__ import annotations

import itertools
import signal
import traceback
from dataclasses import dataclass, field
from typing import Literal

N_POINTS = 17
TARGET_SCORE = 0.0210  # rough "good performance" anchor; not a SOTA value
EPS = 1e-9

CandidateStatus = Literal[
    "ok",
    "syntax_error",
    "exec_error",
    "missing_propose",
    "propose_error",
    "timeout",
    "bad_output",
]


@dataclass
class CandidateResult:
    """One evaluated candidate strategy.

    ``status == "ok"`` means the strategy returned 17 distinct points
    inside the unit square; ``score`` is the minimum triangle area
    over all C(17, 3) = 680 triples. Higher is better.

    Failure statuses (syntax_error / exec_error / timeout / bad_output)
    have ``score = 0`` and an ``error`` field explaining why. The
    bottleneck triple — the three points forming the smallest triangle
    — is recorded so the LLM can see which points it should move next.
    """

    source: str
    status: CandidateStatus
    score: float = 0.0
    soft_score: float = 0.0
    points: list[tuple[float, float]] | None = None
    bottleneck: tuple[int, int, int] | None = None
    error: str | None = None
    history: dict = field(default_factory=dict)


# --- Scoring --------------------------------------------------------------


def _triangle_area(p1: tuple[float, float], p2: tuple[float, float], p3: tuple[float, float]) -> float:
    """Unsigned area of the triangle with the given three vertices."""
    return 0.5 * abs(
        p1[0] * (p2[1] - p3[1]) + p2[0] * (p3[1] - p1[1]) + p3[0] * (p1[1] - p2[1])
    )


def _min_triangle_area(points: list[tuple[float, float]]) -> tuple[float, tuple[int, int, int]]:
    """Return the smallest triangle area and the indices of its
    vertices. Iterates over every triple — O(n^3), trivial at n=17.
    """
    best_area = float("inf")
    best_triple: tuple[int, int, int] = (0, 1, 2)
    for i, j, k in itertools.combinations(range(len(points)), 3):
        area = _triangle_area(points[i], points[j], points[k])
        if area < best_area:
            best_area = area
            best_triple = (i, j, k)
    return best_area, best_triple


# --- Validation -----------------------------------------------------------


def _validate_points(
    points: list[tuple[float, float]],
) -> tuple[bool, str | None]:
    """Check shape, bounds, and uniqueness. Returns ``(ok, message)``;
    the message is ``None`` on success and a short reason otherwise.
    """
    if len(points) != N_POINTS:
        return False, f"expected {N_POINTS} points, got {len(points)}"
    seen: set[tuple[float, float]] = set()
    for i, p in enumerate(points):
        if not (isinstance(p, (list, tuple)) and len(p) == 2):
            return False, f"points[{i}] is not an (x, y) pair"
        try:
            x, y = float(p[0]), float(p[1])
        except (TypeError, ValueError):
            return False, f"points[{i}] has non-numeric coordinates"
        if not (-EPS <= x <= 1 + EPS and -EPS <= y <= 1 + EPS):
            return False, f"points[{i}] = ({x:.4f}, {y:.4f}) is outside the unit square"
        # Duplicates → smallest triangle area is 0; not useful as
        # a candidate. Round to detect "essentially the same point."
        key = (round(x, 9), round(y, 9))
        if key in seen:
            return False, f"points[{i}] duplicates a prior point at ({x:.6f}, {y:.6f})"
        seen.add(key)
    return True, None


# --- safe_exec ------------------------------------------------------------


class _CandidateTimeout(Exception):
    """Raised inside the SIGALRM handler when a candidate runs over budget."""


def _alarm_handler(signum, frame):
    raise _CandidateTimeout()


def _short_tb(limit: int = 6) -> str:
    """Last *limit* lines of the current traceback — enough for the LLM
    to see which line failed without flooding context."""
    tb = traceback.format_exc().strip().splitlines()
    return "\n".join(tb[-limit:])


def run_candidate(
    source: str,
    rng_seed: int = 0,
    timeout_seconds: float = 30.0,
) -> CandidateResult:
    """Compile, exec, run, and score a candidate strategy.

    The candidate must define ``def propose(rng) -> list[(x, y)]``
    returning ``N_POINTS`` distinct points inside the unit square.
    Wall-clock budget enforced via ``signal.setitimer`` so a runaway
    scipy.optimize call can't hang the loop. Returns a structured
    ``CandidateResult`` with the bottleneck triple recorded so the
    LLM can reason about which points to move on the next attempt.
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
        points_raw = propose(rng)
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
        points = [(float(p[0]), float(p[1])) for p in points_raw]
    except Exception:
        return CandidateResult(
            source=source,
            status="bad_output",
            error="propose() did not return an iterable of (x, y) pairs",
        )

    ok, message = _validate_points(points)
    if not ok:
        return CandidateResult(
            source=source,
            status="bad_output",
            points=points,
            error=message,
        )

    score, bottleneck = _min_triangle_area(points)
    return CandidateResult(
        source=source,
        status="ok",
        score=score,
        # Heilbronn has no feasibility-fraction concept — every distinct
        # point set has a well-defined min-triangle area. Mirror it as
        # soft_score so prompts that print soft / hard side-by-side
        # still work cleanly.
        soft_score=score,
        points=points,
        bottleneck=bottleneck,
    )


# --- Pretty-printers used by the evolve / agent cells --------------------


def format_summary(result: CandidateResult, *, show_source: bool = False) -> str:
    """Compact human-readable summary used inside the LLM prompt."""
    lines = [f"status={result.status}, score={result.score:.6f}"]
    if result.error:
        lines.append(f"error: {result.error}")
    if result.bottleneck is not None and result.points is not None:
        i, j, k = result.bottleneck
        p_i, p_j, p_k = result.points[i], result.points[j], result.points[k]
        lines.append(
            f"bottleneck: points[{i}]={p_i}, points[{j}]={p_j}, "
            f"points[{k}]={p_k} → triangle area {result.score:.6f}"
        )
    if show_source:
        lines.append("source:")
        lines.append(result.source.strip())
    return "\n".join(lines)


def result_to_dict(r: CandidateResult) -> dict:
    """Project a ``CandidateResult`` to a plain dict for the carry state.

    The carry value is written to disk as a JSON / Arrow artifact, so
    we can't ride a synthetic-module dataclass through it. Plain dicts
    serialize cleanly through the regular artifact path.
    """
    return {
        "source": r.source,
        "status": r.status,
        "score": r.score,
        "soft_score": r.soft_score,
        "points": r.points,
        "bottleneck": list(r.bottleneck) if r.bottleneck is not None else None,
        "error": r.error,
    }


def format_dict_summary(d: dict, *, show_source: bool = False) -> str:
    """Same as ``format_summary`` but operates on a population dict."""
    lines = [f"status={d['status']}, score={d['score']:.6f}"]
    if d.get("error"):
        lines.append(f"error: {d['error']}")
    if d.get("bottleneck") is not None and d.get("points"):
        i, j, k = d["bottleneck"]
        p_i, p_j, p_k = d["points"][i], d["points"][j], d["points"][k]
        lines.append(
            f"bottleneck: points[{i}]={p_i}, points[{j}]={p_j}, "
            f"points[{k}]={p_k} → triangle area {d['score']:.6f}"
        )
    if show_source:
        lines.append("source:")
        lines.append(d["source"].strip())
    return "\n".join(lines)
