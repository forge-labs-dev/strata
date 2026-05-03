# @name Seed strategies — initial population
#
# Three hand-written ``propose`` functions used as the population's
# starting point. The LLM sees these as "best so far" on the first
# iteration and writes its own variants from there.
#
# Each strategy is stored verbatim as a string so the ``run_candidate``
# pipeline runs it the same way it'll run LLM-proposed code — same
# parsing, same execution, same error surfacing.

# ---- Strategy 1: 5x5 + 1 corner ------------------------------------------
# A boring-but-feasible baseline. 25 equal-radius circles on a 5x5
# grid plus one tiny circle in the unused corner. Sum of radii ≈ 2.48.
SIMPLE_GRID = '''
import numpy as np

def propose(rng):
    """5x5 equal-radius grid plus one tiny corner circle (26 total)."""
    r = 0.099  # leaves 0.005 margin on each side of the 5x5 block
    out = []
    for i in range(5):
        for j in range(5):
            out.append((r + j * 2 * r, r + i * 2 * r, r))
    # 26th: a small circle in the corner outside the grid block.
    # Grid extends 0..0.99 in both axes; remaining strip is 0.99..1.0.
    # Place a tiny one at (0.9975, 0.9975) with r = 0.0024 so it stays
    # inside the unit square and doesn't touch the grid (nearest grid
    # center is at (0.891, 0.891), distance ~ 0.151, sum_r 0.099+0.002 = 0.101).
    out.append((0.9975, 0.9975, 0.0024))
    return out
'''.strip()


# ---- Strategy 2: random feasible — sequential greedy --------------------
# Drops circles one at a time, each as large as possible without
# overlapping the previous ones. Diverse final radii.
RANDOM_GREEDY = '''
import numpy as np

def propose(rng):
    """Sequential greedy: 26 random centers, each circle grown to the
    max radius that fits the unit square and avoids prior circles.
    """
    placed = []
    n_attempts = 200
    for _ in range(26):
        best = None
        for _ in range(n_attempts):
            x = rng.uniform(0, 1)
            y = rng.uniform(0, 1)
            r_max = min(x, y, 1 - x, 1 - y)
            for (px, py, pr) in placed:
                d = np.hypot(x - px, y - py)
                r_max = min(r_max, d - pr)
            if r_max <= 0:
                continue
            if best is None or r_max > best[2]:
                best = (x, y, r_max)
        if best is None:
            # Fall back: tiny circle in a corner-ish unused spot
            best = (0.5, 0.5, 1e-6)
        placed.append(best)
    return placed
'''.strip()


# ---- Strategy 3: scipy refinement from grid start ------------------------
# Initialize from SIMPLE_GRID, then optimize centers + radii jointly under
# a smooth penalty for boundary and overlap. Post-clip ensures feasibility.
SCIPY_REFINE = '''
import numpy as np
from scipy.optimize import minimize

def propose(rng):
    """5x5+1 init, scipy.minimize on penalized objective, post-clip."""
    # Initial layout: 5x5 grid plus a tiny corner circle.
    r0 = 0.099
    init = []
    for i in range(5):
        for j in range(5):
            init.append([r0 + j * 2 * r0, r0 + i * 2 * r0, r0])
    init.append([0.9975, 0.9975, 0.0024])
    init = np.array(init)  # (26, 3)

    PENALTY = 80.0

    def loss(params):
        p = params.reshape(26, 3)
        x, y, r = p[:, 0], p[:, 1], p[:, 2]
        s = -r.sum()
        b = (
            np.maximum(0, r - x).sum()
            + np.maximum(0, x + r - 1).sum()
            + np.maximum(0, r - y).sum()
            + np.maximum(0, y + r - 1).sum()
            + np.maximum(0, -r).sum()
        )
        o = 0.0
        for i in range(26):
            for j in range(i + 1, 26):
                d = np.hypot(x[i] - x[j], y[i] - y[j])
                o += max(0.0, r[i] + r[j] - d)
        return s + PENALTY * (b + o)

    res = minimize(loss, init.flatten(), method="L-BFGS-B", options={"maxiter": 300})
    p = res.x.reshape(26, 3)

    # Post-clip: enforce feasibility by shrinking each radius to the
    # largest value that respects the unit square AND every previously
    # placed circle. Order matters; scipy gave us a near-feasible
    # config so this only trims the slack.
    out = []
    for x, y, r in p:
        x = float(np.clip(x, 0.001, 0.999))
        y = float(np.clip(y, 0.001, 0.999))
        r_eff = max(0.001, min(float(r), x, y, 1 - x, 1 - y))
        for px, py, pr in out:
            d = np.hypot(x - px, y - py)
            r_eff = min(r_eff, d - pr)
        r_eff = max(r_eff, 1e-4)
        out.append((x, y, r_eff))
    return out
'''.strip()


SEED_STRATEGIES = [SIMPLE_GRID, RANDOM_GREEDY, SCIPY_REFINE]


# ---- Initial population: evaluate each seed strategy and stash it -------
# ``state`` is the carry variable for the evolve loop. The shape stays
# stable across iterations so the loop cell never has to do "first-time"
# checks.

_initial_population = []
_initial_failures = []
for source in SEED_STRATEGIES:
    r = run_candidate(source, rng_seed=0, timeout_seconds=30.0)
    print(f"seed status={r.status}, score={r.score:.4f}, soft={r.soft_score:.4f}")
    if r.error:
        print(f"  error: {r.error[:120]}")
    if r.status == "ok":
        _initial_population.append(r)
    else:
        _initial_failures.append(r)

_best = max((r.score for r in _initial_population), default=0.0)
state = {
    "iter": 0,
    "population": [result_to_dict(r) for r in _initial_population],
    "recent_failures": [result_to_dict(r) for r in _initial_failures],
    "best_score": _best,
    "total_tokens": 0,
    # Default predictor for the first iteration's cost — replaced by
    # the actual measurement after each iter. 7000 reserves headroom
    # for the next API call (~5k input + 2k max output).
    "last_iter_tokens": 7000,
    "history": [
        {
            "iter": 0,
            "best_score": _best,
            "n_evaluated": len(SEED_STRATEGIES),
            "n_ok": len(_initial_population),
            "total_tokens": 0,
        }
    ],
}
print(f"\nseed state: best={_best:.4f}, "
      f"ok={len(state['population'])}, failures={len(state['recent_failures'])}")
