# @name Seed strategies — initial population
#
# Three hand-written ``propose`` functions for n=17. Each scores
# > 0 (no collinear triples) so the population starts with real
# numbers the LLM can iterate on.
#
# A regular grid is intentionally NOT among the seeds — three points
# in a row of a grid are collinear, so the min triangle area is 0
# and the candidate scores nothing. Every seed below adds enough
# perturbation or non-grid structure to break collinearity.

# ---- Strategy 1: random uniform with jitter ------------------------------
# Plain uniform random sampling. With 17 points in [0,1]² the chance
# of three exactly collinear is zero (continuous distribution), so this
# always gives a non-zero score. Weak as a baseline (~0.005-0.010) but
# always feasible.
RANDOM_UNIFORM = '''
import numpy as np

def propose(rng):
    """17 i.i.d. uniform random points inside the unit square."""
    pts = []
    for _ in range(17):
        pts.append((float(rng.uniform(0.001, 0.999)),
                    float(rng.uniform(0.001, 0.999))))
    return pts
'''.strip()


# ---- Strategy 2: Halton sequence with tiny jitter ------------------------
# Halton (base 2, base 3) gives a low-discrepancy distribution. Pure
# rational coords can produce exactly collinear triples — a small
# random jitter breaks that without disturbing the structure.
HALTON_JITTERED = '''
import numpy as np

def _halton(i, base):
    f, r = 1.0, 0.0
    while i > 0:
        f /= base
        r += f * (i % base)
        i //= base
    return r

def propose(rng):
    """First 17 Halton (2, 3) points with a tiny per-point jitter."""
    pts = []
    for i in range(1, 18):
        x = _halton(i, 2) + rng.normal(0, 0.003)
        y = _halton(i, 3) + rng.normal(0, 0.003)
        pts.append((float(np.clip(x, 0.001, 0.999)),
                    float(np.clip(y, 0.001, 0.999))))
    return pts
'''.strip()


# ---- Strategy 3: multi-start SLSQP max-min formulation -------------------
# Maximize t subject to (triangle_area_ijk >= t) for every triple of
# points. 680 constraints at n=17 — SLSQP handles this in a few
# seconds. Multi-start over random / Halton inits to escape weak
# local optima from any single seed.
SLSQP_MULTISTART = '''
import numpy as np
from scipy.optimize import minimize

def _halton(i, base):
    f, r = 1.0, 0.0
    while i > 0:
        f /= base
        r += f * (i % base)
        i //= base
    return r

def _optimize_from(init, n=17):
    """Run one SLSQP max-min optimization from *init* (n,2)."""
    x0 = np.concatenate([[0.0], init.flatten()])

    def neg_t(v):
        return -v[0]

    def neg_t_grad(v):
        g = np.zeros_like(v)
        g[0] = -1.0
        return g

    def make_area_constraint(i, j, k):
        def fn(v):
            t = v[0]
            xi, yi = v[1 + 2 * i], v[2 + 2 * i]
            xj, yj = v[1 + 2 * j], v[2 + 2 * j]
            xk, yk = v[1 + 2 * k], v[2 + 2 * k]
            area = 0.5 * abs(xi * (yj - yk) + xj * (yk - yi) + xk * (yi - yj))
            return area - t
        return fn

    constraints = []
    for i in range(n):
        for j in range(i + 1, n):
            for k in range(j + 1, n):
                constraints.append({"type": "ineq", "fun": make_area_constraint(i, j, k)})

    bounds = [(0, 1)] + [(0.001, 0.999)] * (2 * n)
    return minimize(
        neg_t,
        x0,
        jac=neg_t_grad,
        method="SLSQP",
        constraints=constraints,
        bounds=bounds,
        options={"maxiter": 150, "ftol": 1e-9},
    )

def propose(rng):
    n = 17
    inits = []
    # Halton with jitter
    halton = np.array([[_halton(i, 2), _halton(i, 3)] for i in range(1, n + 1)])
    inits.append(np.clip(halton + rng.normal(0, 0.01, halton.shape), 0.001, 0.999))
    # Two pure random uniform starts
    for _ in range(2):
        inits.append(rng.uniform(0.05, 0.95, (n, 2)))
    # 4×5 lattice (drop 3 corners) + jitter
    grid = []
    for i in range(4):
        for j in range(5):
            grid.append([(i + 1) / 5, (j + 1) / 6])
    grid = np.array(grid)
    drop = [0, 4, 19]
    grid = np.delete(grid, drop, axis=0)
    inits.append(np.clip(grid + rng.normal(0, 0.02, grid.shape), 0.001, 0.999))

    best_score = -1.0
    best_points = None
    for init in inits:
        try:
            res = _optimize_from(init, n=n)
            p = res.x[1:].reshape(n, 2)
            # Re-evaluate the actual min triangle area (SLSQP's t can
            # be optimistic if constraints are slightly violated).
            min_area = float("inf")
            for i in range(n):
                for j in range(i + 1, n):
                    for k in range(j + 1, n):
                        a = 0.5 * abs(
                            p[i, 0] * (p[j, 1] - p[k, 1])
                            + p[j, 0] * (p[k, 1] - p[i, 1])
                            + p[k, 0] * (p[i, 1] - p[j, 1])
                        )
                        if a < min_area:
                            min_area = a
            if min_area > best_score:
                best_score = min_area
                best_points = p
        except Exception:
            continue

    if best_points is None:
        # Fallback to a Halton arrangement.
        best_points = np.clip(halton + rng.normal(0, 0.01, halton.shape), 0.001, 0.999)
    return [(float(x), float(y)) for x, y in best_points]
'''.strip()


SEED_STRATEGIES = [RANDOM_UNIFORM, HALTON_JITTERED, SLSQP_MULTISTART]


# ---- Initial population --------------------------------------------------

_initial_population = []
_initial_failures = []
for source in SEED_STRATEGIES:
    r = run_candidate(source, rng_seed=0, timeout_seconds=30.0)
    print(f"seed status={r.status}, score={r.score:.6f}")
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
print(
    f"\nseed state: best={_best:.6f}, "
    f"ok={len(state['population'])}, failures={len(state['recent_failures'])}"
)
