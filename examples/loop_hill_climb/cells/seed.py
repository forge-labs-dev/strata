# @name Seed State
# Seed the hill-climb search from a random point on the Himmelblau surface.
# Himmelblau's function has four equal-valued minima at roughly
# (3, 2), (-2.8, 3.1), (-3.8, -3.3), (3.6, -1.8). That makes it a nice
# demo because different random seeds converge to different basins,
# and a @loop start_from fork can rewind to an earlier state and try
# a different exploration strategy.
import random


def himmelblau(x: float, y: float) -> float:
    return (x**2 + y - 11) ** 2 + (x + y**2 - 7) ** 2


random.seed(42)
x = random.uniform(-5, 5)
y = random.uniform(-5, 5)
score = himmelblau(x, y)

state = {
    "x": x,
    "y": y,
    "best_score": score,
    # Step size shrinks each accepted move so the search sharpens as it
    # approaches a minimum.
    "step": 1.0,
    # history accumulates every proposal (accepted or rejected) so the
    # plot cell can show the whole trajectory, not just the kept moves.
    "history": [{"iter": 0, "x": x, "y": y, "score": score, "accepted": True}],
    # iteration counter lets each loop iteration deterministically seed
    # its own RNG — running the notebook twice reproduces the same
    # trajectory.
    "iter": 0,
}

print(f"Seed at ({x:.3f}, {y:.3f}) with score {score:.3f}")
state
