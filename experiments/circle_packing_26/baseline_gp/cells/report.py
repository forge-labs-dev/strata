# @name Report — best result, plot, history
#
# Reads the final ``state`` from the loop cell and produces:
#   - the best score, target gap, and total iterations evaluated
#   - a matplotlib figure of the best packing
#   - the source of the top-3 strategies for inspection

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

best = max(state["population"], key=lambda d: d["score"]) if state["population"] else None
best_score = state["best_score"]
target = 2.636

print(f"Final best score: {best_score:.4f}")
print(f"Target:           {target}")
print(f"Gap to target:    {target - best_score:+.4f}")
print(f"Iterations run:   {state['iter']}")
print(f"Total candidates: {state['history'][-1]['n_evaluated']}")
print(f"Successful:       {state['history'][-1]['n_ok']}")
print(f"Total tokens:     {state.get('total_tokens', 0):,} (budget 200,000)")

if best is None:
    print("\nNo feasible solution found.")
else:
    # ------- Plot the best packing -------
    fig, ax = plt.subplots(1, 1, figsize=(6, 6))
    ax.set_aspect("equal")
    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.02, 1.02)
    ax.add_patch(
        mpatches.Rectangle((0, 0), 1, 1, fill=False, linewidth=1.5, edgecolor="black")
    )
    for x, y, r in best["circles"]:
        ax.add_patch(
            mpatches.Circle((x, y), r, fill=True, alpha=0.35, edgecolor="navy", linewidth=0.8)
        )
    ax.set_title(
        f"Best packing — sum(r) = {best_score:.4f} "
        f"({'≥' if best_score >= target else '<'} target {target})"
    )
    ax.set_xticks([])
    ax.set_yticks([])
    plt.tight_layout()
    plt.show()
    plt.close(fig)

    # ------- Top-3 source listing -------
    print("\n" + "=" * 60)
    print("TOP STRATEGIES BY SCORE")
    print("=" * 60)
    sorted_pop = sorted(state["population"], key=lambda d: d["score"], reverse=True)[:3]
    for i, d in enumerate(sorted_pop):
        print(f"\n--- #{i + 1}: score {d['score']:.4f} ---")
        print(d["source"])

    # ------- Score-over-time plot -------
    fig2, ax2 = plt.subplots(1, 1, figsize=(8, 3.5))
    iters = [h["iter"] for h in state["history"]]
    bests = [h["best_score"] for h in state["history"]]
    ax2.plot(iters, bests, "-o", markersize=3, label="best score")
    ax2.axhline(target, color="green", linestyle="--", alpha=0.5, label=f"target {target}")
    ax2.set_xlabel("iteration")
    ax2.set_ylabel("best score")
    ax2.set_title("Best score over iterations")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()
    plt.close(fig2)
