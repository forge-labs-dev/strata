# @name Report — best result, plot, history

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

best = max(state["population"], key=lambda d: d["score"]) if state["population"] else None
best_score = state["best_score"]

print(f"Final best score: {best_score:.6f}")
print(f"Iterations run:   {state['iter']}")
print(f"Total candidates: {state['history'][-1]['n_evaluated']}")
print(f"Successful:       {state['history'][-1]['n_ok']}")
print(f"Total tokens:     {state.get('total_tokens', 0):,} (budget 200,000)")

if best is None:
    print("\nNo feasible solution found.")
else:
    # ------- Plot the best point set, highlighting the bottleneck -------
    fig, ax = plt.subplots(1, 1, figsize=(6, 6))
    ax.set_aspect("equal")
    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.02, 1.02)
    ax.add_patch(
        mpatches.Rectangle((0, 0), 1, 1, fill=False, linewidth=1.5, edgecolor="black")
    )
    points = best["points"]
    bottleneck = best.get("bottleneck")
    bn_set = set(bottleneck) if bottleneck else set()
    for i, (x, y) in enumerate(points):
        color = "red" if i in bn_set else "navy"
        size = 60 if i in bn_set else 30
        ax.scatter([x], [y], s=size, color=color, zorder=3)
        ax.annotate(
            str(i),
            (x, y),
            xytext=(4, 4),
            textcoords="offset points",
            fontsize=8,
            color="dimgray",
        )
    if bottleneck is not None:
        bi, bj, bk = bottleneck
        triangle = plt.Polygon(
            [points[bi], points[bj], points[bk]],
            fill=True,
            alpha=0.2,
            facecolor="red",
            edgecolor="red",
            linewidth=1.2,
            zorder=2,
        )
        ax.add_patch(triangle)
    ax.set_title(f"Heilbronn n=17 — best min-triangle area = {best_score:.6f}")
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
        print(f"\n--- #{i + 1}: score {d['score']:.6f} ---")
        print(d["source"])

    # ------- Score-over-time plot -------
    fig2, ax2 = plt.subplots(1, 1, figsize=(8, 3.5))
    iters = [h["iter"] for h in state["history"]]
    bests = [h["best_score"] for h in state["history"]]
    ax2.plot(iters, bests, "-o", markersize=3, label="best score")
    ax2.set_xlabel("iteration")
    ax2.set_ylabel("best min-triangle area")
    ax2.set_title("Best score over iterations")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()
    plt.close(fig2)
