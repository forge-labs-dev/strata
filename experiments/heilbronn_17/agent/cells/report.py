# @name Report — best result, plot, history

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

ok_attempts = [m for m in state["memory"] if m["status"] == "ok"]
best = max(ok_attempts, key=lambda m: m["score"]) if ok_attempts else None
best_score = state["best_score"]

print(f"Final best score: {best_score:.6f}")
print(f"Rounds run:       {state['rounds']}")
print(f"Total attempts:   {len(state['memory'])}")
print(f"Successful:       {len(ok_attempts)}")
print(f"Total tokens:     {state['total_tokens']:,} (budget 200,000)")

if best is None:
    print("\nNo feasible solution found.")
else:
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

    print("\n" + "=" * 60)
    print("TOP ATTEMPTS BY SCORE")
    print("=" * 60)
    sorted_ok = sorted(ok_attempts, key=lambda m: m["score"], reverse=True)[:3]
    for i, m in enumerate(sorted_ok):
        print(f"\n--- #{i + 1}: score {m['score']:.6f} ---")
        print(f"insight: {m.get('insight') or '(none)'}")
        print(m["source"])

    fig2, ax2 = plt.subplots(1, 1, figsize=(8, 3.5))
    running_best = []
    cur = 0.0
    for m in state["memory"]:
        cur = max(cur, m["score"])
        running_best.append(cur)
    ax2.plot(range(1, len(running_best) + 1), running_best, "-o", markersize=3, label="best so far")
    ax2.set_xlabel("attempt index")
    ax2.set_ylabel("best min-triangle area")
    ax2.set_title(
        f"Best score over attempts ({state['rounds']} rounds, "
        f"{state['total_tokens']:,} tokens)"
    )
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()
    plt.close(fig2)
