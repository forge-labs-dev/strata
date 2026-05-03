# @name Report — best result, plot, history
#
# Reads the agent's final ``state`` and produces:
#   - the best score, target gap, total tokens spent
#   - a matplotlib figure of the best packing
#   - the source of the top-3 attempts (by score) for inspection

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

ok_attempts = [m for m in state["memory"] if m["status"] == "ok"]
best = max(ok_attempts, key=lambda m: m["score"]) if ok_attempts else None
best_score = state["best_score"]
target = 2.636

print(f"Final best score: {best_score:.4f}")
print(f"Target:           {target}")
print(f"Gap to target:    {target - best_score:+.4f}")
print(f"Rounds run:       {state['rounds']}")
print(f"Total attempts:   {len(state['memory'])}")
print(f"Successful:       {len(ok_attempts)}")
print(f"Total tokens:     {state['total_tokens']:,} (budget 200,000)")

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
    print("TOP ATTEMPTS BY SCORE")
    print("=" * 60)
    sorted_ok = sorted(ok_attempts, key=lambda m: m["score"], reverse=True)[:3]
    for i, m in enumerate(sorted_ok):
        print(f"\n--- #{i + 1}: score {m['score']:.4f} ---")
        print(f"insight: {m.get('insight') or '(none)'}")
        print(m["source"])

    # ------- Score-over-time plot (per-attempt) -------
    fig2, ax2 = plt.subplots(1, 1, figsize=(8, 3.5))
    # running max of score across attempts
    running_best = []
    cur = 0.0
    for m in state["memory"]:
        cur = max(cur, m["score"])
        running_best.append(cur)
    ax2.plot(range(1, len(running_best) + 1), running_best, "-o", markersize=3, label="best so far")
    ax2.axhline(target, color="green", linestyle="--", alpha=0.5, label=f"target {target}")
    ax2.set_xlabel("attempt index")
    ax2.set_ylabel("best score")
    ax2.set_title(
        f"Best score over attempts ({state['rounds']} rounds, "
        f"{state['total_tokens']:,} tokens)"
    )
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()
    plt.close(fig2)
