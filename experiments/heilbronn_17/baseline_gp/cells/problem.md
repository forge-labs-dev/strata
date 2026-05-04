# Heilbronn Triangle Problem — n = 17 (GP baseline)

## Problem

Place **17 points** inside the unit square `[0, 1] × [0, 1]` to
**maximize the smallest triangle area** formed by any three of them.
There are `C(17, 3) = 680` triples; the objective is

  `H₁₇(P) = min over (i, j, k) of triangle_area(p_i, p_j, p_k)`

and we want to find a point set `P` that maximizes `H₁₇(P)`.

## Status: open

The Heilbronn triangle problem has been studied since the 1950s.
For most `n`, the optimal value is **not known** — only "best so
far" constructions exist, with bounds tightened over time by hand
constructions, computer search, and recently (AlphaEvolve, 2024)
LLM-guided search. For `n=17` specifically, the published bounds
have remained loose, so any score that beats a careful
hand-constructed seed is a genuine improvement.

This makes Heilbronn a much harder thesis test than circle packing
n=26 (where the SOTA is well-documented and Opus has plausibly seen
the answer in training). The LLM may know rough bounds for small `n`,
but the precise SOTA values for `n=17` are not memorisable — the
architecture has to actually search.

## Approach

The same FunSearch-shaped loop that ran circle packing: a
population of 8, top-3 verbatim sources shown to the LLM each
iteration, recent low-scorers shown as feedback (with the
"bottleneck triple" naming the three points that form the smallest
triangle — the LLM's natural target for the next attempt). Stops on
the 200,000-token budget.

## Setup

The cell harness needs `ANTHROPIC_API_KEY` in the runtime env
(set it from the Runtime panel).
