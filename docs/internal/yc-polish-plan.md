# YC S26 Polish Plan

**Status:** Draft. Deadline mid-May 2026 (~4 weeks out as of 2026-04-13).

## Goal

Get the repo, hosted demo, and distributed example to the state where a YC
reviewer spending <5 minutes walks away with one clear, memorable claim and
at least one "oh, that's clever" moment.

This plan is about polish, not features. Feature work competes with polish;
default to polish.

## The reviewer's path

Reviewers don't explore — they skim in a fixed order. Optimize for that order:

1. **README / repo page** — first ~30 seconds. One hero sentence, one GIF,
   one install line. Everything else is optional.
2. **Hosted demo** (strata-notebook.fly.dev) — clicked by maybe 40% of
   readers. First 90 seconds decide if they keep going. Cold boot, empty
   state, and "how do I try something real" all matter.
3. **arXiv classifier example** — the deepest a reviewer will go. Our
   strongest story: one notebook, three runtimes (local, df-cluster Fly,
   GPU Modal), results persist and dedupe. Needs to render clean on first
   open.
4. **Founder write-up / video** — separate artifact, but the repo has to
   back up its claims.

## The single memorable claim

Every piece of polish below assumes we've picked *one* story. Options:

- **Artifact-first notebooks** — every cell output is a versioned artifact;
  reruns dedupe across people and sessions.
- **Crash-safe, resumable compute** — kill the kernel, close the browser,
  come back tomorrow; nothing is lost.
- **Shared cache across your team** — one team member's expensive run
  warms the cache for everyone.
- **Heterogeneous workers in one notebook** — SQL cell on Fly, embedding
  cell on a Modal GPU, aggregation cell local, all orchestrated by
  lineage.

**Open question (blocks README and GIF):** which one?

Leaning toward "artifact-first notebooks" as the umbrella (the other three
are consequences of it), but the arxiv_classifier demo is visually strongest
for the heterogeneous-workers story. Possibly: lead with artifact-first in
copy, show heterogeneous workers in the GIF.

## Work, ordered by leverage per hour

### Must-ship before submission

| # | Item | Estimate | Blocks on |
|---|------|----------|-----------|
| 1 | 60-second demo GIF at top of README | 0.5 day | chosen story |
| 2 | README tightening (elevator + 3 bullets + GIF + install) | 0.5 day | chosen story |
| 3 | Hosted demo: "Try the distributed demo" one-click load | 1 day | — |
| 4 | arxiv_classifier full end-to-end run against deployed workers | 0.5 day | — |
| 5 | Error states audit: no scary stack traces in the UI | 1 day | — |
| 6 | Cold-start latency on hosted notebook (paid Fly instance or warming indicator) | 0.5 day | budget decision |

**Subtotal: ~4 days of focused work.**

### High-leverage if time permits

- Reviewer-safe demo access — rate limit + per-IP Modal/Fly budget so
  anonymous use doesn't blow up cost. Without this, we can't let random
  visitors run cells; with it, we enable the strongest demo.
- 2-minute narrated video (unlisted YouTube) — for the YC application video
  slot. Explains *why*; GIF shows *what*.
- Landing page at a proper domain (strata.dev or similar) instead of the
  GitHub README being the front door.
- Pitch deck support assets — one architecture diagram, one comparison
  slide vs. existing notebook tools (Jupyter, Hex, Deepnote, Databricks).

### Nice-to-have (skip if crunched)

- Dark mode consistency pass, dashboard of example notebooks, technical
  blog post, SEO tuning, social card metadata, favicon.

## Risks and known gaps

- **Cold start** on Fly free tier is 10s. Looks broken to first-time
  visitors. Either pay for always-on, or add explicit "Waking up..."
  state in the frontend bootstrap.
- **Anonymous abuse** — if we enable "Try it" without auth, a single bad
  actor can run unbounded GPU cells on our Modal account. No per-IP
  quotas exist today.
- **Notebook state persistence** — the hosted strata-notebook.fly.dev has
  ephemeral storage. A reviewer's work disappears if the VM recycles.
  May not matter for a 90-second poke, but would matter if they share a
  link.
- **Deployment mode footguns** — just spent an afternoon on the
  personal/service mode bug. There are likely more. An end-to-end review
  of the service-mode UX (what a fresh reviewer sees at
  strata-notebook.fly.dev in service mode) is worth budgeting.
- **Example quality** — arxiv_classifier is great but long. For the GIF
  and first-visit story we may want a shorter "hello world" notebook
  that's faster to grok.

## Open questions (need answers before execution)

1. **Which story is the hero?** (blocks README + GIF)
2. **GIF: who drives the recording** — you (designer eye) or me (screen
   recorder)?
3. **Anonymous demo access** — on or off? If on, what's the per-IP
   budget and who sets up the rate limiting?
4. **Video** — in scope here, or being handled separately alongside the
   pitch deck?
5. **Paid Fly instance for hosted demo** — approved spend, or find a
   workaround (warming indicator, docs-only demo)?
6. **Domain / landing page** — in scope for this sprint, or ship with
   just the README?
7. **Short demo notebook** — do we have budget to write a 3-cell "hello
   world" notebook distinct from arxiv_classifier, or does arxiv_classifier
   serve both the GIF and the deep-dive roles?

## Execution checklist (fill in after answering open questions)

- [ ] Decide hero story
- [ ] Decide budget questions (3, 5)
- [ ] Draft README rewrite
- [ ] Record GIF against rewritten README narrative
- [ ] Wire "Try the distributed demo" on strata-notebook.fly.dev
- [ ] Run arxiv_classifier end-to-end, fix anything broken
- [ ] Error state audit (walk every UI error path)
- [ ] Cold-start fix
- [ ] Optional: video, rate limits, landing domain
- [ ] Final dry run — have someone unfamiliar click through as a reviewer

## Principle

Every item on this list should make a 5-minute reviewer more likely to
remember us. If an item doesn't, it's feature work, not polish — park it.
