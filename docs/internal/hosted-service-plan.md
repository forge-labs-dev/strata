# Strata Notebook — Hosted Service Plan

**Strategic Overview | April 2026 | Forge Labs — Confidential**

---

## 1. Executive Summary

> **Core thesis:** The hosted service monetizes managed compute on top of Strata's content-addressed artifact layer. Users pay for unique computations only — cache hits are free. The artifact store, provenance dedup, and lineage tracking are the moat; managed workers and collaboration are the product.

The hosted Strata Notebook service wraps the open-source notebook in a multi-tenant cloud platform. It adds five capabilities that don't belong in the OSS project: managed compute orchestration, user identity and collaboration, usage-based billing, hosted storage, and enterprise compliance. Everything else — the artifact store, executor protocol, DAG engine, cell runtime — remains in the open-source core.

## 2. Platform Architecture

The hosted service is structured as a set of services layered on top of the open-source Strata core. The key design principle is that no hosted-only logic leaks into the core. The hosted layer is a consumer of the same APIs and executor protocol that self-hosted users rely on.

### Layering

- **Presentation tier:** Web app (notebook UI), CLI, and REST API for programmatic access.
- **Platform tier:** Authentication, workspace management, collaboration, billing metering. Stateless services behind a load balancer.
- **Compute tier:** Managed worker pool implementing the Strata executor protocol. Routes cells to appropriate hardware (CPU, GPU, high-memory) based on cell configuration.
- **Storage tier:** Artifact blob storage (S3/GCS), notebook metadata (managed Postgres), and the per-notebook SQLite artifact databases replicated to object storage.
- **Core tier:** Open-source Strata — artifact store, provenance engine, DAG planner, cell executor. Runs unmodified.

## 3. Component Breakdown

### 3.1 Identity and Access

User authentication, workspace/team management, and role-based access control. The hosted service uses Strata's existing trusted proxy auth model — the platform tier injects X-Strata-Principal and X-Strata-Scopes headers after authenticating users via OAuth/SSO.

| Component | Responsibility | Build vs Buy |
|-----------|---------------|-------------|
| Auth service | OAuth 2.0 / SSO login, JWT issuance, session management | Buy (Auth0 / Clerk) |
| Workspace service | Team creation, member invites, role assignment (owner/editor/viewer) | Build |
| API gateway | Route requests, inject identity headers, rate limiting | Buy (Kong / AWS ALB) |

### 3.2 Managed Compute

The core monetization layer. Manages a fleet of workers that implement the Strata executor protocol. Users select a worker class from the cell toolbar; the platform routes the cell execution to an available worker of that class.

| Component | Responsibility | Build vs Buy |
|-----------|---------------|-------------|
| Worker registry | Track available workers, capabilities (GPU type, memory), health status | Build |
| Scheduler / router | Match cell resource requirements to available workers, queue management | Build |
| Worker fleet | Actual compute instances running the cell harness. Autoscale based on demand | Buy (EC2/GCE + autoscaler) |
| GPU provisioner | Spot instance management, fallback to on-demand, multi-region availability | Build on cloud APIs |

> **Key decision:** V1 should use Modal or RunPod as the GPU backend rather than managing instances directly. This eliminates cold-start engineering, instance lifecycle management, and GPU driver maintenance. Add margin on top of their per-second pricing. Bring compute in-house only after reaching volume that justifies the operational investment.

### 3.3 Storage and Data

Notebook state, artifact blobs, and user data. The open-source notebook stores artifacts in a local SQLite database and local blob directory. The hosted service replaces these with managed equivalents while keeping the same BlobStore interface.

| Component | Responsibility | Build vs Buy |
|-----------|---------------|-------------|
| Notebook metadata DB | Notebook ownership, sharing, version history. Managed Postgres | Buy (RDS / Cloud SQL) |
| Artifact blob storage | Artifact data (Arrow IPC). Use Strata's existing S3BlobStore | Buy (S3 / GCS) |
| Artifact metadata | SQLite per notebook, replicated to S3 for durability. Loaded on session start | Build (replication layer) |
| File mount proxy | Secure access to user's S3/GCS buckets via scoped credentials | Build |
| Dataset catalog | Shared datasets within a workspace for reuse across notebooks | Build (later) |

### 3.4 Collaboration

Real-time multiplayer editing and sharing. This is the area where Hex has set high expectations. The strategy is to ship good-enough collaboration in V1 (share links, commenting, version history) and defer real-time multiplayer to V2.

| Component | Responsibility | Build vs Buy |
|-----------|---------------|-------------|
| Share links | Public/private notebook URLs with viewer/editor permissions | Build |
| Comments | Cell-level comments and threads, resolved/unresolved state | Build |
| Version history | Every execution creates an artifact version. Surface this as browsable history | Build (leverage artifact store) |
| Real-time cursors | See other editors' positions live. Conflict resolution on cell source | Build (V2, CRDT-based) |
| Publishing | One-click publish notebook as a read-only app or dashboard | Build (later) |

### 3.5 Billing and Metering

Usage-based pricing requires precise metering of compute time, storage, and data throughput. Every cell execution already produces an artifact with metadata (duration, row count, byte size) — this is the metering data source.

| Component | Responsibility | Build vs Buy |
|-----------|---------------|-------------|
| Metering pipeline | Collect execution events, compute seconds, bytes stored. Real-time aggregation | Build |
| Billing service | Invoice generation, payment processing, usage dashboards | Buy (Stripe Billing / Orb) |
| Cost attribution | Per-notebook, per-cell cost breakdown. "Saved compute" calculation from cache hits | Build |
| Quota enforcement | Spending limits, concurrent execution caps, storage quotas | Build |

> **Pricing model:** Free tier (local execution, limited cloud storage). Pro tier: compute at cost + 30–40% margin, per-GB storage, no per-seat fee. Team tier: same usage base + $15/editor/month for collaboration features. Enterprise: custom contracts with committed spend.

### 3.6 Developer Experience

APIs and tooling for programmatic access, CI/CD integration, and ecosystem connectivity.

| Component | Responsibility | Build vs Buy |
|-----------|---------------|-------------|
| REST / GraphQL API | Full notebook CRUD, execution triggers, artifact access. SDK wrappers for Python/JS | Build |
| CLI | `strata notebook run` from CI pipelines. Scheduled execution support | Build |
| Git integration | Sync notebooks to/from Git repos. Notebook format is already file-based | Build |
| Environment management | Managed Python environments per notebook. Pre-built images for common stacks (ML, data) | Build |
| Secrets management | Inject API keys, DB credentials into cell execution without exposing in source | Buy (Vault / AWS Secrets Manager) |

### 3.7 Observability and Operations

Platform health, user-facing execution monitoring, and internal ops tooling.

| Component | Responsibility | Build vs Buy |
|-----------|---------------|-------------|
| Execution logs | Cell stdout/stderr, execution traces. Already captured by harness | Build (log shipping) |
| Platform monitoring | Service health, worker utilization, queue depth, error rates | Buy (Datadog / Grafana Cloud) |
| Admin dashboard | Internal tool for workspace management, abuse detection, capacity planning | Build |
| Alerting | Worker pool exhaustion, high error rates, billing anomalies | Buy (PagerDuty / OpsGenie) |

### 3.8 Security and Compliance

Cell execution runs arbitrary user code — sandboxing is critical. Each cell execution must be isolated from other users and from the platform itself.

| Component | Responsibility | Build vs Buy |
|-----------|---------------|-------------|
| Execution sandbox | Container-level isolation per cell execution. See §8.1 for the full problem statement — this is not a table row | Build (Firecracker microVMs) |
| Data encryption | At-rest (S3 SSE) and in-transit (TLS). Customer-managed keys for enterprise | Buy (cloud KMS) |
| Audit logging | Who accessed what, when. Required for SOC 2 | Build |
| SOC 2 compliance | Type II audit. Vanta/Drata automate the paperwork, not the controls. See §8.2 for realistic scoping | Buy tooling + build controls |
| VPC deployment | Enterprise option: run in customer's cloud account | Build (later) |

## 4. Critical Dependencies and Risks

- **GPU availability:** Spot instance volatility and GPU shortages can impact service reliability. Mitigation: multi-cloud, multi-region, fallback to on-demand, or outsource to Modal/RunPod for V1.
- **Cold start latency:** Spinning up a worker for the first cell execution in a session. Mitigation: warm pool of generic workers, pre-pull common images. Content-addressed caching reduces how often cold starts matter.
- **SQLite at scale:** Each notebook uses SQLite for artifact metadata. Works well up to thousands of artifacts per notebook. If notebooks grow very large, may need to migrate to per-notebook Postgres schemas.
- **Real-time collaboration complexity:** CRDT-based multiplayer editing is a significant engineering investment. Defer to V2 and ship simpler share/comment model first.
- **Security surface:** Running arbitrary user code is inherently risky. Must invest heavily in sandboxing before any public launch. gVisor or Firecracker VMs are the minimum acceptable isolation boundary.

## 5. What Makes This Different

Every hosted notebook service (Hex, Deepnote, Noteable, Google Colab) charges for compute time regardless of whether the computation was necessary. Strata's artifact store means cache hits are instant and free. This is not a feature — it's a structural cost advantage that compounds as users iterate.

- **You only pay for unique computations.** Re-run a notebook after changing one cell? Only that cell and its dependents execute. Everything else is a cache hit with zero compute cost.
- **Provenance is the billing receipt.** Every charge maps to an artifact with a provenance hash. Users can see exactly what they paid for and why it wasn't a cache hit.
- **Saved compute is a retention metric.** Show users "this month we saved you 47 compute-hours and $312." This creates a switching cost that grows over time as the artifact store accumulates more cache hits.
- **Collaboration through artifacts, not files.** Sharing a notebook shares the entire artifact lineage. A teammate can pick up where you left off without re-executing anything.

## 6. Phased Rollout

"V1" as originally scoped (OAuth + workspaces + CPU + GPU + S3 + metering + Stripe + share links + gVisor + admin dashboard) is 12–18 months of work for a 3–5 person team. That is not an MVP, it is a full managed service launch. Before spending that, the usage-based pricing thesis has to be validated with real customers who actually pay, and the riskiest technical investment (sandboxing) has to be scoped against a real threat model rather than a bullet point.

The rollout is sequenced so each phase unlocks evidence the next phase needs.

### Phase 0 — Design partners (weeks, not months)

**Goal:** Prove that someone will pay for this. Nothing else matters yet.

- 5–10 hand-picked design partners — teams who already feel the pain of re-executing expensive notebooks
- GitHub OAuth only (no workspaces, no RBAC, no team features)
- One CPU worker class, no GPU
- **No public sandboxing.** Design partners run in dedicated single-tenant instances with documented risk. This is explicit, time-boxed, and off the record.
- S3 storage, no cross-region replication
- Compute seconds tracked in Postgres; billing is a monthly manual invoice
- No share links, no admin dashboard, no self-serve signup
- Goal metric: at least 3 partners sign a second month's invoice without complaint

This phase is not a product, it is a paid user research study. If design partners don't want to pay even when we hand-hold them, the thesis is wrong and no amount of engineering will fix it.

### Phase 1 — Private beta (2–3 months)

**Goal:** Close the loop from signup to payment without a human in the loop.

- Multi-tenant worker pool (Firecracker, see §8.1) — isolation becomes table stakes the moment >1 customer shares hardware
- Workspace model (single org per signup, invite-based)
- Stripe Billing, metered per compute-second and per GB-month
- Share links (viewer + editor, public + private)
- Basic admin dashboard for internal ops
- ~50 paying customers on an invitation waitlist
- Goal metric: cache hit rate across the fleet > 60% and > $0.50 revenue per compute-hour

### Phase 2 — Public GA

**Goal:** Open signup. Scale acquisition.

- Self-serve signup
- Free tier (local execution via OSS, read-only hosted notebooks, 5GB artifact storage)
- GPU workers via Modal/RunPod integration — only now, and only because paying customers asked
- CLI and programmatic API parity (leverages the `strata run` CLI already built)
- Publishing notebooks as read-only dashboards

### Phase 3 — Enterprise

**Goal:** Unlock the upper end of the market.

- SSO (SAML), audit logging, workspace-wide policies
- SOC 2 Type II (§8.2)
- Custom environment images
- VPC deployment option
- Dataset catalog for team-wide artifact reuse
- Real-time multiplayer editing (CRDT-based — this is V2, not V1)

### What stays in the OSS core at every phase

- Artifact store, provenance engine, DAG planner, cell executor
- Executor protocol (hosted workers are just another executor)
- Notebook format (directory + notebook.toml)
- `strata run` CLI
- All cell-level language support (python, prompt)

The hosted layer must never fork the core. If hosted needs a change, the change lands in OSS first.

## 7. Go-to-Market and Monetization

The technical plan describes what we build. This section describes who pays for it and how.

### 7.1 The monetization question

The core thesis — **cache hits are free, you only pay for unique compute** — describes a structural cost advantage. But "cost + 30–40% margin on compute" is the exact same pricing model every competitor uses. If customers execute 20% of the compute seconds Hex customers execute for the same workflow, they pay 20% of the revenue. The savings are 100% passed to the user; there is no pricing mechanism that captures the cache advantage for us.

This is a choice, and it should be made deliberately. Three options:

1. **Pure volume play.** Position as "Hex for 20% of the cost." Thin margins, win on growth. Strong for early adoption, weak for long-term defensibility.
2. **Charge for the substrate, not the compute.** Seats, storage, and premium features are the product; compute becomes a near-zero-margin commodity. This is the Snowflake playbook — storage is cheap, the execution engine and the features around it are what you pay for.
3. **Hybrid: free cache hits, priced cache-aware features.** Cross-notebook artifact reuse, team-wide dedup, saved-compute analytics, provenance audit trails. These only work because of the artifact substrate. Compute is priced normally but the *interesting* features are gated.

**Recommendation: (3).** Compute at cost-plus (because we still need to cover it) and cache-aware features gated behind paid tiers. This keeps the "you only pay for unique compute" story honest while giving us a price point that isn't fungible with AWS.

### 7.2 Pricing — committed

| Tier | Who | Price | What they get |
|---|---|---|---|
| **Free (OSS)** | Solo hobbyist / student | $0 | Full Strata OSS, local execution, CLI, `strata run` for CI. No hosted compute. |
| **Free (hosted)** | Evaluator | $0 | 5 GB artifact storage, 10 compute-hours/month on a CPU worker, public notebooks only |
| **Individual** | Solo professional | $25/month flat + usage | 100 compute-hours included, 50 GB artifact storage, private notebooks, share links. Overage metered. |
| **Team** | 2–20 person team | $15/editor/month + usage | Everything in Individual, plus workspace, team dedup, saved-compute dashboard, comments, audit log |
| **Enterprise** | >20 seats / compliance requirements | Contract | SSO, SOC 2, VPC deployment option, custom limits, dedicated support |

**Metering units:**
- Compute: **CPU-second** (aligned with Modal/RunPod). GPU billed per GPU-second by class.
- Storage: **GB-month** of artifact blob data, measured as the mean of daily snapshots
- Egress: free within the platform, billed at cloud rate for exports outside

**What we charge for that competitors can't:**
- **Team dedup.** If teammate A materialized an artifact, teammate B's cell hits the cache. Requires workspace-scoped provenance indexing. Gated to Team tier and above.
- **Saved-compute dashboard.** Real dollar value of cache hits over time. Gated.
- **Provenance audit trail.** Per-artifact "why did this run execute" lineage with timestamps and inputs. Gated.
- **Cross-notebook artifact browser.** Search your workspace by variable name, schema, value hash. Gated.

Compute-at-cost-plus is a floor, not a moat. The features above are the moat.

### 7.3 Target customer for Phase 0

Not "data scientists" in general. Specifically:

- **ML research teams running evaluation loops.** LLM evals, model comparison sweeps, hyperparameter search — workflows where you change one thing and re-run everything, burning compute on identical intermediate steps.
- **Data teams with expensive SQL/dataframe operations.** Joins over 100M rows, feature engineering pipelines, backtests. Pain is proportional to data size × iteration count.
- **Agent / evaluation framework builders.** LangChain-style workflow development where the same retrieval or preprocessing step runs hundreds of times during prompt iteration.

Common thread: **high cost-per-iteration × high iteration frequency.** The cache advantage is proportional to the product of these two, and these three segments maximize both.

### 7.4 Acquisition channels

In priority order:

1. **OSS → hosted funnel.** The OSS notebook is top-of-funnel. Every install is a potential hosted user. Instrument the "create a notebook" flow to show "run this in the cloud, first 10 hours free" after N local runs. This is the cheapest channel and the one that compounds.
2. **Technical content marketing.** Blog posts and tutorials showing *specific* workflows where the cache advantage is visible (LLM eval loops, ML training). Lead with the cache hit ratio, not the platform pitch.
3. **Direct design-partner outreach.** For Phase 0 only. Founders personally reach out to 50 teams in the target segments, get to 5–10 paying partners.
4. **Conference demos.** NeurIPS, SciPy, PyData — live demo showing "change one cell, re-run, watch the artifact hashes." Visual, memorable, differentiated.

Not doing: paid ads, outbound SDR motion, partner/reseller channel. None of those are worth the investment until Phase 2.

### 7.5 Anti-goals

Things the hosted service will *not* try to be:

- **A general-purpose Jupyter host.** We are a notebook platform built around content-addressed computation. If a user's workflow is "execute once and forget," we are not meaningfully better than Colab.
- **A BI tool.** Hex and Deepnote are drifting toward dashboard/app publishing because notebook revenue alone is hard. Avoid that trap — it is a different product with a different buyer.
- **A model training platform.** Modal, Replicate, and Runpod already do this. We integrate with them, we do not replace them.
- **Cheaper than Colab on raw compute.** Colab's free tier is subsidized by Google. Don't compete on sticker price; compete on the ratio of value to dollars (cache hits × unique compute).

## 8. Critical Engineering Investments

Three items from §§3–5 are underestimated as table rows. They need real scoping before the rollout plan is credible.

### 8.1 Execution sandboxing

**Problem statement:** Cell execution runs arbitrary user code. In a multi-tenant environment, a malicious or buggy cell must not be able to read another user's data, exhaust another user's quota, or escape into the host. This is the single hardest technical problem in the hosted service.

**Decision: Firecracker microVMs, not containers.** Container-level isolation (Docker, gVisor) has repeatedly had kernel escape CVEs. Firecracker is the same isolation technology AWS Lambda uses — a minimal VMM with a hardware-backed boundary. Every cell execution runs in a fresh microVM with:

- No persistent state between invocations
- Scoped egress network policy (S3, artifact service, explicit allowlist — no arbitrary outbound)
- CPU / memory / disk limits enforced by the VMM
- No access to host metadata endpoints
- Kernel image pinned and regularly patched

**Trade-offs:**
- **Cold start is worse than containers.** Firecracker boots in ~125ms but pulling a language runtime image is seconds. Mitigation: warm pool of pre-booted microVMs with the Python runtime loaded, sitting idle on `SIGSTOP` until needed.
- **Warm process pool (the OSS notebook's `WarmProcessPool`) does not survive across cells in microVMs.** Each cell is a fresh VM. The in-memory cache the OSS pool gives us is lost; we fall back purely to the artifact store. This is fine — the artifact store is the point — but it changes the hosted perf profile.
- **Complexity cost.** Firecracker is low-level. Expect a dedicated engineer on this for the full Phase 1, then ongoing maintenance.

**Explicit non-goal:** We will not attempt full VM-level isolation in Phase 0. Phase 0 runs design partners on dedicated single-tenant instances with a written risk acknowledgment. The moment Phase 1 mixes customers on shared hardware, Firecracker is a hard gate.

**Reference:** Amazon's [Firecracker paper (NSDI '20)](https://www.usenix.org/system/files/nsdi20-paper-agache.pdf) is the starting point.

### 8.2 SOC 2 Type II — realistic scoping

SOC 2 is not a tool you buy. Vanta and Drata automate evidence collection — they do not implement the underlying controls. Realistic scoping for a pre-enterprise startup:

**Pre-audit work (3–6 months):**
- Written policies (access control, incident response, change management, vendor management, etc.) — ~15 documents
- Access logging across every production system
- Key rotation automation
- Employee onboarding / offboarding workflows
- Vendor security reviews for every third-party service
- Incident response runbook + tabletop exercises

**Observation period (6 months minimum):**
- Controls must be *operating* for 6+ months before the auditor can issue Type II. That is 6 months of evidence of the controls actually being followed, not just documented.

**Audit (1–2 months):**
- External auditor engages, reviews evidence, issues report. ~$30k–$50k for a first-time audit.

**Total timeline from "we need SOC 2" to "we have a Type II report": 10–14 months.**

Start this no earlier than Phase 2. Attempting SOC 2 during Phase 0 or 1 burns cycles that should go to the product.

### 8.3 Storage egress and data residency

Artifact blob storage in S3 is cheap per-GB, but three costs the original plan ignores:

- **Inter-AZ traffic.** Workers in one AZ reading artifacts stored in another AZ within the same region. Currently ~$0.01/GB on AWS. For ML workloads producing 10–100 GB intermediate artifacts, this can dominate storage costs.
- **Egress on export.** When a customer downloads an artifact out of our platform, AWS charges us $0.05–0.09/GB. This needs to either be billed to the customer or capped by tier.
- **Data residency.** EU customers cannot have their data stored in US regions. Singapore/AU the same. Multi-region architecture is not V1 but needs acknowledgment. Earliest expected: Phase 3.

**Phase 1 posture:** single-region (us-east-1), worker AZ co-located with primary S3 bucket to eliminate inter-AZ cost, export egress absorbed up to a per-tier cap, explicit warning in the ToS that data is stored in the US.

### 8.4 On-call and operations

A multi-tenant service running user compute needs 24/7 response. Realistic minimum staffing: **2 engineers on rotating primary/secondary**, which sets the hosted service's minimum team size at 3 (one engineer has to be off-rotation for sustainability). This is a hard floor; attempting to run hosted with fewer people either burns people out or produces customer-visible outages.

## 9. Competitive Window

The core thesis is that a content-addressed artifact layer gives us a 20–80% compute savings advantage over Hex, Deepnote, Noteable, and Colab. Nobody else can offer this without rebuilding their execution engine around content-addressed artifacts — a 12–18 month rewrite at minimum, against a codebase and pricing model that assumes compute seconds are the unit of value.

**This is a timing play, not a technology play.** The technology advantage is real but copyable. The window is the time between our Phase 1 launch and Hex shipping "now with caching." Best estimate:

- **Phase 0 (now – 3 months):** We are alone in the market. Design partners pay full price for a hand-held experience.
- **Phase 1 (3–6 months):** We are alone in the market. Early paying customers feel like insiders.
- **Phase 2 (6–12 months):** Hex / Deepnote have noticed. They may ship a partial caching feature. We must be shipping team dedup and saved-compute analytics by this point or the differentiation collapses.
- **Phase 3 (12–24 months):** Competitors have shipped real caching. We must have: an OSS community, an enterprise story, and switching costs from workspace-level artifact accumulation.

**Implication for the rollout:** GTM urgency is higher than the technical plan suggests. Phase 0 design-partner recruitment is the most important thing we do in the first quarter, not a side-effect of engineering being done. Every month we spend building before the first paying partner is a month of the competitive window bled.
