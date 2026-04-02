# Service Mode Deployment

This guide explains what `service` mode is for, what is actually shared, and how to run Strata in a way that matches the current implementation.

## What Service Mode Is

`service` mode is the **shared backend deployment** mode for Strata.

Use it when:

- one Strata server should serve multiple users or services
- worker definitions should be managed centrally
- access should be controlled by identity, tenant, and scopes
- you want shared executors, shared cache infrastructure, and shared observability

Do **not** think of service mode as a collaborative live notebook session model. That is not what the current product does.

What is shared:

- one Strata server process
- one worker registry
- one build / artifact transport layer
- one cache / artifact infrastructure
- one set of admin APIs and metrics
- one pool of backend compute capacity

What is **not** shared:

- a live co-edited notebook session
- unrestricted session discovery/reconnect across users

Notebook session discovery APIs are intentionally limited to personal mode. In service mode, notebooks are meant to be reopened from their known path, not rediscovered from in-memory server session state.

## Personal vs Service

Use `personal` mode when you are:

- developing locally
- manually testing notebooks
- using local write/artifact flows directly
- avoiding proxy/auth setup

Use `service` mode when you are:

- deploying Strata behind a real gateway or reverse proxy
- centralizing notebook worker administration
- using trusted-proxy auth and scopes
- isolating users or teams with tenant-aware policy

Important implementation detail:

- `deployment_mode` defaults to `service`
- generic write endpoints are only enabled in `personal`
- signed build transport is available in service mode only when server transforms are enabled

That means a useful notebook service-mode deployment usually needs both:

```bash
STRATA_DEPLOYMENT_MODE=service
STRATA_TRANSFORMS_ENABLED=true
```

## Recommended Topology

The intended production shape is:

```text
Browser / Client
    |
    v
Reverse Proxy / API Gateway
    - authenticates caller
    - injects trusted identity headers
    |
    v
Strata (service mode)
    - enforces scopes / ACLs / tenant context
    - manages notebook worker registry
    - manages build + artifact metadata
    |
    +--> local artifact store or blob store
    |
    +--> notebook executor workers / transform executors
```

Typical shared components:

- proxy: nginx, Envoy, Kong, or an internal gateway
- Strata service: one or more app replicas
- executor services: notebook remote executors and/or transform executors
- artifact storage: local disk for dev, S3/GCS/Azure for real deployments

## Auth Model

Strata does not authenticate end users itself in service mode. It trusts a reverse proxy that injects identity headers after authenticating the caller.

Relevant config:

```bash
STRATA_AUTH_MODE=trusted_proxy
STRATA_PROXY_TOKEN=replace-me
```

Relevant headers:

- `X-Strata-Principal`: stable user or service ID
- `X-Strata-Tenant`: team or org ID
- `X-Strata-Scopes`: space-separated scopes
- `X-Strata-Proxy-Token`: shared secret proving the request came from the trusted proxy

The server-side auth path lives in [server.py](/Users/fangchenli/Workspace/strata/src/strata/server.py).

## What Multiple Users Share

When Alice and Bob both talk to the same Strata service:

- they share the same deployed server
- they share the same executor fleet
- they share the same worker registry
- they share the same admin surfaces and metrics
- they may share the same physical cache and artifact storage backend

But they are separated by:

- principal identity
- tenant context
- scopes
- ACL rules
- tenant-scoped resource ownership
- per-tenant QoS accounting

In other words, “shared deployment” means shared infrastructure with logical isolation, not a shared notebook memory space.

## Notebook Behavior In Service Mode

Notebook-specific behavior is different from personal mode:

- notebook-scoped worker definition editing is disabled
- workers come from a server-managed registry
- authorized operators manage that registry through `/v1/admin/notebook-workers*`
- session list/get APIs are blocked
- signed notebook worker transport requires server transforms to be enabled

Practical implications for notebook users:

- they can choose from the allowed server-managed workers
- they cannot define arbitrary executor URLs in notebook content
- refresh/reopen should come from the notebook path or the UI’s last-known notebook entry, not from a globally shared session list

## Minimal Service Mode Configuration

This is the smallest useful service-mode config for notebook remote workers:

```bash
export STRATA_DEPLOYMENT_MODE=service
export STRATA_TRANSFORMS_ENABLED=true

export STRATA_AUTH_MODE=trusted_proxy
export STRATA_PROXY_TOKEN=replace-me

export STRATA_HOST=0.0.0.0
export STRATA_PORT=8765

# Required in practice for build store / artifact state
export STRATA_ARTIFACT_DIR=/var/lib/strata/artifacts

# Optional but usually recommended for multi-user setups
export STRATA_MULTI_TENANT_ENABLED=true
export STRATA_REQUIRE_TENANT_HEADER=true
```

Then start Strata:

```bash
uv run strata-server
```

Notes:

- `STRATA_DEPLOYMENT_MODE=service` is already the default, but set it explicitly in deployment manifests
- `STRATA_TRANSFORMS_ENABLED=true` matters for signed build transport and server-managed transform flows
- unlike personal mode, service mode does **not** auto-create a default `artifact_dir`; set one explicitly

## Recommended First Real Deployment

For a first shared deployment, keep it simple:

1. one Strata service
2. one reverse proxy that injects trusted-proxy headers
3. one local or cloud artifact store
4. one reference executor or notebook executor service
5. one admin operator identity and one normal notebook user identity

That is enough to validate:

- auth headers
- tenant propagation
- worker admin CRUD
- notebook worker allow/deny policy
- direct or signed remote execution

## Example Roles

### Operator

An operator is an admin user or service that can manage the notebook worker registry.

Typical scope:

- `admin:notebook-workers`

Possible responsibilities:

- add executor workers
- disable unhealthy workers
- refresh worker health
- inspect worker health history

### Notebook User

A normal notebook user does not manage the worker registry.

They should be able to:

- open a notebook
- select from allowed workers
- execute cells
- see when a worker is blocked, unavailable, or disabled by policy

They should not be able to:

- create arbitrary executor workers in service mode
- enumerate all live notebook sessions

## Multi-Tenant Mode

Multi-tenancy is optional, but it is the normal next step for real shared deployments.

Enable it with:

```bash
export STRATA_MULTI_TENANT_ENABLED=true
export STRATA_REQUIRE_TENANT_HEADER=true
```

What this adds:

- request-scoped tenant context
- tenant-aware metrics
- tenant-aware cache and artifact isolation
- tenant-aware build ownership checks
- per-tenant QoS accounting

If you do not enable multi-tenancy, you can still run service mode with trusted proxy auth. That is a valid single-tenant shared backend.

## Reverse Proxy Checklist

Your proxy should:

1. authenticate the caller
2. strip any client-supplied `X-Strata-*` identity headers
3. inject trusted values for:
   - `X-Strata-Principal`
   - `X-Strata-Tenant`
   - `X-Strata-Scopes`
   - `X-Strata-Proxy-Token`
4. ensure only the proxy can reach Strata directly

If clients can bypass the proxy and reach Strata directly, trusted-proxy mode is not safe.

## What Service Mode Is Good For Today

Today, service mode is a good fit for:

- centrally managed notebook workers
- remote executor fleets
- shared backend infrastructure for many users
- trusted-proxy auth and scopes
- tenant-aware resource isolation

It is **not** currently the right mental model for:

- collaborative notebook editing
- globally shared in-memory notebook sessions

## Local Testing Recommendation

For local product development and notebook debugging, prefer `personal` mode.

Use `service` mode locally when you specifically want to test:

- server-managed worker administration
- trusted-proxy auth behavior
- tenant and scope enforcement
- service-mode remote notebook execution policy

The next companion step after this document is a local service-mode stack with:

- proxy
- Strata in service mode
- reference executor
- one admin path and one normal-user path

## Local Demo Stack

The repo now includes a local service-mode demo stack:

```bash
docker compose -f docker-compose.service.yml up -d
```

Entry points:

- normal notebook user: `http://localhost:8865`
- admin notebook user: `http://localhost:8866`

What it includes:

- Strata in service mode with trusted-proxy auth enabled
- a local notebook executor service
- an nginx proxy that injects a fixed demo user identity on port `8865`
- an nginx proxy that injects a fixed admin identity on port `8866`

The service-mode config is checked into:

- [pyproject.toml](/Users/fangchenli/Workspace/strata/.docker/service-mode/pyproject.toml)

The proxy config is checked into:

- [nginx.conf](/Users/fangchenli/Workspace/strata/.docker/service-mode/nginx.conf)

This stack is meant for local service-mode exploration, not production. The proxy injects fixed identities and shared secrets for convenience.

Smoke test it with:

```bash
uv run python scripts/service_mode_smoke.py
```

The smoke script verifies:

1. admin access to the server-managed notebook worker registry
2. non-admin rejection for that registry
3. service-mode session discovery remains blocked
4. notebook creation and worker assignment through the user path
5. successful WebSocket execution on an allowed worker
6. forced rerun rejection after the admin disables that worker
