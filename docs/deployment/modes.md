# Deployment Modes

Strata runs in one of two deployment modes. The mode decides whether the
server accepts writes, who it trusts, and how strict it is about network
binding.

## Summary

| | Personal | Service |
|---|---|---|
| **Audience** | Single developer on a laptop | Multi-user hosted deployment |
| **Writes** | Enabled (`writes_enabled=True`) | Disabled at the surface; server transforms only |
| **Auth** | None (`auth_mode="none"`) | Trusted proxy injects identity headers |
| **Multi-tenancy** | Disabled | Optional (`multi_tenant_enabled=True`) |
| **Default artifact dir** | `~/.strata/artifacts` | Must be set explicitly (or use S3/GCS/Azure) |
| **Network binding** | Loopback only by default | Unrestricted |

## Choosing a mode

- **Personal** — running the notebook on your own machine. Fast to start,
  nothing to configure, writes land in your home directory. This is the
  default for Docker Compose and the "from source" instructions.
- **Service** — hosting Strata for a team, behind an ingress proxy that
  authenticates users. Writes must go through server-side transforms so
  the platform controls what gets materialized and by whom.

## Setting the mode

```bash
export STRATA_DEPLOYMENT_MODE=personal   # or "service"
```

Or in `pyproject.toml`:

```toml
[tool.strata]
deployment_mode = "personal"
```

Default is `service`.

## Personal mode

```bash
STRATA_DEPLOYMENT_MODE=personal uv run strata-server
```

The server binds to `127.0.0.1` by default and refuses non-loopback
addresses unless you opt in:

```bash
STRATA_DEPLOYMENT_MODE=personal \
  STRATA_HOST=0.0.0.0 \
  STRATA_ALLOW_REMOTE_CLIENTS_IN_PERSONAL=true \
  uv run strata-server
```

Opt in only if you have separate protection (firewall, VPN, private
network) — personal mode exposes write endpoints with no authentication.

Artifacts persist to `~/.strata/artifacts` unless `STRATA_ARTIFACT_DIR`
is set. Notebook deletion and session discovery/reconnect APIs are
personal-mode-only.

## Service mode

```bash
STRATA_DEPLOYMENT_MODE=service \
  STRATA_AUTH_MODE=trusted_proxy \
  STRATA_PROXY_TOKEN=<shared-secret> \
  uv run strata-server
```

Service mode expects an upstream proxy (NGINX, Envoy, Cloud Run ingress)
that:

1. Authenticates the caller (JWT, OIDC, mTLS — whatever you use).
2. Injects identity headers: `X-Strata-Principal`, `X-Strata-Tenant`,
   `X-Strata-Scopes`.
3. Forwards `X-Strata-Proxy-Token` with the value Strata is configured
   to expect.

The proxy must be the *only* ingress path — run Strata on a private
network / security group / NetworkPolicy. See
[Trusted Proxy Authorization](../reference/configuration.md) for the
full protocol.

For multi-tenant hosting, add:

```bash
STRATA_MULTI_TENANT_ENABLED=true
STRATA_REQUIRE_TENANT_HEADER=true
```

Per-tenant QoS isolation, cache keying, and metrics kick in
automatically.

## Coherence enforcement

Strata rejects incoherent mode combinations at startup. These combos
raise `ValueError` during config load:

| Combination | Why it's rejected |
|---|---|
| `deployment_mode=personal` + `auth_mode=trusted_proxy` | Personal mode has no upstream proxy; identity headers would come from the loopback client |
| `deployment_mode=personal` + `multi_tenant_enabled=True` | Personal mode is single-user; there are no tenants to isolate |
| `deployment_mode=personal` + `require_tenant_header=True` | Same reason — no tenant dimension in personal mode |

If you see one of these errors, you almost certainly pulled flags from a
service-mode config into a personal-mode deployment. Remove the
service-specific flags or switch to `deployment_mode=service`.

## Mode-independent settings

These apply identically in either mode and can be tuned freely:

- `rate_limit_*` — token-bucket rate limiting
- `acl_config` — deny/allow rules (only effective when `auth_mode` is set)
- `artifact_blob_backend` — local / s3 / gcs / azure
- Tracing, logging, S3 / GCS / Azure credentials
- Cache size, cache directory, metadata DB path
