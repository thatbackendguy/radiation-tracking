# ADR-013: authenticated config writes + secure-by-default CORS

Date: 2026-07-06
Status: Accepted
Authors: M3 (Yash)

---

## Context

The 2026-07-03 `main` review (approach.md, finding (e)) flagged a security hole for the public
DigitalOcean droplet:

> M4 — `POST /config` is unauthenticated with `CORS_ORIGINS=*` default — on the public droplet
> anyone can rewrite the global thresholds/filters.

`POST /config` is not a cosmetic endpoint: it rewrites the active CPM thresholds and area/timespan
filters that are published to `config.updates` and broadcast into the Flink pipeline, so an
anonymous caller could silently re-tune everyone's view and the alerting classifier. Two gaps
compounded it:

1. `cors_origins` defaulted to `*`, so a bare backend accepted any browser origin (and the
   `/ws/stream` origin check, which reuses the same allowlist, allowed any origin too).
2. There was no write authentication of any kind on the mutating endpoint.

[MR !45](https://collaborating.tuhh.de/e-19/teaching/bd26_project_t2_c/-/merge_requests/45) already
threaded `CORS_ORIGINS` through compose so the droplet `.env` *can* restrict origins, but the
default was still open and writes were still ungated.

## Decision

**1. Secure-by-default CORS.** `Settings.cors_origins` now defaults to `""` (deny all cross-origin)
instead of `*`. A bare backend allows no browser origin until `CORS_ORIGINS` is set. Local dev is
unaffected: `docker-compose.yml` sets `CORS_ORIGINS=${CORS_ORIGINS:-*}`, so the compose stack still
gets `*`; the droplet `.env` sets the real public frontend URL.

**2. Optional shared-token gate on writes.** A new `CONFIG_WRITE_TOKEN` setting gates `POST /config`
via a `require_write_auth` FastAPI dependency:

- **Unset (default)** — the gate is a no-op, so local dev and the existing test-suite behaviour are
  unchanged.
- **Set (droplet `.env`)** — every `POST /config` must carry a matching `X-Config-Token` header.
  Missing header → `401`; wrong token → `403`. The comparison uses `secrets.compare_digest`
  (constant-time) so the token can't be recovered by response-timing.

Reads (`GET /config`, `GET /recent`, `/ws/stream`) are unaffected — only the mutating write is gated.

This is application-level defence-in-depth that holds even if the reverse proxy is misconfigured;
it complements (does not replace) the planned proxy-level origin restriction and the port-binding
hardening in `docker-compose.prod.yml` (finding (h)).

## Consequences

- The droplet `.env` must set both `CORS_ORIGINS` (public frontend URL) and `CONFIG_WRITE_TOKEN`
  (a random secret, shared with whatever issues config writes). The token is a secret — it lives in
  the gitignored droplet `.env`, never in the image or compose (H4).
- No client change for local dev. A production writer must send the `X-Config-Token` header.
- The frontend's config form, when pointed at a token-protected backend, will need to send the
  header (out of scope here — tracked for M5 if/when the droplet enables the token).

## Alternatives considered

- **Proxy-only gating.** Relies entirely on the reverse proxy; a single misconfig re-opens the hole
  and leaves no in-app defence. Kept as a complementary layer, not the only one.
- **Full user auth (OAuth/JWT).** Over-scoped for a single-operator demo deploy on a hard deadline;
  a shared token closes the actual finding with far less surface.
