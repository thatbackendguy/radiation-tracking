# Backend Security Review — Week 7 (droplet close)

Date: 2026-07-05
Scope: backend service + its compose/deploy surface, per the Week-7 M4 task
("security review — no secrets in images/compose, use a droplet `.env`") and
hard rule **H4** (never commit secrets).

## 1. Secrets audit — ✅ clean

- `git grep` sweep for `password|secret|token|api key|credential` across
  `backend/`, `docker-compose.yml`, `.gitlab-ci.yml`: only comments, CORS
  middleware flags, and `.dockerignore` patterns — **no secret values**.
- `docker-compose.yml`: no credentials. The Kafka `CLUSTER_ID` is a KRaft
  cluster identifier, not a secret. All tunables are `${VAR:-default}`
  passthroughs resolved from the droplet's `.env`.
- CI (`.gitlab-ci.yml`): DockerHub login uses `$DOCKER_HUB_USER` /
  `$DOCKER_HUB_PASSWORD` **GitLab CI variables** — injected at run time, never
  committed.
- `.gitignore` blocks `.env` / `.env.*` and allows only `.env.example`
  (placeholders + comments), per guidelines §8.2/§8.5.
- Root `.env.example` documents the optional cloud-Kafka SASL variables as
  blank placeholders only. Note: the backend itself does not read SASL settings
  — it connects to the in-stack plaintext broker; the SASL path belongs to the
  (superseded, optional) managed-Kafka wiring from ADR-010.

## 2. Image build — ✅ no secret can be baked in

- `backend/Dockerfile` copies exactly `requirements.txt` and `app/` — a local
  `.env` never enters the image (guidelines §8.4).
- `backend/.dockerignore` (added this week) additionally excludes `.env*`
  (except `.env.example`), venvs, caches and `tests/` from the build context —
  defense-in-depth if the Dockerfile ever gains a broader `COPY`.
- Runtime config is 100% env-driven (`pydantic-settings`); on the droplet all
  values come from an **uncommitted `/root/.../.env`** read by compose.

## 3. Network / endpoint surface

| Exposure | Assessment |
|---|---|
| `8000` backend (REST + WS) | Public on the droplet (fronted by M5's reverse proxy). Read-only data endpoints + `POST /config` (validated, rate naturally bounded by the single Flink consumer). No auth by design — the spec requires a public, auth-less URL. |
| `GET /metrics`, `GET /health` | Expose operational counters/status only — no payload data, no config values, no addresses beyond what the frontend already sees. Acceptable for the graded public demo. |
| `29092` Kafka external listener | **Local-dev convenience** (host-side CLI tools). On the droplet, do not publish it — M5's `docker-compose.prod.yml` override should drop the port mapping (in-stack services use `kafka:9092` on the compose network). |
| `8081` Flink UI, `8001` producer metrics, `5173` frontend | Read-only dashboards / static UI. Flink UI allows job cancel — keep it firewalled (droplet: allow 80/443 + SSH only) rather than published. |

**Droplet recommendation (for M5's deploy):** ufw/DO firewall allowing only
`22`, `80`/`443`; everything else stays on the internal compose network.

## 4. CORS / WebSocket origin policy

- One allowlist drives both the CORS middleware and the `/ws/stream` origin
  check: `CORS_ORIGINS` (comma-separated). Wildcard `*` is for local dev only.
- On the droplet, set in the uncommitted `.env`:
  `CORS_ORIGINS=http://<droplet-ip-or-domain>` — the origin the frontend is
  **served from** (scheme + host [+ port if non-standard]). With TLS later:
  `https://<domain>`.
- Wildcard + credentials is impossible by construction: `allow_credentials`
  is disabled whenever `*` is present (`backend/app/main.py`).
- Disallowed WS origins are closed with `1008` **before** the upgrade; a
  missing `Origin` header (curl, smoke tests) is allowed by design.

## 5. Degraded-mode behaviour (no fail-open surprises)

- Broker unreachable at startup: read paths degrade quietly (empty `/recent`),
  but `POST /config` fails **strict 503 without committing** — the backend and
  Flink can never silently disagree on active thresholds.
- `/health` stays HTTP 200 while serving and reports the degradation in the
  body — deliberate, so the compose `service_healthy` gate doesn't
  restart-loop the stack during a broker blip.

## 6. Follow-ups

- [ ] M5 (`docker-compose.prod.yml`): drop the `29092` port mapping and
      firewall the droplet to 22/80/443.
- [ ] Set restrictive `CORS_ORIGINS` in the droplet `.env` during bring-up and
      re-run the WS origin smoke test from the public URL.
