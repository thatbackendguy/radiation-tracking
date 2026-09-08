# ADR-014: droplet prod compose override + reverse proxy

Date: 2026-07-08
Status: Accepted
Authors: M3 (Yash)

---

## Context

The Cloud pivot (approach.md, 2026-07-02) makes the deploy target a **single Dockerized stack on
one DigitalOcean droplet** — the same `docker compose` that runs locally. Deploy ownership was
reassigned from M5 to M3 (2026-07-06). Two problems block reusing the local compose as-is on a
public droplet:

1. **Public port exposure (review finding (h), risk row).** The base `docker-compose.yml` publishes
   kafka `29092` (PLAINTEXT, no auth), the Flink UI `8081` (accepts arbitrary job submission),
   producer metrics `8001`, backend `8000`, and frontend `5173`. **Docker publishes ports straight
   into iptables and bypasses ufw**, so a firewall alone would not close them — reusing the compose
   verbatim makes all five public.
2. **Cross-origin frontend.** The SPA builds `VITE_API_BASE_URL` to a `localhost:8000` fallback,
   which is unreachable from any non-local browser, and a direct browser→backend hop would need CORS.

`docker compose` **concatenates** the `ports:` lists across `-f` override files — an override cannot
*remove* a base mapping — so "publish only the proxy" cannot be expressed purely in the override.

## Decision

**1. `BIND_HOST` on every base published port.** Each base mapping becomes
`"${BIND_HOST:-0.0.0.0}:<port>"`. Local dev is unchanged (default `0.0.0.0` → reachable). The droplet
`.env` sets `BIND_HOST=127.0.0.1`, so all five ports bind loopback-only — reachable by host-side
tools on the droplet (the provisioner, `failure_demo.py`, an SSH tunnel) but never from the internet.
This is the single source of truth for port privacy; the override adds nothing to `ports:`.

**2. `docker-compose.prod.yml` adds a Caddy reverse proxy** as the only publicly published service
(ports 80 + 443). It serves the SPA and proxies the backend HTTP API + `/ws/stream` WebSocket on
**one origin**, so the browser is same-origin (no CORS hop) and the WebSocket upgrade is transparent.
Caddy is chosen over nginx because the deploy has a domain (`bd26-t2c.thatbackendguy.com`): `{$PUBLIC_URL}`
is Caddy's site address, so an `https://` domain **auto-provisions and renews a Let's Encrypt cert**
with no certbot/cron — a ~10-line Caddyfile vs nginx + manual certs. Issued certs persist in a
`caddy-data` volume so container recycles don't re-hit ACME rate limits. The override also bakes
`PUBLIC_URL` into the frontend build (fails fast if unset) and sets `restart: unless-stopped` on the
browser-facing services for the 24h soak. Setting `PUBLIC_URL=http://<ip>` degrades gracefully to
plain HTTP for a domain-less fallback.

Run on the droplet with:
`docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build`

## Consequences

- The droplet `.env` sets `PUBLIC_URL`, `BIND_HOST=127.0.0.1`, `CORS_ORIGINS=<PUBLIC_URL>`, and
  `CONFIG_WRITE_TOKEN` (ADR-013). CORS stays same-origin defence-in-depth even though the proxy makes
  requests same-origin.
- Only port 80 (and 443 when TLS is added) is public; everything else is loopback. M4's W7 security
  review can verify with `ss -tlnp` / an external port scan.
- Base-compose edit is limited to a host-IP prefix on existing mappings — no behaviour change locally.
- The frontend serves over `https://`, so the SPA's `/ws/stream` uses `wss://` automatically
  (`baseUrl.replace(/^http/, 'ws')` → `wss`); a plain-`ws` page would be blocked as mixed content.
- DNS must point `bd26-t2c.thatbackendguy.com` at the droplet before bring-up, or Caddy's ACME challenge
  fails. The domain is on Cloudflare — the record must be **DNS-only (grey cloud)** so Caddy can complete
  the challenge; a **Proxied (orange cloud)** record intercepts 80/443 and needs a Cloudflare Origin Cert
  + Full (strict) mode instead. `caddy-data` persists the cert across restarts to avoid rate limits.

## Alternatives considered

- **Standalone prod compose (no `-f` base).** Avoids the concatenation limitation but duplicates every
  service definition — two files drift out of sync. Rejected; the `BIND_HOST` prefix keeps one source
  of truth.
- **ufw only.** Does not work — Docker's published ports bypass ufw. Loopback-binding is the actual fix.
- **Direct browser→backend + CORS allowlist.** Two public services and a cross-origin hop; the
  same-origin proxy is simpler and shrinks the public surface to one container.
