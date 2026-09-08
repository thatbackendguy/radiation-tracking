# DigitalOcean droplet deploy (M3)

Status: Scaffolded (Week 7). Runs the same `docker compose` stack as local, fronted by a **Caddy**
reverse proxy that terminates HTTPS (automatic Let's Encrypt), with every internal port bound to
loopback. See [ADR-014](../decisions/ADR-014-droplet-prod-compose-reverse-proxy.md).

Target: `https://bd26-t2c.thatbackendguy.com` on a **4 vCPU / 8 GB / 160 GB amd64** droplet.

The canonical command (on the droplet):

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

## Files

- `docker-compose.prod.yml` — droplet override: adds the `reverse-proxy` (Caddy — the only public
  service, ports 80+443), bakes `PUBLIC_URL` into the frontend build, `restart: unless-stopped`,
  persistent `caddy-data` volume for issued certs.
- `deploy/caddy/Caddyfile` — serves the SPA and proxies the backend API + `/ws/stream` on one origin;
  `{$PUBLIC_URL}` is the site address, so a `https://` domain auto-provisions TLS.
- `.env` (gitignored, created on the droplet from `.env.example`) — the droplet settings below.

## 1. DNS on Cloudflare (do this first — propagation is quick)

`thatbackendguy.com` is on Cloudflare. Add an **A record** in the zone and set it to **DNS only
(grey cloud), NOT Proxied (orange cloud)**:

| Type | Name | Content | Proxy status |
|------|------|---------|--------------|
| A    | `bd26-t2c` | `<droplet public IPv4>` | **DNS only** (grey cloud) |

**Why grey cloud:** Caddy provisions its own Let's Encrypt cert directly on the droplet (HTTP-01 /
TLS-ALPN challenge on ports 80/443). If the record is **Proxied**, Cloudflare terminates TLS at its
edge and intercepts 80/443, so Caddy's ACME challenge can't complete. DNS-only makes the scaffold
work with zero extra config.

Verify before bringing the stack up (Caddy's cert issuance needs the name to resolve to the droplet):

```bash
dig +short bd26-t2c.thatbackendguy.com   # must return the droplet IP (grey cloud → the droplet's own IP)
```

> **Want Cloudflare's proxy/CDN (orange cloud) instead?** Then don't use Caddy's Let's Encrypt:
> generate a **Cloudflare Origin Certificate**, mount it into Caddy (`tls /path/cert /path/key` in
> the Caddyfile), and set the zone SSL/TLS mode to **Full (strict)**. More setup and another failure
> surface (WebSockets still work through the proxy) — grey cloud is recommended for the deadline.

## 2. Droplet prep (one-time)

```bash
# Add swap — DO droplets have none by default; guards against OOM on the 8 GB box.
sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# Docker Engine + compose plugin (see docs.docker.com/engine/install/ubuntu).
# Firewall: SSH + HTTP + HTTPS only. NOTE: ufw does NOT cover Docker-published ports —
# port privacy is enforced by BIND_HOST=127.0.0.1 (below), not ufw. 80 is needed for the
# ACME HTTP challenge + the 80→443 redirect.
sudo ufw allow OpenSSH && sudo ufw allow 80/tcp && sudo ufw allow 443/tcp && sudo ufw enable
```

## 3. Clone + configure `.env`

```bash
git clone <repo-url> && cd bd26_project_t2_c
cp .env.example .env
```

Edit `.env` for the droplet:

```dotenv
PUBLIC_URL=https://bd26-t2c.thatbackendguy.com   # site address + frontend build + CORS origin
BIND_HOST=127.0.0.1                          # bind all base ports to loopback → only Caddy is public
CORS_ORIGINS=https://bd26-t2c.thatbackendguy.com # same as PUBLIC_URL (backend CORS + /ws/stream check)
CONFIG_WRITE_TOKEN=<random-secret>           # gate POST /config (ADR-013); `openssl rand -hex 24`

# Durable Flink checkpointing on the droplet (ADR-015):
FLINK_CHECKPOINT_DIR=file:///opt/flink/state/checkpoints
FLINK_STATE_BACKEND=rocksdb

# Small CSV slice for the soak (full 32 GB stays a local-validation run):
CSV_FILE=./data/sample.csv
PRODUCER_SPEED=10x
```

## 4. Bring the stack up

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

`PUBLIC_URL` is baked into the frontend at build time, so **rebuild the frontend if it changes**
(`--build`). Caddy fetches the TLS cert on first start (needs DNS live + ports 80/443 open); watch it:

```bash
docker compose logs -f reverse-proxy   # look for "certificate obtained successfully"
```

Then create/verify the Kafka topics (M3 provisioner):

```bash
python scripts/provision_topics.py --verify-retention   # localhost:29092 works — loopback-bound
```

## 5. Smoke test (from your laptop, not the droplet)

```bash
curl -sf https://bd26-t2c.thatbackendguy.com/health    # backend via proxy → 200, valid cert
# open https://bd26-t2c.thatbackendguy.com/ in a browser → map loads, live markers,
# devtools Network shows the WebSocket connected over wss://
```

The frontend turns the `https://` origin into `wss://` automatically for `/ws/stream` (an HTTPS page
requires `wss` — plain `ws` would be blocked as mixed content).

## 6. Verify port privacy (the finding-(h) check)

Only 80 + 443 may answer publicly; 29092 / 8081 / 8001 / 8000 / 5173 must be closed:

```bash
# On the droplet: every base port bound to 127.0.0.1, only Caddy on 0.0.0.0.
ss -tlnp | grep -E '29092|8081|8001|8000|5173|:80 |:443 '
# From outside: these must all fail/refuse.
for p in 29092 8081 8001 8000 5173; do nc -z -w3 bd26-t2c.thatbackendguy.com $p && echo "OPEN $p (BAD)"; done
```

## 7. Failure-mode demo + soak

```bash
python scripts/failure_demo.py   # drop the TaskManager, watch recovery via the JM REST
```

Snapshot the droplet before the 24h soak so it can be rebuilt if it dies; scale down / power off
off-hours to preserve the $200 credit. The `caddy-data` volume keeps the issued cert across restarts
so you don't re-hit Let's Encrypt rate limits.

## Plain-IP fallback (no domain)

If you ever deploy without the domain, set `PUBLIC_URL=http://<droplet-ip>` — Caddy then serves plain
HTTP (a bare IP can't get a public cert) and the page uses `ws://`. Drop the `443` ufw rule.
```
