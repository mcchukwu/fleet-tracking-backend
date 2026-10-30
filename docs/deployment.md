# Deployment

Two Compose projects share one codebase:

| Project | Profiles | Data |
|---|---|---|
| `ftb-demo` | `app`, `demo` | Demo Fleet, simulator, demo logins |
| `ftb-pilot` | `app` | Real tenant, no seed, no simulator |

Do not run demo and pilot against the same database.

## Host

One VM, 2 vCPU and 4 GB is enough for the demo and a small pilot. Ubuntu 24.04 or Debian 12.

```bash
# as root, once
adduser deploy
usermod -aG sudo deploy
rsync --archive --chown=deploy:deploy ~/.ssh /home/deploy/

# as deploy
sudo apt update && sudo apt upgrade -y
sudo apt install -y ca-certificates curl ufw
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw --force enable

curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker deploy
newgrp docker
```

Postgres and Redis are bound to loopback in `compose.yaml`. Only Caddy publishes ports.

## Code and secrets

```bash
git clone git@github.com:mcchukwu/fleet-tracking-backend.git /srv/ftb
cd /srv/ftb
git checkout api-v0.2.0
cp .env.example .env
```

Edit `.env`:

```dotenv
APP_ENV=production
DB_NAME=ftdb
DB_USER=ftb
DB_PASSWORD=<generated>
DB_HOST=localhost
DB_PORT=5432
DB_SSLMODE=disable
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=<generated>
PG_DSN=postgres://ftb:<generated>@localhost:5432/ftdb?sslmode=disable
LISTEN_ADDR=:8080
ADMIN_ADDR=:9090
API_ADDR=:8081
SESSION_TTL=12h
COOKIE_SECURE=true
DOMAIN=fleet.example.com
RETENTION_DAYS=60
```

The API host is `DOMAIN`. Caddy obtains a certificate automatically. For a hostname without a domain, use `<name>.<server-ip>.sslip.io`; it resolves to the IP.

## Demo deployment

```bash
cd /srv/ftb
docker compose -p ftb-demo --profile app --profile demo up -d --build
```

This runs migrations, seeds Demo Fleet, starts ingest, the API, the simulator and Caddy.

Verify:

```bash
curl -s https://fleet.example.com/readyz
curl -s -X POST https://fleet.example.com/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"dispatcher@demo.ftb","password":"DemoPass123!"}'
```

The demo password is public by design. Treat the demo environment as public.

## Pilot deployment

```bash
cd /srv/ftb
docker compose -p ftb-pilot --profile app up -d --build
```

The database is empty. Create the tenant, admin and device key:

```bash
docker compose -p ftb-pilot --profile app run --rm --entrypoint /app/bootstrap api \
  -tenant "Operator Name" \
  -email "admin@operator.example" \
  -password '<strong-password>' \
  -name "Operations Admin"
```

The key is printed once. Store it before closing the terminal.

Then create the first geofences, import the vehicle CSV, and point the device adapter at:

```
wss://<DOMAIN>/ws/ingest
```

Device handshake headers are in [frontend-integration.md](frontend-integration.md) for clients and in the README for devices.

## Frontend

Serve the SPA from the same Caddy origin. Replace `deploy/site` with the built frontend, or mount the build output at `/srv`:

```yaml
# compose.yaml, caddy service
- ./ftw/dist:/srv:ro
```

The API is then at `https://<DOMAIN>/api/v1`, the WebSocket at `wss://<DOMAIN>/api/v1/live/ws`, and there is no CORS to configure.

## Domain change

1. Add or change the DNS `A` record to the VM IP.
2. Set `DOMAIN` in `.env`.
3. `docker compose -p ftb-demo up -d --force-recreate caddy api`
4. Users log in again because the cookie host changed.

No code change.

## Backups

```bash
./scripts/backup.sh
```

Writes `backups/<db>-<timestamp>.dump` and deletes files older than `BACKUP_RETENTION_DAYS` (14 by default). For a remote copy:

```bash
rclone copy /srv/ftb/backups remote:ftb-backups
```

Daily cron:

```cron
0 2 * * * cd /srv/ftb && COMPOSE_PROJECT=ftb-demo ./scripts/backup.sh >> /var/log/ftb-backup.log 2>&1
```

Restore drill, into a scratch database:

```bash
./scripts/restore.sh backups/ftdb-20261030T012448Z.dump ftdb_restore_test
```

It prints vehicle and position counts. Drop the scratch database afterwards. Do this once before the pilot, not for the first time during an incident.

## Updates

```bash
cd /srv/ftb
git fetch --tags
git checkout api-v0.2.0
./scripts/backup.sh
docker compose -p ftb-demo --profile app --profile demo up -d --build
```

Migrations run on start. A failed migration leaves the migrate container exited; check `docker compose logs migrate`.

## Operations

- `docker compose -p ftb-demo ps -a`
- `docker compose -p ftb-demo logs -f api ingest caddy`
- `docker compose -p ftb-demo exec -T ingest wget -qO- http://127.0.0.1:9090/readyz`
- Uptime check: `https://<DOMAIN>/readyz` returns 200 only when Postgres and Redis are reachable.
- Admin listener on 9090 is not published. Keep it that way.

## Troubleshooting

**`network ... not found`.** A container from a profile survived `down`. Remove all profiles:

```bash
docker compose --profile "*" down --remove-orphans
docker compose -p ftb-demo --profile app --profile demo up -d --build
```

**`address already in use` on 8080 or 8081.** A host `go run` process is holding the port. In the current Compose file neither port is published; Caddy owns 80 and 443.

**Empty API.** The `app` profile does not seed. Use the `demo` profile or run `go run ./cmd/seed` for the demo tenant, or `cmd/bootstrap` for a real one.

**Certificate failure.** Confirm port 80 and 443 reach the VM, `DOMAIN` resolves to the VM, and Caddy can write `/data`. For local testing with `DOMAIN=localhost`, Caddy uses an internal certificate; `curl -k` is expected.

**Registry timeout during build.** Docker tried an IPv6 route. `docker pull golang:1.26-alpine` then retry `--build`.
