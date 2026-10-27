# Fleet tracking backend

A WebSocket ingest service for GPS devices. It keeps the current position of every vehicle in Redis, writes the full position history to PostgreSQL/PostGIS, and emits geofence entry and exit events.

One process handles many tenants. The tenant and vehicle are established when the device connects and travel with every ping.

## Run it

Needs Go 1.26+ and Docker.

```bash
cp .env.example .env
make dev        # postgres, redis, migrations, dev seed
make run
```

`make migrate` is idempotent. `make seed` loads the dev tenant, one API key and one geofence. The seed is not part of the migration chain; a production database gets none of it until you create them.

Smoke test in another terminal:

```bash
go run ./cmd/routetest -vehicle "route-test-$(date +%s)"
```

The database schema changed in the 000003 migration. If you ran an older checkout, drop the dev volume and start again.

## Device protocol

`ws://localhost:8080/ws/ingest`

| Header | Value |
|---|---|
| `Authorization` | `Bearer <tenant API key>` |
| `X-Device-ID` | IMEI or simulator id, registered for that tenant |

Each frame is JSON:

```json
{"lat": 6.53, "lon": 3.42, "speed_kph": 38.5, "heading_deg": 180, "recorded_at": "2026-10-09T12:00:00Z"}
```

There is no vehicle id in the body. A ping belongs to the identity authenticated at the handshake. A connection that sends nothing for `IDLE_TIMEOUT` is closed.

## Endpoints

Public listener (`LISTEN_ADDR`, `:8080`): `/ws/ingest` only.

Admin listener (`ADMIN_ADDR`, `127.0.0.1:9090` by default):

- `GET /healthz` process is up
- `GET /readyz` Redis and PostgreSQL reachable
- `GET /metrics/ingest` counters and queue depth
- `GET /metrics/alerts` alert latency percentiles
- `GET /debug/alerts` recent alerts
- `GET /debug/vehicle?tenant=<uuid>&id=<external id>` raw Redis state
- `GET /ws/live` live position and alert feed

The admin listener has no authentication. Keep it on loopback or a private network. Binding it to a public address is an explicit choice, not a supported default.

## Configuration

| Variable | Required | Default |
|---|---|---|
| `PG_DSN` | yes | |
| `REDIS_ADDR` | yes | |
| `REDIS_PASSWORD` | no | empty |
| `LISTEN_ADDR` | yes | |
| `ADMIN_ADDR` | yes | |
| `OVERFLOW_POLICY` | yes | `block`, `drop_oldest`, `drop_newest` |
| `IDLE_TIMEOUT` | no | `90s` |
| `ACCEPT_RATE` / `ACCEPT_BURST` | no | `500` / `200` per process |
| `PER_IP_RATE` / `PER_IP_BURST` | no | `10` / `20` |
| `COLDPATH_BATCH_WINDOW` | no | `1s` |
| `COLDPATH_BATCH_SIZE` | no | `500`, max 5000 |

`compose.yaml` reads `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_PORT` and `REDIS_PASSWORD` for the local containers.

## How it works

The handshake resolves the API key to a tenant and the device id to a vehicle row, then returns the internal vehicle UUID. `pipeline.PositionPing` carries tenant, external id and UUID from that point on.

Workers take pings from one bounded channel. A Redis Lua script compares `recorded_at` with the stored value and swaps the set of geofences the vehicle is inside in the same atomic step. A ping older than the current state is counted and skipped for live alerts, but still written to history. Redis keys are scoped to the tenant: `tenant:<tenant>:vehicle:<external id>:state`.

Geofences are loaded in memory at startup and checked without a database round trip. They are not reloaded until restart.

The cold path batches positions and events into Postgres. A failed batch is retried row by row, so one bad row costs one row, and the failure counters are exposed in `/metrics/ingest`.

On `SIGTERM` or `SIGINT` the process closes device connections, stops the pipeline, flushes the cold path, then closes the database.

## Tests

```bash
go test ./...
TEST_REDIS_ADDR=localhost:6379 TEST_REDIS_PASSWORD=devredis go test ./internal/pipeline -run TestApplyPosition
./scripts/integration.sh
```

`scripts/integration.sh` starts the containers, migrates, seeds, runs a route through the service and checks that the positions and geofence events landed in Postgres. Set `SKIP_COMPOSE=1` when the services already exist. CI runs it against PostGIS and Redis services.

## Limits

These are current, not future work:

- One process per deployment. `/ws/live` and the alert ring are per process, so a second replica would show a partial feed. Ingest itself is stateless.
- No retention or partitioning. `position_history` grows until you prune it.
- Losing Redis loses current position and geofence membership. The next ping re-emits `enter` for every zone the vehicle is already in.
- An API key is the security boundary. The device id is a registry lookup, not a per-device secret, so a valid key can claim any registered IMEI for that tenant.
- Revoking a key takes effect within the 60 second cache window for new connections. Open connections are not re-checked.
- No TLS. Terminate it in front of the public listener.
- No backup configuration. Run Postgres with PITR or scheduled dumps; this is the only durable record.
- Admin endpoints have no authentication.

## Operating notes

Create a key with 32 random bytes and store only its hash:

```bash
KEY=$(openssl rand -base64 32)
printf '%s' "$KEY" | sha256sum
```

Insert the hex digest into `api_keys`. Keep `auto_register_devices` false for real tenants; it exists for the dev seed and the simulator.

Decommission a vehicle with `UPDATE vehicles SET disabled_at = now() WHERE id = ...`. Do not delete the row; history and events reference it.
