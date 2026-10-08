# Fleet Tracking Backend

Real-time GPS fleet tracking backend, WebSocket ingestion, in-memory
geofencing, out-of-order-safe hot-path state, Redis/Postgres hot-cold
split. Single Go module; see `docs/requirements.md` for the full
functional/non-functional spec and the deliberate scope decisions behind
this design.

## Layout

```
go.mod                       single module for every binary below
docs/requirements.md         functional + non-functional requirements
migrations/                  full schema + dev seed data (golang-migrate format)
cmd/ingest/                  production binary: WS adapter + two HTTP listeners
cmd/loadgen/                 dev tool: 5,000-connection load + storm generator
cmd/routetest/               dev tool: deterministic correctness checks
internal/pipeline/           adapter-agnostic ingestion engine (worker pool,
                              out-of-order handling, geofence evaluation,
                              optional Broadcaster/ColdPathSink output ports)
internal/auth/                device authentication: tenant API key + device registry check
internal/coldpath/           async batched Postgres writer (durable history + alerts)
internal/hub/                live WebSocket fan-out to dashboard/operator clients
internal/geofence/           in-memory containment engine + Postgres loader
internal/config/             env config, validated at startup (fail fast)
internal/logger/             structured logging (log/slog)
internal/db/                 Postgres + Redis connection setup
internal/validator/          rejects malformed input before it enters the pipeline
internal/apperrors/          typed errors (full payoff arrives once a REST query API exists)
internal/ratelimit/          token bucket (connection-admission rate limiting)
```

Only `cmd/ingest` ships to production. `cmd/loadgen` and `cmd/routetest`
are dev tooling in the same module purely so they can share types like
`pipeline.PositionPing`, a production build runs `go build ./cmd/ingest`
and nothing else, so they never end up in a deployed image.

## Prerequisites

- Go 1.22+
- Redis (`docker run -p 6379:6379 redis:7`)
- Postgres with PostGIS (`docker run -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgis/postgis:16-3.4`)
- `golang-migrate` CLI

## Setup

```
go mod tidy
migrate -database "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" -path migrations up
psql "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" -f migrations/seed_dev.sql
```

## Run the ingest server

```
go run ./cmd/ingest
```

Two listeners, deliberately separate (see `docs/requirements.md`'s scope
decisions for why):

- **Public** (`LISTEN_ADDR`, default `:8080`) — only `/ws/ingest`. This is
  the surface untrusted devices/simulators connect to.
- **Admin** (`ADMIN_ADDR`, default `:9090`) — `/metrics/ingest`,
  `/metrics/alerts`, `/debug/alerts`, `/debug/vehicle?id=`, `/healthz`.
  Not meant to be reachable from wherever devices connect from — in a
  real deployment this stays on a private network, not the public internet.

You should see a startup log line listing the geofences it loaded
(`Test Yard`, from the seed data) and the active config.

## Device authentication

A WS connection to `/ws/ingest` must present two headers:

- `Authorization: Bearer <api-key>` — a tenant-scoped API key. The seed
  data creates one dev key for the dev tenant: `dev-local-only-key`.
  **Never use this value, or this pattern of committing a raw key's hash
  to source control, for a real tenant's credential.**
- `X-Device-ID: <id>` — the device's own identifier (an IMEI for real
  hardware, `sim-42`-style for the simulator). This is now the *only*
  source of vehicle identity — the ping body no longer carries a
  `vehicle_id` field at all, so a ping can never claim an identity
  different from the one its connection authenticated as.

Both `cmd/loadgen` and `cmd/routetest` default `-api-key` to the seeded
dev key, so the commands below work unchanged. An unregistered device
is rejected (`403`) unless its tenant has opted into
`auto_register_devices` — true for the seed dev tenant specifically so
the load generator can spin up thousands of never-before-seen simulator
IDs without a separate pre-registration step; **false is the correct
setting for any real tenant**. See `docs/architecture.md`'s "Device
authentication" section for the full reasoning, including what this
does and doesn't actually prove about a connecting device.

## Run the load generator

```
go run ./cmd/loadgen -vehicles 5000 -interval 7s -duration 2m -rampup 10s
```

Watch it land:

```
watch -n 2 curl -s http://localhost:9090/metrics/ingest
```

**Phase 1 exit criteria**: `sent` from the load generator's final summary
should equal `received` on `/metrics/ingest`, and `dropped` should be 0.
`queue_length` should stay well below `queue_capacity` throughout — if
it's pinned near capacity, workers can't keep up.

## Correctness checks (not the load generator — a deliberate route)

```
go run ./cmd/routetest
```

Runs two checks against a live server:

1. **Crossing test** (Phase 2) — drives one vehicle in a straight line
   through the seeded "Test Yard" polygon, checks for exactly one
   `enter` and one `exit`.
2. **Stale-ping test** (Phase 3) — sends one deliberately old,
   inside-the-geofence ping after the route finishes, and checks that
   neither the hot-path position (`/debug/vehicle`) nor the alert log
   were affected by it.

Prints `PASS`/`FAIL` for each.

Latency: `curl -s http://localhost:9090/metrics/alerts | jq` — for a
meaningful p99 (routetest only gives you a couple of samples), run the
5,000-vehicle load generator against a geofence some fraction of the
random routes will actually cross, then read this after.

## Phase 3 — out-of-order handling

The design, and why it's shaped this way rather than a universal
buffering delay that would blow the p99 latency budget to correctly
handle a minority of traffic:

A ping's hot-path write and live geofence evaluation only happen if its
`recorded_at` (device timestamp) is strictly newer than the most recent
one already applied for that vehicle. The compare-and-write is one atomic
Redis Lua script (`internal/pipeline`'s `applyPositionScript`), not a
Go-side check-then-set — with many workers, two pings for the same
vehicle processed concurrently is rare but not impossible, and this is
the exact property Phase 3 claims to guarantee, so it can't be a race.

A ping that fails that check — a duplicate or a genuinely late arrival —
is counted (`stale` in `/metrics/ingest`), not silently dropped, and does
**not** drive a live alert. See NFR-2 in `docs/requirements.md` for the
honest statement of what this does and doesn't guarantee: on-time data
keeps its sub-100ms p99; late data is safely excluded from live state and
alerts, but doesn't get a delayed-but-correct alert fired for it
retroactively. Reconstructing the fully correct historical record from
late data is cold-path work, arriving with Phase 4.

## Production hardening (NFR-8, NFR-9, NFR-10)

Added after comparing this project against
[saadbutt/device-gateway](https://github.com/saadbutt/device-gateway), a
reference implementation of the real-hardware-facing ingestion problem.
Three things Phase 1's own load test never exercised, because it runs
over loopback against a generator that neither drops connections nor goes
half-open:

- **Reconnect-storm admission control** — `internal/ratelimit`'s token
  bucket gates new connection *admission* (`ACCEPT_RATE`, default 500/s,
  `ACCEPT_BURST`, default 200) before the WS upgrade happens. A rejected
  connection gets a plain `503`, not a hang.
- **Half-open socket detection** — every read is bounded by an idle
  deadline (`IDLE_TIMEOUT`, default `90s`). A connection silent longer
  than that is closed and freed, rather than held open indefinitely on a
  TCP session that may no longer exist behind carrier NAT.
- **Named, configurable backpressure policy** (`OVERFLOW_POLICY`, default
  `drop_oldest`) — `block`, `drop_oldest`, or `drop_newest`. `drop_oldest`
  is the default because a stale queued live-position update is worthless
  the moment a newer one exists.

### Proving it: storm mode

```
go run ./cmd/loadgen -vehicles 5000 -interval 7s -duration 3m -storm 20s
```

Every simulated vehicle disconnects and reconnects roughly every 20s — a
rough analogue of a carrier region recovering from an outage. Watch
`/metrics/ingest`: `rejected` should climb above 0 during each storm (the
limiter is actually being hit), `idle_closed` should stay near 0
(reconnects are succeeding, not going half-open), `dropped` should stay
low. The load generator's own `dial_rejected` count in its summary is
expected to be > 0 too — that's correct client-side backoff against a
503, not an error.

## Phase 4 — cold-path durability and the live dashboard feed

Two optional output ports, wired into `internal/pipeline` as the
`ColdPathSink` and `Broadcaster` interfaces:

- **`internal/coldpath`** — every accepted ping (applied or stale) and
  every geofence alert is batched and written to Postgres asynchronously
  (`COLDPATH_BATCH_WINDOW`, default `1s`, or `COLDPATH_BATCH_SIZE`,
  default `500` — whichever comes first). A vehicle's external id (IMEI,
  or a simulator id) is auto-provisioned into the `vehicles` table on
  first sight and cached, so a batch of 500 pings from one vehicle costs
  one upsert, not 500. This closes NFR-3 (durability).
- **`internal/hub`** — a live WebSocket fan-out at `/ws/live` (on the
  **admin** listener — see "Production readiness gate" below for why it
  isn't public yet) pushing every applied position update and every
  alert to connected dashboard/operator clients in real time, instead of
  requiring them to poll. This is the output half of FR-8 that Phases
  1-3 hadn't built yet.

Try it: connect any WS client to `ws://localhost:9090/ws/live` while the
load generator or routetest is running, and watch live JSON messages
(`{"type":"position",...}` / `{"type":"alert",...}`) arrive as pings are
processed.

Check the cold path landed: `psql ... -c "select count(*) from position_history;"`
after a run — should be non-zero and growing. `coldpath_dropped_positions`
and `coldpath_dropped_alerts` in `/metrics/ingest` should stay at 0 under
normal load; a non-zero value means the cold-path queue is saturated
relative to Postgres write throughput, worth investigating before it's a
silent data-loss source.

## Phase 5 — architecture doc

See `docs/architecture.md` for the full write-up: the component map, the
reasoning behind every major scope decision, and — importantly — a
**production readiness gate** listing what still needs to happen (TLS,
real secrets, backups) before this touches a real fleet's data.
Engineering quality and "safe to put real customer data through" are
different claims; that section is honest about the gap between them.

## Device authentication (post-Phase-5 addition)

`/ws/ingest` now requires a tenant API key and a device-ID header, checked
once at connection time — see "Device authentication" above for how to
run it and `docs/architecture.md`'s section of the same name for the full
design reasoning, including why it's a from-scratch, in-repo check rather
than a network call to the companion `multi-tenant-auth-service` project.

## Known limitations, stated rather than hidden

- `drop_oldest`'s evict-then-send is two separate channel operations, not
  one atomic step — under heavy contention another goroutine could take
  the freed slot first, falling through to counting a drop instead. A
  fully atomic version needs a mutex-guarded ring buffer instead of a
  channel; not yet justified by a measured drop rate.
- The accept-rate limiter throttles at the HTTP-handler level (reject
  before `websocket.Accept`), not a raw TCP accept-loop the way a bare
  TCP server can — forced by sitting on `net/http`, not a shortcut.
- Every ping is checked against a single hardcoded default tenant's
  geofences (`defaultTenantID` in `cmd/ingest/main.go`) — no per-vehicle
  tenant resolution yet. Real multi-tenant work, worth doing once there's
  a second tenant to actually test against.
- No periodic geofence reload (restart required to pick up a geofence
  change), and no REST query API for history/geofence CRUD yet — both
  real remaining work, not done tonight.
- A ping that fails the Redis hot-path write (a transient error, not a
  stale/duplicate one) is not enqueued to the cold path either —
  durability across a Redis outage specifically is further hardening.
- `/ws/ingest` is authenticated (tenant API key + device registry check);
  **`/ws/live` still has no authentication at all** — see
  `docs/architecture.md`'s production readiness gate before exposing it
  anywhere but the internal admin listener it's already confined to.
- The device-ID registry check proves a device is *registered*, not that
  it's cryptographically *who it claims to be* — see `docs/architecture.md`'s
  "Device authentication" section for exactly what threat model this is
  and isn't sized for.
