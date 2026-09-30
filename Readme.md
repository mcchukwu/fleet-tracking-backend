# Fleet Tracking Backend

Real-time GPS fleet tracking backend with in-memory
geofencing, out-of-order-safe hot-path state, Redis/Postgres hot-cold
split. see `docs/requirements.md` for the full functional/non-functional spec 
and the deliberate scope decisions behind this design.

## Layout

```
go.mod                       single module for every binary below
docs/requirements.md         functional + non-functional requirements
migrations/                  full schema + dev seed data (golang-migrate format)
cmd/ingest/                  production binary: WS adapter + two HTTP listeners
cmd/loadgen/                 dev tool: 5,000-connection load + storm generator
cmd/routetest/               dev tool: deterministic correctness checks
internal/pipeline/           adapter-agnostic ingestion engine (worker pool,
                              out-of-order handling, geofence evaluation)
internal/geofence/           in-memory containment engine + Postgres loader
internal/config/             env config, validated at startup (fail fast)
internal/logger/             structured logging (log/slog)
internal/db/                 Postgres + Redis connection setup
internal/validator/          rejects malformed input before it enters the pipeline
internal/apperrors/          typed errors (full payoff arrives with Phase 4's cmd/api)
internal/ratelimit/          token bucket (connection-admission rate limiting)
```

Only `cmd/ingest` ships to production. `cmd/loadgen` and `cmd/routetest`
are dev tooling in the same module purely so they can share types like
`pipeline.PositionPing`. A production build runs `go build ./cmd/ingest`
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

- **Public** (`LISTEN_ADDR`, default `:8080`): only `/ws/ingest`. This is
  the surface untrusted devices/simulators connect to.
- **Admin** (`ADMIN_ADDR`, default `:9090`): `/metrics/ingest`,
  `/metrics/alerts`, `/debug/alerts`, `/debug/vehicle?id=`, `/healthz`.
  Not meant to be reachable from wherever devices connect from, in a
  real deployment this stays on a private network, not the public internet.

You should see a startup log line listing the geofences it loaded
(`Test Yard`, from the seed data) and the active config.

## Run the load generator

```
go run ./cmd/loadgen -vehicles 5000 -interval 7s -duration 2m -rampup 10s
```

Watch it land:

```
watch -n 2 curl -s http://localhost:9090/metrics/ingest
```

**Exit criteria**: `sent` from the load generator's final summary
should equal `received` on `/metrics/ingest`, and `dropped` should be 0.
`queue_length` should stay well below `queue_capacity` throughout, if
it's pinned near capacity, workers can't keep up.

## Correctness checks (not the load generator)

```
go run ./cmd/routetest
```

Runs two checks against a live server:

1. **Crossing test**: drives one vehicle in a straight line
   through the seeded "Test Yard" polygon, checks for exactly one
   `enter` and one `exit`.
2. **Stale-ping test**: sends one deliberately old,
   inside-the-geofence ping after the route finishes, and checks that
   neither the hot-path position (`/debug/vehicle`) nor the alert log
   were affected by it.

Prints `PASS`/`FAIL` for each.

Latency: `curl -s http://localhost:9090/metrics/alerts | jq` for a
meaningful p99 (routetest only gives you a couple of samples), run the
5,000-vehicle load generator against a geofence some fraction of the
random routes will actually cross, then read this after.

## Out-of-order handling

The design, and why it's shaped this way rather than a universal
buffering delay that would blow the p99 latency budget to correctly
handle a minority of traffic:

A ping's hot-path write and live geofence evaluation only happen if its
`recorded_at` (device timestamp) is strictly newer than the most recent
one already applied for that vehicle. The compare-and-write is one atomic
Redis Lua script (`internal/pipeline`'s `applyPositionScript`), not a
Go-side check then set, with many workers, two pings for the same
vehicle processed concurrently is rare but not impossible, and this is
the exact property it claims to guarantee, so it can't be a race.

A ping that fails that check, a duplicate or a genuinely late arrival is 
counted (`stale` in `/metrics/ingest`), not silently dropped, and does
**not** drive a live alert. See NFR-2 in `docs/requirements.md` for the
honest statement of what this does and doesn't guarantee: on-time data
keeps its sub-100ms p99; late data is safely excluded from live state and
alerts, but doesn't get a delayed-but-correct alert fired for it
retroactively. Reconstructing the fully correct historical record from
late data is cold-path work.

## Production hardening (NFR-8, NFR-9, NFR-10)

- **Reconnect-storm admission control**: `internal/ratelimit`'s token
  bucket gates new connection *admission* (`ACCEPT_RATE`, default 500/s,
  `ACCEPT_BURST`, default 200) before the WS upgrade happens. A rejected
  connection gets a plain `503`, not a hang.
- **Half-open socket detection**: every read is bounded by an idle
  deadline (`IDLE_TIMEOUT`, default `90s`). A connection silent longer
  than that is closed and freed, rather than held open indefinitely on a
  TCP session that may no longer exist behind carrier NAT.
- **Named, configurable backpressure policy** (`OVERFLOW_POLICY`, default
  `drop_oldest`): `block`, `drop_oldest`, or `drop_newest`. `drop_oldest`
  is the default because a stale queued live-position update is worthless
  the moment a newer one exists.

### Proving it: storm mode

```
go run ./cmd/loadgen -vehicles 5000 -interval 7s -duration 3m -storm 20s
```

Every simulated vehicle disconnects and reconnects roughly every 20s. A
rough analogue of a carrier region recovering from an outage. Watch
`/metrics/ingest`: `rejected` should climb above 0 during each storm (the
limiter is actually being hit), `idle_closed` should stay near 0
(reconnects are succeeding, not going half-open), `dropped` should stay
low. The load generator's own `dial_rejected` count in its summary is
expected to be > 0 too, that's correct client-side backoff against a
503, not an error.

## Known limitations, stated rather than hidden

- `drop_oldest`'s evict-then-send is two separate channel operations, not
  one atomic step: under heavy contention another goroutine could take
  the freed slot first, falling through to counting a drop instead. A
  fully atomic version needs a mutex-guarded ring buffer instead of a
  channel; not yet justified by a measured drop rate.
- The accept-rate limiter throttles at the HTTP handler level (reject
  before `websocket.Accept`), not a raw TCP accept-loop the way a bare
  TCP server can: forced by sitting on `net/http`, not a shortcut.
- Every ping is checked against a single hardcoded default tenant's
  geofences (`defaultTenantID` in `cmd/ingest/main.go`): no per-vehicle
  tenant resolution yet. Real multi-tenant work, worth doing once there's
  a second tenant to actually test against.
- No Postgres writes yet, and no periodic geofence reload (restart
  required to pick up a geofence change)
