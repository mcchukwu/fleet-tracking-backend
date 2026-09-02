# Fleet Tracking Backend — Phase 1

Core ingestion only: 
WebSocket simulator adapter → buffered channel → worker pool → Redis hot-path state. 
No geofencing, no Postgres writes yet.

## Layout

```
docs/requirements.md        functional + non-functional requirements
migrations/                 full initial schema (golang-migrate format)
cmd/ingest/                 the WS ingestion server
cmd/loadgen/                the 5,000-connection load generator
```

## Prerequisites

- Go 1.22+
- Redis running locally (`docker run -p 6379:6379 redis:7`)
- Postgres with PostGIS, for when Phase 4 needs it, not required yet
  (`docker run -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgis/postgis:16-3.4`)
- `golang-migrate` CLI if you want to apply the migration now, even though
  nothing writes to Postgres until Phase 4:
  `migrate -database "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable" -path migrations up`

## Run the ingest server

```
cd cmd/ingest
go mod tidy
go run .
```

Listens on `:8080`. WS endpoint: `ws://localhost:8080/ws/ingest`.
Metrics: `curl http://localhost:8080/metrics/ingest`

## Run the load generator

In a second terminal, once the server is up:

```
cd cmd/loadgen
go mod tidy
go run . -vehicles 5000 -interval 7s -duration 2m -rampup 10s
```

While it runs, poll the server's metrics in a third terminal:

```
watch -n 2 curl -s http://localhost:8080/metrics/ingest
```

## Phase 1 exit criteria — how to actually check it

- `queue_length` should stay well below `queue_capacity` throughout the
  run. If it's pinned near capacity, workers can't keep up — raise
  `workerPoolSize` in `cmd/ingest/main.go` or investigate why Redis writes
  are slow before touching the channel size.
- At the end of the run, the load generator prints `sent=N`. The server's
  `/metrics/ingest` should show `received=N` and `dropped=0`. If
  `dropped > 0`, Phase 1 isn't done yet — that's the whole point of
  counting drops explicitly instead of hiding them.
- Spot-check a few vehicles' state landed in Redis:
  `redis-cli HGETALL vehicle:sim-0:state`

## Notes on what's deliberately not here yet

- No Postgres writes — Phase 4.
- No geofence logic — Phase 2.
- No out-of-order handling — Phase 3. The load generator here sends
  strictly in-order pings per vehicle; out-of-order injection is added to
  it in Phase 3, not now, so a bug can't hide behind two changes at once.
- This load generator targets the WebSocket simulator adapter only, not a
  real hardware interface — see the comment at the top of
  `cmd/loadgen/main.go`.
