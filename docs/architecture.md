# Architecture

```
device ──ws──► cmd/ingest ──► pipeline.Submit ──► worker pool
                                                    │
                              ┌─────────────────────┼─────────────────────┐
                              ▼                     ▼                     ▼
                        Redis Lua            coldpath queue          hub
                     live state + sets      batched Postgres      live feed
```

## Why the split

A geofence check per ping against PostGIS would put a database round trip in the latency path and make ingest depend on Postgres availability. The hot path reads geofences from memory, keeps current state in Redis, and writes history asynchronously. Postgres is the system of record for definitions and history; it is not consulted to answer "where is this vehicle now".

## Ingest

`cmd/ingest` accepts one WebSocket path on the public listener. At handshake it looks up the API key hash in Postgres and checks the device id against `vehicles` for that tenant. It returns the internal vehicle UUID; the connection carries tenant, external id and UUID for its lifetime.

Frames are decoded into `pipeline.PositionPing` and submitted to one bounded channel. The overflow policy is explicit:

- `block` applies backpressure to the connection
- `drop_oldest` keeps the newest position
- `drop_newest` keeps the oldest queued work

Every drop is counted. `received` counts submissions, not accepted writes.

## Ordering

`applyPositionScript` in `internal/pipeline` is one Lua script. It compares the incoming `recorded_at` with the stored value, writes the new position, and replaces the vehicle's geofence membership set. Because the compare and both writes happen inside Redis, concurrent workers or instances cannot emit duplicate or inverted transitions.

Timestamps are formatted as fixed-width UTC strings so the comparison is exact in Lua; nanosecond integers would lose precision in Lua's float type.

A ping that is not newer is counted as stale. It is not evaluated for live alerts and not broadcast, but it is still written to history. Buffering every ping for a few seconds to sort out late arrivals would add that delay to all traffic, which is a worse trade for a live map.

## Cold path

Two queues feed two batching goroutines. A batch is written as one multi-row insert. If that fails, the same rows are retried one at a time so a single bad row does not discard the batch. Failed rows are counted.

The writer does not resolve vehicle ids. The UUID comes from the handshake, so a deleted or decommissioned vehicle cannot be recreated by a stale cache.

## Shutdown

On `SIGTERM` or `SIGINT`:

1. close device WebSocket connections
2. shut down both HTTP servers
3. close the ingest channel and wait for workers
4. close the cold-path queues, flush the remaining batches
5. close Postgres

Hijacked WebSocket connections are not tracked by `http.Server.Shutdown`, which is why step 1 is explicit.

## Tenancy

Tenant and vehicle UUID are fields on `PositionPing`. Geofence lookup, Redis keys, history rows, events and the live feed all use the value established at handshake. Redis keys are `tenant:<uuid>:vehicle:<external id>:...`; the same IMEI in two tenants is two independent states.

History and event rows carry `tenant_id` and composite foreign keys to `vehicles (tenant_id, id)` and `geofences (tenant_id, id)`, so the database rejects a cross-tenant pair even if application code is wrong.

## Failure behaviour

| Failure | What happens |
|---|---|
| Redis unavailable | Pings are counted as Redis errors and skipped for live state; they are not written to history. The worker does not block forever: Redis calls have a 5s timeout. |
| Postgres unavailable | Cold-path batches fail, retry per row, then increment `coldpath_failed_*`. Ingest keeps running. |
| Queue full | Records are dropped according to the overflow policy and counted. |
| Process killed with SIGKILL | Queued pings and the in-flight batch are lost. The protocol has no acknowledgement, so the device cannot tell. |
| Redis data loss | Current state and geofence membership are gone. The next ping re-emits `enter` for every zone the vehicle is inside. Duplicate events are rejected by the natural-key unique index. |
| Geofence changed in Postgres | Not picked up until restart. |
| One replica dies | Devices reconnect; admission limits slow the reconnect. Live feed clients must reconnect. |

## Scaling

Ingest is stateless: state is in Redis and history is in Postgres, so another replica can take traffic. Two things are per process and stop a naive second replica:

- `/ws/live` only broadcasts updates produced by its own process
- the alert ring and `/debug/alerts` are local

Horizontal scale needs a shared fan-out (Redis pub/sub or a broker) before it is honest. One replica is the supported deployment.

## Decisions

- No Kafka or NATS. 5,000 vehicles at one ping per 5 seconds is about 1,000 messages/second; a bounded channel and 64 workers handle that without another system to operate.
- Geofences in memory rather than a query per ping. The cost is a restart to reload definitions.
- API keys hashed with SHA-256. They are 256-bit random values, so a slow KDF adds startup cost for no brute-force benefit.
- Session-scoped device auth. Identity is checked at handshake, not per frame, which keeps the hot path free of database lookups.
