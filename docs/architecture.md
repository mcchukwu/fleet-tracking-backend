# Architecture

This document describes what was actually built across Phases 1-4, why
each decision was made, and — just as important — what was deliberately
left out and why. See `docs/requirements.md` for the full functional and
non-functional specification this architecture satisfies.

## The core idea: hot path / cold path split

Many independent vehicles continuously emit position updates. The system
needs to (a) know each vehicle's current position with minimal staleness,
(b) detect geofence crossings fast, and (c) keep the full history durably
for later reconstruction and analytics. Doing all three against one
disk-backed relational store collapses under load: a PostGIS containment
query per incoming ping cannot reliably hit sub-100ms p99 once write
volume and query contention compete for the same resource.

The resolution, and the single idea the rest of this document elaborates
on: **Redis holds ephemeral, low-latency live state; Postgres/PostGIS
holds durable history and answers slow analytical questions that were
never on the hot path to begin with.**

## Component map

```
                 ┌─────────────────────────────────────────────┐
                 │              internal/pipeline               │
  WS simulator   │  (adapter-agnostic core)                     │
  (cmd/ingest) ──┼─► Submit() ─► channel ─► worker pool          │
                 │                 │                             │
  future TCP     │                 ├─► applyPosition (Redis,     │
  device adapter │                 │    atomic Lua script)       │
  (Phase 6/7) ───┼─►               │                             │
                 │                 ├─► evaluateGeofences          │
                 │                 │    (in-memory containment,   │
                 │                 │     Redis set diff)          │
                 │                 │                             │
                 │                 ├─► ColdPathSink (optional) ───┼──► internal/coldpath
                 │                 │                             │     (batched Postgres writer)
                 │                 └─► Broadcaster (optional) ────┼──► internal/hub
                 └─────────────────────────────────────────────┘     (live WS fan-out)
```

`internal/pipeline` knows nothing about WebSocket, TCP, Postgres, or
dashboards. It exposes one input (`Submit`) and two optional outputs
(`Broadcaster`, `ColdPathSink`), both interfaces. This is the
ports-and-adapters pattern applied on both sides of the core: a transport
adapter converts its protocol into `PositionPing` and calls `Submit`; a
consumer implements `Broadcaster` or `ColdPathSink` to receive output.
Neither side needs to know the other exists. Today there is one input
adapter (WS simulator) and two output consumers (`coldpath`, `hub`); a
real-hardware TCP adapter is additive, not a rewrite.

## Out-of-order handling (Phase 3)

GPS pings can arrive out of order: network retransmission, multiple
stateless ingestion instances behind a load balancer, or a device
buffering locally during a signal drop and flushing several pings at
once on reconnect. Naively overwriting "current position" with whatever
arrives last means a delayed ping can stomp a newer position, or worse,
fabricate a false geofence re-entry event.

**The design actually used**: a ping's hot-path write and live geofence
evaluation only happen if its `recorded_at` (device timestamp) is
strictly newer than the most recent one already applied for that
vehicle. The compare-and-write is one atomic Redis Lua script
(`applyPositionScript` in `internal/pipeline`), not a Go-side
check-then-set — with many concurrent workers, two pings for the same
vehicle processed concurrently is rare but not impossible, and a race
there would silently undermine the exact property this phase exists to
guarantee.

**What this deliberately does not do**: a universal buffering delay wide
enough to let realistically-late pings (seconds) sort themselves out
before acting on any of them. That would guarantee eventual correctness
for all data, but at the cost of applying a multi-second delay to *all*
live alerting — breaking the sub-100ms p99 claim for the 99%+ of traffic
that isn't out of order, to correctly handle the minority that is. A
ping that fails the staleness check is counted (`stale` in
`/metrics/ingest`), excluded from live geofence evaluation and live
broadcast, but still reaches the cold path — so the durable history is
complete even when the live alert stream correctly excludes it. This is
a real, named scope boundary (NFR-2 in `docs/requirements.md`), not an
unstated limitation.

## Production hardening

Added mid-project after comparing this system against
[saadbutt/device-gateway](https://github.com/saadbutt/device-gateway), a
reference implementation of the real-hardware ingestion problem. Three
things the project's own load test never exercised, because that test
runs over loopback against a generator that neither drops connections
nor goes half-open:

- **Reconnect-storm admission control** (`internal/ratelimit`): a token
  bucket gates new WS connection admission before the upgrade happens, so
  a mass simultaneous reconnect (a carrier region recovering) degrades by
  slowing new admissions, not by the accept path itself becoming the
  bottleneck for connections already up.
- **Half-open socket detection**: every read is bounded by a re-armed
  idle deadline. TCP keepalive alone is too slow (minutes) for this —
  a connection silently dead behind carrier NAT is detected and freed
  within one idle window, not left open indefinitely.
- **Named, configurable backpressure policy**: queue overflow is
  `block` / `drop_oldest` / `drop_newest`, chosen explicitly
  (`drop_oldest` by default — a stale queued live-position update is
  worthless the moment a newer one exists), not one hardcoded behavior.

This is recorded here, with the source acknowledged, because it's a
better demonstration of engineering practice than pretending the design
arrived fully-formed: real systems get better by being checked against
other real systems, not solely by first-principles reasoning in
isolation.

## Scope decisions (the things deliberately not built, and why)

- **No message queue** (Kafka/NATS) between ingestion and processing.
  Expected load is 500-1,000 messages/second at 5,000 vehicles pinging
  every 5-10s; a buffered Go channel and a fixed worker pool handle this
  with room to spare. Reassess only if measured load exceeds this by an
  order of magnitude.
- **No MQTT broker.** Evaluated and rejected: the actual target hardware
  (a CMSV6/CMSV7-compatible dashcam, implying JT/T808; an unidentified
  legacy GPS tracker) speaks raw TCP, not MQTT. Introducing a broker
  would mean translating a device's native protocol into MQTT just to
  have this service subscribe to it — an extra moving part and an extra
  network hop with no consumer that needs the pub/sub fan-out a broker
  provides. A direct TCP adapter parsing the native protocol into
  `pipeline.Submit()` is simpler and has an identical shape to the
  existing WS adapter.
- **No separate time-series database.** If `position_history` ever needs
  retention/compression/time-bucketing at a scale plain Postgres
  struggles with, TimescaleDB is a Postgres extension (hypertables on the
  same database, same PostGIS) — not a parallel system to keep in sync.
- **No PostGIS query in the hot path.** Geofence containment runs
  in-memory (ray-casting over a handful of polygon vertices per check —
  see `internal/geofence`) against polygons loaded from Postgres once at
  startup. PostGIS's actual job is being the system of record for
  geofence definitions and the engine for slow, infrequent analytical
  spatial queries (dwell time, route coverage) — not the per-ping
  containment check.
- **No per-vehicle tenant resolution on the ingestion path yet.** Every
  ping is evaluated against one hardcoded default tenant's geofences.
  Real multi-tenant routing is worth building once there's a second
  tenant to actually test against, not speculatively now.
- **No periodic geofence reload.** The in-memory cache loads once at
  startup; changing a geofence requires a restart. A real gap, closed
  whenever geofence CRUD (Phase 4 remainder) is built.
- **`internal/apperrors` is intentionally light.** Its payoff arrives
  once a REST query API exists to map errors to HTTP status codes
  consistently — not needed by anything built so far.

## Device authentication

A WS connection must present an `Authorization: Bearer <api-key>` header
(tenant-scoped, opaque, SHA-256-hashed at rest — the same revocable-token
pattern used by this author's companion `multi-tenant-auth-service`
project, applied here independently rather than as a runtime dependency —
see "Why this isn't a call to another service" below) and an
`X-Device-ID` header (an IMEI for real hardware, a simulator id like
`sim-42` for the load generator). Both are checked once, at connection
time, by `internal/auth`, not per ping — a real tracker is physically
installed in one truck and only ever reports its own identity for the
life of a connection, so there is no reason to re-validate on every
message. The wire format reflects this: `vehicle_id` was removed from
the ping body entirely, specifically so a ping can never claim an
identity different from the one its connection authenticated as.

**Two checks, two different jobs**: the API key proves which *tenant*
this connection belongs to — it's a real secret, and it's the actual
security boundary. The device-ID lookup against the `vehicles` table
proves this *specific device* is one that tenant has registered — a
registry check, not cryptographic device authentication. Anyone holding
a valid key and a correct device ID can still claim that identity; this
is sized for "random stranger with no key," which is the real, present
risk given how devices are actually provisioned here (IMEI import, not
self-service enrollment). Per-device secrets are the upgrade path if
that threat model ever changes — not built because nothing today
requires it.

An unregistered device is rejected by default (`403`), not silently
added — `tenants.auto_register_devices` is an explicit per-tenant
opt-in for the rare case (this project's own load generator) where
auto-provisioning thousands of never-before-seen simulator IDs is the
desired behavior, not the production default.

**Why this isn't a call to another service.** The natural question —
given `multi-tenant-auth-service` already exists — is why this project
doesn't call it over the network instead of
reimplementing an opaque-token check. Two reasons, not one: first, that
service currently authenticates *humans* logging into a dashboard
(password + session), not machine credentials for a device that never
logs in — there was no endpoint to call. Second, and more fundamentally,
a shared network auth service earns its cost when a capability is
genuinely reused across multiple independently-evolving products; a
standalone open-source demo reaching out to a separate running service
for a single-purpose API-key check is the inverse of that — it would
make this repo's own 5,000-connection load test depend on another
project's uptime for no benefit a reader of this repo would see. The
pattern is intentionally duplicated in miniature here, not imported as a
dependency.

## Production readiness gate — read this before real device data flows through this system

Everything above is built to a genuinely production-grade *standard of
engineering*. That is a different claim from "ready to accept real
customer data on the open internet," and the gap between those two
claims is real, not a formality. Before pointing this at an actual fleet:

1. **No TLS.** `ws://` and plain HTTP throughout. Any real deployment
   needs `wss://` for ingestion and HTTPS for the admin listener, via a
   reverse proxy (Caddy, nginx, or a cloud load balancer) terminating TLS
   in front of both. Without it, the API key now protecting `/ws/ingest`
   is sent in the clear.
2. **Default credentials in config.** The default `PG_DSN` has a
   hardcoded `postgres:postgres` password. Fine for local development;
   never acceptable as a shipped default — production config must come
   from a secrets manager or environment injection, never the fallback
   value.
3. **No backup/retention policy for Postgres.** `position_history` and
   `geofence_events` are the durable record this whole hot/cold split
   exists to protect — an un-backed-up database defeats the purpose.
4. **`/ws/live` has no authentication at all yet**, which is exactly why it's
   wired to the internal admin listener and not the public one (see
   `internal/hub`'s package comment). It cannot move to a public,
   customer-facing listener until point 1 is solved for it too.

None of this is a reason to delay finishing the engineering work — it's
a reason to sequence what comes next deliberately: wire in auth, put TLS
in front of both listeners, rotate the default credentials out, and set
up backups, *before* a real tracker or dashcam's data touches this
system, not after.
