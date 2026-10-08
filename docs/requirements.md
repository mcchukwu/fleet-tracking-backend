# Fleet Tracking Backend Requirements

## 1. Purpose

A real-time fleet tracking backend that ingests vehicle GPS position updates
at scale, maintains low-latency live state, detects geofence entry/exit
events within a strict latency budget, and preserves the full position
history durably for route reconstruction and analytics.

The system is designed around a hot-path/cold-path split: Redis holds
ephemeral, low-latency live state; PostgreSQL/PostGIS holds durable history
and answers analytical spatial queries that are not latency-sensitive.

Ingestion is protocol-agnostic by design — a simulated-vehicle adapter
(WebSocket) and real-hardware adapters (raw TCP/UDP speaking
manufacturer-specific or standard protocols such as JT/T808) both feed the
same internal ingestion pipeline, so no core logic needs to know or care
where a position update originated.

## 2. Functional Requirements

| ID    | Requirement |
|-------|-------------|
| FR-1  | Accept GPS position updates from many concurrent vehicle clients over WebSocket (vehicle identifier, lat, lon, timestamp, optionally speed/heading). |
| FR-2  | Maintain and serve the current/live position of each vehicle with low latency. |
| FR-3  | Detect and emit geofence entry/exit events per vehicle per configured zone. |
| FR-4  | Persist the full position history durably, queryable by vehicle and time range. |
| FR-5  | Support CRUD operations on geofences (polygon geometry, owning tenant/fleet). |
| FR-6  | Support querying/replaying a vehicle's route history for an arbitrary time window. |
| FR-7  | Handle out-of-order and duplicate position pings correctly, using event time (device timestamp) rather than receipt time. |
| FR-8  | Expose live state and events through a push interface (WebSocket) and historical/analytical data through a query interface (REST). |
| FR-9  | Support multiple ingestion adapters (simulated WebSocket clients, real hardware over TCP/UDP) converging on one internal pipeline, without changes to core logic per adapter added. |
| FR-10 | Authenticate every ingestion connection to a tenant (API key) and authorize the specific device against that tenant's registry (device-ID/IMEI lookup) once, at connection time — not per ping, and not derived from client-supplied data in the ping body. |

## 3. Non-Functional Requirements

| ID     | Requirement |
|--------|-------------|
| NFR-1  | **Throughput**: sustain 5,000+ concurrent WebSocket connections with no silently dropped position updates. "Dropped" is defined precisely: every update is either accepted and processed, or rejected with a counted, logged metric — never lost without record. |
| NFR-2  | **Latency**: a geofence entry/exit alert is emitted within p99 < 100ms of the triggering position update being received by the server, measured end-to-end, for on-time (non-superseded) data. A ping that arrives older than the most recent one already applied for its vehicle — a duplicate or a late arrival — is excluded from live alerting by design (see FR-7); it does not get a delayed-but-eventually-correct alert. Reconstructing the fully correct historical record from late data is a cold-path (Phase 4) concern, not a live-alerting one. This is a deliberate scope narrowing, not an oversight: a universal buffering delay wide enough to catch realistic lateness (seconds) would break this NFR for all traffic to correctly handle a minority of it. |
| NFR-3  | **Durability**: every accepted position update is eventually written to PostgreSQL, even if it was rejected on the hot path for being stale relative to a newer update. |
| NFR-4  | **Correctness under disorder**: current live state and geofence events remain correct when position updates for the same vehicle arrive out of order. |
| NFR-5  | **Observability**: the system exposes connection count, ingest rate, alert-latency distribution, and dropped-message count as metrics, so NFR-1 and NFR-2 are provable, not asserted. |
| NFR-6  | **Scalability posture**: ingestion nodes are stateless; all live state lives in Redis, so additional ingestion instances can be added behind a load balancer without an architecture change. |
| NFR-7  | **Cost**: the system runs on a single modest VM plus a managed or self-hosted Postgres/Redis instance; no message queue or stream-processing framework is introduced unless the measured throughput requires it. |
| NFR-8  | **Reconnect-storm resilience**: new connection admission is rate-limited (token bucket), so a mass simultaneous reconnect (e.g. a carrier outage recovering) degrades gracefully — bounded accept rate — rather than the accept path itself becoming the bottleneck. |
| NFR-9  | **Half-open connection detection**: a connection that stops sending data is detected and closed via an application-level idle deadline, not left open on the strength of a TCP session that may no longer exist behind carrier NAT. TCP keepalive alone is too slow (minutes) for this. |
| NFR-10 | **Explicit, configurable backpressure policy**: queue-overflow behavior is a named, chosen policy (block / drop_oldest / drop_newest), not one hardcoded behavior — the correct choice depends on what the data represents (see docs/architecture notes). |

| NFR-11 | **Device authentication is connection-scoped, not per-message.** The cost of authenticating a device must not appear in the per-ping hot path, or it would compete with NFR-2's latency budget. An API-key lookup is cached briefly in-process specifically so a burst of (re)connections doesn't turn into a database query per connection on top of the admission-rate limiting NFR-8 already provides. |

*NFR-8 through NFR-10 were added after Phase 1 was already load-tested and passing — they weren't surfaced by that test, because the test ran over loopback against a generator that never drops a connection or goes half-open. They were identified by comparing this project against a reference implementation solving the same real-hardware-ingestion problem (github.com/saadbutt/device-gateway), not derived internally. Recorded here as a reminder that "the test passed" and "it's production grade" are not the same claim, and that checking against real prior art is now a deliberate part of this project's process, not a one-off.*

*FR-10 and NFR-11 close what the production readiness gate (see docs/architecture.md) originally named as the single most important gap: an unauthenticated `/ws/ingest`. What they deliberately do NOT claim: the device-ID check proves registration, not cryptographic device identity — see docs/architecture.md's "Device authentication" section for the full statement of that boundary.*

## 4. Scope Decisions (explicit, not implicit)

- No message queue (Kafka/NATS) between ingestion and processing in v1.
  Expected load is 500–1,000 messages/second at 5,000 vehicles pinging
  every 5–10s; a buffered Go channel and a fixed worker pool handle this
  with room to spare. Reassess only if measured load exceeds this
  assumption by an order of magnitude.
- Geofence containment checks run in-memory against cached polygons, not
  as a PostGIS query per incoming ping. PostGIS is the system of record for
  geofence definitions and the engine for cold-path analytical spatial
  queries (dwell time, coverage area), not the hot-path containment engine.
- No dedicated "current state" table in PostgreSQL. Redis is the sole
  source of truth for live position; PostgreSQL holds only durable history.
- All application code lives under `internal/` (compiler-enforced:
  unimportable from outside this module), not `pkg/`. `pkg/` is a
  community convention, not a Go language feature, and its entire meaning
  is "importable by other projects", nothing here is a standalone
  library intended for that, so it stays out until something genuinely
  is (e.g. the geofence engine extracted as its own repo).
- Ingestion logic (worker pool, out-of-order handling, geofence
  evaluation) lives in `internal/pipeline`, deliberately transport-agnostic.
  `cmd/ingest`'s only job is translating WebSocket frames into
  `pipeline.PositionPing` and calling `Submit`, the same entry point a
  future real-hardware TCP adapter (Phase 6/7) will call, without
  duplicating any ingestion logic.
- Two HTTP listeners in `cmd/ingest`: a public one exposing only
  `/ws/ingest` (what untrusted devices connect to), and an internal-only
  one exposing `/metrics/*` and `/debug/*`. The internal listener is not
  meant to be reachable from wherever devices connect from.

## 5. Out of Scope for v1

- Live video streaming from dashcams (event-triggered clip upload only).
- Authentication/authorization hardening (assumed handled by a separate
  service).
- Horizontal scale-out actually implemented (only architecturally enabled).
