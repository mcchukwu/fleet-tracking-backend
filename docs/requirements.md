# Fleet Tracking Backend - Requirements

## 1. Purpose

A real-time fleet tracking backend that ingests vehicle GPS position updates
at scale, maintains low-latency live state, detects geofence entry/exit
events within a strict latency budget, and preserves the full position
history durably for route reconstruction and analytics.

The system is designed around a hot-path/cold-path split: Redis holds
ephemeral, low-latency live state; PostgreSQL/PostGIS holds durable history
and answers analytical spatial queries that are not latency-sensitive.

Ingestion is protocol-agnostic by design. A simulated-vehicle adapter
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

## 3. Non-Functional Requirements

| ID     | Requirement |
|--------|-------------|
| NFR-1  | **Throughput**: sustain 5,000+ concurrent WebSocket connections with no silently dropped position updates. "Dropped" is defined precisely: every update is either accepted and processed, or rejected with a counted, logged metric, never lost without record. |
| NFR-2  | **Latency**: a geofence entry/exit alert is emitted within p99 < 100ms of the triggering position update being received by the server, measured end-to-end. |
| NFR-3  | **Durability**: every accepted position update is eventually written to PostgreSQL, even if it was rejected on the hot path for being stale relative to a newer update. |
| NFR-4  | **Correctness under disorder**: current live state and geofence events remain correct when position updates for the same vehicle arrive out of order. |
| NFR-5  | **Observability**: the system exposes connection count, ingest rate, alert-latency distribution, and dropped-message count as metrics, so NFR-1 and NFR-2 are provable, not asserted. |
| NFR-6  | **Scalability posture**: ingestion nodes are stateless; all live state lives in Redis, so additional ingestion instances can be added behind a load balancer without an architecture change. |
| NFR-7  | **Cost**: the system runs on a single modest VM plus a managed or self-hosted Postgres/Redis instance; no message queue or stream-processing framework is introduced unless the measured throughput requires it. |

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

## 5. Out of Scope for v1

- Live video streaming from dashcams (event-triggered clip upload only).
- Authentication/authorization hardening (handled by a separate
  service)
- Horizontal scale-out actually implemented (only architecturally enabled).
