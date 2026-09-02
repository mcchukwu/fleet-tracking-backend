CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- A tenant is a fleet/organization boundary. Every vehicle and geofence
-- belongs to exactly one tenant. This exists from day one even though
-- Phase 1 only exercises a single tenant, because retrofitting
-- multi-tenancy later is a much larger rewrite than including it now.
CREATE TABLE tenants (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- external_id is the identifier the source system uses for this vehicle:
-- an IMEI for real hardware, or a simulator-assigned id (e.g. "sim-42").
-- It is unique per tenant, not globally, since a simulator test run and a
-- real fleet could otherwise collide on id values.
CREATE TABLE vehicles (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL,
    name        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, external_id)
);

-- geography(Polygon, 4326) rather than geometry: geography accounts for
-- earth curvature, which matters once zones span any real distance and
-- keeps containment/distance math correct without manual projection
-- handling. 4326 is plain lat/lon (WGS 84), what GPS devices report in.
CREATE TABLE geofences (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    boundary    geography(Polygon, 4326) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_geofences_boundary ON geofences USING GIST (boundary);
CREATE INDEX idx_geofences_tenant   ON geofences (tenant_id);

-- Append-only durable history. Nothing here is ever updated, an
-- out-of-order or duplicate ping still gets a row; correctness of "current
-- state" is a hot-path (Redis) concern, not a storage concern. recorded_at
-- is the device's own timestamp (event time); received_at is when the
-- server actually saw it, keeping both is what makes out-of-order
-- handling and later auditing possible.
CREATE TABLE position_history (
    id           BIGSERIAL PRIMARY KEY,
    vehicle_id   UUID NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
    recorded_at  TIMESTAMPTZ NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    location     geography(Point, 4326) NOT NULL,
    speed_kph    REAL,
    heading_deg  REAL,
    source       TEXT NOT NULL DEFAULT 'simulator' -- simulator | tracker | dashcam
);

CREATE INDEX idx_position_history_vehicle_time ON position_history (vehicle_id, recorded_at);
CREATE INDEX idx_position_history_location      ON position_history USING GIST (location);

CREATE TYPE geofence_event_type AS ENUM ('enter', 'exit');

-- The durable, queryable record of every geofence transition the hot path
-- decided was real (i.e. already survived the out of order buffering
-- window). This table is what "show me when this vehicle left the yard
-- last month" queries against, not position_history directly.
CREATE TABLE geofence_events (
    id           BIGSERIAL PRIMARY KEY,
    vehicle_id   UUID NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
    geofence_id  UUID NOT NULL REFERENCES geofences(id) ON DELETE CASCADE,
    event_type   geofence_event_type NOT NULL,
    event_time   TIMESTAMPTZ NOT NULL, -- device timestamp of the triggering ping
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_geofence_events_vehicle_time ON geofence_events (vehicle_id, event_time);
