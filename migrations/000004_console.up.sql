CREATE TYPE user_role AS ENUM ('admin', 'dispatcher', 'security', 'viewer');
CREATE TYPE trip_status AS ENUM ('planned', 'assigned', 'active', 'completed', 'cancelled');
CREATE TYPE alert_type AS ENUM (
    'geofence_enter', 'geofence_exit', 'route_deviation', 'idle',
    'speeding', 'device_offline', 'unplanned_trip'
);
CREATE TYPE alert_severity AS ENUM ('low', 'medium', 'high', 'critical');
CREATE TYPE alert_status AS ENUM ('open', 'acked', 'closed');

CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    email         TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    name          TEXT NOT NULL,
    role          user_role NOT NULL DEFAULT 'viewer',
    status        TEXT NOT NULL DEFAULT 'active',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX users_email ON users (lower(email));

CREATE TABLE sessions (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    user_agent TEXT,
    ip_address TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX sessions_user_id ON sessions (user_id);
CREATE INDEX sessions_expires_at ON sessions (expires_at);

CREATE TABLE drivers (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    phone      TEXT,
    licence    TEXT,
    status     TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, id)
);

ALTER TABLE vehicles
    ADD COLUMN plate             TEXT,
    ADD COLUMN vehicle_type      TEXT,
    ADD COLUMN make              TEXT,
    ADD COLUMN model             TEXT,
    ADD COLUMN year              INTEGER,
    ADD COLUMN colour            TEXT,
    ADD COLUMN chassis_number    TEXT,
    ADD COLUMN sim_number        TEXT,
    ADD COLUMN device_model      TEXT,
    ADD COLUMN protocol          TEXT,
    ADD COLUMN depot             TEXT,
    ADD COLUMN department        TEXT,
    ADD COLUMN status            TEXT NOT NULL DEFAULT 'active',
    ADD COLUMN current_driver_id UUID,
    ADD COLUMN last_seen_at      TIMESTAMPTZ;

ALTER TABLE vehicles ADD CONSTRAINT vehicles_current_driver_fk
    FOREIGN KEY (tenant_id, current_driver_id) REFERENCES drivers (tenant_id, id)
    ON DELETE SET NULL (current_driver_id);
CREATE UNIQUE INDEX vehicles_tenant_plate ON vehicles (tenant_id, plate) WHERE plate IS NOT NULL;
CREATE INDEX vehicles_tenant_status ON vehicles (tenant_id, status);

CREATE TABLE assignments (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    vehicle_id UUID NOT NULL,
    driver_id  UUID NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at   TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, vehicle_id) REFERENCES vehicles (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, driver_id) REFERENCES drivers (tenant_id, id) ON DELETE RESTRICT
);
CREATE INDEX assignments_vehicle ON assignments (vehicle_id, started_at DESC);

CREATE TABLE trips (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    vehicle_id    UUID NOT NULL,
    driver_id     UUID,
    origin        TEXT NOT NULL,
    destination   TEXT NOT NULL,
    planned_route geography(LineString, 4326),
    status        trip_status NOT NULL DEFAULT 'planned',
    started_at    TIMESTAMPTZ,
    ended_at      TIMESTAMPTZ,
    created_by    UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, vehicle_id) REFERENCES vehicles (tenant_id, id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, driver_id) REFERENCES drivers (tenant_id, id) ON DELETE SET NULL (driver_id)
);
CREATE INDEX trips_tenant_status ON trips (tenant_id, status);
CREATE INDEX trips_vehicle_time ON trips (vehicle_id, created_at DESC);

CREATE TABLE trip_stops (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_id     UUID NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    sequence    INTEGER NOT NULL,
    geofence_id UUID REFERENCES geofences(id) ON DELETE SET NULL,
    name        TEXT NOT NULL,
    planned_at  TIMESTAMPTZ,
    arrived_at  TIMESTAMPTZ,
    departed_at TIMESTAMPTZ,
    UNIQUE (trip_id, sequence)
);

CREATE TABLE alerts (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    vehicle_id  UUID NOT NULL,
    trip_id     UUID REFERENCES trips(id) ON DELETE SET NULL,
    type        alert_type NOT NULL,
    severity    alert_severity NOT NULL DEFAULT 'medium',
    status      alert_status NOT NULL DEFAULT 'open',
    occurred_at TIMESTAMPTZ NOT NULL,
    location    geography(Point, 4326),
    evidence    JSONB NOT NULL DEFAULT '{}'::jsonb,
    acked_by    UUID REFERENCES users(id) ON DELETE SET NULL,
    acked_at    TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, vehicle_id) REFERENCES vehicles (tenant_id, id) ON DELETE RESTRICT
);
CREATE INDEX alerts_tenant_status ON alerts (tenant_id, status, occurred_at DESC);
CREATE INDEX alerts_tenant_time ON alerts (tenant_id, occurred_at DESC);
CREATE UNIQUE INDEX alerts_open_unique ON alerts (vehicle_id, type) WHERE status = 'open';

CREATE TABLE alert_rules (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    type       alert_type NOT NULL,
    enabled    BOOLEAN NOT NULL DEFAULT true,
    config     JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (tenant_id, type)
);

CREATE TABLE import_jobs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id     UUID REFERENCES users(id) ON DELETE SET NULL,
    filename    TEXT,
    total       INTEGER NOT NULL DEFAULT 0,
    imported    INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'running',
    errors      JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX import_jobs_tenant_time ON import_jobs (tenant_id, created_at DESC);

CREATE TABLE audit_log (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id    UUID REFERENCES users(id) ON DELETE SET NULL,
    action     TEXT NOT NULL,
    entity     TEXT,
    entity_id  TEXT,
    metadata   JSONB NOT NULL DEFAULT '{}'::jsonb,
    ip_address TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX audit_log_tenant_time ON audit_log (tenant_id, created_at DESC);

CREATE INDEX idx_position_history_tenant_time ON position_history (tenant_id, recorded_at);
CREATE INDEX idx_position_history_recorded_brin ON position_history USING BRIN (recorded_at);
