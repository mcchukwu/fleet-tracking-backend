-- Tenant ownership is carried on the rows themselves, so history and
-- events can be queried and constrained without a join.
ALTER TABLE vehicles ADD COLUMN disabled_at TIMESTAMPTZ;

ALTER TABLE position_history ADD COLUMN tenant_id UUID;
UPDATE position_history h SET tenant_id = v.tenant_id FROM vehicles v WHERE v.id = h.vehicle_id;
ALTER TABLE position_history ALTER COLUMN tenant_id SET NOT NULL;
ALTER TABLE position_history ADD COLUMN applied BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE geofence_events ADD COLUMN tenant_id UUID;
UPDATE geofence_events e SET tenant_id = v.tenant_id FROM vehicles v WHERE v.id = e.vehicle_id;
ALTER TABLE geofence_events ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE vehicles ADD CONSTRAINT vehicles_tenant_id_id_key UNIQUE (tenant_id, id);
ALTER TABLE geofences ADD CONSTRAINT geofences_tenant_id_id_key UNIQUE (tenant_id, id);

-- Deleting a vehicle or a geofence must not erase the record of what
-- happened. Decommission instead (vehicles.disabled_at).
ALTER TABLE position_history DROP CONSTRAINT position_history_vehicle_id_fkey;
ALTER TABLE position_history ADD CONSTRAINT position_history_vehicle_fk
    FOREIGN KEY (tenant_id, vehicle_id) REFERENCES vehicles (tenant_id, id) ON DELETE RESTRICT;

ALTER TABLE geofence_events DROP CONSTRAINT geofence_events_vehicle_id_fkey;
ALTER TABLE geofence_events DROP CONSTRAINT geofence_events_geofence_id_fkey;
ALTER TABLE geofence_events ADD CONSTRAINT geofence_events_vehicle_fk
    FOREIGN KEY (tenant_id, vehicle_id) REFERENCES vehicles (tenant_id, id) ON DELETE RESTRICT;
ALTER TABLE geofence_events ADD CONSTRAINT geofence_events_geofence_fk
    FOREIGN KEY (tenant_id, geofence_id) REFERENCES geofences (tenant_id, id) ON DELETE RESTRICT;

CREATE INDEX idx_geofence_events_geofence_id ON geofence_events (geofence_id);

-- Redis can replay a transition after a restart; the same event must not
-- be stored twice.
DELETE FROM geofence_events a
USING geofence_events b
WHERE a.id > b.id
  AND a.vehicle_id = b.vehicle_id
  AND a.geofence_id = b.geofence_id
  AND a.event_type = b.event_type
  AND a.event_time = b.event_time;
CREATE UNIQUE INDEX geofence_events_natural_key
    ON geofence_events (vehicle_id, geofence_id, event_type, event_time);

-- No query reads either index on the write path, and the GIST index is
-- the most expensive one to maintain per inserted position.
DROP INDEX IF EXISTS idx_position_history_location;
DROP INDEX IF EXISTS idx_geofences_boundary;

-- Nothing maintained this column.
ALTER TABLE geofences DROP COLUMN updated_at;

-- The in-memory containment check ignores interior rings, so a polygon with
-- a hole would silently disagree with PostGIS.
ALTER TABLE geofences ADD CONSTRAINT geofences_no_holes
    CHECK (ST_NumInteriorRings(boundary::geometry) = 0);
