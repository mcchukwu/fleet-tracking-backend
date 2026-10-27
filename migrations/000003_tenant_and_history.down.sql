DROP INDEX IF EXISTS geofence_events_natural_key;
DROP INDEX IF EXISTS idx_geofence_events_geofence_id;

ALTER TABLE geofence_events DROP CONSTRAINT geofence_events_geofence_fk;
ALTER TABLE geofence_events DROP CONSTRAINT geofence_events_vehicle_fk;
ALTER TABLE geofence_events ADD CONSTRAINT geofence_events_vehicle_id_fkey
    FOREIGN KEY (vehicle_id) REFERENCES vehicles (id) ON DELETE CASCADE;
ALTER TABLE geofence_events ADD CONSTRAINT geofence_events_geofence_id_fkey
    FOREIGN KEY (geofence_id) REFERENCES geofences (id) ON DELETE CASCADE;
ALTER TABLE geofence_events DROP COLUMN tenant_id;

ALTER TABLE position_history DROP CONSTRAINT position_history_vehicle_fk;
ALTER TABLE position_history ADD CONSTRAINT position_history_vehicle_id_fkey
    FOREIGN KEY (vehicle_id) REFERENCES vehicles (id) ON DELETE CASCADE;
ALTER TABLE position_history DROP COLUMN applied;
ALTER TABLE position_history DROP COLUMN tenant_id;

ALTER TABLE geofences DROP CONSTRAINT geofences_tenant_id_id_key;
ALTER TABLE vehicles DROP CONSTRAINT vehicles_tenant_id_id_key;
ALTER TABLE vehicles DROP COLUMN disabled_at;

ALTER TABLE geofences ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX idx_position_history_location ON position_history USING GIST (location);
CREATE INDEX idx_geofences_boundary ON geofences USING GIST (boundary);
