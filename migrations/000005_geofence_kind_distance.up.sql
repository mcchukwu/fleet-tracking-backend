CREATE TYPE geofence_kind AS ENUM ('depot', 'customer', 'restricted', 'other');

ALTER TABLE geofences ADD COLUMN kind geofence_kind NOT NULL DEFAULT 'other';
ALTER TABLE trips ADD COLUMN distance_km NUMERIC(10, 2);
