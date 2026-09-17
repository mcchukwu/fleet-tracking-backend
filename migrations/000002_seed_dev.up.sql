-- seed_dev.sql
-- One tenant with a fixed, known id (referenced directly in
-- cmd/ingest/main.go's defaultTenantID, so don't change this UUID
-- without updating that constant too) and one geofence: a ~1.1km square
-- roughly centered in the same Lagos-area coordinate space the load
-- generator and routetest tool both use, so a deliberate straight-line
-- route can be driven through it for correctness testing.

INSERT INTO tenants (id, name)
VALUES (
    '00000000-0000-0000-0000-000000000001', 
    'Dev Tenant'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO geofences (id, tenant_id, name, boundary)
VALUES (
    '00000000-0000-0000-0000-000000000101',
    '00000000-0000-0000-0000-000000000001',
    'Test Yard',
    ST_GeogFromText('SRID=4326;POLYGON((
        3.395 6.525,
        3.405 6.525,
        3.405 6.535,
        3.395 6.535,
        3.395 6.525
    ))')
)
ON CONFLICT (id) DO NOTHING;
