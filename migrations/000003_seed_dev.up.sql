-- seed_dev.sql
-- One tenant with a fixed, known id (referenced directly in
-- cmd/ingest/main.go's defaultTenantID, so don't change this UUID
-- without updating that constant too) and one geofence: a ~1.1km square
-- roughly centered in the same Lagos-area coordinate space the load
-- generator and routetest tool both use, so a deliberate straight-line
-- route can be driven through it for correctness testing.

-- auto_register_devices = true here ONLY because this is the dev/demo
-- tenant and the load generator needs to be able to spin up 5,000
-- never-before-seen simulated vehicle ids without a separate 5,000-row
-- pre-registration step. A real tenant should NOT set this — leave it
-- at its false default so an unrecognized device is rejected, matching
-- how devices are actually provisioned (IMEI import), not auto-added.
INSERT INTO tenants (id, name, auto_register_devices)
VALUES ('00000000-0000-0000-0000-000000000001', 'Dev Tenant', true)
ON CONFLICT (id) DO UPDATE SET auto_register_devices = true;
 
-- Dev-only API key. Raw key is "dev-local-only-key" — never use this
-- value, or this pattern of committing a raw key's hash to source
-- control, for a real tenant's credential.
INSERT INTO api_keys (tenant_id, key_hash, label)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    encode(digest('dev-local-only-key', 'sha256'), 'hex'),
    'local dev key — see README'
)
ON CONFLICT (key_hash) DO NOTHING;

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
