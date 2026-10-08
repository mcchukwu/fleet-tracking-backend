-- Device authentication: a tenant-scoped API key, checked once at WS
-- connection time, plus an explicit per-tenant choice about whether an
-- unrecognized device auto-registers (convenient for demos/simulators)
-- or is rejected outright (the correct default for a real tenant whose
-- devices are provisioned by IMEI import, not self-service).
 
ALTER TABLE tenants
    ADD COLUMN auto_register_devices BOOLEAN NOT NULL DEFAULT false;
-- false is the production-safe default: an unrecognized device is
-- rejected, not silently added. A tenant opts into permissive
-- auto-registration explicitly (see seed_dev.sql for why the dev tenant
-- does) rather than it being the system-wide behavior.
 
-- Opaque, hashed, revocable, the same pattern already proven out in
-- multi-tenant-auth-service, for the same reason: a leaked key is
-- revoked by setting revoked_at, not by rotating a secret everywhere
-- it's embedded.
CREATE TABLE api_keys (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key_hash    TEXT NOT NULL UNIQUE, -- sha256(raw key), hex-encoded; the raw key is never stored
    label       TEXT,                 -- e.g. "yard devices", for humans reading the table
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at  TIMESTAMPTZ
);
 
CREATE INDEX idx_api_keys_tenant ON api_keys (tenant_id);
