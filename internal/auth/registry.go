// Package auth is device authentication, deliberately scoped to the
// connection, not the ping. A real tracker is physically installed in
// one truck and only ever reports its own identity for the life of a
// connection, so identity is validated once, at WS handshake time
// (tenant API key + device-ID registry check), and every ping on that
// connection inherits the already-validated identity. This is what keeps
// auth from costing anything on the hot path: the per-ping cost is
// unchanged from before this package existed, because there isn't one.
//
// What this does and does not prove, stated plainly rather than implied:
// the API key is a real secret and is the actual security boundary. The
// device-ID/IMEI check is a registry lookup, not cryptographic device
// authentication, it catches typos, decommissioned devices, and limits
// blast radius if a key leaks, but anyone holding a valid key AND a
// correct device ID can still claim that identity. Per-device secrets
// are the real upgrade path if that threat model ever matters here; not
// needed for how devices are actually provisioned today (IMEI import,
// not self-service enrollment).
package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

var ErrInvalidKey = errors.New("auth: invalid or revoked api key")

type keyCacheEntry struct {
	tenantID string
	expires  time.Time
}

// Registry is the device-auth check against Postgres, with a small
// in-memory cache for API-key lookups so a burst of connection attempts
// (the exact reconnect-storm scenario internal/ratelimit already guards
// admission for) doesn't turn into a database query per connection on
// top of it.
type Registry struct {
	db *sql.DB

	cacheMu  sync.Mutex
	cache    map[string]keyCacheEntry // sha256 hex digest -> cached result
	cacheTTL time.Duration
}

func NewRegistry(db *sql.DB) *Registry {
	return &Registry{
		db:       db,
		cache:    make(map[string]keyCacheEntry),
		cacheTTL: 60 * time.Second,
	}
}

// AuthenticateKey hashes rawKey and looks up the owning, non-revoked
// tenant. Returns ErrInvalidKey for an unknown or revoked key, not a
// generic error, so callers can respond 401 specifically rather than 500.
func (r *Registry) AuthenticateKey(ctx context.Context, rawKey string) (tenantID string, err error) {
	hash := hashKey(rawKey)

	r.cacheMu.Lock()
	if entry, ok := r.cache[hash]; ok && time.Now().Before(entry.expires) {
		r.cacheMu.Unlock()
		return entry.tenantID, nil
	}
	r.cacheMu.Unlock()

	err = r.db.QueryRowContext(ctx, `
		SELECT tenant_id FROM api_keys
		WHERE key_hash = $1 AND revoked_at IS NULL
	`, hash).Scan(&tenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrInvalidKey
	}
	if err != nil {
		return "", err
	}

	r.cacheMu.Lock()
	r.cache[hash] = keyCacheEntry{tenantID: tenantID, expires: time.Now().Add(r.cacheTTL)}
	r.cacheMu.Unlock()
	return tenantID, nil
}

// AuthorizeDevice checks whether externalID (an IMEI, or a simulator id)
// is registered to tenantID. If it isn't, and the tenant has opted into
// auto_register_devices (see migrations/000002_device_auth.up.sql for
// why that's opt-in, not the default), it's registered on the spot and
// allowed; otherwise it's rejected. Returns ok=false, no error, for a
// legitimate "not registered and not allowed" outcome, that's an
// expected result this function is meant to report, not a failure.
func (r *Registry) AuthorizeDevice(ctx context.Context, tenantID, externalID string) (ok bool, err error) {
	var exists bool
	err = r.db.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM vehicles WHERE tenant_id = $1 AND external_id = $2)
	`, tenantID, externalID).Scan(&exists)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	var autoRegister bool
	err = r.db.QueryRowContext(ctx, `SELECT auto_register_devices FROM tenants WHERE id = $1`, tenantID).Scan(&autoRegister)
	if err != nil {
		return false, err
	}
	if !autoRegister {
		return false, nil
	}

	_, err = r.db.ExecContext(ctx, `
		INSERT INTO vehicles (tenant_id, external_id)
		VALUES ($1, $2)
		ON CONFLICT (tenant_id, external_id) DO NOTHING
	`, tenantID, externalID)
	if err != nil {
		return false, err
	}
	return true, nil
}

func hashKey(rawKey string) string {
	sum := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(sum[:])
}
