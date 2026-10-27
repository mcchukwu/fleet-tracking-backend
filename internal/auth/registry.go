// Package auth authenticates a device once, at WebSocket handshake. The
// API key is the secret; the device id is a registry lookup, so a valid
// key can claim any registered device in its tenant.
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

// Registry caches API-key lookups for 60 seconds.
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

// AuthenticateKey returns the tenant for a live key.
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

// AuthorizeDevice returns the internal vehicle id for a registered,
// active device. When the tenant allows auto-registration, an unknown
// device is created first. It returns ok=false for a device that is not
// registered and may not be created.
func (r *Registry) AuthorizeDevice(ctx context.Context, tenantID, externalID string) (vehicleID string, ok bool, err error) {
	err = r.db.QueryRowContext(ctx, `
		SELECT id FROM vehicles
		WHERE tenant_id = $1 AND external_id = $2 AND disabled_at IS NULL
	`, tenantID, externalID).Scan(&vehicleID)
	if err == nil {
		return vehicleID, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}

	var autoRegister bool
	err = r.db.QueryRowContext(ctx, `SELECT auto_register_devices FROM tenants WHERE id = $1`, tenantID).Scan(&autoRegister)
	if err != nil {
		return "", false, err
	}
	if !autoRegister {
		return "", false, nil
	}

	// The update is a no-op used only to make RETURNING work on conflict.
	err = r.db.QueryRowContext(ctx, `
		INSERT INTO vehicles (tenant_id, external_id)
		VALUES ($1, $2)
		ON CONFLICT (tenant_id, external_id) DO UPDATE SET external_id = EXCLUDED.external_id
		RETURNING id
	`, tenantID, externalID).Scan(&vehicleID)
	if err != nil {
		return "", false, err
	}
	return vehicleID, true, nil
}

func hashKey(rawKey string) string {
	sum := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(sum[:])
}
