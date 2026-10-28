package identity

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) UserByEmail(ctx context.Context, email string) (*User, error) {
	const q = `
		SELECT id, tenant_id, email, password_hash, name, role, status
		FROM users
		WHERE lower(email) = lower($1)
	`
	var u User
	err := r.db.QueryRowContext(ctx, q, email).Scan(
		&u.ID, &u.TenantID, &u.Email, &u.PasswordHash, &u.Name, &u.Role, &u.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *Repository) CreateSession(ctx context.Context, userID, tokenHash, userAgent, ip string, expires time.Time) error {
	const q = `
		INSERT INTO sessions (user_id, token_hash, user_agent, ip_address, expires_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err := r.db.ExecContext(ctx, q, userID, tokenHash, userAgent, ip, expires)
	return err
}

func (r *Repository) SessionUser(ctx context.Context, tokenHash string) (*User, error) {
	const q = `
		SELECT u.id, u.tenant_id, u.email, u.name, u.role, u.status
		FROM sessions s
		JOIN users u ON u.id = s.user_id
		WHERE s.token_hash = $1
		  AND s.revoked_at IS NULL
		  AND s.expires_at > now()
		  AND u.status = 'active'
	`
	var u User
	err := r.db.QueryRowContext(ctx, q, tokenHash).Scan(
		&u.ID, &u.TenantID, &u.Email, &u.Name, &u.Role, &u.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, sql.ErrNoRows
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func (r *Repository) RevokeSession(ctx context.Context, tokenHash string) error {
	const q = `UPDATE sessions SET revoked_at = now() WHERE token_hash = $1 AND revoked_at IS NULL`
	_, err := r.db.ExecContext(ctx, q, tokenHash)
	return err
}

func (r *Repository) UserByID(ctx context.Context, tenantID, userID string) (*User, error) {
	const q = `SELECT id, tenant_id, email, name, role, status FROM users WHERE id = $1 AND tenant_id = $2`
	var u User
	if err := r.db.QueryRowContext(ctx, q, userID, tenantID).Scan(&u.ID, &u.TenantID, &u.Email, &u.Name, &u.Role, &u.Status); err != nil {
		return nil, err
	}
	return &u, nil
}
