// Command bootstrap creates a tenant, its first admin and one device key,
// without any demo data.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/mcchukwu/fleet-tracking-backend/internal/config"
	"github.com/mcchukwu/fleet-tracking-backend/internal/db"
	"github.com/mcchukwu/fleet-tracking-backend/internal/identity"
)

func main() {
	tenantName := flag.String("tenant", "", "tenant name")
	email := flag.String("email", "", "admin email")
	password := flag.String("password", "", "admin password (min 8 characters)")
	name := flag.String("name", "", "admin display name (default: tenant admin)")
	keyLabel := flag.String("key-label", "default", "label for the device API key")
	onExists := flag.String("if-exists", "fail", "fail or skip when the tenant name already exists")
	flag.Parse()

	if strings.TrimSpace(*tenantName) == "" || strings.TrimSpace(*email) == "" || len(*password) < 8 {
		fmt.Fprintln(os.Stderr, "-tenant, -email and a password of at least 8 characters are required")
		os.Exit(1)
	}
	if *onExists != "fail" && *onExists != "skip" {
		fmt.Fprintln(os.Stderr, "-if-exists must be fail or skip")
		os.Exit(1)
	}

	dsn, err := config.LoadDatabase()
	if err != nil {
		slog.Error("configuration", "error", err)
		os.Exit(1)
	}
	sqlDB, err := db.ConnectPostgres(dsn)
	if err != nil {
		slog.Error("connect", "error", err)
		os.Exit(1)
	}
	defer sqlDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := create(ctx, sqlDB, *tenantName, *email, *password, *name, *keyLabel, *onExists)
	if err != nil {
		slog.Error("bootstrap failed", "error", err)
		os.Exit(1)
	}
	if result.skipped {
		fmt.Printf("tenant %q already exists, skipped\n", *tenantName)
		return
	}
	fmt.Println("tenant:", result.tenantID)
	fmt.Println("admin: ", result.email)
	fmt.Println("api key:", result.rawKey)
	fmt.Println("store the key now; only its hash is saved")
}

type result struct {
	tenantID string
	email    string
	rawKey   string
	skipped  bool
}

func create(ctx context.Context, sqlDB *sql.DB, tenantName, email, password, name, keyLabel, onExists string) (result, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if name == "" {
		name = tenantName + " Admin"
	}

	tx, err := sqlDB.BeginTx(ctx, nil)
	if err != nil {
		return result{}, err
	}
	defer tx.Rollback()

	var existing string
	err = tx.QueryRowContext(ctx, `SELECT id FROM tenants WHERE name = $1 LIMIT 1`, tenantName).Scan(&existing)
	if err == nil {
		if onExists == "skip" {
			return result{skipped: true}, nil
		}
		return result{}, fmt.Errorf("tenant %q already exists", tenantName)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return result{}, err
	}

	var tenantID string
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO tenants (name) VALUES ($1) RETURNING id
	`, tenantName).Scan(&tenantID); err != nil {
		return result{}, err
	}

	hash, err := identity.HashPassword(password)
	if err != nil {
		return result{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO users (tenant_id, email, password_hash, name, role)
		VALUES ($1, $2, $3, $4, 'admin')
	`, tenantID, email, hash, name); err != nil {
		return result{}, err
	}

	rawKey, keyHash, err := identity.NewToken()
	if err != nil {
		return result{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO api_keys (tenant_id, key_hash, label)
		VALUES ($1, $2, $3)
	`, tenantID, keyHash, keyLabel); err != nil {
		return result{}, err
	}

	for kind, cfg := range map[string]string{
		"geofence_exit":   `{}`,
		"route_deviation": `{"distance_m":2000,"duration_s":180}`,
		"idle":            `{"speed_kph":2,"duration_s":600}`,
		"speeding":        `{"speed_kph":80,"duration_s":120}`,
		"device_offline":  `{"duration_s":600}`,
		"unplanned_trip":  `{}`,
	} {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO alert_rules (tenant_id, type, enabled, config)
			VALUES ($1, $2, true, $3::jsonb)
		`, tenantID, kind, cfg); err != nil {
			return result{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return result{}, err
	}
	return result{tenantID: tenantID, email: email, rawKey: rawKey}, nil
}
