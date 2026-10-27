package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

func ConnectPostgres(dsn string) (*sql.DB, error) {
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := conn.Ping(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	conn.SetMaxOpenConns(20)
	conn.SetMaxIdleConns(10)
	conn.SetConnMaxLifetime(30 * time.Minute)
	conn.SetConnMaxIdleTime(5 * time.Minute)
	return conn, nil
}

// RequireSchema fails fast when the process starts against a database that
// has not had the latest migration applied.
func RequireSchema(ctx context.Context, db *sql.DB) error {
	var ok bool
	err := db.QueryRowContext(ctx, `
		SELECT to_regclass('public.api_keys') IS NOT NULL
		   AND EXISTS (
			   SELECT 1 FROM information_schema.columns
			   WHERE table_name = 'position_history' AND column_name = 'tenant_id'
		   )
	`).Scan(&ok)
	if err != nil {
		return fmt.Errorf("check schema: %w", err)
	}
	if !ok {
		return errors.New("database schema is not current; run the migrations")
	}
	return nil
}

func ConnectRedis(addr, password string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %s: %w", addr, err)
	}
	return client, nil
}
