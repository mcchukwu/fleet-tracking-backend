package db

import (
	"context"
	"database/sql"
	"fmt"

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
	return conn, nil
}

func ConnectRedis(addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %s: %w", addr, err)
	}
	return client, nil
}
