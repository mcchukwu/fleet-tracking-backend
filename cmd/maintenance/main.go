package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/mcchukwu/fleet-tracking-backend/internal/db"
	"github.com/mcchukwu/fleet-tracking-backend/internal/maintenance"
)

func main() {
	days := flag.Int("days", envInt("RETENTION_DAYS", 60), "position history retention in days")
	batch := flag.Int("batch", 10000, "rows deleted per batch")
	flag.Parse()

	_ = godotenv.Load()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		slog.Error("PG_DSN is required")
		os.Exit(1)
	}
	if *days <= 0 || *batch <= 0 {
		slog.Error("days and batch must be positive")
		os.Exit(1)
	}

	sqlDB, err := db.ConnectPostgres(dsn)
	if err != nil {
		slog.Error("connect", "error", err)
		os.Exit(1)
	}
	defer sqlDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	result, err := maintenance.Run(ctx, sqlDB, *days, *batch)
	if err != nil {
		slog.Error("maintenance failed", "error", err)
		os.Exit(1)
	}
	slog.Info("maintenance complete",
		"positions", result.Positions, "sessions", result.Sessions, "imports", result.Imports)
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
