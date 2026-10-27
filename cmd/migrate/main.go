package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/mcchukwu/fleet-tracking-backend/internal/migrate"
)

func main() {
	dir := flag.String("dir", "migrations", "directory containing *.up.sql files")
	flag.Parse()

	_ = godotenv.Load()

	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		slog.Error("PG_DSN is required")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := migrate.Run(ctx, dsn, *dir); err != nil {
		slog.Error("migration failed", "error", err)
		os.Exit(1)
	}
	slog.Info("migrations up to date")
}
