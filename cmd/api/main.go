package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mcchukwu/fleet-tracking-backend/internal/api"
	"github.com/mcchukwu/fleet-tracking-backend/internal/config"
	"github.com/mcchukwu/fleet-tracking-backend/internal/db"
	"github.com/mcchukwu/fleet-tracking-backend/internal/identity"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := config.LoadAPI()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	sqlDB, err := db.ConnectPostgres(cfg.PGDSN)
	if err != nil {
		log.Error("cannot connect to postgres", "error", err)
		os.Exit(1)
	}
	defer sqlDB.Close()
	if err := db.RequireSchema(context.Background(), sqlDB); err != nil {
		log.Error("database is not ready", "error", err)
		os.Exit(1)
	}

	rdb, err := db.ConnectRedis(cfg.RedisAddr, cfg.RedisPassword)
	if err != nil {
		log.Error("cannot connect to redis", "error", err)
		os.Exit(1)
	}

	ids := identity.NewService(identity.NewRepository(sqlDB), cfg.SessionTTL)
	handler := api.New(sqlDB, rdb, ids, cfg, log).Handler()

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	go func() {
		log.Info("api listening", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Info("api stopped")
}
