// Package config centralizes environment-driven configuration and
// validates it at startup. The bug this fixes: previously, an invalid
// OVERFLOW_POLICY value silently fell through to "drop_newest" inside
// the ingestion hot path, with no warning anywhere. A typo in an env var
// should fail loudly at process start, not silently change production
// behavior three layers deep.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Ingest struct {
	RedisAddr           string
	ListenAddr          string // public: device/simulator ingestion only
	AdminAddr           string // internal-only: metrics, debug, health, live dashboard feed
	PGDSN               string
	OverflowPolicy      string
	IdleTimeout         time.Duration
	AcceptRate          float64
	AcceptBurst         float64
	ColdPathBatchWindow time.Duration
	ColdPathBatchSize   int
}

func LoadIngest() (Ingest, error) {
	c := Ingest{
		RedisAddr:           getenv("REDIS_ADDR", "localhost:6379"),
		ListenAddr:          getenv("LISTEN_ADDR", ":8080"),
		AdminAddr:           getenv("ADMIN_ADDR", ":9090"),
		PGDSN:               getenv("PG_DSN", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
		OverflowPolicy:      getenv("OVERFLOW_POLICY", "drop_oldest"),
		IdleTimeout:         getenvDuration("IDLE_TIMEOUT", 90*time.Second),
		AcceptRate:          getenvFloat("ACCEPT_RATE", 500),
		AcceptBurst:         getenvFloat("ACCEPT_BURST", 200),
		ColdPathBatchWindow: getenvDuration("COLDPATH_BATCH_WINDOW", 1*time.Second),
		ColdPathBatchSize:   getenvInt("COLDPATH_BATCH_SIZE", 500),
	}
	if err := c.Validate(); err != nil {
		return Ingest{}, err
	}
	return c, nil
}

func (c Ingest) Validate() error {
	switch c.OverflowPolicy {
	case "block", "drop_oldest", "drop_newest":
	default:
		return fmt.Errorf("invalid OVERFLOW_POLICY %q: must be block, drop_oldest, or drop_newest", c.OverflowPolicy)
	}
	if c.IdleTimeout <= 0 {
		return fmt.Errorf("IDLE_TIMEOUT must be positive, got %s", c.IdleTimeout)
	}
	if c.AcceptRate <= 0 || c.AcceptBurst <= 0 {
		return fmt.Errorf("ACCEPT_RATE and ACCEPT_BURST must be positive")
	}
	if c.ListenAddr == c.AdminAddr {
		return fmt.Errorf("LISTEN_ADDR and ADMIN_ADDR must differ (public ingest vs internal metrics/debug)")
	}
	if c.ColdPathBatchWindow <= 0 {
		return fmt.Errorf("COLDPATH_BATCH_WINDOW must be positive, got %s", c.ColdPathBatchWindow)
	}
	if c.ColdPathBatchSize <= 0 {
		return fmt.Errorf("COLDPATH_BATCH_SIZE must be positive, got %d", c.ColdPathBatchSize)
	}
	return nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func getenvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
