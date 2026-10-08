package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
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

func loadEnv() error {
	if err := godotenv.Load(); err != nil {
		return err
	}

	return nil
}

func LoadIngest() (Ingest, error) {
	loadEnv()

	idleTimeout, err := getenvDuration("IDLE_TIMEOUT", 90*time.Second)
	if err != nil {
		return Ingest{}, err
	}
	acceptRate, err := getenvFloat("ACCEPT_RATE", 500)
	if err != nil {
		return Ingest{}, err
	}
	acceptBurst, err := getenvFloat("ACCEPT_BURST", 200)
	if err != nil {
		return Ingest{}, err
	}
	batchWindow, err := getenvDuration("COLDPATH_BATCH_WINDOW", time.Second)
	if err != nil {
		return Ingest{}, err
	}
	batchSize, err := getenvInt("COLDPATH_BATCH_SIZE", 500)
	if err != nil {
		return Ingest{}, err
	}
	c := Ingest{
		RedisAddr:           getenv("REDIS_ADDR", ""),
		ListenAddr:          getenv("LISTEN_ADDR", ""),
		AdminAddr:           getenv("ADMIN_ADDR", ""),
		PGDSN:               getenv("PG_DSN", ""),
		OverflowPolicy:      getenv("OVERFLOW_POLICY", ""),
		IdleTimeout:         idleTimeout,
		AcceptRate:          acceptRate,
		AcceptBurst:         acceptBurst,
		ColdPathBatchWindow: batchWindow,
		ColdPathBatchSize:   batchSize,
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

func getenvDuration(key string, fallback time.Duration) (time.Duration, error) {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s %q: %w", key, v, err)
		}
		return d, nil
	}
	return fallback, nil
}

func getenvFloat(key string, fallback float64) (float64, error) {
	if v := os.Getenv(key); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid %s %q: %w", key, v, err)
		}
		return f, nil
	}
	return fallback, nil
}

func getenvInt(key string, fallback int) (int, error) {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s %q: %w", key, v, err)
		}
		return n, nil
	}
	return fallback, nil
}
