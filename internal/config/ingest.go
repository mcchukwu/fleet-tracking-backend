package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Ingest struct {
	RedisAddr      string
	ListenAddr     string
	AdminAddr      string
	PGDSN          string
	OverflowPolicy string
	IdleTimeout    time.Duration
	AcceptRate     float64
	AcceptBurst    float64
}

func LoadIngest() (Ingest, error) {
	redisAddr := getenv("REDIS_ADDR", "localhost:6379")
	listenAddr := getenv("LISTEN_ADDR", ":8080")
	adminAddr := getenv("ADMIN_ADDR", ":9090")

	dbHost := getenv("DB_HOST", "localhost")
	dbPort := getenv("DB_PORT", "5435")
	dbName := getenv("DB_NAME", "fleet_tracking")
	dbUser := getenv("DB_USER", "fleet")
	dbPassword := getenv("DB_PASSWORD", "fleet_dev_password")
	dbSSLMode := getenv("DB_SSLMODE", "disable")

	pgDSN := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		dbUser,
		dbPassword,
		dbHost,
		dbPort,
		dbName,
		dbSSLMode,
	)

	overflowPolicy := getenv(
		"OVERFLOW_POLICY",
		"drop_oldest",
	)

	idleTimeout, err := getenvDuration(
		"IDLE_TIMEOUT",
		90*time.Second,
	)
	if err != nil {
		return Ingest{}, err
	}

	acceptRate, err := getenvFloat(
		"ACCEPT_RATE",
		500,
	)
	if err != nil {
		return Ingest{}, err
	}

	acceptBurst, err := getenvFloat(
		"ACCEPT_BURST",
		200,
	)
	if err != nil {
		return Ingest{}, err
	}

	c := Ingest{
		RedisAddr:      redisAddr,
		ListenAddr:     listenAddr,
		AdminAddr:      adminAddr,
		PGDSN:          pgDSN,
		OverflowPolicy: overflowPolicy,
		IdleTimeout:    idleTimeout,
		AcceptRate:     acceptRate,
		AcceptBurst:    acceptBurst,
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
		return fmt.Errorf(
			"invalid OVERFLOW_POLICY %q: must be block, drop_oldest, or drop_newest",
			c.OverflowPolicy,
		)
	}

	if c.IdleTimeout <= 0 {
		return fmt.Errorf(
			"IDLE_TIMEOUT must be positive, got %s",
			c.IdleTimeout,
		)
	}

	if c.AcceptRate <= 0 {
		return fmt.Errorf(
			"ACCEPT_RATE must be positive, got %f",
			c.AcceptRate,
		)
	}

	if c.AcceptBurst <= 0 {
		return fmt.Errorf(
			"ACCEPT_BURST must be positive, got %f",
			c.AcceptBurst,
		)
	}

	if c.ListenAddr == c.AdminAddr {
		return fmt.Errorf(
			"LISTEN_ADDR and ADMIN_ADDR must differ: public ingest vs internal admin",
		)
	}

	return nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func getenvDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)

	if value == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf(
			"invalid %s=%q: must be a valid Go duration: %w",
			key,
			value,
			err,
		)
	}

	return duration, nil
}

func getenvFloat(key string, fallback float64) (float64, error) {
	value := os.Getenv(key)

	if value == "" {
		return fallback, nil
	}

	number, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf(
			"invalid %s=%q: must be a number: %w",
			key,
			value,
			err,
		)
	}

	return number, nil
}
