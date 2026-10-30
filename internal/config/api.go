package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type API struct {
	Addr          string
	PGDSN         string
	RedisAddr     string
	RedisPassword string
	SessionTTL    time.Duration
	CookieSecure  bool
	StaticDir     string
	WSOrigins     []string
}

func LoadAPI() (API, error) {
	if err := loadEnv(); err != nil {
		return API{}, err
	}

	ttl, err := getenvDuration("SESSION_TTL", 12*time.Hour)
	if err != nil {
		return API{}, err
	}
	secure, err := getenvBool("COOKIE_SECURE", true)
	if err != nil {
		return API{}, err
	}

	c := API{
		Addr:          getenv("API_ADDR", ""),
		PGDSN:         getenv("PG_DSN", ""),
		RedisAddr:     getenv("REDIS_ADDR", ""),
		RedisPassword: getenv("REDIS_PASSWORD", ""),
		SessionTTL:    ttl,
		CookieSecure:  secure,
		StaticDir:     getenv("STATIC_DIR", ""),
		WSOrigins:     splitList(getenv("WS_ALLOWED_ORIGINS", "localhost:*,127.0.0.1:*")),
	}
	if c.Addr == "" {
		return API{}, fmt.Errorf("API_ADDR is required")
	}
	if c.PGDSN == "" {
		return API{}, fmt.Errorf("PG_DSN is required")
	}
	if c.RedisAddr == "" {
		return API{}, fmt.Errorf("REDIS_ADDR is required")
	}
	return c, nil
}

func getenvBool(key string, fallback bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	return strconv.ParseBool(v)
}

func splitList(raw string) []string {
	parts := strings.Split(raw, ",")
	out := parts[:0]
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// LoadDatabase is for one-shot commands that only need Postgres.
func LoadDatabase() (string, error) {
	if err := loadEnv(); err != nil {
		return "", err
	}
	dsn := getenv("PG_DSN", "")
	if dsn == "" {
		return "", fmt.Errorf("PG_DSN is required")
	}
	return dsn, nil
}
