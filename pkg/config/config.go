package config

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/mcchukwu/fleet-tracking-backend/pkg/logger"
)

// Create a configuration struct
type Config struct {
	AppName            string        `env:"APP_NAME"`
	AppPort            string        `env:"APP_PORT"`
	AppEnv             string        `env:"APP_ENV"`
	DBURL              string        `env:"DB_URL"`
	RedisAddr          string        `env:"REDIS_ADDR"`
	AccessTokenTTL     time.Duration `env:"ACCESS_TOKEN_TTL"`
	RefreshTokenTTL    time.Duration `env:"REFRESH_TOKEN_TTL"`
	CORSAllowedOrigins []string      `env:"CORS_ALLOWED_ORIGINS"`
}

// Load loads the configuration from the environment variables and returns the configuration struct
func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		logger.Error("Failed to load environment variables")
	}

	accessTokenTTL, err := time.ParseDuration(getEnv("JWT_ACCESS_TTL", "10m"))
	if err != nil {
		logger.Error("Failed to parse access token TTL")
	}

	refreshTokenTTL, err := time.ParseDuration(getEnv("JWT_REFRESH_TTL", "720h"))
	if err != nil {
		logger.Error("Failed to parse refresh token TTL")
	}

	return &Config{
		AppName:            getEnv("APP_NAME", ""),
		AppPort:            getEnv("APP_PORT", ""),
		AppEnv:             getEnv("APP_ENV", ""),
		DBURL:              getEnv("DB_URL", ""),
		RedisAddr:          getEnv("REDIS_ADDR", ""),
		AccessTokenTTL:     accessTokenTTL,
		RefreshTokenTTL:    refreshTokenTTL,
		CORSAllowedOrigins: strings.Split(getEnv("CORS_ALLOWED_ORIGINS", ""), ","),
	}
}

// Validate validates the configuration struct
func Validate(c *Config) error {
	if c.AppName == "" {
		return errors.New("app name is required")
	}
	if c.AppPort == "" {
		return errors.New("app port is required")
	}
	if c.AppEnv != "production" && c.AppEnv != "development" {
		return errors.New("invalid app env")
	}
	if c.DBURL == "" {
		return errors.New("database url is required")
	}
	if len(c.CORSAllowedOrigins) == 0 {
		return errors.New("cors allowed origins is required")
	}
	if c.RedisAddr == "" {
		return errors.New("redis address is required")
	}

	return nil
}

// getEnv gets the value of the enviroment variable key returns the value or a specified fallback value
func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
