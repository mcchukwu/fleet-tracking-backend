package config

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
	"github.com/mcchukwu/fleet-tracking-backend/pkg/logger"
)

type Config struct {
	AppPort   string `env:"APP_PORT"`
	DBURL     string `env:"DB_URL"`
	RedisAddr string `env:"REDIS_ADDR"`
}

// Load loads the configuration from the environment variables
// and returns the configuration struct
func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		logger.Error("failed to load environment variables: %s: %s", err, err.Error())
	}

	return &Config{
		AppPort:   getEnv("APP_PORT", ""),
		DBURL:     getEnv("DB_URL", ""),
		RedisAddr: getEnv("REDIS_ADDR", ""),
	}
}

// Validate validates the configuration struct
func Validate(c *Config) error {
	if c.AppPort == "" {
		return errors.New("app port is required")
	}
	if c.DBURL == "" {
		return errors.New("database url is required")
	}
	if c.RedisAddr == "" {
		return errors.New("redis address is required")
	}

	return nil
}

// getEnv gets the value of the enviroment variable key
// returns the value or a specified fallback value
func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}
