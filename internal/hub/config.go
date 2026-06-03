package hub

import "os"

type Config struct {
	RedisAddr string
	HTTPAddr  string
}

func LoadConfig() (*Config, error) {
	return &Config{
		RedisAddr: envOr("REDIS_ADDR", "localhost:6379"),
		HTTPAddr:  envOr("HTTP_ADDR", ":8081"),
	}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
