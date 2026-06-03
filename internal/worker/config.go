package worker

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	RedisAddr       string
	DBConnStr     string
	ShardRegion     string
	ShardMode       string
	ShardRatingBand string
}

func LoadConfig() (*Config, error) {
	var missing []string
	required := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			missing = append(missing, key)
		}
		return v
	}

	dbHost := required("DB_HOST")
	dbUser := required("DB_USER")
	dbPassword := required("DB_PASSWORD")
	dbName := required("DB_NAME")
	dbPort := envOr("DB_PORT", "5432")

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}

	return &Config{
		RedisAddr:       envOr("REDIS_ADDR", "localhost:6379"),
		DBConnStr:     fmt.Sprintf("postgres://%s:%s@%s:%s/%s", dbUser, dbPassword, dbHost, dbPort, dbName),
		ShardRegion:     envOr("SHARD_REGION", "NA-E"),
		ShardMode:       envOr("SHARD_MODE", "ranked"),
		ShardRatingBand: envOr("SHARD_RATINGBAND", "1000-1200"),
	}, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
