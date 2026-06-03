package reconciler

import (
	"fmt"
	"log/slog"
	"matchmaking/internal/model"
	"os"
	"strings"
)

type Config struct {
	RedisAddr string
	Shards    []model.Shard
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

	shardList := required("SHARD_LIST")

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}

	return &Config{
		RedisAddr: envOr("REDIS_ADDR", "localhost:6379"),
		Shards:    parseShards(shardList),
	}, nil
}

// parseShards parses a comma-separated list of "region/mode/ratingBand" entries.
// Example: "NA-E/ranked/1000-1200,EU-W/ranked/1200-1400"
func parseShards(list string) []model.Shard {
	var shards []model.Shard
	for _, entry := range strings.Split(list, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "/", 3)
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			slog.Warn("invalid shard in SHARD_LIST, skipping", "entry", entry)
			continue
		}
		shards = append(shards, model.Shard{Region: parts[0], Mode: parts[1], RatingBand: parts[2]})
	}
	return shards
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
