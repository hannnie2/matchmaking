package main

import (
	"context"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/publish"
	"matchmaking/internal/reconciler"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: envOr("REDIS_ADDR", "localhost:6379"),
	})

	pub := publish.New(rdb)
	shards := parseShards(os.Getenv("SHARD_LIST"))
	r := reconciler.New(rdb, pub, shards)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := r.Run(ctx); err != nil {
		slog.Error("reconciler exited", "err", err)
		os.Exit(1)
	}
}

// parseShards parses a comma-separated list of "region/mode/ratingBand" entries.
// Example: "NA-E/ranked/1000-1200,NA-E/ranked/1200-1400,EU-W/ranked/1000-1200"
func parseShards(list string) []model.Shard {
	if list == "" {
		return nil
	}
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
