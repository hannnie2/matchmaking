package main

import (
	"context"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/publish"
	"matchmaking/internal/worker"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: envOr("REDIS_ADDR", "localhost:6379"),
	})

	pub := publish.New(rdb)
	mm := worker.New(
		model.Shard{
			Region:    envOr("SHARD_REGION", "NA-E"),
			Mode:      envOr("SHARD_MODE", "ranked"),
			SkillBand: envOr("SHARD_SKILLBAND", "1000-1200"),
		},
		rdb,
		pub,
		24,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := mm.Run(ctx); err != nil {
		slog.Error("worker exited", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
