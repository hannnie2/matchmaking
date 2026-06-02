package main

import (
	"context"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/publish"
	"matchmaking/internal/store"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.New(ctx, envOr("POSTGRES_DSN", "postgres://matchmaking:matchmaking@localhost:5432/matchmaking"))
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	mm := worker.New(
		model.Shard{
			Region:    envOr("SHARD_REGION", "NA-E"),
			Mode:      envOr("SHARD_MODE", "ranked"),
			RatingBand: envOr("SHARD_RATINGBAND", "1000-1200"),
		},
		rdb,
		pub,
		24,
		st,
	)

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
