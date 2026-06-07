package main

import (
	"context"
	"errors"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/publish"
	"matchmaking/internal/store"
	"matchmaking/internal/worker"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("godotenv", "err", err)
	}

	cfg, err := worker.LoadConfig()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	pub := publish.New(rdb)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.New(ctx, cfg.DBConnStr)
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	mm := worker.New(
		model.Shard{
			Region:     cfg.ShardRegion,
			Mode:       cfg.ShardMode,
			RatingBand: cfg.ShardRatingBand,
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
