package main

import (
	"context"
	"log/slog"
	"matchmaking/internal/publish"
	"matchmaking/internal/reconciler"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
)

func main() {
	cfg, err := reconciler.LoadConfig()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	pub := publish.New(rdb)
	r := reconciler.New(rdb, pub, cfg.Shards)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := r.Run(ctx); err != nil {
		slog.Error("reconciler exited", "err", err)
		os.Exit(1)
	}
}
