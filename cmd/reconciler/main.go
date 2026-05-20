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
	rdb := redis.NewClient(&redis.Options{
		Addr: envOr("REDIS_ADDR", "localhost:6379"),
	})

	pub := publish.New(rdb)
	r := reconciler.New(rdb, pub)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := r.Run(ctx); err != nil {
		slog.Error("reconciler exited", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
