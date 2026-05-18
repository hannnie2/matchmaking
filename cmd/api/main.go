package main

import (
	"context"
	"errors"
	"log/slog"
	"matchmaking/internal/api"
	"matchmaking/internal/model"
	"matchmaking/internal/worker"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: envOr("REDIS_ADDR", "localhost:6379"),
	})

	mm := worker.New(
		model.Shard{
			Region: envOr("SHARD_REGION", "NA-E"),
			Mode:   envOr("SHARD_MODE", "ranked"),
		},
		rdb,
		100, // skillWindow
		2,   // matchSize
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	mux := http.NewServeMux()
	api.NewHandler(mm).RegisterRoutes(mux)
	server := &http.Server{Addr: ":8080", Handler: mux}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	slog.Info("listening", "addr", ":8080")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
