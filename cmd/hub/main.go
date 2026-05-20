package main

import (
	"context"
	"errors"
	"log/slog"
	"matchmaking/internal/hub"
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

	h := hub.New(rdb)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		if err := h.Run(ctx); err != nil {
			slog.Error("hub run loop exited", "err", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", h.ServeWS)

	server := &http.Server{Addr: ":8081", Handler: mux}
	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	slog.Info("hub listening", "addr", ":8081")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("hub server error", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
