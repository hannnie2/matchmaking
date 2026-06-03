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
	cfg, err := hub.LoadConfig()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
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

	server := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	slog.Info("hub listening", "addr", cfg.HTTPAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("hub server error", "err", err)
		os.Exit(1)
	}
}
