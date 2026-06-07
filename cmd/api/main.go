package main

import (
	"context"
	"errors"
	"log/slog"
	"matchmaking/internal/api"
	"matchmaking/internal/publish"
	"matchmaking/internal/queue"
	"matchmaking/internal/store"
	"net/http"
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

	cfg, err := api.LoadConfig()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	q := queue.New(rdb)
	pub := publish.New(rdb)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	st, err := store.New(ctx, cfg.DBConnStr)
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	mux := http.NewServeMux()
	api.NewHandler(q, rdb, pub, st).RegisterRoutes(mux)
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}

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

