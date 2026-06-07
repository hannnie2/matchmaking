package main

import (
	"errors"
	"log/slog"
	"os"

	"github.com/joho/godotenv"
)

// rating consumes match completion events and updates TrueSkill ratings (Milestone 6).
func main() {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("godotenv", "err", err)
	}

	slog.Info("rating service not yet implemented")
}
