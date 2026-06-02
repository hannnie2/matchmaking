package reconciler

import (
	"context"
	"fmt"
	"log/slog"
	"matchmaking/internal/matchops"
	"matchmaking/internal/publish"
	"matchmaking/internal/rediskeys"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	confirmationTimeout = 30 * time.Second
	tickInterval        = 5 * time.Second
)

type Reconciler struct {
	rdb *redis.Client
	pub *publish.Publisher
}

func New(rdb *redis.Client, pub *publish.Publisher) *Reconciler {
	return &Reconciler{rdb: rdb, pub: pub}
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	slog.Info("reconciler started")
	for {
		select {
		case <-ticker.C:
			r.processTimedOut(ctx)
		case <-ctx.Done():
			return nil
		}
	}
}

func (r *Reconciler) processTimedOut(ctx context.Context) {
	deadline := float64(time.Now().Add(-confirmationTimeout).UnixMilli())
	matchIDs, err := r.rdb.ZRangeByScore(ctx, rediskeys.FormingMatches(), &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%f", deadline),
	}).Result()
	if err != nil {
		slog.Error("reconciler: failed to query forming matches", "err", err)
		return
	}

	for _, matchID := range matchIDs {
		slog.Info("reconciler: dissolving timed-out match", "match_id", matchID)
		matchops.Dissolve(ctx, r.rdb, r.pub, nil, matchID, "")
	}
}
