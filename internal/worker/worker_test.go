package worker

import (
	"context"
	"matchmaking/internal/model"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestMM(t *testing.T) (*MatchMaker, *redis.Client) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	mm := New(model.Shard{Region: "test", Mode: "casual"}, rdb, 100, 2)
	return mm, rdb
}

func queueEntry(playerID string, skill float64) *model.QueueEntry {
	return &model.QueueEntry{PlayerID: playerID, Skill: skill, EnqueuedAt: time.Now()}
}

func countMatches(t *testing.T, rdb *redis.Client) int {
	t.Helper()
	keys, err := rdb.Keys(context.Background(), "match:*").Result()
	if err != nil {
		t.Fatalf("counting matches: %v", err)
	}
	return len(keys)
}

func TestTickFormsMatch(t *testing.T) {
	mm, rdb := newTestMM(t)
	ctx := context.Background()

	mm.Join(ctx, queueEntry("p1", 1000))
	mm.Join(ctx, queueEntry("p2", 1050))
	mm.syncPool(ctx)
	mm.tick(ctx)

	if len(mm.pool) != 0 {
		t.Fatalf("expected empty pool after match, got %d entries", len(mm.pool))
	}
	if n := countMatches(t, rdb); n != 1 {
		t.Fatalf("expected 1 match in Redis, got %d", n)
	}
}

func TestTickSkillWindowExcludes(t *testing.T) {
	mm, rdb := newTestMM(t)
	ctx := context.Background()

	mm.Join(ctx, queueEntry("p1", 1000))
	mm.Join(ctx, queueEntry("p2", 1200))
	mm.syncPool(ctx)
	mm.tick(ctx)

	if len(mm.pool) != 2 {
		t.Fatalf("expected pool unchanged, got %d entries", len(mm.pool))
	}
	if n := countMatches(t, rdb); n != 0 {
		t.Fatalf("expected no matches, got %d", n)
	}
}

func TestTickFormsMultipleMatches(t *testing.T) {
	mm, rdb := newTestMM(t)
	ctx := context.Background()

	mm.Join(ctx, queueEntry("p1", 1000))
	mm.Join(ctx, queueEntry("p2", 1050))
	mm.Join(ctx, queueEntry("p3", 1500))
	mm.Join(ctx, queueEntry("p4", 1520))
	mm.syncPool(ctx)
	mm.tick(ctx)

	if len(mm.pool) != 0 {
		t.Fatalf("expected empty pool, got %d entries", len(mm.pool))
	}
	if n := countMatches(t, rdb); n != 2 {
		t.Fatalf("expected 2 matches in Redis, got %d", n)
	}
}

func TestCancelRemovesFromPool(t *testing.T) {
	mm, rdb := newTestMM(t)
	ctx := context.Background()

	mm.Join(ctx, queueEntry("p1", 1000))
	mm.Cancel(ctx, "p1")
	mm.syncPool(ctx)
	mm.tick(ctx)

	if n := countMatches(t, rdb); n != 0 {
		t.Fatal("expected no match after cancel")
	}
}

func TestSyncPoolPicksUpCancellation(t *testing.T) {
	mm, _ := newTestMM(t)
	ctx := context.Background()

	mm.Join(ctx, queueEntry("p1", 1000))
	mm.Join(ctx, queueEntry("p2", 1050))
	mm.syncPool(ctx)

	if len(mm.pool) != 2 {
		t.Fatalf("expected 2 entries in pool, got %d", len(mm.pool))
	}

	mm.Cancel(ctx, "p1")
	mm.syncPool(ctx)

	if len(mm.pool) != 1 {
		t.Fatalf("expected 1 entry after cancellation sync, got %d", len(mm.pool))
	}
	if _, exists := mm.pool["p1"]; exists {
		t.Fatal("p1 should have been removed from pool")
	}
}

func TestWorkerSurvivesRestart(t *testing.T) {
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	ctx := context.Background()

	mm1 := New(model.Shard{Region: "test", Mode: "casual"}, rdb, 100, 2)
	mm1.Join(ctx, queueEntry("p1", 1000))
	mm1.Join(ctx, queueEntry("p2", 1050))

	mm2 := New(model.Shard{Region: "test", Mode: "casual"}, rdb, 100, 2)
	mm2.syncPool(ctx)

	if len(mm2.pool) != 2 {
		t.Fatalf("restarted worker expected 2 entries, got %d", len(mm2.pool))
	}

	mm2.tick(ctx)

	if n := countMatches(t, rdb); n != 1 {
		t.Fatalf("restarted worker expected 1 match in Redis, got %d", n)
	}
}

func TestGetMatch(t *testing.T) {
	mm, rdb := newTestMM(t)
	ctx := context.Background()

	mm.Join(ctx, queueEntry("p1", 1000))
	mm.Join(ctx, queueEntry("p2", 1050))
	mm.syncPool(ctx)
	mm.tick(ctx)

	keys, _ := rdb.Keys(ctx, "match:*").Result()
	if len(keys) != 1 {
		t.Fatalf("expected 1 match key, got %d", len(keys))
	}
	matchID := strings.TrimPrefix(keys[0], "match:")

	match, err := mm.GetMatch(ctx, matchID)
	if err != nil {
		t.Fatalf("GetMatch error: %v", err)
	}
	if match == nil {
		t.Fatal("expected match, got nil")
	}
	if len(match.PlayerIDs) != 2 {
		t.Fatalf("expected 2 players in match, got %d", len(match.PlayerIDs))
	}
}
