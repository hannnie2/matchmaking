package queue

import (
	"context"
	"matchmaking/internal/model"
	"matchmaking/internal/rediskeys"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) (*Client, *redis.Client) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	return New(model.Shard{Region: "test", Mode: "casual"}, rdb), rdb
}

func TestJoinWritesToRedis(t *testing.T) {
	c, rdb := newTestClient(t)
	ctx := context.Background()

	if err := c.Join(ctx, &model.QueueEntry{PlayerID: "p1", Skill: 1000, EnqueuedAt: time.Now()}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	n, err := rdb.ZCard(ctx, rediskeys.Queue(c.shard)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 member in sorted set, got %d", n)
	}

	exists, err := rdb.Exists(ctx, rediskeys.QueueEntry("p1")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if exists == 0 {
		t.Fatal("expected entry key to exist")
	}
}

func TestCancelRemovesFromRedis(t *testing.T) {
	c, rdb := newTestClient(t)
	ctx := context.Background()

	c.Join(ctx, &model.QueueEntry{PlayerID: "p1", Skill: 1000, EnqueuedAt: time.Now()})
	c.Cancel(ctx, "p1")

	n, _ := rdb.ZCard(ctx, rediskeys.Queue(c.shard)).Result()
	if n != 0 {
		t.Fatalf("expected empty sorted set after cancel, got %d", n)
	}
	exists, _ := rdb.Exists(ctx, rediskeys.QueueEntry("p1")).Result()
	if exists != 0 {
		t.Fatal("expected entry key to be deleted")
	}
}

func TestGetMatchNotFound(t *testing.T) {
	c, _ := newTestClient(t)
	match, err := c.GetMatch(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if match != nil {
		t.Fatal("expected nil for missing match")
	}
}
