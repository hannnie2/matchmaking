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

	if err := c.Join(ctx, &model.QueueEntry{PlayerID: "p1", Skill: 1050, EnqueuedAt: time.Now()}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	// Skill 1050 → band "1000-1200"
	shard := model.Shard{Region: "test", Mode: "casual", SkillBand: "1000-1200"}
	n, err := rdb.ZCard(ctx, rediskeys.Queue(shard)).Result()
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

func TestCancelDeletesEntry(t *testing.T) {
	c, rdb := newTestClient(t)
	ctx := context.Background()

	c.Join(ctx, &model.QueueEntry{PlayerID: "p1", Skill: 1050, EnqueuedAt: time.Now()})
	c.Cancel(ctx, "p1")

	// queue_entry must be deleted so the worker skips the player on next pop
	exists, _ := rdb.Exists(ctx, rediskeys.QueueEntry("p1")).Result()
	if exists != 0 {
		t.Fatal("expected entry key to be deleted after cancel")
	}

	// player must appear in the cancelled set
	shard := model.Shard{Region: "test", Mode: "casual"}
	isMember, _ := rdb.SIsMember(ctx, rediskeys.Cancelled(shard), "p1").Result()
	if !isMember {
		t.Fatal("expected p1 in cancelled set")
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

// TestJoinRejectedAfterCancel covers the Cancel-before-Join race: a player who
// calls Cancel must not be re-admitted to the queue by a concurrent Join.
func TestJoinRejectedAfterCancel(t *testing.T) {
	c, rdb := newTestClient(t)
	ctx := context.Background()

	if err := c.Cancel(ctx, "p1"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	err := c.Join(ctx, &model.QueueEntry{PlayerID: "p1", Skill: 1050, EnqueuedAt: time.Now()})
	if err != ErrPlayerCancelled {
		t.Fatalf("expected ErrPlayerCancelled, got %v", err)
	}

	shard := model.Shard{Region: "test", Mode: "casual", SkillBand: "1000-1200"}
	n, _ := rdb.ZCard(ctx, rediskeys.Queue(shard)).Result()
	if n != 0 {
		t.Fatalf("expected empty sorted set, got %d", n)
	}
	exists, _ := rdb.Exists(ctx, rediskeys.QueueEntry("p1")).Result()
	if exists != 0 {
		t.Fatal("expected no queue entry key")
	}
}

func TestComputeSkillBand(t *testing.T) {
	cases := []struct {
		skill float64
		want  string
	}{
		{999, "1000-1200"},
		{1000, "1000-1200"},
		{1199, "1000-1200"},
		{1200, "1200-1400"},
		{1500, "1400-1600"},
		{2399, "2200-2400"},
		{2400, "2200-2400"},
		{9999, "2200-2400"},
	}
	for _, tc := range cases {
		if got := computeSkillBand(tc.skill); got != tc.want {
			t.Errorf("computeSkillBand(%.0f) = %q, want %q", tc.skill, got, tc.want)
		}
	}
}
