package worker

import (
	"context"
	"fmt"
	"matchmaking/internal/model"
	"matchmaking/internal/queue"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const testMatchSize = 24

func newTestSetup(t *testing.T) (*MatchMaker, *queue.Client, *redis.Client) {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	workerShard := model.Shard{Region: "test", Mode: "casual", SkillBand: "1000-1200"}
	clientShard := model.Shard{Region: "test", Mode: "casual"}
	return New(workerShard, rdb, testMatchSize, 1), queue.New(clientShard, rdb), rdb
}

func entry(playerID string, skill float64) *model.QueueEntry {
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

// enqueueN joins n players with skill 1050 (falls in "1000-1200" band).
func enqueueN(t *testing.T, q *queue.Client, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if err := q.Join(ctx, entry(fmt.Sprintf("p%d", i), 1050)); err != nil {
			t.Fatalf("Join p%d: %v", i, err)
		}
	}
}

// TestPopOnceFormsMatch verifies that accumulating matchSize players across
// multiple popOnce calls produces exactly one match in Redis.
func TestPopOnceFormsMatch(t *testing.T) {
	mm, q, rdb := newTestSetup(t)
	ctx := context.Background()

	enqueueN(t, q, testMatchSize) // 24 players, 16 popped on first call, 8 on second

	buf, err := mm.popOnce(ctx, nil)
	if err != nil {
		t.Fatalf("first popOnce: %v", err)
	}
	if len(buf) != 16 {
		t.Fatalf("expected buffer of 16 after first pop, got %d", len(buf))
	}
	if n := countMatches(t, rdb); n != 0 {
		t.Fatalf("expected no match yet, got %d", n)
	}

	buf, err = mm.popOnce(ctx, buf)
	if err != nil {
		t.Fatalf("second popOnce: %v", err)
	}
	if len(buf) != 0 {
		t.Fatalf("expected empty buffer after match commit, got %d", len(buf))
	}
	if n := countMatches(t, rdb); n != 1 {
		t.Fatalf("expected 1 match, got %d", n)
	}
}

// TestPopOnceBuffersWithoutMatch verifies that fewer than matchSize players
// do not produce a match.
func TestPopOnceBuffersWithoutMatch(t *testing.T) {
	mm, q, rdb := newTestSetup(t)
	ctx := context.Background()

	enqueueN(t, q, popBatchSize) // exactly one batch, not enough for a match

	buf, err := mm.popOnce(ctx, nil)
	if err != nil {
		t.Fatalf("popOnce: %v", err)
	}
	if len(buf) != popBatchSize {
		t.Fatalf("expected buffer of %d, got %d", popBatchSize, len(buf))
	}
	if n := countMatches(t, rdb); n != 0 {
		t.Fatalf("expected no match, got %d", n)
	}
}

// TestPopOnceSkipsCancelledEntry verifies that a player whose queue_entry was
// deleted by Cancel is silently skipped on pop, preventing a ghost match.
func TestPopOnceSkipsCancelledEntry(t *testing.T) {
	mm, q, rdb := newTestSetup(t)
	ctx := context.Background()

	// Enqueue matchSize players then cancel one — only matchSize-1 valid entries remain.
	enqueueN(t, q, testMatchSize)
	if err := q.Cancel(ctx, "p0"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Two popOnce calls drain the sorted set. p0 is popped but skipped (entry gone).
	buf, _ := mm.popOnce(ctx, nil)
	buf, _ = mm.popOnce(ctx, buf)

	// Buffer has only 23 valid players — not enough for a match.
	if n := countMatches(t, rdb); n != 0 {
		t.Fatalf("expected no match with only %d valid players, got %d match(es)", len(buf), n)
	}
}

// TestCommitMatchRejectsCancelledPlayer verifies that commitMatch returns false
// when a player appears in the cancelled set, and that the group is not written.
func TestCommitMatchRejectsCancelledPlayer(t *testing.T) {
	mm, q, rdb := newTestSetup(t)
	ctx := context.Background()

	enqueueN(t, q, testMatchSize)

	// Drain players into a buffer manually.
	buf, _ := mm.popOnce(ctx, nil)
	// Fetch remaining 8 without triggering a commit by injecting them directly.
	zs, _ := rdb.ZPopMin(ctx, "q:test:casual:1000-1200", 8).Result()
	for _, z := range zs {
		buf = append(buf, bufferedEntry{
			entry: &model.QueueEntry{PlayerID: z.Member.(string), Skill: 1050, EnqueuedAt: time.Now()},
			score: z.Score,
		})
	}

	// Cancel one player after they are already in the buffer (post-pop race).
	q.Cancel(ctx, "p0")

	group := buf[:testMatchSize]
	ok, err := mm.commitMatch(ctx, group)
	if err != nil {
		t.Fatalf("commitMatch: %v", err)
	}
	if ok {
		t.Fatal("expected commitMatch to return false for cancelled player")
	}
	if n := countMatches(t, rdb); n != 0 {
		t.Fatalf("expected no match written, got %d", n)
	}
}
