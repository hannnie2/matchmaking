package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"matchmaking/internal/model"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	lockTTL           = 5 * time.Second
	lockRenewInterval = 2 * time.Second
	tickInterval      = 500 * time.Millisecond
	matchTTL          = 10 * time.Minute
)

// lockRenewScript renews the shard lock TTL only if this worker still owns it.
var lockRenewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
`)

// lockReleaseScript deletes the shard lock only if this worker owns it.
var lockReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`)

// MatchMaker owns the in-memory pool for one shard and drives the tick loop.
// Join, Cancel, and GetMatch are safe to call from the API process since they
// only touch Redis; Run is called exclusively by the worker process.
type MatchMaker struct {
	// pool is exclusively owned by the worker goroutine; no mutex needed.
	pool map[string]*model.QueueEntry

	skillWindow float64
	matchSize   int
	shard       model.Shard
	rdb         *redis.Client
	workerID    string
}

func New(shard model.Shard, rdb *redis.Client, skillWindow float64, matchSize int) *MatchMaker {
	return &MatchMaker{
		pool:        make(map[string]*model.QueueEntry),
		skillWindow: skillWindow,
		matchSize:   matchSize,
		shard:       shard,
		rdb:         rdb,
		workerID:    newWorkerID(),
	}
}

func newWorkerID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

func (m *MatchMaker) queueKey() string {
	return fmt.Sprintf("queue:%s:%s", m.shard.Region, m.shard.Mode)
}

func (m *MatchMaker) lockKey() string {
	return fmt.Sprintf("shard_lock:%s:%s", m.shard.Region, m.shard.Mode)
}

func (m *MatchMaker) entryKey(playerID string) string {
	return "queue_entry:" + playerID
}

func (m *MatchMaker) matchKey(matchID string) string {
	return "match:" + matchID
}

// Join writes the queue entry to Redis. The worker picks it up on the next sync.
func (m *MatchMaker) Join(ctx context.Context, entry *model.QueueEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	pipe := m.rdb.Pipeline()
	pipe.Set(ctx, m.entryKey(entry.PlayerID), data, 0)
	pipe.ZAdd(ctx, m.queueKey(), redis.Z{
		Score:  float64(entry.EnqueuedAt.UnixMilli()),
		Member: entry.PlayerID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

// Cancel removes the player from Redis. The worker detects the removal on the next sync.
func (m *MatchMaker) Cancel(ctx context.Context, playerID string) error {
	pipe := m.rdb.Pipeline()
	pipe.ZRem(ctx, m.queueKey(), playerID)
	pipe.Del(ctx, m.entryKey(playerID))
	_, err := pipe.Exec(ctx)
	return err
}

// GetMatch reads a match record from Redis. Returns nil, nil if not found.
func (m *MatchMaker) GetMatch(ctx context.Context, matchID string) (*model.Match, error) {
	data, err := m.rdb.Get(ctx, m.matchKey(matchID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var match model.Match
	if err := json.Unmarshal(data, &match); err != nil {
		return nil, err
	}
	return &match, nil
}

// Run acquires the shard lock, hydrates the pool from Redis, then runs the
// lock renewer and tick loop under a shared errgroup context. If either
// goroutine fails (e.g. lock lost), the other is cancelled and Run returns.
func (m *MatchMaker) Run(ctx context.Context) error {
	if err := m.acquireShardLock(ctx); err != nil {
		return fmt.Errorf("acquire shard lock: %w", err)
	}
	defer m.releaseShardLock()

	if err := m.syncPool(ctx); err != nil {
		return fmt.Errorf("initial pool sync: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return m.renewLockLoop(ctx) })
	g.Go(func() error { return m.tickLoop(ctx) })
	return g.Wait()
}

func (m *MatchMaker) acquireShardLock(ctx context.Context) error {
	for {
		ok, err := m.rdb.SetNX(ctx, m.lockKey(), m.workerID, lockTTL).Result()
		if err != nil {
			return err
		}
		if ok {
			slog.Info("acquired shard lock", "shard", m.shard)
			return nil
		}
		slog.Info("waiting for shard lock", "shard", m.shard)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (m *MatchMaker) renewLockLoop(ctx context.Context) error {
	ticker := time.NewTicker(lockRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			res, err := lockRenewScript.Run(
				ctx, m.rdb, []string{m.lockKey()},
				m.workerID, int(lockTTL.Seconds()),
			).Int()
			if err != nil {
				return fmt.Errorf("lock renew: %w", err)
			}
			if res == 0 {
				return fmt.Errorf("shard lock lost: %s", m.lockKey())
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *MatchMaker) releaseShardLock() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := lockReleaseScript.Run(ctx, m.rdb, []string{m.lockKey()}, m.workerID).Err(); err != nil {
		slog.Error("failed to release shard lock", "err", err)
	}
	slog.Info("released shard lock", "shard", m.shard)
}

// syncPool brings the in-memory pool in line with the Redis sorted set:
// new entries are fetched and added; entries absent from Redis are removed.
func (m *MatchMaker) syncPool(ctx context.Context) error {
	ids, err := m.rdb.ZRange(ctx, m.queueKey(), 0, -1).Result()
	if err != nil {
		return err
	}

	inRedis := make(map[string]bool, len(ids))
	for _, id := range ids {
		inRedis[id] = true
	}

	for playerID := range m.pool {
		if !inRedis[playerID] {
			delete(m.pool, playerID)
		}
	}

	var newIDs []string
	for _, id := range ids {
		if _, exists := m.pool[id]; !exists {
			newIDs = append(newIDs, id)
		}
	}
	if len(newIDs) == 0 {
		return nil
	}

	pipe := m.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(newIDs))
	for i, id := range newIDs {
		cmds[i] = pipe.Get(ctx, m.entryKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}

	for i, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			continue // entry cancelled between ZRANGE and pipeline fetch
		}
		var entry model.QueueEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			slog.Error("malformed queue entry", "player_id", newIDs[i], "err", err)
			continue
		}
		m.pool[entry.PlayerID] = &entry
	}
	return nil
}

func (m *MatchMaker) tickLoop(ctx context.Context) error {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := m.syncPool(ctx); err != nil {
				slog.Error("pool sync failed", "err", err)
			}
			if err := m.tick(ctx); err != nil {
				slog.Error("tick failed", "err", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *MatchMaker) tick(ctx context.Context) error {
	entries := make([]*model.QueueEntry, 0, len(m.pool))
	for _, e := range m.pool {
		entries = append(entries, e)
	}

	used := make(map[string]bool)
	for i, anchor := range entries {
		if used[anchor.PlayerID] {
			continue
		}
		group := []*model.QueueEntry{anchor}
		for _, candidate := range entries[i+1:] {
			if used[candidate.PlayerID] {
				continue
			}
			if math.Abs(anchor.Skill-candidate.Skill) <= m.skillWindow {
				group = append(group, candidate)
			}
			if len(group) == m.matchSize {
				break
			}
		}
		if len(group) < m.matchSize {
			continue
		}
		if err := m.formMatch(ctx, group, used); err != nil {
			slog.Error("failed to form match", "err", err)
		}
	}
	return nil
}

func (m *MatchMaker) formMatch(ctx context.Context, group []*model.QueueEntry, used map[string]bool) error {
	ids := make([]string, len(group))
	for i, e := range group {
		ids[i] = e.PlayerID
	}

	match := &model.Match{
		ID:        fmt.Sprintf("match-%d", time.Now().UnixNano()),
		PlayerIDs: ids,
		Status:    model.MatchStatusForming,
		FormedAt:  time.Now(),
	}

	matchData, err := json.Marshal(match)
	if err != nil {
		return err
	}

	// Remove players from queue and write match record atomically in one pipeline.
	pipe := m.rdb.Pipeline()
	for _, e := range group {
		pipe.ZRem(ctx, m.queueKey(), e.PlayerID)
		pipe.Del(ctx, m.entryKey(e.PlayerID))
	}
	pipe.Set(ctx, m.matchKey(match.ID), matchData, matchTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	for _, e := range group {
		used[e.PlayerID] = true
		delete(m.pool, e.PlayerID)
	}

	slog.Info("match formed", "match_id", match.ID, "players", ids)
	return nil
}
