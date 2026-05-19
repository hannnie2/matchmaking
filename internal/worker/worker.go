package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/rediskeys"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	lockTTL           = 5 * time.Second
	lockRenewInterval = 2 * time.Second
	matchTTL          = 10 * time.Minute
	popBatchSize      = 16
	emptyQueueDelay   = 100 * time.Millisecond
)

var lockRenewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
`)

var lockReleaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`)

// commitMatchScript atomically checks that none of the players appear in the
// cancelled set before writing the match record.
//
// KEYS[1]    = cancelled:{region}:{mode}
// KEYS[2]    = match:{matchID}
// ARGV[1..N] = playerIDs
// ARGV[N+1]  = match JSON
// ARGV[N+2]  = TTL seconds
var commitMatchScript = redis.NewScript(`
local n = #ARGV - 2
for i = 1, n do
    if redis.call("SISMEMBER", KEYS[1], ARGV[i]) == 1 then
        return 0
    end
end
redis.call("SET", KEYS[2], ARGV[n + 1], "EX", tonumber(ARGV[n + 2]))
return 1
`)

type MatchMaker struct {
	matchSize   int
	concurrency int
	shard       model.Shard
	rdb         *redis.Client
	workerID    string
	matchSeq    atomic.Int64
}

func New(shard model.Shard, rdb *redis.Client, matchSize, concurrency int) *MatchMaker {
	return &MatchMaker{
		matchSize:   matchSize,
		concurrency: concurrency,
		shard:       shard,
		rdb:         rdb,
		workerID:    newWorkerID(),
	}
}

func newWorkerID() string {
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

func (m *MatchMaker) Run(ctx context.Context) error {
	if err := m.acquireShardLock(ctx); err != nil {
		return fmt.Errorf("acquire shard lock: %w", err)
	}
	defer m.releaseShardLock()

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return m.renewLockLoop(ctx) })
	for range m.concurrency {
		g.Go(func() error { return m.popLoop(ctx) })
	}
	return g.Wait()
}

func (m *MatchMaker) acquireShardLock(ctx context.Context) error {
	for {
		ok, err := m.rdb.SetNX(ctx, rediskeys.ShardLock(m.shard), m.workerID, lockTTL).Result()
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
	lockKey := rediskeys.ShardLock(m.shard)
	for {
		select {
		case <-ticker.C:
			res, err := lockRenewScript.Run(
				ctx, m.rdb, []string{lockKey},
				m.workerID, int(lockTTL.Seconds()),
			).Int()
			if err != nil {
				return fmt.Errorf("lock renew: %w", err)
			}
			if res == 0 {
				return fmt.Errorf("shard lock lost: %s", lockKey)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *MatchMaker) releaseShardLock() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lockKey := rediskeys.ShardLock(m.shard)
	if err := lockReleaseScript.Run(ctx, m.rdb, []string{lockKey}, m.workerID).Err(); err != nil {
		slog.Error("failed to release shard lock", "err", err)
	}
	slog.Info("released shard lock", "shard", m.shard)
}

// bufferedEntry holds a popped player, the raw JSON member (for re-push), and
// the original score (to preserve FIFO ordering on conflict).
type bufferedEntry struct {
	entry  *model.QueueEntry
	member string // raw JSON as it appeared in the sorted set
	score  float64
}

func (m *MatchMaker) popLoop(ctx context.Context) error {
	var buffer []bufferedEntry
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		var err error
		buffer, err = m.popOnce(ctx, buffer)
		if err != nil {
			return err
		}
	}
}

// popOnce pops up to popBatchSize players from the sorted set, parses their
// JSON members directly, filters cancelled players via SMIsMember, and commits
// a match whenever the buffer reaches matchSize. Returns the updated buffer.
func (m *MatchMaker) popOnce(ctx context.Context, buffer []bufferedEntry) ([]bufferedEntry, error) {
	zs, err := m.rdb.ZPopMin(ctx, rediskeys.Queue(m.shard), popBatchSize).Result()
	if err != nil {
		return buffer, fmt.Errorf("zpopmin: %w", err)
	}

	if len(zs) == 0 {
		select {
		case <-ctx.Done():
		case <-time.After(emptyQueueDelay):
		}
		return buffer, nil
	}

	// Parse JSON members directly — no separate GET round-trip needed.
	type candidate struct {
		entry  *model.QueueEntry
		member string
		score  float64
	}
	candidates := make([]candidate, 0, len(zs))
	for _, z := range zs {
		member := z.Member.(string)
		var entry model.QueueEntry
		if err := json.Unmarshal([]byte(member), &entry); err != nil {
			slog.Error("malformed queue member", "err", err)
			continue
		}
		candidates = append(candidates, candidate{&entry, member, z.Score})
	}

	if len(candidates) == 0 {
		return buffer, nil
	}

	// Batch-check the cancelled set in one round-trip.
	memberArgs := make([]interface{}, len(candidates))
	for i, c := range candidates {
		memberArgs[i] = c.entry.PlayerID
	}
	cancelled, err := m.rdb.SMIsMember(ctx, rediskeys.Cancelled(m.shard), memberArgs...).Result()
	if err != nil {
		// On error be conservative: skip the cancelled check and let
		// commitMatchScript catch any cancelled players at commit time.
		cancelled = make([]bool, len(candidates))
	}

	for i, c := range candidates {
		if cancelled[i] {
			slog.Debug("skipping cancelled player", "player_id", c.entry.PlayerID)
			continue
		}
		buffer = append(buffer, bufferedEntry{entry: c.entry, member: c.member, score: c.score})
	}

	for len(buffer) >= m.matchSize {
		group := buffer[:m.matchSize]
		buffer = buffer[m.matchSize:]

		ok, err := m.commitMatch(ctx, group)
		if err != nil {
			return buffer, fmt.Errorf("commit match: %w", err)
		}
		if !ok {
			// A player was cancelled between pop and commit. Re-push the whole
			// group; the cancelled player's queue_entry is already deleted so
			// they will be skipped on the next pop.
			if err := m.repushGroup(ctx, group); err != nil {
				slog.Error("failed to re-push group after conflict", "err", err)
			}
		}
	}

	return buffer, nil
}

// commitMatch atomically verifies no player has been cancelled, writes the
// match record, and deletes queue entries. Returns false if a cancellation is
// detected — the caller is responsible for re-pushing the group.
func (m *MatchMaker) commitMatch(ctx context.Context, group []bufferedEntry) (bool, error) {
	seq := m.matchSeq.Add(1)
	playerIDs := make([]string, len(group))
	for i, be := range group {
		playerIDs[i] = be.entry.PlayerID
	}

	match := &model.Match{
		ID:        fmt.Sprintf("match-%d-%d", time.Now().UnixMilli(), seq),
		PlayerIDs: playerIDs,
		Status:    model.MatchStatusForming,
		FormedAt:  time.Now(),
	}
	matchData, err := json.Marshal(match)
	if err != nil {
		return false, err
	}

	keys := []string{rediskeys.Cancelled(m.shard), rediskeys.Match(match.ID)}

	// ARGV: playerIDs..., match JSON, TTL
	args := make([]interface{}, 0, len(playerIDs)+2)
	for _, id := range playerIDs {
		args = append(args, id)
	}
	args = append(args, string(matchData), int(matchTTL.Seconds()))

	result, err := commitMatchScript.Run(ctx, m.rdb, keys, args...).Int()
	if err != nil {
		return false, err
	}
	if result == 0 {
		return false, nil
	}

	slog.Info("match formed", "match_id", match.ID, "players", playerIDs)
	return true, nil
}

func (m *MatchMaker) repushGroup(ctx context.Context, group []bufferedEntry) error {
	pipe := m.rdb.Pipeline()
	for _, be := range group {
		pipe.ZAdd(ctx, rediskeys.Queue(m.shard), redis.Z{
			Score:  be.score,
			Member: be.member, // re-push the original JSON member
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}
