package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/rediskeys"
	"os"
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
// cancelled set before writing the match record and deleting queue entries.
//
// KEYS[1]        = cancelled:{region}:{mode}
// KEYS[2]        = match:{matchID}
// KEYS[3..N+2]   = queue_entry:{playerID} for each of N players
// ARGV[1..N]     = playerIDs
// ARGV[N+1]      = match JSON
// ARGV[N+2]      = TTL seconds
var commitMatchScript = redis.NewScript(`
local n = #ARGV - 2
for i = 1, n do
    if redis.call("SISMEMBER", KEYS[1], ARGV[i]) == 1 then
        return 0
    end
end
redis.call("SET", KEYS[2], ARGV[n + 1], "EX", tonumber(ARGV[n + 2]))
for i = 1, n do
    redis.call("DEL", KEYS[i + 2])
end
return 1
`)

type MatchMaker struct {
	matchSize int
	shard     model.Shard
	rdb       *redis.Client
	workerID  string
	matchSeq  int64
}

func New(shard model.Shard, rdb *redis.Client, matchSize int) *MatchMaker {
	return &MatchMaker{
		matchSize: matchSize,
		shard:     shard,
		rdb:       rdb,
		workerID:  newWorkerID(),
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
	g.Go(func() error { return m.popLoop(ctx) })
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

// bufferedEntry holds a popped player together with their original enqueue
// score so they can be re-pushed with FIFO ordering preserved on conflict.
type bufferedEntry struct {
	entry *model.QueueEntry
	score float64
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

// popOnce pops up to popBatchSize players from the sorted set, appends valid
// entries to buffer, and commits a match whenever the buffer reaches matchSize.
// It returns the updated buffer.
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

	pipe := m.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(zs))
	for i, z := range zs {
		cmds[i] = pipe.Get(ctx, rediskeys.QueueEntry(z.Member.(string)))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return buffer, fmt.Errorf("fetch entries: %w", err)
	}

	for i, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			// nil result means Cancel deleted the entry; discard this player
			slog.Debug("skipping cancelled player", "player_id", zs[i].Member)
			continue
		}
		var entry model.QueueEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			slog.Error("malformed queue entry", "player_id", zs[i].Member, "err", err)
			continue
		}
		buffer = append(buffer, bufferedEntry{entry: &entry, score: zs[i].Score})
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
	m.matchSeq++
	playerIDs := make([]string, len(group))
	for i, be := range group {
		playerIDs[i] = be.entry.PlayerID
	}

	match := &model.Match{
		ID:        fmt.Sprintf("match-%d-%d", time.Now().UnixMilli(), m.matchSeq),
		PlayerIDs: playerIDs,
		Status:    model.MatchStatusForming,
		FormedAt:  time.Now(),
	}
	matchData, err := json.Marshal(match)
	if err != nil {
		return false, err
	}

	// KEYS: cancelled set, match key, then one queue_entry key per player
	keys := make([]string, 0, 2+len(playerIDs))
	keys = append(keys, rediskeys.Cancelled(m.shard), rediskeys.Match(match.ID))
	for _, id := range playerIDs {
		keys = append(keys, rediskeys.QueueEntry(id))
	}

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
			Member: be.entry.PlayerID,
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}
