package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/publish"
	"matchmaking/internal/rediskeys"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

const (
	lockTTL               = 5 * time.Second
	lockRenewInterval     = 2 * time.Second
	matchTTL              = 10 * time.Minute
	popBatchSize          = 24
	emptyQueueDelay       = 100 * time.Millisecond
	ratingDiffAllowance   = 50
	maxRatingDiff         = 300
	ratingExpandPerSecond = 5 // rating window widens by 5 per second of wait
	streamGroup           = "pg-writer"
	streamMaxLen          = 1000
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

// popToProcessingScript atomically pops up to ARGV[1] entries from the queue
// sorted set (KEYS[1]) and writes them into the processing sorted set (KEYS[2])
// with their original scores, so a crashed worker's in-flight entries can be
// recovered by the standby without data loss.
var popToProcessingScript = redis.NewScript(`
local res = redis.call("ZPOPMIN", KEYS[1], ARGV[1])
for i = 1, #res, 2 do
    redis.call("ZADD", KEYS[2], tonumber(res[i+1]), res[i])
end
return res
`)

// drainProcessingScript moves all entries from the processing sorted set
// (KEYS[1]) back into the queue sorted set (KEYS[2]), preserving original
// scores, then deletes the processing set. Called on worker startup to recover
// any entries left in-flight by a previously crashed worker.
var drainProcessingScript = redis.NewScript(`
local entries = redis.call("ZRANGE", KEYS[1], 0, -1, "WITHSCORES")
if #entries == 0 then return 0 end
local args = {}
for i = 1, #entries, 2 do
    table.insert(args, tonumber(entries[i+1]))
    table.insert(args, entries[i])
end
redis.call("ZADD", KEYS[2], unpack(args))
redis.call("DEL", KEYS[1])
return #entries / 2
`)

// commitMatchScript atomically checks the cancelled set, removes the matched
// players from the processing set, writes the match record, sets the status
// key, and adds the match to the forming set.
//
// KEYS[1]          = cancelled:{region}:{mode}
// KEYS[2]          = match:{matchID}
// KEYS[3]          = matches:forming
// KEYS[4]          = match:{matchID}:status
// KEYS[5]          = processing:{shard}
// KEYS[6]          = match-stream:{shard}
// ARGV[1]          = N (number of players)
// ARGV[2..N+1]     = playerIDs (for cancelled check)
// ARGV[N+2..2N+1]  = raw queue members/JSON (for ZREM from processing)
// ARGV[2N+2]       = match JSON
// ARGV[2N+3]       = TTL seconds
// ARGV[2N+4]       = formation score (UnixMilli)
// ARGV[2N+5]       = matchID
// ARGV[2N+6]       = stream max length
var commitMatchScript = redis.NewScript(`
local n = tonumber(ARGV[1])
for i = 2, n+1 do
    if redis.call("SISMEMBER", KEYS[1], ARGV[i]) == 1 then
        return 0
    end
end
redis.call("SET", KEYS[2], ARGV[2*n+2], "EX", tonumber(ARGV[2*n+3]))
redis.call("SET", KEYS[4], "forming", "EX", tonumber(ARGV[2*n+3]))
redis.call("ZADD", KEYS[3], tonumber(ARGV[2*n+4]), ARGV[2*n+5])
local members = {}
for i = 1, n do
    members[i] = ARGV[n+1+i]
end
redis.call("ZREM", KEYS[5], unpack(members))
redis.call("XADD", KEYS[6], "MAXLEN", "~", ARGV[2*n+6], "*", "match_id", ARGV[2*n+5], "data", ARGV[2*n+2])
return 1
`)

// matchStore is the subset of store.Store used by MatchMaker, defined as an
// interface so tests can substitute a fake without a real database.
type matchStore interface {
	InsertMatch(ctx context.Context, m *model.Match) error
}

type MatchMaker struct {
	matchSize int
	shard     model.Shard
	rdb       *redis.Client
	pub       *publish.Publisher // nil = no-op
	st        matchStore         // nil = no-op
	workerID  string
	matchSeq  atomic.Int64
}

func New(shard model.Shard, rdb *redis.Client, pub *publish.Publisher, matchSize int, st matchStore) *MatchMaker {
	return &MatchMaker{
		matchSize: matchSize,
		shard:     shard,
		rdb:       rdb,
		pub:       pub,
		st:        st,
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

	if m.st != nil {
		if err := m.ensureStreamGroup(ctx); err != nil {
			return fmt.Errorf("ensure stream group: %w", err)
		}
		if err := m.claimStaleStreamEntries(ctx); err != nil {
			return fmt.Errorf("claim stale stream entries: %w", err)
		}
	}

	if err := m.drainProcessing(ctx); err != nil {
		return fmt.Errorf("drain processing set: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return m.renewLockLoop(ctx) })
	g.Go(func() error { return m.popLoop(ctx) })
	if m.st != nil {
		g.Go(func() error { return m.streamConsumerLoop(ctx) })
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

func (m *MatchMaker) drainProcessing(ctx context.Context) error {
	n, err := drainProcessingScript.Run(ctx, m.rdb,
		[]string{rediskeys.Processing(m.shard), rediskeys.Queue(m.shard)},
	).Int()
	if err != nil {
		return fmt.Errorf("drain processing: %w", err)
	}
	if n > 0 {
		slog.Info("recovered in-flight entries from processing set", "shard", m.shard, "count", n)
	}
	return nil
}

func (m *MatchMaker) ensureStreamGroup(ctx context.Context) error {
	err := m.rdb.XGroupCreateMkStream(ctx, rediskeys.MatchStream(m.shard), streamGroup, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("xgroup create: %w", err)
	}
	return nil
}

// claimStaleStreamEntries reclaims any entries left unACKed by the previous
// worker and writes them to PostgreSQL. Must run before drainProcessing so
// that players already committed to a match are not re-queued.
func (m *MatchMaker) claimStaleStreamEntries(ctx context.Context) error {
	streamKey := rediskeys.MatchStream(m.shard)
	start := "0-0"
	for {
		msgs, next, err := m.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   streamKey,
			Group:    streamGroup,
			Consumer: m.workerID,
			MinIdle:  0,
			Start:    start,
			Count:    100,
		}).Result()
		if err != nil {
			return fmt.Errorf("xautoclaim: %w", err)
		}
		for _, msg := range msgs {
			if err := m.processStreamEntry(ctx, msg); err != nil {
				slog.Error("failed to process stale stream entry", "id", msg.ID, "err", err)
			}
		}
		if next == "0-0" {
			break
		}
		start = next
	}
	return nil
}

func (m *MatchMaker) streamConsumerLoop(ctx context.Context) error {
	streamKey := rediskeys.MatchStream(m.shard)
	for {
		streams, err := m.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    streamGroup,
			Consumer: m.workerID,
			Streams:  []string{streamKey, ">"},
			Count:    10,
			Block:    time.Second,
		}).Result()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return fmt.Errorf("xreadgroup: %w", err)
		}
		for _, s := range streams {
			for _, msg := range s.Messages {
				if err := m.processStreamEntry(ctx, msg); err != nil {
					// Leave unACKed — claimStaleStreamEntries will retry on next startup.
					slog.Error("failed to write match to postgres", "id", msg.ID, "err", err)
				}
			}
		}
	}
}

func (m *MatchMaker) processStreamEntry(ctx context.Context, msg redis.XMessage) error {
	data, ok := msg.Values["data"].(string)
	if !ok {
		return fmt.Errorf("stream message %s missing data field", msg.ID)
	}
	var match model.Match
	if err := json.Unmarshal([]byte(data), &match); err != nil {
		return fmt.Errorf("unmarshal match: %w", err)
	}
	if err := m.st.InsertMatch(ctx, &match); err != nil {
		return err
	}
	return m.rdb.XAck(ctx, rediskeys.MatchStream(m.shard), streamGroup, msg.ID).Err()
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

// popBatch atomically moves up to popBatchSize entries from the queue sorted
// set into the processing sorted set and returns them as redis.Z values.
func (m *MatchMaker) popBatch(ctx context.Context) ([]redis.Z, error) {
	vals, err := popToProcessingScript.Run(ctx, m.rdb,
		[]string{rediskeys.Queue(m.shard), rediskeys.Processing(m.shard)},
		popBatchSize,
	).Slice()
	if err == redis.Nil || len(vals) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pop to processing: %w", err)
	}
	zs := make([]redis.Z, 0, len(vals)/2)
	for i := 0; i+1 < len(vals); i += 2 {
		member, _ := vals[i].(string)
		score, _ := strconv.ParseFloat(fmt.Sprintf("%s", vals[i+1]), 64)
		zs = append(zs, redis.Z{Member: member, Score: score})
	}
	return zs, nil
}

// popOnce pops up to popBatchSize players from the queue into the processing
// set, parses their JSON members directly, filters cancelled players via
// SMIsMember, and commits a match whenever the buffer reaches matchSize.
// Returns the updated buffer.
func (m *MatchMaker) popOnce(ctx context.Context, buffer []bufferedEntry) ([]bufferedEntry, error) {
	zs, err := m.popBatch(ctx)
	if err != nil {
		return buffer, err
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
		group, remaining, found := selectGroup(buffer, m.matchSize, time.Now())
		if !found {
			break
		}
		buffer = remaining

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

// selectGroup picks the oldest-waiting player as anchor, computes a rating
// window expanded by their wait time, then greedily fills a group of size n
// from the remaining buffer within that window.
//
// Returns the chosen group, the updated buffer with those entries removed, and
// whether a valid group was found. The buffer order is otherwise preserved.
func selectGroup(buffer []bufferedEntry, n int, now time.Time) (group []bufferedEntry, remaining []bufferedEntry, found bool) {
	if len(buffer) < n {
		return nil, buffer, false
	}

	anchor := buffer[0]
	waitSecs := now.Sub(anchor.entry.EnqueuedAt).Seconds()
	window := ratingDiffAllowance + ratingExpandPerSecond*waitSecs
	if window > maxRatingDiff {
		window = maxRatingDiff
	}

	group = append(group, anchor)
	chosen := make([]bool, len(buffer))
	chosen[0] = true

	for i := 1; i < len(buffer) && len(group) < n; i++ {
		diff := buffer[i].entry.Rating - anchor.entry.Rating
		if diff < 0 {
			diff = -diff
		}
		if diff <= window {
			group = append(group, buffer[i])
			chosen[i] = true
		}
	}

	if len(group) < n {
		return nil, buffer, false
	}

	remaining = make([]bufferedEntry, 0, len(buffer)-n)
	for i, be := range buffer {
		if !chosen[i] {
			remaining = append(remaining, be)
		}
	}
	return group, remaining, true
}

// commitMatch atomically verifies no player has been cancelled, writes the
// match record, sets the status key, and registers the match in the forming
// set. Returns false if a cancellation is detected.
func (m *MatchMaker) commitMatch(ctx context.Context, group []bufferedEntry) (bool, error) {
	seq := m.matchSeq.Add(1)
	now := time.Now()

	playerIDs := make([]string, len(group))
	entries := make([]model.QueueEntry, len(group))
	for i, be := range group {
		playerIDs[i] = be.entry.PlayerID
		entries[i] = *be.entry
	}

	match := &model.Match{
		ID:        fmt.Sprintf("{%s/%s}:%s:%d:%d", m.shard.Region, m.shard.Mode, m.shard.RatingBand, now.UnixMilli(), seq),
		Shard:     m.shard,
		PlayerIDs: playerIDs,
		Entries:   entries,
		Status:    model.MatchStatusForming,
		FormedAt:  now,
	}
	matchData, err := json.Marshal(match)
	if err != nil {
		return false, err
	}

	keys := []string{
		rediskeys.Cancelled(m.shard),
		rediskeys.Match(match.ID),
		rediskeys.FormingMatches(m.shard),
		rediskeys.MatchStatusKey(match.ID),
		rediskeys.Processing(m.shard),
		rediskeys.MatchStream(m.shard),
	}
	args := make([]interface{}, 0, 1+2*len(playerIDs)+5)
	args = append(args, len(playerIDs))
	for _, id := range playerIDs {
		args = append(args, id)
	}
	for _, be := range group {
		args = append(args, be.member)
	}
	args = append(args,
		string(matchData),
		int(matchTTL.Seconds()),
		now.UnixMilli(),
		match.ID,
		streamMaxLen,
	)

	result, err := commitMatchScript.Run(ctx, m.rdb, keys, args...).Int()
	if err != nil {
		return false, err
	}
	if result == 0 {
		return false, nil
	}

	slog.Info("match formed", "match_id", match.ID, "players", playerIDs)
	if m.pub != nil {
		m.pub.Publish(ctx, publish.ChannelMatchForming, publish.MatchFormingEvent{
			MatchID:   match.ID,
			PlayerIDs: playerIDs,
		})
	}
	return true, nil
}

func (m *MatchMaker) repushGroup(ctx context.Context, group []bufferedEntry) error {
	members := make([]interface{}, len(group))
	pipe := m.rdb.Pipeline()
	for i, be := range group {
		pipe.ZAdd(ctx, rediskeys.Queue(m.shard), redis.Z{
			Score:  be.score,
			Member: be.member,
		})
		members[i] = be.member
	}
	pipe.ZRem(ctx, rediskeys.Processing(m.shard), members...)
	_, err := pipe.Exec(ctx)
	return err
}
