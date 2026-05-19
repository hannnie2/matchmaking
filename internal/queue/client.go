package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"matchmaking/internal/model"
	"matchmaking/internal/rediskeys"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	skillBandWidth = 200
	skillBandMin   = 1000
	skillBandMax   = 2400

	cancelledSetTTL = 24 * time.Hour
)

// ErrPlayerCancelled is returned by Join when the player has a pending
// cancellation in Redis. The join is rejected to prevent a ghost match.
var ErrPlayerCancelled = errors.New("player has a pending cancellation")

// joinScript atomically checks the cancelled set before writing the entry and
// adding the player to the skill-band sorted set.
//
// KEYS[1] = q:{region}:{mode}:{skillband}
// KEYS[2] = queue_entry:{playerID}
// KEYS[3] = cancelled:{region}:{mode}
// ARGV[1] = enqueue score (UnixMilli as string)
// ARGV[2] = playerID
// ARGV[3] = serialised QueueEntry JSON
var joinScript = redis.NewScript(`
if redis.call("SISMEMBER", KEYS[3], ARGV[2]) == 1 then
    return 0
end
redis.call("SET", KEYS[2], ARGV[3])
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// cancelScript atomically deletes the queue entry, records the cancellation,
// and refreshes the TTL on the cancelled set — closing the race window between
// the DEL and the SADD that a non-atomic pipeline would leave open.
//
// KEYS[1] = queue_entry:{playerID}
// KEYS[2] = cancelled:{region}:{mode}
// ARGV[1] = playerID
// ARGV[2] = cancelled set TTL in seconds
var cancelScript = redis.NewScript(`
redis.call("DEL", KEYS[1])
redis.call("SADD", KEYS[2], ARGV[1])
redis.call("EXPIRE", KEYS[2], ARGV[2])
return 1
`)

type Client struct {
	rdb   *redis.Client
	shard model.Shard // SkillBand is empty; derived per-player in Join
}

func New(shard model.Shard, rdb *redis.Client) *Client {
	return &Client{rdb: rdb, shard: shard}
}

func (c *Client) Join(ctx context.Context, entry *model.QueueEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	shard := model.Shard{
		Region:    c.shard.Region,
		Mode:      c.shard.Mode,
		SkillBand: computeSkillBand(entry.Skill),
	}
	keys := []string{
		rediskeys.Queue(shard),
		rediskeys.QueueEntry(entry.PlayerID),
		rediskeys.Cancelled(shard),
	}
	result, err := joinScript.Run(ctx, c.rdb, keys,
		fmt.Sprintf("%d", entry.EnqueuedAt.UnixMilli()),
		entry.PlayerID,
		string(data),
	).Int()
	if err != nil {
		return err
	}
	if result == 0 {
		return ErrPlayerCancelled
	}
	return nil
}

// Cancel atomically removes the player's queue entry and records the
// cancellation. It does not remove the player from the sorted set because the
// skill band is not known at cancel time; the worker detects the missing entry
// on next pop.
func (c *Client) Cancel(ctx context.Context, playerID string) error {
	return cancelScript.Run(ctx, c.rdb,
		[]string{rediskeys.QueueEntry(playerID), rediskeys.Cancelled(c.shard)},
		playerID, int(cancelledSetTTL.Seconds()),
	).Err()
}

// GetMatch returns nil, nil if the match does not exist or has expired.
func (c *Client) GetMatch(ctx context.Context, matchID string) (*model.Match, error) {
	data, err := c.rdb.Get(ctx, rediskeys.Match(matchID)).Bytes()
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

// computeSkillBand returns the skill band key for a given skill value.
// Bands are [1000,1200), [1200,1400), ..., [2200,2400].
func computeSkillBand(skill float64) string {
	s := int(skill)
	if s < skillBandMin {
		s = skillBandMin
	}
	if s >= skillBandMax {
		s = skillBandMax - skillBandWidth
	}
	lower := (s / skillBandWidth) * skillBandWidth
	return fmt.Sprintf("%d-%d", lower, lower+skillBandWidth)
}
