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

// joinScript atomically checks the cancelled set then ZADDs the serialised
// QueueEntry JSON as the sorted set member. No separate queue_entry key.
//
// KEYS[1] = q:{region}:{mode}:{skillband}
// KEYS[2] = cancelled:{region}:{mode}
// ARGV[1] = enqueue score (UnixMilli as string)
// ARGV[2] = playerID (for cancelled check only)
// ARGV[3] = serialised QueueEntry JSON (used as member)
var joinScript = redis.NewScript(`
if redis.call("SISMEMBER", KEYS[2], ARGV[2]) == 1 then
    return 0
end
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[3])
return 1
`)

// cancelScript atomically records the cancellation and refreshes the TTL.
// There is no queue_entry key to delete; the worker filters cancelled players
// via SMIsMember after each ZPOPMIN.
//
// KEYS[1] = cancelled:{region}:{mode}
// ARGV[1] = playerID
// ARGV[2] = TTL in seconds
var cancelScript = redis.NewScript(`
redis.call("SADD", KEYS[1], ARGV[1])
redis.call("EXPIRE", KEYS[1], ARGV[2])
return 1
`)

type Client struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Client {
	return &Client{rdb: rdb}
}

func (c *Client) Join(ctx context.Context, shard model.Shard, entry *model.QueueEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	qShard := model.Shard{
		Region:     shard.Region,
		Mode:       shard.Mode,
		RatingBand: computeRatingBand(entry.Rating),
	}
	result, err := joinScript.Run(ctx, c.rdb,
		[]string{rediskeys.Queue(qShard), rediskeys.Cancelled(qShard)},
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

// Cancel records the cancellation atomically. The player's JSON member remains
// in the sorted set until the worker pops it; the cancelled set causes the
// worker to discard them before buffering.
func (c *Client) Cancel(ctx context.Context, shard model.Shard, playerID string) error {
	return cancelScript.Run(ctx, c.rdb,
		[]string{rediskeys.Cancelled(shard)},
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

// computeRatingBand returns the rating band key for a given rating value.
// Bands are [1000,1200), [1200,1400), ..., [2200,2400].
func computeRatingBand(rating int32) string {
	r := int(rating)
	if r < skillBandMin {
		r = skillBandMin
	}
	if r >= skillBandMax {
		r = skillBandMax - skillBandWidth
	}
	lower := (r / skillBandWidth) * skillBandWidth
	return fmt.Sprintf("%d-%d", lower, lower+skillBandWidth)
}
