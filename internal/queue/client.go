package queue

import (
	"context"
	"encoding/json"
	"matchmaking/internal/model"
	"matchmaking/internal/rediskeys"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb   *redis.Client
	shard model.Shard
}

func New(shard model.Shard, rdb *redis.Client) *Client {
	return &Client{rdb: rdb, shard: shard}
}

func (c *Client) Join(ctx context.Context, entry *model.QueueEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	pipe := c.rdb.Pipeline()
	pipe.Set(ctx, rediskeys.QueueEntry(entry.PlayerID), data, 0)
	pipe.ZAdd(ctx, rediskeys.Queue(c.shard), redis.Z{
		Score:  float64(entry.EnqueuedAt.UnixMilli()),
		Member: entry.PlayerID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

func (c *Client) Cancel(ctx context.Context, playerID string) error {
	pipe := c.rdb.Pipeline()
	pipe.ZRem(ctx, rediskeys.Queue(c.shard), playerID)
	pipe.Del(ctx, rediskeys.QueueEntry(playerID))
	_, err := pipe.Exec(ctx)
	return err
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
