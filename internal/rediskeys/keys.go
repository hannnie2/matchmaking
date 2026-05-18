package rediskeys

import (
	"fmt"
	"matchmaking/internal/model"
)

func Queue(shard model.Shard) string     { return fmt.Sprintf("queue:%s:%s", shard.Region, shard.Mode) }
func QueueEntry(playerID string) string  { return "queue_entry:" + playerID }
func Match(matchID string) string        { return "match:" + matchID }
func ShardLock(shard model.Shard) string { return fmt.Sprintf("shard_lock:%s:%s", shard.Region, shard.Mode) }
