package rediskeys

import (
	"fmt"
	"matchmaking/internal/model"
)

// Queue is the sorted set for a specific skill band. Score = enqueue time (UnixMilli).
func Queue(shard model.Shard) string     { return fmt.Sprintf("q:%s:%s:%s", shard.Region, shard.Mode, shard.SkillBand) }
func QueueEntry(playerID string) string  { return "queue_entry:" + playerID }
func Match(matchID string) string        { return "match:" + matchID }
func ShardLock(shard model.Shard) string { return fmt.Sprintf("lock:%s:%s:%s", shard.Region, shard.Mode, shard.SkillBand) }

// Cancelled is per (region, mode) only — Cancel does not know the player's skill band.
func Cancelled(shard model.Shard) string { return fmt.Sprintf("cancelled:%s:%s", shard.Region, shard.Mode) }
