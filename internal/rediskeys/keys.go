package rediskeys

import (
	"fmt"
	"matchmaking/internal/model"
)

// hashTag returns the Redis Cluster hash tag for a shard's region+mode pair.
// All keys for a given region+mode share this tag and therefore the same hash
// slot, which is required for cross-key Lua scripts to work on Redis Cluster.
func hashTag(s model.Shard) string {
	return fmt.Sprintf("{%s/%s}", s.Region, s.Mode)
}

// Queue is the sorted set for a specific rating band. Score = enqueue time
// (UnixMilli); member = serialised QueueEntry JSON.
func Queue(shard model.Shard) string {
	return fmt.Sprintf("%s:q:%s", hashTag(shard), shard.RatingBand)
}

func Match(matchID string) string          { return "match:" + matchID }
func MatchStatusKey(matchID string) string { return "match:" + matchID + ":status" }
func MatchAccepted(matchID string) string  { return "match:" + matchID + ":accepted" }
func MatchDeclined(matchID string) string  { return "match:" + matchID + ":declined" }

func ShardLock(shard model.Shard) string {
	return fmt.Sprintf("%s:lock:%s", hashTag(shard), shard.RatingBand)
}

// Cancelled is per (region, mode) — Cancel does not know the player's rating
// band. It shares the region/mode hash tag with all other shard keys so Lua
// scripts can access it alongside queue keys on the same cluster node.
func Cancelled(shard model.Shard) string {
	return fmt.Sprintf("%s:cancelled", hashTag(shard))
}

// FormingMatches is the sorted set of match IDs awaiting player confirmation
// for a specific shard. Score = formation time (UnixMilli) for timeout detection.
func FormingMatches(shard model.Shard) string {
	return fmt.Sprintf("%s:forming:%s", hashTag(shard), shard.RatingBand)
}

// Processing is the sorted set holding entries that have been popped from the
// queue but not yet committed to a match. Score preserves the original enqueue
// timestamp so entries can be re-queued with FIFO ordering on recovery.
// Shares the region/mode hash tag so it is co-slotted with Queue and Cancelled.
func Processing(shard model.Shard) string {
	return fmt.Sprintf("%s:processing:%s", hashTag(shard), shard.RatingBand)
}

// PendingMatchEvent holds the match.found payload for a player who has not yet
// acknowledged receipt. Not accessed by any Lua script; slot placement is irrelevant.
func PendingMatchEvent(playerID string) string { return "pending:match:" + playerID }
