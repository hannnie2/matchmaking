package rediskeys

import (
	"fmt"
	"matchmaking/internal/model"
)

// Queue is the sorted set for a specific skill band. Score = enqueue time
// (UnixMilli); member = serialised QueueEntry JSON.
func Queue(shard model.Shard) string {
	return fmt.Sprintf("q:%s:%s:%s", shard.Region, shard.Mode, shard.SkillBand)
}
func Match(matchID string) string          { return "match:" + matchID }
func MatchStatusKey(matchID string) string { return "match:" + matchID + ":status" }
func MatchAccepted(matchID string) string  { return "match:" + matchID + ":accepted" }
func MatchDeclined(matchID string) string  { return "match:" + matchID + ":declined" }
func ShardLock(shard model.Shard) string {
	return fmt.Sprintf("lock:%s:%s:%s", shard.Region, shard.Mode, shard.SkillBand)
}

// Cancelled is per (region, mode) only — Cancel does not know the player's skill band.
func Cancelled(shard model.Shard) string {
	return fmt.Sprintf("cancelled:%s:%s", shard.Region, shard.Mode)
}

// FormingMatches is a sorted set of match IDs currently awaiting player
// confirmation. Score = formation time (UnixMilli) for timeout detection.
func FormingMatches() string { return "matches:forming" }

// PendingMatchEvent holds the match.found payload for a player who has not yet
// acknowledged receipt. TTL mirrors the confirmation window so it self-cleans.
func PendingMatchEvent(playerID string) string { return "pending:match:" + playerID }
