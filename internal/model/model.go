package model

import (
	"fmt"
	"time"
)

type Player struct {
	ID          string
	Name 		string
	CreatedAt   time.Time
}

type Shard struct {
	Region    string `json:"region"`
	Mode      string `json:"mode"`
	RatingBand string `json:"rating_band,omitempty"`
}

func (s Shard) String() string {
	if s.RatingBand != "" {
		return fmt.Sprintf("%s/%s/%s", s.Region, s.Mode, s.RatingBand)
	}
	return fmt.Sprintf("%s/%s", s.Region, s.Mode)
}

type QueueEntry struct {
	PlayerID   string    `json:"player_id"`
	Rating     int32   `json:"rating"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

type MatchStatus string

const (
	MatchStatusForming   MatchStatus = "forming"
	MatchStatusConfirmed MatchStatus = "confirmed"
	MatchStatusDissolved MatchStatus = "dissolved"
)

// Match is the authoritative record written to Redis when a group is formed.
// Entries stores the original QueueEntry for each player so they can be
// re-queued if the match is dissolved.
type Match struct {
	ID        string       `json:"id"`
	Shard     Shard        `json:"shard"`
	PlayerIDs []string     `json:"player_ids"`
	Entries   []QueueEntry `json:"entries"`
	Status    MatchStatus  `json:"status"`
	FormedAt  time.Time    `json:"formed_at"`
}
