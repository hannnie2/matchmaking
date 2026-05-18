package model

import (
	"fmt"
	"time"
)

type Shard struct {
	Region string
	Mode   string
}

func (s Shard) String() string {
	return fmt.Sprintf("%s/%s", s.Region, s.Mode)
}

type QueueEntry struct {
	PlayerID   string
	Skill      float64
	EnqueuedAt time.Time
}

type MatchStatus string

const (
	MatchStatusForming   MatchStatus = "forming"
	MatchStatusConfirmed MatchStatus = "confirmed"
)

type Match struct {
	ID        string
	PlayerIDs []string
	Status    MatchStatus
	FormedAt  time.Time
}
