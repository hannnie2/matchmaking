package model

import (
	"fmt"
	"time"
)

type Shard struct {
	Region    string
	Mode      string
	SkillBand string // e.g. "1000-1200"; empty for client-side usage
}

func (s Shard) String() string {
	if s.SkillBand != "" {
		return fmt.Sprintf("%s/%s/%s", s.Region, s.Mode, s.SkillBand)
	}
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
