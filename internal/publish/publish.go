package publish

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"
)

const (
	ChannelMatchForming     = "match.forming"
	ChannelMatchConfirmed   = "match.confirmed"
	ChannelMatchDissolved   = "match.dissolved"
	ChannelMatchServerReady = "match.server_ready"
)

type MatchFormingEvent struct {
	MatchID   string   `json:"match_id"`
	PlayerIDs []string `json:"player_ids"`
}

type MatchConfirmedEvent struct {
	MatchID   string   `json:"match_id"`
	PlayerIDs []string `json:"player_ids"`
}

type MatchDissolvedEvent struct {
	MatchID   string   `json:"match_id"`
	PlayerIDs []string `json:"player_ids"`
}

type MatchServerReadyEvent struct {
	MatchID    string   `json:"match_id"`
	ServerAddr string   `json:"server_addr"`
	PlayerIDs  []string `json:"player_ids"`
}

type Publisher struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Publisher {
	return &Publisher{rdb: rdb}
}

func (p *Publisher) Publish(ctx context.Context, channel string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		slog.Error("failed to marshal event", "channel", channel, "err", err)
		return
	}
	if err := p.rdb.Publish(ctx, channel, data).Err(); err != nil {
		slog.Error("failed to publish event", "channel", channel, "err", err)
	}
}
