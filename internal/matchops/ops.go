package matchops

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/publish"
	"matchmaking/internal/queue"
	"matchmaking/internal/rediskeys"
	"time"

	"github.com/redis/go-redis/v9"
)

const MatchTTL = 10 * time.Minute

// transitionStatusScript atomically moves a match from one status to another.
// Returns 0 if the match is missing or not in the expected status (idempotent).
//
// KEYS[1] = match:{id}:status
// ARGV[1] = expected status
// ARGV[2] = new status
// ARGV[3] = TTL seconds
var transitionStatusScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if not current or current ~= ARGV[1] then return 0 end
redis.call("SET", KEYS[1], ARGV[2], "EX", tonumber(ARGV[3]))
return 1
`)

// Dissolve transitions a match from forming → dissolved, re-queues all players
// except the one who explicitly declined, and publishes match.dissolved.
// It is idempotent: concurrent calls for the same match are safe.
func Dissolve(ctx context.Context, rdb *redis.Client, pub *publish.Publisher, matchID, declinedPlayerID string) {
	ok, err := transitionStatusScript.Run(ctx, rdb,
		[]string{rediskeys.MatchStatusKey(matchID)},
		string(model.MatchStatusForming),
		string(model.MatchStatusDissolved),
		int(MatchTTL.Seconds()),
	).Int()
	if err != nil || ok == 0 {
		return // already dissolved or confirmed by another caller
	}

	match := readAndUpdateStatus(ctx, rdb, matchID, model.MatchStatusDissolved)
	if match == nil {
		return
	}

	rdb.ZRem(ctx, rediskeys.FormingMatches(), matchID)

	matchQ := queue.New(model.Shard{Region: match.Shard.Region, Mode: match.Shard.Mode}, rdb)
	for i := range match.Entries {
		e := &match.Entries[i]
		if e.PlayerID == declinedPlayerID {
			continue
		}
		if err := matchQ.Join(ctx, e); err != nil {
			slog.Error("failed to re-queue player on dissolve", "player_id", e.PlayerID, "err", err)
		}
	}

	pub.Publish(ctx, publish.ChannelMatchDissolved, publish.MatchDissolvedEvent{
		MatchID:   matchID,
		PlayerIDs: match.PlayerIDs,
	})
	slog.Info("match dissolved", "match_id", matchID)
}

// Confirm transitions a match from forming → confirmed and publishes
// match.confirmed. Game server allocation is triggered by the caller.
// It is idempotent: concurrent calls for the same match are safe.
func Confirm(ctx context.Context, rdb *redis.Client, pub *publish.Publisher, matchID string) {
	ok, err := transitionStatusScript.Run(ctx, rdb,
		[]string{rediskeys.MatchStatusKey(matchID)},
		string(model.MatchStatusForming),
		string(model.MatchStatusConfirmed),
		int(MatchTTL.Seconds()),
	).Int()
	if err != nil || ok == 0 {
		return
	}

	match := readAndUpdateStatus(ctx, rdb, matchID, model.MatchStatusConfirmed)
	rdb.ZRem(ctx, rediskeys.FormingMatches(), matchID)

	var playerIDs []string
	if match != nil {
		playerIDs = match.PlayerIDs
	}
	pub.Publish(ctx, publish.ChannelMatchConfirmed, publish.MatchConfirmedEvent{
		MatchID:   matchID,
		PlayerIDs: playerIDs,
	})
	slog.Info("match confirmed", "match_id", matchID)

	go allocateServer(pub, matchID, playerIDs)
}

func allocateServer(pub *publish.Publisher, matchID string, playerIDs []string) {
	ctx := context.Background()
	// TODO: call Agones or custom allocator
	serverAddr := fmt.Sprintf("game-server-%s.internal:7777", matchID)
	slog.Info("game server allocated (stub)", "match_id", matchID, "addr", serverAddr)
	pub.Publish(ctx, publish.ChannelMatchServerReady, publish.MatchServerReadyEvent{
		MatchID:    matchID,
		ServerAddr: serverAddr,
		PlayerIDs:  playerIDs,
	})
}

// readAndUpdateStatus reads the match record, sets its Status field to the
// given value, and writes it back. It returns nil if the match is not found.
func readAndUpdateStatus(ctx context.Context, rdb *redis.Client, matchID string, status model.MatchStatus) *model.Match {
	data, err := rdb.Get(ctx, rediskeys.Match(matchID)).Bytes()
	if err != nil {
		return nil
	}
	var match model.Match
	if err := json.Unmarshal(data, &match); err != nil {
		return nil
	}
	match.Status = status
	updated, err := json.Marshal(&match)
	if err != nil {
		return nil
	}
	rdb.Set(ctx, rediskeys.Match(matchID), updated, MatchTTL)
	return &match
}
