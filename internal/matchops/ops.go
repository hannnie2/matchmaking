package matchops

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/publish"
	"matchmaking/internal/rediskeys"
	"matchmaking/internal/store"
	"time"

	"github.com/redis/go-redis/v9"
)

const MatchTTL = 10 * time.Minute

// dissolveScript atomically transitions a match from forming → dissolved,
// removes it from the forming set, updates the match JSON, and re-queues all
// entries except the declined player (skipping any that are in the cancelled set).
//
// KEYS[1] = match:{id}:status
// KEYS[2] = match:{id}
// KEYS[3] = matches:forming
// KEYS[4] = q:{region}:{mode}:{ratingBand}
// KEYS[5] = cancelled:{region}:{mode}
// ARGV[1] = TTL seconds
// ARGV[2] = matchID
// ARGV[3] = declined player ID (empty string if none)
// ARGV[4,7,10,...] = playerID
// ARGV[5,8,11,...] = score (UnixMilli string)
// ARGV[6,9,12,...] = entry JSON member
var dissolveScript = redis.NewScript(`
local status = redis.call("GET", KEYS[1])
if not status or status ~= "forming" then return 0 end

redis.call("SET", KEYS[1], "dissolved", "EX", tonumber(ARGV[1]))
redis.call("ZREM", KEYS[3], ARGV[2])

local raw = redis.call("GET", KEYS[2])
if raw then
    local m = cjson.decode(raw)
    m["status"] = "dissolved"
    redis.call("SET", KEYS[2], cjson.encode(m), "EX", tonumber(ARGV[1]))
end

local declined = ARGV[3]
local i = 4
while i <= #ARGV do
    local pid    = ARGV[i]
    local score  = ARGV[i+1]
    local member = ARGV[i+2]
    if pid ~= declined and redis.call("SISMEMBER", KEYS[5], pid) == 0 then
        redis.call("ZADD", KEYS[4], tonumber(score), member)
    end
    i = i + 3
end

return 1
`)

// confirmScript atomically transitions a match from forming → confirmed,
// removes it from the forming set, and updates the match JSON.
//
// KEYS[1] = match:{id}:status
// KEYS[2] = match:{id}
// KEYS[3] = matches:forming
// ARGV[1] = TTL seconds
// ARGV[2] = matchID
var confirmScript = redis.NewScript(`
local status = redis.call("GET", KEYS[1])
if not status or status ~= "forming" then return 0 end

redis.call("SET", KEYS[1], "confirmed", "EX", tonumber(ARGV[1]))
redis.call("ZREM", KEYS[3], ARGV[2])

local raw = redis.call("GET", KEYS[2])
if raw then
    local m = cjson.decode(raw)
    m["status"] = "confirmed"
    redis.call("SET", KEYS[2], cjson.encode(m), "EX", tonumber(ARGV[1]))
end

return 1
`)

// Dissolve transitions a match from forming → dissolved, re-queues all players
// except the one who explicitly declined, and publishes match.dissolved.
// It is idempotent: the Lua script's status check prevents double-execution.
func Dissolve(ctx context.Context, rdb *redis.Client, pub *publish.Publisher, st *store.Store, matchID, declinedPlayerID string) {
	match := readMatch(ctx, rdb, matchID)
	if match == nil {
		return
	}

	args := make([]interface{}, 0, 3+len(match.Entries)*3)
	args = append(args, int(MatchTTL.Seconds()), matchID, declinedPlayerID)
	for i := range match.Entries {
		e := &match.Entries[i]
		data, err := json.Marshal(e)
		if err != nil {
			slog.Error("failed to marshal entry for dissolve", "player_id", e.PlayerID, "err", err)
			return
		}
		args = append(args, e.PlayerID, fmt.Sprintf("%d", e.EnqueuedAt.UnixMilli()), string(data))
	}

	ok, err := dissolveScript.Run(ctx, rdb,
		[]string{
			rediskeys.MatchStatusKey(matchID),
			rediskeys.Match(matchID),
			rediskeys.FormingMatches(),
			rediskeys.Queue(match.Shard),
			rediskeys.Cancelled(match.Shard),
		},
		args...,
	).Int()
	if err != nil || ok == 0 {
		return
	}

	pub.Publish(ctx, publish.ChannelMatchDissolved, publish.MatchDissolvedEvent{
		MatchID:   matchID,
		PlayerIDs: match.PlayerIDs,
	})
	slog.Info("match dissolved", "match_id", matchID)
	if st != nil {
		now := time.Now()
		go func() {
			if err := st.MarkMatchDissolved(context.Background(), matchID, now); err != nil {
				slog.Error("failed to persist match dissolution", "match_id", matchID, "err", err)
			}
		}()
	}
}

// Confirm transitions a match from forming → confirmed and publishes
// match.confirmed. Game server allocation is triggered after confirmation.
// It is idempotent: the Lua script's status check prevents double-execution.
func Confirm(ctx context.Context, rdb *redis.Client, pub *publish.Publisher, st *store.Store, matchID string) {
	// Read before the script so we have player IDs for the publish event.
	match := readMatch(ctx, rdb, matchID)

	ok, err := confirmScript.Run(ctx, rdb,
		[]string{
			rediskeys.MatchStatusKey(matchID),
			rediskeys.Match(matchID),
			rediskeys.FormingMatches(),
		},
		int(MatchTTL.Seconds()), matchID,
	).Int()
	if err != nil || ok == 0 {
		return
	}

	var playerIDs []string
	if match != nil {
		playerIDs = match.PlayerIDs
	}
	pub.Publish(ctx, publish.ChannelMatchConfirmed, publish.MatchConfirmedEvent{
		MatchID:   matchID,
		PlayerIDs: playerIDs,
	})
	slog.Info("match confirmed", "match_id", matchID)
	if st != nil {
		now := time.Now()
		go func() {
			if err := st.MarkMatchConfirmed(context.Background(), matchID, now); err != nil {
				slog.Error("failed to persist match confirmation", "match_id", matchID, "err", err)
			}
		}()
	}

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

func readMatch(ctx context.Context, rdb *redis.Client, matchID string) *model.Match {
	data, err := rdb.Get(ctx, rediskeys.Match(matchID)).Bytes()
	if err != nil {
		return nil
	}
	var match model.Match
	if err := json.Unmarshal(data, &match); err != nil {
		return nil
	}
	return &match
}
