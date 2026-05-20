package api

import (
	"encoding/json"
	"matchmaking/internal/matchops"
	"matchmaking/internal/model"
	"matchmaking/internal/publish"
	"matchmaking/internal/queue"
	"matchmaking/internal/rediskeys"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

const responseTTL = 10 * time.Minute

// acceptScript atomically records a player's acceptance for a match.
// Returns -1 if the match is not in forming state, -2 if the player already
// declined, -3 if the player already accepted, or the distinct accepted count.
//
// KEYS[1] = match:{id}:accepted  (set of player IDs)
// KEYS[2] = match:{id}:declined
// KEYS[3] = match:{id}:status
// ARGV[1] = playerID
// ARGV[2] = TTL seconds
var acceptScript = redis.NewScript(`
local status = redis.call("GET", KEYS[3])
if not status or status ~= "forming" then return -1 end
if redis.call("SISMEMBER", KEYS[2], ARGV[1]) == 1 then return -2 end
if redis.call("SADD", KEYS[1], ARGV[1]) == 0 then return -3 end
redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
return redis.call("SCARD", KEYS[1])
`)

// declineScript atomically adds the player to the declined set.
// Returns -1 if the match is not in forming state, or 1 if this is the
// first decline (caller should dissolve), 0 if already declined.
//
// KEYS[1] = match:{id}:declined
// KEYS[2] = match:{id}:status
// ARGV[1] = playerID
// ARGV[2] = TTL seconds
var declineScript = redis.NewScript(`
local status = redis.call("GET", KEYS[2])
if not status or status ~= "forming" then return -1 end
local added = redis.call("SADD", KEYS[1], ARGV[1])
redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
return added
`)

type Handler struct {
	q   *queue.Client
	rdb *redis.Client
	pub *publish.Publisher
}

func NewHandler(q *queue.Client, rdb *redis.Client, pub *publish.Publisher) *Handler {
	return &Handler{q: q, rdb: rdb, pub: pub}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /queue/join", h.join)
	mux.HandleFunc("POST /queue/cancel", h.cancel)
	mux.HandleFunc("GET /match/{id}", h.matchStatus)
	mux.HandleFunc("POST /match/{id}/accept", h.accept)
	mux.HandleFunc("POST /match/{id}/decline", h.decline)
}

func (h *Handler) join(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PlayerID string  `json:"player_id"`
		Skill    float64 `json:"skill"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PlayerID == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	entry := &model.QueueEntry{
		PlayerID:   body.PlayerID,
		Skill:      body.Skill,
		EnqueuedAt: time.Now(),
	}
	if err := h.q.Join(r.Context(), entry); err != nil {
		http.Error(w, "failed to join queue", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PlayerID string `json:"player_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PlayerID == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.q.Cancel(r.Context(), body.PlayerID); err != nil {
		http.Error(w, "failed to cancel", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) matchStatus(w http.ResponseWriter, r *http.Request) {
	match, err := h.q.GetMatch(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if match == nil {
		http.Error(w, "match not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(match)
}

func (h *Handler) accept(w http.ResponseWriter, r *http.Request) {
	matchID := r.PathValue("id")
	var body struct {
		PlayerID string `json:"player_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PlayerID == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Read matchSize from the match record to know the confirmation threshold.
	match, err := h.q.GetMatch(r.Context(), matchID)
	if err != nil || match == nil {
		http.Error(w, "match not found", http.StatusNotFound)
		return
	}

	result, err := acceptScript.Run(r.Context(), h.rdb,
		[]string{
			rediskeys.MatchAccepted(matchID),
			rediskeys.MatchDeclined(matchID),
			rediskeys.MatchStatusKey(matchID),
		},
		body.PlayerID, int(responseTTL.Seconds()),
	).Int()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	switch result {
	case -1:
		// Match is not in forming state — distinguish confirmed from not-found.
		status, _ := h.rdb.Get(r.Context(), rediskeys.MatchStatusKey(matchID)).Result()
		switch model.MatchStatus(status) {
		case model.MatchStatusConfirmed:
			w.WriteHeader(http.StatusNoContent) // already confirmed, accept is a no-op
		case model.MatchStatusDissolved:
			http.Error(w, "match dissolved", http.StatusGone)
		default:
			http.Error(w, "match not found", http.StatusNotFound)
		}
		return
	case -2:
		http.Error(w, "player already declined", http.StatusConflict)
		return
	case -3:
		w.WriteHeader(http.StatusNoContent) // duplicate accept is a no-op
		return
	}

	if result == len(match.PlayerIDs) {
		matchops.Confirm(r.Context(), h.rdb, h.pub, matchID)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) decline(w http.ResponseWriter, r *http.Request) {
	matchID := r.PathValue("id")
	var body struct {
		PlayerID string `json:"player_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PlayerID == "" {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	result, err := declineScript.Run(r.Context(), h.rdb,
		[]string{
			rediskeys.MatchDeclined(matchID),
			rediskeys.MatchStatusKey(matchID),
		},
		body.PlayerID, int(responseTTL.Seconds()),
	).Int()
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if result == -1 {
		http.Error(w, "match not found or not forming", http.StatusNotFound)
		return
	}

	if result == 1 {
		// First decline — dissolve the match and re-queue the rest.
		matchops.Dissolve(r.Context(), h.rdb, h.pub, matchID, body.PlayerID)
	}
	w.WriteHeader(http.StatusNoContent)
}
