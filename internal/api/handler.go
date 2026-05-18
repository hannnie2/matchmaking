package api

import (
	"encoding/json"
	"matchmaking/internal/model"
	"matchmaking/internal/worker"
	"net/http"
	"time"
)

type Handler struct {
	mm *worker.MatchMaker
}

func NewHandler(mm *worker.MatchMaker) *Handler {
	return &Handler{mm: mm}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /queue/join", h.join)
	mux.HandleFunc("POST /queue/cancel", h.cancel)
	mux.HandleFunc("GET /match/{id}", h.matchStatus)
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
	if err := h.mm.Join(r.Context(), entry); err != nil {
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
	if err := h.mm.Cancel(r.Context(), body.PlayerID); err != nil {
		http.Error(w, "failed to cancel", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) matchStatus(w http.ResponseWriter, r *http.Request) {
	match, err := h.mm.GetMatch(r.Context(), r.PathValue("id"))
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
