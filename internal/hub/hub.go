package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"matchmaking/internal/model"
	"matchmaking/internal/publish"
	"matchmaking/internal/rediskeys"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	pendingEventTTL = 35 * time.Second
	sendTimeout     = 5 * time.Second
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type wsEvent struct {
	Type       string   `json:"type"`
	MatchID    string   `json:"match_id,omitempty"`
	PlayerIDs  []string `json:"player_ids,omitempty"`
	ServerAddr string   `json:"server_addr,omitempty"`
}

// fanoutMsg carries a computed frame back from a processEvent goroutine to
// the hub goroutine, which is the only goroutine allowed to touch h.clients.
type fanoutMsg struct {
	playerIDs []string
	frame     []byte
}

type Hub struct {
	rdb        *redis.Client
	clients    map[string]*Client
	register   chan *Client
	unregister chan *Client
	fanout     chan fanoutMsg
}

func New(rdb *redis.Client) *Hub {
	return &Hub{
		rdb:        rdb,
		clients:    make(map[string]*Client),
		register:   make(chan *Client, 64),
		unregister: make(chan *Client, 64),
		fanout:     make(chan fanoutMsg, 64),
	}
}

// Run is the hub's single-owner goroutine. It only touches h.clients — all
// Redis I/O is delegated to spawned goroutines that hand results back via
// h.fanout.
func (h *Hub) Run(ctx context.Context) error {
	sub := h.rdb.Subscribe(ctx,
		publish.ChannelMatchForming,
		publish.ChannelMatchConfirmed,
		publish.ChannelMatchDissolved,
		publish.ChannelMatchServerReady,
	)
	defer sub.Close()

	ch := sub.Channel()
	for {
		select {
		case c := <-h.register:
			if old, ok := h.clients[c.playerID]; ok && old != c {
				close(old.send)
			}
			h.clients[c.playerID] = c
			go h.replayPending(ctx, c)

		case c := <-h.unregister:
			if current, ok := h.clients[c.playerID]; ok && current == c {
				delete(h.clients, c.playerID)
				close(c.send)
			}

		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			go h.processEvent(ctx, msg.Channel, []byte(msg.Payload))

		case fm := <-h.fanout:
			h.fanOutDirect(fm.playerIDs, fm.frame)

		case <-ctx.Done():
			return nil
		}
	}
}

// processEvent runs in its own goroutine. It does all Redis I/O, then sends
// the result to h.fanout for the hub goroutine to deliver.
func (h *Hub) processEvent(ctx context.Context, channel string, payload []byte) {
	switch channel {
	case publish.ChannelMatchForming:
		var ev publish.MatchFormingEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			slog.Error("hub: malformed match.forming payload", "err", err)
			return
		}
		frame := h.encode(wsEvent{Type: "match.found", MatchID: ev.MatchID, PlayerIDs: ev.PlayerIDs})
		pipe := h.rdb.Pipeline()
		for _, pid := range ev.PlayerIDs {
			pipe.Set(ctx, rediskeys.PendingMatchEvent(pid), frame, pendingEventTTL)
		}
		pipe.Exec(ctx)
		select {
		case h.fanout <- fanoutMsg{playerIDs: ev.PlayerIDs, frame: frame}:
		case <-ctx.Done():
			return
		}

	case publish.ChannelMatchConfirmed:
		var ev publish.MatchConfirmedEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			return
		}
		frame := h.encode(wsEvent{Type: "match.confirmed", MatchID: ev.MatchID})
		pipe := h.rdb.Pipeline()
		for _, pid := range ev.PlayerIDs {
			pipe.Del(ctx, rediskeys.PendingMatchEvent(pid))
		}
		pipe.Exec(ctx)
		select {
		case h.fanout <- fanoutMsg{playerIDs: ev.PlayerIDs, frame: frame}:
		case <-ctx.Done():
			return
		}

	case publish.ChannelMatchDissolved:
		var ev publish.MatchDissolvedEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			return
		}
		frame := h.encode(wsEvent{Type: "match.dissolved", MatchID: ev.MatchID})
		pipe := h.rdb.Pipeline()
		for _, pid := range ev.PlayerIDs {
			pipe.Del(ctx, rediskeys.PendingMatchEvent(pid))
		}
		pipe.Exec(ctx)
		select {
		case h.fanout <- fanoutMsg{playerIDs: ev.PlayerIDs, frame: frame}:
		case <-ctx.Done():
			return
		}

	case publish.ChannelMatchServerReady:
		var ev publish.MatchServerReadyEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			return
		}
		frame := h.encode(wsEvent{Type: "match.server_ready", MatchID: ev.MatchID, ServerAddr: ev.ServerAddr})
		select {
		case h.fanout <- fanoutMsg{playerIDs: ev.PlayerIDs, frame: frame}:
		case <-ctx.Done():
			return
		}
	}
}

// fanOutDirect is called only from the hub goroutine. It spawns one goroutine
// per recipient so the hub goroutine never blocks. If writePump doesn't drain
// within sendTimeout, the client is disconnected rather than the event dropped.
func (h *Hub) fanOutDirect(playerIDs []string, frame []byte) {
	for _, pid := range playerIDs {
		c, ok := h.clients[pid]
		if !ok {
			continue
		}
		go func(c *Client) {
			select {
			case c.send <- frame:
			case <-time.After(sendTimeout):
				slog.Warn("hub: slow client, disconnecting", "player_id", c.playerID)
				h.unregister <- c
				c.conn.Close()
			}
		}(c)
	}
}

// replayPending runs in its own goroutine. It checks Redis for a pending
// match.found event and delivers it if the match is still forming.
func (h *Hub) replayPending(ctx context.Context, c *Client) {
	data, err := h.rdb.Get(ctx, rediskeys.PendingMatchEvent(c.playerID)).Bytes()
	if err != nil {
		return
	}
	var ev wsEvent
	if err := json.Unmarshal(data, &ev); err != nil || ev.MatchID == "" {
		return
	}
	status, _ := h.rdb.Get(ctx, rediskeys.MatchStatusKey(ev.MatchID)).Result()
	if model.MatchStatus(status) != model.MatchStatusForming {
		h.rdb.Del(ctx, rediskeys.PendingMatchEvent(c.playerID))
		return
	}
	select {
	case c.send <- data:
	case <-ctx.Done():
	}
}

func (h *Hub) encode(v wsEvent) []byte {
	b, _ := json.Marshal(v)
	return b
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	playerID := r.URL.Query().Get("player_id")
	if playerID == "" {
		http.Error(w, "missing player_id", http.StatusBadRequest)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("hub: websocket upgrade failed", "err", err)
		return
	}
	c := &Client{
		hub:      h,
		playerID: playerID,
		conn:     conn,
		send:     make(chan []byte),
	}
	h.register <- c
	go c.writePump()
	c.readPump()
}
