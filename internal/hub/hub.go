// Package hub fans live updates out to dashboard clients. The endpoint
// has no authentication and is meant for the private admin listener.
package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const clientSendBuffer = 64

type Hub struct {
	log       *slog.Logger
	broadcast chan []byte

	mu      sync.RWMutex
	clients map[chan []byte]struct{}
	drops   atomic.Int64
}

func New(log *slog.Logger) *Hub {
	return &Hub{
		log:       log,
		broadcast: make(chan []byte, 1024),
		clients:   make(map[chan []byte]struct{}),
	}
}

// Run fans one message out to every client. A slow client drops, and
// cannot block the others.
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-h.broadcast:
			h.mu.RLock()
			for ch := range h.clients {
				select {
				case ch <- msg:
				default:
					h.drops.Add(1)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Publish is safe to call from pipeline workers.
func (h *Hub) Publish(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		h.log.Error("hub: marshal failed", "error", err)
		return
	}
	select {
	case h.broadcast <- b:
	default:
		h.log.Warn("hub: broadcast channel full, dropping update")
	}
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		h.log.Warn("hub: accept error", "error", err)
		return
	}
	defer conn.CloseNow()

	ch := make(chan []byte, clientSendBuffer)
	h.register(ch)
	defer h.unregister(ch)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// This endpoint is push-only, but without reading at all we would
	// not notice a closed connection until the next write happened to
	// fail. A dedicated read loop (discarding content) surfaces a
	// disconnect promptly instead of leaking this goroutine until then.
	go func() {
		defer cancel()
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Write(writeCtx, websocket.MessageText, msg)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (h *Hub) ActiveClients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) Drops() int64 { return h.drops.Load() }

func (h *Hub) register(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[ch] = struct{}{}
}

func (h *Hub) unregister(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, ch)
}
