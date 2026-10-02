// Package hub implements the live push side of the system: dashboards
// and operators connect over WebSocket and receive position updates and
// geofence alerts as they happen, instead of polling. This is the
// missing half of FR-8, ingestion already had a push interface (devices
// pushing in); this is the matching push interface going out.
//
// Not exposed publicly yet, see cmd/ingest/main.go. There is no
// authentication in front of this endpoint, so it is wired to the
// internal admin listener for now, not the public one. Moving it to a
// public, authenticated listener means wiring in
// multi-tenant auth service, not something to fake here by
// exposing it unauthenticated.
package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const clientSendBuffer = 64

type Hub struct {
	log       *slog.Logger
	broadcast chan []byte

	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

func New(log *slog.Logger) *Hub {
	return &Hub{
		log:       log,
		broadcast: make(chan []byte, 1024),
		clients:   make(map[chan []byte]struct{}),
	}
}

// Run is the single fan-out goroutine: reads from broadcast, writes to
// every connected client's own channel. A slow or stuck client can only
// ever back up its own small buffer, never the shared broadcast.
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
					// Slow client: drop this update rather than block
					// every other client on one stuck connection. Fine
					// for a live-state feed, the next update supersedes
					// it, the same reasoning as drop_oldest on the
					// ingestion side, applied to the output side.
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Publish marshals v and queues it for every connected client. Safe to
// call from any goroutine, including pipeline workers, this is the
// pipeline.Broadcaster interface's only method.
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

// ServeWS registers a dashboard client and pumps messages to it until it
// disconnects.
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
			if err := wsjson.Write(ctx, conn, json.RawMessage(msg)); err != nil {
				return
			}
		}
	}
}

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
