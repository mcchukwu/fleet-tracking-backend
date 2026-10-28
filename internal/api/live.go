package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/mcchukwu/fleet-tracking-backend/internal/bus"
)

func (s *Server) liveWS(w http.ResponseWriter, r *http.Request) {
	user := userFrom(r.Context())

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: s.cfg.WSOrigins,
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1024)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go func() {
		defer cancel()
		for {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}()

	sub := s.rdb.Subscribe(ctx, bus.Channel)
	defer sub.Close()
	messages := sub.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			var envelope struct {
				TenantID string `json:"tenant_id"`
			}
			if err := json.Unmarshal([]byte(msg.Payload), &envelope); err != nil {
				continue
			}
			if envelope.TenantID != user.TenantID {
				continue
			}
			writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Write(writeCtx, websocket.MessageText, []byte(msg.Payload))
			writeCancel()
			if err != nil {
				return
			}
		}
	}
}
