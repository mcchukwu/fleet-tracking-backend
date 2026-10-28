// Package bus carries live updates from ingest to the API process.
package bus

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

const Channel = "ftb:live"

type Publisher struct {
	rdb *redis.Client
}

func NewPublisher(rdb *redis.Client) *Publisher {
	return &Publisher{rdb: rdb}
}

func (p *Publisher) Publish(v any) {
	payload, err := json.Marshal(v)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = p.rdb.Publish(ctx, Channel, payload).Err()
}
