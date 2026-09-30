// Package ratelimit holds a hand-rolled token bucket. It's deliberately
// not tied to WebSocket or HTTP. The reconnect-storm problem it solves
// (bound the rate of new connection admission) applies just as much to a
// future raw-TCP device adapter as it does to cmd/ingest's WS handler,
// so both can construct their own instance of the same primitive rather
// than duplicating it per transport.
package ratelimit

import (
	"sync"
	"time"
)

type TokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64
	last     time.Time
}

func NewTokenBucket(ratePerSec, burst float64) *TokenBucket {
	return &TokenBucket{tokens: burst, capacity: burst, rate: ratePerSec, last: time.Now()}
}

func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
