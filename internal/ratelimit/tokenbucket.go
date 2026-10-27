// Package ratelimit bounds new connection admission.
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
