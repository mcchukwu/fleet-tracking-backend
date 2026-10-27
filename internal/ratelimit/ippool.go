package ratelimit

import (
	"sync"
	"time"
)

// PerIP bounds new connection attempts from one address so a single
// client cannot empty the process-wide bucket.
type PerIP struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64
	burst   float64
	lastGC  time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func NewPerIP(rate, burst float64) *PerIP {
	return &PerIP{
		buckets: make(map[string]*bucket),
		rate:    rate,
		burst:   burst,
		lastGC:  time.Now(),
	}
}

func (p *PerIP) Allow(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	if now.Sub(p.lastGC) > time.Minute {
		for k, b := range p.buckets {
			if now.Sub(b.last) > 3*time.Minute {
				delete(p.buckets, k)
			}
		}
		p.lastGC = now
	}

	b, ok := p.buckets[key]
	if !ok {
		b = &bucket{tokens: p.burst, last: now}
		p.buckets[key] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * p.rate
	if b.tokens > p.burst {
		b.tokens = p.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (p *PerIP) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.buckets)
}
