package alert

import (
	"sort"
	"sync"
)

// AlertLog is a fixed capacity ring buffer: simple slice + mutex + write
// index. This is exactly the amount of infrastructure the current need
// (verify correctness, compute a percentile) justifies. If this needed to
// survive a restart, or be queried across multiple ingestion instances,
// that would be a measured requirement calling for something more
// Postgres persistence (Phase 4) or a real metrics backend. Not yet.
type AlertLog struct {
	mu     sync.Mutex
	events []AlertEvent
	next   int
	filled bool
}

func NewAlertLog(capacity int) *AlertLog {
	return &AlertLog{
		events: make([]AlertEvent, capacity),
	}
}

// Record adds an event to the log. It is not thread-safe.
func (a *AlertLog) Record(e AlertEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events[a.next] = e
	a.next = (a.next + 1) % len(a.events)
	if a.next == 0 {
		a.filled = true
	}
}

// snapshot returns a copy of the current log contents. It is not thread-safe.
func (a *AlertLog) Snapshot() []AlertEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := a.next
	if a.filled {
		n = len(a.events)
	}
	out := make([]AlertEvent, n)
	copy(out, a.events[:n])
	return out
}

// LatencyPercentiles returns the 50th, 95th, 99th, and count of the
// recorded events, in milliseconds.
func (a *AlertLog) LatencyPercentiles() map[string]float64 {
	events := a.Snapshot()
	if len(events) == 0 {
		return map[string]float64{"p50_ms": 0, "p95_ms": 0, "p99_ms": 0, "count": 0}
	}
	latencies := make([]float64, len(events))
	for i, e := range events {
		latencies[i] = float64(e.Latency.Microseconds()) / 1000.0
	}
	sort.Float64s(latencies)
	pct := func(p float64) float64 {
		idx := int(p * float64(len(latencies)-1))
		return latencies[idx]
	}
	return map[string]float64{
		"p50_ms": pct(0.50),
		"p95_ms": pct(0.95),
		"p99_ms": pct(0.99),
		"count":  float64(len(latencies)),
	}
}
