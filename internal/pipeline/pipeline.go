// Package pipeline is the adapter-agnostic core: the buffered channel,
// worker pool, out-of-order-safe hot-path write, and geofence transition
// detection. It knows nothing about WebSocket, TCP, or any other
// transport, an adapter's only job is to convert whatever it receives
// into a PositionPing and call Submit. This is what lets cmd/ingest's WS
// handler and a future real-hardware TCP adapter share every line of
// actual ingestion logic instead of each reimplementing it.
package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mcchukwu/fleet-tracking-backend/internal/geofence"
	"github.com/redis/go-redis/v9"
)

type PositionPing struct {
	VehicleID  string
	Lat        float64
	Lon        float64
	SpeedKPH   float64
	HeadingDeg float64
	RecordedAt time.Time // device/event timestamp
	ReceivedAt time.Time // stamped by the adapter the instant it received the frame
}

type AlertEvent struct {
	VehicleID    string        `json:"vehicle_id"`
	GeofenceID   string        `json:"geofence_id"`
	GeofenceName string        `json:"geofence_name"`
	EventType    string        `json:"event_type"`
	EventTime    time.Time     `json:"event_time"`
	Latency      time.Duration `json:"latency_ns"`
}

const alertLogCapacity = 100_000

// applyPositionScript enforces "only a strictly newer ping wins" in one
// atomic round trip. See Pipeline.applyPosition for why a plain
// check-then-set on the Go side isn't good enough.
var applyPositionScript = redis.NewScript(`
local existing = redis.call('HGET', KEYS[1], 'recorded_at')
if existing and tonumber(existing) >= tonumber(ARGV[1]) then
    return 0
end
redis.call('HSET', KEYS[1], 'lat', ARGV[2], 'lon', ARGV[3], 'speed_kph', ARGV[4], 'heading_deg', ARGV[5], 'recorded_at', ARGV[1])
return 1
`)

type Config struct {
	TenantID          string
	OverflowPolicy    string // "block" | "drop_oldest" | "drop_newest"
	ChannelBufferSize int
	WorkerPoolSize    int
}

type Pipeline struct {
	cfg      Config
	rdb      *redis.Client
	geoCache *geofence.Cache
	log      *slog.Logger

	ingestCh chan PositionPing
	alerts   *alertLog

	receivedCount      int64
	droppedCount       int64
	redisErrCount      int64
	staleCount         int64
	validationErrCount int64
}

func New(rdb *redis.Client, geoCache *geofence.Cache, cfg Config, log *slog.Logger) *Pipeline {
	return &Pipeline{
		cfg:      cfg,
		rdb:      rdb,
		geoCache: geoCache,
		log:      log,
		ingestCh: make(chan PositionPing, cfg.ChannelBufferSize),
		alerts:   newAlertLog(alertLogCapacity),
	}
}

// Start launches the worker pool. Call once, after construction.
func (p *Pipeline) Start() {
	for i := 0; i < p.cfg.WorkerPoolSize; i++ {
		go p.worker()
	}
}

// Submit hands a ping to the pipeline. Every adapter's only entry point.
// Counting a received ping happens here, centrally, so every adapter's
// traffic is counted the same way regardless of transport.
func (p *Pipeline) Submit(ping PositionPing) {
	atomic.AddInt64(&p.receivedCount, 1)
	switch p.cfg.OverflowPolicy {
	case "block":
		p.ingestCh <- ping
	case "drop_oldest":
		select {
		case p.ingestCh <- ping:
		default:
			select {
			case <-p.ingestCh:
			default:
			}
			select {
			case p.ingestCh <- ping:
			default:
				atomic.AddInt64(&p.droppedCount, 1)
			}
		}
	default: // "drop_newest"
		select {
		case p.ingestCh <- ping:
		default:
			atomic.AddInt64(&p.droppedCount, 1)
		}
	}
}

// RecordValidationError lets an adapter report a ping it rejected before
// even calling Submit (e.g. failed internal/validator checks), so that
// traffic is visible in the same metrics rather than silently invisible.
func (p *Pipeline) RecordValidationError() { atomic.AddInt64(&p.validationErrCount, 1) }

func (p *Pipeline) worker() {
	ctx := context.Background()
	for ping := range p.ingestCh {
		applied, err := p.applyPosition(ctx, ping)
		if err != nil {
			atomic.AddInt64(&p.redisErrCount, 1)
			p.log.Error("redis apply-position failed", "vehicle_id", ping.VehicleID, "error", err)
			continue
		}
		if !applied {
			// Older than (or equal to) what's already applied for this
			// vehicle: a duplicate or a late arrival. Counted, not
			// silently dropped, and deliberately excluded from live
			// geofence evaluation, see the package-level doc comment in
			// cmd/ingest/main.go for the full reasoning on why this is
			// the right trade against NFR-2's latency budget.
			atomic.AddInt64(&p.staleCount, 1)
			continue
		}
		p.evaluateGeofences(ctx, ping)
	}
}

// applyPosition returns applied=true only if ping.RecordedAt is strictly
// newer than whatever is currently stored for the vehicle (or nothing is
// stored yet). The compare-and-write happens in one atomic Redis script,
// not a Go-side check-then-set, with 64 workers, two pings for the same
// vehicle processed concurrently is rare but not impossible, and a
// check-then-set race there would silently undermine the exact
// correctness property
func (p *Pipeline) applyPosition(ctx context.Context, ping PositionPing) (bool, error) {
	key := fmt.Sprintf("vehicle:%s:state", ping.VehicleID)
	result, err := applyPositionScript.Run(ctx, p.rdb, []string{key},
		ping.RecordedAt.UnixMilli(), ping.Lat, ping.Lon, ping.SpeedKPH, ping.HeadingDeg,
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (p *Pipeline) evaluateGeofences(ctx context.Context, ping PositionPing) {
	pt := geofence.Point{Lng: ping.Lon, Lat: ping.Lat}
	currentHits := p.geoCache.ContainingPolygons(p.cfg.TenantID, pt)

	current := make(map[string]geofence.Polygon, len(currentHits))
	for _, poly := range currentHits {
		current[poly.ID] = poly
	}

	insideKey := fmt.Sprintf("vehicle:%s:inside_geofences", ping.VehicleID)
	previousIDs, err := p.rdb.SMembers(ctx, insideKey).Result()
	if err != nil {
		p.log.Error("redis smembers failed", "vehicle_id", ping.VehicleID, "error", err)
		return
	}
	previous := make(map[string]bool, len(previousIDs))
	for _, id := range previousIDs {
		previous[id] = true
	}

	now := time.Now()
	for id, poly := range current {
		if !previous[id] {
			p.rdb.SAdd(ctx, insideKey, id)
			p.alerts.record(AlertEvent{VehicleID: ping.VehicleID, GeofenceID: id, GeofenceName: poly.Name,
				EventType: "enter", EventTime: ping.RecordedAt, Latency: now.Sub(ping.ReceivedAt)})
		}
	}
	for id := range previous {
		if _, stillIn := current[id]; !stillIn {
			p.rdb.SRem(ctx, insideKey, id)
			name := id
			if poly, ok := p.geoCache.AllPolygonIDs(p.cfg.TenantID)[id]; ok {
				name = poly.Name
			}
			p.alerts.record(AlertEvent{VehicleID: ping.VehicleID, GeofenceID: id, GeofenceName: name,
				EventType: "exit", EventTime: ping.RecordedAt, Latency: now.Sub(ping.ReceivedAt)})
		}
	}
}

// --- observability surface, read by cmd/ingest's HTTP handlers ---

func (p *Pipeline) QueueLen() int      { return len(p.ingestCh) }
func (p *Pipeline) QueueCap() int      { return cap(p.ingestCh) }
func (p *Pipeline) Received() int64    { return atomic.LoadInt64(&p.receivedCount) }
func (p *Pipeline) Dropped() int64     { return atomic.LoadInt64(&p.droppedCount) }
func (p *Pipeline) RedisErrors() int64 { return atomic.LoadInt64(&p.redisErrCount) }
func (p *Pipeline) Stale() int64       { return atomic.LoadInt64(&p.staleCount) }
func (p *Pipeline) ValidationErrors() int64 {
	return atomic.LoadInt64(&p.validationErrCount)
}
func (p *Pipeline) RecentAlerts() []AlertEvent                  { return p.alerts.snapshot() }
func (p *Pipeline) AlertLatencyPercentiles() map[string]float64 { return p.alerts.latencyPercentiles() }
func (p *Pipeline) PolygonNames() []string {
	polys := p.geoCache.AllPolygonIDs(p.cfg.TenantID)
	names := make([]string, 0, len(polys))
	for _, poly := range polys {
		names = append(names, poly.Name)
	}
	return names
}

// --- alert log: a fixed-capacity ring buffer ---

type alertLog struct {
	mu     sync.Mutex
	events []AlertEvent
	next   int
	filled bool
}

func newAlertLog(capacity int) *alertLog { return &alertLog{events: make([]AlertEvent, capacity)} }

func (a *alertLog) record(e AlertEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events[a.next] = e
	a.next = (a.next + 1) % len(a.events)
	if a.next == 0 {
		a.filled = true
	}
}

func (a *alertLog) snapshot() []AlertEvent {
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

func (a *alertLog) latencyPercentiles() map[string]float64 {
	events := a.snapshot()
	if len(events) == 0 {
		return map[string]float64{"p50_ms": 0, "p95_ms": 0, "p99_ms": 0, "count": 0}
	}
	lat := make([]float64, len(events))
	for i, e := range events {
		lat[i] = float64(e.Latency.Microseconds()) / 1000.0
	}
	sort.Float64s(lat)
	pct := func(p float64) float64 { return lat[int(p*float64(len(lat)-1))] }
	return map[string]float64{"p50_ms": pct(0.50), "p95_ms": pct(0.95), "p99_ms": pct(0.99), "count": float64(len(lat))}
}
