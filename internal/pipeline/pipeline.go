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
	Source     string    // e.g. "simulator", "tracker", "dashcam", set by the adapter
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

// Broadcaster and ColdPathSink are the pipeline's two optional output
// ports, deliberately interfaces, not concrete types, so this package
// stays ignorant of what (if anything) consumes its output, the same way
// it stays ignorant of which transport produced its input. internal/hub
// implements Broadcaster; internal/coldpath implements ColdPathSink.
// Either or both may be nil, see SetBroadcaster/SetColdPathSink.
type Broadcaster interface {
	Publish(v any)
}

type ColdPathSink interface {
	EnqueuePosition(ping PositionPing, tenantID string, applied bool)
	EnqueueAlert(e AlertEvent, tenantID string)
}

// LiveUpdate is the message shape published to a Broadcaster. Defined
// here, not in internal/hub, so the hub stays a generic "marshal and
// fan out whatever it's given" mechanism with no knowledge of this
// domain's message shapes.
type LiveUpdate struct {
	Type       string      `json:"type"` // "position" | "alert"
	VehicleID  string      `json:"vehicle_id"`
	Lat        float64     `json:"lat,omitempty"`
	Lon        float64     `json:"lon,omitempty"`
	RecordedAt time.Time   `json:"recorded_at,omitempty"`
	Alert      *AlertEvent `json:"alert,omitempty"`
}

// applyPositionScript atomically applies a newer position and replaces the
// vehicle's geofence membership. Keeping both operations in Redis prevents
// concurrent workers or ingestion instances from emitting duplicate or
// inverted transitions for the same vehicle.
var applyPositionScript = redis.NewScript(`
local existing = redis.call('HGET', KEYS[1], 'recorded_at')
if existing and existing >= ARGV[1] then
    return {0, {}, {}}
end
redis.call('HSET', KEYS[1], 'lat', ARGV[2], 'lon', ARGV[3], 'speed_kph', ARGV[4], 'heading_deg', ARGV[5], 'recorded_at', ARGV[1])

local current = {}
for i = 6, #ARGV do
    current[ARGV[i]] = true
end

local previous = redis.call('SMEMBERS', KEYS[2])
local previousSet = {}
for _, id in ipairs(previous) do
    previousSet[id] = true
end

local entered = {}
for id, _ in pairs(current) do
    if not previousSet[id] then
        redis.call('SADD', KEYS[2], id)
        table.insert(entered, id)
    end
end

local exited = {}
for _, id in ipairs(previous) do
    if not current[id] then
        redis.call('SREM', KEYS[2], id)
        table.insert(exited, id)
    end
end

return {1, entered, exited}
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

	broadcaster Broadcaster  // optional; nil until SetBroadcaster is called
	coldSink    ColdPathSink // optional; nil until SetColdPathSink is called

	receivedCount      atomic.Int64
	droppedCount       atomic.Int64
	redisErrCount      atomic.Int64
	staleCount         atomic.Int64
	validationErrCount atomic.Int64
}

// SetBroadcaster and SetColdPathSink wire optional output ports in after
// construction. Call before Start(); nil is a valid, supported value,
// the pipeline works standalone with neither wired in, which is exactly
// what let Phases 1-3 ship before either existed.
func (p *Pipeline) SetBroadcaster(b Broadcaster)   { p.broadcaster = b }
func (p *Pipeline) SetColdPathSink(s ColdPathSink) { p.coldSink = s }

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
	for range p.cfg.WorkerPoolSize {
		go p.worker()
	}
}

// Submit hands a ping to the pipeline. Every adapter's only entry point.
// Counting a received ping happens here, centrally, so every adapter's
// traffic is counted the same way regardless of transport.
func (p *Pipeline) Submit(ping PositionPing) {
	p.receivedCount.Add(1)
	switch p.cfg.OverflowPolicy {
	case "block":
		p.ingestCh <- ping
	case "drop_oldest":
		select {
		case p.ingestCh <- ping:
		default:
			select {
			case <-p.ingestCh:
				p.droppedCount.Add(1)
			default:
			}
			select {
			case p.ingestCh <- ping:
			default:
				p.droppedCount.Add(1)
			}
		}
	default: // "drop_newest"
		select {
		case p.ingestCh <- ping:
		default:
			p.droppedCount.Add(1)
		}
	}
}

// RecordValidationError lets an adapter report a ping it rejected before
// even calling Submit (e.g. failed internal/validator checks), so that
// traffic is visible in the same metrics rather than silently invisible.
func (p *Pipeline) RecordValidationError() { p.validationErrCount.Add(1) }

func (p *Pipeline) worker() {
	ctx := context.Background()
	for ping := range p.ingestCh {
		current := p.containingPolygons(ping)
		applied, entered, exited, err := p.applyPosition(ctx, ping, current)
		if err != nil {
			p.redisErrCount.Add(1)
			p.log.Error("redis apply-position failed", "vehicle_id", ping.VehicleID, "error", err)
			// Known limitation, stated rather than hidden: a ping that
			// fails here (a transient Redis error, not a stale/duplicate
			// one) is not enqueued to the cold path either. Durability
			// across a Redis outage specifically is further hardening,
			// not this phase's scope.
			continue
		}

		if p.coldSink != nil {
			p.coldSink.EnqueuePosition(ping, p.cfg.TenantID, applied)
		}

		if !applied {
			// Older than (or equal to) what's already applied for this
			// vehicle: a duplicate or a late arrival. Counted, not
			// silently dropped, and deliberately excluded from live
			// geofence evaluation and live broadcast, see the
			// package-level doc comment in cmd/ingest/main.go for the
			// full reasoning on why this is the right trade against
			// NFR-2's latency budget. It still reached the cold path
			// above, which makes it eligible for cold-path persistence.
			p.staleCount.Add(1)
			continue
		}

		if p.broadcaster != nil {
			p.broadcaster.Publish(LiveUpdate{
				Type: "position", VehicleID: ping.VehicleID,
				Lat: ping.Lat, Lon: ping.Lon, RecordedAt: ping.RecordedAt,
			})
		}

		p.publishTransitions(ping, current, entered, exited)
	}
}

func (p *Pipeline) containingPolygons(ping PositionPing) map[string]geofence.Polygon {
	currentHits := p.geoCache.ContainingPolygons(p.cfg.TenantID, geofence.Point{Lng: ping.Lon, Lat: ping.Lat})
	current := make(map[string]geofence.Polygon, len(currentHits))
	for _, poly := range currentHits {
		current[poly.ID] = poly
	}
	return current
}

// applyPosition returns the transitions that belong to a newer ping. The
// timestamp is a fixed-width UTC string, so Redis/Lua can compare it exactly;
// numeric Unix nanoseconds would lose precision in Lua's floating point type.
func (p *Pipeline) applyPosition(
	ctx context.Context, ping PositionPing, current map[string]geofence.Polygon,
) (bool, []string, []string, error) {
	key := fmt.Sprintf("vehicle:%s:state", ping.VehicleID)
	insideKey := fmt.Sprintf("vehicle:%s:inside_geofences", ping.VehicleID)
	args := make([]any, 0, 5+len(current))
	args = append(args,
		ping.RecordedAt.UTC().Format("2006-01-02T15:04:05.000000000Z"),
		ping.Lat, ping.Lon, ping.SpeedKPH, ping.HeadingDeg,
	)
	for id := range current {
		args = append(args, id)
	}
	result, err := applyPositionScript.Run(ctx, p.rdb, []string{key, insideKey}, args...).Result()
	if err != nil {
		return false, nil, nil, err
	}
	values, ok := result.([]any)
	if !ok || len(values) != 3 {
		return false, nil, nil, fmt.Errorf("unexpected apply-position response %T", result)
	}
	applied, ok := values[0].(int64)
	if !ok {
		return false, nil, nil, fmt.Errorf("unexpected apply-position status %T", values[0])
	}
	entered, err := redisIDs(values[1])
	if err != nil {
		return false, nil, nil, err
	}
	exited, err := redisIDs(values[2])
	if err != nil {
		return false, nil, nil, err
	}
	return applied == 1, entered, exited, nil
}

func (p *Pipeline) publishTransitions(
	ping PositionPing, current map[string]geofence.Polygon, entered, exited []string,
) {
	now := time.Now()
	for _, id := range entered {
		poly := current[id]
		event := AlertEvent{VehicleID: ping.VehicleID, GeofenceID: id, GeofenceName: poly.Name,
			EventType: "enter", EventTime: ping.RecordedAt, Latency: now.Sub(ping.ReceivedAt)}
		p.alerts.record(event)
		p.publishAlert(event)
	}
	allPolygons := p.geoCache.AllPolygonIDs(p.cfg.TenantID)
	for _, id := range exited {
		name := id
		if poly, ok := allPolygons[id]; ok {
			name = poly.Name
		}
		event := AlertEvent{VehicleID: ping.VehicleID, GeofenceID: id, GeofenceName: name,
			EventType: "exit", EventTime: ping.RecordedAt, Latency: now.Sub(ping.ReceivedAt)}
		p.alerts.record(event)
		p.publishAlert(event)
	}
}

func redisIDs(value any) ([]string, error) {
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected Redis ID list %T", value)
	}
	ids := make([]string, len(values))
	for i, value := range values {
		id, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected Redis ID type %T", value)
		}
		ids[i] = id
	}
	return ids, nil
}

// publishAlert enqueues an alert to the cold path and broadcasts it to
// live dashboard clients, the two things every alert should reach,
// factored out so the enter/exit branches above don't each repeat the
// nil-checks.
func (p *Pipeline) publishAlert(event AlertEvent) {
	if p.coldSink != nil {
		p.coldSink.EnqueueAlert(event, p.cfg.TenantID)
	}
	if p.broadcaster != nil {
		p.broadcaster.Publish(LiveUpdate{Type: "alert", VehicleID: event.VehicleID, Alert: &event})
	}
}

// --- observability surface, read by cmd/ingest's HTTP handlers ---

func (p *Pipeline) QueueLen() int      { return len(p.ingestCh) }
func (p *Pipeline) QueueCap() int      { return cap(p.ingestCh) }
func (p *Pipeline) Received() int64    { return p.receivedCount.Load() }
func (p *Pipeline) Dropped() int64     { return p.droppedCount.Load() }
func (p *Pipeline) RedisErrors() int64 { return p.redisErrCount.Load() }
func (p *Pipeline) Stale() int64       { return p.staleCount.Load() }
func (p *Pipeline) ValidationErrors() int64 {
	return p.validationErrCount.Load()
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
	if !a.filled {
		out := make([]AlertEvent, a.next)
		copy(out, a.events[:a.next])
		return out
	}
	out := make([]AlertEvent, len(a.events))
	n := copy(out, a.events[a.next:])
	copy(out[n:], a.events[:a.next])
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
