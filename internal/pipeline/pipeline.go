// Package pipeline applies pings to Redis, evaluates geofences and sends
// results to optional sinks. It knows nothing about the transport.
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
	TenantID    string
	VehicleID   string // external id: IMEI or simulator id
	VehicleUUID string // internal vehicles.id, resolved at handshake
	Lat         float64
	Lon         float64
	SpeedKPH    float64
	HeadingDeg  float64
	RecordedAt  time.Time // device/event timestamp
	ReceivedAt  time.Time // stamped by the adapter the instant it received the frame
	Source      string    // e.g. "simulator", "tracker", "dashcam", set by the adapter
}

type AlertEvent struct {
	TenantID     string        `json:"tenant_id"`
	VehicleID    string        `json:"vehicle_id"`
	VehicleUUID  string        `json:"-"`
	GeofenceID   string        `json:"geofence_id"`
	GeofenceName string        `json:"geofence_name"`
	EventType    string        `json:"event_type"`
	EventTime    time.Time     `json:"event_time"`
	Latency      time.Duration `json:"latency_ns"`
}

const alertLogCapacity = 100_000

// Broadcaster and ColdPathSink are optional. Either may be nil.
type Broadcaster interface {
	Publish(v any)
}

type ColdPathSink interface {
	EnqueuePosition(ping PositionPing, applied bool)
	EnqueueAlert(e AlertEvent)
}

// LiveUpdate is what the hub fans out.
type LiveUpdate struct {
	Type       string      `json:"type"` // "position" | "alert"
	TenantID   string      `json:"tenant_id"`
	VehicleID  string      `json:"vehicle_id"`
	Lat        float64     `json:"lat,omitempty"`
	Lon        float64     `json:"lon,omitempty"`
	RecordedAt time.Time   `json:"recorded_at,omitempty"`
	Alert      *AlertEvent `json:"alert,omitempty"`
}

// applyPositionScript updates position and geofence membership in one
// step. Two workers cannot read the same previous state.
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

	wg sync.WaitGroup

	receivedCount      atomic.Int64
	droppedCount       atomic.Int64
	redisErrCount      atomic.Int64
	staleCount         atomic.Int64
	validationErrCount atomic.Int64
}

// SetBroadcaster and SetColdPathSink must be called before Start.
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
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.worker()
		}()
	}
}

// Stop closes the queue and waits for the workers to drain it. It must run
// after the adapters stop calling Submit.
func (p *Pipeline) Stop() {
	close(p.ingestCh)
	p.wg.Wait()
}

// Submit is the only input. It never blocks under drop policies.
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

// RecordValidationError counts a frame rejected before Submit.
func (p *Pipeline) RecordValidationError() { p.validationErrCount.Add(1) }

func (p *Pipeline) worker() {
	ctx := context.Background()
	for ping := range p.ingestCh {
		current := p.containingPolygons(ping)
		applied, entered, exited, err := p.applyPosition(ctx, ping, current)
		if err != nil {
			p.redisErrCount.Add(1)
			p.log.Error("redis apply-position failed", "tenant", ping.TenantID, "vehicle", ping.VehicleID, "error", err)
			// Live state is lost, history is not: enqueue it as unapplied.
			if p.coldSink != nil {
				p.coldSink.EnqueuePosition(ping, false)
			}
			continue
		}

		if p.coldSink != nil {
			p.coldSink.EnqueuePosition(ping, applied)
		}

		if !applied {
			// Duplicate or late. Excluded from live state and alerts,
			// still written to history above.
			p.staleCount.Add(1)
			continue
		}

		if p.broadcaster != nil {
			p.broadcaster.Publish(LiveUpdate{
				Type: "position", TenantID: ping.TenantID, VehicleID: ping.VehicleID,
				Lat: ping.Lat, Lon: ping.Lon, RecordedAt: ping.RecordedAt,
			})
		}

		p.publishTransitions(ping, current, entered, exited)
	}
}

func (p *Pipeline) containingPolygons(ping PositionPing) map[string]geofence.Polygon {
	currentHits := p.geoCache.ContainingPolygons(ping.TenantID, geofence.Point{Lng: ping.Lon, Lat: ping.Lat})
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
	key := fmt.Sprintf("tenant:%s:vehicle:%s:state", ping.TenantID, ping.VehicleID)
	insideKey := fmt.Sprintf("tenant:%s:vehicle:%s:inside_geofences", ping.TenantID, ping.VehicleID)
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
		event := AlertEvent{TenantID: ping.TenantID, VehicleID: ping.VehicleID, VehicleUUID: ping.VehicleUUID, GeofenceID: id, GeofenceName: poly.Name,
			EventType: "enter", EventTime: ping.RecordedAt, Latency: now.Sub(ping.ReceivedAt)}
		p.alerts.record(event)
		p.publishAlert(event)
	}
	allPolygons := p.geoCache.AllPolygonIDs(ping.TenantID)
	for _, id := range exited {
		name := id
		if poly, ok := allPolygons[id]; ok {
			name = poly.Name
		}
		event := AlertEvent{TenantID: ping.TenantID, VehicleID: ping.VehicleID, VehicleUUID: ping.VehicleUUID, GeofenceID: id, GeofenceName: name,
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

// publishAlert sends one alert to the cold path and the live feed.
func (p *Pipeline) publishAlert(event AlertEvent) {
	if p.coldSink != nil {
		p.coldSink.EnqueueAlert(event)
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
