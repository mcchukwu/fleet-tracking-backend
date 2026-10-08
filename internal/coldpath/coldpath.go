// Package coldpath batches asynchronous Postgres writes. It keeps database
// latency off the ingest path, but its bounded queues can drop records when
// full; callers must expose those counters and must not treat it as durable
// delivery through an outage.
package coldpath

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mcchukwu/fleet-tracking-backend/internal/pipeline"
)

type PositionRecord struct {
	VehicleExternalID string
	TenantID          string
	RecordedAt        time.Time
	ReceivedAt        time.Time
	Lat, Lon          float64
	SpeedKPH          float64
	HeadingDeg        float64
	Source            string
}

type AlertRecord struct {
	VehicleExternalID string
	TenantID          string
	GeofenceID        string
	EventType         string
	EventTime         time.Time
}

type Writer struct {
	db  *sql.DB
	log *slog.Logger

	positionCh chan PositionRecord
	alertCh    chan AlertRecord

	droppedPositions atomic.Int64
	droppedAlerts    atomic.Int64

	vehicleMu    sync.Mutex
	vehicleCache map[string]string // tenantID+"|"+externalID -> vehicles.id
}

func NewWriter(db *sql.DB, log *slog.Logger, queueSize int) *Writer {
	return &Writer{
		db:           db,
		log:          log,
		positionCh:   make(chan PositionRecord, queueSize),
		alertCh:      make(chan AlertRecord, queueSize),
		vehicleCache: make(map[string]string),
	}
}

// Start launches the batching goroutines. Call once.
func (w *Writer) Start(batchInterval time.Duration, batchSize int) {
	go batchLoop(w.positionCh, batchInterval, batchSize, w.flushPositions)
	go batchLoop(w.alertCh, batchInterval, batchSize, w.flushAlerts)
}

// EnqueuePosition implements pipeline.ColdPathSink. applied records
// whether this ping won the hot-path staleness check, kept here even
// though the current schema doesn't have a column for it yet, so it's
// available the moment that's worth adding rather than needing a second
// pass through this code.
func (w *Writer) EnqueuePosition(ping pipeline.PositionPing, tenantID string, applied bool) {
	select {
	case w.positionCh <- PositionRecord{
		VehicleExternalID: ping.VehicleID, TenantID: tenantID,
		RecordedAt: ping.RecordedAt, ReceivedAt: ping.ReceivedAt,
		Lat: ping.Lat, Lon: ping.Lon, SpeedKPH: ping.SpeedKPH, HeadingDeg: ping.HeadingDeg,
		Source: ping.Source,
	}:
	default:
		w.droppedPositions.Add(1)
	}
}

// EnqueueAlert implements pipeline.ColdPathSink.
func (w *Writer) EnqueueAlert(e pipeline.AlertEvent, tenantID string) {
	select {
	case w.alertCh <- AlertRecord{
		VehicleExternalID: e.VehicleID, TenantID: tenantID,
		GeofenceID: e.GeofenceID, EventType: e.EventType, EventTime: e.EventTime,
	}:
	default:
		w.droppedAlerts.Add(1)
	}
}

func (w *Writer) DroppedPositions() int64 { return w.droppedPositions.Load() }
func (w *Writer) DroppedAlerts() int64    { return w.droppedAlerts.Load() }

// batchLoop collects items from ch until either batchSize is reached or
// interval elapses, then flushes, whichever comes first, so a quiet
// period doesn't hold data indefinitely and a burst doesn't wait the
// full interval. Generic over the record type so the position and alert
// batchers share one implementation instead of two near-identical copies.
func batchLoop[T any](ch <-chan T, interval time.Duration, size int, flush func([]T)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	batch := make([]T, 0, size)
	for {
		select {
		case item, ok := <-ch:
			if !ok {
				if len(batch) > 0 {
					flush(batch)
				}
				return
			}
			batch = append(batch, item)
			if len(batch) >= size {
				flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flush(batch)
				batch = batch[:0]
			}
		}
	}
}

func (w *Writer) flushPositions(batch []PositionRecord) {
	ctx := context.Background()
	values := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*8)
	i := 1
	for _, r := range batch {
		vehicleID, err := w.resolveVehicle(ctx, r.TenantID, r.VehicleExternalID)
		if err != nil {
			w.log.Error("coldpath: resolve vehicle failed", "vehicle", r.VehicleExternalID, "error", err)
			continue
		}
		values = append(values, fmt.Sprintf(
			"($%d, $%d, $%d, ST_SetSRID(ST_MakePoint($%d, $%d), 4326)::geography, $%d, $%d, $%d)",
			i, i+1, i+2, i+3, i+4, i+5, i+6, i+7))
		args = append(args, vehicleID, r.RecordedAt, r.ReceivedAt, r.Lon, r.Lat, r.SpeedKPH, r.HeadingDeg, r.Source)
		i += 8
	}
	if len(values) == 0 {
		return
	}
	query := "INSERT INTO position_history (vehicle_id, recorded_at, received_at, location, speed_kph, heading_deg, source) VALUES " +
		strings.Join(values, ",")
	if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
		w.log.Error("coldpath: batch insert position_history failed", "count", len(values), "error", err)
	}
}

func (w *Writer) flushAlerts(batch []AlertRecord) {
	ctx := context.Background()
	values := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*4)
	i := 1
	for _, a := range batch {
		vehicleID, err := w.resolveVehicle(ctx, a.TenantID, a.VehicleExternalID)
		if err != nil {
			w.log.Error("coldpath: resolve vehicle failed", "vehicle", a.VehicleExternalID, "error", err)
			continue
		}
		// event_type is cast explicitly to its Postgres enum type
		// (geofence_event_type) rather than left for the driver to infer.
		// A bare parameter into an enum column is exactly the kind of
		// thing that works in one driver/version and silently doesn't in
		// another; the cast makes the intent unambiguous regardless.
		values = append(values, fmt.Sprintf("($%d, $%d, $%d::geofence_event_type, $%d)", i, i+1, i+2, i+3))
		args = append(args, vehicleID, a.GeofenceID, a.EventType, a.EventTime)
		i += 4
	}
	if len(values) == 0 {
		return
	}
	query := "INSERT INTO geofence_events (vehicle_id, geofence_id, event_type, event_time) VALUES " +
		strings.Join(values, ",")
	if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
		w.log.Error("coldpath: batch insert geofence_events failed", "count", len(values), "error", err)
	}
}

// resolveVehicle maps a source system's own vehicle identifier (an IMEI,
// or a simulator id like "sim-42") to this system's internal vehicle
// UUID, auto-provisioning a vehicle row on first sight. Cached so a
// batch of 500 pings from the same vehicle costs one upsert, not 500.
func (w *Writer) resolveVehicle(ctx context.Context, tenantID, externalID string) (string, error) {
	cacheKey := tenantID + "|" + externalID
	w.vehicleMu.Lock()
	if id, ok := w.vehicleCache[cacheKey]; ok {
		w.vehicleMu.Unlock()
		return id, nil
	}
	w.vehicleMu.Unlock()

	var id string
	err := w.db.QueryRowContext(ctx, `
		INSERT INTO vehicles (tenant_id, external_id)
		VALUES ($1, $2)
		ON CONFLICT (tenant_id, external_id) DO UPDATE SET external_id = EXCLUDED.external_id
		RETURNING id
	`, tenantID, externalID).Scan(&id)
	if err != nil {
		return "", err
	}

	w.vehicleMu.Lock()
	w.vehicleCache[cacheKey] = id
	w.vehicleMu.Unlock()
	return id, nil
}
