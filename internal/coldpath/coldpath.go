// Package coldpath batches durable writes to Postgres. Queues are
// bounded; drops and failures are counted.
package coldpath

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mcchukwu/fleet-tracking-backend/internal/pipeline"
)

type PositionRecord struct {
	TenantID          string
	VehicleUUID       string
	VehicleExternalID string
	RecordedAt        time.Time
	ReceivedAt        time.Time
	Lat, Lon          float64
	SpeedKPH          float64
	HeadingDeg        float64
	Source            string
	Applied           bool
}

type AlertRecord struct {
	TenantID          string
	VehicleUUID       string
	VehicleExternalID string
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
	failedPositions  atomic.Int64
	failedAlerts     atomic.Int64

	wg        sync.WaitGroup
	closeOnce sync.Once
}

func NewWriter(db *sql.DB, log *slog.Logger, queueSize int) *Writer {
	return &Writer{
		db:         db,
		log:        log,
		positionCh: make(chan PositionRecord, queueSize),
		alertCh:    make(chan AlertRecord, queueSize),
	}
}

func (w *Writer) Start(batchInterval time.Duration, batchSize int) {
	w.wg.Add(2)
	go func() {
		defer w.wg.Done()
		batchLoop(w.positionCh, batchInterval, batchSize, w.flushPositions)
	}()
	go func() {
		defer w.wg.Done()
		batchLoop(w.alertCh, batchInterval, batchSize, w.flushAlerts)
	}()
}

// Close flushes queued records. Call before closing the database.
func (w *Writer) Close() {
	w.closeOnce.Do(func() {
		close(w.positionCh)
		close(w.alertCh)
	})
	w.wg.Wait()
}

func (w *Writer) EnqueuePosition(ping pipeline.PositionPing, applied bool) {
	select {
	case w.positionCh <- PositionRecord{
		TenantID: ping.TenantID, VehicleUUID: ping.VehicleUUID,
		VehicleExternalID: ping.VehicleID,
		RecordedAt:        ping.RecordedAt, ReceivedAt: ping.ReceivedAt,
		Lat: ping.Lat, Lon: ping.Lon, SpeedKPH: ping.SpeedKPH, HeadingDeg: ping.HeadingDeg,
		Source: ping.Source, Applied: applied,
	}:
	default:
		w.droppedPositions.Add(1)
	}
}

func (w *Writer) EnqueueAlert(e pipeline.AlertEvent) {
	select {
	case w.alertCh <- AlertRecord{
		TenantID: e.TenantID, VehicleUUID: e.VehicleUUID, VehicleExternalID: e.VehicleID,
		GeofenceID: e.GeofenceID, EventType: e.EventType, EventTime: e.EventTime,
	}:
	default:
		w.droppedAlerts.Add(1)
	}
}

func (w *Writer) DroppedPositions() int64 { return w.droppedPositions.Load() }
func (w *Writer) DroppedAlerts() int64    { return w.droppedAlerts.Load() }
func (w *Writer) FailedPositions() int64  { return w.failedPositions.Load() }
func (w *Writer) FailedAlerts() int64     { return w.failedAlerts.Load() }
func (w *Writer) QueueLen() (positions, alerts int) {
	return len(w.positionCh), len(w.alertCh)
}

// batchLoop flushes on a full batch, on the interval, or when ch closes.
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := w.insertPositions(ctx, batch)
	if err == nil {
		return
	}
	w.log.Error("coldpath: position batch failed, retrying per row", "count", len(batch), "error", err)

	for _, r := range batch {
		rowCtx, rowCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := w.insertPositions(rowCtx, []PositionRecord{r}); err != nil {
			w.failedPositions.Add(1)
			w.log.Error("coldpath: position write failed",
				"tenant", r.TenantID, "vehicle", r.VehicleExternalID, "error", err)
		}
		rowCancel()
	}
}

func (w *Writer) insertPositions(ctx context.Context, batch []PositionRecord) error {
	values := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*10)
	i := 1
	for _, r := range batch {
		values = append(values, fmt.Sprintf(
			"($%d,$%d,$%d,$%d,ST_SetSRID(ST_MakePoint($%d,$%d),4326)::geography,$%d,$%d,$%d,$%d)",
			i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9))
		args = append(args, r.TenantID, r.VehicleUUID, r.RecordedAt, r.ReceivedAt,
			r.Lon, r.Lat, r.SpeedKPH, r.HeadingDeg, r.Source, r.Applied)
		i += 10
	}
	query := "INSERT INTO position_history (tenant_id, vehicle_id, recorded_at, received_at, location, speed_kph, heading_deg, source, applied) VALUES " +
		strings.Join(values, ",")
	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

func (w *Writer) flushAlerts(batch []AlertRecord) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := w.insertAlerts(ctx, batch)
	if err == nil {
		w.insertAlertRows(ctx, batch)
		return
	}
	w.log.Error("coldpath: alert batch failed, retrying per row", "count", len(batch), "error", err)

	for _, r := range batch {
		rowCtx, rowCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := w.insertAlerts(rowCtx, []AlertRecord{r}); err != nil {
			w.failedAlerts.Add(1)
			w.log.Error("coldpath: alert write failed",
				"tenant", r.TenantID, "vehicle", r.VehicleExternalID, "error", err)
		} else {
			w.insertAlertRows(rowCtx, []AlertRecord{r})
		}
		rowCancel()
	}
}

func (w *Writer) insertAlerts(ctx context.Context, batch []AlertRecord) error {
	values := make([]string, 0, len(batch))
	args := make([]any, 0, len(batch)*4)
	i := 1
	for _, r := range batch {
		values = append(values, fmt.Sprintf("($%d,$%d,$%d,$%d::geofence_event_type,$%d)", i, i+1, i+2, i+3, i+4))
		args = append(args, r.TenantID, r.VehicleUUID, r.GeofenceID, r.EventType, r.EventTime)
		i += 5
	}
	query := "INSERT INTO geofence_events (tenant_id, vehicle_id, geofence_id, event_type, event_time) VALUES " +
		strings.Join(values, ",") + " ON CONFLICT DO NOTHING"
	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

func (w *Writer) insertAlertRows(ctx context.Context, batch []AlertRecord) {
	for _, r := range batch {
		kind := "geofence_enter"
		severity := "medium"
		if r.EventType == "exit" {
			kind = "geofence_exit"
			severity = "high"
		}
		evidence, _ := json.Marshal(map[string]string{"geofence_id": r.GeofenceID})
		_, err := w.db.ExecContext(ctx, `
			INSERT INTO alerts (tenant_id, vehicle_id, type, severity, occurred_at, evidence)
			VALUES ($1, $2, $3::alert_type, $4::alert_severity, $5, $6::jsonb)
			ON CONFLICT DO NOTHING
		`, r.TenantID, r.VehicleUUID, kind, severity, r.EventTime, string(evidence))
		if err != nil {
			w.log.Error("coldpath: alert insert failed", "tenant", r.TenantID, "vehicle", r.VehicleExternalID, "error", err)
		}
	}
}
