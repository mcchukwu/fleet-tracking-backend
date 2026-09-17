// ingestion server.
//
// Phase 1 shape unchanged: one goroutine per WS connection, reading only,
// handing pings to a buffered channel; a fixed worker pool drains it.
// Phase 2 adds, inside that same worker: an in-memory geofence containment
// check against a Cache loaded once from Postgres at startup, transition
// detection against per-vehicle state kept in Redis, and an in-memory
// alert log used both to verify correctness and to measure the p99
// alert-latency NFR.
//
// Deliberate Phase 2 simplification, stated explicitly rather than left
// implicit: every incoming ping is evaluated against a single hardcoded
// default tenant's geofences. Vehicles aren't yet resolved to a tenant on
// the ingestion path. That resolution is real multi-tenant work worth
// doing when there's an actual second tenant to test against, not
// before, per the "earn it from measured requirements" rule.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/mcchukwu/fleet-tracking-backend/internal/alert"
	"github.com/mcchukwu/fleet-tracking-backend/internal/geofence"
	"github.com/mcchukwu/fleet-tracking-backend/pkg/config"
	"github.com/mcchukwu/fleet-tracking-backend/pkg/logger"
	"github.com/redis/go-redis/v9"
)

// defaultTenantID must match the tenant id used in migrations/seed_dev.sql.
const defaultTenantID = "00000000-0000-0000-0000-000000000001"

type PositionPing struct {
	VehicleID  string    `json:"vehicle_id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph,omitempty"`
	HeadingDeg float64   `json:"heading_deg,omitempty"`
	RecordedAt time.Time `json:"recorded_at"` // device/event timestamp

	// ReceivedAt is set server-side the instant the frame is read off the
	// socket — never populated from client JSON. This, not RecordedAt, is
	// what NFR-2's "p99 < 100ms from receipt" is measured against.
	ReceivedAt time.Time `json:"-"`
}

const (
	channelBufferSize = 20_000
	workerPoolSize    = 64
	alertLogCapacity  = 100_000
)

var (
	receivedCount atomic.Int64
	droppedCount  atomic.Int64
	redisErrCount atomic.Int64
)

func main() {
	cfg := config.Load()
	if err := config.Validate(cfg); err != nil {
		logger.Error("invalid enviroment configuration: %s", err)
		os.Exit(1)
	}
	logger.Info("loaded environment configuration")

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Error("unable to reach redis")
		os.Exit(1)
	}
	logger.Info("connected to redis")

	db, err := sql.Open("pgx", cfg.DBURL)
	if err != nil {
		logger.Error("unable to open postgres connection")
		os.Exit(1)
	}
	logger.Info("connected to postgres")
	defer db.Close()

	geoCache, err := geofence.LoadFromPostgres(context.Background(), db)
	if err != nil {
		logger.Error("unable to load geofences")
		os.Exit(1)
	}
	logger.Info("loaded geofences for tenant %s: %v", defaultTenantID, polygonNames(geoCache.AllPolygonIDs(defaultTenantID)))

	alerts := alert.NewAlertLog(alertLogCapacity)
	ingestCh := make(chan PositionPing, channelBufferSize)

	for range workerPoolSize {
		go worker(rdb, geoCache, alerts, ingestCh)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws/ingest", func(w http.ResponseWriter, r *http.Request) {
		handleConnection(w, r, ingestCh)
	})
	mux.HandleFunc("/metrics/ingest", metricsIngestHandler(ingestCh))
	mux.HandleFunc("/metrics/alerts", metricsAlertsHandler(alerts))
	mux.HandleFunc("/debug/alerts", debugAlertsHandler(alerts))

	srv := &http.Server{
		Addr:    ":" + cfg.AppPort,
		Handler: mux,
	}

	go func() {
		logger.Info("ingest server listening...")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("unable to start ingest server")
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	logger.Info("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func handleConnection(w http.ResponseWriter, r *http.Request, ingestCh chan<- PositionPing) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		logger.Info("websocket accept error")
		return
	}
	defer conn.CloseNow()

	ctx := r.Context()
	for {
		var ping PositionPing
		if err := wsjson.Read(ctx, conn, &ping); err != nil {
			return
		}
		ping.ReceivedAt = time.Now() // stamped here, not by the client
		receivedCount.Add(1)

		select {
		case ingestCh <- ping:
		default:
			droppedCount.Add(1)
		}
	}
}

// worker does three things per ping, in order: write current position to
// Redis (Phase 1, unchanged), evaluate geofence containment against the
// in-memory cache and diff it against the vehicle's previous inside-set
// to find transitions, then record any transitions to the alert log with
// latency measured from ReceivedAt to right now.
func worker(rdb *redis.Client, geoCache *geofence.Cache, alerts *alert.AlertLog, ingestCh <-chan PositionPing) {
	ctx := context.Background()
	for ping := range ingestCh {
		writePosition(ctx, rdb, ping)
		evaluateGeofences(ctx, rdb, geoCache, alerts, ping)
	}
}

func writePosition(ctx context.Context, rdb *redis.Client, ping PositionPing) {
	key := fmt.Sprintf("vehicle:%s:state", ping.VehicleID)
	_, err := rdb.HSet(ctx, key, map[string]any{
		"lat":         ping.Lat,
		"lon":         ping.Lon,
		"speed_kph":   ping.SpeedKPH,
		"heading_deg": ping.HeadingDeg,
		"recorded_at": ping.RecordedAt.UnixMilli(),
	}).Result()
	if err != nil {
		redisErrCount.Add(1)
		logger.Info("redis write error for %s: %v", ping.VehicleID, err)
	}
}

func evaluateGeofences(ctx context.Context, rdb *redis.Client, geoCache *geofence.Cache, alerts *alert.AlertLog, ping PositionPing) {
	pt := geofence.Point{Lng: ping.Lon, Lat: ping.Lat}
	currentHits := geoCache.ContainingPolygons(defaultTenantID, pt)

	current := make(map[string]geofence.Polygon, len(currentHits))
	for _, poly := range currentHits {
		current[poly.ID] = poly
	}

	insideKey := fmt.Sprintf("vehicle:%s:inside_geofences", ping.VehicleID)
	previousIDs, err := rdb.SMembers(ctx, insideKey).Result()
	if err != nil {
		logger.Info("redis smembers error for %s: %v", ping.VehicleID, err)
		return
	}
	previous := make(map[string]bool, len(previousIDs))
	for _, id := range previousIDs {
		previous[id] = true
	}

	now := time.Now()

	// Entered: in current, not in previous.
	for id, poly := range current {
		if !previous[id] {
			rdb.SAdd(ctx, insideKey, id)
			alerts.Record(alert.AlertEvent{
				VehicleID: ping.VehicleID, GeofenceID: id, GeofenceName: poly.Name,
				EventType: "enter", EventTime: ping.RecordedAt, Latency: now.Sub(ping.ReceivedAt),
			})
		}
	}
	// Exited: in previous, not in current.
	for id := range previous {
		if _, stillIn := current[id]; !stillIn {
			rdb.SRem(ctx, insideKey, id)
			name := id
			if poly, ok := geoCache.AllPolygonIDs(defaultTenantID)[id]; ok {
				name = poly.Name
			}
			alerts.Record(alert.AlertEvent{
				VehicleID: ping.VehicleID, GeofenceID: id, GeofenceName: name,
				EventType: "exit", EventTime: ping.RecordedAt, Latency: now.Sub(ping.ReceivedAt),
			})
		}
	}
}

func metricsIngestHandler(ingestCh chan PositionPing) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{
			"received":       receivedCount.Load(),
			"dropped":        droppedCount.Load(),
			"redis_errors":   redisErrCount.Load(),
			"queue_length":   int64(len(ingestCh)),
			"queue_capacity": int64(cap(ingestCh)),
		})
	}
}

func metricsAlertsHandler(alerts *alert.AlertLog) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(alerts.LatencyPercentiles())
	}
}

func debugAlertsHandler(alerts *alert.AlertLog) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(alerts.Snapshot())
	}
}

func polygonNames(polys map[string]geofence.Polygon) []string {
	names := make([]string, 0, len(polys))
	for _, p := range polys {
		names = append(names, p.Name)
	}
	return names
}
