// cmd/ingest is the WebSocket adapter binary: it knows how to speak
// WebSocket+JSON and how to admit/police connections, and nothing else.
// Tis file's only job is translating WebSocket frames into pipeline.PositionPing.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/joho/godotenv"
	"github.com/mcchukwu/fleet-tracking-backend/internal/config"
	"github.com/mcchukwu/fleet-tracking-backend/internal/db"
	"github.com/mcchukwu/fleet-tracking-backend/internal/geofence"
	"github.com/mcchukwu/fleet-tracking-backend/internal/pipeline"
	"github.com/mcchukwu/fleet-tracking-backend/internal/ratelimit"
	"github.com/mcchukwu/fleet-tracking-backend/internal/validator"
	"github.com/redis/go-redis/v9"
)

const (
	defaultTenantID   = "00000000-0000-0000-0000-000000000001"
	channelBufferSize = 20_000
	workerPoolSize    = 64
)

// Connection-level counters. These live here, not in internal/pipeline,
// because they're specific to this adapter's transport (accepting WS
// connections, detecting idle ones), a future TCP adapter would keep
// its own equivalent counters rather than share these.
var (
	acceptedCount   atomic.Int64
	rejectedCount   atomic.Int64
	idleClosedCount atomic.Int64
)

// wsPing is the wire format for the WS adapter specifically. Separate
// from pipeline.PositionPing on purpose, so a change to the JSON wire
// format never forces a change to the adapter-agnostic core type.
type wsPing struct {
	VehicleID  string    `json:"vehicle_id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph,omitempty"`
	HeadingDeg float64   `json:"heading_deg,omitempty"`
	RecordedAt time.Time `json:"recorded_at"`
}

func main() {
	log := slog.Default()

	if err := godotenv.Load(); err != nil {
		log.Debug("failed to load environment variables", "error", err)
	}

	cfg, err := config.LoadIngest()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	rdb, err := db.ConnectRedis(cfg.RedisAddr)
	if err != nil {
		log.Error("cannot connect to redis", "error", err)
		os.Exit(1)
	}
	defer rdb.Close()

	pg, err := db.ConnectPostgres(cfg.PGDSN)
	if err != nil {
		log.Error("cannot connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pg.Close()

	geoCache, err := geofence.LoadFromPostgres(context.Background(), pg)
	if err != nil {
		log.Error("cannot load geofences", "error", err)
		os.Exit(1)
	}

	pl := pipeline.New(rdb, geoCache, pipeline.Config{
		TenantID:          defaultTenantID,
		OverflowPolicy:    cfg.OverflowPolicy,
		ChannelBufferSize: channelBufferSize,
		WorkerPoolSize:    workerPoolSize,
	}, log)
	pl.Start()

	log.Info("geofences loaded", "tenant", defaultTenantID, "names", pl.PolygonNames())
	log.Info("config",
		"overflow_policy", cfg.OverflowPolicy, "idle_timeout", cfg.IdleTimeout,
		"accept_rate", cfg.AcceptRate, "accept_burst", cfg.AcceptBurst,
		"public_addr", cfg.ListenAddr, "admin_addr", cfg.AdminAddr)

	acceptLimiter := ratelimit.NewTokenBucket(cfg.AcceptRate, cfg.AcceptBurst)

	publicMux := http.NewServeMux()
	publicMux.HandleFunc("/ws/ingest", func(w http.ResponseWriter, r *http.Request) {
		handleConnection(w, r, pl, acceptLimiter, cfg.IdleTimeout, log)
	})

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/metrics/ingest", metricsIngestHandler(pl))
	adminMux.HandleFunc("/metrics/alerts", metricsAlertsHandler(pl))
	adminMux.HandleFunc("/debug/alerts", debugAlertsHandler(pl))
	adminMux.HandleFunc("/debug/vehicle", debugVehicleHandler(rdb))
	adminMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	publicSrv := &http.Server{Addr: cfg.ListenAddr, Handler: publicMux}
	adminSrv := &http.Server{Addr: cfg.AdminAddr, Handler: adminMux}

	go func() {
		log.Info("public ingest listener up", "addr", cfg.ListenAddr)
		if err := publicSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("public server error", "error", err)
			os.Exit(1)
		}
	}()
	go func() {
		log.Info("internal admin listener up (metrics/debug — do not expose publicly)", "addr", cfg.AdminAddr)
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("admin server error", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	log.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = publicSrv.Shutdown(ctx)
	_ = adminSrv.Shutdown(ctx)
}

// handleConnection is the entire WS adapter: admission-rate check, then a
// read loop with a re-armed idle deadline per read (half-open detection),
// validating each frame before translating it into a pipeline.PositionPing
// and handing it to the pipeline. No ingestion logic lives here, only
// "how to speak WebSocket safely."
func handleConnection(
	w http.ResponseWriter, r *http.Request, pl *pipeline.Pipeline,
	acceptLimiter *ratelimit.TokenBucket, idleTimeout time.Duration, log *slog.Logger,
) {
	if !acceptLimiter.Allow() {
		rejectedCount.Add(1)
		http.Error(w, "connection admission rate exceeded, retry shortly", http.StatusServiceUnavailable)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Warn("accept error", "error", err)
		return
	}
	defer conn.CloseNow()
	acceptedCount.Add(1)

	parentCtx := r.Context()
	for {
		readCtx, cancel := context.WithTimeout(parentCtx, idleTimeout)
		var raw wsPing
		err := wsjson.Read(readCtx, conn, &raw)
		deadlineExceeded := readCtx.Err() == context.DeadlineExceeded
		cancel()
		if err != nil {
			if deadlineExceeded {
				idleClosedCount.Add(1)
			}
			return // idle timeout, client close, or bad frame. connection is done either way
		}

		receivedAt := time.Now()
		if err := validator.ValidatePing(raw.VehicleID, raw.Lat, raw.Lon, raw.RecordedAt); err != nil {
			pl.RecordValidationError()
			log.Warn("rejected invalid ping", "vehicle_id", raw.VehicleID, "error", err)
			continue
		}

		pl.Submit(pipeline.PositionPing{
			VehicleID: raw.VehicleID, Lat: raw.Lat, Lon: raw.Lon,
			SpeedKPH: raw.SpeedKPH, HeadingDeg: raw.HeadingDeg,
			RecordedAt: raw.RecordedAt, ReceivedAt: receivedAt,
		})
	}
}

func metricsIngestHandler(pl *pipeline.Pipeline) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"received":          pl.Received(),
			"dropped":           pl.Dropped(),
			"redis_errors":      pl.RedisErrors(),
			"stale":             pl.Stale(),
			"validation_errors": pl.ValidationErrors(),
			"accepted":          acceptedCount.Load(),
			"rejected":          rejectedCount.Load(),
			"idle_closed":       idleClosedCount.Load(),
			"queue_length":      pl.QueueLen(),
			"queue_capacity":    pl.QueueCap(),
		})
	}
}

func metricsAlertsHandler(pl *pipeline.Pipeline) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(pl.AlertLatencyPercentiles())
	}
}

func debugAlertsHandler(pl *pipeline.Pipeline) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(pl.RecentAlerts())
	}
}

// debugVehicleHandler exposes a vehicle's raw hot-path Redis state.
// admin-only (see the two-listener split at the top of this file), used
// by cmd/routetest to verify a stale ping never overwrote the real
// current position.
func debugVehicleHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "id query parameter is required", http.StatusBadRequest)
			return
		}
		state, err := rdb.HGetAll(r.Context(), fmt.Sprintf("vehicle:%s:state", id)).Result()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state)
	}
}
