// cmd/ingest is the WebSocket adapter binary: it knows how to speak
// WebSocket+JSON, how to authenticate and admit/police connections, and
// nothing else. All ingestion logic: the worker pool, the
// out-of-order-safe hot-path write, geofence evaluation lives in
// internal/pipeline, which this file constructs once and feeds via
// Submit(). A future real-hardware adapter (a separate cmd speaking
// TCP/JT808) would construct the same pipeline.Pipeline and call the
// same Submit, this file's only job is translating WebSocket frames
// into pipeline.PositionPing.
//
// Two HTTP listeners, deliberately: a public one (LISTEN_ADDR) exposing
// only /ws/ingest the surface untrusted devices connect to, and an
// internal one (ADMIN_ADDR) exposing /metrics/*, /debug/*, and /ws/live,
// meant to stay off the public internet entirely.
//
// Device authentication: a WS connection must present an
// `Authorization: Bearer <api-key>` header (tenant-scoped, validated via
// internal/auth against Postgres) and an `X-Device-ID` header (the
// device's own identifier, an IMEI for real hardware, a simulator id
// like "sim-42" for the load generator), checked once at connection time,
// not per ping. Every ping on that connection inherits the identity
// validated at handshake, the wire format no longer carries vehicle_id
// in the ping body at all, specifically so a ping's body can never claim
// an identity different from the one the connection authenticated as.
//
// Known, named limitation: the validated tenant_id is used
// to gate the connection and to authorize the device, but
// pipeline.Pipeline is still constructed with one fixed TenantID
// (defaultTenantID) for geofence evaluation, it does not yet route each
// ping's geofence lookup by its own validated tenant. That's fine while
// there's one real tenant (the dev/seed tenant) and is the next thing to
// change the moment a second tenant exists; not done in to
// keep this change reviewable on its own.
//
// Out-of-order handling lives in internal/pipeline,
// pings from the hot path and from live alerting" design was chosen over
// a universal buffering delay, and what that honestly does and doesn't
// guarantee against NFR-2's latency budget.
//
// Two optional output ports are wired into the pipeline:
// internal/coldpath, an async batched Postgres writer giving every
// accepted ping and every alert durable storage (NFR-3); and internal/hub,
// a live WebSocket fan-out to dashboard/operator clients (the output half
// of FR-8).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mcchukwu/fleet-tracking-backend/internal/auth"
	"github.com/mcchukwu/fleet-tracking-backend/internal/coldpath"
	"github.com/mcchukwu/fleet-tracking-backend/internal/config"
	"github.com/mcchukwu/fleet-tracking-backend/internal/db"
	"github.com/mcchukwu/fleet-tracking-backend/internal/geofence"
	"github.com/mcchukwu/fleet-tracking-backend/internal/hub"
	"github.com/mcchukwu/fleet-tracking-backend/internal/pipeline"
	"github.com/mcchukwu/fleet-tracking-backend/internal/ratelimit"
	"github.com/mcchukwu/fleet-tracking-backend/internal/validator"
	"github.com/redis/go-redis/v9"
)

const (
	defaultTenantID   = "00000000-0000-0000-0000-000000000001"
	channelBufferSize = 20_000
	workerPoolSize    = 64
	coldPathQueueSize = 20_000
)

// Connection-level counters. These live here, not in internal/pipeline,
// because they're specific to this adapter's transport (accepting WS
// connections, authenticating them, detecting idle ones), a future TCP
// adapter would keep its own equivalent counters rather than share these.
var (
	acceptedCount   atomic.Int64
	rejectedCount   atomic.Int64
	idleClosedCount atomic.Int64
	authFailedCount atomic.Int64
)

// wsPing is the wire format for the WS adapter specifically, separate
// from pipeline.PositionPing on purpose, so a change to the JSON wire
// format never forces a change to the adapter-agnostic core type. Note
// there is no vehicle_id field: identity comes from the authenticated
// connection (X-Device-ID), never from the ping body.
type wsPing struct {
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph,omitempty"`
	HeadingDeg float64   `json:"heading_deg,omitempty"`
	RecordedAt time.Time `json:"recorded_at"`
}

func main() {
	log := slog.Default()

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

	deviceRegistry := auth.NewRegistry(pg)

	pl := pipeline.New(rdb, geoCache, pipeline.Config{
		TenantID:          defaultTenantID,
		OverflowPolicy:    cfg.OverflowPolicy,
		ChannelBufferSize: channelBufferSize,
		WorkerPoolSize:    workerPoolSize,
	}, log)

	coldWriter := coldpath.NewWriter(pg, log, coldPathQueueSize)
	coldWriter.Start(cfg.ColdPathBatchWindow, cfg.ColdPathBatchSize)
	pl.SetColdPathSink(coldWriter)

	liveHub := hub.New(log)
	hubCtx, hubCancel := context.WithCancel(context.Background())
	defer hubCancel()
	go liveHub.Run(hubCtx)
	pl.SetBroadcaster(liveHub)

	pl.Start()

	log.Info("geofences loaded", "tenant", defaultTenantID, "names", pl.PolygonNames())
	log.Info("config",
		"overflow_policy", cfg.OverflowPolicy, "idle_timeout", cfg.IdleTimeout,
		"accept_rate", cfg.AcceptRate, "accept_burst", cfg.AcceptBurst,
		"coldpath_batch_window", cfg.ColdPathBatchWindow, "coldpath_batch_size", cfg.ColdPathBatchSize,
		"public_addr", cfg.ListenAddr, "admin_addr", cfg.AdminAddr)

	acceptLimiter := ratelimit.NewTokenBucket(cfg.AcceptRate, cfg.AcceptBurst)

	publicMux := http.NewServeMux()
	publicMux.HandleFunc("/ws/ingest", func(w http.ResponseWriter, r *http.Request) {
		handleConnection(w, r, pl, acceptLimiter, cfg.IdleTimeout, log, deviceRegistry)
	})

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/metrics/ingest", metricsIngestHandler(pl, coldWriter))
	adminMux.HandleFunc("/metrics/alerts", metricsAlertsHandler(pl))
	adminMux.HandleFunc("/debug/alerts", debugAlertsHandler(pl))
	adminMux.HandleFunc("/debug/vehicle", debugVehicleHandler(rdb))
	adminMux.HandleFunc("/ws/live", liveHub.ServeWS)
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
		log.Info("internal admin listener up (metrics/debug/live — do not expose publicly)", "addr", cfg.AdminAddr)
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

// handleConnection is the entire WS adapter: admission-rate check,
// device authentication, then a read loop with a re-armed idle deadline
// per read (half-open detection), validating each frame before
// translating it into a pipeline.PositionPing and handing it to the
// pipeline. No ingestion logic lives here, only "how to speak
// WebSocket safely, to someone who's allowed to."
func handleConnection(
	w http.ResponseWriter, r *http.Request, pl *pipeline.Pipeline,
	acceptLimiter *ratelimit.TokenBucket, idleTimeout time.Duration, log *slog.Logger,
	deviceRegistry *auth.Registry,
) {
	if !acceptLimiter.Allow() {
		rejectedCount.Add(1)
		http.Error(w, "connection admission rate exceeded, retry shortly", http.StatusServiceUnavailable)
		return
	}

	apiKey := bearerToken(r.Header.Get("Authorization"))
	deviceID := r.Header.Get("X-Device-ID")
	if apiKey == "" || deviceID == "" {
		authFailedCount.Add(1)
		http.Error(w, "Authorization bearer token and X-Device-ID header are both required", http.StatusUnauthorized)
		return
	}

	tenantID, err := deviceRegistry.AuthenticateKey(r.Context(), apiKey)
	if err != nil {
		authFailedCount.Add(1)
		if err != auth.ErrInvalidKey {
			log.Error("api key lookup failed", "error", err)
		}
		http.Error(w, "invalid api key", http.StatusUnauthorized)
		return
	}

	authorized, err := deviceRegistry.AuthorizeDevice(r.Context(), tenantID, deviceID)
	if err != nil {
		log.Error("device authorization check failed", "device_id", deviceID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if !authorized {
		authFailedCount.Add(1)
		http.Error(w, "device is not registered for this tenant", http.StatusForbidden)
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
			return // idle timeout, client close, or bad frame, connection is done either way
		}

		receivedAt := time.Now()
		if err := validator.ValidatePing(deviceID, raw.Lat, raw.Lon, raw.RecordedAt); err != nil {
			pl.RecordValidationError()
			log.Warn("rejected invalid ping", "vehicle_id", deviceID, "error", err)
			continue
		}

		pl.Submit(pipeline.PositionPing{
			VehicleID: deviceID, Lat: raw.Lat, Lon: raw.Lon,
			SpeedKPH: raw.SpeedKPH, HeadingDeg: raw.HeadingDeg,
			RecordedAt: raw.RecordedAt, ReceivedAt: receivedAt, Source: "simulator",
		})
	}
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	token, ok := strings.CutPrefix(header, prefix)
	if ok {
		return token
	}

	return ""
}

func metricsIngestHandler(pl *pipeline.Pipeline, cw *coldpath.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"received":                   pl.Received(),
			"dropped":                    pl.Dropped(),
			"redis_errors":               pl.RedisErrors(),
			"stale":                      pl.Stale(),
			"validation_errors":          pl.ValidationErrors(),
			"accepted":                   acceptedCount.Load(),
			"rejected":                   rejectedCount.Load(),
			"idle_closed":                idleClosedCount.Load(),
			"auth_failed":                authFailedCount.Load(),
			"queue_length":               pl.QueueLen(),
			"queue_capacity":             pl.QueueCap(),
			"coldpath_dropped_positions": cw.DroppedPositions(),
			"coldpath_dropped_alerts":    cw.DroppedAlerts(),
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

// debugVehicleHandler exposes a vehicle's raw hot-path Redis state,
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
