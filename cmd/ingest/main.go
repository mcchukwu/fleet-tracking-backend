// cmd/ingest is the WebSocket adapter: authenticate the device, decode
// frames into pipeline.PositionPing, call Submit. Ingestion logic lives in
// internal/pipeline.
//
// The public listener exposes only /ws/ingest. The admin listener exposes
// metrics, debug and the live feed and is not authenticated.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	channelBufferSize = 20_000
	workerPoolSize    = 64
	coldPathQueueSize = 20_000
	maxConnections    = 20_000
	wsReadLimit       = 32 << 10
)

// Transport-level counters, not pipeline counters.
var (
	acceptedCount   atomic.Int64
	rejectedCount   atomic.Int64
	idleClosedCount atomic.Int64
	authFailedCount atomic.Int64
	activeCount     atomic.Int64

	connMu sync.Mutex
	conns  = make(map[*websocket.Conn]struct{})
)

// wsPing has no vehicle id: identity comes from the handshake.
type wsPing struct {
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph,omitempty"`
	HeadingDeg float64   `json:"heading_deg,omitempty"`
	RecordedAt time.Time `json:"recorded_at"`
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg, err := config.LoadIngest()
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	rdb, err := db.ConnectRedis(cfg.RedisAddr, cfg.RedisPassword)
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
	if err := db.RequireSchema(context.Background(), pg); err != nil {
		log.Error("database is not ready", "error", err)
		os.Exit(1)
	}

	geoCache, err := geofence.LoadFromPostgres(context.Background(), pg)
	if err != nil {
		log.Error("cannot load geofences", "error", err)
		os.Exit(1)
	}

	deviceRegistry := auth.NewRegistry(pg)

	pl := pipeline.New(rdb, geoCache, pipeline.Config{
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

	log.Info("geofences loaded", "names", geoCache.AllPolygonNames())
	log.Info("config",
		"overflow_policy", cfg.OverflowPolicy, "idle_timeout", cfg.IdleTimeout,
		"accept_rate", cfg.AcceptRate, "accept_burst", cfg.AcceptBurst,
		"coldpath_batch_window", cfg.ColdPathBatchWindow, "coldpath_batch_size", cfg.ColdPathBatchSize,
		"public_addr", cfg.ListenAddr, "admin_addr", cfg.AdminAddr)

	acceptLimiter := ratelimit.NewTokenBucket(cfg.AcceptRate, cfg.AcceptBurst)
	ipLimiter := ratelimit.NewPerIP(cfg.PerIPRate, cfg.PerIPBurst)

	publicMux := http.NewServeMux()
	publicMux.HandleFunc("/ws/ingest", func(w http.ResponseWriter, r *http.Request) {
		handleConnection(w, r, pl, ipLimiter, acceptLimiter, cfg.IdleTimeout, log, deviceRegistry)
	})

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/metrics/ingest", metricsIngestHandler(pl, coldWriter, liveHub))
	adminMux.HandleFunc("/metrics/alerts", metricsAlertsHandler(pl))
	adminMux.HandleFunc("/debug/alerts", debugAlertsHandler(pl))
	adminMux.HandleFunc("/debug/vehicle", debugVehicleHandler(rdb))
	adminMux.HandleFunc("/ws/live", liveHub.ServeWS)
	adminMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	adminMux.HandleFunc("/readyz", readyHandler(pg, rdb))

	publicSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           publicMux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	adminSrv := &http.Server{
		Addr:              cfg.AdminAddr,
		Handler:           adminMux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	go func() {
		log.Info("public ingest listener up", "addr", cfg.ListenAddr)
		if err := publicSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("public server error", "error", err)
			os.Exit(1)
		}
	}()
	go func() {
		log.Info("internal admin listener up (metrics/debug/live. do not expose publicly)", "addr", cfg.AdminAddr)
		if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("admin server error", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Info("shutting down")

	closeConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = publicSrv.Shutdown(ctx)
	_ = adminSrv.Shutdown(ctx)
	pl.Stop()
	coldWriter.Close()
	hubCancel()
	log.Info("shutdown complete")
}

// handleConnection admits and authenticates one device, then reads frames
// until the connection closes or goes idle.
func handleConnection(
	w http.ResponseWriter, r *http.Request, pl *pipeline.Pipeline,
	ipLimiter *ratelimit.PerIP, acceptLimiter *ratelimit.TokenBucket, idleTimeout time.Duration,
	log *slog.Logger, deviceRegistry *auth.Registry,
) {
	if !ipLimiter.Allow(remoteIP(r)) {
		rejectedCount.Add(1)
		http.Error(w, "connection rate exceeded for this address", http.StatusTooManyRequests)
		return
	}
	if activeCount.Load() >= maxConnections {
		rejectedCount.Add(1)
		http.Error(w, "connection limit reached", http.StatusServiceUnavailable)
		return
	}
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

	vehicleID, authorized, err := deviceRegistry.AuthorizeDevice(r.Context(), tenantID, deviceID)
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
	trackConnection(conn)
	defer untrackConnection(conn)
	conn.SetReadLimit(wsReadLimit)
	acceptedCount.Add(1)
	activeCount.Add(1)
	defer activeCount.Add(-1)

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
		if err := validator.ValidateMotion(raw.SpeedKPH, raw.HeadingDeg); err != nil {
			pl.RecordValidationError()
			log.Warn("rejected invalid motion", "vehicle_id", deviceID, "error", err)
			continue
		}

		pl.Submit(pipeline.PositionPing{
			TenantID: tenantID, VehicleID: deviceID, VehicleUUID: vehicleID,
			Lat: raw.Lat, Lon: raw.Lon,
			SpeedKPH: raw.SpeedKPH, HeadingDeg: raw.HeadingDeg,
			RecordedAt: raw.RecordedAt, ReceivedAt: receivedAt, Source: "simulator",
		})
	}
}

func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	token, ok := strings.CutPrefix(header, prefix)
	if ok {
		return token
	}

	return ""
}

func metricsIngestHandler(pl *pipeline.Pipeline, cw *coldpath.Writer, liveHub *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		positions, alerts := cw.QueueLen()
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
			"active_connections":         activeCount.Load(),
			"live_clients":               liveHub.ActiveClients(),
			"live_drops":                 liveHub.Drops(),
			"coldpath_dropped_positions": cw.DroppedPositions(),
			"coldpath_dropped_alerts":    cw.DroppedAlerts(),
			"coldpath_failed_positions":  cw.FailedPositions(),
			"coldpath_failed_alerts":     cw.FailedAlerts(),
			"coldpath_queue_positions":   positions,
			"coldpath_queue_alerts":      alerts,
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

// debugVehicleHandler exposes raw Redis state for one tenant and vehicle.
func debugVehicleHandler(rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.URL.Query().Get("tenant")
		id := r.URL.Query().Get("id")
		if tenantID == "" || id == "" {
			http.Error(w, "tenant and id query parameters are required", http.StatusBadRequest)
			return
		}
		state, err := rdb.HGetAll(r.Context(), fmt.Sprintf("tenant:%s:vehicle:%s:state", tenantID, id)).Result()
		if err != nil {
			http.Error(w, "redis unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state)
	}
}

func trackConnection(c *websocket.Conn) {
	connMu.Lock()
	conns[c] = struct{}{}
	connMu.Unlock()
}

func untrackConnection(c *websocket.Conn) {
	connMu.Lock()
	delete(conns, c)
	connMu.Unlock()
}

// closeConnections runs before http.Server.Shutdown: hijacked WebSocket
// connections are not tracked by Shutdown, so they have to be closed here
// or the pipeline would keep receiving frames while it is being stopped.
func closeConnections() {
	connMu.Lock()
	defer connMu.Unlock()
	for c := range conns {
		c.CloseNow()
	}
	clear(conns)
}

func readyHandler(pg *sql.DB, rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := pg.PingContext(ctx); err != nil {
			http.Error(w, "postgres unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
