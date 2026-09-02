// Ingestion server.
//
// Design, in one paragraph: each WebSocket connection is owned by exactly
// one goroutine, whose only job is to read frames off the socket and push
// them onto a shared buffered channel, it does no I/O to Redis and never
// blocks on anything downstream. A fixed size pool of worker goroutines
// drains that channel and does the actual Redis write. This separation is
// the whole point: a slow or backed-up Redis write must never stall a
// socket read, or every other connection queues up behind one bad write.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mcchukwu/fleet-tracking-backend/pkg/config"
	"github.com/mcchukwu/fleet-tracking-backend/pkg/logger"
	"github.com/redis/go-redis/v9"
)

// PositionPing is the internal representation every ingestion adapter
// converges on. A WebSocket simulator client and, later, a JT808 TCP
// adapter for real hardware both produce one of these and hand it to the
// same channel. The worker pool below never needs to know which adapter
// a given ping came from.
type PositionPing struct {
	VehicleID  string    `json:"vehicle_id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph,omitempty"`
	HeadingDeg float64   `json:"heading_deg,omitempty"`
	RecordedAt time.Time `json:"recorded_at"` // device/event timestamp, not receipt time
}

const (
	// Sized generously above expected burst (5,000 vehicles at a 5-10s
	// cadence is ~500-1,000 msgs/sec sustained). The channel absorbs
	// short bursts; the worker pool sizing is what actually has to keep
	// pace on average. Tune both against real load-test numbers, not
	// guesses, this value is a starting point, not a claim.
	channelBufferSize = 20_000
	workerPoolSize    = 64
)

var (
	receivedCount atomic.Int64
	droppedCount  atomic.Int64
	redisErrCount atomic.Int64
)

func main() {
	cfg := config.Load()

	if err := config.Validate(cfg); err != nil {
		logger.Error("invalid configuration")
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		logger.Error("cannot reach redis")
		os.Exit(1)
	}

	ingestCh := make(chan PositionPing, channelBufferSize)

	for range workerPoolSize {
		go worker(rdb, ingestCh)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/ws/ingest", func(w http.ResponseWriter, r *http.Request) {
		handleConnection(w, r, ingestCh)
	})
	mux.HandleFunc("/metrics/ingest", metricsHandler(ingestCh))

	server := &http.Server{
		Addr:    ":" + cfg.AppPort,
		Handler: mux,
	}

	go func() {
		logger.Info("ingest server listening")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed to start")
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	logger.Info("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

// handleConnection is the entire lifetime of one WebSocket connection: it
// accepts, reads in a loop, and returns (closing the connection) the
// moment the client disconnects or sends something unparseable. It does
// nothing else, no Redis calls, no blocking work by design.
func handleConnection(w http.ResponseWriter, r *http.Request, ingestCh chan<- PositionPing) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("accept error: %v", err)
		return
	}
	defer conn.CloseNow()

	ctx := r.Context()
	for {
		var ping PositionPing
		if err := wsjson.Read(ctx, conn, &ping); err != nil {
			// Client closed, network error, or bad frame, either way
			// this connection is done. Nothing to clean up beyond the
			// deferred CloseNow(); the goroutine simply exits.
			return
		}
		// atomic.AddInt64(&receivedCount, 1)
		receivedCount.Add(1)

		select {
		case ingestCh <- ping:
			// handed off; this goroutine goes straight back to reading
		default:
			// Channel is full. We do NOT block the read loop waiting for
			// space, a stalled read loop backs up the TCP receive buffer
			// and eventually the client's own send. Instead we count the
			// drop honestly (NFR-1 requires drops to be counted, not
			// silent) and move on. Hitting this path under the target
			// load is a signal to raise channelBufferSize or
			// workerPoolSize, not to hide it.
			// atomic.AddInt64(&droppedCount, 1)
			droppedCount.Add(1)
		}
	}
}

// worker drains the shared channel and writes current position to Redis.
// This is intentionally the only place Redis is touched in Phase 1 — no
// Postgres write yet (that's Phase 4's cold-path writer).
func worker(rdb *redis.Client, ingestCh <-chan PositionPing) {
	ctx := context.Background()
	for ping := range ingestCh {
		key := fmt.Sprintf("vehicle:%s:state", ping.VehicleID)
		_, err := rdb.HSet(ctx, key, map[string]any{
			"lat":         ping.Lat,
			"lon":         ping.Lon,
			"speed_kph":   ping.SpeedKPH,
			"heading_deg": ping.HeadingDeg,
			"recorded_at": ping.RecordedAt.UnixMilli(),
		}).Result()
		if err != nil {
			// atomic.AddInt64(&redisErrCount, 1)
			redisErrCount.Add(1)
			logger.Error("redis write error for %s: %v", ping.VehicleID, err)
		}
	}
}

func metricsHandler(ingestCh chan PositionPing) http.HandlerFunc {
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
