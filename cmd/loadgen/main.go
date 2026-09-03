// Load generator for Phase 1's exit criteria:

// open 5,000+ concurrent WebSocket connections against the ingest
// server's simulator-facing endpoint and send a realistic per-vehicle
// ping cadence, long enough to prove sustained throughput rather than a burst.
//
// Important scope note: this simulates against the WEBSOCKET adapter,
// the interface a browser/demo client (or this simulator) speaks. It does
// NOT simulate the real-hardware interface. Real trackers and dashcams
// speak raw TCP/UDP in a manufacturer/standard-specific binary protocol
// (e.g. JT/T808), not WebSocket + JSON. That's a separate adapter, built
// in a later phase, feeding the same internal pipeline. This generator
// proves the WS adapter and the core pipeline behind it; it says nothing
// about the hardware adapters yet.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mcchukwu/fleet-tracking-backend/pkg/logger"
)

type PositionPing struct {
	VehicleID  string    `json:"vehicle_id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph,omitempty"`
	HeadingDeg float64   `json:"heading_deg,omitempty"`
	RecordedAt time.Time `json:"recorded_at"`
}

func main() {
	url := flag.String("url", "ws://localhost:8080/ws/ingest", "target ws endpoint")
	vehicles := flag.Int("vehicles", 5000, "number of simulated vehicles (concurrent connections)")
	interval := flag.Duration("interval", 7*time.Second, "average ping interval per vehicle")
	duration := flag.Duration("duration", 2*time.Minute, "how long each vehicle keeps pinging")
	rampUp := flag.Duration("rampup", 10*time.Second, "time to open all connections over, to avoid a connection-storm at t=0")
	flag.Parse()

	var connected, dialErrors, sent, sendErrors int64
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), *duration+*rampUp+30*time.Second)
	defer cancel()

	// Stagger connection opens across rampUp instead of firing all 5,000
	// at once, a real fleet doesn't power on simultaneously, and a
	// simultaneous connection storm tests your accept-loop and OS socket
	// limits more than it tests sustained ingestion, which is what this
	// phase is actually trying to prove.
	perConnDelay := time.Duration(0)
	if *vehicles > 0 {
		perConnDelay = *rampUp / time.Duration(*vehicles)
	}

	for i := 0; i < *vehicles; i++ {
		wg.Add(1)
		go func(vehicleID int) {
			defer wg.Done()
			simulateVehicle(ctx, *url, vehicleID, *interval, *duration,
				&connected, &dialErrors, &sent, &sendErrors)
		}(i)
		time.Sleep(perConnDelay)
	}

	logger.Info("all %d connection goroutines launched, running for ~%s", *vehicles, *duration)
	wg.Wait()

	fmt.Printf(
		"connected=%d dial_errors=%d sent=%d send_errors=%d\n",
		atomic.LoadInt64(&connected), atomic.LoadInt64(&dialErrors),
		atomic.LoadInt64(&sent), atomic.LoadInt64(&sendErrors),
	)
	fmt.Println("compare `sent` above against the ingest server's /metrics/ingest " +
		"`received` and `dropped` counters. sent should equal received, and " +
		"dropped should be 0 for Phase 1 to pass.")
}

func simulateVehicle(
	ctx context.Context,
	url string,
	id int,
	interval,
	duration time.Duration,
	connected,
	dialErrors,
	sent,
	sendErrors *int64,
) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		atomic.AddInt64(dialErrors, 1)
		return
	}
	defer conn.CloseNow()
	atomic.AddInt64(connected, 1)

	// Start each simulated vehicle somewhere in the Lagos area with a
	// small random offset, then have it drift with each ping, realistic
	// enough to exercise the pipeline without needing real route data.
	lat := 6.5244 + rand.Float64()*0.1
	lon := 3.3792 + rand.Float64()*0.1

	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		// Jitter the interval so 5,000 vehicles don't end up pinging in
		// lockstep, which would understate real-world burstiness.
		wait := interval/2 + time.Duration(rand.Int63n(int64(interval)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		lat += (rand.Float64() - 0.5) * 0.001
		lon += (rand.Float64() - 0.5) * 0.001

		ping := PositionPing{
			VehicleID:  fmt.Sprintf("sim-%d", id),
			Lat:        lat,
			Lon:        lon,
			SpeedKPH:   rand.Float64() * 80,
			HeadingDeg: rand.Float64() * 360,
			RecordedAt: time.Now(),
		}

		writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := wsjson.Write(writeCtx, conn, ping)
		cancel()
		if err != nil {
			atomic.AddInt64(sendErrors, 1)
			return
		}
		atomic.AddInt64(sent, 1)
	}
}
